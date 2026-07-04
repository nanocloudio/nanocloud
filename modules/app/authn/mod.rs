//! Authn — the identity-extraction decision as a PIC module (the API plane's
//! auth chain). The TLS handshake and the mTLS peer identity (SPIFFE-style) are
//! the fluxor `tls` foundation's job — a host fact. The *logic* here is mapping
//! a credential to a Kubernetes identity: an mTLS peer identity resolved
//! through /peer-ids/, or a bearer ServiceAccount token verified against the
//! published keyset.
//!
//! Data model (request/response over the store seam, as the API plane uses):
//!   /authn-req/<reqid>  = "peer=<spiffe>"  |  "token=<bearer>"
//!   /peer-ids/<spiffe>  = "id=<k8s identity>"   (OPTIONAL mTLS peer → identity remap)
//!   /authn-resp/<reqid> = "200;<identity>"  |  "401;anonymous"
//!
//! A verified mTLS peer always authenticates: `/peer-ids/<peer>` remaps it to a
//! k8s identity when a binding exists, otherwise the peer subject itself is the
//! identity (the peer certificate's DistinguishedName semantics).
//!
//! **A bearer token is VERIFIED, not looked up.** It arrives as a compact JWS
//! and is checked — signature, `kid`, suite and validity window — against the
//! keyset at `/authn-keys/`, using kagi's own fragments.
//! The identity is the `sub` the credential carries, once the signature says
//! the credential is genuine.
//!
//! **No credential is ever stored.** A bearer token is admitted because its
//! signature verifies under a published key, not because a matching record
//! exists somewhere — so listing a prefix yields no credentials, "is this
//! valid" is a cryptographic question rather than a lookup, and a token's
//! length is bounded by the request, not by a key-size limit.

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::convert::TryInto;
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");
// Verification primitives. `ed25519` references Sha512 (sha384.rs) and helpers
// from p256.rs, so the include order is fixed — the same order kagi's own
// consumers use.
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/ed25519.rs");

// kagi owns the identity protocols, so it owns the code that decides whether
// a credential is genuine. These come from its published `kagi-common` tree
// rather than being reimplemented here — a second JOSE parser in nanocloud is
// a second place for "what is a claim" to be answered differently.
#[path = "../../../target/fluxor/kagi-common/auth_wire.rs"]
mod auth_wire;
#[path = "../../../target/fluxor/kagi-common/b64.rs"]
mod b64;
#[path = "../../../target/fluxor/kagi-common/jose.rs"]
mod jose;
#[path = "../../../target/fluxor/kagi-common/verify_keyset.rs"]
mod verify_keyset;
use verify_keyset::Keyset;

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes + CAS, storage.namespace
// (0x13) prefix LIST + change SUBSCRIBE. Changes are pushed to us as
// namespace.change on the `changes` input channel (allocated by a graph
// self-edge); we drain them to detect movement, then re-service.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const REQ_PREFIX: &[u8] = b"/authn-req/";
const RESP_PREFIX: &[u8] = b"/authn-resp/";
/// Operator-written verification keys, one kagi `MSG_KEY_ADD` frame each.
const KEYS_PREFIX: &[u8] = b"/authn-keys/";
const PEER_PREFIX: &[u8] = b"/peer-ids/";

/// Longest store KEY. 160 is ample: keys here are `/authn-req/<reqid>` and
/// `/peer-ids/<spiffe>`, and nothing indexes by a credential. A key is an
/// index; an index has no business being large, and a request that wants a
/// larger one is asking to key by payload.
const MAX_KEY: usize = 160;
/// Longest store VALUE — the opposite case to `MAX_KEY`, because a value is a
/// PAYLOAD: `/authn-req/` carries `token=<compact JWS>`. A ServiceAccount JWT
/// with a real issuer and subject runs past 330 bytes and a claim-laden one
/// past 800, and a value cut short truncates the credential mid-signature —
/// which presents as a verification failure, not as a buffer problem.
const MAX_VALUE: usize = 1024;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start service pass have run.
    subscribed: u8,
    authns: u32,
    /// The keyset bearer credentials are checked against, loaded from
    /// `/authn-keys/` as `MSG_KEY_ADD` / `MSG_KEYSET_SNAPSHOT` frames.
    ///
    /// Empty until an operator supplies one, and an empty keyset verifies
    /// nothing — every bearer credential is anonymous until this module has
    /// been told whose signatures to trust. That is the fail-closed
    /// direction: the alternative is trusting whatever arrives.
    keyset: Keyset,
    /// Bearer credentials refused, by reason — for an operator-facing line.
    tok_no_key: u32,
    tok_bad: u32,
}

// ---- helpers ----

/// Look up a binding under `prefix` + `cred`, returning its `id=` value length.
unsafe fn lookup_id(
    sys: &SyscallTable,
    prefix: &[u8],
    cred: &[u8],
    dst: &mut [u8],
) -> Option<usize> {
    let mut key = [0u8; MAX_KEY];
    let klen = prefix.len() + cred.len();
    if klen > key.len() {
        return None;
    }
    key[..prefix.len()].copy_from_slice(prefix);
    key[prefix.len()..klen].copy_from_slice(cred);
    let mut vbuf = [0u8; MAX_VALUE];
    let vlen = get_value(sys, &key[..klen], &mut vbuf)?;
    let id = field(&vbuf[..vlen], b"id=")?;
    Some(append(dst, 0, id))
}

/// Load the keyset from `/authn-keys/`.
///
/// **Kagi's wire bytes, on nanocloud's seam.** Each record holds one
/// `MSG_KEY_ADD` / `MSG_KEYSET_SNAPSHOT` frame exactly as kagi encodes it,
/// and `Keyset::apply` — kagi's own — decodes it. The transport is the
/// store because that is the seam every nanocloud module already speaks;
/// inventing a second encoding to avoid a channel would only re-introduce
/// the divergence between minting and verification that this seam removes.
///
/// An operator-written prefix, like `/peer-ids/`. It is not reachable from a
/// request: nothing in `/authn-req/` names a key.
///
/// # Safety
/// Caller holds an exclusive `&mut State` with a live syscall table.
unsafe fn load_keyset(s: &mut State, sys: &SyscallTable) {
    let mut walk = ListWalk::new(KEYS_PREFIX);
    while let Some(key) = walk.next(sys) {
        let mut frame = [0u8; 1024];
        let Some(n) = get_value(sys, key, &mut frame) else {
            continue;
        };
        // `[msg_type:u8][len:u16 LE][payload]` — kagi's envelope.
        if n < 3 {
            continue;
        }
        let plen = u16::from_le_bytes([frame[1], frame[2]]) as usize;
        if 3 + plen > n {
            continue;
        }
        let _ = s.keyset.apply(frame[0], &frame[3..3 + plen]);
    }
}

/// Verify a bearer credential and return its subject, or `None`.
///
/// **Verified, not looked up.** The credential is a compact JWS; this checks
/// the signature against the key its own header NAMES, and refuses a `kid`
/// the keyset does not hold rather than falling back to whatever key is
/// loaded. A `kid` nothing indexes by is worse than none — it reads like a
/// binding.
///
/// The identity is the `sub` claim, taken only AFTER the signature verifies.
/// Reading a claim out of an unverified credential is reading attacker input.
///
/// # Safety
/// Caller holds an exclusive `&mut State` with a live syscall table.
unsafe fn verify_bearer(
    s: &mut State,
    sys: &SyscallTable,
    token: &[u8],
    dst: &mut [u8],
) -> Option<usize> {
    if s.keyset.is_empty() {
        // No keyset means nothing to trust. Anonymous, and counted — an
        // operator who has not delivered one should be able to see that this
        // is why every bearer credential is being refused.
        s.tok_no_key = s.tok_no_key.wrapping_add(1);
        return None;
    }
    let jws = jose::Jws::split(token)?;

    // Which key signed this is the credential's own claim, in its header.
    let mut header_json = [0u8; 512];
    let hdr_len = b64::decode(jws.header_b64, &mut header_json).unwrap_or(0);
    let kid = jose::claim_str(&header_json[..hdr_len], b"kid").unwrap_or(b"");

    // A window check against a clock that reads 0 concludes "not yet expired"
    // for every credential ever issued, so no clock is a refusal, not a zero.
    let now = dev_unix_millis(sys) / 1000;
    if now == 0 {
        s.tok_bad = s.tok_bad.wrapping_add(1);
        return None;
    }
    let key = s.keyset.select(kid, now)?;

    // The header's `alg` must agree with the suite the key was delivered
    // under, or a credential names an algorithm the key was never meant for
    // — the algorithm-confusion class.
    let hdr_alg = jose::claim_str(&header_json[..hdr_len], b"alg").unwrap_or(b"");
    if auth_wire::suite::from_jose_alg(hdr_alg) != key.suite {
        s.tok_bad = s.tok_bad.wrapping_add(1);
        return None;
    }

    let mut sig = [0u8; 64];
    if b64::decode(jws.signature_b64, &mut sig) != Some(64) {
        s.tok_bad = s.tok_bad.wrapping_add(1);
        return None;
    }
    let pubkey = &key.pubkey[..usize::from(key.pubkey_len)];
    let ok = match key.suite {
        auth_wire::suite::ES256 => {
            let digest = sha256(jws.signing_input);
            ecdsa_verify(pubkey, &digest, &sig)
        }
        auth_wire::suite::ED25519 => match pubkey.try_into() {
            Ok(pk32) => ed25519_verify(pk32, jws.signing_input, &sig),
            Err(_) => false,
        },
        _ => false,
    };
    if !ok {
        s.tok_bad = s.tok_bad.wrapping_add(1);
        return None;
    }

    // Signature is good; now the credential must be live and must name a
    // subject. Claims are read only at this point.
    let mut payload_json = [0u8; 1024];
    let plen = b64::decode(jws.payload_b64, &mut payload_json)?;
    let json = &payload_json[..plen];
    let iat = jose::claim_u64(json, b"iat").unwrap_or(0);
    let exp = jose::claim_u64(json, b"exp").unwrap_or(0);
    if !jose::within_window(now, iat, exp, 60) {
        s.tok_bad = s.tok_bad.wrapping_add(1);
        return None;
    }
    let sub = jose::claim_str(json, b"sub")?;
    if sub.is_empty() {
        s.tok_bad = s.tok_bad.wrapping_add(1);
        return None;
    }
    Some(append(dst, 0, sub))
}

/// Resolve one credential to a response doc `<status>;<body>`.
unsafe fn authenticate(s: &mut State, sys: &SyscallTable, req: &[u8], doc: &mut [u8]) -> usize {
    // mTLS peer identity (from fluxor tls) via /peer-ids/, else bearer token.
    let mut idbuf = [0u8; MAX_VALUE];
    let id = if let Some(peer) = field(req, b"peer=") {
        if peer.is_empty() {
            None
        } else {
            // /peer-ids/<peer> remaps a verified peer to a k8s identity when a
            // binding exists; absent one, the verified peer subject IS the
            // identity — the same semantics as the certificate's
            // DistinguishedName(subject). So mTLS clients authenticate out of the
            // box and /peer-ids/ is an optional remap layer, not a requirement.
            // (Token creds get no such fallback: a bearer token is not a
            // self-describing identity, so an unknown token stays anonymous.)
            match lookup_id(sys, PEER_PREFIX, peer, &mut idbuf) {
                Some(n) => Some(n),
                None => Some(append(&mut idbuf, 0, peer)),
            }
        }
    } else if let Some(token) = field(req, b"token=") {
        verify_bearer(s, sys, token, &mut idbuf)
    } else {
        None
    };
    match id {
        Some(n) => {
            let d = append(doc, 0, b"200;");
            append(doc, d, &idbuf[..n])
        }
        None => append(doc, 0, b"401;anonymous"),
    }
}

/// Service every /authn-req/ without a response yet.
unsafe fn reconcile(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut served = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        if klen <= REQ_PREFIX.len() {
            continue;
        }
        let tail = &key[REQ_PREFIX.len()..];
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);

        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue; // already answered
        }

        let mut req = [0u8; MAX_VALUE];
        let Some(rqlen) = get_value(sys, key, &mut req) else {
            continue;
        };

        let mut doc = [0u8; MAX_VALUE];
        let dlen = authenticate(s, sys, &req[..rqlen], &mut doc);
        if put_value(sys, &rkey[..rlen], &doc[..dlen]) {
            served += 1;
        }
    }
    served
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<State>() {
            return -2;
        }
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.out_chan = out_chan;
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.keyset = Keyset::new();
        s.tok_no_key = 0;
        s.tok_bad = 0;
        s.authns = 0;
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return 0;
        }
        let s = &mut *(state as *mut State);
        if s.syscalls.is_null() {
            return 0;
        }
        let sys = &*s.syscalls;

        // Cold start: resolve the change-sink channel (self-edge allocated),
        // SUBSCRIBE the request prefix onto it, then service pre-existing requests.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            load_keyset(s, sys);
            s.authns = s.authns.wrapping_add(reconcile(s, sys));
            return 0;
        }

        // Keys first: a request serviced before the keyset landed would be
        // refused for want of a key that was about to arrive.
        load_keyset(s, sys);

        // A pushed namespace.change means the request set moved — re-service.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.authns = s.authns.wrapping_add(reconcile(s, sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
