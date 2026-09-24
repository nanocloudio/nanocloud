//! sa_token — nanocloud's ServiceAccount token issuer as a PIC module — the k8s
//! `serviceaccounts/token` surface. It mirrors cert_manager's shape: on start
//! it self-generates a P-256 signing keypair, deposits the private scalar in
//! the kernel KEY_VAULT (contract 0x0010) by handle, wipes the in-module copy,
//! and publishes the public key. Each request MINTS a signed JWS (ES256 = ECDSA
//! P-256 + SHA-256 — exactly what KV_SIGN does) and returns it. The token IS a
//! real, verifiable JWT bearer token for `system:service account:<ns>:<name>`.
//!
//! Data model (request/response over the store):
//!
//!   /sa-pubkey          = "<signing public key, 65-byte EC point, hex>"   (on start)
//!   /token-req/<id>     = "ns=<namespace>;sa=<serviceaccount>[;aud=<audience>]"
//!   /token-resp/<id>    = "token=<JWT>"
//!
//! Every token carries a real `iat`/`exp` window (one hour), read from the
//! module's wall clock (`dev_unix_millis`): a bearer token that never expires
//! is one a single disclosure compromises permanently, and there is no
//! revocation here to contain it. A clock that reads zero is a REFUSAL to
//! mint, never a claim of the epoch.
//!
//! **The verification key is published as a kagi `MSG_KEY_ADD` frame** at
//! `/authn-keys/sa`, which is where `kagi_verify` reads it and pushes it to
//! kagi's verifier. Minting and verification therefore share one key and one
//! wire format: what this module signs is exactly what checks it.
//!
//! **The signing key is opened by LABEL** (`OPEN_OR_GENERATE`), not drawn
//! fresh on every start. That removes the race two instances would otherwise
//! have — both seeing absence, both generating, one signing under a key
//! nothing else trusts — and it survives a scheduler reset.
//!
//! **The issuer key does not yet survive a process restart**: fluxor's
//! software vault keeps sealed blobs in a `static mut`, so they go with the
//! process, and two runs publish different public keys. A durable home for
//! sealed blobs is fluxor's half; until it exists, a cold start re-issues.

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const KV_SIGN: u32 = 0x1003;
/// `key_vault::suite::P256` — the suite id the key is created under.
const KV_SUITE_P256: u16 = 1;
/// `usage::SIGN | usage::PERSIST` — what the LABELLED signing key is opened
/// with.
///
/// `PERSIST` has to be asked for. It is sealed into the key at creation and
/// checked, not assumed, so a key born without it cannot acquire it by a
/// later caller reopening with a wider mask. That is the point of the mask:
/// permitted uses are decided once, at birth.
///
/// And per the contract, persistence says nothing about ISOLATION — that is
/// `TIER`'s business. A key that survives a restart on a host with no
/// device-unique sealing is still `tier::SOFTWARE`, which is exactly what
/// kagi's `key_custody` refuses production issuance on.
const KV_USAGE_SIGN_PERSIST: u32 = (1 << 0) | (1 << 4);
/// `sign_mode::DIGEST` — ES256 signs the digest, stated on the wire rather
/// than inferred from the key type.
const KV_SIGN_MODE_DIGEST: u8 = 1;

const REQ_PREFIX: &[u8] = b"/token-req/";
const RESP_PREFIX: &[u8] = b"/token-resp/";
const PUBKEY_KEY: &[u8] = b"/sa-pubkey";
const ISSUER: &[u8] = b"https://kubernetes.default.svc";
const DEFAULT_AUD: &[u8] = b"https://kubernetes.default.svc";
// b64url of `{"alg":"ES256","typ":"JWT"}` — the fixed JWS header.
/// `{"alg":"ES256","typ":"JWT","kid":"sa-1"}`, base64url.
///
/// The `kid` is new and load-bearing: a verifier selects the key a credential
/// NAMES. Without one it must either refuse or fall back to whatever key is
/// loaded, and the fallback is what makes a `kid` decorative — a credential
/// signed by a key nobody trusted then verifies whenever the header is
/// ignored.
const JWT_HEADER_B64: &[u8] = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ";
/// The `kid` inside that header, as the verifier looks it up.
const SA_KID: &[u8] = b"sa-1";
/// The vault label the signing key lives under. A LABEL, not a fresh key per
/// start — see `kv_open_or_generate`.
const SA_KEY_LABEL: &[u8] = b"nanocloud-sa-signing-v1";
/// How long a minted ServiceAccount token is good for.
///
/// One hour. A bearer token that never expires is one that a single
/// disclosure compromises permanently, and there is no revocation here to
/// contain it.
const TOKEN_TTL_SECS: u64 = 3600;
/// Where the verification key is published for `kagi_verify` to load.
const AUTHN_KEYS_KEY: &[u8] = b"/authn-keys/sa";

const MAX_KEY: usize = 96;
const MAX_VALUE: usize = 512;
const JWT_CAP: usize = 1024;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    sink: i32,
    subscribed: u8,
    ready: u8,
    key_handle: i32,
    minted: u32,
}

// ---- storage.object / storage.namespace ----

// ---- KEY_VAULT ----

unsafe fn kv_sign(sys: &SyscallTable, handle: i32, hash: &[u8; 32]) -> Option<[u8; 64]> {
    // KEY_VAULT: `[sign_mode:u8][_pad:u8][input_len:u32][input]
    // [sig_out_ptr:u64][sig_out_cap:u16][sig_len_out:u16]`.
    let mut sig = [0u8; 64];
    let mut arg = [0u8; 1 + 1 + 4 + 32 + 8 + 2 + 2];
    arg[0] = KV_SIGN_MODE_DIGEST;
    arg[2..6].copy_from_slice(&32u32.to_le_bytes());
    arg[6..38].copy_from_slice(hash);
    arg[38..46].copy_from_slice(&(sig.as_mut_ptr() as u64).to_le_bytes());
    arg[46..48].copy_from_slice(&64u16.to_le_bytes());
    let rc = (sys.provider_call)(handle, KV_SIGN, arg.as_mut_ptr(), arg.len());
    if rc != 0 {
        return None;
    }
    // An ES256 slot that produced anything but 64 bytes is not one.
    if u16::from_le_bytes([arg[48], arg[49]]) != 64 {
        return None;
    }
    Some(sig)
}

// ---- helpers ----

fn cp(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

fn hex_encode(src: &[u8], dst: &mut [u8]) -> usize {
    let hexd = b"0123456789abcdef";
    let mut o = 0;
    for &b in src {
        if o + 2 > dst.len() {
            break;
        }
        dst[o] = hexd[(b >> 4) as usize];
        dst[o + 1] = hexd[(b & 0xf) as usize];
        o += 2;
    }
    o
}

/// base64url (RFC 4648 §5, no padding) into `dst`; returns the byte length.
fn b64url(src: &[u8], dst: &mut [u8]) -> usize {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut o = 0;
    let mut i = 0;
    while i + 3 <= src.len() && o + 4 <= dst.len() {
        let n = ((src[i] as u32) << 16) | ((src[i + 1] as u32) << 8) | (src[i + 2] as u32);
        dst[o] = A[((n >> 18) & 63) as usize];
        dst[o + 1] = A[((n >> 12) & 63) as usize];
        dst[o + 2] = A[((n >> 6) & 63) as usize];
        dst[o + 3] = A[(n & 63) as usize];
        o += 4;
        i += 3;
    }
    let rem = src.len() - i;
    if rem == 1 && o + 2 <= dst.len() {
        let n = (src[i] as u32) << 16;
        dst[o] = A[((n >> 18) & 63) as usize];
        dst[o + 1] = A[((n >> 12) & 63) as usize];
        o += 2;
    } else if rem == 2 && o + 3 <= dst.len() {
        let n = ((src[i] as u32) << 16) | ((src[i + 1] as u32) << 8);
        dst[o] = A[((n >> 18) & 63) as usize];
        dst[o + 1] = A[((n >> 12) & 63) as usize];
        dst[o + 2] = A[((n >> 6) & 63) as usize];
        o += 3;
    }
    o
}

/// Mint a signed JWS for `system:serviceaccount:<ns>:<sa>` into `out`; returns
/// the token length (0 on failure). header.payload signed ES256 by KV handle.
/// Open the labelled ES256 signing key, generating one only if absent.
///
/// KEY_VAULT `OPEN_OR_GENERATE`:
/// `[suite:u16][usage_mask:u32][flags:u8][label_len:u8][label]
///  [pub_out_ptr:u64][pub_out_cap:u16][pub_len_out:u16]`.
///
/// The mask is SIGN only, and it is SEALED into the key at creation. On an
/// existing key it is checked rather than applied — reopening with a wider
/// mask than the key was born with is refused, so permitted uses are not
/// something a later caller widens by asking.
///
/// # Safety
/// Caller supplies a live syscall table; `pub_out` receives the SEC1 point.
unsafe fn kv_open_or_generate(sys: &SyscallTable, pub_out: &mut [u8; 65]) -> i32 {
    const KV_OPEN_OR_GENERATE: u32 = 0x1009;
    let label = SA_KEY_LABEL;
    let mut arg = [0u8; 2 + 4 + 1 + 1 + 32 + 8 + 2 + 2];
    let mut p = 0usize;
    arg[p..p + 2].copy_from_slice(&KV_SUITE_P256.to_le_bytes());
    p += 2;
    arg[p..p + 4].copy_from_slice(&KV_USAGE_SIGN_PERSIST.to_le_bytes());
    p += 4;
    arg[p] = 0; // flags: none
    p += 1;
    arg[p] = u8::try_from(label.len()).unwrap_or(0);
    p += 1;
    arg[p..p + label.len()].copy_from_slice(label);
    p += label.len();
    arg[p..p + 8].copy_from_slice(&(pub_out.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&65u16.to_le_bytes());
    p += 2;
    // arg[p..p+2] = pub_len_out, written by the vault
    p += 2;
    let rc = (sys.provider_call)(-1, KV_OPEN_OR_GENERATE, arg.as_mut_ptr(), p);
    if rc < 0 {
        return rc;
    }
    // A P-256 slot that reported anything but a 65-byte point is not one.
    let written = u16::from_le_bytes([arg[p - 2], arg[p - 1]]);
    if written != 65 {
        return -22;
    }
    rc
}

/// Append a decimal `u64`. Returns the new offset.
fn put_u64(dst: &mut [u8], at: usize, mut v: u64) -> usize {
    let mut tmp = [0u8; 20];
    let mut n = 0usize;
    if v == 0 {
        tmp[0] = b'0';
        n = 1;
    }
    while v > 0 {
        tmp[n] = b'0' + u8::try_from(v % 10).unwrap_or(0);
        v /= 10;
        n += 1;
    }
    let mut p = at;
    while n > 0 && p < dst.len() {
        n -= 1;
        dst[p] = tmp[n];
        p += 1;
    }
    p
}

/// Publish the verification key as a kagi `MSG_KEY_ADD` frame, where
/// `kagi_verify` loads it from.
///
/// **This is what makes the pair compose.** The verifier checks signatures, so
/// the one thing it needs from the issuer is the verification key — published
/// here, under the label it looks for.
///
/// kagi's wire format rather than a nanocloud one: kagi owns the key
/// lifecycle, and a second encoding for "here is a verification key" is how
/// the two ends drift.
///
/// # Safety
/// Caller supplies a live syscall table; `pubkey` is the 65-byte SEC1 point.
unsafe fn publish_verification_key(sys: &SyscallTable, pubkey: &[u8; 65]) -> bool {
    const MSG_KEY_ADD: u8 = 0x22;
    const PROFILE_ACCESS_TOKEN: u16 = 1;
    const SUITE_ES256: u16 = 1;
    const KEY_STATE_ACTIVE: u8 = 1;
    const KEY_USE_VERIFY: u8 = 0x01;

    let mut body = [0u8; 160];
    let mut p = 0usize;
    // field8(issuer)
    body[p] = u8::try_from(ISSUER.len()).unwrap_or(0);
    p += 1;
    body[p..p + ISSUER.len()].copy_from_slice(ISSUER);
    p += ISSUER.len();
    body[p..p + 2].copy_from_slice(&PROFILE_ACCESS_TOKEN.to_le_bytes());
    p += 2;
    // field8(kid)
    body[p] = u8::try_from(SA_KID.len()).unwrap_or(0);
    p += 1;
    body[p..p + SA_KID.len()].copy_from_slice(SA_KID);
    p += SA_KID.len();
    body[p..p + 2].copy_from_slice(&SUITE_ES256.to_le_bytes());
    p += 2;
    body[p] = KEY_STATE_ACTIVE;
    p += 1;
    body[p] = KEY_USE_VERIFY;
    p += 1;
    body[p..p + 4].copy_from_slice(&1u32.to_le_bytes()); // generation
    p += 4;
    body[p..p + 8].copy_from_slice(&0u64.to_le_bytes()); // activate_after: now
    p += 8;
    body[p..p + 8].copy_from_slice(&0u64.to_le_bytes()); // remove_after: none
    p += 8;
    // field16(material) — the PUBLIC half only. A verifier holds public keys.
    body[p..p + 2].copy_from_slice(&65u16.to_le_bytes());
    p += 2;
    body[p..p + 65].copy_from_slice(pubkey);
    p += 65;

    let mut frame = [0u8; 176];
    frame[0] = MSG_KEY_ADD;
    frame[1..3].copy_from_slice(&u16::try_from(p).unwrap_or(0).to_le_bytes());
    frame[3..3 + p].copy_from_slice(&body[..p]);
    put_value(sys, AUTHN_KEYS_KEY, &frame[..3 + p])
}

unsafe fn mint_token(
    sys: &SyscallTable,
    handle: i32,
    ns: &[u8],
    sa: &[u8],
    aud: &[u8],
    out: &mut [u8],
) -> usize {
    // header.payload
    let mut p = cp(out, 0, JWT_HEADER_B64);
    if p + 1 >= out.len() {
        return 0;
    }
    out[p] = b'.';
    p += 1;

    // A real validity window. `now == 0` is a REFUSAL, not a claim of the
    // epoch: a window checked against a clock that reads zero concludes "not
    // yet expired" for every credential ever issued.
    let now = dev_unix_millis(sys) / 1000;
    if now == 0 {
        return 0;
    }

    // Build the claims, then b64url them onto the signing input.
    let mut payload = [0u8; MAX_VALUE];
    let mut pl = cp(&mut payload, 0, b"{\"iss\":\"");
    pl = cp(&mut payload, pl, ISSUER);
    pl = cp(&mut payload, pl, b"\",\"sub\":\"system:serviceaccount:");
    pl = cp(&mut payload, pl, ns);
    pl = cp(&mut payload, pl, b":");
    pl = cp(&mut payload, pl, sa);
    pl = cp(&mut payload, pl, b"\",\"aud\":\"");
    pl = cp(&mut payload, pl, aud);
    pl = cp(&mut payload, pl, b"\",\"kubernetes.io\":{\"namespace\":\"");
    pl = cp(&mut payload, pl, ns);
    pl = cp(&mut payload, pl, b"\",\"serviceaccount\":{\"name\":\"");
    pl = cp(&mut payload, pl, sa);
    pl = cp(&mut payload, pl, b"\"}},\"iat\":");
    pl = put_u64(&mut payload, pl, now);
    pl = cp(&mut payload, pl, b",\"exp\":");
    pl = put_u64(&mut payload, pl, now + TOKEN_TTL_SECS);
    pl = cp(&mut payload, pl, b"}");
    p += b64url(&payload[..pl], &mut out[p..]);

    // Sign SHA-256(signing input) with the SA key, append b64url(signature).
    let hash = sha256(&out[..p]);
    let Some(sig) = kv_sign(sys, handle, &hash) else {
        return 0;
    };
    if p + 1 >= out.len() {
        return 0;
    }
    out[p] = b'.';
    p += 1;
    p += b64url(&sig, &mut out[p..]);
    p
}

/// Mint tokens for every `/token-req/` without a `/token-resp/`. Returns the
/// count minted.
unsafe fn reconcile(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut minted = 0u32;
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
        let id = &key[REQ_PREFIX.len()..];

        // Already answered?
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + id.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(id);
        if exists(sys, &rkey[..rlen]) {
            continue;
        }

        // Parse ns/sa/aud from the request.
        let mut req = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut req) else {
            continue;
        };
        let ns = field(&req[..vlen], b"ns=").unwrap_or(b"default");
        let Some(sa) = field(&req[..vlen], b"sa=") else {
            continue;
        };
        let aud = field(&req[..vlen], b"aud=").unwrap_or(DEFAULT_AUD);

        // Copy the request fields out (they borrow `req`) before minting.
        let mut nsb = [0u8; 64];
        let nsl = ns.len().min(nsb.len());
        nsb[..nsl].copy_from_slice(&ns[..nsl]);
        let mut sab = [0u8; 64];
        let sal = sa.len().min(sab.len());
        sab[..sal].copy_from_slice(&sa[..sal]);
        let mut audb = [0u8; 128];
        let audl = aud.len().min(audb.len());
        audb[..audl].copy_from_slice(&aud[..audl]);

        let mut jwt = [0u8; JWT_CAP];
        let jl = mint_token(
            sys,
            s.key_handle,
            &nsb[..nsl],
            &sab[..sal],
            &audb[..audl],
            &mut jwt,
        );
        if jl == 0 {
            continue;
        }
        let mut resp = [0u8; JWT_CAP + 8];
        let mut rp = cp(&mut resp, 0, b"token=");
        rp = cp(&mut resp, rp, &jwt[..jl]);
        if put_value(sys, &rkey[..rlen], &resp[..rp]) {
            minted += 1;
        }
    }
    minted
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
        s.sink = in_chan;
        s.subscribed = 0;
        s.ready = 0;
        s.key_handle = -1;
        s.minted = 0;
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

        // One-time setup: generate the SA signing key → vault → publish pubkey.
        if s.ready == 0 {
            // OPEN the labelled signing key, generating it only if absent.
            //
            // **This is what stops every restart being a new issuer.**
            // Drawing fresh randomness per start would retire every token
            // already minted the moment the module bounced — silently,
            // because a verifier with the previous key just sees a bad
            // signature.
            //
            // One operation rather than "does it exist?" then "create it",
            // because those two are a race: two instances starting together
            // would both see absence, both generate, and one would sign under
            // a key nothing else trusts.
            //
            // What it buys, and what it does not: the race is gone and the
            // key survives a scheduler reset, but NOT a process restart —
            // fluxor's software vault holds sealed blobs in a `static mut`
            // table, so they go with the process and two runs of this graph
            // publish different public keys. Closing that is fluxor's half
            // (sealed blobs want a durable home rather than kernel RAM); this
            // call is the right one either way, and it starts persisting the
            // day the backend does.
            let mut pubkey = [0u8; 65];
            let handle = kv_open_or_generate(sys, &mut pubkey);
            if handle < 0 {
                return 0;
            }
            s.key_handle = handle;
            let mut hex = [0u8; 130];
            let n = hex_encode(&pubkey, &mut hex);
            put_value(sys, PUBKEY_KEY, &hex[..n]);
            // And the same key as a kagi `MSG_KEY_ADD` frame, which is what
            // `kagi_verify` loads. `/sa-pubkey` stays as the readable hex an
            // operator can eyeball; this is the one a verifier consumes.
            publish_verification_key(sys, &pubkey);

            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.ready = 1;
            s.minted = s.minted.wrapping_add(reconcile(s, sys));
            return 0;
        }

        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.minted = s.minted.wrapping_add(reconcile(s, sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
