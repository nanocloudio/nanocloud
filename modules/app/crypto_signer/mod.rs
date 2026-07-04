//! crypto_signer — real cryptography in an app module on existing fluxor
//! capabilities — the foundation `cert_manager` builds on, and the reason it
//! needs no capability surface of its own. On start it generates a P-256
//! keypair with the SDK (`p256::ecdh_keygen`) seeded from the kernel CSPRNG,
//! deposits the private key in the kernel KEY_VAULT (contract 0x0010 — an infra
//! contract, implicitly granted, so no `requires_contract`), and wipes the
//! in-module copy. Then it signs each request BY HANDLE via KEY_VAULT `SIGN` —
//! the private key never re-enters the module — and self-verifies with
//! `p256::ecdsa_verify`. This is the exact mechanism `modules/foundation/tls`
//! uses for its identity key, reused from an app module.
//!
//! Data model (request/response over the store):
//!
//!   /signer-pubkey       = "<uncompressed-P256-point, 130 hex>"  (written on start)
//!   /sign-req/<id>       = "<message text>"
//!   /sign-resp/<id>      = "sig=<64-byte-ECDSA, 128 hex>;valid=<0|1>"
//!
//! `valid=1` means the signature verified against the (kernel-held) key's public
//! point — the whole keygen → vault-store → vault-sign → verify chain, in an app
//! module, with existing capabilities only. cert_manager layers an in-module
//! X.509 DER encoder on top; the crypto foundation is proven here.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/store.rs");
// p256 (ECDSA/ECDH) depends on sha256/sha384/hmac (RFC-6979 nonce); mirror the
// tls module's crypto include set.
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");

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

// KEY_VAULT contract (0x0010) — an infra contract, class-byte routed on op>>8.
const KV_STORE: u32 = 0x1001;
const KV_SIGN: u32 = 0x1003;
/// `key_vault::suite::P256` — the suite id the key is created under. A `u16`
/// because a suite is more than a key type: ML-DSA and ML-KEM are three
/// parameter sets each.
const KV_SUITE_P256: u16 = 1;
/// `usage::SIGN` — sealed at creation and checked per operation. An empty
/// mask permits nothing, which is what an uninitialised slot has.
const KV_USAGE_SIGN: u32 = 1;
/// `sign_mode::DIGEST` — P-256 signs the digest. Explicit on the wire, so a
/// caller cannot hand a P-256 slot a whole message and get back a valid
/// signature over the wrong thing.
const KV_SIGN_MODE_DIGEST: u8 = 1;
const RANDOM_FILL: u32 = 0x0C3C;

const REQ_PREFIX: &[u8] = b"/sign-req/";
const RESP_PREFIX: &[u8] = b"/sign-resp/";
const PUBKEY_KEY: &[u8] = b"/signer-pubkey";

const MAX_KEY: usize = 96;
const MAX_VALUE: usize = 512;
const LIST_BUF: usize = 2048;
// ── Development gate ───────────────────────────────────────────────────────
//
// This module REFUSES TO CONSTRUCT unless the graph sets `development: 1`.
//
// Unlike `cert_manager` there is nothing WRONG with what it does — it signs
// by handle through KEY_VAULT and the private key never re-enters the
// module, which is the mechanism `modules/foundation/tls` uses. The point is
// what it is FOR: it is a KEY_VAULT conformance fixture that exists to prove
// an app module can do real cryptography on existing capabilities.
//
// A fixture in a production graph is a signing oracle. It signs any message
// written to `/sign-req/<id>` and publishes the signature, so anything that
// can write the store can have arbitrary bytes signed by a key the vault
// holds. That is not a defect in the fixture; it is what a fixture is, and
// it is why it must not be reachable outside the tests that use it.

mod params_def {
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, development, u8, 0
            => |s, d, len| { if len >= 1 { s.development = *d; } };
    }
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    /// 1 when the graph declared `development: 1`. See the development gate.
    development: u8,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE has run.
    subscribed: u8,
    ca_handle: i32,
    pubkey: [u8; 65],
    ready: u8,
    signed: u32,
}

// ---- KEY_VAULT ops (in-place arg convention, NOT the caller-output tail) ----

/// STORE a 32-byte P-256 scalar; returns the vault slot handle (>=0) or a
/// negative errno.
///
/// KEY_VAULT layout: `[suite:u16][usage_mask:u32][key_len:u32][key]`. The
/// shape is exact — a call built to a different one is refused by the vault,
/// and the module then publishes no public key at all.
unsafe fn kv_store(sys: &SyscallTable, scalar: &[u8; 32]) -> i32 {
    let mut arg = [0u8; 2 + 4 + 4 + 32];
    arg[0..2].copy_from_slice(&KV_SUITE_P256.to_le_bytes());
    arg[2..6].copy_from_slice(&KV_USAGE_SIGN.to_le_bytes());
    arg[6..10].copy_from_slice(&32u32.to_le_bytes());
    arg[10..42].copy_from_slice(scalar);
    (sys.provider_call)(-1, KV_STORE, arg.as_mut_ptr(), arg.len())
}

/// SIGN a 32-byte hash by vault handle; returns the raw 64-byte ECDSA sig.
///
/// KEY_VAULT layout:
/// `[sign_mode:u8][_pad:u8][input_len:u32][input][sig_out_ptr:u64]
///  [sig_out_cap:u16][sig_len_out:u16]`.
///
/// The signature is variable-length and returned through a caller pointer,
/// because 64 bytes is right for ES256 and wrong for everything past it —
/// ML-DSA-65's is 3309 bytes. This fixture only ever holds P-256, so 64 is
/// the true capacity here and a short buffer would be a bug rather than a
/// suite it must grow for.
unsafe fn kv_sign(sys: &SyscallTable, handle: i32, hash: &[u8; 32]) -> Option<[u8; 64]> {
    let mut sig = [0u8; 64];
    let mut arg = [0u8; 1 + 1 + 4 + 32 + 8 + 2 + 2];
    arg[0] = KV_SIGN_MODE_DIGEST;
    // arg[1] = pad
    arg[2..6].copy_from_slice(&32u32.to_le_bytes());
    arg[6..38].copy_from_slice(hash);
    arg[38..46].copy_from_slice(&(sig.as_mut_ptr() as u64).to_le_bytes());
    arg[46..48].copy_from_slice(&64u16.to_le_bytes());
    // arg[48..50] = sig_len_out, written by the vault
    let rc = (sys.provider_call)(handle, KV_SIGN, arg.as_mut_ptr(), arg.len());
    if rc != 0 {
        return None;
    }
    let written = u16::from_le_bytes([arg[48], arg[49]]);
    if written != 64 {
        // A P-256 slot that produced anything but 64 bytes is not a P-256
        // slot. Refused rather than truncated.
        return None;
    }
    Some(sig)
}

// ---- helpers ----

fn hex_byte(b: u8) -> [u8; 2] {
    const H: &[u8; 16] = b"0123456789abcdef";
    [H[(b >> 4) as usize], H[(b & 0x0f) as usize]]
}

fn hex_encode(src: &[u8], dst: &mut [u8]) -> usize {
    let mut p = 0;
    for &b in src {
        if p + 2 > dst.len() {
            break;
        }
        let h = hex_byte(b);
        dst[p] = h[0];
        dst[p + 1] = h[1];
        p += 2;
    }
    p
}

/// Sign every /sign-req/ that has no response yet.
unsafe fn reconcile(sys: &SyscallTable, handle: i32, pubkey: &[u8; 65]) -> u32 {
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

        // /sign-resp/<tail> — answered guard.
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);
        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        // The message to sign.
        let mut msg = [0u8; MAX_VALUE];
        let Some(mlen) = get_value(sys, key, &mut msg) else {
            continue;
        };

        // sha256(msg) → sign via the vault → self-verify.
        let hash = sha256(&msg[..mlen]);
        let Some(sig) = kv_sign(sys, handle, &hash) else {
            continue;
        };
        let valid = ecdsa_verify(pubkey, &hash, &sig);

        // Response: "sig=<128hex>;valid=<0|1>".
        let mut doc = [0u8; 160];
        let mut d = 0;
        doc[..4].copy_from_slice(b"sig=");
        d += 4;
        d += hex_encode(&sig, &mut doc[d..]);
        doc[d..d + 7].copy_from_slice(b";valid=");
        d += 7;
        doc[d] = if valid { b'1' } else { b'0' };
        d += 1;

        if put_value(sys, &rkey[..rlen], &doc[..d]) {
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

/// Kernel module-ABI entry point.
///
/// # Safety
///
/// `state`/`syscalls` are the loader-owned instance arena and syscall table,
/// and `params` is the config TLV blob (may be null when `params_len == 0`).
/// All are valid for the lifetime the loader guarantees and are never called
/// concurrently. `unsafe` because it READS `params` — the development gate
/// below is driven from it.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
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
        // The development gate — see the block above `struct State`. Read the
        // params BEFORE anything else touches the world: this module's next
        // act is to generate a signing keypair and publish its public point,
        // and a refusal after that would already have done what it refuses.
        s.development = 0;
        params_def::parse_tlv(s, params, params_len);
        if s.development != 1 {
            let m: &[u8] = b"[crypto_signer] REFUSING TO START: KEY_VAULT conformance fixture, not a service. It signs anything written to /sign-req/. Set `development: 1` in the graph.";
            dev_log(&*s.syscalls, 1, m.as_ptr(), m.len());
            return -3;
        }
        s.out_chan = out_chan;
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.ca_handle = -1;
        s.pubkey = [0u8; 65];
        s.ready = 0;
        s.signed = 0;
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

        // One-time key setup: CSPRNG → keypair → vault → wipe private → publish pub.
        if s.ready == 0 {
            let mut random = [0u8; 32];
            let rc = dev_csprng_fill(sys, random.as_mut_ptr(), 32);
            if rc < 0 {
                return 0; // CSPRNG not ready — retry next step
            }
            let (mut priv_key, pub_key) = ecdh_keygen(&random);
            let handle = kv_store(sys, &priv_key);
            // Wipe the in-module private material immediately.
            for b in priv_key.iter_mut() {
                core::ptr::write_volatile(b, 0);
            }
            for b in random.iter_mut() {
                core::ptr::write_volatile(b, 0);
            }
            if handle < 0 {
                return 0; // vault unavailable — retry
            }
            s.ca_handle = handle;
            s.pubkey = pub_key;

            // Publish the public key for external verifiers.
            let mut hex = [0u8; 130];
            let n = hex_encode(&pub_key, &mut hex);
            put_value(sys, PUBKEY_KEY, &hex[..n]);

            // Resolve the change-sink channel (self-edge allocated) and SUBSCRIBE
            // /sign-req/ onto it, then service any pre-existing requests.
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.ready = 1;
            s.signed = s
                .signed
                .wrapping_add(reconcile(sys, s.ca_handle, &s.pubkey));
            return 0;
        }

        // A pushed namespace.change means new /sign-req/ to sign.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.signed = s
                .signed
                .wrapping_add(reconcile(sys, s.ca_handle, &s.pubkey));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
