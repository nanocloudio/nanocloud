//! Webhook validator — nanocloud's webhook status controller as a PIC module,
//! a *validation* reconciler: watch /webhooks/, validate each Webhook's spec,
//! and write its status back — a different reconciler shape from
//! `endpoints_reconciler` (per-object validation, not cross-object
//! aggregation), over the ONE shared store.
//!
//! Data model (the compact projection format):
//!
//!   /webhooks/<ns>/<name>       = "path=<p>;has_secret=<0|1>;secret_name=<n>;
//!                                  secret_key=<k>;hmac=<0|1>;containers=<N>"
//!   /webhook-status/<ns>/<name> = "ready=1"  |  "ready=0;msg=<issues>"
//!
//! Validation:
//!   - path must be set and start with '/'
//!   - a job template must have at least one container
//!   - when a secretRef is set: secret_name and secret_key must be non-empty,
//!     and hmac must be set
//!
//! Loop shape (level-triggered): SUBSCRIBE /webhooks/ + a cold-start pass; on
//! any change, recompute every webhook's status and PUT only when it changed (a
//! GET-compare guards the write, so a quiet cluster spends no revisions).

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

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes + CAS, storage.namespace
// (0x13) prefix LIST + change SUBSCRIBE. Changes are pushed to us as
// namespace.change on the `changes` input channel (allocated by a graph
// self-edge); we drain them to detect movement, then re-reconcile.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const WEBHOOKS_PREFIX: &[u8] = b"/webhooks/";
const STATUS_PREFIX: &[u8] = b"/webhook-status/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 512;
const MAX_STATUS: usize = 256;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start pass have run.
    subscribed: u8,
    /// Count of statuses (re)written — the observable of progress.
    writes: u32,
}

// ---- compact-format parsing (byte scanning; the format is ours) ----

/// The `<ns>/<name>` tail after `prefix` in `key`.
fn tail_after<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    key.get(prefix.len()..)
}

/// Parse a leading run of ASCII digits as a u32 (no core::str — the PIC link
/// surface has no from_utf8). Stops at the first non-digit.
fn parse_u32(b: &[u8]) -> u32 {
    let mut n: u32 = 0;
    for &c in b {
        if c.is_ascii_digit() {
            n = n.wrapping_mul(10).wrapping_add((c - b'0') as u32);
        } else {
            break;
        }
    }
    n
}

/// Compute a webhook's status doc from its compact spec. Mirrors
/// WebhookSpec::validate + compute_webhook_status: returns the bytes of
/// `ready=1` or `ready=0;msg=<issues>`. `doc` must be at least MAX_STATUS.
fn compute_status(value: &[u8], doc: &mut [u8]) -> usize {
    // Accumulate issues into a message buffer, comma-separated.
    let mut msg = [0u8; MAX_STATUS];
    let mut mlen = 0usize;
    let mut add = |m: &[u8], mlen: &mut usize| {
        if *mlen > 0 {
            *mlen = append(&mut msg, *mlen, b", ");
        }
        *mlen = append(&mut msg, *mlen, m);
    };

    match field(value, b"path=") {
        None => add(b"path must be set", &mut mlen),
        Some(p) if p.first() != Some(&b'/') => add(b"path must start with '/'", &mut mlen),
        Some(_) => {}
    }

    // A job template must have at least one container.
    let containers = field(value, b"containers=").map(parse_u32).unwrap_or(0);
    if containers == 0 {
        add(b"job template must have at least one container", &mut mlen);
    }

    // secretRef well-formedness (only when one is configured).
    if field(value, b"has_secret=") == Some(b"1") {
        if field(value, b"secret_name=").is_none() {
            add(b"secretRef.name must not be empty", &mut mlen);
        }
        if field(value, b"secret_key=").is_none() {
            add(b"secretRef.key must not be empty", &mut mlen);
        }
        if field(value, b"hmac=") != Some(b"1") {
            add(b"hmac_header must be set when secretRef is set", &mut mlen);
        }
    }

    if mlen == 0 {
        append(doc, 0, b"ready=1")
    } else {
        let mut d = append(doc, 0, b"ready=0;msg=");
        d = append(doc, d, &msg[..mlen]);
        d
    }
}

/// Recompute every webhook's status; PUT only the ones that changed. Returns
/// the count of statuses (re)written.
unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(WEBHOOKS_PREFIX);
    let mut wrote = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        let mut valbuf = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut valbuf) else {
            continue;
        };

        let mut doc = [0u8; MAX_STATUS];
        let dlen = compute_status(&valbuf[..vlen], &mut doc);

        // /webhook-status/<ns>/<name>
        let Some(tail) = tail_after(key, WEBHOOKS_PREFIX) else {
            continue;
        };
        let mut skey = [0u8; MAX_KEY];
        let sklen = STATUS_PREFIX.len() + tail.len();
        if sklen > skey.len() {
            continue;
        }
        skey[..STATUS_PREFIX.len()].copy_from_slice(STATUS_PREFIX);
        skey[STATUS_PREFIX.len()..sklen].copy_from_slice(tail);

        // Only spend a revision when the status changed.
        let mut current = [0u8; MAX_STATUS];
        if let Some(clen) = get_value(sys, &skey[..sklen], &mut current) {
            if current[..clen] == doc[..dlen] {
                continue;
            }
        }
        if put_value(sys, &skey[..sklen], &doc[..dlen]) {
            wrote += 1;
        }
    }
    wrote
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
        s.writes = 0;
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

        // Cold start: resolve the change-sink channel (self-edge allocated), then
        // SUBSCRIBE the watched prefix onto it, then a cold-start pass (webhooks
        // may predate the watch).
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, WEBHOOKS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.writes = s.writes.wrapping_add(reconcile_all(sys));
        }
        // Level-triggered: recompute on any /webhooks/ change.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.writes = s.writes.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
