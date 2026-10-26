//! Wormhole's VIP bindings → a flat row per Service the status chain reads.
//!
//! See `manifest.toml`. The VM cannot decode a binary record, so this does
//! that and nothing else: it decodes the `VipBinding` Wormhole keeps for a
//! Service's address (Wormhole's own decoder, strictly) and states its phase
//! and address; whether the Service's load-balancer status may name it is
//! the chain's to decide (wormhole.plan `wh_status`).

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
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/store.rs");
include!("../_shared/wormhole_traffic.rs");
include!("../_shared/whemit.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;

/// wormhole_emit's index of the Services Wormhole is told of.
const STATUS: &[u8] = b"/wh-status/";
/// This module's rows.
const VIPS: &[u8] = b"/wh-vip/";
const POLL_MS: u64 = 2000;
name_table!(SUBS = [b"/wh/", b"/wh-status/"]);

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    event: i32,
    subscribed: u32,
    dirty: u32,
    poll: u8,
    retry: u8,
    poll_ms: u64,
    nag: ReadNag,
    decoded: u32,
    refused: u32,
}

enum Pass {
    Idle,
    Again,
    Failed,
}

fn cat(out: &mut [u8], parts: &[&[u8]]) -> usize {
    let mut p = 0;
    for part in parts {
        p = append(out, p, part);
    }
    p
}

/// `<ns>/<svc>` split.
fn split_tail(tail: &[u8]) -> Option<(&[u8], &[u8])> {
    let i = tail.iter().position(|&c| c == b'/')?;
    let (ns, name) = (&tail[..i], &tail[i + 1..]);
    (!ns.is_empty() && !name.is_empty() && !wh_has(name, b'/')).then_some((ns, name))
}

/// One family's part of a row: `<pk><phase><ak><addr>` (`<ek>` when its
/// binding does not decode). Sets `any` when there is a binding at all.
#[allow(
    clippy::too_many_arguments,
    reason = "the literals are passed at the call so a PIC module holds no table of pointers"
)]
unsafe fn family(
    s: &mut State,
    store: &Store,
    ids: (&[u8; 16], &[u8; 16]),
    fam: u8,
    lits: (&[u8], &[u8], &[u8]),
    out: &mut [u8],
    at: usize,
    any: &mut bool,
) -> Option<usize> {
    let (tenant, service) = ids;
    let (pk, ak, ek) = lits;
    let mut k = [0u8; MAX_KEY];
    let kl = wh_vip_key(tenant, service, fam, &mut k)?;
    let mut v = [0u8; 1024];
    let got = get_value(store, &k[..kl], &mut v);
    let mut p = append(out, at, pk);
    match got.map(|n| wh_vip(&v[..n], service, fam)) {
        Some(Some((phase, addr))) => {
            *any = true;
            s.decoded = s.decoded.wrapping_add(1);
            p = wh_dec(out, p, phase as u64)?;
            p = append(out, p, ak);
            p = wh_fmt_addr(&addr, out, p)?;
        }
        Some(None) => {
            *any = true;
            s.refused = s.refused.wrapping_add(1);
            p = append(out, p, b"0");
            p = append(out, p, ak);
            p = append(out, p, ek);
        }
        None => {
            p = append(out, p, b"0");
            p = append(out, p, ak);
        }
    }
    Some(p)
}

/// `p4=<phase>;a4=<addr>;p6=<phase>;a6=<addr>` for the Service `ns/name`
/// into `out`: a family with no binding (or one that does not decode, said
/// `e<4|6>=invalid`) reads phase 0 and no address. None when it has no
/// binding in either family.
unsafe fn row(
    s: &mut State,
    store: &Store,
    ns: &[u8],
    name: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let tenant = wh_tenant_id(ns);
    let service = wh_service_id(ns, name);
    let mut any = false;
    let p = family(
        s,
        store,
        (&tenant, &service),
        traffic::AF_INET,
        (b"p4=", b";a4=", b";e4=invalid"),
        out,
        0,
        &mut any,
    )?;
    let p = family(
        s,
        store,
        (&tenant, &service),
        traffic::AF_INET6,
        (b";p6=", b";a6=", b";e6=invalid"),
        out,
        p,
        &mut any,
    )?;
    any.then_some(p)
}

/// One pass: every Service Wormhole is told of, its row put-if-changed (or
/// deleted when it has no binding); then every row whose Service is no
/// longer told of, deleted.
unsafe fn pass(s: &mut State) -> Pass {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut walk = ListWalk::new(STATUS);
    while let Some(key) = walk.next(&store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let tail = &kb[STATUS.len().min(kl)..kl];
        let Some((ns, name)) = split_tail(tail) else {
            continue;
        };
        let mut want = [0u8; 256];
        let wl = row(s, &store, ns, name, &mut want);
        if store.stopped() {
            return if store.waiting() {
                Pass::Again
            } else {
                Pass::Failed
            };
        }
        let mut rk = [0u8; MAX_KEY];
        let rl = cat(&mut rk, &[VIPS, tail]);
        let mut cur = [0u8; 256];
        let have = get_value(&store, &rk[..rl], &mut cur);
        if store.stopped() {
            return if store.waiting() {
                Pass::Again
            } else {
                Pass::Failed
            };
        }
        let a = match (wl, have) {
            (Some(w), Some(h)) if want[..w] == cur[..h] => continue,
            (Some(w), _) => put_value(&store, &rk[..rl], &want[..w]),
            (None, Some(_)) => delete_value(&store, &rk[..rl]),
            (None, None) => continue,
        };
        // One write a pass: the next pass continues from the store.
        return match a {
            WriteAnswer::Decided(0) | WriteAnswer::Pending => Pass::Again,
            WriteAnswer::Decided(_) => Pass::Failed,
        };
    }
    if walk.failed {
        return Pass::Failed;
    }
    let mut walk = ListWalk::new(VIPS);
    while let Some(key) = walk.next(&store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let mut sk = [0u8; MAX_KEY];
        let sl = cat(&mut sk, &[STATUS, &kb[VIPS.len().min(kl)..kl]]);
        if exists(&store, &sk[..sl]) || store.stopped() {
            continue;
        }
        return match delete_value(&store, &kb[..kl]) {
            WriteAnswer::Decided(0) | WriteAnswer::Pending => Pass::Again,
            WriteAnswer::Decided(_) => Pass::Failed,
        };
    }
    if walk.failed || store.stopped() {
        return Pass::Failed;
    }
    Pass::Idle
}

// ---- module ABI ----

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

/// # Safety
/// Kernel module-ABI entry point: `state`/`syscalls` are the loader-owned
/// instance arena and syscall table; `params` is the config TLV blob (may be
/// null when `params_len == 0`). All are valid for the lifetime the loader
/// guarantees; never called concurrently.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    _in_chan: i32,
    _out_chan: i32,
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
        s.event = -1;
        s.subscribed = 0;
        s.dirty = 1;
        s.poll = 0;
        s.retry = 0;
        s.poll_ms = 0;
        s.nag = ReadNag::new();
        s.decoded = 0;
        s.refused = 0;
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
        if s.event < 0 && s.subscribed == 0 {
            s.event = store_event(sys);
            if s.event < 0 {
                let m = b"[wormhole_status] ERROR no kernel event - changes will NOT wake this module, it looks again every POLL_MS";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.poll = 1;
                s.subscribed = 2;
            }
        }
        // Wormhole's records and the index of what it is told: its inputs,
        // never its output (/wh-vip/).
        while s.subscribed < 2 {
            let rc = store_subscribe(sys, SUBS.get(s.subscribed as usize), s.event, 0);
            if not_yet(rc) {
                return 0;
            }
            if rc < 0 {
                s.poll = 1;
            }
            s.subscribed += 1;
        }
        if s.poll != 0 || s.retry != 0 {
            let now = dev_millis(sys);
            if now.wrapping_sub(s.poll_ms) >= POLL_MS {
                s.poll_ms = now;
                s.retry = 0;
                s.dirty = 1;
            }
        }
        if store_take(sys, s.event, |_| {}) > 0 {
            s.dirty = 1;
        }
        if s.dirty == 0 {
            return 0;
        }
        match pass(s) {
            Pass::Idle => s.dirty = 0,
            Pass::Again => {}
            Pass::Failed => {
                let m = b"[wormhole_status] ERROR a pass FAILED (a read refused, a listing failed, or a write refused) - run again in POLL_MS";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.dirty = 0;
                s.retry = 1;
                s.poll_ms = dev_millis(sys);
            }
        }
        0
    }
}
