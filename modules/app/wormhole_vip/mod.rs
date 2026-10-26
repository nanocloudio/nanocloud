//! A Service's load-balancer address → the VIP move or release Wormhole's
//! failover coordinator runs for it.
//!
//! See `manifest.toml` and `_shared/whvip.rs`. One request a change, from a
//! ledger written before it is sent, so a restart re-sends the same bytes.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/wormhole_traffic.rs");
include!("../_shared/whemit.rs");
include!("../_shared/whvip.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;
const POLL_MS: u64 = 2000;
/// Operation ids sent in this life, so a pass that finds its ledger again
/// does not resend: a restart (an empty set) does, byte for byte.
const SENT: usize = 32;
name_table!(SUBS = [b"/lb-addr/"]);

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_out: i32,
    event: i32,
    subscribed: u32,
    dirty: u32,
    poll: u8,
    retry: u8,
    sent_at: u8,
    _pad: u8,
    poll_ms: u64,
    nag: ReadNag,
    requests: u32,
    refused: u32,
    /// The namespace (tenant) whose Services are asked for: the one the
    /// node controllers publish under (`tenant` = H("nc-tenant", ns)).
    ns: [u8; 64],
    ns_len: u8,
    node_len: u8,
    _pad2: [u8; 6],
    /// This node: the one coordinator's controller is beside it, so a
    /// request names this node or is not sent.
    node: [u8; 64],
    sent: [[u8; 16]; SENT],
    doc: [u8; MAX_VALUE],
}

mod params_def {
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, namespace, str, 0
            => |s, d, len| {
                let n = if len > 64 { 0 } else { len };
                s.ns_len = n as u8;
                if n > 0 { core::ptr::copy_nonoverlapping(d, s.ns.as_mut_ptr(), n); }
            };

        2, node_name, str, 0
            => |s, d, len| {
                let n = if len > 64 { 0 } else { len };
                s.node_len = n as u8;
                if n > 0 { core::ptr::copy_nonoverlapping(d, s.node.as_mut_ptr(), n); }
            };
    }
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

/// `prefix` then the address as its u32 in decimal (node_net's keys).
fn addr_key(prefix: &[u8], addr: [u8; 4], out: &mut [u8]) -> usize {
    let p = append(out, 0, prefix);
    wh_dec(out, p, u64::from(wv_u32(addr))).unwrap_or(0)
}

/// Whether `/nodes/<name>` says Ready.
unsafe fn node_ready(s: &mut State, store: &Store, name: &[u8]) -> bool {
    let mut k = [0u8; MAX_KEY];
    let kl = cat(&mut k, &[WV_NODES, name]);
    match get_value(store, &k[..kl], &mut s.doc) {
        Some(n) => j_get2(&s.doc[..n], b"status", b"ready") == Some(&b"true"[..]),
        None => false,
    }
}

/// `node=` of the record at `key`, into `out`; its length (0: none).
unsafe fn holder(store: &Store, key: &[u8], out: &mut [u8; 64]) -> usize {
    let mut v = [0u8; 128];
    let Some(n) = get_value(store, key, &mut v) else {
        return 0;
    };
    let node = wh_field(&v[..n], b"node=");
    if node.len() > out.len() {
        return 0;
    }
    out[..node.len()].copy_from_slice(node);
    node.len()
}

/// The node a move names: the own-hold claimant, so its controller adopts
/// the address in place, or with no claim any ready node — and only this
/// node, whose controller the coordinator beside it reaches. 0 when it is
/// not this node's to ask for (yet).
unsafe fn destination(s: &mut State, store: &Store, addr: [u8; 4], out: &mut [u8; 64]) -> usize {
    let mut k = [0u8; MAX_KEY];
    let kl = addr_key(WV_HOLDS, addr, &mut k);
    let mut claim = [0u8; 64];
    let n = holder(store, &k[..kl], &mut claim);
    let me_len = s.node_len as usize;
    let mut me = [0u8; 64];
    me[..me_len].copy_from_slice(&s.node[..me_len]);
    if me_len == 0 || (n > 0 && claim[..n] != me[..me_len]) {
        return 0;
    }
    if !node_ready(s, store, &me[..me_len]) {
        return 0;
    }
    out[..me_len].copy_from_slice(&me[..me_len]);
    me_len
}

/// What Wormhole's published binding for the Service says of `addr`:
/// `(owned, epoch)`; unowned at epoch 0 when there is none.
unsafe fn binding_of(store: &Store, ns: &[u8], name: &[u8], addr: [u8; 4]) -> (bool, u64) {
    let tenant = wh_tenant_id(ns);
    let service = wh_service_id(ns, name);
    let mut k = [0u8; MAX_KEY];
    let Some(kl) = wh_vip_key(&tenant, &service, traffic::AF_INET, &mut k) else {
        return (false, 0);
    };
    let mut v = [0u8; 1024];
    match get_value(store, &k[..kl], &mut v) {
        Some(n) => wv_binding(&v[..n], &service, addr).unwrap_or((false, 0)),
        None => (false, 0),
    }
}

/// Send `i`'s request once in this life. False when `request_out` has no
/// room (asked again next pass).
unsafe fn send(s: &mut State, ns: &[u8], name: &[u8], i: &WvIntent) -> bool {
    let op = wv_operation_id(ns, name, i);
    if i.node_len == 0 || s.sent.contains(&op) {
        return true;
    }
    let mut f = [0u8; WV_FRAME_MAX];
    let Some(n) = wv_request(ns, name, i, &mut f) else {
        s.refused = s.refused.wrapping_add(1);
        return true;
    };
    let sys = &*s.syscalls;
    if s.request_out < 0 || (sys.channel_write)(s.request_out, f.as_ptr(), n) != n as i32 {
        return false;
    }
    s.sent[s.sent_at as usize] = op;
    s.sent_at = ((s.sent_at as usize + 1) % SENT) as u8;
    s.requests = s.requests.wrapping_add(1);
    true
}

/// The release of the move `led` recorded: from the node whose controller
/// holds the address, at the epoch the binding has now.
unsafe fn release_of(
    s: &State,
    store: &Store,
    ns: &[u8],
    name: &[u8],
    led: &WvIntent,
) -> Option<WvIntent> {
    let mut k = [0u8; MAX_KEY];
    let kl = addr_key(WV_MARKS, led.addr, &mut k);
    let mut node = [0u8; 64];
    let n = holder(store, &k[..kl], &mut node);
    let (owned, epoch) = binding_of(store, ns, name, led.addr);
    // Nothing holds it, or another node's controller does (which this
    // node's coordinator cannot reach): nothing to send from here.
    let mine = node[..n] == s.node[..s.node_len as usize];
    let n = if owned && mine { n } else { 0 };
    wv_intent(true, led.addr, led.rev, epoch, &node[..n])
}

unsafe fn write_ledger(store: &Store, key: &[u8], i: &WvIntent) -> Pass {
    let mut row = [0u8; WV_ROW_MAX];
    let Some(n) = wv_ledger(i, &mut row) else {
        return Pass::Failed;
    };
    match put_value(store, key, &row[..n]) {
        WriteAnswer::Decided(0) | WriteAnswer::Pending => Pass::Again,
        WriteAnswer::Decided(_) => Pass::Failed,
    }
}

macro_rules! stop_if {
    ($store:expr) => {
        if $store.stopped() {
            return if $store.waiting() {
                Pass::Again
            } else {
                Pass::Failed
            };
        }
    };
}

/// One pass: every allocation against its ledger row, then every ledger
/// row whose allocation is gone. One write a pass.
unsafe fn pass(s: &mut State) -> Pass {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut walk = ListWalk::new(WV_LB);
    while let Some(key) = walk.next(&store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let tail = &kb[WV_LB.len().min(kl)..kl];
        let Some((ns, name)) = split_tail(tail) else {
            continue;
        };
        if ns != &s.ns[..s.ns_len as usize] {
            continue;
        }
        let mut row = [0u8; 128];
        let got = get_value_rev(&store, &kb[..kl], &mut row);
        stop_if!(store);
        let Some((rl, rev)) = got else {
            continue;
        };
        let Some(addr) = wv_lb_addr(&row[..rl]) else {
            continue;
        };
        let mut lk = [0u8; MAX_KEY];
        let ll = cat(&mut lk, &[WV_LEDGER, tail]);
        let mut lv = [0u8; WV_ROW_MAX];
        let led = get_value(&store, &lk[..ll], &mut lv).and_then(|n| wv_parse_ledger(&lv[..n]));
        stop_if!(store);
        match led {
            Some(i) if !i.release && i.addr == addr && i.rev == rev => {
                if !send(s, ns, name, &i) {
                    return Pass::Again;
                }
            }
            Some(i) if !i.release => {
                // The allocation moved: release the old address first.
                let Some(r) = release_of(s, &store, ns, name, &i) else {
                    return Pass::Failed;
                };
                stop_if!(store);
                return write_ledger(&store, &lk[..ll], &r);
            }
            Some(i) => {
                if !send(s, ns, name, &i) {
                    return Pass::Again;
                }
                // Released: the new allocation's move is next.
                return match delete_value(&store, &lk[..ll]) {
                    WriteAnswer::Decided(0) | WriteAnswer::Pending => Pass::Again,
                    WriteAnswer::Decided(_) => Pass::Failed,
                };
            }
            None => {
                let mut node = [0u8; 64];
                let n = destination(s, &store, addr, &mut node);
                let (owned, epoch) = binding_of(&store, ns, name, addr);
                stop_if!(store);
                // An owned binding is the controllers' already; a move would
                // be a takeover. No ready node: nothing to name yet.
                if n == 0 && !owned {
                    continue;
                }
                let n = if owned { 0 } else { n };
                let Some(i) = wv_intent(false, addr, rev, epoch, &node[..n]) else {
                    return Pass::Failed;
                };
                return write_ledger(&store, &lk[..ll], &i);
            }
        }
    }
    if walk.failed {
        return Pass::Failed;
    }
    let mut walk = ListWalk::new(WV_LEDGER);
    while let Some(key) = walk.next(&store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let tail = &kb[WV_LEDGER.len().min(kl)..kl];
        let Some((ns, name)) = split_tail(tail) else {
            continue;
        };
        let mut ak = [0u8; MAX_KEY];
        let al = cat(&mut ak, &[WV_LB, tail]);
        if exists(&store, &ak[..al]) || store.stopped() {
            continue;
        }
        let mut lv = [0u8; WV_ROW_MAX];
        let Some(i) = get_value(&store, &kb[..kl], &mut lv).and_then(|n| wv_parse_ledger(&lv[..n]))
        else {
            stop_if!(store);
            continue;
        };
        if !i.release {
            let Some(r) = release_of(s, &store, ns, name, &i) else {
                return Pass::Failed;
            };
            stop_if!(store);
            return write_ledger(&store, &kb[..kl], &r);
        }
        if !send(s, ns, name, &i) {
            return Pass::Again;
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
/// instance arena and syscall table. All are valid for the lifetime the
/// loader guarantees; never called concurrently.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    _in_chan: i32,
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
        core::ptr::write_bytes(state, 0, core::mem::size_of::<State>());
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.request_out = out_chan;
        s.event = -1;
        s.dirty = 1;
        s.nag = ReadNag::new();
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.ns_len == 0 {
            s.ns[..7].copy_from_slice(b"default");
            s.ns_len = 7;
        }
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
                let m = b"[wormhole_vip] ERROR no kernel event - changes will NOT wake this module, it looks again every POLL_MS";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.subscribed = 1;
            }
        }
        while s.subscribed < 1 {
            let rc = store_subscribe(sys, SUBS.get(s.subscribed as usize), s.event, 0);
            if not_yet(rc) {
                return 0;
            }
            s.subscribed += 1;
        }
        // The claims, marks, nodes and Wormhole's bindings this decides on
        // are not subscribed (they move with every node's step): a pass
        // also runs every POLL_MS.
        let now = dev_millis(sys);
        if now.wrapping_sub(s.poll_ms) >= POLL_MS {
            s.poll_ms = now;
            s.retry = 0;
            s.dirty = 1;
        }
        if store_take(sys, s.event, |_| {}) > 0 {
            s.dirty = 1;
        }
        if s.dirty == 0 || s.retry != 0 {
            return 0;
        }
        match pass(s) {
            Pass::Idle => s.dirty = 0,
            Pass::Again => {}
            Pass::Failed => {
                let m = b"[wormhole_vip] ERROR a pass FAILED (a read refused, a listing failed, or a write refused) - run again in POLL_MS";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.dirty = 0;
                s.retry = 1;
            }
        }
        0
    }
}
