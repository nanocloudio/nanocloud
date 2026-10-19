//! NodePort and LoadBalancer allocation, claimed in the store.
//!
//! See `manifest.toml` for what is allocated and the order of the actions.
//! The store is the allocator's only memory: a claim key per port and per
//! address, created under the ABSENT precondition, so a restart (or a second
//! control plane) cannot hand out what is already held.

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
include!("../_shared/json.rs");
include!("../_shared/store.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;

const PORT_INPUT: u8 = 0;
const MAX_PARAM: usize = 32;
/// A Service object, read whole.
const DOC: usize = 16 * 1024;
/// Room for what allocation adds to it.
const OUT: usize = DOC + 1024;
/// Candidates probed per step before the search resumes on the next.
const PROBES: u32 = 256;
/// Ports of one Service this allocator fills.
const MAX_PORTS: usize = 16;

const SERVICES: &[u8] = b"/services/";
const NP_CLAIMS: &[u8] = b"/ipam/nodeport/";
const LB_CLAIMS: &[u8] = b"/ipam/lb/";

/// What one pass did.
enum Pass {
    /// Nothing is owed.
    Idle,
    /// It acted (or must look again): run another pass next step.
    Again,
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    sink: i32,
    subscribed: u32,
    dirty: u32,

    lo: u16,
    hi: u16,
    pool_net: u32,
    pool_size: u32,
    port_cursor: u16,
    ip_cursor: u32,
    ports_txt: [u8; MAX_PARAM],
    ports_len: u8,
    pool_txt: [u8; MAX_PARAM],
    pool_len: u8,

    /// A write the store answered EINPROGRESS: asked again, byte-identical,
    /// before anything else.
    owed: u8,
    owed_pre: u8,
    owed_del: u8,
    owed_rev: u64,
    owed_key: [u8; MAX_KEY],
    owed_klen: u16,
    owed_vlen: u32,

    doc: [u8; DOC],
    out: [u8; OUT],
    nag: ReadNag,

    allocated: u32,
    released: u32,
    dup_said: u8,
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PARAM;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, node_ports, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.ports_len = n as u8;
                if n > 0 { ptr_copy(s.ports_txt.as_mut_ptr(), d, n); }
            };

        2, lb_pool, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.pool_len = n as u8;
                if n > 0 { ptr_copy(s.pool_txt.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

fn parse_num(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 10 {
        return None;
    }
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
    }
    (v <= u32::MAX as u64).then_some(v as u32)
}

fn parse_ip4(b: &[u8]) -> Option<u32> {
    let mut out = 0u32;
    let mut n = 0;
    for part in b.split(|&c| c == b'.') {
        let v = parse_num(part)?;
        if v > 255 || n == 4 {
            return None;
        }
        out = (out << 8) | v;
        n += 1;
    }
    (n == 4).then_some(out)
}

/// Decimal of `v` into `out` at `at`.
fn put_dec(out: &mut [u8], at: usize, v: u64) -> Option<usize> {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    put(out, at, &tmp[i..])
}

fn put(dst: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    let d = dst.get_mut(at..end)?;
    for (o, &i) in d.iter_mut().zip(src.iter()) {
        *o = i;
    }
    Some(end)
}

/// `a.b.c.d` of `a` into `out` at `at`.
fn put_ip(out: &mut [u8], mut at: usize, a: u32) -> Option<usize> {
    for (i, o) in a.to_be_bytes().iter().enumerate() {
        if i > 0 {
            at = put(out, at, b".")?;
        }
        at = put_dec(out, at, *o as u64)?;
    }
    Some(at)
}

/// The claim key `<prefix><n>` into `k`; its length.
fn claim_key(k: &mut [u8; MAX_KEY], prefix: &[u8], n: u64) -> usize {
    put(k, 0, prefix)
        .and_then(|p| put_dec(k, p, n))
        .unwrap_or(0)
}

/// What a Service is to this allocator.
struct Svc {
    /// NodePort or LoadBalancer: owed node ports.
    node: bool,
    /// LoadBalancer.
    lb: bool,
    /// Its ports' nodePorts, 0 where none.
    ports: [u16; MAX_PORTS],
    nports: usize,
    /// Its load-balancer address (0: none).
    lb_ip: u32,
}

fn read_svc(doc: &[u8]) -> Svc {
    let t = j_get2(doc, b"spec", b"type").unwrap_or(&[]);
    let lb = t == b"LoadBalancer";
    let mut s = Svc {
        node: lb || t == b"NodePort",
        lb,
        ports: [0; MAX_PORTS],
        nports: 0,
        lb_ip: 0,
    };
    if let Some(ports) = j_sub2(doc, b"spec", b"ports") {
        while s.nports < MAX_PORTS {
            let Some(el) = j_idx(ports, s.nports) else {
                break;
            };
            s.ports[s.nports] = j_get1(el, b"nodePort")
                .and_then(parse_num)
                .filter(|&p| p > 0 && p <= 65535)
                .unwrap_or(0) as u16;
            s.nports += 1;
        }
    }
    if let Some(ing) = j_sub2(doc, b"status", b"loadBalancer").and_then(|l| j_sub1(l, b"ingress")) {
        s.lb_ip = j_idx(ing, 0)
            .and_then(|e| j_get1(e, b"ip"))
            .and_then(parse_ip4)
            .unwrap_or(0);
    }
    s
}

/// Ask (or ask again) the owed write.
unsafe fn ask_owed(s: &mut State) {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let key = &s.owed_key[..s.owed_klen as usize];
    let a = if s.owed_del != 0 {
        delete_cond(&store, key, s.owed_pre, s.owed_rev)
    } else {
        put_cond(
            &store,
            key,
            &s.out[..s.owed_vlen as usize],
            s.owed_pre,
            s.owed_rev,
        )
    };
    s.owed = (a == WriteAnswer::Pending) as u8;
}

/// Write `key` (`s.out[..vlen]`, or a delete) under `pre`/`rev`, owed until
/// the store decides.
unsafe fn act(s: &mut State, key: &[u8], vlen: usize, del: bool, pre: u8, rev: u64) {
    let kl = key.len().min(MAX_KEY);
    s.owed_key[..kl].copy_from_slice(&key[..kl]);
    s.owed_klen = kl as u16;
    s.owed_vlen = vlen as u32;
    s.owed_del = del as u8;
    s.owed_pre = pre;
    s.owed_rev = rev;
    ask_owed(s);
}

/// Claim what a Service holds and nobody has claimed. True when it acted.
unsafe fn claim_held(s: &mut State, store: &Store, owner: &[u8], svc: &Svc) -> bool {
    let mut k = [0u8; MAX_KEY];
    let mut cur = [0u8; MAX_KEY];
    let mut held = [0u64; MAX_PORTS + 1];
    let mut nh = 0usize;
    for &p in svc.ports[..svc.nports].iter() {
        if svc.node && p != 0 {
            held[nh] = p as u64;
            nh += 1;
        }
    }
    for &n in held[..nh].iter() {
        let kl = claim_key(&mut k, NP_CLAIMS, n);
        if kl == 0 {
            continue;
        }
        match get_value(store, &k[..kl], &mut cur) {
            Some(cl) => {
                if &cur[..cl] != owner && s.dup_said == 0 {
                    s.dup_said = 1;
                    let m = b"[svc_alloc] a nodePort two Services name is held by the first";
                    dev_log(&*s.syscalls, 1, m.as_ptr(), m.len());
                }
            }
            None if store.waiting() => return false,
            None => {
                let Some(vl) = put(&mut s.out, 0, owner) else {
                    continue;
                };
                act(s, &k[..kl], vl, false, PRE_ABSENT, 0);
                return true;
            }
        }
    }
    if svc.lb && svc.lb_ip != 0 {
        let kl = claim_key(&mut k, LB_CLAIMS, svc.lb_ip as u64);
        if get_value(store, &k[..kl], &mut cur).is_none() && !store.waiting() {
            let Some(vl) = put(&mut s.out, 0, owner) else {
                return false;
            };
            act(s, &k[..kl], vl, false, PRE_ABSENT, 0);
            return true;
        }
    }
    false
}

/// Release one claim under `prefix` whose owner no longer holds it. True
/// when it acted.
unsafe fn release(s: &mut State, store: &Store, prefix: &[u8], lb: bool) -> bool {
    let mut walk = ListWalk::new(prefix);
    while let Some(key) = walk.next(store) {
        let mut k = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        k[..kl].copy_from_slice(&key[..kl]);
        let Some(n) = k.get(prefix.len()..kl).and_then(parse_num) else {
            continue;
        };
        let mut owner = [0u8; MAX_KEY];
        let Some(ol) = get_value(store, &k[..kl], &mut owner) else {
            continue;
        };
        let mut sk = [0u8; MAX_KEY];
        let Some(skl) = put(&mut sk, 0, SERVICES).and_then(|p| put(&mut sk, p, &owner[..ol]))
        else {
            continue;
        };
        let holds = match get_value(store, &sk[..skl], &mut s.doc) {
            Some(dl) => {
                let svc = read_svc(&s.doc[..dl]);
                if lb {
                    svc.lb && svc.lb_ip == n
                } else {
                    svc.node && !svc.ports[..svc.nports].iter().all(|&p| p as u32 != n)
                }
            }
            None => false,
        };
        if store.waiting() {
            return false;
        }
        if !holds {
            act(s, &k[..kl], 0, true, PRE_ANY, 0);
            s.released = s.released.wrapping_add(1);
            return true;
        }
    }
    false
}

/// The next free claim of `prefix` at or after `cursor` in `[lo, lo+span)`,
/// not in `taken`; the cursor advanced past it. None: none free within this
/// step's probes (`*budget` spent), or the store has not answered.
unsafe fn free_one(
    store: &Store,
    prefix: &[u8],
    lo: u32,
    span: u32,
    cursor: &mut u32,
    taken: &[u32],
    budget: &mut u32,
) -> Option<u32> {
    let mut k = [0u8; MAX_KEY];
    while *budget > 0 {
        *budget -= 1;
        let c = lo + cursor.checked_rem(span)?;
        *cursor = (*cursor + 1).checked_rem(span)?;
        if !taken.iter().all(|&t| t != c) {
            continue;
        }
        let kl = claim_key(&mut k, prefix, c as u64);
        if exists(store, &k[..kl]) {
            continue;
        }
        if store.waiting() {
            return None;
        }
        return Some(c);
    }
    None
}

/// Give the Service at `key` what it is owed, written back conditional on the
/// revision read. True when it acted or must look again.
unsafe fn allocate(s: &mut State, store: &Store, key: &[u8]) -> bool {
    let Some((dl, rev)) = get_value_rev(store, key, &mut s.doc) else {
        return store.waiting();
    };
    let svc = read_svc(&s.doc[..dl]);
    let mut budget = PROBES;
    // Node ports first: one per port that has none.
    let mut picks = [0u32; MAX_PORTS];
    let mut taken = [0u32; MAX_PORTS];
    let mut nt = 0usize;
    for &p in svc.ports[..svc.nports].iter() {
        if p != 0 {
            taken[nt] = p as u32;
            nt += 1;
        }
    }
    if svc.node && s.hi >= s.lo {
        let span = (s.hi - s.lo) as u32 + 1;
        for (pick, &have) in picks.iter_mut().zip(svc.ports[..svc.nports].iter()) {
            if have != 0 {
                continue;
            }
            let mut cur = s.port_cursor as u32;
            let got = free_one(
                store,
                NP_CLAIMS,
                s.lo as u32,
                span,
                &mut cur,
                &taken[..nt],
                &mut budget,
            );
            s.port_cursor = cur as u16;
            match got {
                Some(p) => {
                    *pick = p;
                    if nt < taken.len() {
                        taken[nt] = p;
                        nt += 1;
                    }
                }
                // None free this step: the search resumes on the next.
                None => return true,
            }
        }
    }
    let mut lb_ip = 0u32;
    if svc.lb && svc.lb_ip == 0 && s.pool_size > 0 {
        let mut cur = s.ip_cursor;
        let got = free_one(
            store,
            LB_CLAIMS,
            s.pool_net,
            s.pool_size,
            &mut cur,
            &[],
            &mut budget,
        );
        s.ip_cursor = cur;
        match got {
            Some(a) => lb_ip = a,
            None => return true,
        }
    }
    if picks.iter().all(|&p| p == 0) && lb_ip == 0 {
        return false;
    }
    // The object with each pick in its port, rewritten in place.
    let doc = &s.doc[..dl];
    let mut ol = 0usize;
    let mut from = 0usize;
    let ports_at = j_obj_get(doc, 0, b"spec").and_then(|sp| j_obj_get(doc, sp, b"ports"));
    if let Some(pa) = ports_at {
        for (i, &p) in picks.iter().enumerate() {
            if p == 0 {
                continue;
            }
            let Some(es) = j_arr_get(doc, pa, i) else {
                continue;
            };
            if doc[es] != b'{' {
                continue;
            }
            let after = j_ws(doc, es + 1);
            let empty = doc.get(after) == Some(&b'}');
            let Some(e) = put(&mut s.out, ol, &doc[from..=es])
                .and_then(|e| put(&mut s.out, e, b"\"nodePort\":"))
                .and_then(|e| put_dec(&mut s.out, e, p as u64))
                .and_then(|e| {
                    if empty {
                        Some(e)
                    } else {
                        put(&mut s.out, e, b",")
                    }
                })
            else {
                return false;
            };
            ol = e;
            from = es + 1;
        }
    }
    let Some(e) = put(&mut s.out, ol, &doc[from..]) else {
        return false;
    };
    ol = e;
    if lb_ip != 0 {
        let mut v = [0u8; 64];
        let Some(vl) = put(&mut v, 0, b"{\"ingress\":[{\"ip\":\"")
            .and_then(|e| put_ip(&mut v, e, lb_ip))
            .and_then(|e| put(&mut v, e, b"\"}]}"))
        else {
            return false;
        };
        let n = j_set2(
            &s.out[..ol],
            b"status",
            b"loadBalancer",
            &v[..vl],
            &mut s.doc,
        );
        if n == 0 || n >= DOC {
            return false;
        }
        let Some(e) = put(&mut s.out, 0, &s.doc[..n]) else {
            return false;
        };
        ol = e;
    }
    act(s, key, ol, false, PRE_ETAG, rev);
    s.allocated = s.allocated.wrapping_add(1);
    true
}

/// One pass: the first action owed, in the manifest's order.
unsafe fn pass(s: &mut State) -> Pass {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    // claim, and find the first Service owed something
    let mut owed = [0u8; MAX_KEY];
    let mut owed_len = 0usize;
    let mut walk = ListWalk::new(SERVICES);
    while let Some(key) = walk.next(&store) {
        let mut k = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        k[..kl].copy_from_slice(&key[..kl]);
        let Some(dl) = get_value(&store, &k[..kl], &mut s.doc) else {
            continue;
        };
        let svc = read_svc(&s.doc[..dl]);
        if !svc.node {
            continue;
        }
        let owner = &k[SERVICES.len()..kl];
        let mut ow = [0u8; MAX_KEY];
        ow[..owner.len()].copy_from_slice(owner);
        if claim_held(s, &store, &ow[..owner.len()], &svc) {
            return Pass::Again;
        }
        let owes = !svc.ports[..svc.nports].iter().all(|&p| p != 0)
            || (svc.lb && svc.lb_ip == 0 && s.pool_size > 0);
        if owes && owed_len == 0 {
            owed[..kl].copy_from_slice(&k[..kl]);
            owed_len = kl;
        }
    }
    if store.waiting() || walk.failed {
        return Pass::Again;
    }
    // release
    if release(s, &store, NP_CLAIMS, false) || release(s, &store, LB_CLAIMS, true) {
        return Pass::Again;
    }
    if store.waiting() {
        return Pass::Again;
    }
    // allocate
    if owed_len > 0 && allocate(s, &store, &owed[..owed_len]) {
        return Pass::Again;
    }
    if store.waiting() {
        return Pass::Again;
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
    in_chan: i32,
    _out_chan: i32,
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
        s.sink = in_chan;
        s.subscribed = 0;
        s.dirty = 1;
        s.ports_len = 0;
        s.pool_len = 0;
        s.owed = 0;
        s.port_cursor = 0;
        s.ip_cursor = 0;
        s.allocated = 0;
        s.released = 0;
        s.dup_said = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        let sys = &*s.syscalls;
        // node_ports: `<lo>-<hi>`, default upstream's 30000-32767.
        s.lo = 30000;
        s.hi = 32767;
        let pt = &s.ports_txt[..s.ports_len as usize];
        if !pt.is_empty() {
            let dash = pt.iter().position(|&c| c == b'-');
            let range = dash.and_then(|d| Some((parse_num(&pt[..d])?, parse_num(&pt[d + 1..])?)));
            match range {
                Some((lo, hi)) if lo > 0 && lo <= hi && hi <= 65535 => {
                    s.lo = lo as u16;
                    s.hi = hi as u16;
                }
                _ => {
                    let m = b"[svc_alloc] FAULT: `node_ports` is not <lo>-<hi>";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                    return -3;
                }
            }
        }
        // lb_pool: `<a.b.c.d>/<n>`, its hosts (network and broadcast
        // excepted; a /31 or /32 is every address it names).
        s.pool_net = 0;
        s.pool_size = 0;
        let pl = &s.pool_txt[..s.pool_len as usize];
        if !pl.is_empty() {
            let slash = pl.iter().position(|&c| c == b'/');
            let cidr = slash.and_then(|i| Some((parse_ip4(&pl[..i])?, parse_num(&pl[i + 1..])?)));
            match cidr {
                Some((a, n)) if n <= 32 => {
                    let mask = if n == 0 { 0 } else { u32::MAX << (32 - n) };
                    let net = a & mask;
                    let all = (!mask).wrapping_add(1);
                    if n >= 31 {
                        s.pool_net = net;
                        s.pool_size = if n == 32 { 1 } else { 2 };
                    } else {
                        s.pool_net = net + 1;
                        s.pool_size = all - 2;
                    }
                }
                _ => {
                    let m = b"[svc_alloc] FAULT: `lb_pool` is not <a.b.c.d>/<n>";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                    return -3;
                }
            }
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
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 && store_subscribe(sys, SERVICES, s.sink, 0) == READ_WAIT {
                return 0;
            }
            s.subscribed = 1;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.dirty = 1;
        }
        if s.owed != 0 {
            ask_owed(s);
            if s.owed != 0 {
                return 0;
            }
            s.dirty = 1;
        }
        if s.dirty != 0 {
            s.dirty = 0;
            if let Pass::Again = pass(s) {
                s.dirty = 1;
            }
        }
        0
    }
}
