//! Rows under a store prefix → one `net.policy` table.
//!
//! See `manifest.toml` for the row grammar and the rationale. The chains
//! decide; this encodes their rows as rule records, replaces the table whole,
//! and writes down whether the platform enforces it.

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
include!("../_shared/store.rs");
include!("../_shared/field.rs");

use abi::contracts::net::policy as wire;

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
const PORT_OUTPUT: u8 = 1;

/// The table this module builds: the provider's own per-table bound on the
/// host profile, so a table that fits here fits there.
const TABLE: usize = 8192;
const MAX_PARAM: usize = 96;
/// Steps between re-applies while a failure stands (nft missing, a refused
/// table): retried, not spun.
const RETRY_STEPS: u32 = 1000;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    sink: i32,
    resolved: u32,
    subscribed: u32,
    dirty: u32,
    backoff: u32,

    prefix: [u8; MAX_PARAM],
    prefix_len: u16,
    table: [u8; wire::MAX_TABLE_NAME],
    table_len: u16,
    status_key: [u8; MAX_PARAM],
    status_key_len: u16,
    node_addr: [u8; 16],
    node_addr_len: u8,
    /// The status record owed to the store: written, and asked again each
    /// step until the store has decided it.
    status_val: [u8; 128],
    status_len: u8,
    status_owed: u8,

    /// FNV-1a of the last table APPLIED, so an unchanged row set does not
    /// re-render the platform's policy on every store change.
    applied_hash: u64,
    applied_any: u32,

    applied: u32,
    refused: u32,
    skipped: u32,
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PARAM;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, prefix, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.prefix_len = n as u16;
                if n > 0 { ptr_copy(s.prefix.as_mut_ptr(), d, n); }
            };

        2, table, str, 0
            => |s, d, len| {
                let n = if len > super::wire::MAX_TABLE_NAME { 0 } else { len };
                s.table_len = n as u16;
                if n > 0 { ptr_copy(s.table.as_mut_ptr(), d, n); }
            };

        3, status_key, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.status_key_len = n as u16;
                if n > 0 { ptr_copy(s.status_key.as_mut_ptr(), d, n); }
            };

        4, node_address, str, 0
            => |s, d, len| {
                let n = if len > 16 { 0 } else { len };
                s.node_addr_len = n as u8;
                if n > 0 { ptr_copy(s.node_addr.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

fn fnv(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in b {
        h ^= c as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn parse_u32(b: &[u8]) -> Option<u32> {
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
        let v = parse_u32(part)?;
        if v > 255 || n == 4 {
            return None;
        }
        out = (out << 8) | v;
        n += 1;
    }
    (n == 4).then_some(out)
}

/// `a.b.c.d` (a /32) or `a.b.c.d/n`.
fn parse_cidr(b: &[u8]) -> Option<(u32, u8)> {
    match b.iter().position(|&c| c == b'/') {
        Some(i) => {
            let p = parse_u32(&b[i + 1..])?;
            (p <= 32).then_some(())?;
            Some((parse_ip4(&b[..i])?, p as u8))
        }
        None => Some((parse_ip4(b)?, 32)),
    }
}

/// The most CIDRs one peer's `cidr except …` becomes. Each exception inside
/// the peer's range splits it into at most 32 pieces; past this bound the
/// peer is refused — allows nothing — rather than rendered wider than written.
const MAX_PIECES: usize = 64;

/// `base` less every range in `excepts` (a `+`-joined list), as the CIDRs
/// that cover exactly what is left — the ipBlock `except` of a NetworkPolicy
/// peer, expressed with allows alone. `None` when an exception does not parse
/// or the result exceeds `MAX_PIECES`; the peer then allows nothing.
fn cidr_minus(base: (u32, u8), excepts: &[u8], out: &mut [(u32, u8); MAX_PIECES]) -> Option<usize> {
    fn mask(len: u8) -> u32 {
        if len == 0 {
            0
        } else {
            u32::MAX << (32 - len as u32)
        }
    }
    let mut n = 1usize;
    out[0] = (base.0 & mask(base.1), base.1);
    if excepts.is_empty() || excepts == b"-" {
        return Some(n);
    }
    for e in excepts.split(|&c| c == b'+') {
        let (ea, el) = parse_cidr(e)?;
        let ea = ea & mask(el);
        let mut next = [(0u32, 0u8); MAX_PIECES];
        let mut m = 0usize;
        for &(ca, cl) in out.iter().take(n) {
            let inside_e = cl >= el && (ca & mask(el)) == ea; // e covers c
            let e_inside = el > cl && (ea & mask(cl)) == ca; // c strictly covers e
            if inside_e {
                continue;
            }
            if !e_inside {
                *next.get_mut(m)? = (ca, cl);
                m += 1;
                continue;
            }
            // c less e: the sibling of every prefix on the path from c to e.
            for l in cl..el {
                let bit = 1u32 << (31 - l as u32);
                let sib = ((ea & mask(l + 1)) ^ bit, l + 1);
                *next.get_mut(m)? = sib;
                m += 1;
            }
        }
        *out = next;
        n = m;
    }
    Some(n)
}

/// A port: a number 1..=65535. A named port (`http`) cannot be resolved
/// here and is None — the entry allows nothing rather than everything.
fn parse_port(b: &[u8]) -> Option<u16> {
    let v = parse_u32(b)?;
    (1..=65535).contains(&v).then_some(v as u16)
}

/// The port a Pod names `name`: its `containerPort` in `nums`, aligned with
/// `names` (`spec.containers.0.ports[].name` / `.containerPort`).
fn named_port(names: &[u8], nums: &[u8], name: &[u8]) -> Option<u16> {
    let i = names.split(|&c| c == b',').position(|n| n == name)?;
    parse_port(nth(nums, i))
}

/// Kubernetes' protocol names; absent means TCP. SCTP has no rule here.
fn parse_proto(b: &[u8]) -> Option<u8> {
    match b {
        b"" | b"-" | b"TCP" => Some(wire::rule::PROTO_TCP),
        b"UDP" => Some(wire::rule::PROTO_UDP),
        _ => None,
    }
}

/// The `i`-th entry of a `,` list, or `""` past its end.
fn nth(list: &[u8], i: usize) -> &[u8] {
    list.split(|&c| c == b',').nth(i).unwrap_or(&[])
}

fn count(list: &[u8]) -> usize {
    if list.is_empty() {
        0
    } else {
        list.split(|&c| c == b',').count()
    }
}

struct Out<'a> {
    buf: &'a mut [u8],
    len: usize,
    rules: u32,
    full: bool,
}

impl Out<'_> {
    fn push(&mut self, rec: &[u8]) {
        if self.len + rec.len() > self.buf.len() {
            self.full = true;
            return;
        }
        self.buf[self.len..self.len + rec.len()].copy_from_slice(rec);
        self.len += rec.len();
        self.rules += 1;
    }
}

fn dir_of(b: &[u8]) -> Option<u8> {
    match b {
        b"in" => Some(wire::rule::DIR_INGRESS),
        b"out" => Some(wire::rule::DIR_EGRESS),
        _ => None,
    }
}

/// Encode one row. False when the row is not one this grammar knows.
/// One DNAT record: `(proto, addr, port)` to `be:tport`.
fn dnat(out: &mut Out, proto: u8, addr: u32, port: u16, be: u32, tport: u16) {
    let len = wire::rule::DNAT_HEAD + wire::rule::DNAT_BACKEND;
    let mut r = [0u8; 2 + wire::rule::DNAT_HEAD + wire::rule::DNAT_BACKEND];
    r[0] = wire::rule::DNAT;
    r[1] = len as u8;
    r[2] = proto;
    r[3..7].copy_from_slice(&addr.to_be_bytes());
    r[7..9].copy_from_slice(&port.to_be_bytes());
    r[9] = 1;
    r[10..14].copy_from_slice(&be.to_be_bytes());
    r[14..16].copy_from_slice(&tport.to_be_bytes());
    out.push(&r);
}

/// Encode one row; `node` is this node's address (its node ports' target).
fn encode(row: &[u8], out: &mut Out, node: Option<u32>) -> bool {
    let get = |t: &[u8]| field(row, t).unwrap_or(&[]);
    match get(b"kind=") {
        k @ (b"allow" | b"drop") => {
            let kind = if k == b"allow" {
                wire::rule::ALLOW
            } else {
                wire::rule::DROP
            };
            let (Some(dir), Some((subj, sp))) = (dir_of(get(b"dir=")), parse_cidr(get(b"subj=")))
            else {
                return false;
            };
            let (peers, ports, protos) = (get(b"peers="), get(b"ports="), get(b"protos="));
            let excepts = get(b"except=");
            let np = count(peers).max(1);
            let nq = count(ports).max(1);
            for i in 0..np {
                let base = if peers.is_empty() {
                    (0, 0)
                } else {
                    match parse_cidr(nth(peers, i)) {
                        Some(v) => v,
                        None => continue, // a peer nothing here can name
                    }
                };
                // The peer less its exceptions, as the pieces that remain.
                let mut pieces = [(0u32, 0u8); MAX_PIECES];
                let ex = if excepts.is_empty() {
                    &[][..]
                } else {
                    nth(excepts, i)
                };
                let Some(npc) = cidr_minus(base, ex, &mut pieces) else {
                    continue; // an exception that cannot be honoured allows nothing
                };
                for &(peer, pp) in pieces.iter().take(npc) {
                    for j in 0..nq {
                        let (proto, port) = if ports.is_empty() {
                            (wire::rule::PROTO_ANY, 0)
                        } else {
                            match (parse_proto(nth(protos, j)), parse_port(nth(ports, j))) {
                                (Some(pr), Some(po)) => (pr, po),
                                _ => continue,
                            }
                        };
                        let mut r = [0u8; 2 + wire::rule::FILTER_BODY];
                        r[0] = kind;
                        r[1] = wire::rule::FILTER_BODY as u8;
                        r[2] = dir;
                        r[3] = proto;
                        r[4..8].copy_from_slice(&subj.to_be_bytes());
                        r[8] = sp;
                        r[9..13].copy_from_slice(&peer.to_be_bytes());
                        r[13] = pp;
                        r[14..16].copy_from_slice(&port.to_be_bytes());
                        r[16..18].copy_from_slice(&port.to_be_bytes());
                        out.push(&r);
                    }
                }
            }
            true
        }
        b"isolate" => {
            let (Some(dir), Some((subj, sp))) = (dir_of(get(b"dir=")), parse_cidr(get(b"subj=")))
            else {
                return false;
            };
            let mut r = [0u8; 2 + wire::rule::ISOLATE_BODY];
            r[0] = wire::rule::ISOLATE;
            r[1] = wire::rule::ISOLATE_BODY as u8;
            r[2] = dir;
            r[3..7].copy_from_slice(&subj.to_be_bytes());
            r[7] = sp;
            out.push(&r);
            true
        }
        b"dnat" => {
            let (Some(vip), Some(be)) = (parse_ip4(get(b"vip=")), parse_ip4(get(b"backend=")))
            else {
                return false;
            };
            let (vports, tports, protos) = (get(b"vports="), get(b"tports="), get(b"protos="));
            for i in 0..count(vports) {
                let (Some(proto), Some(vport)) =
                    (parse_proto(nth(protos, i)), parse_port(nth(vports, i)))
                else {
                    continue;
                };
                let tp = nth(tports, i);
                let tport = if tp.is_empty() || tp == b"-" {
                    vport
                } else {
                    // A number, or a name the backend's Pod gives a port
                    // (its `pnames`/`pnums`); a name it does not have
                    // leads nowhere on this backend.
                    match parse_port(tp).or_else(|| named_port(get(b"pnames="), get(b"pnums="), tp))
                    {
                        Some(p) => p,
                        None => continue,
                    }
                };
                dnat(out, proto, vip, vport, be, tport);
                // The node's own address at the port's nodePort, and the
                // load-balancer address at the port: the same backend.
                let np = nth(get(b"nports="), i);
                if let (Some(node), Some(np)) = (node, parse_port(np)) {
                    dnat(out, proto, node, np, be, tport);
                }
                if let Some(lb) = parse_ip4(get(b"lbip=")) {
                    dnat(out, proto, lb, vport, be, tport);
                }
            }
            true
        }
        _ => false,
    }
}

/// Decimal of `v` appended at `at`.
fn put_dec(out: &mut [u8], at: usize, v: u64) -> usize {
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
    append(out, at, &tmp[i..])
}

/// Write the status record, put-if-changed.
unsafe fn status(s: &mut State, sys: &SyscallTable, text: &[u8]) {
    if s.status_key_len == 0 || text.len() > s.status_val.len() {
        return;
    }
    let key = &s.status_key[..s.status_key_len as usize];
    let mut cur = [0u8; 128];
    if let Some(n) = get_value(&Store::plain(sys, &s.nag), key, &mut cur) {
        if &cur[..n] == text {
            return;
        }
    }
    s.status_val[..text.len()].copy_from_slice(text);
    s.status_len = text.len() as u8;
    s.status_owed = 1;
    ask_status(s, sys);
}

/// Ask for the owed status write; it stays owed until the store decides.
unsafe fn ask_status(s: &mut State, sys: &SyscallTable) {
    let key = &s.status_key[..s.status_key_len as usize];
    let val = &s.status_val[..s.status_len as usize];
    if put_value(&Store::plain(sys, &s.nag), key, val) != WriteAnswer::Pending {
        s.status_owed = 0;
    }
}

/// Rebuild the table from the rows and apply it. True when the platform took
/// it (or it was unchanged).
unsafe fn apply(s: &mut State, sys: &SyscallTable) -> bool {
    let prefix_len = s.prefix_len as usize;
    let mut prefix = [0u8; MAX_PARAM];
    prefix[..prefix_len].copy_from_slice(&s.prefix[..prefix_len]);
    let mut rules = [0u8; TABLE];
    let mut out = Out {
        buf: &mut rules,
        len: 0,
        rules: 0,
        full: false,
    };
    let store = Store::plain(sys, &s.nag);
    let mut walk = ListWalk::new(&prefix[..prefix_len]);
    let mut skipped = 0u32;
    let node = parse_ip4(&s.node_addr[..s.node_addr_len as usize]);
    while let Some(key) = walk.next(&store) {
        let mut k = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        k[..kl].copy_from_slice(&key[..kl]);
        let mut row = [0u8; MAX_VALUE];
        let Some(n) = get_value(&store, &k[..kl], &mut row) else {
            continue;
        };
        if !encode(&row[..n], &mut out, node) {
            skipped += 1;
        }
        if out.full {
            break;
        }
    }
    if store.waiting() {
        // A row or a page was not answered yet (`EAGAIN`): what was read is
        // not the table, so the old one stands and this is asked again.
        return false;
    }
    s.skipped = s.skipped.wrapping_add(skipped);
    let mut st = [0u8; 128];
    if walk.failed {
        // Could not list: the old table stands, and this is retried.
        let p = append(&mut st, 0, b"error=list;rules=0");
        status(s, sys, &st[..p]);
        return false;
    }
    if out.full {
        let p = append(&mut st, 0, b"error=table-full;rules=");
        let p = put_dec(&mut st, p, out.rules as u64);
        status(s, sys, &st[..p]);
        return false;
    }
    let (len, n_rules) = (out.len, out.rules);
    let h = fnv(&rules[..len]);
    if s.applied_any != 0 && h == s.applied_hash {
        return true;
    }

    let name_len = s.table_len as usize;
    let mut gen = 0u64;
    let mut arg = [0u8; 1 + wire::MAX_TABLE_NAME + 20];
    arg[0] = name_len as u8;
    arg[1..1 + name_len].copy_from_slice(&s.table[..name_len]);
    let mut p = 1 + name_len;
    arg[p..p + 8].copy_from_slice(&(rules.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(len as u32).to_le_bytes());
    p += 4;
    arg[p..p + 8].copy_from_slice(&(&mut gen as *mut u64 as u64).to_le_bytes());
    p += 8;
    let r = (sys.provider_call)(-1, wire::REPLACE, arg.as_mut_ptr(), p);
    if r != 0 {
        s.refused = s.refused.wrapping_add(1);
        let q = append(&mut st, 0, b"error=");
        let q = put_dec(&mut st, q, r.unsigned_abs() as u64);
        let q = append(&mut st, q, b";rules=");
        let q = put_dec(&mut st, q, n_rules as u64);
        status(s, sys, &st[..q]);
        return false;
    }
    s.applied = s.applied.wrapping_add(1);
    s.applied_hash = h;
    s.applied_any = 1;
    // What the platform ENFORCES, asked rather than assumed.
    let mut caps = [0u8; 4];
    let enforced = (sys.provider_call)(-1, wire::CAPS, caps.as_mut_ptr(), 4) == 0
        && (u32::from_le_bytes(caps) & wire::caps::ENFORCED) != 0;
    let q = append(
        &mut st,
        0,
        if enforced {
            b"enforced=1;rules="
        } else {
            b"enforced=0;rules="
        },
    );
    let q = put_dec(&mut st, q, n_rules as u64);
    let q = append(&mut st, q, b";generation=");
    let q = put_dec(&mut st, q, gen);
    status(s, sys, &st[..q]);
    true
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
        s.resolved = 0;
        s.subscribed = 0;
        s.dirty = 1;
        s.backoff = 0;
        s.prefix_len = 0;
        s.table_len = 0;
        s.status_key_len = 0;
        s.node_addr_len = 0;
        s.applied_hash = 0;
        s.applied_any = 0;
        s.applied = 0;
        s.refused = 0;
        s.skipped = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.prefix_len == 0 || s.table_len == 0 {
            let sys = &*s.syscalls;
            let m = b"[net_apply] FAULT: `prefix` and `table` are required";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -3;
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
            if s.sink >= 0 {
                let pl = s.prefix_len as usize;
                let mut p = [0u8; MAX_PARAM];
                p[..pl].copy_from_slice(&s.prefix[..pl]);
                if store_subscribe(sys, &p[..pl], s.sink, 0) == READ_WAIT {
                    // Not yet: no subscription was made; asked next step.
                    return 0;
                }
            }
            s.subscribed = 1;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.dirty = 1;
        }
        if s.status_owed != 0 {
            ask_status(s, sys);
        }
        if s.backoff > 0 {
            s.backoff -= 1;
            if s.backoff == 0 {
                s.dirty = 1;
            }
        }
        if s.dirty != 0 && s.backoff == 0 {
            s.dirty = 0;
            if !apply(s, sys) {
                s.backoff = RETRY_STEPS;
            }
        }
        0
    }
}
