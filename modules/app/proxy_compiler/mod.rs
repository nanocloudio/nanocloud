//! Service proxy compiler — nanocloud's kube-proxy equivalent as a PIC module —
//! a *compiler* reconciler, the sibling of `netpolicy_compiler`. It watches
//! /services/ and /endpoints/, compiles the node's nftables NAT ruleset
//! (ClusterIP → backend DNAT load-balancing), and publishes it to the store at
//! /dataplane/proxy; the node's network backend programs nft from there.
//! Decision here, effect there.
//!
//! Data model (services carry proxy fields alongside the `sel=` that
//! endpoints_reconciler reads — one record, two consumers; backends come from
//! endpoints_reconciler's own output):
//!   /services/<ns>/<name>  = "sel=...;clusterip=<ip>;port=<p>;targetport=<tp>"
//!   /endpoints/<ns>/<name> = "<pod>=<addr>,<pod>=<addr>"   (backend addrs)
//! Output: the compiled ruleset at /dataplane/proxy — an `nft -f` script for
//! the `ip nanocloud-nat` table, a full declarative replace each change.
//!
//! nft shape: nat base chains at prerouting + output
//! (so both external and node-local traffic to a ClusterIP is caught) jump to
//! NCLD-SERVICES, which per service matches `ip daddr <clusterIP> tcp dport
//! <port>` and DNATs — a single backend directly, several via
//! `numgen random mod N map { i : <addr>:<targetport> }` (the nft
//! load-balancing form; kube-proxy uses iptables statistic-probability). A
//! service with no ready endpoints gets no rule (ClusterIP unreachable).
//!
//! NOTE: the exact nft LB token syntax is validated where the ruleset is
//! programmed; the E2E here proves the compiler's decision (which services,
//! backends and ports the ruleset names).

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
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");

// The control-plane store, via the standard fluxor storage contracts.
// storage.object (0x14): read services/endpoints, publish the
// compiled ruleset. storage.namespace (0x13): prefix LIST + change SUBSCRIBE
// (self-edge sink). Programming nft is a Linux dataplane concern, not a portable fluxor
// capability: the DECISION (compile the LB ruleset) is a pure store transform
// here, and the EFFECT (program nft) is a node-backend detail that consumes
// the published ruleset.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const SERVICES_PREFIX: &[u8] = b"/services/";
const ENDPOINTS_PREFIX: &[u8] = b"/endpoints/";
// The compiled node LB ruleset is published here; the node's network backend
// reads it and programs the dataplane (nft) — decision here, effect there.
const RULESET_KEY: &[u8] = b"/dataplane/proxy";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const RULESET: usize = 8192;
const SVC_SECTION: usize = 4096;
const MAP_BUF: usize = 1024;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// FNV of the last ruleset published — skip re-publish when unchanged.
    last_hash: u64,
    applies: u32,
}

// ---- storage.object / storage.namespace ops ----

/// Publish the compiled node LB ruleset to the store; the node's network backend
/// consumes `RULESET_KEY` and programs the dataplane. Decision here, effect there.
unsafe fn publish_ruleset(sys: &SyscallTable, ruleset: &[u8]) -> bool {
    put_value(sys, RULESET_KEY, ruleset)
}

// ---- parsing + helpers ----

/// FNV-1a (change detection). Named to avoid the SDK's own `fnv1a`.
fn svc_hash(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}
const FNV_SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// Write a u32 as decimal into `dst[at..]`; returns the new length.
fn write_u32(dst: &mut [u8], at: usize, mut n: u32) -> usize {
    if at >= dst.len() {
        return at;
    }
    if n == 0 {
        dst[at] = b'0';
        return at + 1;
    }
    let mut tmp = [0u8; 10];
    let mut i = 0;
    while n > 0 && i < tmp.len() {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    let mut p = at;
    while i > 0 && p < dst.len() {
        i -= 1;
        dst[p] = tmp[i];
        p += 1;
    }
    p
}

/// The `<ns>/<name>` tail after `prefix`.
fn tail_after<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    key.get(prefix.len()..)
}

/// Build the full nft NAT ruleset from services + their endpoints. Returns the
/// byte length written into `buf`.
unsafe fn render_ruleset(sys: &SyscallTable, buf: &mut [u8]) -> usize {
    let mut svcs = ListWalk::new(SERVICES_PREFIX);

    let mut rules = [0u8; SVC_SECTION];
    let mut rlen = 0usize;

    while let Some(sk) = svcs.next(sys) {
        let klen = sk.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut svckey = [0u8; MAX_KEY];
        svckey[..klen].copy_from_slice(sk);

        let mut svcval = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &svckey[..klen], &mut svcval) else {
            continue;
        };
        // A service without a ClusterIP is not proxied (headless / ExternalName).
        let Some(cip) = j_get2(&svcval[..vlen], b"spec", b"clusterIP") else {
            continue;
        };
        let Some(port) = j_get4(&svcval[..vlen], b"spec", b"ports", b"0", b"port") else {
            continue;
        };
        // targetPort falls back to port when unset.
        let tport = j_get4(&svcval[..vlen], b"spec", b"ports", b"0", b"targetPort").unwrap_or(port);

        // Backends from /endpoints/<same ns/name> — "<pod>=<addr>,...".
        let Some(tail) = tail_after(&svckey[..klen], SERVICES_PREFIX) else {
            continue;
        };
        let mut epkey = [0u8; MAX_KEY];
        let eklen = ENDPOINTS_PREFIX.len() + tail.len();
        if eklen > epkey.len() {
            continue;
        }
        epkey[..ENDPOINTS_PREFIX.len()].copy_from_slice(ENDPOINTS_PREFIX);
        epkey[ENDPOINTS_PREFIX.len()..eklen].copy_from_slice(tail);

        let mut epval = [0u8; MAX_VALUE];
        let eplen = match get_value(sys, &epkey[..eklen], &mut epval) {
            Some(n) => n,
            None => continue, // no endpoints doc → no backends
        };

        // Build the DNAT map entries + count backends.
        let mut map = [0u8; MAP_BUF];
        let mut mlen = 0usize;
        let mut n = 0u32;
        let ep = &epval[..eplen];
        let mut es = 0;
        while es <= ep.len() {
            let ee = ep[es..]
                .iter()
                .position(|&b| b == b',')
                .map(|i| es + i)
                .unwrap_or(ep.len());
            let seg = &ep[es..ee];
            // "<pod>=<addr>" → take the addr after '='.
            if let Some(eq) = seg.iter().position(|&b| b == b'=') {
                let addr = &seg[eq + 1..];
                if !addr.is_empty() {
                    if n > 0 {
                        mlen = append(&mut map, mlen, b", ");
                    }
                    mlen = write_u32(&mut map, mlen, n);
                    mlen = append(&mut map, mlen, b" : ");
                    mlen = append(&mut map, mlen, addr);
                    mlen = append(&mut map, mlen, b":");
                    mlen = append(&mut map, mlen, tport);
                    n += 1;
                }
            }
            if ee >= ep.len() {
                break;
            }
            es = ee + 1;
        }
        if n == 0 {
            continue; // no ready backends → ClusterIP unreachable, no rule
        }

        // One NCLD-SERVICES rule: match ClusterIP:port, DNAT to the backend(s).
        rlen = append(&mut rules, rlen, b"    ip daddr ");
        rlen = append(&mut rules, rlen, cip);
        rlen = append(&mut rules, rlen, b" tcp dport ");
        rlen = append(&mut rules, rlen, port);
        rlen = append(&mut rules, rlen, b" dnat to ");
        if n == 1 {
            // The map is exactly "0 : <addr>:<tport>"; the target follows the
            // 4-byte "0 : " prefix. Emit it directly (no load-balance wrapper).
            rlen = append(&mut rules, rlen, &map[4..mlen]);
        } else {
            rlen = append(&mut rules, rlen, b"numgen random mod ");
            rlen = write_u32(&mut rules, rlen, n);
            rlen = append(&mut rules, rlen, b" map { ");
            rlen = append(&mut rules, rlen, &map[..mlen]);
            rlen = append(&mut rules, rlen, b" }");
        }
        rlen = append(&mut rules, rlen, b"\n");
    }

    // Assemble the nat table: prerouting + output base chains → NCLD-SERVICES.
    let mut m = append(
        buf,
        0,
        b"flush table ip nanocloud-nat\ntable ip nanocloud-nat {\n",
    );
    m = append(buf, m, b"  chain NCLD-SVC-PRE {\n    type nat hook prerouting priority -100; policy accept;\n    jump NCLD-SERVICES\n  }\n");
    m = append(buf, m, b"  chain NCLD-SVC-OUT {\n    type nat hook output priority -100; policy accept;\n    jump NCLD-SERVICES\n  }\n");
    m = append(buf, m, b"  chain NCLD-SERVICES {\n");
    m = append(buf, m, &rules[..rlen]);
    m = append(buf, m, b"  }\n}\n");
    m
}

/// Recompute + apply the NAT ruleset if it changed. Returns 1 if applied.
unsafe fn reconcile(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut buf = [0u8; RULESET];
    let len = render_ruleset(sys, &mut buf);
    let hash = svc_hash(FNV_SEED, &buf[..len]);
    if hash == s.last_hash {
        return 0;
    }
    if publish_ruleset(sys, &buf[..len]) {
        s.last_hash = hash;
        1
    } else {
        0
    }
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
        s.last_hash = 0;
        s.applies = 0;
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
        // SUBSCRIBE services + endpoints onto it, then a cold-start compile+publish.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, SERVICES_PREFIX, s.sink, 0);
                store_subscribe(sys, ENDPOINTS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.applies = s.applies.wrapping_add(reconcile(s, sys));
            return 0;
        }

        // A pushed namespace.change (either prefix) → recompile + republish.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.applies = s.applies.wrapping_add(reconcile(s, sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
