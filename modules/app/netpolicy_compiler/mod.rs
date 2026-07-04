//! NetworkPolicy compiler — nanocloud's NetworkPolicy controller as a PIC
//! module — a *compiler* reconciler. It watches /networkpolicies/ and /pods/,
//! compiles the node's nftables ruleset, and publishes it to the store at
//! /dataplane/netpolicy; the node's network backend programs nft from there.
//! Decision here, effect there.
//!
//! Data model:
//!   /networkpolicies/<ns>/<name> = k8s NetworkPolicy JSON:
//!       spec.podSelector.matchLabels, spec.policyTypes[], and standard
//!       spec.ingress[]/egress[] rules (from[].ipBlock.cidr × ports[].port)
//!   /pods/<ns>/<name>            = k8s Pod JSON (metadata.labels, status.podIP/ready)
//! Output: the compiled ruleset at /dataplane/netpolicy — an `nft -f` script
//! for the `inet nanocloud` table, a full declarative replace each change.
//!
//! nft shape: base chain NCLD-NP (hook forward,
//! policy accept) jumps `ip daddr <pod_ip>` to a per-pod chain; the per-pod
//! chain has `ip saddr <cidr> <proto> dport <port> counter return` allow rules
//! aggregated across every policy that selects the pod, then `counter drop`.
//! A pod selected by no policy gets no chain (k8s: unrestricted).
//!
//! Chain names use an FNV-1a hash (SHA1 is impractical in no_std). This module
//! owns the whole ruleset, so a name need only be unique, stable and a valid
//! nft identifier.

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

fn find_sub(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > hay.len() {
        return false;
    }
    let mut i = 0;
    while i + needle.len() <= hay.len() {
        if &hay[i..i + needle.len()] == needle {
            return true;
        }
        i += 1;
    }
    false
}

// The control-plane store, via the standard fluxor storage contracts.
// storage.object (0x14): read policies/pods, publish the compiled
// ruleset. storage.namespace (0x13): prefix LIST + change SUBSCRIBE (self-edge
// sink). Programming nft is a Linux dataplane concern, not a portable fluxor
// capability: the DECISION (compile the ruleset) is a pure store transform
// here, and the EFFECT (program nft) is a node-backend detail that consumes
// the published ruleset out of band.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const NP_PREFIX: &[u8] = b"/networkpolicies/";
const PODS_PREFIX: &[u8] = b"/pods/";
// The compiled node ruleset is published here; the node's network backend reads
// it and programs the dataplane (nft) — decision here, effect there.
const RULESET_KEY: &[u8] = b"/dataplane/netpolicy";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const RULESET: usize = 8192;
const SECTION: usize = 4096;
const PODCHAIN: usize = 1024;

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

/// Publish the compiled node ruleset to the store; the node's network backend
/// consumes `RULESET_KEY` and programs the dataplane. Decision here, effect there.
unsafe fn publish_ruleset(sys: &SyscallTable, ruleset: &[u8]) -> bool {
    put_value(sys, RULESET_KEY, ruleset)
}

// ---- parsing + helpers ----

fn labels_contain(labels: &[u8], token: &[u8]) -> bool {
    let mut start = 0;
    while start <= labels.len() {
        let end = labels[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(labels.len());
        if &labels[start..end] == token {
            return true;
        }
        if end >= labels.len() {
            break;
        }
        start = end + 1;
    }
    false
}

/// Every selector token appears in the pod's labels. An EMPTY selector matches
/// ALL pods (a NetworkPolicy with `podSelector: {}` applies namespace-wide).
fn selector_matches(selector: &[u8], labels: &[u8]) -> bool {
    if selector.is_empty() {
        return true;
    }
    let mut start = 0;
    while start <= selector.len() {
        let end = selector[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(selector.len());
        if !labels_contain(labels, &selector[start..end]) {
            return false;
        }
        if end >= selector.len() {
            break;
        }
        start = end + 1;
    }
    true
}

/// FNV-1a over bytes (chain-name hashing + ruleset change detection).
fn nf_hash(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}
const FNV_SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// Write the low 48 bits of `h` as 12 lowercase hex chars.
fn write_hex12(dst: &mut [u8], at: usize, h: u64) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut p = at;
    for i in (0..12).rev() {
        let nibble = ((h >> (i * 4)) & 0xf) as usize;
        if p < dst.len() {
            dst[p] = HEX[nibble];
            p += 1;
        }
    }
    p
}

/// Render one nft allow line: `ip <match_kw> <cidr> <proto> dport <port> counter
/// return`. `match_kw` is `saddr` for ingress (allow FROM the peer) or `daddr`
/// for egress (allow TO the peer). Empty `cidr`/`proto`/`port` are omitted. The
/// k8s protocol (`TCP`/`UDP`) is lowercased for nft.
fn render_allow(
    dst: &mut [u8],
    at: usize,
    cidr: &[u8],
    proto: &[u8],
    port: &[u8],
    match_kw: &[u8],
) -> usize {
    let mut p = append(dst, at, b"      ");
    if !cidr.is_empty() {
        p = append(dst, p, b"ip ");
        p = append(dst, p, match_kw);
        p = append(dst, p, b" ");
        p = append(dst, p, cidr);
        p = append(dst, p, b" ");
    }
    if !proto.is_empty() {
        let mut lc = [0u8; 8];
        let n = proto.len().min(lc.len());
        for i in 0..n {
            lc[i] = proto[i].to_ascii_lowercase();
        }
        p = append(dst, p, &lc[..n]);
        if !port.is_empty() {
            p = append(dst, p, b" dport ");
            p = append(dst, p, port);
        }
        p = append(dst, p, b" ");
    }
    append(dst, p, b"counter return\n")
}

/// Walk a k8s NetworkPolicy `ingress[]`/`egress[]` array and append an nft allow
/// line for each `from[].ipBlock.cidr` (× each `ports[]`) within each rule. A
/// missing `from` allows any source; a missing `ports` allows any port. `peer`
/// is the key holding the peer list (`from` for ingress, `to` for egress).
fn render_allows(dst: &mut [u8], at: usize, rules: &[u8], peer: &[u8], match_kw: &[u8]) -> usize {
    let mut p = at;
    let mut ri = 0usize;
    while let Some(rule) = j_idx(rules, ri) {
        ri += 1;
        let from = j_sub1(rule, peer); // Option<array bytes>
        let ports = j_sub1(rule, b"ports");
        // Peers (cidr) crossed with ports; None on either side = "any".
        let mut fi = 0usize;
        loop {
            let cidr: &[u8] = match from {
                Some(f) => match j_idx(f, fi) {
                    Some(pobj) => j_get2(pobj, b"ipBlock", b"cidr").unwrap_or(b""),
                    None => break,
                },
                None => b"",
            };
            let mut pi = 0usize;
            loop {
                let (proto, port): (&[u8], &[u8]) = match ports {
                    Some(pp) => match j_idx(pp, pi) {
                        Some(po) => (
                            j_get1(po, b"protocol").unwrap_or(b"tcp"),
                            j_get1(po, b"port").unwrap_or(b""),
                        ),
                        None => break,
                    },
                    None => (b"", b""),
                };
                p = render_allow(dst, p, cidr, proto, port, match_kw);
                pi += 1;
                if ports.is_none() {
                    break;
                }
            }
            fi += 1;
            if from.is_none() {
                break;
            }
        }
    }
    p
}

/// Build the full nft ruleset from the current policies + pods. Returns the
/// byte length written into `buf`.
unsafe fn render_ruleset(sys: &SyscallTable, buf: &mut [u8]) -> usize {
    // Walk the pods; for each, walk the policies. The inner walk re-lists
    // per pod — LIST calls, not memory — and neither prefix has a ceiling.

    let mut jumps = [0u8; SECTION];
    let mut jlen = 0usize;
    let mut chains = [0u8; SECTION];
    let mut clen = 0usize;

    // For each pod, aggregate allow-rules from every policy that selects it.
    let mut pods = ListWalk::new(PODS_PREFIX);
    while let Some(pk) = pods.next(sys) {
        let klen = pk.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut podkey = [0u8; MAX_KEY];
        podkey[..klen].copy_from_slice(pk);

        let mut podval = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &podkey[..klen], &mut podval) else {
            continue;
        };
        let Some(ip) = j_get2(&podval[..vlen], b"status", b"podIP") else {
            continue;
        };
        let labels = j_sub2(&podval[..vlen], b"metadata", b"labels").unwrap_or(b"{}");

        // Aggregate ingress + egress allow rules across matching policies. A
        // policy governs a direction if its `types=` lists it (default:
        // ingress, so an ingress-only record still works).
        let mut ing = [0u8; PODCHAIN];
        let mut ilen = 0usize;
        let mut ing_gov = false;
        let mut eg = [0u8; PODCHAIN];
        let mut elen = 0usize;
        let mut eg_gov = false;

        let mut nps = ListWalk::new(NP_PREFIX);
        while let Some(nk) = nps.next(sys) {
            let nklen = nk.len();
            if nklen > MAX_KEY {
                continue;
            }
            let mut npkey = [0u8; MAX_KEY];
            npkey[..nklen].copy_from_slice(nk);

            let mut npval = [0u8; MAX_VALUE];
            let Some(nvlen) = get_value(sys, &npkey[..nklen], &mut npval) else {
                continue;
            };
            let sel =
                j_sub3(&npval[..nvlen], b"spec", b"podSelector", b"matchLabels").unwrap_or(b"{}");
            if !j_obj_subset(sel, labels) {
                continue;
            }
            let types =
                j_sub2(&npval[..nvlen], b"spec", b"policyTypes").unwrap_or(b"[\"Ingress\"]");
            if find_sub(types, b"Ingress") {
                ing_gov = true;
                if let Some(rules) = j_sub2(&npval[..nvlen], b"spec", b"ingress") {
                    ilen = render_allows(&mut ing, ilen, rules, b"from", b"saddr");
                }
            }
            if find_sub(types, b"Egress") {
                eg_gov = true;
                if let Some(rules) = j_sub2(&npval[..nvlen], b"spec", b"egress") {
                    elen = render_allows(&mut eg, elen, rules, b"to", b"daddr");
                }
            }
        }

        if !ing_gov && !eg_gov {
            continue; // pod selected by no policy → unrestricted
        }

        let h = nf_hash(FNV_SEED, &podkey[..klen]);

        // Ingress: `ip daddr <pod>` jumps to the pod's ingress chain (allow
        // FROM matched sources, then drop).
        if ing_gov {
            let mut cname = [0u8; 24];
            let mut cn = append(&mut cname, 0, b"NCLD-NPI");
            cn = write_hex12(&mut cname, cn, h);
            let cname = &cname[..cn];
            jlen = append(&mut jumps, jlen, b"    ip daddr ");
            jlen = append(&mut jumps, jlen, ip);
            jlen = append(&mut jumps, jlen, b" counter jump ");
            jlen = append(&mut jumps, jlen, cname);
            jlen = append(&mut jumps, jlen, b"\n");
            clen = append(&mut chains, clen, b"  chain ");
            clen = append(&mut chains, clen, cname);
            clen = append(&mut chains, clen, b" {\n");
            clen = append(&mut chains, clen, &ing[..ilen]);
            clen = append(&mut chains, clen, b"      counter drop\n  }\n");
        }

        // Egress: `ip saddr <pod>` jumps to the pod's egress chain (allow TO
        // matched destinations, then drop).
        if eg_gov {
            let mut cname = [0u8; 24];
            let mut cn = append(&mut cname, 0, b"NCLD-NPE");
            cn = write_hex12(&mut cname, cn, h);
            let cname = &cname[..cn];
            jlen = append(&mut jumps, jlen, b"    ip saddr ");
            jlen = append(&mut jumps, jlen, ip);
            jlen = append(&mut jumps, jlen, b" counter jump ");
            jlen = append(&mut jumps, jlen, cname);
            jlen = append(&mut jumps, jlen, b"\n");
            clen = append(&mut chains, clen, b"  chain ");
            clen = append(&mut chains, clen, cname);
            clen = append(&mut chains, clen, b" {\n");
            clen = append(&mut chains, clen, &eg[..elen]);
            clen = append(&mut chains, clen, b"      counter drop\n  }\n");
        }
    }

    // Assemble: header + base chain (policy accept + jumps) + per-pod chains.
    let mut n = append(
        buf,
        0,
        b"flush table inet nanocloud\ntable inet nanocloud {\n",
    );
    n = append(
        buf,
        n,
        b"  chain NCLD-NP {\n    type filter hook forward priority 0; policy accept;\n",
    );
    n = append(buf, n, &jumps[..jlen]);
    n = append(buf, n, b"  }\n");
    n = append(buf, n, &chains[..clen]);
    n = append(buf, n, b"}\n");
    n
}

/// Recompute + apply the ruleset if it changed. Returns 1 if applied, else 0.
unsafe fn reconcile(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut buf = [0u8; RULESET];
    let len = render_ruleset(sys, &mut buf);
    let hash = nf_hash(FNV_SEED, &buf[..len]);
    if hash == s.last_hash {
        return 0; // no change since last publish
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
        // SUBSCRIBE policies + pods onto it, then a cold-start compile+publish.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, NP_PREFIX, s.sink, 0);
                store_subscribe(sys, PODS_PREFIX, s.sink, 0);
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
