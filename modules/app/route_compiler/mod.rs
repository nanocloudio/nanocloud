//! Route compiler — nanocloud's edge-route compiler as a PIC module — a
//! *compiler* reconciler, sibling of `proxy_compiler`. It watches /routes/ (+
//! /route-status/ for the validated gate) and /endpoints/, resolves each
//! Route's service to its ready backends, and emits ONE KEY PER ROUTE carrying
//! the complete backend set to the `/dataplane/edge/` prefix. The fluxor `http`
//! edge SUBSCRIBEs that prefix via the table_consumer helper and populates its
//! DynRoute table.
//!
//! Data model (JSON Route objects in, compact edge rows out):
//!   /routes/<ns>/<name>       = { "spec": { "host":…, "to":{"name":…},
//!                                            "port":…, "path":… } }   (JSON)
//!   /route-status/<ns>/<name> = "ready=1;endpoint=<s>:<p>" | "ready=0;…"
//!   /endpoints/<ns>/<svc>     = "<pod>=<addr>,<pod>=<addr>"           (compact)
//!   /dataplane/edge/<ns>/<name> =
//!       host=<h>;path=<prefix>;be=<ip>:<port>:<w>:<r>,<ip>:<port>:<w>:<r>,…
//!
//! Rules:
//!   * ONE key per route carrying the full backend set — a backend-set
//!     change is a single atomic PUT (replace), a route removal a single
//!     DELETE. Deliberately NOT per-(route×backend) rows.
//!   * Compile only route_validator-validated routes (ready=1). An
//!     unready or removed route's edge key is DELETEd.
//!   * Backend set bounded by the edge's arena; truncate by weight order
//!     and report the count on the compiler's own status key (the
//!     compiler IS a writer, unlike the edge).
//!   * Single-writer discipline: route_compiler owns `/dataplane/edge/`
//!     (and its own `/route-compiler-status`) and nothing else.

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

// The control-plane store via the standard fluxor storage contracts.
// storage.object (0x14): read routes/status/endpoints, publish edge rows.
// storage.namespace (0x13): prefix LIST + change SUBSCRIBE (self-edge sink).
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const ROUTES_PREFIX: &[u8] = b"/routes/";
const ROUTE_STATUS_PREFIX: &[u8] = b"/route-status/";
const ENDPOINTS_PREFIX: &[u8] = b"/endpoints/";
// The compiled edge routes — one key per route. The `http`-module edge
// consumes this prefix (`routes_prefix`).
const EDGE_PREFIX: &[u8] = b"/dataplane/edge/";
// Desired dynamic listeners: one object per
// listener, keyed by name, value `port=<p>;tls=<0|1>`. The compiler
// resolves each into an edge-listener row the http anchor binds mid-life.
const LISTENERS_PREFIX: &[u8] = b"/listeners/";
// Compiled edge listeners — one key per bound port. The `http`-module edge
// consumes this prefix (`listeners_prefix`) and mid-life-binds each port
// from its pre-leased pool.
const EDGE_LISTENERS_PREFIX: &[u8] = b"/dataplane/edge-listeners/";
// Truncation report — the compiler's own status key (outside
// /dataplane/edge/ so it never looks like a route to the edge).
const STATUS_KEY: &[u8] = b"/route-compiler-status";

// Backend-set cap. Matches the http host profile's MAX_ROUTE_BACKENDS
// (modules/sdk/abi/config.rs::http) — the edge arena that consumes these
// rows. Excess backends are truncated by weight order and counted.
const EDGE_MAX_BACKENDS: usize = 8;

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const MAX_STATUS: usize = 256;
const MAX_EDGE_VALUE: usize = 1024;
const LIST_BUF: usize = 4096;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// Count of edge rows (re)written — the observable of progress.
    writes: u32,
    /// Cumulative backends truncated across compiles.
    truncated: u32,
}

// ---- storage.object / storage.namespace ops ----

// ---- parsing + helpers ----

fn tail_after<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    key.get(prefix.len()..)
}

/// The namespace tail (`<ns>` before the first '/') of a `<ns>/<name>` tail.
fn ns_of(tail: &[u8]) -> &[u8] {
    match tail.iter().position(|&b| b == b'/') {
        Some(i) => &tail[..i],
        None => tail,
    }
}

/// Field lookup in a compact `;`/`=`-record (the route-status format).
fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';')
            .map(|i| start + i)
            .unwrap_or(value.len());
        let seg = &value[start..end];
        if seg.len() >= tag.len() && &seg[..tag.len()] == tag {
            return Some(&seg[tag.len()..]);
        }
        if end >= value.len() {
            break;
        }
        start = end + 1;
    }
    None
}

/// True iff `/route-status/<ns>/<name>` says `ready=1` — the
/// route_validator admission gate.
unsafe fn route_ready(sys: &SyscallTable, tail: &[u8]) -> bool {
    let mut skey = [0u8; MAX_KEY];
    let sklen = ROUTE_STATUS_PREFIX.len() + tail.len();
    if sklen > skey.len() {
        return false;
    }
    skey[..ROUTE_STATUS_PREFIX.len()].copy_from_slice(ROUTE_STATUS_PREFIX);
    skey[ROUTE_STATUS_PREFIX.len()..sklen].copy_from_slice(tail);
    let mut sval = [0u8; MAX_STATUS];
    match get_value(sys, &skey[..sklen], &mut sval) {
        Some(n) => field(&sval[..n], b"ready=")
            .map(|v| v.first() == Some(&b'1'))
            .unwrap_or(false),
        None => false,
    }
}

/// Compile one validated route into its `/dataplane/edge/` value.
/// Returns `(edge_value_len, truncated_backends)`, or `None` when the
/// route can't be resolved (missing spec fields → nothing to publish).
unsafe fn render_route(sys: &SyscallTable, tail: &[u8], out: &mut [u8]) -> Option<(usize, u32)> {
    // Route JSON.
    let mut rkey = [0u8; MAX_KEY];
    let rklen = ROUTES_PREFIX.len() + tail.len();
    if rklen > rkey.len() {
        return None;
    }
    rkey[..ROUTES_PREFIX.len()].copy_from_slice(ROUTES_PREFIX);
    rkey[ROUTES_PREFIX.len()..rklen].copy_from_slice(tail);
    let mut rval = [0u8; MAX_VALUE];
    let rlen = get_value(sys, &rkey[..rklen], &mut rval)?;

    let host = j_get2(&rval[..rlen], b"spec", b"host")?;
    let service = j_get3(&rval[..rlen], b"spec", b"to", b"name")?;
    let port = j_get2(&rval[..rlen], b"spec", b"port")?;
    let path = j_get2(&rval[..rlen], b"spec", b"path").unwrap_or(b"/");

    // Endpoints for this route's service: /endpoints/<ns>/<service>.
    let ns = ns_of(tail);
    let mut ekey = [0u8; MAX_KEY];
    let eklen = ENDPOINTS_PREFIX.len() + ns.len() + 1 + service.len();
    if eklen > ekey.len() {
        return None;
    }
    let mut ep = 0;
    ep = append(&mut ekey, ep, ENDPOINTS_PREFIX);
    ep = append(&mut ekey, ep, ns);
    ep = append(&mut ekey, ep, b"/");
    ep = append(&mut ekey, ep, service);
    let _ = ep;
    let mut eval = [0u8; MAX_VALUE];
    // No endpoints doc → a route with zero backends (still published so
    // the edge sees the host/path with an empty set, not a stale row).
    let elen = get_value(sys, &ekey[..eklen], &mut eval).unwrap_or(0);

    // host=<h>;path=<prefix>;be=<ip>:<port>:1:1,...
    let mut w = 0usize;
    w = append(out, w, b"host=");
    w = append(out, w, host);
    w = append(out, w, b";path=");
    w = append(out, w, path);
    w = append(out, w, b";be=");

    let mut n_be = 0usize;
    let mut truncated = 0u32;
    let ep_doc = &eval[..elen];
    let mut es = 0usize;
    while es <= ep_doc.len() && elen > 0 {
        let ee = ep_doc[es..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| es + i)
            .unwrap_or(ep_doc.len());
        let seg = &ep_doc[es..ee];
        // "<pod>=<addr>" → the addr after '='.
        if let Some(eq) = seg.iter().position(|&b| b == b'=') {
            let addr = &seg[eq + 1..];
            if !addr.is_empty() {
                if n_be >= EDGE_MAX_BACKENDS {
                    // Truncate by weight order (all weight 1 here → drop
                    // the tail) and count the drop.
                    truncated += 1;
                } else {
                    if n_be > 0 {
                        w = append(out, w, b",");
                    }
                    w = append(out, w, addr);
                    w = append(out, w, b":");
                    w = append(out, w, port);
                    w = append(out, w, b":1:1"); // weight=1, ready=1
                    n_be += 1;
                }
            }
        }
        if ee >= ep_doc.len() {
            break;
        }
        es = ee + 1;
    }
    Some((w, truncated))
}

/// Full reconcile: (re)publish every validated route's edge row (PUT
/// only on change), then DELETE edge rows whose route was removed or is
/// no longer ready. Returns the count of rows (re)written.
unsafe fn reconcile(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut wrote = 0u32;
    let mut truncated_total = 0u32;

    // ── Compile pass: validated routes → edge rows ──
    let mut walk = ListWalk::new(ROUTES_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        let Some(tail) = tail_after(key, ROUTES_PREFIX) else {
            continue;
        };
        // Admission gate: compile only route_validator-validated routes.
        if !route_ready(sys, tail) {
            continue;
        }
        let mut edge_val = [0u8; MAX_EDGE_VALUE];
        let Some((vlen, trunc)) = render_route(sys, tail, &mut edge_val) else {
            continue;
        };
        truncated_total += trunc;

        // Edge key /dataplane/edge/<ns>/<name>; PUT only when changed
        // (atomic replace of the whole backend set —).
        let mut ekey = [0u8; MAX_KEY];
        let eklen = EDGE_PREFIX.len() + tail.len();
        if eklen > ekey.len() {
            continue;
        }
        ekey[..EDGE_PREFIX.len()].copy_from_slice(EDGE_PREFIX);
        ekey[EDGE_PREFIX.len()..eklen].copy_from_slice(tail);

        let mut cur = [0u8; MAX_EDGE_VALUE];
        if let Some(clen) = get_value(sys, &ekey[..eklen], &mut cur) {
            if cur[..clen] == edge_val[..vlen] {
                continue; // unchanged
            }
        }
        if put_value(sys, &ekey[..eklen], &edge_val[..vlen]) {
            wrote += 1;
        }
    }

    // ── Deletion pass: edge rows without a live, ready route ──
    let mut walk = ListWalk::new(EDGE_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        let Some(tail) = tail_after(key, EDGE_PREFIX) else {
            continue;
        };
        if !route_ready(sys, tail) {
            // Route removed or no longer validated → drop its edge row.
            if delete_value(sys, key) {
                wrote += 1;
            }
        }
    }

    // ── Truncation status (the compiler IS a writer —) ──
    if truncated_total != s.truncated {
        s.truncated = truncated_total;
        let mut doc = [0u8; 64];
        let mut d = append(&mut doc, 0, b"truncated=");
        d = write_u32(&mut doc, d, truncated_total);
        put_value(sys, STATUS_KEY, &doc[..d]);
    }

    // ── Listener compile ──
    wrote += reconcile_listeners(sys);

    wrote
}

/// Max desired listeners tracked in one reconcile pass — the edge's
/// pooled-listener cap (http `MAX_DYN_LISTENERS`). Excess is ignored.
const MAX_LISTENERS: usize = 8;
const PORT_STR_MAX: usize = 5;

/// Compile desired `/listeners/<name>` objects (`port=<p>;tls=<0|1>`) into
/// `/dataplane/edge-listeners/<port> = proto=tcp;tls=<0|1>` rows the http
/// anchor binds mid-life, then DELETE edge-listener rows whose desired
/// object was removed. Single-writer discipline: the compiler owns the
/// `/dataplane/edge-listeners/` prefix.
unsafe fn reconcile_listeners(sys: &SyscallTable) -> u32 {
    let mut wrote = 0u32;
    // Desired ports seen this pass (for the deletion sweep).
    let mut desired: [[u8; PORT_STR_MAX]; MAX_LISTENERS] = [[0u8; PORT_STR_MAX]; MAX_LISTENERS];
    let mut desired_len = [0usize; MAX_LISTENERS];
    let mut n_desired = 0usize;

    // ── Compile pass: desired listeners → edge-listener rows ──
    let mut walk = ListWalk::new(LISTENERS_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        // Desired object value: `port=<p>;tls=<0|1>`.
        let mut oval = [0u8; MAX_STATUS];
        let Some(olen) = get_value(sys, key, &mut oval) else {
            continue;
        };
        let Some(port) = field(&oval[..olen], b"port=") else {
            continue;
        };
        // Trim the port to a bare digit run (defensive vs. trailing bytes).
        let port = {
            let end = port
                .iter()
                .position(|&b| !b.is_ascii_digit())
                .unwrap_or(port.len());
            &port[..end]
        };
        if port.is_empty() || port.len() > PORT_STR_MAX {
            continue;
        }
        let tls = field(&oval[..olen], b"tls=")
            .map(|v| v.first() == Some(&b'1'))
            .unwrap_or(false);

        // Record the desired port for the deletion sweep.
        if n_desired < MAX_LISTENERS {
            desired[n_desired][..port.len()].copy_from_slice(port);
            desired_len[n_desired] = port.len();
            n_desired += 1;
        }

        // Edge key /dataplane/edge-listeners/<port>; PUT only when changed.
        let mut ekey = [0u8; MAX_KEY];
        let eklen = EDGE_LISTENERS_PREFIX.len() + port.len();
        if eklen > ekey.len() {
            continue;
        }
        ekey[..EDGE_LISTENERS_PREFIX.len()].copy_from_slice(EDGE_LISTENERS_PREFIX);
        ekey[EDGE_LISTENERS_PREFIX.len()..eklen].copy_from_slice(port);

        let mut eval = [0u8; 32];
        let mut w = append(&mut eval, 0, b"proto=tcp;tls=");
        eval[w] = if tls { b'1' } else { b'0' };
        w += 1;

        let mut cur = [0u8; 32];
        if let Some(clen) = get_value(sys, &ekey[..eklen], &mut cur) {
            if cur[..clen] == eval[..w] {
                continue; // unchanged
            }
        }
        if put_value(sys, &ekey[..eklen], &eval[..w]) {
            wrote += 1;
        }
    }

    // ── Deletion pass: edge-listener rows without a desired object ──
    let mut walk = ListWalk::new(EDGE_LISTENERS_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        let Some(tail) = tail_after(key, EDGE_LISTENERS_PREFIX) else {
            continue;
        };
        let mut present = false;
        for i in 0..n_desired {
            if desired[i][..desired_len[i]] == *tail {
                present = true;
                break;
            }
        }
        if !present && delete_value(sys, key) {
            wrote += 1;
        }
    }

    wrote
}

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
        s.truncated = 0;
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
        // SUBSCRIBE routes + route-status + endpoints onto it, then a
        // cold-start compile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, ROUTES_PREFIX, s.sink, 0);
                store_subscribe(sys, ROUTE_STATUS_PREFIX, s.sink, 0);
                store_subscribe(sys, ENDPOINTS_PREFIX, s.sink, 0);
                // Dynamic listeners.
                store_subscribe(sys, LISTENERS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.writes = s.writes.wrapping_add(reconcile(s, sys));
            return 0;
        }

        // A pushed namespace.change (any watched prefix) → recompile.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.writes = s.writes.wrapping_add(reconcile(s, sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
