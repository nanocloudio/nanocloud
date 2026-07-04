//! API responder — the minimal API-plane app module. It carries the shape every
//! API module shares: *a Kubernetes API call is a pure transform — request in,
//! store reads/writes, response out* — reached over the store's
//! request/response seam (the same seam sandbox exec uses).
//!
//! `api_ingress` writes a request and polls for the response; this module
//! handles the op and writes the response back, and the front retires both
//! keys once it has the answer.
//!
//! Data model:
//!   /api-req/<reqid>  = "<op>[:<arg>]"   e.g. "healthz" or "count:/pods/"
//!   /api-resp/<reqid> = "<status>;<body>" e.g. "200;ok" or "200;3"
//!
//! Ops (v1): `healthz` → 200 ok (the trivial case); `count:<prefix>` → 200 with
//! the number of live keys under <prefix> (a real store transform — it LISTs
//! the store); anything else → 404. The point is the shape, not the op set.

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

const REQ_PREFIX: &[u8] = b"/api-req/";
const RESP_PREFIX: &[u8] = b"/api-resp/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 256;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start service pass have run.
    subscribed: u8,
    responses: u32,
}

// ---- store ops ----

/// Live key count under `prefix` (LIST then count).
unsafe fn live_count(sys: &SyscallTable, prefix: &[u8]) -> u32 {
    list_count(sys, prefix) as u32
}

// ---- helpers ----

fn tail_after<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    key.get(prefix.len()..)
}

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

/// Render a compact `k=v;k=v` record as a JSON object `{"k":"v",...}` into
/// `dst`; returns the length. v1 does not escape special characters (the
/// control-plane records are simple identifiers / addresses); a full escaper is
/// a follow-up. Bounded — oversized records truncate rather than overflow.
fn json_from_compact(record: &[u8], dst: &mut [u8]) -> usize {
    let mut p = append(dst, 0, b"{");
    let mut first = true;
    let mut start = 0;
    while start <= record.len() {
        let end = record[start..]
            .iter()
            .position(|&b| b == b';')
            .map(|i| start + i)
            .unwrap_or(record.len());
        let seg = &record[start..end];
        if let Some(eq) = seg.iter().position(|&b| b == b'=') {
            if !first {
                p = append(dst, p, b",");
            }
            first = false;
            p = append(dst, p, b"\"");
            p = append(dst, p, &seg[..eq]);
            p = append(dst, p, b"\":\"");
            p = append(dst, p, &seg[eq + 1..]);
            p = append(dst, p, b"\"");
        }
        if end >= record.len() {
            break;
        }
        start = end + 1;
    }
    append(dst, p, b"}")
}

/// Compute the response `<status>;<body>` for a request op. `count:<prefix>` and
/// `getjson:<key>` read the store (the transforms); `healthz` is the trivial
/// case. `getjson` returns the compact record at <key> as a JSON object — the
/// essence: request → store read → structured response.
unsafe fn handle_op(sys: &SyscallTable, op: &[u8], doc: &mut [u8]) -> usize {
    if op == b"healthz" {
        return append(doc, 0, b"200;ok");
    }
    if op.len() > 6 && &op[..6] == b"count:" {
        let prefix = &op[6..];
        let n = live_count(sys, prefix);
        let d = append(doc, 0, b"200;");
        return write_u32(doc, d, n);
    }
    if op.len() > 8 && &op[..8] == b"getjson:" {
        let key = &op[8..];
        let mut valbuf = [0u8; MAX_VALUE];
        return match get_value(sys, key, &mut valbuf) {
            Some(n) => {
                let mut json = [0u8; MAX_VALUE];
                let jlen = json_from_compact(&valbuf[..n], &mut json);
                let d = append(doc, 0, b"200;");
                append(doc, d, &json[..jlen])
            }
            None => append(doc, 0, b"404;{}"),
        };
    }
    append(doc, 0, b"404;unknown op")
}

/// Service every /api-req/<reqid> that has no response yet.
unsafe fn reconcile_requests(sys: &SyscallTable) -> u32 {
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

        // Response key: swap the prefix.
        if klen <= REQ_PREFIX.len() {
            continue;
        }
        let tail = &key[REQ_PREFIX.len()..];
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);

        // Already answered? (front hasn't deleted it yet.)
        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        let mut opbuf = [0u8; MAX_VALUE];
        let Some(olen) = get_value(sys, key, &mut opbuf) else {
            continue;
        };

        let mut doc = [0u8; MAX_VALUE];
        let dlen = handle_op(sys, &opbuf[..olen], &mut doc);
        if put_value(sys, &rkey[..rlen], &doc[..dlen]) {
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
        s.responses = 0;
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
        // SUBSCRIBE the request prefix onto it, then service pre-existing requests.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.responses = s.responses.wrapping_add(reconcile_requests(sys));
            return 0;
        }

        // A pushed namespace.change means the request set moved — re-service.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.responses = s.responses.wrapping_add(reconcile_requests(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
