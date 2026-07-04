//! core_api — core/v1 CRUD as a PIC module (the last link of the API write
//! path: … → admission → core_api). A Kubernetes API handler is a pure
//! transform — request in, store read/CAS-write, response out. No host facts.
//!
//! Data model (request/response over the store seam):
//!   /core-req/<reqid>        = "verb=<get|list|create|update|delete>;resource=<r>;ns=<ns>;name=<n>;obj=<obj>"
//!   /<resource>/<ns>/<name>  = <the object — real nested k8s JSON, stored verbatim>
//!   /core-resp/<reqid>       = "<status>;<body>"
//!
//! Verbs:
//!   get    → 200;<obj>            | 404;not found
//!   list   → 200;<name>,<name>    (names under /<resource>/<ns>/)
//!   create → 201;<obj>            | 409;already exists
//!   update → 200;<obj>            | 404;not found
//!   delete → 200;deleted          | 404;not found

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

const REQ_PREFIX: &[u8] = b"/core-req/";
const RESP_PREFIX: &[u8] = b"/core-resp/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 4096;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start service pass have run.
    subscribed: u8,
    ops: u32,
}

// ---- helpers ----

/// Everything after `marker` in `req` (the trailing object payload, which may
/// itself contain `;` as JSON). Empty if the marker is absent.
fn rest_after<'a>(req: &'a [u8], marker: &[u8]) -> &'a [u8] {
    let mut i = 0;
    while i + marker.len() <= req.len() {
        if &req[i..i + marker.len()] == marker {
            return &req[i + marker.len()..];
        }
        i += 1;
    }
    &req[req.len()..]
}

/// Build `/<resource>/<ns>/<name>` (or the prefix, name empty → trailing slash).
fn object_key(dst: &mut [u8], resource: &[u8], ns: &[u8], name: &[u8]) -> usize {
    let mut p = append(dst, 0, b"/");
    p = append(dst, p, resource);
    p = append(dst, p, b"/");
    p = append(dst, p, ns);
    p = append(dst, p, b"/");
    append(dst, p, name)
}

/// Run one CRUD request into `doc`; returns its length.
unsafe fn handle(sys: &SyscallTable, req: &[u8], doc: &mut [u8]) -> usize {
    let verb = field(req, b"verb=").unwrap_or(b"");
    let resource = field(req, b"resource=").unwrap_or(b"");
    let ns = field(req, b"ns=").unwrap_or(b"default");
    let name = field(req, b"name=").unwrap_or(b"");
    // The object is the trailing field; read everything after `;obj=` so a
    // JSON object (which may contain `;`) survives the op encoding intact.
    let obj = rest_after(req, b";obj=");

    // LIST: names under /<resource>/<ns>/.
    if verb == b"list" {
        let mut pfx = [0u8; MAX_KEY];
        let pl = object_key(&mut pfx, resource, ns, b"");
        let mut walk = ListWalk::new(&pfx[..pl]);
        let mut d = append(doc, 0, b"200;");
        let mut first = true;
        while let Some(entry) = walk.next(sys) {
            let nm = last_seg(entry);
            if !first {
                d = append(doc, d, b",");
            }
            first = false;
            // append needs a fresh borrow; copy nm out first.
            let mut nbuf = [0u8; MAX_KEY];
            let nl = nm.len().min(MAX_KEY);
            nbuf[..nl].copy_from_slice(&nm[..nl]);
            d = append(doc, d, &nbuf[..nl]);
        }
        return d;
    }

    // The object key for the single-object verbs.
    let mut okey = [0u8; MAX_KEY];
    let okl = object_key(&mut okey, resource, ns, name);
    let mut cur = [0u8; MAX_VALUE];
    let present = get_value(sys, &okey[..okl], &mut cur);

    match verb {
        b"get" => match present {
            Some(n) => {
                let mut vbuf = [0u8; MAX_VALUE];
                vbuf[..n].copy_from_slice(&cur[..n]);
                let d = append(doc, 0, b"200;");
                append(doc, d, &vbuf[..n])
            }
            None => append(doc, 0, b"404;not found"),
        },
        b"create" => {
            if present.is_some() {
                append(doc, 0, b"409;already exists")
            } else if put_value(sys, &okey[..okl], obj) {
                let d = append(doc, 0, b"201;");
                append(doc, d, obj)
            } else {
                append(doc, 0, b"500;write failed")
            }
        }
        b"update" => {
            if present.is_none() {
                append(doc, 0, b"404;not found")
            } else if put_value(sys, &okey[..okl], obj) {
                let d = append(doc, 0, b"200;");
                append(doc, d, obj)
            } else {
                append(doc, 0, b"500;write failed")
            }
        }
        b"delete" => {
            if present.is_none() {
                append(doc, 0, b"404;not found")
            } else if delete_value(sys, &okey[..okl]) {
                append(doc, 0, b"200;deleted")
            } else {
                append(doc, 0, b"500;delete failed")
            }
        }
        _ => append(doc, 0, b"400;bad verb"),
    }
}

/// Service every /core-req/ without a response yet.
unsafe fn reconcile(sys: &SyscallTable) -> u32 {
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
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);

        // The probe must hold ANY response — a too-small buffer makes get_value
        // return None for a large existing response, so the guard would think
        // this request is unanswered and re-run it. core_api mutates the store,
        // so re-processing against changed state gives a wrong result.
        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        let mut req = [0u8; MAX_VALUE];
        let Some(rqlen) = get_value(sys, key, &mut req) else {
            continue;
        };

        let mut doc = [0u8; MAX_VALUE];
        let dlen = handle(sys, &req[..rqlen], &mut doc);
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
        s.ops = 0;
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
            s.ops = s.ops.wrapping_add(reconcile(sys));
            return 0;
        }

        // A pushed namespace.change means the request set moved — re-service.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.ops = s.ops.wrapping_add(reconcile(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
