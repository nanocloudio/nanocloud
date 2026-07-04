//! Kubernetes request decoder — wave HttpRequest → Chronicle record frame.
//!
//! See `manifest.toml` for the wire layouts and the rationale. The short version:
//! Chronicle's VM cannot split a variable-arity path (no iteration, by
//! construction) and its record frame is flat, so something must project a
//! Kubernetes request into decided-on fields before a `decision` can route it.
//! That something is a domain module composed as a graph node — not a new VM
//! capability.
//!
//! This module decides NOTHING. It reshapes bytes. The routing, authorization
//! and admission meaning stay in Chronicle params.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/kube_path.rs");

/// wave HttpRequest fixed head: conn_id, stream_id, method, flags, then the
/// three section lengths.
const REQ_HEAD: usize = 2 + 2 + 1 + 1 + 2 + 2 + 2;

/// Chronicle record-frame value types (`pipeline_core.rs`).
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const BUF: usize = 8192;
const DEFAULT_MAX_BODY: u32 = 4096;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    req_in: i32,
    record_out: i32,
    max_body: u32,
    /// Requests seen; requests emitted; requests dropped as unroutable.
    seen: u32,
    emitted: u32,
    dropped: u32,
    in_buf: [u8; BUF],
    out_buf: [u8; BUF],
}

mod params_def {
    use super::p_u32;
    use super::State;
    use super::DEFAULT_MAX_BODY;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, max_body, u32, DEFAULT_MAX_BODY
            => |s, d, len| { s.max_body = p_u32(d, len, 0, DEFAULT_MAX_BODY); };
    }
}

/// Append one Chronicle frame field. Returns the new offset, or `None` when the
/// field would not fit — a truncated frame is never emitted.
fn put_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    if payload.len() > u16::MAX as usize || at + 4 + payload.len() > out.len() {
        return None;
    }
    out[at] = number;
    out[at + 1] = ty;
    out[at + 2..at + 4].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    out[at + 4..at + 4 + payload.len()].copy_from_slice(payload);
    Some(at + 4 + payload.len())
}

fn put_i64(out: &mut [u8], at: usize, number: u8, v: i64) -> Option<usize> {
    put_field(out, at, number, TY_I64, &v.to_le_bytes())
}

/// Project one wave HttpRequest envelope into a Chronicle record frame.
/// Returns the frame length in `out`, or 0 when the request is not a Kubernetes
/// resource path (discovery documents, health probes — the graph does not route
/// them and a frame claiming empty fields would be a lie).
fn project(req: &[u8], out: &mut [u8], max_body: u32) -> usize {
    if req.len() < REQ_HEAD {
        return 0;
    }
    // The four correlation bytes are carried WHOLE and never re-split: under
    // HTTP/2 `stream_id` distinguishes parallel streams on one connection, and
    // under HTTP/1.1 it is the request generation that stops a late answer
    // matching the next holder of a recycled conn_id.
    let wave_request_id = u32::from_le_bytes(req[0..4].try_into().unwrap());
    let method = req[4];
    // Offsets: conn_id 0..2, stream_id 2..4, method 4, flags 5, then the three
    // section lengths at 6, 8, 10 — the head is 12 bytes, not 14.
    let path_len = u16::from_le_bytes(req[6..8].try_into().unwrap()) as usize;
    let hdr_len = u16::from_le_bytes(req[8..10].try_into().unwrap()) as usize;
    let body_len = u16::from_le_bytes(req[10..12].try_into().unwrap()) as usize;

    let path_at = REQ_HEAD;
    let body_at = path_at + path_len + hdr_len;
    if body_at + body_len > req.len() {
        return 0; // truncated envelope — refuse rather than project garbage
    }
    let path = &req[path_at..path_at + path_len];
    let body = &req[body_at..body_at + body_len];

    let Some((resource, namespace, name)) = parse_rest_path(path) else {
        return 0;
    };

    let capped = body.len().min(max_body as usize);

    let mut p = 1usize; // [0] is the field count, filled in last
    let mut n = 0u8;
    macro_rules! field {
        ($e:expr) => {
            match $e {
                Some(v) => {
                    p = v;
                    n += 1;
                }
                None => return 0,
            }
        };
    }
    field!(put_i64(out, p, 1, wave_request_id as i64));
    field!(put_i64(out, p, 2, method as i64));
    field!(put_field(out, p, 3, TY_BYTES, resource));
    field!(put_field(out, p, 4, TY_BYTES, namespace));
    field!(put_field(out, p, 5, TY_BYTES, name));
    field!(put_field(out, p, 6, TY_BYTES, &body[..capped]));

    // Field 7: the STORE KEY, `/<resource>/<ns>/<name>`.
    //
    // Assembled here because the Chronicle VM cannot build it: `ADD` is
    // integer-only, there is no string-concatenation builtin, and adding one
    // would be a new VM opcode rather than a new artefact. The key convention
    // is nanocloud's
    // anyway, so nanocloud owns it, and a decision downstream just forwards the
    // field. A collection request (no name) yields the prefix, which is the
    // right key for a LIST and harmless for a GET that will simply miss.
    let mut key = [0u8; 192];
    let mut k = 0usize;
    let mut push = |b: &[u8], k: &mut usize| {
        let n = b.len().min(key_cap(*k));
        key[*k..*k + n].copy_from_slice(&b[..n]);
        *k += n;
    };
    fn key_cap(at: usize) -> usize {
        192usize.saturating_sub(at)
    }
    push(b"/", &mut k);
    push(resource, &mut k);
    push(b"/", &mut k);
    if !namespace.is_empty() {
        push(namespace, &mut k);
        push(b"/", &mut k);
    }
    push(name, &mut k);
    field!(put_field(out, p, 7, TY_BYTES, &key[..k]));

    out[0] = n;
    p
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
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.req_in = in_chan;
        s.record_out = out_chan;
        s.seen = 0;
        s.emitted = 0;
        s.dropped = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.max_body == 0 {
            s.max_body = DEFAULT_MAX_BODY;
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
        if s.syscalls.is_null() || s.req_in < 0 || s.record_out < 0 {
            return 0;
        }
        let sys = &*s.syscalls;

        let poll = (sys.channel_poll)(s.req_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return 0;
        }
        let n = (sys.channel_read)(s.req_in, s.in_buf.as_mut_ptr(), BUF);
        if n <= 0 {
            return 0;
        }
        s.seen = s.seen.wrapping_add(1);

        let req = core::slice::from_raw_parts(s.in_buf.as_ptr(), n as usize);
        let mut out = [0u8; BUF];
        let flen = project(req, &mut out, s.max_body);
        if flen == 0 {
            // Not a resource path, or an envelope we will not vouch for. Drop it
            // rather than emit a frame with empty fields a decision would then
            // route on.
            s.dropped = s.dropped.wrapping_add(1);
            return 0;
        }
        s.out_buf[..flen].copy_from_slice(&out[..flen]);
        let wrote = (sys.channel_write)(s.record_out, s.out_buf.as_ptr(), flen);
        if wrote > 0 {
            s.emitted = s.emitted.wrapping_add(1);
        }
        // Return 0, ALWAYS. A non-zero `module_step` return means DONE: the
        // scheduler stops stepping the module. Returning 1 to mean "I did
        // work" retires it after its first record, which presents as a
        // downstream stall rather than as the module being finished.
        0
    }
}
