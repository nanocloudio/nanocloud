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
include!("../_shared/json.rs");

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
    /// Emit a frame for a path that is not a Kubernetes resource path.
    passthrough: u32,
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

        2, passthrough, u32, 0
            => |s, d, len| { s.passthrough = p_u32(d, len, 0, 0); };
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

/// Is `name=true` (or `name=1`) present in a `&`-separated query string?
///
/// Kubernetes spells a boolean query parameter `?watch=true`; `?watch=1` is
/// accepted the same way, and a bare `?watch` is NOT — an explicit value is
/// what the API takes, and inventing a third spelling here would make this
/// server answer requests the real one refuses.
fn query_flag(query: &[u8], name: &[u8]) -> bool {
    let mut at = 0usize;
    while at <= query.len() {
        let end = query[at..]
            .iter()
            .position(|&b| b == b'&')
            .map(|i| at + i)
            .unwrap_or(query.len());
        let pair = &query[at..end];
        if let Some(eq) = pair.iter().position(|&b| b == b'=') {
            if &pair[..eq] == name {
                let v = &pair[eq + 1..];
                if v == b"true" || v == b"1" {
                    return true;
                }
            }
        }
        if end >= query.len() {
            break;
        }
        at = end + 1;
    }
    false
}

/// Find one header's value in wave's RAW header block.
///
/// The block is the bytes as they arrived: `Name: value` lines separated by
/// CRLF (wave hands the application the block unparsed on purpose — it owns
/// framing, not meaning). Names are compared ASCII-case-insensitively because
/// HTTP field names are case-insensitive and a client that sends
/// `authorization:` lowercase is not sending a different header.
fn header_value<'a>(headers: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut at = 0usize;
    while at < headers.len() {
        let mut end = at;
        while end < headers.len() && headers[end] != b'\n' {
            end += 1;
        }
        let mut line = &headers[at..end];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            let (n, v) = (&line[..colon], &line[colon + 1..]);
            if n.eq_ignore_ascii_case(name) {
                let mut v = v;
                while let [b' ' | b'\t', rest @ ..] = v {
                    v = rest;
                }
                return Some(v);
            }
        }
        at = end + 1;
    }
    None
}

/// `Bearer <token>` -> `<token>`. Any other scheme is not a bearer credential
/// and yields `None` rather than the whole value: handing `Basic dXNlcjpwdw==`
/// to a JWS verifier as though it were a token is how a credential of one kind
/// gets checked by the rules of another.
fn strip_bearer(value: &[u8]) -> Option<&[u8]> {
    const SCHEME: &[u8] = b"bearer ";
    if value.len() <= SCHEME.len() {
        return None;
    }
    let (head, rest) = value.split_at(SCHEME.len());
    if !head
        .iter()
        .zip(SCHEME)
        .all(|(a, b)| a.to_ascii_lowercase() == *b)
    {
        return None;
    }
    let mut rest = rest;
    while let [b' ' | b'\t', tail @ ..] = rest {
        rest = tail;
    }
    if rest.is_empty() {
        None
    } else {
        Some(rest)
    }
}

/// Project one wave HttpRequest envelope into a Chronicle record frame.
/// Returns the frame length in `out`, or 0 when the request is not a Kubernetes
/// resource path (discovery documents, health probes — the graph does not route
/// them and a frame claiming empty fields would be a lie).
fn project(req: &[u8], out: &mut [u8], max_body: u32, passthrough: u32) -> usize {
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
    let raw_target = &req[path_at..path_at + path_len];
    // Split the QUERY off before anything parses the path. Nothing upstream
    // does it — wave hands the application the request-target exactly as it
    // arrived — and without it `/…/pods?watch=true` parses as a resource
    // literally named `pods?watch=true`.
    let qmark = raw_target.iter().position(|&b| b == b'?');
    let path = match qmark {
        Some(i) => &raw_target[..i],
        None => raw_target,
    };
    let query = match qmark {
        Some(i) => &raw_target[i + 1..],
        None => &raw_target[..0],
    };
    let headers = &req[path_at + path_len..body_at];
    let body = &req[body_at..body_at + body_len];

    let Some((resource, namespace, name)) = parse_rest_path(path) else {
        // Not a resource path: a discovery document, a health probe, something
        // this graph may or may not answer. DROPPED by default, because a frame
        // claiming empty resource/namespace/name would be a lie a decision then
        // routes on.
        //
        // `passthrough = 1` emits it anyway, with those fields EMPTY and the
        // request target at 9 — for a graph whose first decision answers static
        // documents by target and refuses everything else. The lie is only a lie
        // when somebody reads it as a resource, and such a graph does not.
        if passthrough != 0 {
            let mut p = 1usize;
            let mut n = 0u8;
            macro_rules! pfield {
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
            let capped = body.len().min(max_body as usize);
            pfield!(put_i64(out, p, 1, wave_request_id as i64));
            pfield!(put_i64(out, p, 2, method as i64));
            pfield!(put_field(out, p, 3, TY_BYTES, b""));
            pfield!(put_field(out, p, 4, TY_BYTES, b""));
            pfield!(put_field(out, p, 5, TY_BYTES, b""));
            pfield!(put_field(out, p, 6, TY_BYTES, &body[..capped]));
            pfield!(put_field(out, p, 7, TY_BYTES, b""));
            let credential = header_value(headers, b"authorization")
                .and_then(strip_bearer)
                .unwrap_or(b"");
            pfield!(put_field(out, p, 8, TY_BYTES, credential));
            pfield!(put_field(out, p, 9, TY_BYTES, path));
            pfield!(put_field(out, p, 10, TY_BYTES, query));
            pfield!(put_i64(out, p, 11, i64::from(query_flag(query, b"watch"))));
            out[0] = n;
            return p;
        }
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
    // A CREATE names its object in the BODY, not the path: `POST /pods` with
    // `{"metadata":{"name":"web-1"}}` addresses `/pods/<ns>/web-1`. That is the
    // Kubernetes key convention, which this module already owns (it assembles
    // the key at all because the VM cannot concatenate), so it is the same
    // knowledge and not a new kind of it.
    //
    // Path name WINS when present: a PUT to `.../pods/web-0` carrying a body
    // that says `web-1` must not write `web-1`. The request-target is the
    // address; the body is a payload that may disagree with it.
    // The path is built on the STACK, never written as `&[b"metadata", b"name"]`.
    // A slice-of-slices literal lands in `.rodata` as an array of fat pointers,
    // and this PIC build does not relocate them: the callee reads wild pointers
    // and the module wedges in the panic handler's `loop {}` — no log, no
    // record, just a request that never comes back. Every other `j_path` caller
    // in this repo passes a runtime-built `&segs[..n]` for the same reason.
    let mut segs: [&[u8]; 2] = [b"", b""];
    segs[0] = b"metadata";
    segs[1] = b"name";
    let body_name = if name.is_empty() {
        j_path(&body[..capped], &segs).unwrap_or(b"")
    } else {
        b""
    };
    push(if name.is_empty() { body_name } else { name }, &mut k);
    field!(put_field(out, p, 7, TY_BYTES, &key[..k]));

    // Field 8: the bearer CREDENTIAL, scheme stripped. Extracted here for the
    // same reason as the key: the VM cannot scan a header block for a name and
    // split a value on a space. What it is NOT is a decision — this module does
    // not check the token, does not know which suite signed it and does not
    // care whether it is empty. An absent or non-bearer Authorization header
    // yields an empty field, and a downstream decision refuses on emptiness
    // rather than this module dropping the request: "no credential" is an
    // authorization answer (401), not an unroutable envelope.
    let credential = header_value(headers, b"authorization")
        .and_then(strip_bearer)
        .unwrap_or(b"");
    field!(put_field(out, p, 8, TY_BYTES, credential));

    // Field 9: the raw request-target, carried verbatim. A proof-of-possession
    // credential is bound to the method and URI it was presented with, so the
    // verifier needs the URI as the client sent it — not the reassembly of it
    // that field 7 performs.
    field!(put_field(out, p, 9, TY_BYTES, path));

    // Field 10: the query string, and field 11: `watch=true` as a NUMBER.
    //
    // The flag is extracted here rather than compared downstream because the VM
    // has no substring test: a decision can compare a whole field, and
    // `watch=true&resourceVersion=0` is not equal to anything useful. Which
    // parameters exist is HTTP; what a watch MEANS stays in params.
    field!(put_field(out, p, 10, TY_BYTES, query));
    field!(put_i64(out, p, 11, i64::from(query_flag(query, b"watch"))));

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
        s.passthrough = 0;
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
        let flen = project(req, &mut out, s.max_body, s.passthrough);
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
