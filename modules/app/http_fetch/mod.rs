//! Chronicle connector for an HTTP origin (a registry, an artifact server).
//!
//! See `manifest.toml` for the record layouts. One GET at a time over the net
//! pair (`Connection: close`, one connection per request); the answer comes
//! back as a record carrying the JSON members the request asked for, or lands
//! as a sha256-verified file. It decides nothing and knows no API.

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
use abi::contracts::net::net_proto::{
    write_connect_to, Target, CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CONNECT_TO_MAX,
    REQUESTER_TAG_NONE,
};
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/jsonq.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 256;

const FS_CLOSE: u32 = 0x0903;
const FS_FSYNC: u32 = 0x0905;
const FS_WRITE: u32 = 0x0906;
const FS_OPEN_CREATE: u32 = 0x0909;
const FS_UNLINK: u32 = 0x090A;
const FS_MKDIR: u32 = 0x090B;

const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
/// The record's carried context (chronicle `CARRY_FIELD`): one nested
/// message, echoed unchanged beside the legacy 30..=39 range.
const CARRY_FIELD: u8 = 254;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const SINK_REPLY: i64 = 1;
const SINK_FILE: i64 = 2;

const DEFAULT_PORT: u16 = 443;
const MAX_AUTHORITY: usize = 96;
const MAX_DIR: usize = 96;
const MAX_PATH: usize = 384;
const MAX_ACCEPT: usize = 256;
const MAX_FILE: usize = 96;
const REQ_BUF: usize = 8192;
const NET_BUF_SIZE: usize = 3 + 2 + 8192;
const HDR_BUF_SIZE: usize = 2048;
/// A JSON answer (a manifest, an index, an image config).
const BODY_BUF_SIZE: usize = 32768;
const TX_BUF_SIZE: usize = 1024;
/// One answer to one query.
const ANSWER: usize = 2048;
const MAX_QUERIES: u8 = 8;

const CONNECT_TIMEOUT_MS: u64 = 10_000;
const RESPONSE_TIMEOUT_MS: u64 = 30_000;
const BACKOFF_INIT_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 30_000;
/// Transport attempts before a request is answered 0.
const MAX_ATTEMPTS: u8 = 6;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Idle = 0,
    Connecting = 1,
    WaitConnect = 2,
    RecvHeader = 3,
    RecvBody = 4,
    Backoff = 5,
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    request_in: i32,
    response_out: i32,
    resolved: u8,

    authority: [u8; MAX_AUTHORITY],
    authority_len: u8,
    authority_over: u8,
    port: u16,
    dir: [u8; MAX_DIR],
    dir_len: u8,
    chunk_bytes: u32,
    boot_delay_ms: u32,

    phase: Phase,
    inflight: u8,
    attempts: u8,
    conn_id: u16,
    prev_conn_id: u16,
    conn_present: u8,
    state_start_ms: u64,
    backoff_ms: u64,

    /// The request being served, held whole: its fields are re-read at reply.
    held: [u8; REQ_BUF],
    held_len: u16,
    sink: u8,
    size: u64,
    pos: u64,
    file_fd: i32,
    hasher: Sha256,

    status: u16,
    hdr_fill: u32,
    content_length: u64,
    body_received: u64,
    /// `body_received` when the current response began (a Range chunk is one
    /// response of several).
    resp_base: u64,
    body_fill: u32,
    ctype: [u8; 96],
    ctype_len: u8,

    tx_len: u16,
    tx_sent: u16,

    hdr_buf: [u8; HDR_BUF_SIZE],
    body: [u8; BODY_BUF_SIZE],
    tx_buf: [u8; TX_BUF_SIZE],
    net_buf: [u8; NET_BUF_SIZE],
    out_buf: [u8; REQ_BUF],
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::SCHEMA_MAX;
    use super::{MAX_AUTHORITY, MAX_DIR};

    define_params! {
        State;

        1, authority, str, 0
            => |s, d, len| {
                if len > MAX_AUTHORITY { s.authority_over = 1; return; }
                s.authority_len = len as u8;
                if len > 0 { ptr_copy(s.authority.as_mut_ptr(), d, len); }
            };

        2, dir, str, 0
            => |s, d, len| {
                let n = if len > MAX_DIR { MAX_DIR } else { len };
                s.dir_len = n as u8;
                if n > 0 { ptr_copy(s.dir.as_mut_ptr(), d, n); }
            };

        3, chunk_bytes, u32, 0
            => |s, d, len| { s.chunk_bytes = p_u32(d, len, 0, 0); };

        4, boot_delay_ms, u32, 0
            => |s, d, len| { s.boot_delay_ms = p_u32(d, len, 0, 0); };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        if p + 4 > frame.len() {
            return None;
        }
        let num = frame[p];
        let ty = frame[p + 1];
        let len = u16::from_le_bytes([frame[p + 2], frame[p + 3]]) as usize;
        if p + 4 + len > frame.len() {
            return None;
        }
        if num == number {
            return Some((ty, &frame[p + 4..p + 4 + len]));
        }
        p += 4 + len;
    }
    None
}

fn frame_i64(frame: &[u8], number: u8) -> Option<i64> {
    match frame_field(frame, number) {
        Some((TY_I64, b)) if b.len() == 8 => Some(i64::from_le_bytes(b.try_into().unwrap())),
        _ => None,
    }
}

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, b)) => b,
        _ => &[],
    }
}

fn frame_len(data: &[u8]) -> Option<usize> {
    let count = *data.first()? as usize;
    let mut off = 1usize;
    for _ in 0..count {
        if off + 4 > data.len() {
            return None;
        }
        off += 4 + u16::from_le_bytes([data[off + 2], data[off + 3]]) as usize;
    }
    (off <= data.len()).then_some(off)
}

/// Join the BYTES fields `lo..=hi` in order. None when they do not fit.
fn join(frame: &[u8], lo: u8, hi: u8, out: &mut [u8]) -> Option<usize> {
    let mut n = 0usize;
    for f in lo..=hi {
        let part = frame_bytes(frame, f);
        if n + part.len() > out.len() {
            return None;
        }
        out[n..n + part.len()].copy_from_slice(part);
        n += part.len();
    }
    Some(n)
}

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

fn parse_dec(b: &[u8], at: usize) -> (u64, usize) {
    let mut v = 0u64;
    let mut i = at;
    while i < b.len() && b[i].is_ascii_digit() {
        v = v.saturating_mul(10).saturating_add((b[i] - b'0') as u64);
        i += 1;
    }
    (v, i - at)
}

fn write_dec(dst: &mut [u8], at: usize, v: u64) -> usize {
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
    append(dst, at, &tmp[i..])
}

fn hasher_hex(h: &Sha256) -> [u8; 64] {
    let digest = h.clone().finalize();
    let mut hex = [0u8; 64];
    const H: &[u8; 16] = b"0123456789abcdef";
    for i in 0..32 {
        hex[i * 2] = H[(digest[i] >> 4) as usize];
        hex[i * 2 + 1] = H[(digest[i] & 0x0F) as usize];
    }
    hex
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

unsafe fn log(s: &State, level: u8, msg: &[u8]) {
    dev_log(&*s.syscalls, level, msg.as_ptr(), msg.len());
}

fn authority(s: &State) -> &[u8] {
    &s.authority[..s.authority_len as usize]
}

fn held(s: &State) -> &[u8] {
    &s.held[..s.held_len as usize]
}

/// `<dir>/<file>` into `out`; the length, or 0 when the name is unsafe.
fn file_path(s: &State, out: &mut [u8]) -> usize {
    let mut name = [0u8; MAX_FILE];
    let Some(n) = join(held(s), 8, 10, &mut name) else {
        return 0;
    };
    // One path segment of plain characters: a name the chain built must not
    // be able to climb out of `dir`.
    if n == 0
        || name[..n]
            .iter()
            .any(|&c| !(c.is_ascii_alphanumeric() || c == b'-' || c == b'_' || c == b'.'))
        || name[0] == b'.'
    {
        return 0;
    }
    let p = append(out, 0, &s.dir[..s.dir_len as usize]);
    let p = append(out, p, b"/");
    append(out, p, &name[..n])
}

unsafe fn close_conn(s: &mut State) {
    if s.conn_present == 1 {
        let sys = &*s.syscalls;
        let payload = s.conn_id.to_le_bytes();
        let _ = net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            payload.as_ptr(),
            2,
            s.net_buf.as_mut_ptr(),
            NET_BUF_SIZE,
        );
        s.prev_conn_id = s.conn_id;
        s.conn_present = 0;
    }
}

unsafe fn close_file(s: &mut State, discard: bool) {
    let sys = &*s.syscalls;
    if s.file_fd >= 0 {
        let mut z = [0u8; 4];
        (sys.provider_call)(s.file_fd, FS_CLOSE, z.as_mut_ptr(), 0);
        s.file_fd = -1;
    }
    if discard {
        let mut path = [0u8; MAX_DIR + 1 + MAX_FILE];
        let n = file_path(s, &mut path);
        if n > 0 {
            let _ = (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), n);
        }
    }
}

/// Answer the held request and free the slot.
unsafe fn reply(s: &mut State, status: u16) {
    let sys = &*s.syscalls;
    close_conn(s);
    // Only a file THIS request created is discarded: a skipped request (304)
    // or one refused before its file was opened must not unlink what an
    // earlier request landed and verified.
    if s.sink == SINK_FILE as u8 && s.file_fd >= 0 {
        close_file(s, status != 200);
    }
    let h = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let body = core::slice::from_raw_parts(s.body.as_ptr(), s.body_fill as usize);
    let out = &mut s.out_buf;
    let mut p = 1usize;
    let mut n = 0u8;
    macro_rules! field {
        ($e:expr) => {
            match $e {
                Some(v) => {
                    p = v;
                    n += 1;
                }
                None => {
                    s.inflight = 0;
                    s.held_len = 0;
                    s.phase = Phase::Idle;
                    return;
                }
            }
        };
    }
    let cid = frame_i64(h, 1).unwrap_or(0);
    field!(put_field(out, p, 1, TY_I64, &cid.to_le_bytes()));
    field!(put_field(out, p, 2, TY_I64, &(status as i64).to_le_bytes()));
    field!(put_field(
        out,
        p,
        3,
        TY_I64,
        &(s.body_received as i64).to_le_bytes()
    ));
    field!(put_field(
        out,
        p,
        4,
        TY_BYTES,
        &s.ctype[..s.ctype_len as usize]
    ));
    // Every answer field, always — empty when not asked or not answered: a
    // decision reading a field the record lacks produces nothing at all.
    for q in 0..MAX_QUERIES {
        let query = frame_bytes(h, 14 + q);
        let mut ans = [0u8; ANSWER];
        let len = if status == 200 && s.sink == SINK_REPLY as u8 && !query.is_empty() {
            jq(body, query, &mut ans).unwrap_or(0)
        } else {
            0
        };
        field!(put_field(out, p, 40 + q, TY_BYTES, &ans[..len]));
    }
    for f in (CARRY_LO..=CARRY_HI).chain(core::iter::once(CARRY_FIELD)) {
        if let Some((ty, v)) = frame_field(h, f) {
            field!(put_field(out, p, f, ty, v));
        }
    }
    out[0] = n;
    let _ = (sys.channel_write)(s.response_out, s.out_buf.as_ptr(), p);
    s.inflight = 0;
    s.held_len = 0;
    s.phase = Phase::Idle;
}

/// A transport failure: retry after a backoff, or answer 0 when spent.
unsafe fn transport_failed(s: &mut State, why: &[u8]) {
    log(s, 1, why);
    close_conn(s);
    s.attempts += 1;
    if s.attempts >= MAX_ATTEMPTS {
        reply(s, 0);
        return;
    }
    s.backoff_ms = if s.backoff_ms == 0 {
        BACKOFF_INIT_MS
    } else {
        (s.backoff_ms * 2).min(BACKOFF_MAX_MS)
    };
    s.state_start_ms = dev_millis(&*s.syscalls);
    s.phase = Phase::Backoff;
}

/// (Re)start the transfer of the held request from its current position.
unsafe fn begin(s: &mut State) {
    s.status = 0;
    s.hdr_fill = 0;
    s.content_length = 0;
    s.tx_len = 0;
    s.tx_sent = 0;
    s.conn_present = 0;
    s.phase = Phase::Connecting;
}

/// Whether the held request is a Range request (a chunked file).
fn ranged(s: &State) -> bool {
    s.sink == SINK_FILE as u8 && s.chunk_bytes > 0 && s.size > 0
}

unsafe fn build_request(s: &mut State) -> usize {
    let h = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let mut path = [0u8; MAX_PATH];
    let pl = join(h, 3, 6, &mut path).unwrap_or(0);
    let accept = frame_bytes(h, 7);
    let mut buf = [0u8; TX_BUF_SIZE];
    let mut o = append(&mut buf, 0, b"GET ");
    o = append(&mut buf, o, &path[..pl]);
    o = append(&mut buf, o, b" HTTP/1.1\r\nHost: ");
    o = append(&mut buf, o, authority(s));
    if !accept.is_empty() && accept.len() <= MAX_ACCEPT {
        o = append(&mut buf, o, b"\r\nAccept: ");
        o = append(&mut buf, o, accept);
    }
    if ranged(s) {
        let end = (s.pos + s.chunk_bytes as u64).min(s.size) - 1;
        o = append(&mut buf, o, b"\r\nRange: bytes=");
        o = write_dec(&mut buf, o, s.pos);
        o = append(&mut buf, o, b"-");
        o = write_dec(&mut buf, o, end);
    }
    o = append(&mut buf, o, b"\r\nConnection: close\r\n\r\n");
    s.tx_buf[..o].copy_from_slice(&buf[..o]);
    o
}

unsafe fn flush_tx(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    while s.tx_sent < s.tx_len {
        let chunk = ((s.tx_len - s.tx_sent) as usize).min(1024);
        let mut payload = [0u8; 2 + 1024];
        payload[..2].copy_from_slice(&s.conn_id.to_le_bytes());
        payload[2..2 + chunk]
            .copy_from_slice(&s.tx_buf[s.tx_sent as usize..s.tx_sent as usize + chunk]);
        if net_write_frame(
            sys,
            s.net_out,
            NET_CMD_SEND,
            payload.as_ptr(),
            2 + chunk,
            s.net_buf.as_mut_ptr(),
            NET_BUF_SIZE,
        ) == 0
        {
            return false;
        }
        s.tx_sent += chunk as u16;
    }
    true
}

/// Read the status line and headers. Returns false for a response this
/// connector cannot take (chunked, no length) — answered, not retried.
unsafe fn parse_header(s: &mut State, end: usize) -> bool {
    let h = core::slice::from_raw_parts(s.hdr_buf.as_ptr(), end);
    s.status = if h.len() > 12 {
        parse_dec(h, 9).0 as u16
    } else {
        0
    };
    let mut cl: Option<u64> = None;
    s.ctype_len = 0;
    for line in h.split(|&c| c == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&c| c == b':') else {
            continue;
        };
        let (name, mut v) = (&line[..colon], &line[colon + 1..]);
        while v.first() == Some(&b' ') {
            v = &v[1..];
        }
        if name.eq_ignore_ascii_case(b"content-length") {
            let (x, n) = parse_dec(v, 0);
            if n > 0 {
                cl = Some(x);
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            return false;
        } else if name.eq_ignore_ascii_case(b"content-type") {
            let n = v.len().min(s.ctype.len());
            s.ctype[..n].copy_from_slice(&v[..n]);
            s.ctype_len = n as u8;
        }
    }
    let Some(cl) = cl else {
        return false;
    };
    s.content_length = cl;
    true
}

unsafe fn body_bytes(s: &mut State, data: &[u8]) -> bool {
    if data.is_empty() {
        return true;
    }
    s.hasher.update(data);
    if s.sink == SINK_FILE as u8 && s.status / 100 == 2 {
        let sys = &*s.syscalls;
        let mut off = 0usize;
        while off < data.len() {
            let wrote = (sys.provider_call)(
                s.file_fd,
                FS_WRITE,
                data.as_ptr().add(off) as *mut u8,
                data.len() - off,
            );
            if wrote <= 0 {
                return false;
            }
            off += wrote as usize;
        }
        s.pos += data.len() as u64;
    } else {
        let fill = s.body_fill as usize;
        if fill + data.len() > BODY_BUF_SIZE {
            return false;
        }
        s.body[fill..fill + data.len()].copy_from_slice(data);
        s.body_fill += data.len() as u32;
    }
    s.body_received += data.len() as u64;
    true
}

/// The response body is complete: verify and answer, or chain the next chunk.
unsafe fn body_done(s: &mut State) {
    let sys = &*s.syscalls;
    close_conn(s);
    s.attempts = 0;
    s.backoff_ms = 0;
    let ok = if ranged(s) { 206 } else { 200 };
    if s.status != ok && !(s.status == 200 && ranged(s) && s.pos == s.content_length) {
        // An HTTP refusal is final; its body (if any) is not the object.
        let st = s.status;
        s.body_fill = 0;
        reply(s, st);
        return;
    }
    if ranged(s) && s.pos < s.size {
        begin(s);
        return;
    }
    let want = frame_bytes(held(s), 11);
    let size = frame_i64(held(s), 13).unwrap_or(0);
    let hex = hasher_hex(&s.hasher);
    if (!want.is_empty() && want != &hex[..]) || (size > 0 && s.body_received != size as u64) {
        log(s, 1, b"[http_fetch] digest or size mismatch; discarded");
        s.body_fill = 0;
        reply(s, 422);
        return;
    }
    if s.sink == SINK_FILE as u8 {
        let mut z = [0u8; 4];
        if (sys.provider_call)(s.file_fd, FS_FSYNC, z.as_mut_ptr(), 0) < 0 {
            reply(s, 500);
            return;
        }
    }
    reply(s, 200);
}

unsafe fn pump_net(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let poll = (sys.channel_poll)(s.net_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return false;
    }
    let nbuf = s.net_buf.as_mut_ptr();
    let (msg_type, payload_len) = net_read_frame(sys, s.net_in, nbuf, NET_BUF_SIZE);
    if msg_type == 0 {
        return false;
    }
    let payload = core::slice::from_raw_parts(nbuf.add(3), payload_len);
    match msg_type {
        NET_MSG_CONNECTED => {
            if s.phase == Phase::WaitConnect && payload_len >= 2 {
                let tag = if payload_len >= 3 {
                    payload[2]
                } else {
                    REQUESTER_TAG_NONE
                };
                if tag == REQUESTER_TAG_NONE || tag == dev_requester_tag(sys) {
                    s.conn_id = u16::from_le_bytes([payload[0], payload[1]]);
                    s.conn_present = 1;
                    s.tx_len = build_request(s) as u16;
                    s.tx_sent = 0;
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::RecvHeader;
                    let _ = flush_tx(s);
                }
            }
        }
        NET_MSG_DATA => {
            if payload_len < 2 || s.conn_present == 0 {
                return true;
            }
            if u16::from_le_bytes([payload[0], payload[1]]) != s.conn_id {
                return true;
            }
            let mut tmp = [0u8; 8192];
            let m = (payload_len - 2).min(tmp.len());
            tmp[..m].copy_from_slice(&payload[2..2 + m]);
            let mut data = &tmp[..m];
            if s.phase == Phase::RecvHeader {
                let fill = s.hdr_fill as usize;
                let n = data.len().min(HDR_BUF_SIZE - fill);
                s.hdr_buf[fill..fill + n].copy_from_slice(&data[..n]);
                s.hdr_fill += n as u32;
                let have = core::slice::from_raw_parts(s.hdr_buf.as_ptr(), s.hdr_fill as usize);
                let Some(end) = find(have, b"\r\n\r\n") else {
                    if s.hdr_fill as usize >= HDR_BUF_SIZE {
                        reply(s, 502);
                    }
                    return true;
                };
                if !parse_header(s, end) {
                    reply(s, 502);
                    return true;
                }
                s.phase = Phase::RecvBody;
                let mut early = [0u8; HDR_BUF_SIZE];
                let el = s.hdr_fill as usize - (end + 4);
                early[..el].copy_from_slice(&s.hdr_buf[end + 4..s.hdr_fill as usize]);
                data = &data[n..];
                let limit = s.content_length;
                let take = (el as u64).min(limit) as usize;
                if !body_bytes(s, &early[..take]) {
                    reply(s, 507);
                    return true;
                }
            }
            if s.phase == Phase::RecvBody {
                let left = s.content_length - (s.body_received_in_response());
                let take = (data.len() as u64).min(left) as usize;
                if !body_bytes(s, &data[..take]) {
                    reply(s, 507);
                    return true;
                }
                if s.body_received_in_response() >= s.content_length {
                    body_done(s);
                }
            }
        }
        NET_MSG_CLOSED => {
            if payload_len >= 2
                && s.conn_present == 1
                && u16::from_le_bytes([payload[0], payload[1]]) == s.conn_id
            {
                s.conn_present = 0;
                if s.phase == Phase::RecvHeader || s.phase == Phase::RecvBody {
                    transport_failed(s, b"[http_fetch] connection closed mid-response");
                }
            }
        }
        NET_MSG_ERROR => {
            let cid = if payload_len >= 2 {
                u16::from_le_bytes([payload[0], payload[1]])
            } else {
                0
            };
            let stale = payload_len >= 2 && cid == s.prev_conn_id && cid != s.conn_id;
            if (s.conn_present == 1 && cid == s.conn_id)
                || (s.phase == Phase::WaitConnect && !stale)
            {
                transport_failed(s, b"[http_fetch] net error");
            }
        }
        _ => {}
    }
    true
}

impl State {
    /// Body bytes received in the CURRENT response (a Range chunk's own).
    fn body_received_in_response(&self) -> u64 {
        self.body_received - self.resp_base
    }
}

/// Take one record: skip it, or start its transfer.
unsafe fn admit(s: &mut State) {
    let sys = &*s.syscalls;
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    let pk = (sys.channel_peek)(s.request_in, s.held.as_mut_ptr(), REQ_BUF);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REQ_BUF)
    } else if pk <= 0 {
        return;
    } else {
        match frame_len(&s.held[..pk as usize]) {
            Some(fl) => (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), fl),
            None if (pk as usize) < REQ_BUF => return,
            None => {
                (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REQ_BUF);
                return;
            }
        }
    };
    if n <= 0 {
        return;
    }
    s.held_len = n as u16;
    let sink = frame_i64(held(s), 2).unwrap_or(0);
    if sink != SINK_REPLY && sink != SINK_FILE {
        s.held_len = 0;
        return; // the decision's filter
    }
    s.sink = sink as u8;
    s.inflight = 1;
    s.attempts = 0;
    s.backoff_ms = 0;
    s.body_fill = 0;
    s.body_received = 0;
    s.resp_base = 0;
    s.pos = 0;
    s.ctype_len = 0;
    s.size = frame_i64(held(s), 13).unwrap_or(0).max(0) as u64;
    s.hasher = Sha256::new();
    // Already there: answered without a byte on the wire.
    let mut k = [0u8; MAX_KEY];
    let (a, b) = (frame_bytes(held(s), 12), frame_bytes(held(s), 22));
    if !a.is_empty() && a.len() + b.len() <= MAX_KEY {
        k[..a.len()].copy_from_slice(a);
        k[a.len()..a.len() + b.len()].copy_from_slice(b);
        if head_rev(sys, &k[..a.len() + b.len()]).is_some() {
            reply(s, 304);
            return;
        }
    }
    if s.sink == SINK_FILE as u8 {
        let mut path = [0u8; MAX_DIR + 1 + MAX_FILE];
        let pl = file_path(s, &mut path);
        let want = frame_bytes(held(s), 11);
        if pl == 0 || want.len() != 64 {
            reply(s, 400);
            return;
        }
        let _ = (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), pl);
        let fd = (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), pl);
        if fd < 0 {
            log(s, 1, b"[http_fetch] cannot create the file");
            reply(s, 507);
            return;
        }
        s.file_fd = fd;
    }
    begin(s);
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
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.request_in = -1;
        s.response_out = -1;
        s.resolved = 0;
        s.phase = Phase::Idle;
        s.inflight = 0;
        s.conn_present = 0;
        s.file_fd = -1;
        s.held_len = 0;
        s.authority_len = 0;
        s.authority_over = 0;
        s.dir_len = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.authority_over == 1 || s.authority_len == 0 {
            log(s, 1, b"[http_fetch] authority (host[:port]) is required");
            return -3;
        }
        let Some((_, port)) = Target::parse(authority(s)) else {
            log(s, 1, b"[http_fetch] authority is not host[:port]");
            return -3;
        };
        s.port = port.unwrap_or(DEFAULT_PORT);
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
        if s.resolved == 0 {
            s.request_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.response_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            if s.dir_len > 0 {
                let mut dir = [0u8; MAX_DIR];
                let dl = s.dir_len as usize;
                dir[..dl].copy_from_slice(&s.dir[..dl]);
                let _ = (sys.provider_call)(-1, FS_MKDIR, dir.as_mut_ptr(), dl);
            }
            s.resolved = 1;
        }
        if s.request_in < 0 || s.response_out < 0 || s.net_in < 0 || s.net_out < 0 {
            return 0;
        }
        let mut drained = 0;
        while drained < 64 && pump_net(s) {
            drained += 1;
        }
        match s.phase {
            Phase::Idle => {
                if s.inflight == 0 && dev_millis(sys) >= s.boot_delay_ms as u64 {
                    admit(s);
                }
            }
            Phase::Connecting => {
                let mut payload = [0u8; CONNECT_TO_MAX];
                let n = match Target::parse(authority(s)) {
                    Some((target, _)) => write_connect_to(
                        &mut payload,
                        SOCK_TYPE_STREAM,
                        s.port,
                        &target,
                        Some(dev_requester_tag(sys)),
                    ),
                    None => 0,
                };
                if n == 0 {
                    reply(s, 0);
                    return 0;
                }
                // Each response's body starts where the object stands now.
                s.resp_base = s.body_received;
                if net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CONNECT_TO,
                    payload.as_ptr(),
                    n,
                    s.net_buf.as_mut_ptr(),
                    NET_BUF_SIZE,
                ) > 0
                {
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::WaitConnect;
                }
            }
            Phase::WaitConnect => {
                if dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS {
                    transport_failed(s, b"[http_fetch] connect timeout");
                }
            }
            Phase::RecvHeader | Phase::RecvBody => {
                if s.tx_sent < s.tx_len {
                    let _ = flush_tx(s);
                }
                if dev_millis(sys).wrapping_sub(s.state_start_ms) > RESPONSE_TIMEOUT_MS {
                    transport_failed(s, b"[http_fetch] response timeout");
                }
            }
            Phase::Backoff => {
                if dev_millis(sys).wrapping_sub(s.state_start_ms) >= s.backoff_ms {
                    // A retried file starts again from what is on disk: the
                    // Range position when chunked, else from nothing.
                    if !ranged(s) {
                        s.hasher = Sha256::new();
                        s.body_fill = 0;
                        s.body_received = 0;
                        s.pos = 0;
                        if s.sink == SINK_FILE as u8 {
                            let sys = &*s.syscalls;
                            close_file(s, false);
                            let mut path = [0u8; MAX_DIR + 1 + MAX_FILE];
                            let pl = file_path(s, &mut path);
                            let _ = (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), pl);
                            let fd = (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), pl);
                            if fd < 0 {
                                reply(s, 507);
                                return 0;
                            }
                            s.file_fd = fd;
                        }
                    } else {
                        s.body_received = s.pos;
                    }
                    begin(s);
                }
            }
        }
        0
    }
}
