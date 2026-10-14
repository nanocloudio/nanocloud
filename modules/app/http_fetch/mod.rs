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
    connected_parts, error_parts, write_connect_to, Target, CMD_CONNECT_TO as NET_CMD_CONNECT_TO,
    CONNECT_TO_MAX, REQUESTER_TAG_NONE,
};
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/jsonq.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/b64.rs");

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
/// A bearer token held between requests (a registry's JWT is ~1–3 KiB).
const TOKEN_MAX: usize = 4096;
/// The request line and headers, a token among them.
const TX_BUF_SIZE: usize = 1024 + TOKEN_MAX;
/// `user:password` read from the `credentials` key.
const MAX_CRED: usize = 256;
/// One answer to one query.
const ANSWER: usize = 2048;
const MAX_QUERIES: u8 = 8;

const CONNECT_TIMEOUT_MS: u64 = 10_000;
const RESPONSE_TIMEOUT_MS: u64 = 30_000;
const BACKOFF_INIT_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 30_000;
/// Transport attempts before a request is answered 0.
const MAX_ATTEMPTS: u8 = 6;
/// A file provider that answers "not yet" (fat32 on metal, mid-flush):
/// the open is asked again this long after, not counted as a failure.
const FS_EAGAIN: i32 = -11;
const OPEN_RETRY_MS: u64 = 20;

/// Which request is on the wire: the record's, or a token from the realm the
/// origin's Bearer challenge named.
const LEG_OBJECT: u8 = 0;
const LEG_TOKEN: u8 = 1;

// The chunked transfer coding, byte by byte.
const CK_SIZE: u8 = 0;
const CK_DATA: u8 = 1;
const CK_CRLF: u8 = 2;
const CK_TRAILER: u8 = 3;

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
    /// The store key whose value is `user:password` for the token realm.
    credentials: [u8; MAX_KEY],
    credentials_len: u16,

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

    /// Bearer auth: the leg on the wire, whether this request has had its
    /// one token fetch (re-armed by every 2xx), the token, and the realm of
    /// the last challenge (its authority, port, and path with the query).
    leg: u8,
    auth_tried: u8,
    challenged: u8,
    token_len: u16,
    token: [u8; TOKEN_MAX],
    realm_auth: [u8; MAX_AUTHORITY],
    realm_auth_len: u8,
    realm_port: u16,
    realm_path: [u8; MAX_PATH],
    realm_path_len: u16,

    /// The chunked transfer coding: on, its state, the chunk's bytes left,
    /// the size line's digits, past an extension, a trailer line's length.
    chunked: u8,
    ck_state: u8,
    ck_left: u64,
    ck_digits: u8,
    ck_ext: u8,
    ck_line: u8,

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
    use super::{MAX_AUTHORITY, MAX_DIR, MAX_KEY};

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

        5, credentials, str, 0
            => |s, d, len| {
                let n = if len > MAX_KEY { 0 } else { len };
                s.credentials_len = n as u16;
                if n > 0 { ptr_copy(s.credentials.as_mut_ptr(), d, n); }
            };
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
    s.challenged = 0;
    s.chunked = 0;
    s.ck_state = CK_SIZE;
    s.ck_left = 0;
    s.ck_digits = 0;
    s.ck_ext = 0;
    s.ck_line = 0;
    s.phase = Phase::Connecting;
}

/// Start the held request again from what it has: a Range file from its
/// position, anything else from nothing (its file truncated). False when
/// the file cannot be reopened — answered 507.
/// Open the held request's file afresh (unlinked first). `Some(true)`
/// opened; `Some(false)` the provider is not ready, and the open is asked
/// again after `OPEN_RETRY_MS`; `None` it refused, answered 507.
unsafe fn open_file(s: &mut State) -> Option<bool> {
    let sys = &*s.syscalls;
    let mut path = [0u8; MAX_DIR + 1 + MAX_FILE];
    let pl = file_path(s, &mut path);
    let _ = (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), pl);
    let fd = (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), pl);
    if fd == FS_EAGAIN {
        s.file_fd = -1;
        s.backoff_ms = OPEN_RETRY_MS;
        s.state_start_ms = dev_millis(sys);
        s.phase = Phase::Backoff;
        return Some(false);
    }
    if fd < 0 {
        log(s, 1, b"[http_fetch] cannot create the file");
        reply(s, 507);
        return None;
    }
    s.file_fd = fd;
    Some(true)
}

unsafe fn restart(s: &mut State) -> bool {
    // A file the provider was not ready to create: created now, from nothing.
    if s.sink == SINK_FILE as u8 && s.file_fd < 0 {
        s.hasher = Sha256::new();
        s.body_fill = 0;
        s.body_received = 0;
        s.pos = 0;
        return open_file(s) == Some(true);
    }
    if ranged(s) {
        s.body_received = s.pos;
        s.body_fill = 0;
        return true;
    }
    s.hasher = Sha256::new();
    s.body_fill = 0;
    s.body_received = 0;
    s.pos = 0;
    if s.sink == SINK_FILE as u8 {
        close_file(s, false);
        return open_file(s) == Some(true);
    }
    true
}

/// Whether the held request is a Range request (a chunked file).
fn ranged(s: &State) -> bool {
    s.sink == SINK_FILE as u8 && s.chunk_bytes > 0 && s.size > 0
}

unsafe fn build_request(s: &mut State) -> usize {
    let sys = &*s.syscalls;
    // `tx_buf` is written while the request's parts are read from `s`.
    let st = s as *mut State;
    let tx = &mut (*st).tx_buf;
    let mut o;
    if s.leg == LEG_TOKEN {
        let path = core::slice::from_raw_parts(s.realm_path.as_ptr(), s.realm_path_len as usize);
        let host = core::slice::from_raw_parts(s.realm_auth.as_ptr(), s.realm_auth_len as usize);
        o = append(tx, 0, b"GET ");
        o = append(tx, o, path);
        o = append(tx, o, b" HTTP/1.1\r\nHost: ");
        o = append(tx, o, host);
        o = append(tx, o, b"\r\nAccept: application/json");
        // The realm's credentials, when the deployment gave a key for them;
        // without, the token asked for is an anonymous one.
        if s.credentials_len > 0 {
            let key = core::slice::from_raw_parts(s.credentials.as_ptr(), s.credentials_len as usize);
            let mut cred = [0u8; MAX_CRED];
            if let Some(n) = get_value_live(sys, key, &mut cred) {
                let mut b = [0u8; MAX_CRED.div_ceil(3) * 4];
                if let Some(bn) = b64_encode(&cred[..n], &mut b) {
                    o = append(tx, o, b"\r\nAuthorization: Basic ");
                    o = append(tx, o, &b[..bn]);
                }
            }
        }
    } else {
        let h = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
        let mut path = [0u8; MAX_PATH];
        let pl = join(h, 3, 6, &mut path).unwrap_or(0);
        let accept = frame_bytes(h, 7);
        o = append(tx, 0, b"GET ");
        o = append(tx, o, &path[..pl]);
        o = append(tx, o, b" HTTP/1.1\r\nHost: ");
        o = append(tx, o, authority(&*st));
        if !accept.is_empty() && accept.len() <= MAX_ACCEPT {
            o = append(tx, o, b"\r\nAccept: ");
            o = append(tx, o, accept);
        }
        if s.token_len > 0 {
            o = append(tx, o, b"\r\nAuthorization: Bearer ");
            o = append(tx, o, core::slice::from_raw_parts(s.token.as_ptr(), s.token_len as usize));
        }
        if ranged(s) {
            let end = (s.pos + s.chunk_bytes as u64).min(s.size) - 1;
            o = append(tx, o, b"\r\nRange: bytes=");
            o = write_dec(tx, o, s.pos);
            o = append(tx, o, b"-");
            o = write_dec(tx, o, end);
        }
    }
    append(tx, o, b"\r\nConnection: close\r\n\r\n")
}

/// `%`-encode a query value: everything but the unreserved set and `:`,
/// `/` (a registry scope is `repository:<name>:pull`).
fn query_enc(out: &mut [u8], mut at: usize, v: &[u8]) -> usize {
    const H: &[u8; 16] = b"0123456789ABCDEF";
    for &c in v {
        if c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_' | b'~' | b':' | b'/') {
            at = append(out, at, core::slice::from_ref(&c));
        } else {
            at = append(out, at, &[b'%', H[(c >> 4) as usize], H[(c & 15) as usize]]);
        }
    }
    at
}

/// `<sep><key>=<value>` onto a query, when there is a value.
fn query_param(out: &mut [u8], at: usize, sep: &mut u8, key: &[u8], val: Option<&[u8]>) -> usize {
    let Some(val) = val else {
        return at;
    };
    let mut o = append(out, at, core::slice::from_ref(sep));
    o = append(out, o, key);
    o = append(out, o, b"=");
    *sep = b'&';
    query_enc(out, o, val)
}

/// One `key="value"` of a challenge's parameters (RFC 7235 auth-params).
fn auth_param<'a>(params: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut rest = params;
    loop {
        while let Some((&c, r)) = rest.split_first() {
            if c == b' ' || c == b',' {
                rest = r;
            } else {
                break;
            }
        }
        let eq = rest.iter().position(|&c| c == b'=')?;
        let name = &rest[..eq];
        let mut v = &rest[eq + 1..];
        let value;
        if v.first() == Some(&b'"') {
            v = &v[1..];
            let close = v.iter().position(|&c| c == b'"')?;
            value = &v[..close];
            rest = &v[close + 1..];
        } else {
            let end = v.iter().position(|&c| c == b',').unwrap_or(v.len());
            value = &v[..end];
            rest = &v[end..];
        }
        if name.eq_ignore_ascii_case(key) {
            return Some(value);
        }
    }
}

/// A Bearer challenge: where its realm is, and the token request for it
/// (`<realm path>?service=<service>&scope=<scope>`). False when it is not
/// one this connector can answer (another scheme, a realm it cannot dial).
fn take_challenge(s: &mut State, v: &[u8]) -> bool {
    if v.len() < 7 || !v[..7].eq_ignore_ascii_case(b"Bearer ") {
        return false;
    }
    let params = &v[7..];
    let Some(realm) = auth_param(params, b"realm") else {
        return false;
    };
    let (rest, port) = if realm.len() > 8 && realm[..8].eq_ignore_ascii_case(b"https://") {
        (&realm[8..], 443u16)
    } else if realm.len() > 7 && realm[..7].eq_ignore_ascii_case(b"http://") {
        (&realm[7..], 80u16)
    } else {
        return false;
    };
    let slash = rest.iter().position(|&c| c == b'/').unwrap_or(rest.len());
    let host = &rest[..slash];
    let path: &[u8] = if slash < rest.len() { &rest[slash..] } else { b"/" };
    if host.is_empty() || host.len() > MAX_AUTHORITY {
        return false;
    }
    let Some((_, p)) = Target::parse(host) else {
        return false;
    };
    s.realm_auth[..host.len()].copy_from_slice(host);
    s.realm_auth_len = host.len() as u8;
    s.realm_port = p.unwrap_or(port);
    let mut o = append(&mut s.realm_path, 0, path);
    let mut sep = if path.iter().any(|&c| c == b'?') { b'&' } else { b'?' };
    // service, then scope — two calls, not a loop over an array of byte
    // strings (unrelocated in a PIC module's .rodata).
    o = query_param(&mut s.realm_path, o, &mut sep, b"service", auth_param(params, b"service"));
    o = query_param(&mut s.realm_path, o, &mut sep, b"scope", auth_param(params, b"scope"));
    if o == s.realm_path.len() {
        return false; // did not fit: a cut query asks for another token
    }
    s.realm_path_len = o as u16;
    true
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
/// connector cannot take (another transfer coding, no length) — answered,
/// not retried. A 401's Bearer challenge is kept for the token leg.
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
            if !v.eq_ignore_ascii_case(b"chunked") {
                return false;
            }
            s.chunked = 1;
        } else if name.eq_ignore_ascii_case(b"www-authenticate") {
            if s.status == 401 && s.leg == LEG_OBJECT && s.challenged == 0 {
                s.challenged = take_challenge(s, v) as u8;
            }
        } else if name.eq_ignore_ascii_case(b"content-type") {
            let n = v.len().min(s.ctype.len());
            s.ctype[..n].copy_from_slice(&v[..n]);
            s.ctype_len = n as u8;
        }
    }
    if s.chunked == 1 {
        s.content_length = 0; // known when the last chunk is
        return true;
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
    // Only the object's own bytes are the object: a refusal's body, or a
    // token's, is neither hashed nor written.
    let object = s.leg == LEG_OBJECT && s.status / 100 == 2;
    if object {
        s.hasher.update(data);
    }
    if s.sink == SINK_FILE as u8 && object {
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
    if s.leg == LEG_TOKEN {
        // The realm's answer: its token (`token`, or OAuth's
        // `access_token`), and the object asked for again with it. No token
        // answers the object's own refusal.
        s.leg = LEG_OBJECT;
        let body = core::slice::from_raw_parts(s.body.as_ptr(), s.body_fill as usize);
        let tok = if s.status == 200 {
            // `jq_plain` builds its path on the stack: a literal array of
            // byte strings would sit unrelocated in a PIC module's .rodata.
            jq_plain(body, b"token")
                .filter(|t| !t.is_empty())
                .or_else(|| jq_plain(body, b"access_token"))
                .unwrap_or(&[])
        } else {
            &[]
        };
        if tok.is_empty() || tok.len() > TOKEN_MAX || tok.iter().any(|&c| c <= b' ' || c >= 0x7f) {
            log(s, 1, b"[http_fetch] the realm gave no token");
            s.body_fill = 0;
            reply(s, 401);
            return;
        }
        let n = tok.len();
        // `tok` is a view of `body`: copied, not borrowed, into the token.
        core::ptr::copy_nonoverlapping(tok.as_ptr(), s.token.as_mut_ptr(), n);
        s.token_len = n as u16;
        if restart(s) {
            begin(s);
        }
        return;
    }
    if s.status == 401 && s.challenged == 1 && s.auth_tried == 0 {
        // The origin wants a token (none held, or the held one is spent or
        // for another scope): one fetch from the realm it named, then the
        // request again.
        s.auth_tried = 1;
        s.token_len = 0;
        s.leg = LEG_TOKEN;
        s.body_fill = 0;
        begin(s);
        return;
    }
    let ok = if ranged(s) { 206 } else { 200 };
    if s.status != ok && !(s.status == 200 && ranged(s) && s.pos == s.content_length) {
        // An HTTP refusal is final; its body (if any) is not the object.
        let st = s.status;
        s.body_fill = 0;
        reply(s, st);
        return;
    }
    // A token that served is good for another refresh when it expires.
    s.auth_tried = 0;
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

/// Transfer-coded bytes: each chunk's data to `body_bytes`. Ok(true) once the
/// last chunk and its trailer are in; Err(status) on a malformed coding (502)
/// or a body the sink refused (507).
unsafe fn dechunk(s: &mut State, mut data: &[u8]) -> Result<bool, u16> {
    while let Some(&c) = data.first() {
        match s.ck_state {
            CK_SIZE => {
                data = &data[1..];
                if c == b'\n' {
                    if s.ck_digits == 0 {
                        return Err(502);
                    }
                    s.ck_state = if s.ck_left == 0 { CK_TRAILER } else { CK_DATA };
                    s.ck_line = 0;
                } else if s.ck_ext == 0 {
                    let v = match c {
                        b'0'..=b'9' => c - b'0',
                        b'a'..=b'f' => c - b'a' + 10,
                        b'A'..=b'F' => c - b'A' + 10,
                        b';' | b'\r' | b' ' | b'\t' => {
                            s.ck_ext = 1;
                            continue;
                        }
                        _ => return Err(502),
                    };
                    if s.ck_digits == 15 {
                        return Err(502);
                    }
                    s.ck_left = s.ck_left * 16 + v as u64;
                    s.ck_digits += 1;
                }
            }
            CK_DATA => {
                let n = (data.len() as u64).min(s.ck_left) as usize;
                if !body_bytes(s, &data[..n]) {
                    return Err(507);
                }
                s.ck_left -= n as u64;
                data = &data[n..];
                if s.ck_left == 0 {
                    s.ck_state = CK_CRLF;
                }
            }
            CK_CRLF => {
                data = &data[1..];
                if c == b'\n' {
                    s.ck_state = CK_SIZE;
                    s.ck_digits = 0;
                    s.ck_ext = 0;
                } else if c != b'\r' {
                    return Err(502);
                }
            }
            _ => {
                data = &data[1..];
                if c == b'\n' {
                    if s.ck_line == 0 {
                        return Ok(true);
                    }
                    s.ck_line = 0;
                } else if c != b'\r' {
                    s.ck_line = 1;
                }
            }
        }
    }
    Ok(false)
}

/// Response body bytes off the wire, framed by Content-Length or the chunked
/// coding; the response done when its framing says so.
unsafe fn body_in(s: &mut State, data: &[u8]) {
    if s.chunked == 1 {
        match dechunk(s, data) {
            Ok(true) => {
                s.content_length = s.body_received_in_response();
                body_done(s);
            }
            Ok(false) => {}
            Err(st) => reply(s, st),
        }
        return;
    }
    let left = s.content_length - s.body_received_in_response();
    let take = (data.len() as u64).min(left) as usize;
    if !body_bytes(s, &data[..take]) {
        reply(s, 507);
        return;
    }
    if s.body_received_in_response() >= s.content_length {
        body_done(s);
    }
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
                let (cid, tag) = connected_parts(payload);
                if tag == REQUESTER_TAG_NONE || tag == dev_requester_tag(sys) {
                    s.conn_id = cid;
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
                body_in(s, &early[..el]);
                if s.phase != Phase::RecvBody || data.is_empty() {
                    return true;
                }
            }
            if s.phase == Phase::RecvBody {
                body_in(s, data);
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
            if payload_len < 3 {
                return true;
            }
            // A tagged error is a connect-phase failure, attributed by its
            // tag alone (its conn_id means nothing there); an untagged one
            // is an established connection's, by conn_id.
            let (cid, _errno, tag) = error_parts(payload);
            let ours = if tag != REQUESTER_TAG_NONE {
                s.phase == Phase::WaitConnect && tag == dev_requester_tag(sys)
            } else {
                s.conn_present == 1 && cid == s.conn_id
            };
            if ours {
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
    s.leg = LEG_OBJECT;
    s.auth_tried = 0;
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
        // Not ready yet: asked again shortly (Backoff → restart); refused:
        // answered by open_file.
        match open_file(s) {
            Some(true) => {}
            _ => return,
        }
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
        s.credentials_len = 0;
        s.token_len = 0;
        s.leg = LEG_OBJECT;
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
                let (dial, port) = if s.leg == LEG_TOKEN {
                    (&s.realm_auth[..s.realm_auth_len as usize], s.realm_port)
                } else {
                    (authority(s), s.port)
                };
                let n = match Target::parse(dial) {
                    Some((target, _)) => write_connect_to(
                        &mut payload,
                        SOCK_TYPE_STREAM,
                        port,
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
                    // Range position when chunked, else from nothing. A
                    // token leg is asked again as it was.
                    if s.leg == LEG_TOKEN {
                        s.body_fill = 0;
                    } else if !restart(s) {
                        return 0;
                    }
                    begin(s);
                }
            }
        }
        0
    }
}
