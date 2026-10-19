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
include!("../_shared/regcred.rs");

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
/// The file `<dir>/<file>` dropped from the cache: no byte on the wire.
const SINK_DROP: i64 = 3;
/// The provider's answer for a name that is not there.
const FS_ENOENT: i32 = -2;

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
/// A credential as `Authorization: Basic` carries it (`base64(user:password)`).
const BASIC_MAX: usize = 384;
/// A Secret as the API stores it, and its decoded registry config.
const SECRET_MAX: usize = 4096;
const SECRET_CONFIG_MAX: usize = 3072;
/// The store prefix whose records name a pull's imagePullSecrets.
const MAX_PULL_AUTH: usize = 64;
/// The candidates a challenge is answered with, at most: a pull's Secrets,
/// the node's credentials, and none.
const MAX_CANDIDATES: u8 = 32;
/// One answer to one query.
const ANSWER: usize = 2048;
const MAX_QUERIES: u8 = 8;

const CONNECT_TIMEOUT_MS: u64 = 10_000;
const RESPONSE_TIMEOUT_MS: u64 = 30_000;
const BACKOFF_INIT_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 30_000;
/// Transport attempts before a request is answered 0, once the origin has
/// answered some of it (a transfer cut off mid-way resumes from what it has).
const MAX_ATTEMPTS: u8 = 6;
/// The same, while the origin has answered nothing of it at all (no
/// connection, no TLS session, no response header): it is answered 0 after
/// one retry, so the chain that asked decides — and backs off, and says so
/// (an image pull's ImagePullBackOff) — instead of this connector holding
/// every request behind it on an origin that is not there.
const MAX_CONNECT_ATTEMPTS: u8 = 2;
/// The fs provider never waits on its device inside a call. `EAGAIN`: not
/// now, nothing moved, ask again. `EINPROGRESS`: taken, not yet decided;
/// the byte-identical request is asked again and a later answer decides.
/// Neither is a failure: the file's steps (`FileStep`) resume where they
/// stopped, asked again this long after.
const FS_EAGAIN: i32 = -11;
const FS_EINPROGRESS: i32 = -36;
const OPEN_RETRY_MS: u64 = 20;
/// Body bytes the file provider has not taken yet (a WRITE answered short or
/// `EAGAIN`): at most one network frame's, plus the header buffer's early
/// bytes. The network is not read again until they are written.
const SPILL_SIZE: usize = 8192 + HDR_BUF_SIZE;

// Where opening the held request's file stands; each step is asked again
// until it decides, then the next runs.
const FO_IDLE: u8 = 0;
const FO_MKDIR: u8 = 1;
const FO_UNLINK: u8 = 2;
const FO_CREATE: u8 = 3;

/// An answer that is not a decision yet: asked again on a later step.
fn fs_pending(r: i32) -> bool {
    r == FS_EAGAIN || r == FS_EINPROGRESS
}

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
    /// The body is in and verified: its spilled bytes written, then FSYNC
    /// asked until it decides, then answered.
    Fsync = 6,
    /// An answer that discards the request's file: UNLINK asked until it
    /// decides, then `final_status` answered.
    Discard = 7,
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
    /// `pull_auth`: the store prefix whose records (`rk=<pull>;s=<ns>/<name>,…`)
    /// name the imagePullSecrets of the Pods a request's pull (field 25) serves.
    pull_auth: [u8; MAX_PULL_AUTH],
    pull_auth_len: u8,

    phase: Phase,
    inflight: u8,
    /// The held record's "already there" probe was not answered yet
    /// (`EAGAIN`): it is admitted again, from the record held, next step.
    probe_owed: u8,
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
    /// `dir` exists (its MKDIR decided); the file's open, mid-way (`FO_*`).
    dir_made: u8,
    file_step: u8,
    /// The answer a discarding reply gives once its UNLINK decides.
    final_status: u16,
    /// Body bytes taken off the wire but not yet by the file provider:
    /// `spill[spill_off..spill_len]`, written before anything else.
    spill_off: u32,
    spill_len: u32,

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

    /// The request's own registry (field 23), dialled and named in its Host
    /// header in place of `authority`; empty: `authority`.
    host: [u8; MAX_AUTHORITY],
    host_len: u8,
    host_port: u16,
    /// Whom the held token and Basic credential were won for: the request's
    /// registry, its credentials' name and its pull (fields 23, 24, 25). A
    /// request for another drops both — a token one Pod's Secret won never
    /// rides a pull it was not offered for.
    owner: u32,
    /// The credential candidate in use (0: none yet; see `cand_cred`), and
    /// the `Authorization: Basic` value it gave (empty: none).
    cand: u8,
    basic_len: u16,
    basic: [u8; BASIC_MAX],
    /// The origin asked Basic itself: the object request carries `basic`.
    basic_on: u8,
    /// A candidate's store read was not answered: the challenge (its
    /// scheme) is answered again, with the same candidate, after a backoff.
    cred_owed: u8,
    /// Scratch for a Secret and its decoded registry config.
    sec: [u8; SECRET_MAX],
    sec_cfg: [u8; SECRET_CONFIG_MAX],
    /// Auth: the leg on the wire, whether this request has had its one
    /// refresh with the credential in use (re-armed by every 2xx), the
    /// challenge (1 Bearer, 2 Basic), the token, and the realm of the last
    /// Bearer challenge (its authority, port, and path with the query).
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
    spill: [u8; SPILL_SIZE],
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::SCHEMA_MAX;
    use super::{MAX_AUTHORITY, MAX_DIR, MAX_KEY, MAX_PULL_AUTH};

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

        6, pull_auth, str, 0
            => |s, d, len| {
                let n = if len > MAX_PULL_AUTH { 0 } else { len };
                s.pull_auth_len = n as u8;
                if n > 0 { ptr_copy(s.pull_auth.as_mut_ptr(), d, n); }
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

/// Where the held request's object is asked, and its port: the request's
/// own registry (field 23), or the configured `authority`.
fn object_authority(s: &State) -> (&[u8], u16) {
    if s.host_len > 0 {
        (&s.host[..s.host_len as usize], s.host_port)
    } else {
        (authority(s), s.port)
    }
}

/// FNV-1a over `parts`, each closed by 0xFF: whom a credential is for.
fn owner_of(parts: [&[u8]; 3]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for p in parts {
        for &c in p.iter().chain(core::iter::once(&0xFFu8)) {
            h ^= c as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
    }
    h
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

/// Release the file's FD and drop what it had not taken. CLOSE never waits
/// and reports no writeback: a file that must be durable is fenced (FSYNC)
/// before this.
unsafe fn close_file(s: &mut State) {
    let sys = &*s.syscalls;
    if s.file_fd >= 0 {
        let mut z = [0u8; 4];
        (sys.provider_call)(s.file_fd, FS_CLOSE, z.as_mut_ptr(), 0);
        s.file_fd = -1;
    }
    s.spill_off = 0;
    s.spill_len = 0;
}

/// Write `data` through the file's FD while bytes move: what moved, which
/// may be short (the provider answered `EAGAIN`: nothing more now); `None`
/// on a refusal. Bounded: every turn moves at least one byte.
unsafe fn write_some(s: &State, data: *const u8, len: usize) -> Option<usize> {
    let sys = &*s.syscalls;
    let mut off = 0usize;
    while off < len {
        let wrote = (sys.provider_call)(s.file_fd, FS_WRITE, data.add(off) as *mut u8, len - off);
        if fs_pending(wrote) {
            break;
        }
        if wrote <= 0 {
            return None;
        }
        off += (wrote as usize).min(len - off);
    }
    Some(off)
}

/// Write what the provider had not taken. `Some(true)` once it is all
/// written; `None` on a refusal.
unsafe fn drain_spill(s: &mut State) -> Option<bool> {
    let off = s.spill_off as usize;
    let end = (s.spill_len as usize).min(SPILL_SIZE);
    if off >= end {
        s.spill_off = 0;
        s.spill_len = 0;
        return Some(true);
    }
    let moved = write_some(s, s.spill.as_ptr().add(off), end - off)?;
    if off + moved >= end {
        s.spill_off = 0;
        s.spill_len = 0;
        return Some(true);
    }
    s.spill_off = (off + moved) as u32;
    Some(false)
}

/// Answer the held request and free the slot.
unsafe fn reply(s: &mut State, status: u16) {
    close_conn(s);
    // Only a file THIS request created is discarded: a skipped request (304)
    // or one refused before its file was opened must not unlink what an
    // earlier request landed and verified. The UNLINK is asked until it
    // decides (`Phase::Discard`) before the answer goes.
    if s.sink == SINK_FILE as u8 && s.file_fd >= 0 {
        close_file(s);
        if status != 200 {
            s.final_status = status;
            s.phase = Phase::Discard;
            return;
        }
    }
    answer(s, status);
}

/// `Phase::Discard`: the discarded file's UNLINK, asked again until it
/// decides; then the answer.
unsafe fn discard_step(s: &mut State) {
    let sys = &*s.syscalls;
    let mut path = [0u8; MAX_DIR + 1 + MAX_FILE];
    let n = file_path(s, &mut path);
    let rc = if n > 0 {
        (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), n)
    } else {
        0
    };
    if n > 0 && fs_pending(rc) {
        return;
    }
    // A drop asked for is done when the name is gone — removed now, or not
    // there at all; any other refusal is the provider's to report (500).
    if s.sink == SINK_DROP as u8 {
        answer(s, if rc == 0 || rc == FS_ENOENT { 200 } else { 500 });
        return;
    }
    answer(s, s.final_status);
}

/// `Phase::Fsync`: the file's bytes durable before it is answered 200.
/// FSYNC is asked again, unchanged, until it decides.
unsafe fn fsync_step(s: &mut State) {
    if s.spill_len != 0 {
        return; // written first (module_step drains it)
    }
    let sys = &*s.syscalls;
    let mut z = [0u8; 4];
    let r = (sys.provider_call)(s.file_fd, FS_FSYNC, z.as_mut_ptr(), 0);
    if fs_pending(r) {
        return;
    }
    reply(s, if r < 0 { 500 } else { 200 });
}

/// The outcome record for the held request; the slot freed.
unsafe fn answer(s: &mut State, status: u16) {
    let sys = &*s.syscalls;
    let h = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let body = core::slice::from_raw_parts(s.body.as_ptr(), s.body_fill as usize);
    // The answer's own digest, for a body answered whole: what a tag
    // resolved to, so a chain keys the content by it.
    let sha = hasher_hex(&s.hasher);
    let sha_len = if status == 200 && s.sink == SINK_REPLY as u8 {
        sha.len()
    } else {
        0
    };
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
    field!(put_field(out, p, 5, TY_BYTES, &sha[..sha_len]));
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
    let cap = if s.status == 0 && s.body_received == 0 {
        MAX_CONNECT_ATTEMPTS
    } else {
        MAX_ATTEMPTS
    };
    if s.attempts >= cap {
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

/// Open the held request's file afresh: `dir` made (once), the file
/// unlinked, then created. Each step is asked again until it decides, and
/// the sequence resumes at the step that had not, never from the start.
/// `Some(true)` opened; `Some(false)` a step has not decided yet, and the
/// open resumes after `OPEN_RETRY_MS`; `None` it refused, answered 507.
unsafe fn open_file(s: &mut State) -> Option<bool> {
    let sys = &*s.syscalls;
    s.file_fd = -1;
    if s.file_step == FO_IDLE {
        s.file_step = if s.dir_len > 0 && s.dir_made == 0 {
            FO_MKDIR
        } else {
            FO_UNLINK
        };
    }
    if s.file_step == FO_MKDIR {
        let mut dir = [0u8; MAX_DIR];
        let dl = (s.dir_len as usize).min(MAX_DIR);
        dir[..dl].copy_from_slice(&s.dir[..dl]);
        if fs_pending((sys.provider_call)(-1, FS_MKDIR, dir.as_mut_ptr(), dl)) {
            return open_later(s);
        }
        // Made, or there already; a refusal is the create's to report.
        s.dir_made = 1;
        s.file_step = FO_UNLINK;
    }
    let mut path = [0u8; MAX_DIR + 1 + MAX_FILE];
    let pl = file_path(s, &mut path);
    if s.file_step == FO_UNLINK {
        if fs_pending((sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), pl)) {
            return open_later(s);
        }
        // Gone, or never there.
        s.file_step = FO_CREATE;
    }
    let fd = (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), pl);
    if fs_pending(fd) {
        return open_later(s);
    }
    s.file_step = FO_IDLE;
    if fd < 0 {
        log(s, 1, b"[http_fetch] cannot create the file");
        reply(s, 507);
        return None;
    }
    s.file_fd = fd;
    Some(true)
}

/// A file step that has not decided: resumed after `OPEN_RETRY_MS`.
unsafe fn open_later(s: &mut State) -> Option<bool> {
    s.backoff_ms = OPEN_RETRY_MS;
    s.state_start_ms = dev_millis(&*s.syscalls);
    s.phase = Phase::Backoff;
    Some(false)
}

/// Start the held request again from what it has: a Range file from its
/// position, anything else from nothing (its file truncated). False when
/// the file is not open yet (resumed later) or cannot be (answered 507).
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
        close_file(s);
        return open_file(s) == Some(true);
    }
    true
}

/// Whether the held request is a Range request (a chunked file).
fn ranged(s: &State) -> bool {
    s.sink == SINK_FILE as u8 && s.chunk_bytes > 0 && s.size > 0
}

unsafe fn build_request(s: &mut State) -> usize {
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
        // The credential in use (`cand_cred`); without one, the token
        // asked for is an anonymous one.
        if s.basic_len > 0 {
            o = append(tx, o, b"\r\nAuthorization: Basic ");
            o = append(
                tx,
                o,
                core::slice::from_raw_parts(s.basic.as_ptr(), s.basic_len as usize),
            );
        }
    } else {
        let h = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
        let mut path = [0u8; MAX_PATH];
        let pl = join(h, 3, 6, &mut path).unwrap_or(0);
        let accept = frame_bytes(h, 7);
        o = append(tx, 0, b"GET ");
        o = append(tx, o, &path[..pl]);
        o = append(tx, o, b" HTTP/1.1\r\nHost: ");
        o = append(tx, o, object_authority(&*st).0);
        if !accept.is_empty() && accept.len() <= MAX_ACCEPT {
            o = append(tx, o, b"\r\nAccept: ");
            o = append(tx, o, accept);
        }
        if s.basic_on == 1 && s.basic_len > 0 {
            o = append(tx, o, b"\r\nAuthorization: Basic ");
            o = append(
                tx,
                o,
                core::slice::from_raw_parts(s.basic.as_ptr(), s.basic_len as usize),
            );
        } else if s.token_len > 0 {
            o = append(tx, o, b"\r\nAuthorization: Bearer ");
            o = append(
                tx,
                o,
                core::slice::from_raw_parts(s.token.as_ptr(), s.token_len as usize),
            );
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
    let path: &[u8] = if slash < rest.len() {
        &rest[slash..]
    } else {
        b"/"
    };
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
    let mut sep = if path.iter().all(|&c| c != b'?') {
        b'?'
    } else {
        b'&'
    };
    // service, then scope — two calls, not a loop over an array of byte
    // strings (unrelocated in a PIC module's .rodata).
    o = query_param(
        &mut s.realm_path,
        o,
        &mut sep,
        b"service",
        auth_param(params, b"service"),
    );
    o = query_param(
        &mut s.realm_path,
        o,
        &mut sep,
        b"scope",
        auth_param(params, b"scope"),
    );
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
                // Bearer (the OCI token flow: 1), or Basic asked of the
                // origin itself (2).
                s.challenged = if take_challenge(s, v) {
                    1
                } else if v.len() >= 5 && v[..5].eq_ignore_ascii_case(b"Basic") {
                    2
                } else {
                    0
                };
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
        // Written while the provider takes bytes; the rest spilled, in
        // order, behind anything spilled before, and written on later steps.
        let moved = if s.spill_len == 0 {
            match write_some(s, data.as_ptr(), data.len()) {
                Some(m) => m,
                None => return false,
            }
        } else {
            0
        };
        let rest = data.get(moved..).unwrap_or(&[]);
        if !rest.is_empty() {
            let at = s.spill_len as usize;
            let Some(dst) = s.spill.get_mut(at..at + rest.len()) else {
                return false;
            };
            if s.spill_off != 0 {
                return false;
            }
            core::ptr::copy_nonoverlapping(rest.as_ptr(), dst.as_mut_ptr(), rest.len());
            s.spill_len = (at + rest.len()) as u32;
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

/// How a challenge is answered: a token from a realm, or Basic to the origin.
const CRED_BEARER: u8 = 1;
const CRED_BASIC: u8 = 2;

/// What `cand_cred` found for a candidate.
#[derive(Clone, Copy, PartialEq)]
enum Cand {
    /// A credential, in `basic`.
    Found,
    /// None: the anonymous candidate (a Bearer realm's anonymous token).
    Anon,
    /// No such candidate: every one was offered.
    Spent,
    /// A store read was not answered yet (`EAGAIN`): asked again shortly.
    Pending,
}

/// Candidate `idx` (1-based) for the held request, in the order a kubelet
/// offers them: the imagePullSecrets of the Pods its pull serves (field 25,
/// the `pull_auth` records naming it, each Secret's entry for the registry
/// named by field 24 — `rc_basic_for`), then the node's own `credentials`,
/// then none.
unsafe fn cand_cred(s: &mut State, idx: u8) -> Cand {
    let st = s as *mut State;
    let sys = &*(*st).syscalls;
    let h = core::slice::from_raw_parts((*st).held.as_ptr(), (*st).held_len as usize);
    // The selector and the registry's name, copied off the held record.
    let mut sel = [0u8; MAX_KEY];
    let sl = append(&mut sel, 0, frame_bytes(h, 25));
    let mut reg = [0u8; MAX_AUTHORITY];
    let rf = frame_bytes(h, 24);
    let rl = if rf.is_empty() {
        append(&mut reg, 0, object_authority(&*st).0)
    } else {
        append(&mut reg, 0, rf)
    };
    let (sel, reg) = (&sel[..sl], &reg[..rl]);
    let mut count = 0u8;
    if (*st).pull_auth_len > 0 && !sel.is_empty() {
        let store = Store::plain(sys, &(*st).nag);
        let prefix =
            core::slice::from_raw_parts((*st).pull_auth.as_ptr(), (*st).pull_auth_len as usize);
        let mut walk = ListWalk::new(prefix);
        while let Some(key) = walk.next(&store) {
            let mut rec = [0u8; 512];
            let Some(rn) = get_value(&store, key, &mut rec) else {
                if store.waiting() {
                    return Cand::Pending;
                }
                continue;
            };
            let Some((ns, names)) = rc_record(&rec[..rn], sel) else {
                continue;
            };
            for name in names.split(|&c| c == b',') {
                if name.is_empty() {
                    continue;
                }
                let mut sk = [0u8; MAX_KEY];
                let mut kl = append(&mut sk, 0, b"/secrets/");
                kl = append(&mut sk, kl, ns);
                kl = append(&mut sk, kl, b"/");
                kl = append(&mut sk, kl, name);
                if kl == sk.len() {
                    continue;
                }
                let Some(sn) = get_value(&store, &sk[..kl], &mut (*st).sec) else {
                    if store.waiting() {
                        return Cand::Pending;
                    }
                    continue;
                };
                let secret = core::slice::from_raw_parts((*st).sec.as_ptr(), sn);
                if let Some(bn) = rc_basic_for(secret, reg, &mut (*st).sec_cfg, &mut (*st).basic) {
                    count += 1;
                    if count == idx {
                        (*st).basic_len = bn as u16;
                        return Cand::Found;
                    }
                }
            }
        }
        if walk.pending || store.waiting() {
            return Cand::Pending;
        }
    }
    if (*st).credentials_len > 0 {
        let key =
            core::slice::from_raw_parts((*st).credentials.as_ptr(), (*st).credentials_len as usize);
        let mut cred = [0u8; MAX_CRED];
        match get_value_live(sys, &(*st).nag, key, &mut cred) {
            Got::Found(n) => {
                count += 1;
                if count == idx {
                    return match b64_encode(&cred[..n], &mut (*st).basic) {
                        Some(bn) => {
                            (*st).basic_len = bn as u16;
                            Cand::Found
                        }
                        None => Cand::Spent,
                    };
                }
            }
            Got::Pending => return Cand::Pending,
            Got::Absent => {}
        }
    }
    if idx == count + 1 {
        (*st).basic_len = 0;
        return Cand::Anon;
    }
    Cand::Spent
}

/// Answer a challenge (`scheme`) with the credential in use, or (`advance`)
/// the next candidate: a Bearer realm is asked for a token with it, a Basic
/// origin is asked again with it. Every candidate offered answers 401.
unsafe fn next_credential(s: &mut State, scheme: u8, advance: bool) {
    s.auth_tried = 1;
    let mut adv = advance || s.cand == 0;
    loop {
        if adv {
            if s.cand >= MAX_CANDIDATES {
                s.cand = 0;
                s.token_len = 0;
                s.basic_on = 0;
                reply(s, 401);
                return;
            }
            s.cand += 1;
        }
        match cand_cred(s, s.cand) {
            Cand::Pending => {
                // This same candidate, asked again shortly.
                s.cred_owed = scheme;
                s.backoff_ms = BACKOFF_INIT_MS;
                s.state_start_ms = dev_millis(&*s.syscalls);
                s.phase = Phase::Backoff;
                return;
            }
            Cand::Spent => {
                log(s, 1, b"[http_fetch] no credential the registry accepts");
                s.cand = 0;
                s.token_len = 0;
                s.basic_on = 0;
                reply(s, 401);
                return;
            }
            // Basic is not answered with nothing: the next one.
            Cand::Anon if scheme == CRED_BASIC => adv = true,
            Cand::Found | Cand::Anon => break,
        }
    }
    if scheme == CRED_BEARER {
        s.basic_on = 0;
        s.token_len = 0;
        s.leg = LEG_TOKEN;
        begin(s);
        return;
    }
    s.basic_on = 1;
    s.token_len = 0;
    if restart(s) {
        begin(s);
    }
}

/// The response body is complete: verify and answer, or chain the next chunk.
unsafe fn body_done(s: &mut State) {
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
            // The credential in use won none: the next one is offered.
            next_credential(s, CRED_BEARER, true);
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
    if s.status == 401 && s.challenged != 0 {
        // The origin wants credentials: a token from the realm its Bearer
        // challenge named, or Basic of its own. The credential in use is
        // offered again once after a 2xx (a token spent mid-transfer); any
        // other refusal moves on to the next candidate, and none left
        // answers the 401.
        s.body_fill = 0;
        let scheme = if s.challenged == 1 {
            CRED_BEARER
        } else {
            CRED_BASIC
        };
        next_credential(s, scheme, s.auth_tried != 0 || s.cand == 0);
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
        // Durable before it is answered: FSYNC decides, over later steps.
        s.phase = Phase::Fsync;
        fsync_step(s);
        return;
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
                    if s.tx_len == 0 {
                        // The credentials' read was not answered yet: the
                        // leg is asked again after a short backoff, which
                        // spends no attempt.
                        close_conn(s);
                        s.backoff_ms = BACKOFF_INIT_MS;
                        s.phase = Phase::Backoff;
                        return true;
                    }
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
    if s.probe_owed != 0 {
        s.probe_owed = 0;
        start(s);
        return;
    }
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
    start(s);
}

/// Start the transfer of the held record (or skip it).
unsafe fn start(s: &mut State) {
    let sys = &*s.syscalls;
    let sink = frame_i64(held(s), 2).unwrap_or(0);
    if sink != SINK_REPLY && sink != SINK_FILE && sink != SINK_DROP {
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
    s.file_step = FO_IDLE;
    s.spill_off = 0;
    s.spill_len = 0;
    s.cred_owed = 0;
    // A file dropped from the cache: its UNLINK, asked until it decides.
    if s.sink == SINK_DROP as u8 {
        s.final_status = 200;
        s.phase = Phase::Discard;
        return;
    }
    // Where it is asked: the request's own registry, or `authority`.
    let h = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let host = frame_bytes(h, 23);
    s.host_len = 0;
    if !host.is_empty() {
        match Target::parse(host) {
            Some((_, p)) if host.len() <= MAX_AUTHORITY => {
                ptr_copy(s.host.as_mut_ptr(), host.as_ptr(), host.len());
                s.host_len = host.len() as u8;
                s.host_port = p.unwrap_or(DEFAULT_PORT);
            }
            _ => {
                reply(s, 400);
                return;
            }
        }
    }
    // A token or a Basic credential won for another registry, credentials'
    // name or pull is not this request's.
    let who = owner_of([frame_bytes(h, 23), frame_bytes(h, 24), frame_bytes(h, 25)]);
    if who != s.owner {
        s.owner = who;
        s.token_len = 0;
        s.basic_len = 0;
        s.basic_on = 0;
        s.cand = 0;
    }
    // Nothing won and held: the candidates are offered from the first again
    // (a Pod may have brought credentials since the last refusal).
    if s.token_len == 0 && s.basic_on == 0 {
        s.cand = 0;
    }
    // Already there: answered without a byte on the wire.
    let mut k = [0u8; MAX_KEY];
    let (a, b) = (frame_bytes(held(s), 12), frame_bytes(held(s), 22));
    if !a.is_empty() && a.len() + b.len() <= MAX_KEY {
        k[..a.len()].copy_from_slice(a);
        k[a.len()..a.len() + b.len()].copy_from_slice(b);
        match head_rev(sys, &s.nag, &k[..a.len() + b.len()]) {
            Got::Found(_) => {
                reply(s, 304);
                return;
            }
            Got::Pending => {
                // Not answered yet: the record stays held and is admitted
                // again next step — not fetched again over a blob it has.
                s.inflight = 0;
                s.probe_owed = 1;
                return;
            }
            Got::Absent => {}
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
        s.dir_made = 0;
        s.file_step = FO_IDLE;
        s.spill_off = 0;
        s.spill_len = 0;
        s.held_len = 0;
        s.authority_len = 0;
        s.authority_over = 0;
        s.dir_len = 0;
        s.credentials_len = 0;
        s.pull_auth_len = 0;
        s.token_len = 0;
        s.host_len = 0;
        s.owner = 0;
        s.cand = 0;
        s.basic_len = 0;
        s.basic_on = 0;
        s.cred_owed = 0;
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
            s.resolved = 1;
        }
        if s.request_in < 0 || s.response_out < 0 || s.net_in < 0 || s.net_out < 0 {
            return 0;
        }
        // Body bytes the file provider had not taken go first; the wire is
        // not read again until they are written, and its timers do not run
        // while it waits on the file.
        if s.spill_len != 0 {
            match drain_spill(s) {
                None => reply(s, 507),
                Some(true) => {}
                Some(false) => {
                    if matches!(
                        s.phase,
                        Phase::WaitConnect | Phase::RecvHeader | Phase::RecvBody
                    ) {
                        s.state_start_ms = dev_millis(sys);
                    }
                }
            }
        }
        let mut drained = 0;
        while drained < 64 && s.spill_len == 0 && pump_net(s) {
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
                    object_authority(s)
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
            Phase::Fsync => fsync_step(s),
            Phase::Discard => discard_step(s),
            Phase::Backoff => {
                if dev_millis(sys).wrapping_sub(s.state_start_ms) >= s.backoff_ms
                    && s.cred_owed != 0
                {
                    let scheme = s.cred_owed;
                    s.cred_owed = 0;
                    next_credential(s, scheme, false);
                } else if dev_millis(sys).wrapping_sub(s.state_start_ms) >= s.backoff_ms {
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
