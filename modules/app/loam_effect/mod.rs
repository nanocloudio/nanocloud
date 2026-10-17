//! Chronicle connector for Loam: a volume effect in, a job of admin-wire
//! exchanges out, the typed outcome back as a record.
//!
//! See `manifest.toml` for the wire and the naming. The job engine is
//! `_shared/loam_ops.rs`, over loam-common's own codecs; the session with
//! Loam's admin gate is loam-common's `admin_client`, which presents this
//! node's capability before any request. This module keeps one job in flight,
//! keeps the session up, and maps names and outcomes.

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
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");

#[path = "../../../target/fluxor/loam-common/mechanics/loam_admin_wire.rs"]
mod admin;
#[path = "../../../target/fluxor/loam-common/mechanics/admin_client.rs"]
mod admin_client;
#[path = "../../../target/fluxor/loam-common/mechanics/loam_body_wire.rs"]
mod body_wire;
#[path = "../../../target/fluxor/loam-common/mechanics/loam_limits.rs"]
mod limits;
#[path = "../../../target/fluxor/loam-common/mechanics/loam_volume_map_wire.rs"]
mod map;
#[path = "../../../target/fluxor/loam-common/mechanics/loam_wire.rs"]
mod wire;

use admin_client as ac;

include!("../_shared/loam_ops.rs");

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_FIELD: u8 = 254;
const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const REC: usize = 8192;
/// The largest request: a full root page and its header.
const OUT: usize = map::MAX_PAGE_LEN + 64;
const _: () = assert!(OUT <= admin::REQUEST_MAX);
/// Longest capability file path a parameter carries.
const CAP_PATH_MAX: usize = 255;

/// The tag this module's dial carries through `tls`.
const DIAL_TAG: abi::contracts::net::net_proto::RequesterTag = 1;
/// Before dialing again after the transport failed or the capability file
/// could not be read.
const RETRY_MS: u64 = 1_000;
/// Before presenting again after the gate refused this node's capability:
/// an operator has to mint a new one.
const REFUSED_RETRY_MS: u64 = 30_000;

/// `logged_why` beyond the client's reasons: nothing said yet, and the
/// capability file unreadable.
const LOGGED_NOTHING: u8 = 0xFF;
const LOGGED_CAP_FILE: u8 = 0xFE;

/// The session with the gate.
const LINK_DOWN: u8 = 0;
const LINK_UP: u8 = 1;

const OP_LEASE_ACQUIRE: i64 = 1;
const OP_LEASE_RENEW: i64 = 2;
const OP_LEASE_RELEASE: i64 = 3;
const OP_LOAM_CREATE: i64 = 30;
const OP_LOAM_SNAPSHOT: i64 = 73;
const OP_LOAM_RESTORE: i64 = 74;
const OP_LOAM_SNAP_DELETE: i64 = 76;
const OP_LOAM_DELETE: i64 = 82;

const VOLUMES: &[u8] = b"/volumes/";
const SNAPSHOTS: &[u8] = b"/snapshots/";

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    admin_in: i32,
    response_out: i32,
    admin_out: i32,
    resolved: u8,
    /// A job is in flight.
    busy: u8,
    /// The job's request is with the client.
    handed: u8,
    /// 0 unchecked, 1 configured, 2 a required parameter is missing.
    configured: u8,

    lease_ttl_ms: u32,
    extent: u32,
    timeout_ms: u32,
    /// The gate's `host:port`, the capability file, the namespace root
    /// every volume lives under.
    admin: [u8; ac::AUTHORITY_MAX],
    admin_len: u8,
    cap_path: [u8; CAP_PATH_MAX],
    cap_path_len: u8,
    root: [u8; KEY_CAP],
    root_len: u8,

    /// The session: up (dialing, presenting or ready) or down until
    /// `retry_at`. A job meeting a down link waits for the next session
    /// within `timeout_ms`, unless `down_now` says no session will help
    /// without an operator (the gate refused the capability, or the file
    /// cannot be read): then it is answered `down_code` at once.
    link: u8,
    down_code: Code,
    down_now: u8,
    /// The last failure was logged; a run of the same one is not.
    logged_why: u8,
    retry_at: u64,

    /// The pipeline op being answered.
    op: i64,
    cid: u32,
    started: u64,
    /// The record being answered (`held[..held_len]`).
    held: [u8; REC],
    held_len: u16,
    /// The name the outcome reports: the created volume (10), or the
    /// snapshot or clone (14).
    name: [u8; 2 * KEY_CAP + 1],
    name_len: u16,

    job: LoamOp,
    out: [u8; OUT],
    out_len: u32,
    /// A finished reply `response_out` has not taken yet.
    reply: [u8; REC],
    reply_len: u16,
    /// Zeroed children for a new volume's root page.
    zeros: [[u8; 32]; map::PAGE_ENTRIES],

    /// The `tls` clear side's frames, read here and handed to the client,
    /// so a session's end leaves the stream's framing intact for the next.
    reader: ac::NetReader,
    client: ac::Client,
    cap_file: [u8; ac::CAP_FILE_MAX],
    /// Where the capability file's read stands (one step of it per poll;
    /// zeroed state is a load not yet begun).
    cap_load: ac::ChainFileLoad,

    answered: u32,
    dropped: u32,
}

mod params_def {
    use super::p_u32;
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, lease_ttl_ms, u32, 30000
            => |s, d, len| { s.lease_ttl_ms = p_u32(d, len, 0, 30000); };

        2, extent, u32, 4096
            => |s, d, len| { s.extent = p_u32(d, len, 0, 4096); };

        3, timeout_ms, u32, 10000
            => |s, d, len| { s.timeout_ms = p_u32(d, len, 0, 10000); };

        4, admin, str, 0
            => |s, d, len| { s.admin_len = super::set_str(&mut s.admin, d, len); };

        5, capability, str, 0
            => |s, d, len| { s.cap_path_len = super::set_str(&mut s.cap_path, d, len); };

        6, root, str, 0
            => |s, d, len| { s.root_len = super::set_str(&mut s.root, d, len); };
    }
}

/// A string parameter into `dst`; its length, or 0 (unset) when it does not
/// fit — a cut path or authority would name something else.
unsafe fn set_str(dst: &mut [u8], d: *const u8, len: usize) -> u8 {
    if len > dst.len() || len > u8::MAX as usize {
        return 0;
    }
    dst[..len].copy_from_slice(core::slice::from_raw_parts(d, len));
    len as u8
}

// ---- record frames ----

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

fn frame_i64(frame: &[u8], number: u8) -> i64 {
    match frame_field(frame, number) {
        Some((TY_I64, b)) if b.len() == 8 => i64::from_le_bytes(b.try_into().unwrap()),
        _ => 0,
    }
}

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, b)) => b,
        _ => &[],
    }
}

/// Byte length of the first record frame in `data`, or None while truncated.
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

fn put(out: &mut [u8], at: usize, number: u8, ty: u8, v: &[u8]) -> Option<usize> {
    if v.len() > u16::MAX as usize || at + 4 + v.len() > out.len() {
        return None;
    }
    out[at] = number;
    out[at + 1] = ty;
    out[at + 2..at + 4].copy_from_slice(&(v.len() as u16).to_le_bytes());
    out[at + 4..at + 4 + v.len()].copy_from_slice(v);
    Some(at + 4 + v.len())
}

// ---- naming ----

/// `<root>:<path>` split at its first ':'.
fn split_vol(vol: &[u8]) -> Option<(&[u8], &[u8])> {
    let at = vol.iter().position(|&b| b == b':')?;
    let (root, path) = (&vol[..at], &vol[at + 1..]);
    (!root.is_empty() && path.first() == Some(&b'/')).then_some((root, path))
}

/// `<ns>/<rest>` split at its first '/'.
fn split_key(key: &[u8]) -> Option<(&[u8], &[u8])> {
    let at = key.iter().position(|&b| b == b'/')?;
    Some((&key[..at], &key[at + 1..]))
}

/// `/<ns><kind><name>` into `out` — a namespace's volume or snapshot under
/// the cluster's root; the length, or None when a part is empty, the
/// namespace holds a '/', or it does not fit.
#[allow(
    clippy::manual_contains,
    reason = "a byte slice's `contains` calls memchr, which the PIC link does not carry"
)]
fn scoped(out: &mut [u8], ns: &[u8], kind: &[u8], name: &[u8]) -> Option<usize> {
    let n = 1 + ns.len() + kind.len() + name.len();
    if ns.is_empty() || name.is_empty() || ns.iter().any(|&b| b == b'/') || n > out.len() {
        return None;
    }
    out[0] = b'/';
    out[1..1 + ns.len()].copy_from_slice(ns);
    out[1 + ns.len()..1 + ns.len() + kind.len()].copy_from_slice(kind);
    out[1 + ns.len() + kind.len()..n].copy_from_slice(name);
    Some(n)
}

/// `scoped` of a `<ns>/<name>` key.
fn scoped_key(out: &mut [u8], kind: &[u8], key: &[u8]) -> Option<usize> {
    let (ns, name) = split_key(key)?;
    scoped(out, ns, kind, name)
}

/// A Kubernetes quantity of bytes, as a claim's `resources.requests.storage`
/// spells it: whole digits and an optional binary (`Ki` … `Ei`) or decimal
/// (`k` … `E`) suffix. A fraction is refused rather than rounded: the volume's
/// size is exactly what was asked or nothing.
fn quantity(b: &[u8]) -> Option<u64> {
    let digits = b.iter().take_while(|c| c.is_ascii_digit()).count();
    let n = decimal(&b[..digits])?;
    let unit: u64 = match &b[digits..] {
        b"" => 1,
        b"Ki" => 1 << 10,
        b"Mi" => 1 << 20,
        b"Gi" => 1 << 30,
        b"Ti" => 1 << 40,
        b"Pi" => 1 << 50,
        b"Ei" => 1 << 60,
        b"k" => 1_000,
        b"M" => 1_000_000,
        b"G" => 1_000_000_000,
        b"T" => 1_000_000_000_000,
        b"P" => 1_000_000_000_000_000,
        b"E" => 1_000_000_000_000_000_000,
        _ => return None,
    };
    n.checked_mul(unit)
}

fn decimal(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 19 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    Some(v)
}

/// The lease holder for a claim: the first 16 bytes of sha256(claim), the
/// one the node's volume graph writes under. A recovery holds the lease as
/// its own owner, `rc:<claim>`.
fn holder_of(claim: &[u8], recovery: bool) -> [u8; 16] {
    holder(if recovery { b"rc:" } else { &[] }, claim)
}

/// The holder a create, delete or bind takes its short lease under: never
/// the claim's writer, so a live writer lease refuses it.
fn admin_holder_of(claim: &[u8]) -> [u8; 16] {
    holder(b"loam_effect admin:", claim)
}

fn holder(prefix: &[u8], claim: &[u8]) -> [u8; 16] {
    let mut h = Sha256::new();
    h.update(prefix);
    h.update(claim);
    let d = h.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&d[..16]);
    out
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
/// null when `params_len == 0`).
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
    if syscalls.is_null() || state.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<State>() {
        return -2;
    }
    // The arena is zeroed: every buffer, the zero children and the client
    // included.
    let s = &mut *(state as *mut State);
    s.syscalls = syscalls as *const SyscallTable;
    s.request_in = in_chan;
    s.response_out = out_chan;
    s.admin_in = -1;
    s.admin_out = -1;
    s.job = LoamOp::empty();
    s.link = LINK_DOWN;
    s.down_code = Code::Unavailable;
    s.logged_why = LOGGED_NOTHING;
    params_def::set_defaults(s);
    params_def::parse_tlv(s, params, params_len);
    0
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
            s.admin_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.admin_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.resolved = 1;
        }
        if s.request_in < 0 || s.response_out < 0 || s.admin_out < 0 || s.admin_in < 0 {
            return 0;
        }
        if s.configured == 0 {
            configure(s, sys);
        }
        if s.configured == 1 {
            drive_link(s, sys);
        }
        if s.reply_len != 0 {
            flush_reply(s, sys);
            return 0;
        }
        if s.busy != 0 {
            pump(s, sys);
            return 0;
        }
        admit(s, sys);
        0
    }
}

unsafe fn log(sys: &SyscallTable, level: u8, m: &[u8]) {
    dev_log(sys, level, m.as_ptr(), m.len());
}

unsafe fn configure(s: &mut State, sys: &SyscallTable) {
    if s.admin_len == 0 || s.cap_path_len == 0 || s.root_len == 0 {
        log(
            sys,
            1,
            b"[loam_effect] FAULT: admin, capability and root are required; every effect is answered unavailable",
        );
        s.configured = 2;
        return;
    }
    s.configured = 1;
}

// ---- the session with the gate ----

/// Keep the session up: dial and present when it is down and due, drive it
/// while up, and hand it what `tls` sent. A session that fails answers the
/// job in flight and is dialed again after a pause.
unsafe fn drive_link(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys);
    if s.link == LINK_DOWN && now >= s.retry_at {
        open_link(s, sys, now);
    }
    if s.link == LINK_UP {
        ac::poll(&mut s.client, sys, now);
    }
    read_transport(s, sys);
    if s.link == LINK_UP && ac::has_failed(&s.client) {
        link_failed(s, sys, now);
    } else if s.link == LINK_UP && ac::is_ready(&s.client) && s.logged_why != ac::WHY_NONE {
        // Said once per recovery, so the failure that ended it is said again
        // if it recurs.
        log(
            sys,
            3,
            b"[loam_effect] session with the gate ready: capability granted",
        );
        s.logged_why = ac::WHY_NONE;
    }
}

/// A fresh client: the capability file read again (an operator may have
/// replaced it), then a dial through `tls`.
unsafe fn open_link(s: &mut State, sys: &SyscallTable, now: u64) {
    core::ptr::write_bytes(&mut s.client as *mut ac::Client, 0, 1);
    let path: &[u8] = &*(&s.cap_path[..s.cap_path_len as usize] as *const [u8]);
    match ac::load_chain_file(&mut s.client, sys, path, &mut s.cap_file, &mut s.cap_load) {
        // The file system has not answered yet: asked again next step, the
        // read continuing where it stopped.
        ac::LoadPoll::Pending => return,
        ac::LoadPoll::Loaded => {}
        ac::LoadPoll::Failed(code) => {
            if s.logged_why != LOGGED_CAP_FILE {
                // One call per arm: no message chosen among literals.
                match code {
                    ac::LOAD_UNOPENABLE => log(
                        sys,
                        1,
                        b"[loam_effect] the capability file cannot be opened",
                    ),
                    ac::LOAD_UNREADABLE => {
                        log(sys, 1, b"[loam_effect] the capability file cannot be read")
                    }
                    ac::LOAD_TOO_LARGE => log(
                        sys,
                        1,
                        b"[loam_effect] the capability file is larger than any set of chains",
                    ),
                    ac::LOAD_BAD_LINE => log(
                        sys,
                        1,
                        b"[loam_effect] the capability file holds a line that is not a chain",
                    ),
                    _ => log(sys, 1, b"[loam_effect] the capability file holds no chain"),
                }
                s.logged_why = LOGGED_CAP_FILE;
            }
            s.down_code = Code::Unavailable;
            s.down_now = 1;
            s.retry_at = now + RETRY_MS;
            return;
        }
    }
    s.down_now = 0;
    let authority: &[u8] = &*(&s.admin[..s.admin_len as usize] as *const [u8]);
    if !ac::init_net(
        &mut s.client,
        s.admin_in,
        s.admin_out,
        DIAL_TAG,
        authority,
        now,
    ) {
        s.retry_at = now + RETRY_MS;
        return;
    }
    // Frames come through this module's reader, which outlives a client.
    ac::share_transport(&mut s.client);
    s.link = LINK_UP;
}

/// Read `tls`'s clear side a frame at a time: to the client while one is
/// up and has room, discarded while none is (a closed session's last
/// frames).
unsafe fn read_transport(s: &mut State, sys: &SyscallTable) {
    for _ in 0..16 {
        if s.link == LINK_UP && !ac::has_room(&s.client) {
            return;
        }
        let reader = &mut *(&mut s.reader as *mut ac::NetReader);
        match ac::read_frame(reader, sys, s.admin_in) {
            ac::Frame::Whole(ty, payload) => {
                if s.link == LINK_UP {
                    ac::feed(&mut s.client, sys, ty, payload);
                }
            }
            ac::Frame::None => return,
            ac::Frame::Broken => {
                // `tls` sent a frame longer than net_proto allows: the
                // session cannot be followed.
                if s.link == LINK_UP {
                    ac::close(&mut s.client, sys);
                }
                return;
            }
        }
    }
}

/// The session ended: say why once, close it, answer the job it carried,
/// and dial again after a pause — a long one when the gate refused this
/// node's capability.
unsafe fn link_failed(s: &mut State, sys: &SyscallTable, now: u64) {
    let why = s.client.why;
    // A session the gate saw no peer on is dialed again soon: the gate can
    // read a presentation before `tls` has told it who the peer is.
    let refused = matches!(why, ac::WHY_REFUSED | ac::WHY_GRANTS_FULL);
    if why != s.logged_why {
        // One call per arm: a match yielding the message would lower to a
        // table of absolute pointers, which a PIC module cannot hold.
        match why {
            ac::WHY_REFUSED => log(
                sys,
                1,
                b"[loam_effect] the gate refused this node's capability",
            ),
            ac::WHY_UNAUTHENTICATED => log(
                sys,
                2,
                b"[loam_effect] the gate saw no authenticated peer on the session; dialing again",
            ),
            ac::WHY_GRANTS_FULL => log(
                sys,
                1,
                b"[loam_effect] the capability file holds more chains than a session may present",
            ),
            ac::WHY_TIMEOUT => log(
                sys,
                2,
                b"[loam_effect] the gate did not answer the session's setup",
            ),
            ac::WHY_PROTOCOL => log(
                sys,
                1,
                b"[loam_effect] the gate's stream could not be followed",
            ),
            _ => log(sys, 2, b"[loam_effect] the session with the gate ended"),
        }
        s.logged_why = why;
    }
    ac::close(&mut s.client, sys);
    s.link = LINK_DOWN;
    s.down_code = if refused {
        Code::Refused
    } else {
        Code::Unavailable
    };
    s.down_now = u8::from(refused);
    s.retry_at = now + if refused { REFUSED_RETRY_MS } else { RETRY_MS };
    if s.busy == 0 {
        return;
    }
    if refused {
        answer(s, sys, s.down_code);
    } else {
        // The job's current request goes again to the next session: every
        // step is idempotent at the gate (loam_ops.rs), and its timeout
        // still runs from when the job began.
        s.handed = 0;
    }
}

// ---- jobs ----

/// Take one record and start the job it names.
unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    let Some(len) = take_record(s, sys) else {
        return;
    };
    s.held_len = len as u16;
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), len);
    s.op = frame_i64(rec, 2);
    if s.configured != 1 {
        if is_loam_op(s.op) {
            answer(s, sys, Code::Unavailable);
        }
        return;
    }
    if !start(s, sys, rec) {
        return;
    }
    if s.link == LINK_DOWN && s.down_now != 0 {
        answer(s, sys, s.down_code);
        return;
    }
    s.busy = 1;
    s.started = dev_millis(sys);
    next_request(s, sys);
}

fn is_loam_op(op: i64) -> bool {
    matches!(
        op,
        OP_LEASE_ACQUIRE
            | OP_LEASE_RENEW
            | OP_LEASE_RELEASE
            | OP_LOAM_CREATE
            | OP_LOAM_SNAPSHOT
            | OP_LOAM_RESTORE
            | OP_LOAM_SNAP_DELETE
            | OP_LOAM_DELETE
    )
}

/// Set the job up for the record's op. False when the op is not Loam's (the
/// record is dropped) or the job cannot start (answered `failed` here).
unsafe fn start(s: &mut State, sys: &SyscallTable, rec: &[u8]) -> bool {
    if !is_loam_op(s.op) {
        s.dropped = s.dropped.wrapping_add(1);
        return false;
    }
    let claim = frame_bytes(rec, 3);
    let a1 = frame_bytes(rec, 4);
    let a2 = frame_bytes(rec, 5);
    let a3 = frame_bytes(rec, 6);
    let recovery = frame_bytes(rec, 8) == b"recovery";
    let root: &[u8] = &*(&s.root[..s.root_len as usize] as *const [u8]);
    s.name_len = 0;
    s.job = LoamOp::empty();
    s.job.holder = holder_of(claim, recovery);
    s.job.ttl_ms = s.lease_ttl_ms;
    let mut path = [0u8; KEY_CAP];
    let started = match s.op {
        OP_LEASE_ACQUIRE | OP_LEASE_RENEW | OP_LEASE_RELEASE => match split_vol(a1) {
            Some((vroot, p)) => {
                let job = match s.op {
                    OP_LEASE_ACQUIRE => Job::Acquire,
                    OP_LEASE_RENEW => Job::Renew,
                    _ => Job::Release,
                };
                s.job.fence = frame_i64(rec, 9) as u64;
                s.job.start(job, vroot, p)
            }
            None => false,
        },
        OP_LOAM_CREATE => match (scoped(&mut path, a1, VOLUMES, a2), quantity(a3)) {
            (Some(n), Some(size)) => {
                s.job.size = size;
                s.job.extent = s.extent;
                dev_csprng_fill(sys, s.job.volume_id.as_mut_ptr(), 16);
                set_name(s, root, &path[..n]) && s.job.start(Job::Create, root, &path[..n])
            }
            _ => false,
        },
        OP_LOAM_SNAPSHOT => match (split_vol(a1), scoped_key(&mut path, SNAPSHOTS, a2)) {
            (Some((src_root, src_path)), Some(n)) => {
                set_name(s, root, &path[..n])
                    && s.job.start(Job::Bind, root, &path[..n])
                    && s.job.set_source(src_root, src_path)
            }
            _ => false,
        },
        OP_LOAM_RESTORE => match (split_vol(a1), scoped_key(&mut path, VOLUMES, a2)) {
            (Some((src_root, src_path)), Some(n)) => {
                set_name(s, root, &path[..n])
                    && s.job.start(Job::Bind, root, &path[..n])
                    && s.job.set_source(src_root, src_path)
            }
            _ => false,
        },
        OP_LOAM_SNAP_DELETE => match scoped_key(&mut path, SNAPSHOTS, a1) {
            Some(n) => s.job.start(Job::Delete, root, &path[..n]),
            None => false,
        },
        _ => match split_vol(a1) {
            Some((vroot, p)) => s.job.start(Job::Delete, vroot, p),
            None => false,
        },
    };
    if !started {
        answer(s, sys, Code::Failed);
        return false;
    }
    if !matches!(s.op, OP_LEASE_ACQUIRE | OP_LEASE_RENEW | OP_LEASE_RELEASE) {
        s.job.holder = admin_holder_of(claim);
    }
    true
}

/// Remember `<root>:<path>` as the name the outcome reports.
fn set_name(s: &mut State, root: &[u8], path: &[u8]) -> bool {
    let n = root.len() + 1 + path.len();
    if n > s.name.len() {
        return false;
    }
    s.name[..root.len()].copy_from_slice(root);
    s.name[root.len()] = b':';
    s.name[root.len() + 1..n].copy_from_slice(path);
    s.name_len = n as u16;
    true
}

/// Build the job's next request into `out` and hand it to the client.
unsafe fn next_request(s: &mut State, sys: &SyscallTable) {
    s.cid = s.cid.wrapping_add(1).max(1);
    let now = dev_millis(sys) as i64;
    match s.job.request(&mut s.out, s.cid, now, &s.zeros) {
        Some(n) => {
            s.out_len = n as u32;
            s.handed = 0;
            hand(s, sys);
        }
        None => answer(s, sys, Code::Failed),
    }
}

/// Hand the built request over once the session can take one: after the
/// capability is granted, one request at a time.
unsafe fn hand(s: &mut State, sys: &SyscallTable) {
    if s.handed != 0 || s.link != LINK_UP || !ac::can_send(&s.client) {
        return;
    }
    let frame: &[u8] = &*(&s.out[..s.out_len as usize] as *const [u8]);
    if ac::send(&mut s.client, sys, frame) {
        s.handed = 1;
    }
}

/// Hand the request over, collect its ack, and act on it.
unsafe fn pump(s: &mut State, sys: &SyscallTable) {
    hand(s, sys);
    while s.link == LINK_UP {
        let Some(ack) = ac::recv(&mut s.client) else {
            break;
        };
        // The ack stays in the client until `take`; nothing below receives
        // another before this one is read.
        let ack: &[u8] = &*(ack as *const [u8]);
        let mine = s.handed != 0
            && ack.len() >= 5
            && u32::from_le_bytes([ack[1], ack[2], ack[3], ack[4]]) == s.cid;
        // An ack that is not the job's is a job answered on its timeout.
        let progress = mine.then(|| s.job.on_ack(ack));
        ac::take(&mut s.client);
        match progress {
            Some(Progress::Next) => {
                next_request(s, sys);
                return;
            }
            Some(Progress::Done(code)) => {
                answer(s, sys, code);
                return;
            }
            None => continue,
        }
    }
    if s.busy != 0 && dev_millis(sys).saturating_sub(s.started) > u64::from(s.timeout_ms) {
        answer(s, sys, Code::Unavailable);
    }
}

fn code_name(c: Code) -> &'static [u8] {
    match c {
        Code::Ok => b"ok",
        Code::Held => b"held",
        Code::Lost => b"lost",
        Code::Refused => b"refused",
        Code::Failed => b"failed",
        Code::Unavailable => b"unavailable",
    }
}

/// Build the outcome record for the held request and end the job.
unsafe fn answer(s: &mut State, sys: &SyscallTable, code: Code) {
    s.busy = 0;
    s.out_len = 0;
    s.handed = 0;
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let ok = code == Code::Ok;
    let name = core::slice::from_raw_parts(s.name.as_ptr(), s.name_len as usize);
    let lease = matches!(s.op, OP_LEASE_ACQUIRE | OP_LEASE_RENEW) && ok;
    let fence = if s.op == OP_LEASE_ACQUIRE && ok {
        s.job.fence as i64
    } else {
        0
    };
    let lease_exp = if lease { s.job.lease_exp } else { 0 };
    let vol: &[u8] = if s.op == OP_LOAM_CREATE && ok {
        name
    } else {
        &[]
    };
    let r1: &[u8] = if matches!(s.op, OP_LOAM_SNAPSHOT | OP_LOAM_RESTORE) && ok {
        name
    } else {
        &[]
    };
    let mut out = [0u8; REC];
    let mut p = 1usize;
    let mut n = 0u8;
    let mut f = |out: &mut [u8], num: u8, ty: u8, v: &[u8]| -> bool {
        match put(out, p, num, ty, v) {
            Some(q) => {
                p = q;
                n += 1;
                true
            }
            None => false,
        }
    };
    let fits = f(&mut out, 1, TY_I64, &frame_i64(held, 1).to_le_bytes())
        && f(&mut out, 2, TY_I64, &s.op.to_le_bytes())
        && f(&mut out, 3, TY_BYTES, frame_bytes(held, 3))
        && f(&mut out, 4, TY_BYTES, code_name(code))
        && f(&mut out, 5, TY_I64, &fence.to_le_bytes())
        && f(&mut out, 6, TY_I64, &lease_exp.to_le_bytes())
        && f(&mut out, 7, TY_I64, &0i64.to_le_bytes())
        && f(&mut out, 8, TY_BYTES, &[])
        && f(&mut out, 9, TY_BYTES, &[])
        && f(&mut out, 10, TY_BYTES, vol)
        && f(&mut out, 11, TY_BYTES, &[])
        && f(&mut out, 12, TY_BYTES, &[])
        && f(&mut out, 13, TY_I64, &0i64.to_le_bytes())
        && f(&mut out, 14, TY_BYTES, r1)
        && f(&mut out, 15, TY_BYTES, &[])
        && f(&mut out, 16, TY_BYTES, &[])
        && match frame_field(held, CARRY_FIELD) {
            Some((ty, v)) => f(&mut out, CARRY_FIELD, ty, v),
            None => true,
        };
    if !fits {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    out[0] = n;
    s.reply[..p].copy_from_slice(&out[..p]);
    s.reply_len = p as u16;
    flush_reply(s, sys);
}

/// Put the finished outcome on `response_out`, or keep it for the next step.
unsafe fn flush_reply(s: &mut State, sys: &SyscallTable) {
    let n = s.reply_len as usize;
    if (sys.channel_write)(s.response_out, s.reply.as_ptr(), n) > 0 {
        s.reply_len = 0;
        s.answered = s.answered.wrapping_add(1);
    }
}

/// Take one whole record off `request_in` into `held`: a mailbox cannot be
/// peeked (EINVAL) and is read whole; a byte stream is self-framed.
unsafe fn take_record(s: &mut State, sys: &SyscallTable) -> Option<usize> {
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(s.request_in, s.held.as_mut_ptr(), REC);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REC)
    } else if pk <= 0 {
        return None;
    } else {
        match frame_len(&s.held[..pk as usize]) {
            Some(fl) => (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), fl),
            None if (pk as usize) < REC => return None,
            None => {
                (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REC);
                s.dropped = s.dropped.wrapping_add(1);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}
