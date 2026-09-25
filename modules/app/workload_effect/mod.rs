//! Chronicle connector for the `workload` contract (0x1A) and its host-process
//! companion (0x1B).
//!
//! See `manifest.toml` for the record layouts. The chains decide what happens
//! to a sandbox; this performs the op, keeps the one thing that cannot live in
//! the store — the live workload handles — and reports what it observes:
//! lifecycle transitions (WAIT), output (READ), exec results and PTY traffic.

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
include!("../_shared/argv.rs");

const WL_CREATE: u32 = 0x1A00;
const WL_START: u32 = 0x1A01;
const WL_WAIT: u32 = 0x1A02;
const WL_SIGNAL: u32 = 0x1A03;
const WL_DESTROY: u32 = 0x1A04;
const WL_PAUSE: u32 = 0x1A0B;
const WL_RESUME: u32 = 0x1A0C;
const WL_CAPS: u32 = 0x1AFF;
const WL_READ: u32 = 0x1B01;
const WL_EXEC: u32 = 0x1B02;
const WL_TTY_OPEN: u32 = 0x1B03;
const WL_TTY_STEP: u32 = 0x1B04;
const WL_TTY_CLOSE: u32 = 0x1B06;

const CAPS_OP_SIGNAL: u16 = 1 << 3;
const CAPS_OP_PAUSE: u16 = 1 << 4;
const SIG_TERM: u32 = 1;
const SIG_KILL: u32 = 2;

const WL_CREATE_HEADER: usize = 68;
const POSTURE_SHARED: u8 = 0;
const SOURCE_HOST_PROCESS: u8 = 1;
const NET_ISO_OWN: u8 = 1;
const NET_FAM_IPV4: u8 = 4;
const OPT_REQUIRED: u8 = 0x01;

const ST_RUNNING: u8 = 0;
const ST_EXITED: u8 = 1;
const ST_SIGNALLED: u8 = 2;
const ST_PAUSED: u8 = 3;

const OP_CREATE: i64 = 1;
const OP_START: i64 = 2;
const OP_DESTROY: i64 = 3;
const OP_SIGNAL: i64 = 4;
const OP_PAUSE: i64 = 5;
const OP_RESUME: i64 = 6;
const OP_EXEC: i64 = 7;
const OP_TTY_OPEN: i64 = 8;
const OP_TTY_CLOSE: i64 = 9;
const OP_TTY_IN: i64 = 10;

const EV_STATUS: i64 = 1;
const EV_LOG: i64 = 2;
const EV_EXEC: i64 = 3;
const EV_TTY_OUT: i64 = 4;
const EV_TTY_STATUS: i64 = 5;
const EV_BOOT: i64 = 6;
const EV_TTY_IN: i64 = 7;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
/// The record's carried context (chronicle `CARRY_FIELD`): one nested
/// message, echoed unchanged beside the legacy 30..=39 range.
const CARRY_FIELD: u8 = 254;
const CHAN_EINVAL: i32 = -22;

const REQ_BUF: usize = 8192;
const EV_BUF: usize = 8192;
const MAX_ID: usize = 128;
const MAX_SB: usize = 16;
const MAX_TTY: usize = 4;
const MAX_SRC: usize = 4096;
const MAX_OPTS: usize = 1024;
const LOG_READ: usize = 512;
const EXEC_CAP: usize = 2048;
const TTY_CAP: usize = 4096;
const TTY_IN_CAP: usize = 1024;
const INPUTS: usize = 5;
const EXEC_SEEN: usize = 16;

#[repr(C)]
#[derive(Clone, Copy)]
struct Sandbox {
    in_use: bool,
    started: bool,
    exited: bool,
    paused: bool,
    sig_sent: u8,
    pause_sent: u8,
    id_len: u8,
    handle: i32,
    /// The last lifecycle label reported, so a re-offered CREATE for a live id
    /// re-reports rather than creates twice.
    last: u8,
    id: [u8; MAX_ID],
}

const SB_EMPTY: Sandbox = Sandbox {
    in_use: false,
    started: false,
    exited: false,
    paused: false,
    sig_sent: 0,
    pause_sent: 0,
    id_len: 0,
    handle: -1,
    last: 0,
    id: [0; MAX_ID],
};

#[repr(C)]
#[derive(Clone, Copy)]
struct Tty {
    in_use: bool,
    sid: i32,
    tail_len: u8,
    tail: [u8; MAX_ID],
    stdin_len: u16,
    /// The revision of the last input taken (op field 5): the same bytes
    /// re-offered before their key is cleared are not typed twice.
    in_rev: i64,
    stdin: [u8; TTY_IN_CAP],
}

const TTY_EMPTY: Tty = Tty {
    in_use: false,
    sid: -1,
    tail_len: 0,
    tail: [0; MAX_ID],
    stdin_len: 0,
    in_rev: 0,
    stdin: [0; TTY_IN_CAP],
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    inputs: [i32; INPUTS],
    event_out: i32,
    resolved: u8,
    caps_ops: u16,
    next_input: u8,
    boot: [u8; 20],
    boot_len: u8,
    booted: u8,

    sandboxes: [Sandbox; MAX_SB],
    ttys: [Tty; MAX_TTY],
    /// The exec requests already run (by their name, carried at 30), so a
    /// level-triggered re-offer before its result lands does not run twice.
    execs: [[u8; MAX_ID]; EXEC_SEEN],
    /// Sessions that ended in this boot: a re-offered open between the end and
    /// its recorded status must not run the command again.
    tty_done: [[u8; MAX_ID]; EXEC_SEEN],
    tty_done_lens: [u8; EXEC_SEEN],
    tty_done_next: u8,
    exec_lens: [u8; EXEC_SEEN],
    /// The revision each was read at: a request name the caller reuses (a
    /// probe's) runs again once rewritten.
    exec_revs: [i64; EXEC_SEEN],
    exec_next: u8,

    /// An event the ring has not taken (all or the unsent tail): delivered
    /// before anything else happens, so no event is ever lost or torn.
    pend: [u8; EV_BUF],
    pend_at: u16,
    pend_len: u16,

    req: [u8; REQ_BUF],
    ev: [u8; EV_BUF],
}

// ---- record helpers ----

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

fn parse_u32(b: &[u8]) -> u32 {
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = (v * 10 + (c - b'0') as u64).min(u32::MAX as u64);
    }
    v as u32
}

fn parse_ipv4(v: &[u8]) -> Option<[u8; 4]> {
    let mut out = [0u8; 4];
    let mut n = 0usize;
    for part in v.split(|&c| c == b'.') {
        if n == 4 || part.is_empty() || part.len() > 3 {
            return None;
        }
        let x = parse_u32(part);
        if x > 255 || !part.iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        out[n] = x as u8;
        n += 1;
    }
    (n == 4).then_some(out)
}

// ---- events ----

impl State {
    fn can_emit(&self) -> bool {
        self.pend_len == 0
    }
}

/// Deliver what a partial write still owes. True when nothing is owed.
unsafe fn flush(s: &mut State) -> bool {
    if s.pend_len == 0 {
        return true;
    }
    let sys = &*s.syscalls;
    let (at, len) = (s.pend_at as usize, s.pend_len as usize);
    let w = (sys.channel_write)(s.event_out, s.pend.as_ptr().add(at), len - at);
    if w > 0 {
        s.pend_at += w as u16;
    }
    if s.pend_at as usize >= len {
        s.pend_at = 0;
        s.pend_len = 0;
        true
    } else {
        false
    }
}

/// Emit one event. It is written, or held whole in `pend` (callers check
/// `can_emit` first, so there is always room to hold it).
#[allow(
    clippy::too_many_arguments,
    reason = "one event record, field by field"
)]
unsafe fn emit(
    s: &mut State,
    cid: i64,
    kind: i64,
    id: &[u8],
    label: &[u8],
    code: i64,
    bytes: &[u8],
    carry_from: Option<&[u8]>,
) {
    let sys = &*s.syscalls;
    let ev = &mut s.ev;
    let mut p = 1usize;
    let mut n = 0u8;
    macro_rules! field {
        ($e:expr) => {
            match $e {
                Some(v) => {
                    p = v;
                    n += 1;
                }
                None => return,
            }
        };
    }
    field!(put_field(ev, p, 1, TY_I64, &cid.to_le_bytes()));
    field!(put_field(ev, p, 2, TY_I64, &kind.to_le_bytes()));
    field!(put_field(ev, p, 3, TY_BYTES, id));
    field!(put_field(ev, p, 4, TY_BYTES, label));
    field!(put_field(ev, p, 5, TY_I64, &code.to_le_bytes()));
    field!(put_field(ev, p, 6, TY_BYTES, bytes));
    field!(put_field(
        ev,
        p,
        7,
        TY_BYTES,
        &s.boot[..s.boot_len as usize]
    ));
    // The code again as decimal text: a record's value parts are text, and a
    // decision cannot render a number.
    let mut digits = [0u8; 21];
    let dn = {
        let mut tmp = [0u8; 21];
        let mut i = tmp.len();
        let mut u = code.unsigned_abs();
        loop {
            i -= 1;
            tmp[i] = b'0' + (u % 10) as u8;
            u /= 10;
            if u == 0 {
                break;
            }
        }
        if code < 0 {
            i -= 1;
            tmp[i] = b'-';
        }
        let n = tmp.len() - i;
        digits[..n].copy_from_slice(&tmp[i..]);
        n
    };
    field!(put_field(ev, p, 8, TY_BYTES, &digits[..dn]));
    if let Some(req) = carry_from {
        for f in (CARRY_LO..=CARRY_HI).chain(core::iter::once(CARRY_FIELD)) {
            if let Some((ty, v)) = frame_field(req, f) {
                field!(put_field(ev, p, f, ty, v));
            }
        }
    }
    ev[0] = n;
    let w = (sys.channel_write)(s.event_out, s.ev.as_ptr(), p);
    let w = if w < 0 { 0 } else { w as usize };
    if w < p {
        s.pend[..p - w].copy_from_slice(&s.ev[w..p]);
        s.pend_at = 0;
        s.pend_len = (p - w) as u16;
    }
}

fn label_of(state: u8, code: i32) -> &'static [u8] {
    match state {
        ST_EXITED if code == 0 => b"exited",
        ST_EXITED => b"failed",
        ST_SIGNALLED => b"killed",
        _ => b"failed",
    }
}

// ---- slots ----

fn find_slot(s: &State, id: &[u8]) -> Option<usize> {
    s.sandboxes
        .iter()
        .position(|sb| sb.in_use && &sb.id[..sb.id_len as usize] == id)
}

fn find_tty(s: &State, tail: &[u8]) -> Option<usize> {
    s.ttys
        .iter()
        .position(|t| t.in_use && &t.tail[..t.tail_len as usize] == tail)
}

/// `[isolate:u8][rootfs_len:u16 LE][rootfs][argv NUL-separated]`. `cmd` is
/// a record's argv: tokens separated by whitespace, each with `%XX` standing
/// for a byte it cannot carry raw (a space inside `sh -c "a; b"`), decoded
/// here.
fn spawn_src(buf: &mut [u8], cmd: &[u8], rootfs: &[u8], isolate: bool) -> Option<usize> {
    if 3 + rootfs.len() > buf.len() {
        return None;
    }
    buf[0] = isolate as u8;
    buf[1..3].copy_from_slice(&(rootfs.len() as u16).to_le_bytes());
    buf[3..3 + rootfs.len()].copy_from_slice(rootfs);
    let mut p = 3 + rootfs.len();
    let mut any = false;
    for tok in argv_tokens(cmd) {
        if any {
            *buf.get_mut(p)? = 0;
            p += 1;
        }
        p += argv_decode(tok, buf.get_mut(p..)?)?;
        any = true;
    }
    any.then_some(p)
}

/// The Tier-2 `mount`/`bind` entries for `<src>:<dst>[:ro],…`. None when an
/// entry is malformed — a volume that cannot be stated is refused, not dropped.
fn bind_options(binds: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0usize;
    if binds.is_empty() {
        return Some(0);
    }
    for item in binds.split(|&c| c == b',') {
        // `name=<bind>` (a listing of rows) or the bind alone.
        let item = match item.iter().position(|&c| c == b'=') {
            Some(e) => &item[e + 1..],
            None => item,
        };
        let mut parts = item.split(|&c| c == b':');
        let (Some(src), Some(dst)) = (parts.next(), parts.next()) else {
            return None;
        };
        let ro = parts.next() == Some(b"ro");
        if src.is_empty() || dst.is_empty() {
            return None;
        }
        let val_len = src.len() + 1 + dst.len() + 1 + 1;
        let need = 1 + 5 + 1 + 4 + 1 + 2 + val_len;
        if p + need > out.len() {
            return None;
        }
        out[p] = 5;
        out[p + 1..p + 6].copy_from_slice(b"mount");
        out[p + 6] = 4;
        out[p + 7..p + 11].copy_from_slice(b"bind");
        out[p + 11] = OPT_REQUIRED;
        out[p + 12..p + 14].copy_from_slice(&(val_len as u16).to_le_bytes());
        let mut q = p + 14;
        out[q..q + src.len()].copy_from_slice(src);
        q += src.len();
        out[q] = 0;
        q += 1;
        out[q..q + dst.len()].copy_from_slice(dst);
        q += dst.len();
        out[q] = 0;
        q += 1;
        out[q] = if ro { b'1' } else { b'0' };
        p = q + 1;
    }
    Some(p)
}

// ---- ops ----

unsafe fn op_create(s: &mut State, req: &[u8], cid: i64, id: &[u8]) {
    let sys = &*s.syscalls;
    if let Some(i) = find_slot(s, id) {
        // Already live: report what it is rather than create it twice.
        let sb = s.sandboxes[i];
        let label: &[u8] = if sb.started { b"running" } else { b"created" };
        let _ = sb;
        emit(s, cid, EV_STATUS, id, label, 0, &[], Some(req));
        return;
    }
    if id.len() > MAX_ID {
        emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
        return;
    }
    let cmd = frame_bytes(req, 4);
    let rootfs = frame_bytes(req, 5);
    let isolate = frame_bytes(req, 6) == b"1";
    let tasks = parse_u32(frame_bytes(req, 7));
    let addr = frame_bytes(req, 8);
    let prefix = parse_u32(frame_bytes(req, 9)).min(32) as u8;
    let mut src = [0u8; MAX_SRC];
    let mut opts = [0u8; MAX_OPTS];
    let (Some(sl), Some(ol)) = (
        spawn_src(&mut src, cmd, rootfs, isolate),
        bind_options(frame_bytes(req, 10), &mut opts),
    ) else {
        emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
        return;
    };
    let mut arg = [0u8; WL_CREATE_HEADER + MAX_SRC + MAX_OPTS];
    arg[16] = POSTURE_SHARED;
    arg[17] = SOURCE_HOST_PROCESS;
    arg[32..36].copy_from_slice(&tasks.to_le_bytes());
    arg[40..42].copy_from_slice(&(sl as u16).to_le_bytes());
    arg[44..48].copy_from_slice(&(ol as u32).to_le_bytes());
    if !addr.is_empty() {
        let Some(a) = parse_ipv4(addr) else {
            emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
            return;
        };
        arg[18] = NET_ISO_OWN;
        arg[48] = NET_FAM_IPV4;
        arg[49] = if prefix == 0 { 24 } else { prefix };
        arg[52..56].copy_from_slice(&a);
    }
    arg[WL_CREATE_HEADER..WL_CREATE_HEADER + sl].copy_from_slice(&src[..sl]);
    arg[WL_CREATE_HEADER + sl..WL_CREATE_HEADER + sl + ol].copy_from_slice(&opts[..ol]);
    let handle = (sys.provider_call)(-1, WL_CREATE, arg.as_mut_ptr(), WL_CREATE_HEADER + sl + ol);
    if handle < 0 {
        emit(
            s,
            cid,
            EV_STATUS,
            id,
            b"failed",
            handle as i64,
            &[],
            Some(req),
        );
        return;
    }
    let Some(i) = s.sandboxes.iter().position(|sb| !sb.in_use) else {
        let mut z = [0u8; 1];
        (sys.provider_call)(handle, WL_DESTROY, z.as_mut_ptr(), 0);
        emit(s, cid, EV_STATUS, id, b"failed", -12, &[], Some(req));
        return;
    };
    let sb = &mut s.sandboxes[i];
    *sb = SB_EMPTY;
    sb.in_use = true;
    sb.handle = handle;
    sb.id_len = id.len() as u8;
    sb.id[..id.len()].copy_from_slice(id);
    emit(s, cid, EV_STATUS, id, b"created", 0, &[], Some(req));
}

unsafe fn op(s: &mut State, req: &[u8]) {
    let sys = &*s.syscalls;
    let cid = frame_i64(req, 1).unwrap_or(0);
    let opc = frame_i64(req, 2).unwrap_or(0);
    let mut idb = [0u8; MAX_ID];
    let idr = frame_bytes(req, 3);
    let il = idr.len().min(MAX_ID);
    idb[..il].copy_from_slice(&idr[..il]);
    let id = &idb[..il];
    let slot = find_slot(s, id);
    let mut z = [0u8; 1];
    match opc {
        OP_CREATE => op_create(s, req, cid, id),
        OP_START => {
            if let Some(i) = slot {
                if !s.sandboxes[i].started
                    && (sys.provider_call)(s.sandboxes[i].handle, WL_START, z.as_mut_ptr(), 0) >= 0
                {
                    s.sandboxes[i].started = true;
                    emit(s, cid, EV_STATUS, id, b"running", 0, &[], Some(req));
                }
            }
        }
        OP_DESTROY => {
            if let Some(i) = slot {
                (sys.provider_call)(s.sandboxes[i].handle, WL_DESTROY, z.as_mut_ptr(), 0);
                s.sandboxes[i] = SB_EMPTY;
                emit(s, cid, EV_STATUS, id, b"destroyed", 0, &[], Some(req));
            }
        }
        OP_SIGNAL => {
            let want: u8 = match frame_bytes(req, 4) {
                b"term" => 1,
                b"kill" => 2,
                _ => 0,
            };
            if let Some(i) = slot {
                let sb = s.sandboxes[i];
                if want > 0
                    && s.caps_ops & CAPS_OP_SIGNAL != 0
                    && sb.started
                    && !sb.exited
                    && sb.sig_sent < want
                {
                    let mut a = (if want == 2 { SIG_KILL } else { SIG_TERM }).to_le_bytes();
                    if (sys.provider_call)(sb.handle, WL_SIGNAL, a.as_mut_ptr(), 4) >= 0 {
                        s.sandboxes[i].sig_sent = want;
                    }
                }
            }
        }
        OP_PAUSE | OP_RESUME => {
            let want: u8 = if opc == OP_PAUSE { 1 } else { 2 };
            if let Some(i) = slot {
                let sb = s.sandboxes[i];
                if s.caps_ops & CAPS_OP_PAUSE != 0
                    && sb.started
                    && !sb.exited
                    && sb.pause_sent != want
                {
                    let o = if want == 1 { WL_PAUSE } else { WL_RESUME };
                    if (sys.provider_call)(sb.handle, o, z.as_mut_ptr(), 1) >= 0 {
                        s.sandboxes[i].pause_sent = want;
                    }
                }
            }
        }
        OP_EXEC => {
            let tag = match frame_field(req, CARRY_LO) {
                Some((TY_BYTES, t)) if !t.is_empty() && t.len() <= MAX_ID => t,
                _ => &[][..],
            };
            let rev = frame_i64(req, 11).unwrap_or(0);
            if !tag.is_empty() {
                let seen =
                    (0..EXEC_SEEN).position(|k| &s.execs[k][..s.exec_lens[k] as usize] == tag);
                match seen {
                    Some(k) if s.exec_revs[k] == rev => return,
                    Some(k) => s.exec_revs[k] = rev,
                    None => {
                        let k = s.exec_next as usize % EXEC_SEEN;
                        s.execs[k][..tag.len()].copy_from_slice(tag);
                        s.exec_lens[k] = tag.len() as u8;
                        s.exec_revs[k] = rev;
                        s.exec_next = s.exec_next.wrapping_add(1);
                    }
                }
            }
            // A sandbox that is not live answers 255 rather than hanging the
            // caller (a probe must fail deterministically).
            let mut buf = [0u8; EXEC_CAP];
            let (code, out_len) = match slot {
                Some(i) if s.sandboxes[i].started && !s.sandboxes[i].exited => {
                    let cmd = frame_bytes(req, 4);
                    if 4 + cmd.len() + 1 > buf.len() {
                        (255, 0)
                    } else {
                        buf[0..4].copy_from_slice(&s.sandboxes[i].handle.to_le_bytes());
                        buf[4..4 + cmd.len()].copy_from_slice(cmd);
                        buf[4 + cmd.len()] = 0;
                        let c = (sys.provider_call)(-1, WL_EXEC, buf.as_mut_ptr(), buf.len());
                        let ol = (u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize)
                            .min(buf.len() - 8);
                        (c, ol)
                    }
                }
                _ => (255, 0),
            };
            let mut out = [0u8; EXEC_CAP];
            out[..out_len].copy_from_slice(&buf[8..8 + out_len]);
            emit(
                s,
                cid,
                EV_EXEC,
                id,
                b"",
                code as i64,
                &out[..out_len],
                Some(req),
            );
        }
        OP_TTY_OPEN => {
            if find_tty(s, id).is_some()
                || (0..EXEC_SEEN).any(|k| &s.tty_done[k][..s.tty_done_lens[k] as usize] == id)
            {
                return;
            }
            let sbid = frame_bytes(req, 4);
            let started =
                find_slot(s, sbid).map(|i| (s.sandboxes[i].started, s.sandboxes[i].handle));
            let Some((true, handle)) = started else {
                return; // not live yet: the chain re-offers it
            };
            let rows = parse_u32(frame_bytes(req, 5)).min(u16::MAX as u32) as u16;
            let cols = parse_u32(frame_bytes(req, 6)).min(u16::MAX as u32) as u16;
            let cmd = frame_bytes(req, 7);
            let cmd = if cmd.is_empty() {
                b"/bin/sh".as_ref()
            } else {
                cmd
            };
            let mut arg = [0u8; 8 + 1024];
            let cl = cmd.len().min(1023);
            arg[0..4].copy_from_slice(&handle.to_le_bytes());
            arg[4..6].copy_from_slice(&(if rows == 0 { 24 } else { rows }).to_le_bytes());
            arg[6..8].copy_from_slice(&(if cols == 0 { 80 } else { cols }).to_le_bytes());
            arg[8..8 + cl].copy_from_slice(&cmd[..cl]);
            arg[8 + cl] = 0;
            let sid = (sys.provider_call)(-1, WL_TTY_OPEN, arg.as_mut_ptr(), 8 + cl + 1);
            if sid < 0 {
                emit(
                    s,
                    cid,
                    EV_TTY_STATUS,
                    id,
                    b"failed",
                    sid as i64,
                    &[],
                    Some(req),
                );
                return;
            }
            if let Some(t) = s.ttys.iter().position(|t| !t.in_use) {
                let tt = &mut s.ttys[t];
                *tt = TTY_EMPTY;
                tt.in_use = true;
                tt.sid = sid;
                tt.tail_len = id.len() as u8;
                tt.tail[..id.len()].copy_from_slice(id);
                emit(s, cid, EV_TTY_STATUS, id, b"open", 0, &[], Some(req));
            } else {
                let mut ck = (sid as u32).to_le_bytes();
                (sys.provider_call)(-1, WL_TTY_CLOSE, ck.as_mut_ptr(), 4);
                emit(s, cid, EV_TTY_STATUS, id, b"failed", -12, &[], Some(req));
            }
        }
        OP_TTY_CLOSE => {
            if let Some(t) = find_tty(s, id) {
                let mut ck = (s.ttys[t].sid as u32).to_le_bytes();
                (sys.provider_call)(-1, WL_TTY_CLOSE, ck.as_mut_ptr(), 4);
                s.ttys[t] = TTY_EMPTY;
            }
        }
        OP_TTY_IN => {
            if let Some(t) = find_tty(s, id) {
                let rev = frame_i64(req, 11).unwrap_or(0);
                if rev != 0 && rev <= s.ttys[t].in_rev {
                    return;
                }
                s.ttys[t].in_rev = rev;
                let bytes = frame_bytes(req, 4);
                let have = s.ttys[t].stdin_len as usize;
                let n = bytes.len().min(TTY_IN_CAP - have);
                s.ttys[t].stdin[have..have + n].copy_from_slice(&bytes[..n]);
                s.ttys[t].stdin_len = (have + n) as u16;
                // Acknowledged at once: the bytes are queued for the pty and
                // the chain may clear the key they came from.
                emit(s, cid, EV_TTY_IN, id, b"", n as i64, &[], Some(req));
            }
        }
        _ => {}
    }
}

/// One whole record from input `k`, or None.
unsafe fn take(s: &mut State, k: usize) -> Option<usize> {
    let sys = &*s.syscalls;
    let ch = s.inputs[k];
    if ch < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(ch, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(ch, s.req.as_mut_ptr(), REQ_BUF);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(ch, s.req.as_mut_ptr(), REQ_BUF)
    } else if pk <= 0 {
        return None;
    } else {
        match frame_len(&s.req[..pk as usize]) {
            Some(fl) => (sys.channel_read)(ch, s.req.as_mut_ptr(), fl),
            None if (pk as usize) < REQ_BUF => return None,
            None => {
                (sys.channel_read)(ch, s.req.as_mut_ptr(), REQ_BUF);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}

// ---- observation ----

unsafe fn poll_sandboxes(s: &mut State) {
    let sys = &*s.syscalls;
    for i in 0..MAX_SB {
        if !s.can_emit() {
            return;
        }
        let sb = s.sandboxes[i];
        if !sb.in_use || !sb.started {
            continue;
        }
        // Output first: a workload that printed and exited has its output
        // recorded before its end.
        let mut arg = [0u8; 4 + LOG_READ];
        arg[0..4].copy_from_slice(&sb.handle.to_le_bytes());
        let n = (sys.provider_call)(-1, WL_READ, arg.as_mut_ptr(), arg.len());
        if n > 0 {
            let n = (n as usize).min(LOG_READ);
            let mut out = [0u8; LOG_READ];
            out[..n].copy_from_slice(&arg[4..4 + n]);
            emit(
                s,
                0,
                EV_LOG,
                &sb.id[..sb.id_len as usize],
                b"",
                0,
                &out[..n],
                None,
            );
            continue;
        }
        if sb.exited {
            continue;
        }
        let mut w = [0u8; 5];
        if (sys.provider_call)(sb.handle, WL_WAIT, w.as_mut_ptr(), 5) != 5 {
            continue;
        }
        let (state, code) = (w[0], i32::from_le_bytes([w[1], w[2], w[3], w[4]]));
        let id = &sb.id[..sb.id_len as usize];
        match state {
            ST_RUNNING => {
                if sb.paused {
                    s.sandboxes[i].paused = false;
                    emit(s, 0, EV_STATUS, id, b"running", 0, &[], None);
                }
            }
            ST_PAUSED => {
                if !sb.paused {
                    s.sandboxes[i].paused = true;
                    emit(s, 0, EV_STATUS, id, b"paused", 0, &[], None);
                }
            }
            _ => {
                s.sandboxes[i].exited = true;
                s.sandboxes[i].paused = false;
                emit(
                    s,
                    0,
                    EV_STATUS,
                    id,
                    label_of(state, code),
                    code as i64,
                    &[],
                    None,
                );
            }
        }
    }
}

unsafe fn poll_ttys(s: &mut State) {
    let sys = &*s.syscalls;
    for i in 0..MAX_TTY {
        if !s.can_emit() {
            return;
        }
        if !s.ttys[i].in_use {
            continue;
        }
        let t = s.ttys[i];
        let sn = (t.stdin_len as usize).min(TTY_CAP - 8);
        let mut arg = [0u8; TTY_CAP];
        arg[0..4].copy_from_slice(&(t.sid as u32).to_le_bytes());
        arg[4..8].copy_from_slice(&(sn as u32).to_le_bytes());
        arg[8..8 + sn].copy_from_slice(&t.stdin[..sn]);
        let rc = (sys.provider_call)(-1, WL_TTY_STEP, arg.as_mut_ptr(), TTY_CAP);
        let tail = &t.tail[..t.tail_len as usize];
        if rc < 0 {
            s.ttys[i] = TTY_EMPTY;
            continue;
        }
        s.ttys[i].stdin_len = 0;
        let rlen = (u32::from_le_bytes([arg[0], arg[1], arg[2], arg[3]]) as usize).min(TTY_CAP - 9);
        let state = arg[4];
        let code = i32::from_le_bytes([arg[5], arg[6], arg[7], arg[8]]);
        if rlen > 0 {
            let mut out = [0u8; TTY_CAP];
            out[..rlen].copy_from_slice(&arg[9..9 + rlen]);
            emit(s, 0, EV_TTY_OUT, tail, b"", 0, &out[..rlen], None);
            if state != 1 {
                continue;
            }
            if !s.can_emit() {
                // The exit is seen again on the next pump.
                continue;
            }
        }
        if state == 1 {
            emit(s, 0, EV_TTY_STATUS, tail, b"exited", code as i64, &[], None);
            let k = s.tty_done_next as usize % EXEC_SEEN;
            let tl = t.tail_len as usize;
            s.tty_done[k][..tl].copy_from_slice(&t.tail[..tl]);
            s.tty_done_lens[k] = tl as u8;
            s.tty_done_next = s.tty_done_next.wrapping_add(1);
            let mut ck = (t.sid as u32).to_le_bytes();
            (sys.provider_call)(-1, WL_TTY_CLOSE, ck.as_mut_ptr(), 4);
            s.ttys[i] = TTY_EMPTY;
        }
    }
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
        s.inputs = [in_chan, -1, -1, -1, -1];
        s.event_out = out_chan;
        s.resolved = 0;
        s.caps_ops = 0;
        s.next_input = 0;
        s.boot_len = 0;
        s.booted = 0;
        s.sandboxes = [SB_EMPTY; MAX_SB];
        s.ttys = [TTY_EMPTY; MAX_TTY];
        s.exec_lens = [0; EXEC_SEEN];
        s.exec_revs = [0; EXEC_SEEN];
        s.tty_done_lens = [0; EXEC_SEEN];
        s.tty_done_next = 0;
        s.exec_next = 0;
        s.pend_at = 0;
        s.pend_len = 0;
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
        if s.syscalls.is_null() || s.event_out < 0 {
            return 0;
        }
        let sys = &*s.syscalls;
        if s.resolved == 0 {
            for k in 1..INPUTS {
                s.inputs[k] = dev_channel_port(sys, 0, k as u8);
            }
            let mut buf = [0u8; 64];
            let rc = (sys.provider_call)(-1, WL_CAPS, buf.as_mut_ptr(), buf.len());
            s.caps_ops = if rc < 5 {
                0
            } else {
                u16::from_le_bytes([buf[2], buf[3]])
            };
            // This process's boot: the wall clock at start, which a restart
            // cannot repeat.
            let mut v = dev_unix_millis(sys);
            let mut tmp = [0u8; 20];
            let mut i = tmp.len();
            loop {
                i -= 1;
                tmp[i] = b'0' + (v % 10) as u8;
                v /= 10;
                if v == 0 {
                    break;
                }
            }
            let n = tmp.len() - i;
            s.boot[..n].copy_from_slice(&tmp[i..]);
            s.boot_len = n as u8;
            s.resolved = 1;
        }
        if !flush(s) {
            return 0;
        }
        if s.booted == 0 {
            emit(s, 0, EV_BOOT, b"", b"", 0, &[], None);
            s.booted = 1;
            return 0;
        }
        // Ops, one per input per step, round-robin — then observation.
        for _ in 0..INPUTS {
            if !s.can_emit() {
                break;
            }
            let k = s.next_input as usize % INPUTS;
            s.next_input = s.next_input.wrapping_add(1);
            if let Some(n) = take(s, k) {
                let mut req = [0u8; REQ_BUF];
                req[..n].copy_from_slice(&s.req[..n]);
                op(s, &req[..n]);
            }
        }
        poll_sandboxes(s);
        poll_ttys(s);
        0
    }
}
