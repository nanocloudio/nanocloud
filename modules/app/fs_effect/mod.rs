//! A chain's filesystem effect: a directory tree (or a file) under a
//! configured root removed through the fs contract, resumably. See
//! `manifest.toml`.

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

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
const CARRY_FIELD: u8 = 254;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const REC: usize = 8192;

const FS_CLOSE: u32 = 0x0903;
const FS_OPENDIR: u32 = 0x0907;
const FS_READDIR: u32 = 0x0908;
const FS_UNLINK: u32 = 0x090A;
const FS_RENAME: u32 = 0x090D;
const FS_READLINK: u32 = 0x0914;
const FS_RMDIR: u32 = 0x0915;

const ENOENT: i32 = -2;
const EAGAIN: i32 = -11;
const EISDIR: i32 = -21;
const EINVAL: i32 = -22;
const ENOSYS: i32 = -38;
const ENOTEMPTY: i32 = -39;
const EINPROGRESS: i32 = -36;

/// The longest path the fs contract takes (a provider's limit, 255).
const PATH_MAX: usize = 255;
/// A directory deeper than this is moved up beside the top before it is
/// walked, so its own entries' paths stay addressable.
const DEEP: usize = 160;
const ROOT_MAX: usize = 128;
const LIST: usize = 1024;

const OP_NOOP: i64 = 0;
const OP_REMOVE: i64 = 1;

/// Where a REMOVE stands.
const ST_IDLE: u8 = 0;
/// READLINK of the path itself: a symlink is removed as a name.
const ST_PROBE: u8 = 1;
/// Walking the tree from `cur`.
const ST_WALK: u8 = 2;

/// An answer that is not a decision yet: asked again on a later step.
fn pending(r: i32) -> bool {
    r == EAGAIN || r == EINPROGRESS
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    stage: u8,
    budget: u32,

    root: [u8; ROOT_MAX],
    root_len: u8,

    held: [u8; REC],
    held_len: u16,
    reply: [u8; REC],
    reply_len: u16,

    /// The path removed (`top_len` bytes of `cur`) and the directory the
    /// walk is in (`cur_len`), NUL-free.
    cur: [u8; PATH_MAX + 1],
    top_len: u16,
    cur_len: u16,
    /// Branches moved up so far (their names: `.gc<n>`), and entries removed.
    moved: u32,
    removed: i64,
    list: [u8; LIST],
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::ROOT_MAX;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, root, str, 0
            => |s, d, len| {
                let n = if len > ROOT_MAX { 0 } else { len };
                s.root_len = n as u8;
                if n > 0 { ptr_copy(s.root.as_mut_ptr(), d, n); }
            };

        2, budget, u32, 64
            => |s, d, len| { s.budget = p_u32(d, len, 0, 64).max(4); };
    }
}

unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

// ---- record frames ----

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let [num, ty, l0, l1]: [u8; 4] = frame.get(p..p + 4)?.try_into().ok()?;
        let len = u16::from_le_bytes([l0, l1]) as usize;
        let body = frame.get(p + 4..p + 4 + len)?;
        if num == number {
            return Some((ty, body));
        }
        p += 4 + len;
    }
    None
}

fn frame_i64(frame: &[u8], number: u8) -> i64 {
    match frame_field(frame, number) {
        Some((TY_I64, b)) => b.try_into().map(i64::from_le_bytes).unwrap_or(0),
        _ => 0,
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
        let [_, _, l0, l1]: [u8; 4] = data.get(off..off + 4)?.try_into().ok()?;
        off += 4 + u16::from_le_bytes([l0, l1]) as usize;
    }
    (off <= data.len()).then_some(off)
}

fn put(out: &mut [u8], at: usize, number: u8, ty: u8, v: &[u8]) -> Option<usize> {
    let end = at.checked_add(4 + v.len())?;
    if v.len() > u16::MAX as usize {
        return None;
    }
    let l = (v.len() as u16).to_le_bytes();
    let q = append(out, at, &[number, ty, l[0], l[1]])?;
    append(out, q, v)?;
    Some(end)
}

/// `src` onto `dst` at `at`: the end, or None when it does not fit.
fn append(dst: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    for (d, c) in dst.get_mut(at..end)?.iter_mut().zip(src) {
        *d = *c;
    }
    Some(end)
}

/// A decimal into `dst` at `at`.
fn append_dec(dst: &mut [u8], at: usize, mut v: u32) -> Option<usize> {
    let mut tmp = [0u8; 10];
    let mut k = tmp.len();
    while k > 0 {
        k -= 1;
        if let Some(t) = tmp.get_mut(k) {
            *t = b'0' + (v % 10) as u8;
        }
        v /= 10;
        if v == 0 {
            break;
        }
    }
    append(dst, at, tmp.get(k..)?)
}

/// One path segment of plain characters: nothing a chain built can climb
/// out of `root` with.
fn plain_name(n: &[u8]) -> bool {
    !n.is_empty()
        && n.first() != Some(&b'.')
        && n.iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_' || c == b'.')
}

// ---- the walk ----

/// `cur` as the provider takes a path: its bytes, a NUL past them (the
/// buffer keeps one byte spare).
unsafe fn path_call(s: &mut State, op: u32) -> i32 {
    let sys = &*s.syscalls;
    let n = s.cur_len as usize;
    (sys.provider_call)(-1, op, s.cur.as_mut_ptr(), n)
}

/// Answer the held request: its status and what was removed.
unsafe fn answer(s: &mut State, status: i64) {
    s.stage = ST_IDLE;
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let out = &mut s.reply;
    let mut p = 1usize;
    let mut n = 0u8;
    let mut ok = true;
    let mut f = |out: &mut [u8; REC], num: u8, ty: u8, v: &[u8]| match put(out, p, num, ty, v) {
        Some(q) => {
            p = q;
            n += 1;
        }
        None => ok = false,
    };
    f(out, 1, TY_I64, &frame_i64(held, 1).to_le_bytes());
    f(out, 2, TY_I64, &status.to_le_bytes());
    f(out, 3, TY_I64, &s.removed.to_le_bytes());
    for num in (CARRY_LO..=CARRY_HI).chain(core::iter::once(CARRY_FIELD)) {
        if let Some((ty, v)) = frame_field(held, num) {
            f(out, num, ty, v);
        }
    }
    s.held_len = 0;
    if !ok {
        s.reply_len = 0;
        return;
    }
    out[0] = n;
    s.reply_len = p as u16;
    flush_reply(s);
}

unsafe fn flush_reply(s: &mut State) {
    let sys = &*s.syscalls;
    let n = s.reply_len as usize;
    if n > 0 && (sys.channel_write)(s.response_out, s.reply.as_ptr(), n) > 0 {
        s.reply_len = 0;
    }
}

/// Up from `cur` to its parent: false when it is the top.
fn pop(s: &mut State) -> bool {
    let top = s.top_len as usize;
    let cur = s.cur_len as usize;
    if cur <= top {
        return false;
    }
    let at = s
        .cur
        .get(top..cur)
        .and_then(|t| t.iter().rposition(|&c| c == b'/'))
        .map_or(top, |i| top + i);
    s.cur_len = at as u16;
    true
}

/// `cur` + "/" + `name`, in place; false when it does not fit a path.
fn push(s: &mut State, name: &[u8]) -> bool {
    let at = s.cur_len as usize;
    let Some(q) = append(&mut s.cur, at, b"/") else {
        return false;
    };
    match append(&mut s.cur, q, name) {
        Some(e) if e <= PATH_MAX => {
            s.cur_len = e as u16;
            true
        }
        _ => false,
    }
}

/// Move the entry `name` of `cur` up beside the top, as `<top>/.gc<n>`:
/// its own entries' paths become short again. The provider's answer.
unsafe fn move_up(s: &mut State, name: &[u8]) -> i32 {
    let sys = &*s.syscalls;
    let mut arg = [0u8; 2 * (PATH_MAX + 2)];
    let cur = s.cur_len as usize;
    let src_len = cur + 1 + name.len();
    let top = s.top_len as usize;
    let mut dst = [0u8; PATH_MAX];
    let Some(d) = append(&mut dst, 0, s.cur.get(..top).unwrap_or(&[]))
        .and_then(|d| append(&mut dst, d, b"/.gc"))
        .and_then(|d| append_dec(&mut dst, d, s.moved))
    else {
        return EINVAL;
    };
    let mut p = 0usize;
    let ok = (|| {
        p = append(&mut arg, p, &(src_len as u16).to_le_bytes())?;
        p = append(&mut arg, p, s.cur.get(..cur)?)?;
        p = append(&mut arg, p, b"/")?;
        p = append(&mut arg, p, name)?;
        p = append(&mut arg, p, &(d as u16).to_le_bytes())?;
        p = append(&mut arg, p, dst.get(..d)?)?;
        Some(())
    })();
    if ok.is_none() {
        return EINVAL;
    }
    let rc = (sys.provider_call)(-1, FS_RENAME, arg.as_mut_ptr(), p);
    if rc == 0 {
        s.moved = s.moved.wrapping_add(1);
    }
    rc
}

/// `name` of `cur`, unlinked: the provider's answer.
unsafe fn unlink_child(s: &mut State, name: &[u8]) -> i32 {
    let base = s.cur_len;
    if !push(s, name) {
        return EINVAL;
    }
    let rc = path_call(s, FS_UNLINK);
    s.cur_len = base;
    rc
}

/// One step of the held REMOVE: at most `budget` fs calls.
unsafe fn work(s: &mut State) {
    let sys = &*s.syscalls;
    let mut calls = 0u32;
    if s.stage == ST_PROBE {
        // The path itself a symlink: removed as a name, never followed.
        let mut buf = [0u8; PATH_MAX + 3];
        let n = s.cur_len as usize;
        let lb = (n as u16).to_le_bytes();
        buf[0] = lb[0];
        buf[1] = lb[1];
        if append(&mut buf, 2, s.cur.get(..n).unwrap_or(&[])).is_none() {
            answer(s, 414);
            return;
        }
        let rc = (sys.provider_call)(-1, FS_READLINK, buf.as_mut_ptr(), buf.len());
        if pending(rc) {
            return;
        }
        if rc >= 0 {
            let u = path_call(s, FS_UNLINK);
            if pending(u) {
                return;
            }
            if u == 0 || u == ENOENT {
                s.removed += i64::from(u == 0);
                answer(s, 200);
            } else {
                answer(s, 500);
            }
            return;
        }
        if rc == ENOENT {
            answer(s, 200);
            return;
        }
        // Not a link (EINVAL), or a provider without links: walked.
        s.stage = ST_WALK;
    }
    while calls < s.budget {
        calls += 1;
        let h = path_call(s, FS_OPENDIR);
        if pending(h) {
            return;
        }
        if h < 0 {
            if s.cur_len > s.top_len {
                // A branch gone from under the walk: on with its parent.
                pop(s);
                continue;
            }
            // The top is not a directory it can list: a file, or nothing.
            let u = path_call(s, FS_UNLINK);
            if pending(u) {
                return;
            }
            if u == 0 || u == ENOENT {
                s.removed += i64::from(u == 0);
                answer(s, 200);
            } else {
                answer(s, 500);
            }
            return;
        }
        calls += 1;
        let got = (sys.provider_call)(h, FS_READDIR, s.list.as_mut_ptr(), LIST);
        let mut z = [0u8; 4];
        (sys.provider_call)(h, FS_CLOSE, z.as_mut_ptr(), 0);
        if pending(got) {
            return;
        }
        if got < 0 {
            answer(s, 500);
            return;
        }
        if got == 0 {
            // Empty: the directory goes, and the walk goes up.
            calls += 1;
            let r = path_call(s, FS_RMDIR);
            if pending(r) {
                return;
            }
            if r == ENOTEMPTY {
                continue; // something arrived: listed again
            }
            if r != 0 && r != ENOENT {
                answer(s, 500);
                return;
            }
            s.removed += i64::from(r == 0);
            if !pop(s) {
                answer(s, 200);
                return;
            }
            continue;
        }
        // [count u16] then [len u8][type u8][name] per entry.
        let total = (got as usize).min(LIST);
        let mut list = [0u8; LIST];
        let _ = append(&mut list, 0, s.list.get(..total).unwrap_or(&[]));
        let count = u16::from_le_bytes([list[0], list[1]]) as usize;
        let mut at = 2usize;
        let mut descended = false;
        for _ in 0..count {
            if calls >= s.budget {
                return;
            }
            let Some(&nl) = list.get(at) else { break };
            let is_dir = list.get(at + 1) == Some(&1);
            let Some(name) = list.get(at + 2..at + 2 + nl as usize) else {
                break;
            };
            at += 2 + nl as usize;
            if (s.cur_len as usize) + 1 + name.len() > PATH_MAX {
                answer(s, 414);
                return;
            }
            calls += 1;
            if is_dir {
                if (s.cur_len as usize) + 1 + name.len() > DEEP && s.cur_len > s.top_len {
                    let r = move_up(s, name);
                    if pending(r) {
                        return;
                    }
                    if r != 0 {
                        answer(s, 500);
                        return;
                    }
                    continue;
                }
                push(s, name);
                descended = true;
                break;
            }
            let r = unlink_child(s, name);
            if pending(r) {
                return;
            }
            if r == EISDIR {
                // A directory the listing did not type: walked into.
                push(s, name);
                descended = true;
                break;
            }
            if r != 0 && r != ENOENT {
                answer(s, 500);
                return;
            }
            s.removed += i64::from(r == 0);
        }
        let _ = descended;
    }
}

/// Take the next record and start it.
unsafe fn admit(s: &mut State) {
    let sys = &*s.syscalls;
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    let pk = (sys.channel_peek)(s.request_in, s.held.as_mut_ptr(), REC);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REC)
    } else if pk <= 0 {
        return;
    } else {
        match frame_len(s.held.get(..pk as usize).unwrap_or(&[])) {
            Some(fl) => (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), fl),
            None if (pk as usize) < REC => return,
            None => {
                (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REC);
                return;
            }
        }
    };
    if n <= 0 {
        return;
    }
    s.held_len = n as u16;
    s.removed = 0;
    s.moved = 0;
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    match frame_i64(held, 2) {
        OP_REMOVE => {}
        OP_NOOP => {
            answer(s, 204);
            return;
        }
        _ => {
            answer(s, 400);
            return;
        }
    }
    // `<root>/<one plain name>`, and nothing else.
    let path = frame_bytes(held, 3);
    let root = s.root.get(..s.root_len as usize).unwrap_or(&[]);
    let under = s.root_len > 0
        && path.len() > root.len() + 1
        && path.get(..root.len()) == Some(root)
        && path.get(root.len()) == Some(&b'/')
        && path.get(root.len() + 1..).is_some_and(plain_name);
    if !under || path.len() > PATH_MAX {
        answer(s, 403);
        return;
    }
    let pl = path.len();
    let _ = append(&mut s.cur, 0, path);
    s.cur_len = pl as u16;
    s.top_len = pl as u16;
    s.stage = ST_PROBE;
    work(s);
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
    let s = &mut *(state as *mut State);
    s.syscalls = syscalls as *const SyscallTable;
    s.request_in = in_chan;
    s.response_out = out_chan;
    s.stage = ST_IDLE;
    s.root_len = 0;
    s.held_len = 0;
    s.reply_len = 0;
    params_def::set_defaults(s);
    params_def::parse_tlv(s, params, params_len);
    if s.root_len == 0 {
        let m = b"[fs_effect] `root` is required: nothing is removed outside it";
        dev_log(&*s.syscalls, 1, m.as_ptr(), m.len());
        return -3;
    }
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
        if s.syscalls.is_null() || s.request_in < 0 || s.response_out < 0 {
            return 0;
        }
        if s.reply_len != 0 {
            flush_reply(s);
            return 0;
        }
        if s.stage != ST_IDLE {
            work(s);
            return 0;
        }
        admit(s);
        0
    }
}
