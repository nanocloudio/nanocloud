//! The claim chain's mount actuator: a Mnt record in, the raw facts of what
//! ran back. See `manifest.toml`.

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
use abi::platform::linux::host_mount as hm;
use abi::platform::linux::host_process as hp;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_FIELD: u8 = 254;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const REC: usize = 8192;
const MAX_ARG: usize = 128;
const CMD: usize = 320;

const OP_MOUNT: i64 = 50;
const OP_UNMOUNT: i64 = 51;
/// `blkid -p`'s exit status for a device with no signature it recognises.
const BLKID_NOTHING: i32 = 2;

const ST_IDLE: u8 = 0;
/// `blkid -p` is running.
const ST_PROBE: u8 = 1;
const ST_MKFS: u8 = 2;
const ST_FSCK: u8 = 3;
/// The platform is mounting or unmounting off this thread; asked again each
/// step until it answers.
const ST_MOUNT: u8 = 4;
const ST_UMOUNT: u8 = 5;

/// The platform's "still running, ask again".
const EAGAIN: i32 = -11;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    stage: u8,
    _pad: [u8; 3],
    /// The running tool's handle, or -1.
    proc_h: i32,

    held: [u8; REC],
    held_len: u16,
    reply: [u8; REC],
    reply_len: u16,

    /// The facts gathered so far; -1 for what did not run.
    mkfs: i64,
    fsck: i64,

    answered: u32,
    dropped: u32,
    drain: [u8; 256],
}

mod params_def {
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;
    }
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

/// A word the command line may carry: no whitespace (the executor splits on
/// it), nothing empty, nothing long.
/// The make option that leaves the device's blocks alone, per filesystem.
fn nodiscard(fs: &[u8]) -> Option<(&'static [u8], &'static [u8])> {
    match fs {
        b"ext2" | b"ext3" | b"ext4" => Some((b"-E", b"nodiscard")),
        b"xfs" => Some((b"-K", b"")),
        _ => None,
    }
}

fn word(b: &[u8]) -> bool {
    !b.is_empty() && b.len() <= MAX_ARG && b.iter().all(|&c| c > b' ' && c != 0x7f)
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
/// instance arena and syscall table.
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
    s.proc_h = -1;
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
        if s.syscalls.is_null() || s.request_in < 0 || s.response_out < 0 {
            return 0;
        }
        let sys = &*s.syscalls;
        if s.reply_len != 0 {
            flush_reply(s, sys);
            return 0;
        }
        match s.stage {
            ST_IDLE => {}
            ST_MOUNT => {
                mount(s, sys);
                return 0;
            }
            ST_UMOUNT => {
                umount(s, sys);
                return 0;
            }
            _ => {
                poll_tool(s, sys);
                return 0;
            }
        }
        admit(s, sys);
        0
    }
}

unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    let Some(len) = take_record(s, sys) else {
        return;
    };
    s.held_len = len as u16;
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), len);
    s.mkfs = -1;
    s.fsck = -1;
    let op = frame_i64(rec, 2);
    if op != OP_MOUNT && op != OP_UNMOUNT {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    // Nothing runs without the privilege, and the answer says so.
    // `PRIV` is a bit set: a root runtime answers SYS_ADMIN | CHOWN.
    let held = (sys.provider_call)(-1, hm::PRIV, core::ptr::null_mut(), 0);
    if held < 0 || held & hm::PRIV_SYS_ADMIN == 0 {
        answer(s, sys, 0, -1, 0);
        return;
    }
    if op == OP_UNMOUNT {
        umount(s, sys);
        return;
    }
    let (dev, fs) = (frame_bytes(rec, 4), frame_bytes(rec, 5));
    if !word(dev) || !word(fs) {
        answer(s, sys, 1, i64::from(-errno_inval()), 0);
        return;
    }
    if frame_bytes(rec, 7) == b"1" {
        spawn(s, sys, ST_PROBE, &[b"blkid", b"-p", dev]);
    } else {
        spawn(s, sys, ST_FSCK, &[b"fsck", b"-p", dev]);
    }
}

fn errno_inval() -> i32 {
    -22
}

/// Run a tool; `stage` names what its exit status means.
unsafe fn spawn(s: &mut State, sys: &SyscallTable, stage: u8, words: &[&[u8]]) {
    let mut cmd = [0u8; CMD];
    let mut n = 0usize;
    // An empty word is no argument at all.
    for w in words.iter().filter(|w| !w.is_empty()) {
        if n + w.len() + 1 > CMD {
            answer(s, sys, 1, i64::from(-errno_inval()), 0);
            return;
        }
        if n > 0 {
            cmd[n] = b' ';
            n += 1;
        }
        cmd[n..n + w.len()].copy_from_slice(w);
        n += w.len();
    }
    let h = (sys.provider_call)(-1, hp::PROC_SPAWN, cmd.as_mut_ptr(), n);
    if h < 0 {
        // The tool could not start (off the node's allowlist, no slot): its
        // stage reports a failure of its own.
        finish_stage(s, sys, stage, 127);
        return;
    }
    s.proc_h = h;
    s.stage = stage;
}

/// Drain the running tool's output; act on its exit once it has finished.
unsafe fn poll_tool(s: &mut State, sys: &SyscallTable) {
    let _ = (sys.provider_call)(s.proc_h, hp::PROC_READ, s.drain.as_mut_ptr(), s.drain.len());
    let mut code = [0u8; 4];
    let running = (sys.provider_call)(s.proc_h, hp::PROC_STATUS, code.as_mut_ptr(), 4);
    if running == 1 {
        return;
    }
    let _ = (sys.provider_call)(s.proc_h, hp::PROC_CLOSE, core::ptr::null_mut(), 0);
    s.proc_h = -1;
    let stage = s.stage;
    s.stage = ST_IDLE;
    let exit = if running == 0 {
        i32::from_le_bytes(code)
    } else {
        -1
    };
    finish_stage(s, sys, stage, exit);
}

unsafe fn finish_stage(s: &mut State, sys: &SyscallTable, stage: u8, exit: i32) {
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let (dev, fs) = (frame_bytes(rec, 4), frame_bytes(rec, 5));
    let mut dev_buf = [0u8; MAX_ARG];
    let mut fs_buf = [0u8; MAX_ARG];
    dev_buf[..dev.len()].copy_from_slice(dev);
    fs_buf[..fs.len()].copy_from_slice(fs);
    let (dev, fs) = (&dev_buf[..dev.len()], &fs_buf[..fs.len()]);
    match stage {
        // A signature is there: check it rather than make over it.
        // Made without discarding the device first: a blank device reads
        // zeros already, and a whole-device DISCARD down a remote block
        // stack outlasts the device's I/O timeout.
        ST_PROBE if exit == BLKID_NOTHING => match nodiscard(fs) {
            Some(opt) => spawn(s, sys, ST_MKFS, &[b"mkfs", b"-t", fs, opt.0, opt.1, dev]),
            None => spawn(s, sys, ST_MKFS, &[b"mkfs", b"-t", fs, dev]),
        },
        ST_PROBE => spawn(s, sys, ST_FSCK, &[b"fsck", b"-p", dev]),
        ST_MKFS => {
            s.mkfs = i64::from(exit);
            if exit == 0 {
                mount(s, sys);
            } else {
                answer(s, sys, 1, -1, 0);
            }
        }
        ST_FSCK => {
            s.fsck = i64::from(exit);
            // fsck(8): 4 and above leaves errors, or could not run.
            if (0..4).contains(&exit) {
                mount(s, sys);
            } else {
                answer(s, sys, 1, -1, 0);
            }
        }
        _ => answer(s, sys, 1, -1, 0),
    }
}

/// Mount the claim's device under the platform's root.
unsafe fn mount(s: &mut State, sys: &SyscallTable) {
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let (dev, claim, fs) = (
        frame_bytes(rec, 4),
        frame_bytes(rec, 3),
        frame_bytes(rec, 5),
    );
    let mut arg = [0u8; 6 + 3 * MAX_ARG];
    if dev.len() > MAX_ARG || claim.len() > MAX_ARG || fs.len() > MAX_ARG || claim.is_empty() {
        answer(s, sys, 1, i64::from(-errno_inval()), 0);
        return;
    }
    arg[0..2].copy_from_slice(&(dev.len() as u16).to_le_bytes());
    arg[2..4].copy_from_slice(&(claim.len() as u16).to_le_bytes());
    arg[4] = fs.len() as u8;
    arg[5] = 0;
    let mut n = 6;
    for part in [dev, claim, fs] {
        arg[n..n + part.len()].copy_from_slice(part);
        n += part.len();
    }
    let rc = (sys.provider_call)(-1, hm::MOUNT, arg.as_mut_ptr(), n);
    if rc == EAGAIN {
        s.stage = ST_MOUNT;
        return;
    }
    answer(s, sys, 1, i64::from(-rc.min(0)), 0);
}

/// Unmount the held claim's directory.
unsafe fn umount(s: &mut State, sys: &SyscallTable) {
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let mut claim = [0u8; MAX_ARG];
    let c = frame_bytes(rec, 3);
    let n = c.len().min(MAX_ARG);
    claim[..n].copy_from_slice(&c[..n]);
    let rc = (sys.provider_call)(-1, hm::UMOUNT, claim.as_mut_ptr(), n);
    if rc == EAGAIN {
        s.stage = ST_UMOUNT;
        return;
    }
    // 1 unmounted, 0 nothing was mounted, else the errno that kept it.
    let (was, code) = match rc {
        1 => (1, 0),
        0 => (0, 0),
        e => (1, i64::from(-e)),
    };
    answer(s, sys, 1, code, was);
}

/// The mount point of the held claim: the platform's root, '/', the claim.
unsafe fn mount_dir(s: &State, sys: &SyscallTable, out: &mut [u8; 2 * MAX_ARG + 2]) -> usize {
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let claim = frame_bytes(rec, 3);
    let root = (sys.provider_call)(-1, hm::ROOT, out.as_mut_ptr(), MAX_ARG);
    if root <= 0 || claim.is_empty() || claim.len() > MAX_ARG {
        return 0;
    }
    let mut n = root as usize;
    out[n] = b'/';
    n += 1;
    out[n..n + claim.len()].copy_from_slice(claim);
    n + claim.len()
}

/// Build the Done record for the held request.
unsafe fn answer(s: &mut State, sys: &SyscallTable, privileged: i64, code: i64, was: i64) {
    s.stage = ST_IDLE;
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let op = frame_i64(held, 2);
    let mut dir = [0u8; 2 * MAX_ARG + 2];
    let dir_len = if op == OP_MOUNT && privileged == 1 && code == 0 {
        mount_dir(s, sys, &mut dir)
    } else {
        0
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
        && f(&mut out, 2, TY_I64, &op.to_le_bytes())
        && f(&mut out, 3, TY_BYTES, frame_bytes(held, 3))
        && f(&mut out, 4, TY_I64, &privileged.to_le_bytes())
        && f(&mut out, 5, TY_I64, &s.fsck.to_le_bytes())
        && f(&mut out, 6, TY_I64, &code.to_le_bytes())
        && f(&mut out, 7, TY_BYTES, &dir[..dir_len])
        && f(&mut out, 8, TY_I64, &was.to_le_bytes())
        && f(&mut out, 9, TY_I64, &s.mkfs.to_le_bytes())
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

unsafe fn flush_reply(s: &mut State, sys: &SyscallTable) {
    let n = s.reply_len as usize;
    if (sys.channel_write)(s.response_out, s.reply.as_ptr(), n) > 0 {
        s.reply_len = 0;
        s.answered = s.answered.wrapping_add(1);
    }
}

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
