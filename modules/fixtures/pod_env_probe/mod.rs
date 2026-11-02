//! pod_env_probe — a graph-backed Pod's environment and volumes, as its
//! module sees them. See `manifest.toml` for the lines it logs.
#![no_std]
#![allow(
    dead_code,
    unused_imports,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

const FS_OPEN: u32 = 0x0900;
const FS_READ: u32 = 0x0901;
const FS_SEEK: u32 = 0x0902;
const FS_CLOSE: u32 = 0x0903;
const FS_FSYNC: u32 = 0x0905;
const FS_WRITE: u32 = 0x0906;
const FS_OPEN_CREATE: u32 = 0x0909;
const EAGAIN: i32 = -11;
const EINPROGRESS: i32 = -36;
const ENOENT: i32 = -2;
const STR: usize = 256;

const P_PARAMS: u8 = 0;
const P_SECRET_OPEN: u8 = 1;
const P_SECRET_READ: u8 = 2;
const P_RO: u8 = 3;
const P_STATE_OPEN: u8 = 4;
const P_STATE_READ: u8 = 5;
const P_STATE_CREATE: u8 = 6;
const P_STATE_WRITE: u8 = 7;
const P_STATE_FSYNC: u8 = 8;
const P_DONE: u8 = 9;
const P_WATCH_OPEN: u8 = 10;
const P_WATCH_READ: u8 = 11;
/// Steps between two reads of `secret_file` once the lines are done.
const WATCH_STEPS: u32 = 250;

#[repr(C)]
pub struct State {
    syscalls: *const SyscallTable,
    phase: u8,
    fd: i32,
    count: u32,
    name: [u8; STR],
    name_len: usize,
    node: [u8; STR],
    node_len: usize,
    cell: [u8; STR],
    cell_len: usize,
    token: [u8; STR],
    token_len: usize,
    secret: [u8; STR],
    secret_len: usize,
    state: [u8; STR],
    state_len: usize,
    path: [u8; STR],
    /// `secret_file`'s first line as last logged, and the steps since it
    /// was last read.
    seen: [u8; 128],
    seen_len: usize,
    idle: u32,
}

fn keep(dst: &mut [u8; STR], n: &mut usize, d: *const u8, len: usize) {
    let l = len.min(STR);
    if !d.is_null() && l > 0 {
        // SAFETY: the kernel hands the handler `len` readable bytes.
        unsafe { core::ptr::copy_nonoverlapping(d, dst.as_mut_ptr(), l) };
    }
    *n = l;
}

mod params_def {
    use super::keep;
    use super::State;
    use super::SCHEMA_MAX;
    define_params! {
        State;
        1, pod, str, 0 => |s, d, len| { keep(&mut s.name, &mut s.name_len, d, len); };
        2, node, str, 0 => |s, d, len| { keep(&mut s.node, &mut s.node_len, d, len); };
        3, cell, str, 0 => |s, d, len| { keep(&mut s.cell, &mut s.cell_len, d, len); };
        4, token, str, 0 => |s, d, len| { keep(&mut s.token, &mut s.token_len, d, len); };
        5, secret_file, str, 0 => |s, d, len| { keep(&mut s.secret, &mut s.secret_len, d, len); };
        6, state_file, str, 0 => |s, d, len| { keep(&mut s.state, &mut s.state_len, d, len); };
    }
}

struct Line {
    b: [u8; 512],
    n: usize,
}

impl Line {
    fn new() -> Line {
        Line { b: [0; 512], n: 0 }
    }
    fn put(&mut self, s: &[u8]) {
        for &c in s {
            if let Some(d) = self.b.get_mut(self.n) {
                *d = c;
                self.n += 1;
            }
        }
    }
    fn int(&mut self, v: i64) {
        if v < 0 {
            self.put(b"-");
        }
        let mut u = v.unsigned_abs();
        let mut d = [0u8; 20];
        let mut k = 0usize;
        loop {
            if let Some(x) = d.get_mut(k) {
                *x = b'0' + (u % 10) as u8;
            }
            k += 1;
            u /= 10;
            if u == 0 || k == d.len() {
                break;
            }
        }
        while k > 0 {
            k -= 1;
            let c = d.get(k).copied().unwrap_or(b'0');
            self.put(&[c]);
        }
    }
    /// # Safety
    /// `sys` is the live syscall table.
    unsafe fn say(&self, sys: &SyscallTable) {
        dev_log(sys, 3, self.b.as_ptr(), self.n);
    }
}

/// `buf[..n]`'s leading decimal digits.
fn parse(buf: &[u8]) -> u32 {
    let mut v = 0u32;
    for &c in buf {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.wrapping_mul(10).wrapping_add(u32::from(c - b'0'));
    }
    v
}

/// One fs call: `arg[..len]` (a path, a buffer).
unsafe fn fs(sys: &SyscallTable, h: i32, op: u32, arg: *mut u8, len: usize) -> i32 {
    (sys.provider_call)(h, op, arg, len)
}

/// The path at `src[..n]` into `s.path`, its length.
fn path_of(path: &mut [u8; STR], src: &[u8]) -> usize {
    let n = src.len().min(STR);
    for (d, &c) in path.iter_mut().zip(src.iter().take(n)) {
        *d = c;
    }
    n
}

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
/// instance arena and syscall table; `params` the config TLV blob.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    _in_chan: i32,
    _out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if syscalls.is_null() || state.is_null() || state_size < core::mem::size_of::<State>() {
        return -22;
    }
    core::ptr::write_bytes(state, 0, core::mem::size_of::<State>());
    let s = &mut *(state as *mut State);
    s.syscalls = syscalls as *const SyscallTable;
    s.fd = -1;
    params_def::set_defaults(s);
    if !params.is_null() && params_len > 0 {
        params_def::parse_tlv(s, params, params_len);
    }
    0
}

/// # Safety
/// Kernel module-ABI entry point: `state` is this module's instance block.
#[no_mangle]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return 0;
    }
    let s = &mut *(state as *mut State);
    if s.syscalls.is_null() {
        return 0;
    }
    let sys = &*s.syscalls;
    match s.phase {
        P_PARAMS => {
            let mut l = Line::new();
            l.put(b"[envp] pod=");
            l.put(s.name.get(..s.name_len).unwrap_or(&[]));
            l.put(b" node=");
            l.put(s.node.get(..s.node_len).unwrap_or(&[]));
            l.put(b" cell=");
            l.put(s.cell.get(..s.cell_len).unwrap_or(&[]));
            l.put(b" token=");
            l.put(s.token.get(..s.token_len).unwrap_or(&[]));
            l.say(sys);
            s.phase = if s.secret_len > 0 {
                P_SECRET_OPEN
            } else {
                P_STATE_OPEN
            };
        }
        P_SECRET_OPEN => {
            let n = path_of(&mut s.path, s.secret.get(..s.secret_len).unwrap_or(&[]));
            let rc = fs(sys, -1, FS_OPEN, s.path.as_mut_ptr(), n);
            if rc == EAGAIN {
                return 0;
            }
            if rc < 0 {
                let mut l = Line::new();
                l.put(b"[envp] secret error=");
                l.int(i64::from(rc));
                l.say(sys);
                s.phase = P_RO;
            } else {
                s.fd = rc;
                s.phase = P_SECRET_READ;
            }
        }
        P_SECRET_READ => {
            let mut buf = [0u8; 128];
            let rc = fs(sys, s.fd, FS_READ, buf.as_mut_ptr(), buf.len());
            if rc == EAGAIN {
                return 0;
            }
            let mut l = Line::new();
            l.put(b"[envp] secret=");
            if rc >= 0 {
                let got = buf.get(..rc as usize).unwrap_or(&[]);
                let end = got.iter().position(|&c| c == b'\n').unwrap_or(got.len());
                let line = got.get(..end).unwrap_or(&[]);
                l.put(line);
                s.seen_len = line.len().min(s.seen.len());
                for (d, &c) in s.seen.iter_mut().zip(line) {
                    *d = c;
                }
            } else {
                l.put(b"error ");
                l.int(i64::from(rc));
            }
            l.say(sys);
            let mut z = [0u8; 1];
            fs(sys, s.fd, FS_CLOSE, z.as_mut_ptr(), 0);
            s.fd = -1;
            s.phase = P_RO;
        }
        P_RO => {
            // A file beside the secret: its volume is read-only.
            let sec = s.secret.get(..s.secret_len).unwrap_or(&[]);
            let dir = sec.iter().rposition(|&c| c == b'/').unwrap_or(0);
            let mut n = path_of(&mut s.path, sec.get(..dir).unwrap_or(&[]));
            for &c in b"/envp-write" {
                if let Some(d) = s.path.get_mut(n) {
                    *d = c;
                    n += 1;
                }
            }
            let rc = fs(sys, -1, FS_OPEN_CREATE, s.path.as_mut_ptr(), n);
            if rc == EAGAIN || rc == EINPROGRESS {
                return 0;
            }
            if rc >= 0 {
                let mut z = [0u8; 1];
                fs(sys, rc, FS_CLOSE, z.as_mut_ptr(), 0);
            }
            let mut l = Line::new();
            l.put(b"[envp] ro_write=");
            l.int(i64::from(rc.min(0)));
            l.say(sys);
            s.phase = P_STATE_OPEN;
        }
        P_STATE_OPEN => {
            if s.state_len == 0 {
                s.phase = P_DONE;
                return 0;
            }
            let n = path_of(&mut s.path, s.state.get(..s.state_len).unwrap_or(&[]));
            let rc = fs(sys, -1, FS_OPEN, s.path.as_mut_ptr(), n);
            if rc == EAGAIN {
                return 0;
            }
            if rc == ENOENT {
                s.count = 0;
                s.phase = P_STATE_CREATE;
            } else if rc < 0 {
                let mut l = Line::new();
                l.put(b"[envp] state error=");
                l.int(i64::from(rc));
                l.say(sys);
                s.phase = P_DONE;
            } else {
                s.fd = rc;
                s.phase = P_STATE_READ;
            }
        }
        P_STATE_READ => {
            let mut buf = [0u8; 32];
            let rc = fs(sys, s.fd, FS_READ, buf.as_mut_ptr(), buf.len());
            if rc == EAGAIN {
                return 0;
            }
            s.count = if rc > 0 {
                parse(buf.get(..rc as usize).unwrap_or(&[]))
            } else {
                0
            };
            let mut z = [0u8; 1];
            fs(sys, s.fd, FS_CLOSE, z.as_mut_ptr(), 0);
            s.fd = -1;
            s.phase = P_STATE_CREATE;
        }
        P_STATE_CREATE => {
            let n = path_of(&mut s.path, s.state.get(..s.state_len).unwrap_or(&[]));
            let rc = fs(sys, -1, FS_OPEN_CREATE, s.path.as_mut_ptr(), n);
            if rc == EAGAIN || rc == EINPROGRESS {
                return 0;
            }
            if rc < 0 {
                let mut l = Line::new();
                l.put(b"[envp] state error=");
                l.int(i64::from(rc));
                l.say(sys);
                s.phase = P_DONE;
            } else {
                s.fd = rc;
                s.count = s.count.wrapping_add(1);
                s.phase = P_STATE_WRITE;
            }
        }
        P_STATE_WRITE => {
            let mut off = [0u8; 4];
            fs(sys, s.fd, FS_SEEK, off.as_mut_ptr(), off.len());
            let mut l = Line::new();
            l.int(i64::from(s.count));
            l.put(b"\n");
            let rc = fs(sys, s.fd, FS_WRITE, l.b.as_mut_ptr(), l.n);
            if rc == EAGAIN {
                return 0;
            }
            s.phase = P_STATE_FSYNC;
        }
        P_STATE_FSYNC => {
            let mut z = [0u8; 1];
            let rc = fs(sys, s.fd, FS_FSYNC, z.as_mut_ptr(), 0);
            if rc == EAGAIN || rc == EINPROGRESS {
                return 0;
            }
            fs(sys, s.fd, FS_CLOSE, z.as_mut_ptr(), 0);
            s.fd = -1;
            let mut l = Line::new();
            l.put(b"[envp] state=");
            l.int(i64::from(s.count));
            l.say(sys);
            s.phase = P_DONE;
        }
        // The secret read again now and then: a changed first line logged
        // once (an update of the mounted Secret, seen by the graph).
        P_DONE => {
            if s.secret_len == 0 {
                return 0;
            }
            s.idle = s.idle.wrapping_add(1);
            if s.idle >= WATCH_STEPS {
                s.idle = 0;
                s.phase = P_WATCH_OPEN;
            }
        }
        P_WATCH_OPEN => {
            let n = path_of(&mut s.path, s.secret.get(..s.secret_len).unwrap_or(&[]));
            let rc = fs(sys, -1, FS_OPEN, s.path.as_mut_ptr(), n);
            if rc == EAGAIN {
                return 0;
            }
            if rc < 0 {
                s.phase = P_DONE;
            } else {
                s.fd = rc;
                s.phase = P_WATCH_READ;
            }
        }
        P_WATCH_READ => {
            let mut buf = [0u8; 128];
            let rc = fs(sys, s.fd, FS_READ, buf.as_mut_ptr(), buf.len());
            if rc == EAGAIN {
                return 0;
            }
            let mut z = [0u8; 1];
            fs(sys, s.fd, FS_CLOSE, z.as_mut_ptr(), 0);
            s.fd = -1;
            s.phase = P_DONE;
            if rc < 0 {
                return 0;
            }
            let got = buf.get(..rc as usize).unwrap_or(&[]);
            let end = got.iter().position(|&c| c == b'\n').unwrap_or(got.len());
            let line = got.get(..end).unwrap_or(&[]);
            if line != s.seen.get(..s.seen_len).unwrap_or(&[]) {
                s.seen_len = line.len().min(s.seen.len());
                for (d, &c) in s.seen.iter_mut().zip(line) {
                    *d = c;
                }
                let mut l = Line::new();
                l.put(b"[envp] updated=");
                l.put(line);
                l.say(sys);
            }
        }
        _ => {}
    }
    0
}
