//! A container's httpGet and tcpSocket probes. See `manifest.toml` for the
//! wire.
#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::contracts::net::net_proto::{
    connected_parts, error_parts, write_connect_to, Target, CMD_CLOSE as NET_CMD_CLOSE,
    CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CMD_SEND as NET_CMD_SEND, CONNECT_TO_MAX,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_CONNECTED as NET_MSG_CONNECTED, MSG_DATA as NET_MSG_DATA,
    MSG_ERROR as NET_MSG_ERROR, REQUESTER_TAG_NONE,
};
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/argv.rs");

const CHAN_EINVAL: i32 = -22;
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const REC: usize = 8192;
const NET_BUF: usize = 3 + 8192;
/// The HTTP request (its frame stays under net_out's max_record).
const REQ: usize = 2048;
const OUT: usize = 512;
const KEY: usize = 160;
const OP_PUT: i64 = 3;
/// Attempts already answered, by (request, revision): not dialled again.
const DONE: usize = 32;

const P_IDLE: u8 = 0;
const P_CONNECT: u8 = 1;
const P_HTTP: u8 = 2;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    request_in: i32,
    response_out: i32,
    timeout_ms: u32,
    phase: u8,
    http: bool,
    conn_id: u16,
    conn_present: bool,
    started_ms: u64,
    key: [u8; KEY],
    key_len: usize,
    rev: i64,
    /// The HTTP request, written once connected.
    req: [u8; REQ],
    req_len: usize,
    /// The response's first bytes: its status line.
    resp: [u8; 256],
    resp_len: usize,
    /// The result record on its way out.
    out: [u8; OUT],
    out_len: usize,
    done: [(u32, i64); DONE],
    done_next: usize,
    rec: [u8; REC],
    net_buf: [u8; NET_BUF],
    tx: [u8; NET_BUF],
    passed: u32,
    failed: u32,
    last_hb_ms: u64,
    last_hb_sum: u32,
}

define_params! {
    State;

    1, timeout_ms, u32, 10000
        => |s, d, len| { s.timeout_ms = p_u32(d, len, 0, 10_000); };
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
    let sys = &*(syscalls as *const SyscallTable);
    s.syscalls = sys;
    s.net_in = in_chan;
    s.net_out = out_chan;
    s.request_in = dev_channel_port(sys, 0, 1);
    s.response_out = dev_channel_port(sys, 1, 1);
    s.phase = P_IDLE;
    s.out_len = 0;
    s.done = [(0, 0); DONE];
    s.done_next = 0;
    set_defaults(s);
    if !params.is_null() && params_len >= 4 {
        parse_tlv(s, params, params_len);
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
        if s.syscalls.is_null() {
            return 0;
        }
        let sys = &*s.syscalls;
        // Every frame on the shared channel, ours or not.
        for _ in 0..64 {
            if !pump_net(s, sys) {
                break;
            }
        }
        if s.out_len != 0 {
            if s.response_out < 0
                || (sys.channel_write)(s.response_out, s.out.as_ptr(), s.out_len) > 0
            {
                s.out_len = 0;
            } else {
                return 0;
            }
        }
        let now = dev_millis(sys);
        if s.phase != P_IDLE && now.wrapping_sub(s.started_ms) >= u64::from(s.timeout_ms.max(100)) {
            finish(s, sys, false);
        }
        if s.phase == P_IDLE && s.out_len == 0 {
            next(s, sys, now);
        }
        heartbeat(s, sys, now);
        0
    }
}

/// The next request that is this module's and not yet answered: dialled.
unsafe fn next(s: &mut State, sys: &SyscallTable, now: u64) {
    for _ in 0..16 {
        let Some(n) = take(sys, s.request_in, &mut s.rec) else {
            return;
        };
        let mut keyb = [0u8; KEY];
        let mut argvb = [0u8; 2048];
        let (kl, al, rev) = {
            let rec = s.rec.get(..n).unwrap_or(&[]);
            let k = frame_bytes(rec, 3);
            let a = frame_bytes(rec, 4);
            if k.len() > KEY || a.len() > argvb.len() {
                continue;
            }
            for (d, &c) in keyb.iter_mut().zip(k) {
                *d = c;
            }
            for (d, &c) in argvb.iter_mut().zip(a) {
                *d = c;
            }
            (k.len(), a.len(), frame_i64(rec, 2).unwrap_or(0))
        };
        let key = keyb.get(..kl).unwrap_or(&[]);
        let argv = argvb.get(..al).unwrap_or(&[]);
        if !argv.starts_with(b"@tcp ") && !argv.starts_with(b"@http ") {
            continue;
        }
        let h = fnv(key);
        if key.is_empty() || s.done.iter().any(|&(k, r)| k == h && r == rev) {
            continue;
        }
        // The argv's tokens, decoded.
        let mut toks = [[0u8; 128]; 3];
        let mut tl = [0usize; 3];
        let mut extra = [0u8; 1024];
        let mut el = 0usize;
        let mut k = 0usize;
        let mut ok = true;
        for t in argv_tokens(argv).skip(1) {
            if k < 3 {
                match argv_decode(t, &mut toks[k]) {
                    Some(m) => tl[k] = m,
                    None => ok = false,
                }
            } else {
                // A header, `<name>:<value>`, as its line.
                let mut hb = [0u8; 256];
                match argv_decode(t, &mut hb) {
                    Some(m) if el + m + 2 <= extra.len() => {
                        for (d, &c) in extra.iter_mut().skip(el).zip(hb.iter().take(m)) {
                            *d = c;
                        }
                        el += m;
                        if let Some(d) = extra.get_mut(el..el + 2) {
                            d[0] = b'\r';
                            d[1] = b'\n';
                        }
                        el += 2;
                    }
                    _ => ok = false,
                }
            }
            k += 1;
        }
        let http = argv.starts_with(b"@http ");
        let port = parse_port(toks[1].get(..tl[1]).unwrap_or(&[]));
        if !ok || k < if http { 3 } else { 2 } || port == 0 {
            // A probe this node cannot dial: failed, so it is not waited on.
            remember_key(s, key, rev);
            result(s, false);
            continue;
        }
        remember_key(s, key, rev);
        s.http = http;
        s.req_len = 0;
        if http {
            let host = toks[0].get(..tl[0]).unwrap_or(&[]);
            let path = toks[2].get(..tl[2]).unwrap_or(&[]);
            let mut pd = [0u8; 8];
            let pn = dec(u64::from(port), &mut pd);
            let mut n = 0usize;
            let r = &mut s.req;
            for part in [
                b"GET ".as_slice(),
                path,
                b" HTTP/1.1\r\nHost: ",
                host,
                b":",
                pd.get(..pn).unwrap_or(&[]),
                b"\r\nUser-Agent: kube-probe/1.30\r\nAccept: */*\r\nConnection: close\r\n",
                extra.get(..el).unwrap_or(&[]),
                b"\r\n",
            ] {
                for &c in part {
                    if let Some(d) = r.get_mut(n) {
                        *d = c;
                        n += 1;
                    }
                }
            }
            s.req_len = n;
        }
        let host = toks[0].get(..tl[0]).unwrap_or(&[]);
        let mut payload = [0u8; CONNECT_TO_MAX];
        let pl = match Target::parse(host) {
            Some((t, _)) => write_connect_to(
                &mut payload,
                SOCK_TYPE_STREAM,
                port,
                &t,
                Some(dev_requester_tag(sys)),
            ),
            None => 0,
        };
        if pl == 0
            || net_write_frame(
                sys,
                s.net_out,
                NET_CMD_CONNECT_TO,
                payload.as_ptr(),
                pl,
                s.tx.as_mut_ptr(),
                NET_BUF,
            ) == 0
        {
            result(s, false);
            continue;
        }
        s.phase = P_CONNECT;
        s.conn_present = false;
        s.resp_len = 0;
        s.started_ms = now;
        return;
    }
}

/// One frame off the shared channel, acted on when it is this attempt's.
/// False when there was none.
unsafe fn pump_net(s: &mut State, sys: &SyscallTable) -> bool {
    let poll = (sys.channel_poll)(s.net_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return false;
    }
    let nb = s.net_buf.as_mut_ptr();
    let (msg, plen) = net_read_frame(sys, s.net_in, nb, NET_BUF);
    if msg == 0 {
        return false;
    }
    let payload = core::slice::from_raw_parts(nb.add(3), plen);
    match msg {
        NET_MSG_CONNECTED if s.phase == P_CONNECT && plen >= 2 => {
            let (cid, tag) = connected_parts(payload);
            if tag == dev_requester_tag(sys) {
                s.conn_id = cid;
                s.conn_present = true;
                if !s.http {
                    // Accepted: a tcpSocket probe has passed.
                    finish(s, sys, true);
                } else {
                    let mut buf = [0u8; 2 + REQ];
                    buf[0] = cid as u8;
                    buf[1] = (cid >> 8) as u8;
                    for (d, &c) in buf.iter_mut().skip(2).zip(s.req.iter().take(s.req_len)) {
                        *d = c;
                    }
                    if net_write_frame(
                        sys,
                        s.net_out,
                        NET_CMD_SEND,
                        buf.as_ptr(),
                        2 + s.req_len,
                        s.tx.as_mut_ptr(),
                        NET_BUF,
                    ) == 0
                    {
                        finish(s, sys, false);
                    } else {
                        s.phase = P_HTTP;
                    }
                }
            }
        }
        NET_MSG_DATA if s.phase == P_HTTP && plen >= 2 && s.conn_present => {
            if u16::from_le_bytes([payload[0], payload[1]]) == s.conn_id {
                for &c in payload.get(2..).unwrap_or(&[]) {
                    if let Some(d) = s.resp.get_mut(s.resp_len) {
                        *d = c;
                        s.resp_len += 1;
                    }
                }
                let have = s.resp.get(..s.resp_len).unwrap_or(&[]);
                if let Some(eol) = have.iter().position(|&c| c == b'\n') {
                    let code = status_code(have.get(..eol).unwrap_or(&[]));
                    finish(s, sys, (200..400).contains(&code));
                } else if s.resp_len >= s.resp.len() {
                    finish(s, sys, false);
                }
            }
        }
        NET_MSG_CLOSED if s.phase != P_IDLE && s.conn_present && plen >= 2 => {
            if u16::from_le_bytes([payload[0], payload[1]]) == s.conn_id {
                s.conn_present = false;
                finish(s, sys, false);
            }
        }
        NET_MSG_ERROR if s.phase != P_IDLE && plen >= 3 => {
            let (cid, _errno, tag) = error_parts(payload);
            let ours = if tag != REQUESTER_TAG_NONE {
                s.phase == P_CONNECT && tag == dev_requester_tag(sys)
            } else {
                s.conn_present && cid == s.conn_id
            };
            if ours {
                s.conn_present = false;
                finish(s, sys, false);
            }
        }
        _ => {}
    }
    true
}

/// `HTTP/1.x <code> …` -> code; 0 for anything else.
fn status_code(line: &[u8]) -> u16 {
    if !line.starts_with(b"HTTP/") {
        return 0;
    }
    let Some(sp) = line.iter().position(|&c| c == b' ') else {
        return 0;
    };
    let mut v = 0u16;
    for &c in line.iter().skip(sp + 1).take(3) {
        if !c.is_ascii_digit() {
            return 0;
        }
        v = v * 10 + u16::from(c - b'0');
    }
    v
}

/// The attempt is over: its connection closed, its result written.
unsafe fn finish(s: &mut State, sys: &SyscallTable, passed: bool) {
    if s.conn_present {
        let cid = s.conn_id.to_le_bytes();
        let _ = net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            cid.as_ptr(),
            2,
            s.tx.as_mut_ptr(),
            NET_BUF,
        );
        s.conn_present = false;
    }
    s.phase = P_IDLE;
    result(s, passed);
}

/// `/sandbox-exec-result/<key>` = 0 (passed) or 1, as an exit code.
fn result(s: &mut State, passed: bool) {
    if passed {
        s.passed = s.passed.wrapping_add(1);
    } else {
        s.failed = s.failed.wrapping_add(1);
    }
    let key_len = s.key_len;
    let mut key = [0u8; KEY];
    for (d, &c) in key.iter_mut().zip(s.key.iter().take(key_len)) {
        *d = c;
    }
    let code: &[u8] = if passed { b"0" } else { b"1" };
    let mut p = 1usize;
    let mut ok = true;
    let cid = 0i64.to_le_bytes();
    let op = OP_PUT.to_le_bytes();
    for (num, ty, val) in [
        (1u8, TY_I64, cid.as_slice()),
        (2, TY_I64, op.as_slice()),
        (3, TY_BYTES, key.get(..key_len).unwrap_or(&[])),
        (4, TY_BYTES, code),
    ] {
        match put_field(&mut s.out, p, num, ty, val) {
            Some(e) => p = e,
            None => ok = false,
        }
    }
    if let Some(c) = s.out.get_mut(0) {
        *c = 4;
    }
    s.out_len = if ok { p } else { 0 };
}

fn remember_key(s: &mut State, key: &[u8], rev: i64) {
    let n = key.len().min(KEY);
    for (d, &c) in s.key.iter_mut().zip(key.iter().take(n)) {
        *d = c;
    }
    s.key_len = n;
    s.rev = rev;
    if let Some(e) = s.done.get_mut(s.done_next % DONE) {
        *e = (fnv(key), rev);
    }
    s.done_next = s.done_next.wrapping_add(1);
}

fn fnv(b: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &c in b {
        h ^= u32::from(c);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn parse_port(b: &[u8]) -> u16 {
    if b.is_empty() || b.len() > 5 || !b.iter().all(|c| c.is_ascii_digit()) {
        return 0;
    }
    let mut v = 0u32;
    for &c in b {
        v = v * 10 + u32::from(c - b'0');
    }
    if v > 65535 {
        0
    } else {
        v as u16
    }
}

fn dec(v: u64, out: &mut [u8]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 || i == 0 {
            break;
        }
    }
    let n = tmp.len() - i;
    for (d, &c) in out.iter_mut().zip(tmp.iter().skip(i)) {
        *d = c;
    }
    n.min(out.len())
}

fn put_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    let len = u16::try_from(payload.len()).ok()?;
    let end = at.checked_add(4 + payload.len())?;
    let dst = out.get_mut(at..end)?;
    let le = len.to_le_bytes();
    let head = [number, ty, le[0], le[1]];
    for (d, &c) in dst.iter_mut().zip(head.iter().chain(payload.iter())) {
        *d = c;
    }
    Some(end)
}

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let num = *frame.get(p)?;
        let ty = *frame.get(p + 1)?;
        let len = u16::from_le_bytes([*frame.get(p + 2)?, *frame.get(p + 3)?]) as usize;
        let payload = frame.get(p + 4..p + 4 + len)?;
        if num == number {
            return Some((ty, payload));
        }
        p += 4 + len;
    }
    None
}

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, b)) => b,
        _ => &[],
    }
}

fn frame_i64(frame: &[u8], number: u8) -> Option<i64> {
    match frame_field(frame, number) {
        Some((TY_I64, b)) => <[u8; 8]>::try_from(b).ok().map(i64::from_le_bytes),
        _ => None,
    }
}

fn frame_len(data: &[u8]) -> Option<usize> {
    let count = *data.first()? as usize;
    let mut off = 1usize;
    for _ in 0..count {
        let l = u16::from_le_bytes([*data.get(off + 2)?, *data.get(off + 3)?]) as usize;
        off += 4 + l;
    }
    (off <= data.len()).then_some(off)
}

unsafe fn take(sys: &SyscallTable, chan: i32, out: &mut [u8; REC]) -> Option<usize> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(chan, out.as_mut_ptr(), REC);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(chan, out.as_mut_ptr(), REC)
    } else if pk <= 0 {
        return None;
    } else {
        match frame_len(out.get(..pk as usize).unwrap_or(&[])) {
            Some(fl) => (sys.channel_read)(chan, out.as_mut_ptr(), fl),
            None if (pk as usize) < REC => return None,
            None => {
                (sys.channel_read)(chan, out.as_mut_ptr(), REC);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}

/// `[probe_dial] hb passed=… failed=…` when they moved, at most every 10 s.
unsafe fn heartbeat(s: &mut State, sys: &SyscallTable, now: u64) {
    let sum = s.passed.wrapping_add(s.failed);
    if sum == s.last_hb_sum || now.wrapping_sub(s.last_hb_ms) < 10_000 {
        return;
    }
    s.last_hb_ms = now;
    s.last_hb_sum = sum;
    let mut m = [0u8; 96];
    let mut n = 0usize;
    let mut put = |b: &[u8], n: &mut usize| {
        for &c in b {
            if let Some(d) = m.get_mut(*n) {
                *d = c;
                *n += 1;
            }
        }
    };
    let mut d = [0u8; 20];
    put(b"[probe_dial] hb passed=", &mut n);
    let k = dec(u64::from(s.passed), &mut d);
    put(d.get(..k).unwrap_or(&[]), &mut n);
    put(b" failed=", &mut n);
    let k = dec(u64::from(s.failed), &mut d);
    put(d.get(..k).unwrap_or(&[]), &mut n);
    dev_log(sys, 3, m.as_ptr(), n);
}
