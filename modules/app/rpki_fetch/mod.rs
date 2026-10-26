//! RPKI fetching: a repository's ROAs over RRDP → validated ROA payloads in
//! the store.
//!
//! See `manifest.toml`. Every `poll_ms`: GET the notification file
//! (`notify`, an `http://a.b.c.d:port/path` URI), and when its session or
//! serial moved, GET the snapshot it names, check the snapshot against the
//! notification's hash, session and serial (`_shared/rrdp.rs`), and validate
//! every published `.roa` against the trust anchor held at
//! `/wh-rpki/anchor` (base64 DER) with `_shared/rpki.rs`. The validated
//! payloads replace `/wh-rpki/vrps` whole (one row: a snapshot is all or
//! nothing), and `/wh-rpki/status` says what was taken and refused. One
//! connection at a time, `Connection: close`; a failed fetch leaves the
//! rows as they were.

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
    connected_parts, write_connect_to, Target, CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CONNECT_TO_MAX,
};
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/rsa.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/b64.rs");
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;
include!("../_shared/store.rs");
include!("../_shared/wormhole_traffic.rs");
include!("../_shared/rpki.rs");
include!("../_shared/rrdp.rs");

const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;

const ANCHOR_ROW: &[u8] = b"/wh-rpki/anchor";
const VRPS_ROW: &[u8] = b"/wh-rpki/vrps";
const STATUS_ROW: &[u8] = b"/wh-rpki/status";
/// One HTTP response, header and body.
const RESP_MAX: usize = 65536;
const NET_BUF: usize = 3 + 2 + 4096;
const ANCHOR_MAX: usize = 2048;
const OBJ_MAX: usize = 4096;
const VRPS_TEXT_MAX: usize = 4096;
const TIMEOUT_MS: u64 = 10_000;

mod phase {
    pub const IDLE: u8 = 0;
    pub const CONNECTING: u8 = 1;
    pub const RECEIVING: u8 = 2;
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    poll_ms: u32,
    phase: u8,
    /// 0: the notification is being fetched; 1: the snapshot.
    leg: u8,
    conn_present: u8,
    last_note: u8,
    conn_id: u16,
    _pad: u16,
    started_ms: u64,
    next_ms: u64,
    notify: [u8; RRDP_URI_MAX],
    notify_len: u8,
    /// The serial last taken (and its session), so an unmoved repository is
    /// not fetched again.
    taken_serial: u64,
    taken_session: [u8; RRDP_SESSION_LEN],
    notification: RrdpNotification,
    resp_len: usize,
    nag: ReadNag,
    pub fetched: u32,
    pub roas: u32,
    pub refused: u32,
    pub vrps: u32,
    pub failures: u32,
    job: RsaVerifyJob,
    resp: [u8; RESP_MAX],
    net: [u8; NET_BUF],
    anchor: [u8; ANCHOR_MAX],
    obj: [u8; OBJ_MAX],
    scratch: [u8; OBJ_MAX * 2],
    text: [u8; VRPS_TEXT_MAX],
}

define_params! {
    State;

    1, notify, str, 0 => |s, d, len| {
        let t = core::slice::from_raw_parts(d, len);
        let t = t.split(|&b| b == 0).next().unwrap_or(&[]);
        if t.len() <= RRDP_URI_MAX {
            s.notify[..t.len()].copy_from_slice(t);
            s.notify_len = t.len() as u8;
        }
    };
    2, poll_ms, u32, 5000 => |s, d, len| { s.poll_ms = p_u32(d, len, 0, 5000); };
}

/// `http://a.b.c.d[:port]/path` → (address, port, path).
fn parse_uri(u: &[u8]) -> Option<([u8; 4], u16, &[u8])> {
    let rest = u.strip_prefix(b"http://")?;
    let slash = rest.iter().position(|&c| c == b'/')?;
    let (auth, path) = (&rest[..slash], &rest[slash..]);
    let (host, port) = match auth.iter().position(|&c| c == b':') {
        Some(c) => {
            let mut p = 0u32;
            for &d in &auth[c + 1..] {
                if !d.is_ascii_digit() {
                    return None;
                }
                p = p * 10 + u32::from(d - b'0');
            }
            (&auth[..c], u16::try_from(p).ok()?)
        }
        None => (auth, 80),
    };
    let mut a = [0u8; 4];
    let mut k = 0;
    for part in host.split(|&c| c == b'.') {
        if k == 4 || part.is_empty() {
            return None;
        }
        let mut v = 0u32;
        for &d in part {
            if !d.is_ascii_digit() {
                return None;
            }
            v = v * 10 + u32::from(d - b'0');
        }
        a[k] = u8::try_from(v).ok()?;
        k += 1;
    }
    (k == 4).then_some((a, port, path))
}

unsafe fn note(s: &mut State, code: u8, m: &[u8]) {
    if s.last_note != code {
        s.last_note = code;
        dev_log(&*s.syscalls, 2, m.as_ptr(), m.len());
    }
}

unsafe fn fail_fetch(s: &mut State, m: &[u8]) {
    s.failures = s.failures.wrapping_add(1);
    note(s, 1, m);
    close(s);
    s.phase = phase::IDLE;
    s.next_ms = dev_millis(&*s.syscalls) + u64::from(s.poll_ms);
}

unsafe fn close(s: &mut State) {
    if s.conn_present == 1 {
        let sys = &*s.syscalls;
        let id = s.conn_id.to_le_bytes();
        let _ = net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            id.as_ptr(),
            2,
            s.net.as_mut_ptr(),
            NET_BUF,
        );
        s.conn_present = 0;
    }
}

/// Start a GET of `uri`.
unsafe fn start(s: &mut State, leg: u8) {
    let sys = &*s.syscalls;
    let uri: &[u8] = if leg == 0 {
        &s.notify[..usize::from(s.notify_len)]
    } else {
        &s.notification.snapshot_uri[..s.notification.snapshot_uri_len]
    };
    let Some((addr, port, _)) = parse_uri(uri) else {
        fail_fetch(
            s,
            b"[rpki_fetch] a URI that is not http://a.b.c.d[:port]/path",
        );
        return;
    };
    let mut p = [0u8; CONNECT_TO_MAX];
    let n = write_connect_to(
        &mut p,
        SOCK_TYPE_STREAM,
        port,
        &Target::V4(addr),
        Some(dev_requester_tag(sys)),
    );
    if n == 0 {
        fail_fetch(s, b"[rpki_fetch] no dial");
        return;
    }
    if net_write_frame(
        sys,
        s.net_out,
        NET_CMD_CONNECT_TO,
        p.as_ptr(),
        n,
        s.net.as_mut_ptr(),
        NET_BUF,
    ) > 0
    {
        s.leg = leg;
        s.resp_len = 0;
        s.phase = phase::CONNECTING;
        s.started_ms = dev_millis(sys);
    }
}

/// The request for the leg in flight, sent once connected.
unsafe fn send_request(s: &mut State) {
    let sys = &*s.syscalls;
    let uri: &[u8] = if s.leg == 0 {
        &s.notify[..usize::from(s.notify_len)]
    } else {
        &s.notification.snapshot_uri[..s.notification.snapshot_uri_len]
    };
    let Some((addr, port, path)) = parse_uri(uri) else {
        return;
    };
    let mut req = [0u8; 2 + 512];
    req[..2].copy_from_slice(&s.conn_id.to_le_bytes());
    let mut n = 2;
    for part in [&b"GET "[..], path, b" HTTP/1.1\r\nHost: "] {
        req[n..n + part.len()].copy_from_slice(part);
        n += part.len();
    }
    for (i, o) in addr.iter().enumerate() {
        let mut d = [0u8; 3];
        let mut k = 3;
        let mut v = *o;
        loop {
            k -= 1;
            d[k] = b'0' + v % 10;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        req[n..n + 3 - k].copy_from_slice(&d[k..]);
        n += 3 - k;
        if i < 3 {
            req[n] = b'.';
            n += 1;
        }
    }
    let _ = port;
    let tail = b"\r\nAccept: application/xml\r\nConnection: close\r\n\r\n";
    req[n..n + tail.len()].copy_from_slice(tail);
    n += tail.len();
    let _ = net_write_frame(
        sys,
        s.net_out,
        NET_CMD_SEND,
        req.as_ptr(),
        n,
        s.net.as_mut_ptr(),
        NET_BUF,
    );
}

fn find(h: &[u8], n: &[u8]) -> Option<usize> {
    rrdp_find(h, n)
}

/// The body of a whole `200` response in `resp`, or `None`.
fn body(resp: &[u8]) -> Option<&[u8]> {
    if !resp.starts_with(b"HTTP/1.1 200") && !resp.starts_with(b"HTTP/1.0 200") {
        return None;
    }
    let end = find(resp, b"\r\n\r\n")?;
    Some(&resp[end + 4..])
}

/// One validated payload as its text line: `<prefix>/<len> <max> AS<asn>`.
fn vrp_line(v: &traffic::Vrp, out: &mut [u8], at: usize) -> Option<usize> {
    let mut n = at;
    let mut put = |b: &[u8], n: &mut usize| -> Option<()> {
        out.get_mut(*n..*n + b.len())?.copy_from_slice(b);
        *n += b.len();
        Some(())
    };
    let dec = |mut v: u32, d: &mut [u8; 10]| -> usize {
        let mut k = 10;
        loop {
            k -= 1;
            d[k] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        k
    };
    let mut d = [0u8; 10];
    let a = &v.prefix.addr;
    if a.family == 4 {
        for i in 0..4 {
            let k = dec(u32::from(a.bytes[12 + i]), &mut d);
            put(&d[k..], &mut n)?;
            if i < 3 {
                put(b".", &mut n)?;
            }
        }
    } else {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for i in 0..8 {
            let w = u16::from_be_bytes([a.bytes[2 * i], a.bytes[2 * i + 1]]);
            for sh in [12, 8, 4, 0] {
                put(&[HEX[usize::from((w >> sh) & 15)]], &mut n)?;
            }
            if i < 7 {
                put(b":", &mut n)?;
            }
        }
    }
    put(b"/", &mut n)?;
    let k = dec(u32::from(v.prefix.len), &mut d);
    put(&d[k..], &mut n)?;
    put(b" ", &mut n)?;
    let k = dec(u32::from(v.max_len), &mut d);
    put(&d[k..], &mut n)?;
    put(b" AS", &mut n)?;
    let k = dec(v.asn, &mut d);
    put(&d[k..], &mut n)?;
    put(b"\n", &mut n)?;
    Some(n)
}

/// The notification arrived: fetch the snapshot if the repository moved.
unsafe fn on_notification(s: &mut State) {
    let resp = s.resp;
    let Some(b) = body(&resp[..s.resp_len]) else {
        fail_fetch(s, b"[rpki_fetch] the notification was not a 200");
        return;
    };
    match rrdp_notification(b) {
        Ok(n) => {
            if n.serial == s.taken_serial && n.session == s.taken_session {
                s.phase = phase::IDLE;
                s.next_ms = dev_millis(&*s.syscalls) + u64::from(s.poll_ms);
                return;
            }
            s.notification = n;
            start(s, 1);
        }
        Err(_) => fail_fetch(s, b"[rpki_fetch] the notification does not read"),
    }
}

/// The snapshot arrived: validate every ROA and replace the rows.
unsafe fn on_snapshot(s: &mut State) {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let Some(an) = get_value(&store, ANCHOR_ROW, &mut s.text) else {
        fail_fetch(s, b"[rpki_fetch] no trust anchor at /wh-rpki/anchor");
        return;
    };
    let mut scratch = [0u8; VRPS_TEXT_MAX];
    let text = s.text;
    let Some(al) = rrdp_b64(&text[..an], &mut scratch, &mut s.anchor) else {
        fail_fetch(s, b"[rpki_fetch] the trust anchor row is not base64");
        return;
    };
    let now_s = dev_unix_millis(sys) / 1000;
    let resp = s.resp;
    let Some(snap) = body(&resp[..s.resp_len]) else {
        fail_fetch(s, b"[rpki_fetch] the snapshot was not a 200");
        return;
    };
    let mut text_n = 0usize;
    let mut roas = 0u32;
    let mut refused = 0u32;
    let mut vrps = 0u32;
    let mut overflow = false;
    let n = s.notification;
    let State {
        job,
        anchor,
        obj,
        scratch: sc,
        text: out,
        ..
    } = s;
    let r = rrdp_snapshot(snap, &n, &mut |uri, b64| {
        if !uri.ends_with(b".roa") {
            return;
        }
        roas += 1;
        let Some(len) = rrdp_b64(b64, sc, obj) else {
            refused += 1;
            return;
        };
        match roa_validate(job, &anchor[..al], &obj[..len], now_s) {
            Ok(v) => {
                for p in &v.vrps[..v.n] {
                    let vrp = traffic::Vrp {
                        prefix: p.prefix,
                        max_len: p.max_len,
                        asn: v.asn,
                    };
                    match vrp_line(&vrp, out, text_n) {
                        Some(e) => {
                            text_n = e;
                            vrps += 1;
                        }
                        None => overflow = true,
                    }
                }
            }
            Err(_) => refused += 1,
        }
    });
    if r.is_err() || overflow {
        fail_fetch(
            s,
            b"[rpki_fetch] the snapshot is not this notification's, or too large",
        );
        return;
    }
    let text = s.text;
    if !matches!(
        put_value(&store, VRPS_ROW, &text[..text_n]),
        WriteAnswer::Decided(0)
    ) {
        fail_fetch(s, b"[rpki_fetch] the store did not take the VRPs");
        return;
    }
    let mut st = [0u8; 160];
    let mut k = append(&mut st, 0, b"session=");
    k = append(&mut st, k, &s.notification.session);
    for (label, v) in [
        (&b";serial="[..], s.notification.serial),
        (&b";roas="[..], u64::from(roas)),
        (&b";refused="[..], u64::from(refused)),
        (&b";vrps="[..], u64::from(vrps)),
    ] {
        k = append(&mut st, k, label);
        let mut d = [0u8; 20];
        let mut i = 20;
        let mut x = v;
        loop {
            i -= 1;
            d[i] = b'0' + (x % 10) as u8;
            x /= 10;
            if x == 0 {
                break;
            }
        }
        k = append(&mut st, k, &d[i..]);
    }
    let _ = put_value(&store, STATUS_ROW, &st[..k]);
    s.taken_serial = s.notification.serial;
    s.taken_session = s.notification.session;
    s.fetched = s.fetched.wrapping_add(1);
    s.roas = roas;
    s.refused = refused;
    s.vrps = vrps;
    note(s, 2, b"[rpki_fetch] snapshot taken");
    s.phase = phase::IDLE;
    s.next_ms = dev_millis(sys) + u64::from(s.poll_ms);
}

unsafe fn pump_net(s: &mut State) {
    let sys = &*s.syscalls;
    for _ in 0..16 {
        let (t, plen) = net_read_frame(sys, s.net_in, s.net.as_mut_ptr(), NET_BUF);
        if t == 0 {
            return;
        }
        let p = core::slice::from_raw_parts(s.net.as_ptr().add(3), plen);
        match t {
            NET_MSG_CONNECTED if s.phase == phase::CONNECTING && plen >= 2 => {
                let (cid, tag) = connected_parts(p);
                if tag == dev_requester_tag(sys) {
                    s.conn_id = cid;
                    s.conn_present = 1;
                    s.phase = phase::RECEIVING;
                    send_request(s);
                }
            }
            NET_MSG_ERROR if s.phase == phase::CONNECTING => {
                fail_fetch(s, b"[rpki_fetch] the repository refused the connection");
            }
            NET_MSG_DATA if s.phase == phase::RECEIVING && plen > 2 => {
                if u16::from_le_bytes([p[0], p[1]]) != s.conn_id {
                    continue;
                }
                let d = &p[2..];
                if s.resp_len + d.len() > RESP_MAX {
                    fail_fetch(s, b"[rpki_fetch] a response past the limit");
                    return;
                }
                let at = s.resp_len;
                s.resp[at..at + d.len()].copy_from_slice(d);
                s.resp_len += d.len();
            }
            NET_MSG_CLOSED if s.phase == phase::RECEIVING && plen >= 2 => {
                if u16::from_le_bytes([p[0], p[1]]) != s.conn_id {
                    continue;
                }
                s.conn_present = 0;
                if s.leg == 0 {
                    on_notification(s);
                } else {
                    on_snapshot(s);
                }
            }
            _ => {}
        }
    }
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
        core::ptr::write_bytes(state, 0, core::mem::size_of::<State>());
        let s = &mut *(state as *mut State);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.poll_ms = 5000;
        s.nag = ReadNag::new();
        s.job = RsaVerifyJob::new();
        parse_tlv(s, params, params_len);
        if s.net_in < 0
            || s.net_out < 0
            || s.poll_ms == 0
            || parse_uri(&s.notify[..usize::from(s.notify_len)]).is_none()
        {
            let m =
                b"[rpki_fetch] net_in/net_out wired and notify an http://a.b.c.d[:port]/path URI";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -3;
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
        if s.syscalls.is_null() {
            return 0;
        }
        pump_net(s);
        let now = dev_millis(&*s.syscalls);
        match s.phase {
            phase::IDLE if now >= s.next_ms => start(s, 0),
            phase::CONNECTING | phase::RECEIVING if now.wrapping_sub(s.started_ms) > TIMEOUT_MS => {
                fail_fetch(s, b"[rpki_fetch] the repository did not answer in time");
            }
            _ => {}
        }
        0
    }
}
