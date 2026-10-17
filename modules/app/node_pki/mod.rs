//! A node's own PKI: certifies the API's vault-held TLS key through kagi's
//! authority, and mints the single-use join secret that releases the
//! operator's first credential.
//!
//! See `manifest.toml` for the wire layouts and the rationale.

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
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/exchange_call.rs");

// kagi owns the encoding its authority speaks.
#[path = "../../../target/fluxor/kagi-common/b64.rs"]
mod b64;

// The control-plane store (storage.object 0x14, storage.namespace 0x13), as
// `_shared/store.rs` expects its includer to name them.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 160;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const ID_MSG_REQ: u8 = 0x5B;
const ID_MSG_CERT: u8 = 0x5C;
const POINT: usize = 65;
/// A certificate, and the authority's: what tls holds (its chain buffer).
const CHAIN_MAX: usize = 3072;
const CERT_MAX: usize = 2048;
const TOKEN_MAX: usize = 2048;
const PARAM_MAX: usize = 128;
const BUF: usize = 8192;
/// Between reads of an awaited token, and between retries of a refused one.
const POLL_MS: u64 = 250;
const RETRY_MS: u64 = 5000;
/// A minted token not answered in this long is asked for again.
const TOKEN_WAIT_MS: u64 = 60_000;
/// A write the store has not decided is asked again no sooner than this. A
/// replicated store answers a write `Pending` until its log decides it, and
/// every ask is a new proposal: asked each step, one delete became thousands
/// of tombstones on the board, flushed into thousands of run files.
const REASK_MS: u64 = 1000;

const JOIN_PREFIX: &[u8] = b"/join/";
/// Worker slots a join may carry credentials for (`join_nodes`).
const MAX_SLOTS: usize = 4;
/// The join record: the admin token, each slot's, and the authority.
const PUB_MAX: usize = TOKEN_MAX * (1 + MAX_SLOTS) + CERT_MAX * 2 + 256;
/// Between reads of the Device while its certificate is awaited.
const REMOTE_POLL_MS: u64 = 1000;

// Remote (worker) steps: what the next call asks.
const R_IDLE: u8 = 0;
const R_SPEC: u8 = 1;
const R_CLEAR: u8 = 2;
const R_POLL: u8 = 3;

// Join phases.
const J_OFF: u8 = 0;
const J_CLEAN: u8 = 1;
const J_ASK: u8 = 2;
const J_WAIT: u8 = 3;
const J_DROP_REQ: u8 = 4;
const J_DROP_RESP: u8 = 5;
const J_PUBLISH: u8 = 6;
const J_DONE: u8 = 7;
const J_DECLARE: u8 = 8;
/// The secret is spent (its record taken): nothing more to say.
const J_SPENT: u8 = 9;
/// Between repeats of an unspent join's announcement. Its only reader may
/// be a lossy log (a board's telemetry drops and truncates lines), so the
/// line is said again until the secret buys its credential.
const ANNOUNCE_EVERY_MS: u64 = 20_000;

/// Further local identities one instance certifies (`id2_*`, `id3_*`): a
/// plane's peer link ends beside its API, on port pairs 2 and 3 — one
/// module where a board seated near its module ceiling had three.
const EXTRA_IDS: usize = 2;

#[repr(C)]
struct Extra {
    subject: [u8; PARAM_MAX],
    subject_len: u8,
    tenant: [u8; PARAM_MAX],
    tenant_len: u8,
    device: [u8; PARAM_MAX],
    device_len: u8,
    dns: [u8; PARAM_MAX],
    dns_len: u8,
    req_in: i32,
    cert_out: i32,
    point: [u8; POINT],
    point_owed: u8,
    chain: [u8; 3 + CHAIN_MAX],
    chain_len: u16,
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    id_req_in: i32,
    extra: [Extra; EXTRA_IDS],
    /// Which identity the authority call in flight certifies (0 the
    /// first; 1.. the extras).
    cur: u8,
    response_in: i32,
    id_cert_out: i32,
    request_out: i32,
    resolved: u8,

    subject: [u8; PARAM_MAX],
    subject_len: u8,
    tenant: [u8; PARAM_MAX],
    tenant_len: u8,
    device: [u8; PARAM_MAX],
    device_len: u8,
    dns: [u8; PARAM_MAX],
    dns_len: u8,
    join: u8,
    join_ns: [u8; PARAM_MAX],
    join_ns_len: u8,
    join_sa: [u8; PARAM_MAX],
    join_sa_len: u8,
    join_ttl: u32,
    /// `join_nodes`: the worker slots (replica ids) a join carries a
    /// bootstrap credential for.
    slots: [u8; MAX_SLOTS],
    nslots: u8,
    /// `remote`: the API path of the Device this identity is certified
    /// through (a worker's); empty asks the authority directly.
    remote: [u8; PARAM_MAX],
    remote_len: u8,
    /// `bearer`: the credential the API is asked with (a slot's token).
    bearer: [u8; TOKEN_MAX],
    bearer_len: u16,
    /// The blob's stated length (u16::MAX: past TOKEN_MAX).
    bearer_total: u16,
    r_phase: u8,
    r_next_at: u64,
    /// The certificate the Device held when this ask began (FNV-1a): the
    /// answer is a certificate other than it.
    r_old: u32,
    /// The point asked for, base64url, as the Device states it.
    r_key: [u8; 96],
    r_key_len: u8,
    r_refused: u32,

    /// The point tls last asked to have certified, and whether a call for it
    /// is still owed.
    point: [u8; POINT],
    point_owed: u8,
    call: Call,

    /// A chain frame tls has not taken yet.
    chain: [u8; 3 + CHAIN_MAX],
    chain_len: u16,
    /// The authority's certificate, from its last answer.
    ca: [u8; CERT_MAX],
    ca_len: u16,

    phase: u8,
    next_at: u64,
    asked_at: u64,
    /// When the current phase's write was last asked (0 = not yet).
    wrote_at: u64,
    join_id: [u8; 16],
    secret: [u8; 32],
    token: [u8; TOKEN_MAX],
    token_len: u16,
    /// Which credential the join is minting: 0 the admin's, i the i-th
    /// slot's; and whether the last asked for was answered.
    tok_n: u8,
    tok_ok: u8,
    /// The bootstrap object `declared` is writing.
    decl_i: u8,
    /// When the join last said where it stands (`[node_pki] join hb`).
    hb_at: u64,
    node_tokens: [[u8; TOKEN_MAX]; MAX_SLOTS],
    node_token_len: [u16; MAX_SLOTS],
    pub_buf: [u8; PUB_MAX],

    certified: u32,
    cert_refused: u32,
    joins_minted: u32,

    in_buf: [u8; BUF],
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

mod params_def {
    use super::State;
    use super::PARAM_MAX;
    use super::SCHEMA_MAX;
    use super::{p_u32, p_u8};

    unsafe fn set(dst: &mut [u8; PARAM_MAX], dlen: &mut u8, d: *const u8, len: usize) {
        let n = if len > PARAM_MAX { 0 } else { len };
        if n > 0 {
            core::ptr::copy_nonoverlapping(d, dst.as_mut_ptr(), n);
        }
        *dlen = n as u8;
    }

    define_params! {
        State;

        1, subject, str, 0
            => |s, d, len| { set(&mut s.subject, &mut s.subject_len, d, len); };
        2, tenant, str, 0
            => |s, d, len| { set(&mut s.tenant, &mut s.tenant_len, d, len); };
        3, device, str, 0
            => |s, d, len| { set(&mut s.device, &mut s.device_len, d, len); };
        4, dns, str, 0
            => |s, d, len| { set(&mut s.dns, &mut s.dns_len, d, len); };
        5, join, u8, 0
            => |s, d, len| { s.join = p_u8(d, len, 0, 0); };
        6, join_namespace, str, 0
            => |s, d, len| { set(&mut s.join_ns, &mut s.join_ns_len, d, len); };
        7, join_serviceaccount, str, 0
            => |s, d, len| { set(&mut s.join_sa, &mut s.join_sa_len, d, len); };
        // The admin credential's lifetime, seconds (30 days).
        8, join_ttl, u32, 2592000
            => |s, d, len| { s.join_ttl = p_u32(d, len, 0, 2592000); };
        // Comma-separated replica ids, each a worker slot whose bootstrap
        // credential (ServiceAccount node-bootstrap-<id>) the join carries.
        9, join_nodes, str, 0
            => |s, d, len| {
                let mut v = 0u16;
                let mut any = false;
                s.nslots = 0;
                for i in 0..=len {
                    let c = if i < len { *d.add(i) } else { b',' };
                    if c.is_ascii_digit() {
                        v = v * 10 + u16::from(c - b'0');
                        any = true;
                        if v > 255 { return; }
                    } else if c == b',' {
                        if any {
                            if s.nslots as usize >= super::MAX_SLOTS { return; }
                            s.slots[s.nslots as usize] = v as u8;
                            s.nslots += 1;
                        }
                        v = 0;
                        any = false;
                    }
                }
            };
        12, id2_subject, str, 0
            => |s, d, len| { set(&mut s.extra[0].subject, &mut s.extra[0].subject_len, d, len); };
        13, id2_tenant, str, 0
            => |s, d, len| { set(&mut s.extra[0].tenant, &mut s.extra[0].tenant_len, d, len); };
        14, id2_device, str, 0
            => |s, d, len| { set(&mut s.extra[0].device, &mut s.extra[0].device_len, d, len); };
        15, id2_dns, str, 0
            => |s, d, len| { set(&mut s.extra[0].dns, &mut s.extra[0].dns_len, d, len); };
        16, id3_subject, str, 0
            => |s, d, len| { set(&mut s.extra[1].subject, &mut s.extra[1].subject_len, d, len); };
        17, id3_tenant, str, 0
            => |s, d, len| { set(&mut s.extra[1].tenant, &mut s.extra[1].tenant_len, d, len); };
        18, id3_device, str, 0
            => |s, d, len| { set(&mut s.extra[1].device, &mut s.extra[1].device_len, d, len); };
        19, id3_dns, str, 0
            => |s, d, len| { set(&mut s.extra[1].dns, &mut s.extra[1].dns_len, d, len); };
        10, remote, str, 0
            => |s, d, len| { set(&mut s.remote, &mut s.remote_len, d, len); };
        // A blob, from a file (`bearer: "${file:<path>}"`): a credential is
        // a secret kept beside the graph, never written into it. Chunks; the
        // first led by the total length (2 bytes LE).
        11, bearer, blob, 0
            => |s, d, len| {
                let (mut p, mut n) = (d, len);
                if s.bearer_total == 0 {
                    if n < 2 { return; }
                    s.bearer_total = u16::from_le_bytes([*p, *p.add(1)]);
                    p = p.add(2);
                    n -= 2;
                    if s.bearer_total as usize > super::TOKEN_MAX {
                        s.bearer_total = u16::MAX;
                    }
                }
                let at = s.bearer_len as usize;
                if s.bearer_total == u16::MAX || at + n > super::TOKEN_MAX { return; }
                core::ptr::copy_nonoverlapping(p, s.bearer.as_mut_ptr().add(at), n);
                s.bearer_len = (at + n) as u16;
            };
    }
}

fn or<'a>(v: &'a [u8], dflt: &'a [u8]) -> &'a [u8] {
    if v.is_empty() {
        dflt
    } else {
        v
    }
}

/// Append `src` to `out` at `at`; None when it does not fit.
fn put(out: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    out.get_mut(at..end)?.copy_from_slice(src);
    Some(end)
}

fn hex_into(src: &[u8], out: &mut [u8]) -> usize {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut n = 0;
    for &b in src {
        if n + 2 > out.len() {
            break;
        }
        out[n] = H[(b >> 4) as usize];
        out[n + 1] = H[(b & 15) as usize];
        n += 2;
    }
    n
}

fn dec_u32(v: u32, out: &mut [u8; 10]) -> usize {
    let mut tmp = [0u8; 10];
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
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// One DER element at `pos`: `(content start, content length, element end)`.
fn der_at(d: &[u8], pos: usize) -> Option<(usize, usize, usize)> {
    let l0 = *d.get(pos + 1)? as usize;
    let (hdr, len) = if l0 < 0x80 {
        (2, l0)
    } else {
        let n = l0 & 0x7F;
        if n == 0 || n > 2 {
            return None;
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | *d.get(pos + 2 + i)? as usize;
        }
        (2 + n, len)
    };
    let start = pos + hdr;
    let end = start.checked_add(len)?;
    (end <= d.len()).then_some((start, len, end))
}

/// The certificate's SubjectPublicKeyInfo, whole (tag and length included).
fn spki_of(cert: &[u8]) -> Option<&[u8]> {
    let (c, _, _) = der_at(cert, 0)?; // Certificate
    let (mut p, _, _) = der_at(cert, c)?; // TBSCertificate content
    if *cert.get(p)? == 0xA0 {
        p = der_at(cert, p)?.2; // [0] version
    }
    for _ in 0..5 {
        p = der_at(cert, p)?.2; // serial, signature, issuer, validity, subject
    }
    let (_, _, end) = der_at(cert, p)?;
    cert.get(p..end)
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
/// null when `params_len == 0`). Never called concurrently.
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
    s.id_req_in = in_chan;
    s.id_cert_out = out_chan;
    s.response_in = -1;
    s.request_out = -1;
    s.resolved = 0;
    s.call.reset();
    s.point_owed = 0;
    s.chain_len = 0;
    s.cur = 0;
    for e in s.extra.iter_mut() {
        e.subject_len = 0;
        e.tenant_len = 0;
        e.device_len = 0;
        e.dns_len = 0;
        e.req_in = -1;
        e.cert_out = -1;
        e.point_owed = 0;
        e.chain_len = 0;
    }
    s.ca_len = 0;
    s.phase = J_OFF;
    s.token_len = 0;
    s.tok_n = 0;
    s.nslots = 0;
    s.remote_len = 0;
    s.bearer_len = 0;
    s.bearer_total = 0;
    s.r_phase = R_IDLE;
    s.r_next_at = 0;
    params_def::set_defaults(s);
    params_def::parse_tlv(s, params, params_len);
    let sys = &*s.syscalls;
    if s.remote_len != 0 {
        if s.bearer_total == u16::MAX || s.bearer_len != s.bearer_total {
            let m = b"[node_pki] refusing to construct: the bearer credential is incomplete or past 2048 bytes";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -1;
        }
        // A file's trailing newline is not the credential's.
        while s.bearer_len > 0 && s.bearer[s.bearer_len as usize - 1].is_ascii_whitespace() {
            s.bearer_len -= 1;
        }
        if s.bearer_len == 0 {
            let m = b"[node_pki] refusing to construct: a remote identity needs the API's bearer credential";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -1;
        }
        if s.join != 0 {
            let m =
                b"[node_pki] refusing to construct: a remote identity's node does not mint joins";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -1;
        }
    } else if s.subject_len == 0 || s.tenant_len == 0 || s.device_len == 0 {
        let m =
            b"[node_pki] refusing to construct: subject, tenant and device name the certificate";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return -1;
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
        if s.resolved == 0 {
            s.response_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.request_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            for (i, e) in s.extra.iter_mut().enumerate() {
                if e.subject_len != 0 && s.remote_len == 0 {
                    e.req_in = dev_channel_port(sys, PORT_INPUT, 2 + i as u8);
                    e.cert_out = dev_channel_port(sys, PORT_OUTPUT, 2 + i as u8);
                }
            }
            s.resolved = 1;
        }
        if s.id_req_in < 0 || s.id_cert_out < 0 || s.response_in < 0 || s.request_out < 0 {
            return 0;
        }
        flush_chain(s, sys);
        take_requests(s, sys);
        for k in 0..EXTRA_IDS {
            extra_io(s, sys, k);
        }
        flush_call(s, sys);
        // Always drained: the authority's answers are fanned to every
        // requester, and one that reads only while it waits holds the
        // authority for all of them.
        drain_answers(s, sys);
        if s.remote_len != 0 {
            remote_step(s, sys);
        } else if s.call.idle() && s.point_owed != 0 && s.chain_len == 0 {
            ask_certificate(s, sys);
        } else if s.call.idle() {
            for k in 0..EXTRA_IDS {
                let e = &s.extra[k];
                if e.cert_out >= 0 && e.point_owed != 0 && e.chain_len == 0 {
                    ask_extra(s, sys, k);
                    break;
                }
            }
        }
        if s.join != 0 {
            join_step(s, sys);
        }
        0
    }
}

// ---- certify ----

/// Take tls's certificate requests; the latest point is the one asked for.
unsafe fn take_requests(s: &mut State, sys: &SyscallTable) {
    loop {
        let poll = (sys.channel_poll)(s.id_req_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let mut hdr = [0u8; 3];
        if (sys.channel_read)(s.id_req_in, hdr.as_mut_ptr(), 3) != 3 {
            return;
        }
        let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
        let mut body = [0u8; 256];
        let take = len.min(body.len());
        let mut got = 0usize;
        while got < take {
            let n = (sys.channel_read)(s.id_req_in, body.as_mut_ptr().add(got), take - got);
            if n <= 0 {
                break;
            }
            got += n as usize;
        }
        let mut rest = len - take;
        while rest > 0 {
            let mut sink = [0u8; 64];
            let n = (sys.channel_read)(s.id_req_in, sink.as_mut_ptr(), rest.min(64));
            if n <= 0 {
                break;
            }
            rest -= n as usize;
        }
        if hdr[0] == ID_MSG_REQ && got == POINT && len == POINT && body[0] == 0x04 {
            // tls repeats its ask until it holds a certificate: a remote
            // ask already under way for this point is that ask.
            if s.remote_len != 0 && s.r_phase != R_IDLE && s.point[..] == body[..POINT] {
                continue;
            }
            s.point.copy_from_slice(&body[..POINT]);
            s.point_owed = 1;
            s.r_phase = R_IDLE;
        }
    }
}

/// Ask the authority to certify the owed point.
unsafe fn ask_certificate(s: &mut State, sys: &SyscallTable) {
    let mut body = [0u8; 1024];
    let Some(at) = cert_body(
        &s.point,
        &s.subject[..s.subject_len as usize],
        &s.tenant[..s.tenant_len as usize],
        &s.device[..s.device_len as usize],
        &s.dns[..s.dns_len as usize],
        &mut body,
    ) else {
        return;
    };
    let port = s.request_out;
    let body = &body[..at];
    if s.call.begin(port, |id, out| {
        call_head(id, 3, b"/pki/certificate", b"", body, out)
    }) {
        s.point_owed = 0;
        s.cur = 0;
        flush_call(s, sys);
    }
}

/// Ask the authority to certify extra identity `k`'s owed point.
unsafe fn ask_extra(s: &mut State, sys: &SyscallTable, k: usize) {
    let mut body = [0u8; 1024];
    let at = {
        let e = &s.extra[k];
        cert_body(
            &e.point,
            &e.subject[..e.subject_len as usize],
            &e.tenant[..e.tenant_len as usize],
            &e.device[..e.device_len as usize],
            &e.dns[..e.dns_len as usize],
            &mut body,
        )
    };
    let Some(at) = at else {
        return;
    };
    let port = s.request_out;
    let body = &body[..at];
    if s.call.begin(port, |id, out| {
        call_head(id, 3, b"/pki/certificate", b"", body, out)
    }) {
        s.extra[k].point_owed = 0;
        s.cur = (k + 1) as u8;
        flush_call(s, sys);
    }
}

/// The certificate request for a point: `{"subject","tenant","device",
/// "public_key"[,"dns"]}` (`public_key` base64url; no `dns` asked for, the
/// member is left out rather than sent empty). Its length.
fn cert_body(
    point: &[u8; POINT],
    subject: &[u8],
    tenant: &[u8],
    device: &[u8],
    dns: &[u8],
    body: &mut [u8],
) -> Option<usize> {
    let mut key = [0u8; 96];
    let kl = b64::encode(point, &mut key)?;
    let mut at = put(body, 0, b"{\"subject\":\"")?;
    at = put(body, at, subject)?;
    at = put(body, at, b"\",\"tenant\":\"")?;
    at = put(body, at, tenant)?;
    at = put(body, at, b"\",\"device\":\"")?;
    at = put(body, at, device)?;
    at = put(body, at, b"\",\"public_key\":\"")?;
    at = put(body, at, key.get(..kl)?)?;
    if !dns.is_empty() {
        at = put(body, at, b"\",\"dns\":\"")?;
        at = put(body, at, dns)?;
    }
    put(body, at, b"\"}")
}

/// Extra identity `k`'s side of the step: its chain handed to its tls,
/// and its tls's latest request taken.
unsafe fn extra_io(s: &mut State, sys: &SyscallTable, k: usize) {
    let e = &mut s.extra[k];
    if e.req_in < 0 || e.cert_out < 0 {
        return;
    }
    let n = e.chain_len as usize;
    if n > 0 && (sys.channel_write)(e.cert_out, e.chain.as_ptr(), n) == n as i32 {
        e.chain_len = 0;
        let m = b"[node_pki] certificate issued to the API's identity";
        dev_log(sys, 2, m.as_ptr(), m.len());
    }
    loop {
        let poll = (sys.channel_poll)(e.req_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let mut frame = [0u8; 3 + POINT];
        let n = (sys.channel_read)(e.req_in, frame.as_mut_ptr(), frame.len());
        if n <= 0 {
            return;
        }
        if n as usize == frame.len()
            && frame[0] == ID_MSG_REQ
            && u16::from_le_bytes([frame[1], frame[2]]) as usize == POINT
            && frame[3] == 0x04
        {
            e.point.copy_from_slice(&frame[3..]);
            e.point_owed = 1;
        }
    }
}

unsafe fn flush_call(s: &mut State, sys: &SyscallTable) {
    let mut abort = [0u8; 32];
    if let Some(n) = s.call.abort_record(&mut abort) {
        (sys.channel_write)(s.request_out, abort.as_ptr(), n);
    }
    if let Some(req) = s.call.outgoing() {
        if (sys.channel_write)(s.request_out, req.as_ptr(), req.len()) > 0 {
            s.call.sent();
        }
    }
}

unsafe fn drain_answers(s: &mut State, sys: &SyscallTable) {
    loop {
        let poll = (sys.channel_poll)(s.response_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.response_in, s.in_buf.as_mut_ptr(), BUF);
        if n <= 0 {
            return;
        }
        let rec = core::slice::from_raw_parts(s.in_buf.as_ptr(), n as usize);
        match s.call.take(rec) {
            CallEvent::Answered => {
                let (status, body) = s.call.answer();
                let body = core::slice::from_raw_parts(body.as_ptr(), body.len());
                if s.remote_len != 0 {
                    remote_answered(s, sys, status, body);
                } else {
                    answered(s, sys, status, body);
                }
            }
            CallEvent::Failed(status) => {
                if s.remote_len != 0 {
                    remote_answered(s, sys, status, &[]);
                } else {
                    answered(s, sys, status, &[]);
                }
            }
            CallEvent::Nothing | CallEvent::LinkDown | CallEvent::LinkUp => {}
        }
    }
}

/// The authority answered: the chain goes to tls; a refusal waits for tls's
/// next request.
unsafe fn answered(s: &mut State, sys: &SyscallTable, status: u16, body: &[u8]) {
    let mut leaf = [0u8; CERT_MAX];
    let mut ca = [0u8; CERT_MAX];
    let decoded = if status == 200 {
        let l = j_get1(body, b"certificate").and_then(|t| b64::decode(t, &mut leaf));
        let c = j_get1(body, b"ca").and_then(|t| b64::decode(t, &mut ca));
        l.zip(c)
    } else {
        None
    };
    let Some((ll, cl)) = decoded.filter(|(ll, cl)| ll + cl <= CHAIN_MAX) else {
        s.cert_refused = s.cert_refused.wrapping_add(1);
        if s.cert_refused & 15 == 1 {
            log_status(
                sys,
                b"[node_pki] the authority refused a certificate: ",
                status,
            );
        }
        return;
    };
    if s.cur != 0 {
        // An extra identity's chain, handed to its tls by `extra_io`.
        let e = &mut s.extra[s.cur as usize - 1];
        let total = ll + cl;
        e.chain[0] = ID_MSG_CERT;
        e.chain[1..3].copy_from_slice(&(total as u16).to_le_bytes());
        e.chain[3..3 + ll].copy_from_slice(&leaf[..ll]);
        e.chain[3 + ll..3 + total].copy_from_slice(&ca[..cl]);
        e.chain_len = (3 + total) as u16;
        s.certified = s.certified.wrapping_add(1);
        return;
    }
    hold_chain(s, sys, &leaf[..ll], &ca[..cl]);
    if s.join != 0 && s.phase == J_OFF {
        s.phase = J_CLEAN;
    }
}

/// `text` and an HTTP status, as one log line.
unsafe fn log_status(sys: &SyscallTable, text: &[u8], status: u16) {
    let mut m = [0u8; 96];
    let mut w = put(&mut m, 0, text).unwrap_or(0);
    let mut d = [0u8; 10];
    let dn = dec_u32(u32::from(status), &mut d);
    w = put(&mut m, w, &d[..dn]).unwrap_or(w);
    dev_log(sys, 1, m.as_ptr(), w);
}

/// The chain tls is handed, leaf then authority; held until it is taken.
unsafe fn hold_chain(s: &mut State, sys: &SyscallTable, leaf: &[u8], ca: &[u8]) {
    let (ll, cl) = (leaf.len(), ca.len());
    let total = ll + cl;
    s.chain[0] = ID_MSG_CERT;
    s.chain[1..3].copy_from_slice(&(total as u16).to_le_bytes());
    s.chain[3..3 + ll].copy_from_slice(leaf);
    s.chain[3 + ll..3 + total].copy_from_slice(ca);
    s.chain_len = (3 + total) as u16;
    s.ca[..cl].copy_from_slice(ca);
    s.ca_len = cl as u16;
    s.certified = s.certified.wrapping_add(1);
    flush_chain(s, sys);
}

// ---- remote: certified through the API ----
//
// A worker holds no authority. Its slot's Device (`remote`) is pre-declared
// by the control plane, and the slot's credential (`bearer`) may patch it and
// read it back. Each ask is answered with a certificate issued for it:
//   1. PATCH <remote>         {"spec":{"publicKey":"<point>"}}
//   2. PATCH <remote>/status  {"status":{"publicKey":null}} — the key is
//      no longer the one certified, so the device chain certifies it (again)
//   3. GET <remote>, once a second, until status.publicKey is this point
//      and status.certificate is not the one step 1 answered.
// A certificate issued now is what tls renews from: it renews at two
// thirds of the lifetime it is handed, measured from when it takes it, so a
// stale certificate left in the Device must not be handed.

/// FNV-1a, 32-bit: telling one certificate from another.
fn fnv(b: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &c in b {
        h ^= u32::from(c);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Ask the API the current step's call, when one is due.
unsafe fn remote_step(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys);
    if !s.call.idle() || now < s.r_next_at {
        return;
    }
    if s.r_phase == R_IDLE {
        if s.point_owed == 0 || s.chain_len != 0 {
            return;
        }
        let Some(kl) = b64::encode(&s.point, &mut s.r_key) else {
            s.point_owed = 0;
            return;
        };
        s.r_key_len = kl as u8;
        s.point_owed = 0;
        s.r_phase = R_SPEC;
    }
    let mut hdr = [0u8; TOKEN_MAX + 96];
    let mut h = 0usize;
    let hparts: [&[u8]; 3] = [
        b"authorization: Bearer ",
        &s.bearer[..s.bearer_len as usize],
        b"\r\n",
    ];
    for p in hparts {
        h = put(&mut hdr, h, p).unwrap_or(h);
    }
    let mut path = [0u8; PARAM_MAX + 8];
    let mut pl = put(&mut path, 0, &s.remote[..s.remote_len as usize]).unwrap_or(0);
    let mut body = [0u8; 160];
    let mut bl = 0usize;
    let method = match s.r_phase {
        R_SPEC => {
            let parts: [&[u8]; 3] = [
                b"{\"spec\":{\"publicKey\":\"",
                &s.r_key[..s.r_key_len as usize],
                b"\"}}",
            ];
            for p in parts {
                bl = put(&mut body, bl, p).unwrap_or(bl);
            }
            h = put(
                &mut hdr,
                h,
                b"content-type: application/merge-patch+json\r\n",
            )
            .unwrap_or(h);
            6
        }
        R_CLEAR => {
            pl = put(&mut path, pl, b"/status").unwrap_or(pl);
            bl = put(&mut body, 0, b"{\"status\":{\"publicKey\":null}}").unwrap_or(0);
            h = put(
                &mut hdr,
                h,
                b"content-type: application/merge-patch+json\r\n",
            )
            .unwrap_or(h);
            6
        }
        _ => 1,
    };
    let port = s.request_out;
    let (hdr, path, body) = (&hdr[..h], &path[..pl], &body[..bl]);
    if s.call
        .begin(port, |id, out| call_head(id, method, path, hdr, body, out))
    {
        flush_call(s, sys);
    }
}

/// The API answered the current step.
unsafe fn remote_answered(s: &mut State, sys: &SyscallTable, status: u16, body: &[u8]) {
    let now = dev_millis(sys);
    let ok = (200..300).contains(&status);
    match s.r_phase {
        R_SPEC | R_CLEAR if !ok => {
            s.r_refused = s.r_refused.wrapping_add(1);
            if s.r_refused & 15 == 1 {
                log_status(
                    sys,
                    b"[node_pki] the API refused the Device's key: ",
                    status,
                );
            }
            s.r_next_at = now + RETRY_MS;
        }
        R_SPEC => {
            s.r_old = fnv(j_get2(body, b"status", b"certificate").unwrap_or(&[]));
            s.r_phase = R_CLEAR;
            s.r_next_at = now;
        }
        R_CLEAR => {
            s.r_phase = R_POLL;
            s.r_next_at = now + POLL_MS;
        }
        R_POLL => {
            s.r_next_at = now + REMOTE_POLL_MS;
            if status != 200 {
                return;
            }
            let key = j_get2(body, b"status", b"publicKey").unwrap_or(&[]);
            let crt = j_get2(body, b"status", b"certificate").unwrap_or(&[]);
            if key != &s.r_key[..s.r_key_len as usize] || crt.is_empty() || fnv(crt) == s.r_old {
                return;
            }
            let mut leaf = [0u8; CERT_MAX];
            let mut ca = [0u8; CERT_MAX];
            let l = b64::decode(crt, &mut leaf);
            let c = j_get2(body, b"status", b"ca").and_then(|t| b64::decode(t, &mut ca));
            let Some((ll, cl)) = l.zip(c).filter(|(ll, cl)| ll + cl <= CHAIN_MAX) else {
                return;
            };
            s.r_phase = R_IDLE;
            hold_chain(s, sys, &leaf[..ll], &ca[..cl]);
        }
        _ => {}
    }
}

/// Hand the held chain to tls; held until its channel takes it whole.
unsafe fn flush_chain(s: &mut State, sys: &SyscallTable) {
    let n = s.chain_len as usize;
    if n == 0 {
        return;
    }
    if (sys.channel_write)(s.id_cert_out, s.chain.as_ptr(), n) == n as i32 {
        s.chain_len = 0;
        let m = b"[node_pki] certificate issued to the API's identity";
        dev_log(sys, 2, m.as_ptr(), m.len());
    }
}

// ---- join ----

unsafe fn join_step(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys);
    // Until the join is published, where it stands, every 5 s: a join
    // that never announces is otherwise silent (on a board, its only
    // account is this line).
    if s.phase != J_DONE && s.phase != J_SPENT && now.saturating_sub(s.hb_at) >= 5000 {
        s.hb_at = now;
        let mut m = [0u8; 64];
        let mut w = put(&mut m, 0, b"[node_pki] join hb ph=").unwrap_or(0);
        let mut d = [0u8; 10];
        let mut field = |m: &mut [u8; 64], w: usize, tag: &[u8], v: u32| -> usize {
            let w = put(m, w, tag).unwrap_or(w);
            let n = dec_u32(v, &mut d);
            put(m, w, &d[..n]).unwrap_or(w)
        };
        w = field(&mut m, w, b"", u32::from(s.phase));
        w = field(&mut m, w, b" tok=", u32::from(s.tok_n));
        w = field(&mut m, w, b" decl=", u32::from(s.decl_i));
        w = field(&mut m, w, b" ca=", u32::from(s.ca_len));
        dev_log(sys, 2, m.as_ptr(), w);
    }
    if now < s.next_at {
        return;
    }
    let st = Store::plain(sys, &s.nag);
    match s.phase {
        J_CLEAN => {
            // A stale secret of an earlier boot: the first listed name that
            // still exists is deleted, once, and waited for; with none
            // existing the pass is done. A listed name that is not a key
            // costs nothing, so a listing can never drive a write loop.
            let mut walk = ListWalk::new(JOIN_PREFIX);
            let mut key = [0u8; MAX_KEY];
            let mut found = 0usize;
            let mut seen = 0u32;
            while let Some(name) = walk.next(&st) {
                seen += 1;
                let kl = if name.starts_with(JOIN_PREFIX) {
                    put(&mut key, 0, name).unwrap_or(0)
                } else {
                    put(&mut key, 0, JOIN_PREFIX)
                        .and_then(|p| put(&mut key, p, name))
                        .unwrap_or(0)
                };
                if kl > 0 && exists(&st, &key[..kl]) {
                    found = kl;
                    break;
                }
                if seen >= 64 {
                    break;
                }
            }
            if st.waiting() {
                // A page or a probe was not answered yet: asked next step.
                return;
            }
            if walk.failed {
                s.next_at = now + RETRY_MS;
                return;
            }
            if found == 0 {
                s.phase = J_DECLARE;
                s.decl_i = 0;
                s.wrote_at = 0;
                return;
            }
            ask_delete(s, &st, &key[..found], now);
        }
        J_DECLARE => {
            // What the credentials mean, each object written only if absent
            // (an operator's edit is never undone), then minted.
            let mut key = [0u8; MAX_KEY];
            let mut val = [0u8; DECL_MAX];
            let Some((kl, vl)) = declared(s, s.decl_i, &mut key, &mut val) else {
                s.phase = J_ASK;
                s.wrote_at = 0;
                return;
            };
            let present = exists(&st, &key[..kl]);
            if st.waiting() {
                return; // not answered yet (`EAGAIN`): asked next step
            }
            if present {
                s.decl_i += 1;
                s.wrote_at = 0;
            } else if s.wrote_at == 0 || now - s.wrote_at >= REASK_MS {
                s.wrote_at = now;
                let _ = put_value(&st, &key[..kl], &val[..vl]);
            }
        }
        J_ASK => {
            let mut rnd = [0u8; 24];
            if dev_csprng_fill(sys, rnd.as_mut_ptr(), rnd.len()) < 0 {
                s.next_at = now + RETRY_MS;
                return;
            }
            // A fresh request id for every credential; one secret for the
            // join, drawn with the first.
            hex_into(&rnd[..8], &mut s.join_id);
            if s.tok_n == 0 {
                hex_into(&rnd[8..], &mut s.secret);
            }
            for b in rnd.iter_mut() {
                core::ptr::write_volatile(b, 0);
            }
            s.phase = J_WAIT;
            s.asked_at = now;
            s.wrote_at = 0;
            ask_token(s, sys);
        }
        J_WAIT => {
            let mut key = [0u8; 48];
            let kl = token_key(s, b"/token-resp/join-", &mut key);
            let mut val = [0u8; TOKEN_MAX + 16];
            let got = get_value(&st, &key[..kl], &mut val);
            if st.waiting() {
                return; // not answered yet (`EAGAIN`), not absent
            }
            match got {
                Some(n) if val[..n].starts_with(b"token=") && n - 6 <= TOKEN_MAX => {
                    let t = s.tok_n as usize;
                    if t == 0 {
                        s.token[..n - 6].copy_from_slice(&val[6..n]);
                        s.token_len = (n - 6) as u16;
                    } else {
                        s.node_tokens[t - 1][..n - 6].copy_from_slice(&val[6..n]);
                        s.node_token_len[t - 1] = (n - 6) as u16;
                    }
                    s.tok_ok = 1;
                    s.phase = J_DROP_REQ;
                }
                Some(_) => {
                    let m = b"[node_pki] the ServiceAccount mint refused a join credential; asking again";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                    s.phase = J_DROP_REQ;
                    s.tok_ok = 0;
                }
                None => {
                    if now - s.asked_at > TOKEN_WAIT_MS {
                        s.phase = J_DROP_REQ;
                        s.tok_ok = 0;
                    } else {
                        // The request may still be owed to the store.
                        ask_token(s, sys);
                        s.next_at = now + POLL_MS;
                    }
                }
            }
        }
        J_DROP_REQ => {
            // The request first: with it gone, removing the answer cannot
            // make the mint answer it again.
            let mut key = [0u8; 48];
            let kl = token_key(s, b"/token-req/join-", &mut key);
            let present = exists(&st, &key[..kl]);
            if st.waiting() {
                return;
            }
            if !present {
                s.phase = J_DROP_RESP;
                s.wrote_at = 0;
            } else {
                ask_delete(s, &st, &key[..kl], now);
            }
        }
        J_DROP_RESP => {
            let mut key = [0u8; 48];
            let kl = token_key(s, b"/token-resp/join-", &mut key);
            let present = exists(&st, &key[..kl]);
            if st.waiting() {
                return;
            }
            if !present {
                s.wrote_at = 0;
                if s.tok_ok == 0 {
                    s.phase = J_ASK;
                    s.next_at = now + RETRY_MS;
                } else if s.tok_n < s.nslots {
                    // The next slot's credential.
                    s.tok_n += 1;
                    s.tok_ok = 0;
                    s.phase = J_ASK;
                } else {
                    s.phase = J_PUBLISH;
                }
            } else {
                ask_delete(s, &st, &key[..kl], now);
            }
        }
        J_PUBLISH => {
            let mut key = [0u8; 48];
            let mut kl = put(&mut key, 0, JOIN_PREFIX).unwrap_or(0);
            kl = put(&mut key, kl, &s.secret).unwrap_or(kl);
            let at = join_record(s);
            // Published once it reads back; asked at most every REASK_MS.
            let present = exists(&st, &key[..kl]);
            if st.waiting() {
                return;
            }
            let answer = if present {
                WriteAnswer::Decided(0)
            } else if s.wrote_at == 0 || now - s.wrote_at >= REASK_MS {
                s.wrote_at = now;
                let val = core::slice::from_raw_parts(s.pub_buf.as_ptr(), at);
                put_value(&st, &key[..kl], val)
            } else {
                WriteAnswer::Pending
            };
            match answer {
                WriteAnswer::Pending => {}
                WriteAnswer::Decided(0) => {
                    for b in s.token.iter_mut() {
                        core::ptr::write_volatile(b, 0);
                    }
                    for t in s.node_tokens.iter_mut() {
                        for b in t.iter_mut() {
                            core::ptr::write_volatile(b, 0);
                        }
                    }
                    for b in s.pub_buf.iter_mut() {
                        core::ptr::write_volatile(b, 0);
                    }
                    s.node_token_len = [0; MAX_SLOTS];
                    s.token_len = 0;
                    s.joins_minted = s.joins_minted.wrapping_add(1);
                    s.phase = J_DONE;
                    announce(s, sys);
                    s.next_at = now + ANNOUNCE_EVERY_MS;
                }
                WriteAnswer::Decided(_) => s.next_at = now + RETRY_MS,
            }
        }
        J_DONE => {
            // Said again while the record is there to take; once taken,
            // the secret is spent and the join is over.
            let mut key = [0u8; 48];
            let mut kl = put(&mut key, 0, JOIN_PREFIX).unwrap_or(0);
            kl = put(&mut key, kl, &s.secret).unwrap_or(kl);
            let present = exists(&st, &key[..kl]);
            if st.waiting() {
                // Not answered yet: the secret is not spent on a read that
                // has not come back.
                return;
            }
            if present {
                announce(s, sys);
                s.next_at = now + ANNOUNCE_EVERY_MS;
            } else {
                s.phase = J_SPENT;
            }
        }
        _ => {}
    }
}

/// The largest bootstrap object `declared` lays out.
const DECL_MAX: usize = 768;

/// Bootstrap object `i` the join declares: `(key length, value length)`, or
/// None past the last. What its credentials mean —
///   0  /namespaces/default, 1 /namespaces/kube-system — where the
///      credentials and an operator's first objects live;
///   2  /clusterroles/cluster-admin — every verb on everything;
///   3  /clusterrolebindings/nanocloud-admin — to the join's ServiceAccount;
/// and, with worker slots, nanocloud-peers and per slot <n>:
///   4            /namespaces/nanocloud-peers
///   5 + 4k + 0,1 /devices.nanocloud.io/nanocloud-peers/replica-<n>.server,
///                .client — declared without a key;
///   5 + 4k + 2   /roles/nanocloud-peers/node-bootstrap-<n> — get and patch
///                those two Devices (and their status), nothing else;
///   5 + 4k + 3   /rolebindings/… — to kube-system/node-bootstrap-<n>.
fn declared(s: &State, i: u8, key: &mut [u8], val: &mut [u8]) -> Option<(usize, usize)> {
    // Laid out piece by piece: an array of byte-string literals can be
    // emitted as one constant whose inner pointers a PIC module never
    // relocates (see the workspace's PIC notes).
    const RBAC: &[u8] = b"rbac.authorization.k8s.io/v1";
    let ns = or(&s.join_ns[..s.join_ns_len as usize], b"kube-system");
    let sa = or(&s.join_sa[..s.join_sa_len as usize], b"admin");
    let i = i as usize;
    let slots = s.nslots as usize;
    let last = if slots == 0 { 4 } else { 5 + 4 * slots };
    if i >= last {
        return None;
    }
    let mut d = [0u8; 10];
    let (slot, role) = if i >= 5 {
        ((i - 5) / 4, (i - 5) % 4)
    } else {
        (0, 0)
    };
    let dn = dec_u32(u32::from(*s.slots.get(slot)?), &mut d);
    let n = d.get(..dn)?;
    let (mut kl, mut vl) = (0usize, 0usize);
    match (i, role) {
        (0 | 1, _) => {
            let ns: &[u8] = if i == 0 { b"default" } else { b"kube-system" };
            kl = put(key, kl, b"/namespaces/")?;
            kl = put(key, kl, ns)?;
            vl = put(
                val,
                vl,
                b"{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{\"name\":\"",
            )?;
            vl = put(val, vl, ns)?;
            vl = put(val, vl, b"\"}}")?;
        }
        (2, _) => {
            kl = put(key, kl, b"/clusterroles/cluster-admin")?;
            vl = put(val, vl, b"{\"apiVersion\":\"")?;
            vl = put(val, vl, RBAC)?;
            vl = put(val, vl, b"\",\"kind\":\"ClusterRole\",\"metadata\":{\"name\":\"cluster-admin\"},\"rules\":[{\"apiGroups\":[\"*\"],\"resources\":[\"*\"],\"verbs\":[\"*\"]},{\"nonResourceURLs\":[\"*\"],\"verbs\":[\"*\"]}]}")?;
        }
        (3, _) => {
            kl = put(key, kl, b"/clusterrolebindings/nanocloud-admin")?;
            vl = put(val, vl, b"{\"apiVersion\":\"")?;
            vl = put(val, vl, RBAC)?;
            vl = put(val, vl, b"\",\"kind\":\"ClusterRoleBinding\",\"metadata\":{\"name\":\"nanocloud-admin\"},\"subjects\":[{\"kind\":\"ServiceAccount\",\"name\":\"")?;
            vl = put(val, vl, sa)?;
            vl = put(val, vl, b"\",\"namespace\":\"")?;
            vl = put(val, vl, ns)?;
            vl = put(val, vl, b"\"}],\"roleRef\":{\"apiGroup\":\"rbac.authorization.k8s.io\",\"kind\":\"ClusterRole\",\"name\":\"cluster-admin\"}}")?;
        }
        (4, _) => {
            kl = put(key, kl, b"/namespaces/nanocloud-peers")?;
            vl = put(val, vl, b"{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{\"name\":\"nanocloud-peers\"}}")?;
        }
        (_, 0 | 1) => {
            let end: &[u8] = if role == 0 { b".server" } else { b".client" };
            kl = put(key, kl, b"/devices.nanocloud.io/nanocloud-peers/replica-")?;
            kl = put(key, kl, n)?;
            kl = put(key, kl, end)?;
            vl = put(val, vl, b"{\"apiVersion\":\"nanocloud.io/v1\",\"kind\":\"Device\",\"metadata\":{\"name\":\"replica-")?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, end)?;
            vl = put(
                val,
                vl,
                b"\",\"namespace\":\"nanocloud-peers\"},\"spec\":{}}",
            )?;
        }
        (_, 2) => {
            kl = put(key, kl, b"/roles/nanocloud-peers/node-bootstrap-")?;
            kl = put(key, kl, n)?;
            vl = put(val, vl, b"{\"apiVersion\":\"")?;
            vl = put(val, vl, RBAC)?;
            vl = put(
                val,
                vl,
                b"\",\"kind\":\"Role\",\"metadata\":{\"name\":\"node-bootstrap-",
            )?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, b"\",\"namespace\":\"nanocloud-peers\"},\"rules\":[{\"apiGroups\":[\"nanocloud.io\"],\"resources\":[\"devices\",\"devices/status\"],\"resourceNames\":[\"replica-")?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, b".server\",\"replica-")?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, b".client\"],\"verbs\":[\"get\",\"patch\"]}]}")?;
        }
        _ => {
            kl = put(key, kl, b"/rolebindings/nanocloud-peers/node-bootstrap-")?;
            kl = put(key, kl, n)?;
            vl = put(val, vl, b"{\"apiVersion\":\"")?;
            vl = put(val, vl, RBAC)?;
            vl = put(
                val,
                vl,
                b"\",\"kind\":\"RoleBinding\",\"metadata\":{\"name\":\"node-bootstrap-",
            )?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, b"\",\"namespace\":\"nanocloud-peers\"},\"subjects\":[{\"kind\":\"ServiceAccount\",\"name\":\"node-bootstrap-")?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, b"\",\"namespace\":\"")?;
            vl = put(val, vl, ns)?;
            vl = put(val, vl, b"\"}],\"roleRef\":{\"apiGroup\":\"rbac.authorization.k8s.io\",\"kind\":\"Role\",\"name\":\"node-bootstrap-")?;
            vl = put(val, vl, n)?;
            vl = put(val, vl, b"\"}}")?;
        }
    }
    Some((kl, vl))
}

/// The join record into `pub_buf`: {"token","ca","nodes":{"<id>":"<jwt>"}}
/// — the operator's credential, the authority, and each worker slot's
/// bootstrap credential. Its length.
fn join_record(s: &mut State) -> usize {
    let mut ca = [0u8; CERT_MAX * 2];
    let cl = b64::encode(&s.ca[..s.ca_len as usize], &mut ca).unwrap_or(0);
    let out = &mut s.pub_buf;
    let mut at = 0usize;
    let parts: [&[u8]; 4] = [
        b"{\"token\":\"",
        &s.token[..s.token_len as usize],
        b"\",\"ca\":\"",
        &ca[..cl],
    ];
    for p in parts {
        at = put(out, at, p).unwrap_or(at);
    }
    at = put(out, at, b"\",\"nodes\":{").unwrap_or(at);
    for i in 0..s.nslots as usize {
        let mut d = [0u8; 10];
        let dn = dec_u32(u32::from(s.slots[i]), &mut d);
        let sep: &[u8] = if i == 0 { b"\"" } else { b",\"" };
        let parts: [&[u8]; 4] = [
            sep,
            &d[..dn],
            b"\":\"",
            &s.node_tokens[i][..s.node_token_len[i] as usize],
        ];
        for p in parts {
            at = put(out, at, p).unwrap_or(at);
        }
        at = put(out, at, b"\"").unwrap_or(at);
    }
    put(out, at, b"}}").unwrap_or(at)
}

/// `<prefix><join id>` into `out`; the length.
fn token_key(s: &State, prefix: &[u8], out: &mut [u8; 48]) -> usize {
    let kl = put(out, 0, prefix).unwrap_or(0);
    put(out, kl, &s.join_id).unwrap_or(kl)
}

/// Ask the ServiceAccount mint for the admin token (again, while owed).
unsafe fn ask_token(s: &mut State, sys: &SyscallTable) {
    let mut key = [0u8; 48];
    let kl = token_key(s, b"/token-req/join-", &mut key);
    let mut ttl = [0u8; 10];
    let tl = dec_u32(s.join_ttl, &mut ttl);
    let mut val = [0u8; 3 * PARAM_MAX + 64];
    let mut at = 0usize;
    // The admin's credential first, then each slot's node-bootstrap-<id>.
    let mut sa = [0u8; PARAM_MAX];
    let sl = if s.tok_n == 0 {
        put(
            &mut sa,
            0,
            or(&s.join_sa[..s.join_sa_len as usize], b"admin"),
        )
        .unwrap_or(0)
    } else {
        let mut d = [0u8; 10];
        let dn = dec_u32(u32::from(s.slots[s.tok_n as usize - 1]), &mut d);
        let p = put(&mut sa, 0, b"node-bootstrap-").unwrap_or(0);
        put(&mut sa, p, &d[..dn]).unwrap_or(p)
    };
    let parts: [&[u8]; 6] = [
        b"ns=",
        or(&s.join_ns[..s.join_ns_len as usize], b"kube-system"),
        b";sa=",
        &sa[..sl],
        b";aud=nanocloud;ttl=",
        &ttl[..tl],
    ];
    for p in parts {
        at = put(&mut val, at, p).unwrap_or(at);
    }
    let st = Store::plain(sys, &s.nag);
    let now = dev_millis(sys);
    let present = exists(&st, &key[..kl]);
    // A probe not answered yet (`EAGAIN`) asks nothing: asked again later.
    if !present && !st.waiting() && (s.wrote_at == 0 || now - s.wrote_at >= REASK_MS) {
        s.wrote_at = now;
        let _ = put_value(&st, &key[..kl], &val[..at]);
    }
}

/// Delete `key`, asked at most every REASK_MS while it still exists.
unsafe fn ask_delete(s: &mut State, st: &Store, key: &[u8], now: u64) {
    if s.wrote_at == 0 || now - s.wrote_at >= REASK_MS {
        s.wrote_at = now;
        let _ = delete_value(st, key);
    }
}

/// Say the secret, with the digest of the authority's key an operator checks
/// the API's presented authority against.
unsafe fn announce(s: &mut State, sys: &SyscallTable) {
    let spki = spki_of(&s.ca[..s.ca_len as usize]).unwrap_or(&[]);
    let digest = sha256(spki);
    let mut m = [0u8; 160];
    let mut w = put(&mut m, 0, b"[join] secret=").unwrap_or(0);
    w = put(&mut m, w, &s.secret).unwrap_or(w);
    w = put(&mut m, w, b" ca-sha256=").unwrap_or(w);
    w += hex_into(&digest, &mut m[w..]);
    dev_log(sys, 2, m.as_ptr(), w);
}
