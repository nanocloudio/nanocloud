//! A Device's claim token, and the `nanocloud.io/device` signer that redeems
//! it for a certificate.
//!
//! See `manifest.toml` for the wire layouts and the rationale.

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
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");
include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/exchange_call.rs");
include!("../_shared/pkcs10.rs");

#[path = "../../../target/fluxor/kagi-common/b64.rs"]
mod b64;

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 160;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const BUF: usize = 8192;
/// A Device or a staged body, read whole.
const DOC_MAX: usize = 8192;
/// The CSR a redemption answers, and the c_answer it rides in. A Chronicle
/// record is 4 KiB, so the CSR's own `spec.request` is not repeated in it.
const ANSWER_MAX: usize = 2560;
const NAME_MAX: usize = 64;
const PARAM_MAX: usize = 128;
const POINT: usize = 65;
const CERT_MAX: usize = 2048;

/// The admit record's fields this module reads and writes
/// (apiplane_admit.plan `VerifyCall`).
const F_KEY: u8 = 32;
const F_RESOURCE: u8 = 33;
const F_NAME: u8 = 34;
const F_BODY: u8 = 35;
const F_VERDICT: u8 = 37;
const F_ANSWER: u8 = 38;
const F_METHOD: u8 = 31;
/// The verdict admit leaves on a request this module answers: what a graph
/// without it answers.
const V_UNSERVED: i64 = 501;
const M_GET: i64 = 1;
const M_POST: i64 = 3;

const SIGNER: &[u8] = b"nanocloud.io/device";
const DEVICES: &[u8] = b"/devices.nanocloud.io/";
const CLAIM_OF: &[u8] = b"/device-claim-of/";
const CLAIMS: &[u8] = b"/device-claims/";
const CSRS: &[u8] = b"/device-csrs/";
const BLOBS: &[u8] = b"/blobs/sha256/";
/// `/device-keys/<fp>`: the Device a key was enrolled for, which is how a
/// key that holds no valid certificate any more finds its Device.
const KEYS: &[u8] = b"/device-keys/";
/// A Device's certificate lifetime when it names none: a day.
const DEFAULT_LIFETIME_S: u64 = 86400;

const SWEEP_MS: u64 = 1000;
/// Devices one sweep reads before it stops: a sweep must not hold the step.
const SWEEP_MAX: u32 = 64;

/// Redemption phases.
const P_IDLE: u8 = 0;
/// Reading, checking and binding: run again each step until it answers or
/// asks the authority.
const P_PREP: u8 = 1;
/// The authority's answer is awaited.
const P_SIGN: u8 = 2;
/// The CSR, the Device's status and the spent token are written, in order.
const P_FINISH: u8 = 3;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    record_in: i32,
    response_in: i32,
    result_out: i32,
    request_out: i32,
    resolved: u32,
    nag: ReadNag,
    call: Call,

    trust_domain: [u8; PARAM_MAX],
    trust_domain_len: u8,
    broker: [u8; PARAM_MAX],
    broker_len: u8,
    claim_ttl_s: u32,
    /// The cell's bounds on a Device's `spec.certificate.lifetime`, seconds.
    min_lifetime_s: u32,
    max_lifetime_s: u32,

    /// The request being served, and the record that answers it.
    held: [u8; BUF],
    held_len: u16,
    out: [u8; BUF],
    out_len: u16,
    phase: u8,
    /// FINISH's next write: 0 the CSR, 1 the Device, 2 the token, 3 answer.
    fin: u8,
    method: i64,
    /// A re-enrollment by the Device's enrolled key, not a token redemption.
    renew: u8,
    /// The lifetime asked of the authority, and the one its certificate has.
    lifetime_s: u32,
    issued_s: u32,

    /// The redemption's facts, from the offer and the request.
    id: [u8; 6],
    ns: [u8; NAME_MAX],
    ns_len: u8,
    name: [u8; NAME_MAX],
    name_len: u8,
    uid: [u8; NAME_MAX],
    uid_len: u8,
    prefix: [u8; PARAM_MAX],
    prefix_len: u8,
    point: [u8; POINT],
    fp: [u8; 64],
    /// The certificate issued, PEM then base64, and its `notAfter`.
    cert_b64: [u8; CERT_MAX],
    cert_b64_len: u16,
    not_after: [u8; 20],
    spiffe: [u8; PARAM_MAX],
    spiffe_len: u8,
    /// The request's `metadata.name`.
    req_name: [u8; NAME_MAX],
    req_name_len: u8,
    /// The CSR record built once and written as built.
    csr: [u8; ANSWER_MAX],
    csr_len: u16,

    /// A token drawn for a Device and not yet written: kept until its write
    /// is decided, so a pending write is asked again with the same bytes.
    mint_key: [u8; MAX_KEY],
    mint_key_len: u8,
    mint_token: [u8; 23],
    mint_uid: [u8; NAME_MAX],
    mint_uid_len: u8,
    sweep_at: u64,

    /// The write the store has taken and not yet decided (`EINPROGRESS`):
    /// asked again from these same buffers, byte for byte, until it is, and
    /// no other write is asked before (storage.object's pending rule).
    owed: u8,
    owed_put: u8,
    owed_pre: u8,
    owed_rev: u64,
    owed_key: [u8; MAX_KEY],
    owed_key_len: u8,
    owed_val: [u8; DOC_MAX],
    owed_len: u16,
    owed_fence: [u8; 62],

    minted: u32,
    redeemed: u32,
    refused: u32,

    doc: [u8; DOC_MAX],
    doc2: [u8; DOC_MAX],
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

        1, trust_domain, str, 0
            => |s, d, len| { set(&mut s.trust_domain, &mut s.trust_domain_len, d, len); };
        2, broker, str, 0
            => |s, d, len| { set(&mut s.broker, &mut s.broker_len, d, len); };
        3, claim_ttl_s, u32, 86400
            => |s, d, len| { s.claim_ttl_s = p_u32(d, len, 0, 86400); };
        4, min_lifetime_s, u32, 3600
            => |s, d, len| { s.min_lifetime_s = p_u32(d, len, 0, 3600); };
        5, max_lifetime_s, u32, 7776000
            => |s, d, len| { s.max_lifetime_s = p_u32(d, len, 0, 7776000); };
    }
}

// ── small byte helpers ─────────────────────────────────────────────────────

fn put(out: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    out.get_mut(at..end)?.copy_from_slice(src);
    Some(end)
}

/// Copy what fits of `src` into `dst`. Never a panic path: a module has no
/// unwinder, and every length here was checked where it was read.
fn cp(dst: &mut [u8], src: &[u8]) {
    let n = if dst.len() < src.len() {
        dst.len()
    } else {
        src.len()
    };
    unsafe { core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), n) };
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

fn dec_u64(mut v: u64, out: &mut [u8; 20]) -> usize {
    let mut t = [0u8; 20];
    let mut n = 0;
    loop {
        t[n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    for i in 0..n {
        out[i] = t[n - 1 - i];
    }
    n
}

fn parse_u64(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 19 {
        return None;
    }
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    Some(v)
}

/// Equal, in time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut d = 0u8;
    for i in 0..a.len() {
        d |= a[i] ^ b[i];
    }
    d == 0
}

/// A member's scalar (a string's bytes, unquoted), or empty. One per depth:
/// a path built of literals would be a table of pointers, which a
/// position-independent module cannot hold.
fn js1<'a>(doc: &'a [u8], a: &[u8]) -> &'a [u8] {
    j_get1(doc, a).unwrap_or(b"")
}
fn js2<'a>(doc: &'a [u8], a: &[u8], b: &[u8]) -> &'a [u8] {
    j_get2(doc, a, b).unwrap_or(b"")
}
fn js3<'a>(doc: &'a [u8], a: &[u8], b: &[u8], c: &[u8]) -> &'a [u8] {
    j_get3(doc, a, b, c).unwrap_or(b"")
}

/// Bytes laid end to end into a buffer; `None` once one does not fit.
struct W<'a> {
    buf: &'a mut [u8],
    at: usize,
    ok: bool,
}

impl<'a> W<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        W {
            buf,
            at: 0,
            ok: true,
        }
    }
    fn s(&mut self, b: &[u8]) -> &mut Self {
        if self.ok {
            match put(self.buf, self.at, b) {
                Some(n) => self.at = n,
                None => self.ok = false,
            }
        }
        self
    }
    fn end(&self) -> Option<usize> {
        if self.ok {
            Some(self.at)
        } else {
            None
        }
    }
}

/// A name a key may hold: a DNS label's characters, and not empty.
fn name_ok(v: &[u8]) -> bool {
    !v.is_empty()
        && v.len() <= NAME_MAX
        && v.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.')
}

/// `<id>.<secret>`, the bootstrap-token shape.
fn token_shape(t: &[u8]) -> bool {
    t.len() == 23
        && t[6] == b'.'
        && t.iter()
            .enumerate()
            .all(|(i, &c)| i == 6 || c.is_ascii_lowercase() || c.is_ascii_digit())
}

fn sha256_hex(data: &[u8], out: &mut [u8; 64]) {
    let mut h = Sha256::new();
    h.update(data);
    hex_into(&h.finalize(), out);
}

// ── Chronicle frames ───────────────────────────────────────────────────────

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

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, v)) => v,
        _ => b"",
    }
}

fn frame_i64(frame: &[u8], number: u8) -> i64 {
    match frame_field(frame, number) {
        Some((TY_I64, v)) if v.len() == 8 => i64::from_le_bytes(v.try_into().unwrap()),
        _ => 0,
    }
}

/// `held` again, with `c_verdict` = `status`, `c_answer` = `answer` and the
/// body and credential (`c_body`, `c_key`) emptied: the chain answers what is
/// left, and a token or a request has no business in the records after this.
fn answered_frame(held: &[u8], status: i64, answer: &[u8], out: &mut [u8]) -> usize {
    let Some(&count) = held.first() else {
        return 0;
    };
    let mut p = 1usize;
    let mut w = 1usize;
    for _ in 0..count {
        if p + 4 > held.len() {
            return 0;
        }
        let num = held[p];
        let ty = held[p + 1];
        let len = u16::from_le_bytes([held[p + 2], held[p + 3]]) as usize;
        if p + 4 + len > held.len() {
            return 0;
        }
        let payload: &[u8] = &held[p + 4..p + 4 + len];
        let st = status.to_le_bytes();
        let (ty, v): (u8, &[u8]) = match num {
            F_VERDICT => (TY_I64, &st),
            F_ANSWER => (TY_BYTES, answer),
            F_BODY | F_KEY => (TY_BYTES, b""),
            _ => (ty, payload),
        };
        if w + 4 + v.len() > out.len() || v.len() > u16::MAX as usize {
            return 0;
        }
        out[w] = num;
        out[w + 1] = ty;
        cp(&mut out[w + 2..w + 4], &(v.len() as u16).to_le_bytes());
        cp(&mut out[w + 4..w + 4 + v.len()], v);
        w += 4 + v.len();
        p += 4 + len;
    }
    out[0] = count;
    w
}

/// A Kubernetes `Status` failure.
fn status_json(code: i64, reason: &[u8], msg: &[u8], out: &mut [u8]) -> usize {
    let mut d = [0u8; 20];
    let dn = dec_u64(code as u64, &mut d);
    let mut at = 0;
    for part in [
        &b"{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"reason\":\""[..],
        reason,
        b"\",\"code\":",
        &d[..dn],
        b",\"message\":\"",
        msg,
        b"\"}",
    ] {
        match put(out, at, part) {
            Some(n) => at = n,
            None => return 0,
        }
    }
    at
}

// ── DER: a certificate's notAfter ──────────────────────────────────────────

/// A duration as Kubernetes writes one (`24h`, `1h30m`, `90s`): numbers each
/// followed by `h`, `m` or `s`. `None` for anything else, or zero.
fn parse_duration(b: &[u8]) -> Option<u64> {
    if b.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut n = 0u64;
    let mut digits = 0;
    for &c in b {
        if c.is_ascii_digit() {
            n = n.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
            digits += 1;
            continue;
        }
        let unit = match c {
            b'h' => 3600,
            b'm' => 60,
            b's' => 1,
            _ => return None,
        };
        if digits == 0 {
            return None;
        }
        total = total.checked_add(n.checked_mul(unit)?)?;
        n = 0;
        digits = 0;
    }
    if digits != 0 || total == 0 {
        return None;
    }
    Some(total)
}

/// `secs` as the shortest whole duration: `24h`, `90m`, `45s`.
fn fmt_duration(secs: u64, out: &mut [u8; 24]) -> usize {
    let (v, unit) = if secs.is_multiple_of(3600) {
        (secs / 3600, b'h')
    } else if secs.is_multiple_of(60) {
        (secs / 60, b'm')
    } else {
        (secs, b's')
    };
    let mut d = [0u8; 20];
    let n = dec_u64(v, &mut d);
    cp(&mut out[..n], &d[..n]);
    out[n] = unit;
    n + 1
}

/// Seconds since the epoch of an RFC 3339 UTC time (`2026-11-08T12:00:00Z`).
fn rfc3339_unix(b: &[u8]) -> Option<u64> {
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return None;
    }
    let num = |i: usize, n: usize| -> Option<u64> { parse_u64(&b[i..i + n]) };
    let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    let (h, mi, sec) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
    if !(1..=12).contains(&mo) || d == 0 || d > 31 || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    // Days from civil (Howard Hinnant), for years past 1970.
    let y = if mo <= 2 { y - 1 } else { y };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = (era * 146097 + doe).checked_sub(719468)?;
    Some(days * 86400 + h * 3600 + mi * 60 + sec)
}

/// The lifetime a Device asks for (`spec.certificate.lifetime`, a day when
/// absent or unreadable), within the cell's bounds.
fn device_lifetime(s: &State, dev: &[u8]) -> u32 {
    let want = parse_duration(js3(dev, b"spec", b"certificate", b"lifetime"))
        .unwrap_or(DEFAULT_LIFETIME_S);
    let (min, max) = (u64::from(s.min_lifetime_s), u64::from(s.max_lifetime_s));
    let v = if want > max { max } else { want };
    let v = if v < min { min } else { v };
    if v > u64::from(u32::MAX) {
        u32::MAX
    } else {
        v as u32
    }
}

/// One TLV at `b`: (tag, value start, value end).
fn der_tlv(b: &[u8], at: usize) -> Option<(u8, usize, usize)> {
    let tag = *b.get(at)?;
    let first = *b.get(at + 1)?;
    let (len, hdr) = if first < 0x80 {
        (first as usize, 2)
    } else if first == 0x81 {
        (*b.get(at + 2)? as usize, 3)
    } else if first == 0x82 {
        (
            (*b.get(at + 2)? as usize) << 8 | *b.get(at + 3)? as usize,
            4,
        )
    } else {
        return None;
    };
    let s = at + hdr;
    let e = s.checked_add(len)?;
    if e > b.len() {
        return None;
    }
    Some((tag, s, e))
}

/// The certificate's `notAfter`, RFC 3339 (`2026-11-08T12:00:00Z`).
fn not_after(cert: &[u8], out: &mut [u8; 20]) -> bool {
    validity_time(cert, 1, out)
}

/// The certificate's `notBefore` (`which` 0) or `notAfter` (1), RFC 3339.
fn validity_time(cert: &[u8], which: u8, out: &mut [u8; 20]) -> bool {
    let Some((0x30, cs, _)) = der_tlv(cert, 0) else {
        return false;
    };
    let Some((0x30, ts, te)) = der_tlv(cert, cs) else {
        return false;
    };
    let mut at = ts;
    // [0] version, serial, signature, issuer, then validity.
    let mut seen = 0;
    while at < te {
        let Some((tag, s, e)) = der_tlv(cert, at) else {
            return false;
        };
        if tag == 0xA0 {
            at = e;
            continue;
        }
        seen += 1;
        if seen == 4 {
            if tag != 0x30 {
                return false;
            }
            let Some((nt, nbs, nb_end)) = der_tlv(cert, s) else {
                return false;
            };
            let Some((t, vs, ve)) = (if which == 0 {
                Some((nt, nbs, nb_end))
            } else {
                der_tlv(cert, nb_end)
            }) else {
                return false;
            };
            let v = &cert[vs..ve];
            let (year, rest): ([u8; 4], &[u8]) = match (t, v.len()) {
                (0x17, 13) => {
                    let yy = (v[0] - b'0') * 10 + (v[1] - b'0');
                    let c = if yy >= 50 { *b"19" } else { *b"20" };
                    ([c[0], c[1], v[0], v[1]], &v[2..])
                }
                (0x18, 15) => ([v[0], v[1], v[2], v[3]], &v[4..]),
                _ => return false,
            };
            if rest[10] != b'Z' || !rest[..10].iter().all(u8::is_ascii_digit) {
                return false;
            }
            cp(&mut out[..4], &year);
            out[4] = b'-';
            cp(&mut out[5..7], &rest[0..2]);
            out[7] = b'-';
            cp(&mut out[8..10], &rest[2..4]);
            out[10] = b'T';
            cp(&mut out[11..13], &rest[4..6]);
            out[13] = b':';
            cp(&mut out[14..16], &rest[6..8]);
            out[16] = b':';
            cp(&mut out[17..19], &rest[8..10]);
            out[19] = b'Z';
            return true;
        }
        let _ = s;
        at = e;
    }
    false
}

// ── module ABI ─────────────────────────────────────────────────────────────

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
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<State>() {
            return -2;
        }
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.record_in = in_chan;
        s.result_out = out_chan;
        s.response_in = -1;
        s.request_out = -1;
        s.resolved = 0;
        s.call.reset();
        s.held_len = 0;
        s.out_len = 0;
        s.phase = P_IDLE;
        s.mint_key_len = 0;
        s.owed = 0;
        s.sweep_at = 0;
        s.minted = 0;
        s.redeemed = 0;
        s.refused = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.trust_domain_len == 0 {
            let td = b"nanocloud.local";
            cp(&mut s.trust_domain[..td.len()], td);
            s.trust_domain_len = td.len() as u8;
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
        let sys = &*s.syscalls;
        if s.resolved == 0 {
            s.response_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.request_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.resolved = 1;
        }
        if s.out_len != 0 {
            let n = s.out_len as usize;
            if (sys.channel_write)(s.result_out, s.out.as_ptr(), n) <= 0 {
                return 0;
            }
            s.out_len = 0;
        }
        if s.request_out >= 0 {
            flush_call(s, sys);
        }
        if s.response_in >= 0 {
            drain_answers(s, sys);
        }
        match s.phase {
            P_PREP => prep(s, sys),
            P_FINISH => finish(s, sys),
            P_SIGN => {}
            _ => take_record(s, sys),
        }
        if s.phase == P_IDLE && s.out_len == 0 {
            sweep(s, sys);
        }
        0
    }
}

/// Read one record: another's goes on as it came; ours starts a redemption.
unsafe fn take_record(s: &mut State, sys: &SyscallTable) {
    let poll = (sys.channel_poll)(s.record_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    let n = (sys.channel_read)(s.record_in, s.held.as_mut_ptr(), BUF);
    if n <= 0 {
        return;
    }
    let len = n as usize;
    let held = &s.held[..len];
    if frame_bytes(held, F_RESOURCE) != b"certificatesigningrequests"
        || frame_i64(held, F_VERDICT) != V_UNSERVED
    {
        cp(&mut s.out[..len], held);
        s.out_len = len as u16;
        return;
    }
    s.held_len = len as u16;
    s.method = frame_i64(held, F_METHOD);
    s.phase = P_PREP;
    s.fin = 0;
    s.csr_len = 0;
    s.cert_b64_len = 0;
    prep(s, sys);
}

/// Answer the held request and return to taking records.
unsafe fn answer(s: &mut State, status: i64, body: &[u8]) {
    let n = {
        let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
        answered_frame(held, status, body, &mut s.out)
    };
    s.out_len = n as u16;
    s.held_len = 0;
    s.phase = P_IDLE;
    if status >= 400 {
        s.refused = s.refused.wrapping_add(1);
    }
}

unsafe fn refuse(s: &mut State, code: i64, reason: &[u8], msg: &[u8]) {
    let mut j = [0u8; 512];
    let n = status_json(code, reason, msg, &mut j);
    answer(s, code, &j[..n]);
}

/// Steps 1-4 of a redemption, from the store each time: a step that waits
/// (a read not answered, a write pending) leaves the phase as it is and the
/// whole is run again next step, going past what is already done.
unsafe fn prep(s: &mut State, sys: &SyscallTable) {
    let st = Store::plain(sys, &s.nag);
    let held_len = s.held_len as usize;
    let mut tok = [0u8; 23];
    let mut name = [0u8; NAME_MAX];
    let name_len;
    {
        let held = core::slice::from_raw_parts(s.held.as_ptr(), held_len);
        let t = frame_bytes(held, F_KEY);
        if t.is_empty() && s.method == M_POST {
            return reenroll(s, sys);
        }
        if !token_shape(t) {
            return refuse(
                s,
                401,
                b"Unauthorized",
                b"a device claim token is the bearer credential",
            );
        }
        cp(&mut tok, t);
        // GET names the CSR in the path (admit leaves the target in c_name).
        let target = frame_bytes(held, F_NAME);
        let tail = b"/certificatesigningrequests/";
        let nm: &[u8] = match target.windows(tail.len()).position(|w| w == tail) {
            Some(i) => &target[i + tail.len()..],
            None => b"",
        };
        if nm.len() > NAME_MAX {
            return refuse(s, 404, b"NotFound", b"no such certificate signing request");
        }
        cp(&mut name[..nm.len()], nm);
        name_len = nm.len();
    }
    cp(&mut s.id, &tok[..6]);

    // 1. The offer, and the secret's hash.
    let mut key = [0u8; MAX_KEY];
    let kl = put(&mut key, 0, CLAIMS)
        .and_then(|p| put(&mut key, p, &tok[..6]))
        .unwrap_or(0);
    let mut offer = [0u8; 1024];
    let got = get_value_rev(&st, &key[..kl], &mut offer);
    if st.waiting() {
        return;
    }
    if st.refusal().is_some() {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the claim could not be read; present it again",
        );
    }
    let Some((ol, orev)) = got else {
        return refuse(
            s,
            401,
            b"Unauthorized",
            b"unknown or withdrawn device claim token",
        );
    };
    let offer = &offer[..ol];
    let mut want = [0u8; 64];
    sha256_hex(&tok[7..], &mut want);
    if !same(js1(offer, b"secret"), &want) {
        return refuse(
            s,
            401,
            b"Unauthorized",
            b"unknown or withdrawn device claim token",
        );
    }
    let (ns, dn, uid, pfx) = (
        js1(offer, b"ns"),
        js1(offer, b"name"),
        js1(offer, b"uid"),
        js1(offer, b"prefix"),
    );
    if !name_ok(ns)
        || !name_ok(dn)
        || uid.is_empty()
        || uid.len() > NAME_MAX
        || pfx.len() > PARAM_MAX
    {
        return refuse(s, 503, b"ServiceUnavailable", b"the claim is not whole");
    }
    cp(&mut s.ns[..ns.len()], ns);
    s.ns_len = ns.len() as u8;
    cp(&mut s.name[..dn.len()], dn);
    s.name_len = dn.len() as u8;
    cp(&mut s.uid[..uid.len()], uid);
    s.uid_len = uid.len() as u8;
    cp(&mut s.prefix[..pfx.len()], pfx);
    s.prefix_len = pfx.len() as u8;
    let bound = js1(offer, b"key");
    let mut csr_name = [0u8; 13];
    cp(&mut csr_name[..7], b"device-");
    cp(&mut csr_name[7..], &tok[..6]);
    let mut ck = [0u8; MAX_KEY];
    let ckl = put(&mut ck, 0, CSRS)
        .and_then(|p| put(&mut ck, p, &csr_name))
        .unwrap_or(0);

    if s.method == M_GET {
        if name[..name_len] != csr_name[..] {
            return refuse(
                s,
                403,
                b"Forbidden",
                b"a claim token reads its own certificate signing request alone",
            );
        }
        let mut csr = [0u8; ANSWER_MAX];
        let got = get_value(&st, &ck[..ckl], &mut csr);
        if st.waiting() {
            return;
        }
        return match got {
            Some(n) => answer(s, 200, &csr[..n]),
            None if st.refusal().is_some() => refuse(
                s,
                503,
                b"ServiceUnavailable",
                b"the request could not be read; ask again",
            ),
            None => refuse(
                s,
                404,
                b"NotFound",
                b"no certificate signing request under this token yet",
            ),
        };
    }
    if s.method != M_POST {
        return refuse(
            s,
            405,
            b"MethodNotAllowed",
            b"create a certificate signing request, or get your own",
        );
    }

    match request_key(s, &st, held_len) {
        Got3::Yes => {}
        Got3::Wait => return,
        Got3::Done => return,
    }
    if s.req_name[..s.req_name_len as usize] != csr_name[..] {
        return refuse(
            s,
            403,
            b"Forbidden",
            b"the request must be named device-<token id>",
        );
    }
    let fp = s.fp;

    // 2. The Device the token was minted for, still.
    let mut dk = [0u8; MAX_KEY];
    let dkl = device_key(s, &mut dk);
    let got = get_value(&st, &dk[..dkl], &mut s.doc2);
    if st.waiting() {
        return;
    }
    let live = match got {
        Some(n) => js2(&s.doc2[..n], b"metadata", b"uid") == &s.uid[..s.uid_len as usize],
        None if st.refusal().is_some() => {
            return refuse(
                s,
                503,
                b"ServiceUnavailable",
                b"the Device could not be read; present the claim again",
            )
        }
        None => false,
    };
    if !live {
        return refuse(
            s,
            403,
            b"Forbidden",
            b"the Device this token was made for no longer exists",
        );
    }
    let dev = core::slice::from_raw_parts(s.doc2.as_ptr(), got.map_or(0, |n| n));
    if js2(dev, b"spec", b"revoked") == b"true" {
        return refuse(s, 403, b"Forbidden", b"the Device is revoked");
    }
    s.lifetime_s = device_lifetime(s, dev);
    s.renew = 0;

    // 3. The token bound to this key: the one step its expiry decides.
    if bound.is_empty() {
        let tt = dev_trusted_unix(sys);
        let now = u64::from_le_bytes(tt[0..8].try_into().unwrap());
        let exp = parse_u64(js1(offer, b"exp")).unwrap_or(0);
        if now == 0 || now >= exp {
            return refuse(
                s,
                401,
                b"Unauthorized",
                b"the device claim token has expired",
            );
        }
        let mut next = [0u8; 1024];
        let nl = j_set_top(offer, b"key", &quoted(&fp), &mut next);
        if nl == 0 {
            return refuse(
                s,
                503,
                b"ServiceUnavailable",
                b"the claim could not be bound",
            );
        }
        let a = wr(s, sys, &key[..kl], Some(&next[..nl]), PRE_ETAG, orev);
        match a {
            WriteAnswer::Decided(0) | WriteAnswer::Pending | WriteAnswer::Decided(ERR_EAGAIN) => {}
            _ => {
                return refuse(
                    s,
                    503,
                    b"ServiceUnavailable",
                    b"the claim could not be bound; present it again",
                )
            }
        }
        return; // read again next step, bound
    }
    if !same(bound, &fp) {
        return refuse(
            s,
            403,
            b"Forbidden",
            b"this device claim token was redeemed by another key",
        );
    }

    // 4. Issued already: the CSR exists.
    let there = exists(&st, &ck[..ckl]);
    if st.waiting() {
        return;
    }
    if there {
        return refuse(
            s,
            409,
            b"AlreadyExists",
            b"certificatesigningrequests.certificates.k8s.io already exists; get it under the same token",
        );
    }

    // 5. The authority, for the identity the offer names.
    ask_authority(s, sys);
}

/// Ask the authority to certify `point` for the Device named in `ns`/`name`,
/// for `lifetime_s`.
unsafe fn ask_authority(s: &mut State, sys: &SyscallTable) {
    let mut cb = [0u8; 1024];
    let Some(at) = cert_body(s, &mut cb) else {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the certificate request could not be built",
        );
    };
    let port = s.request_out;
    if port < 0 {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"no authority is wired to this signer",
        );
    }
    let cb = &cb[..at];
    if s.call.begin(port, |id, out| {
        call_head(id, 3, b"/pki/certificate", b"", cb, out)
    }) {
        s.phase = P_SIGN;
        flush_call(s, sys);
    }
}

/// A step that may have to be run again.
enum Got3 {
    Yes,
    Wait,
    /// Answered already (refused).
    Done,
}

/// The held request's CSR: read (inline, or staged by http_chain as
/// `sha256:<hex>`), its signer checked and its signature proved. Its key
/// lands in `point`/`fp`, its name in `req_name`.
unsafe fn request_key(s: &mut State, st: &Store, held_len: usize) -> Got3 {
    // The request: inline, or staged by http_chain as `sha256:<hex>`.
    let mut body_len = 0usize;
    {
        let held = core::slice::from_raw_parts(s.held.as_ptr(), held_len);
        let b = frame_bytes(held, F_BODY);
        if b.len() == 71 && b.starts_with(b"sha256:") {
            let mut bk = [0u8; MAX_KEY];
            let bkl = put(&mut bk, 0, BLOBS)
                .and_then(|p| put(&mut bk, p, &b[7..]))
                .unwrap_or(0);
            match get_value(st, &bk[..bkl], &mut s.doc) {
                Some(n) => body_len = n,
                None => {
                    if st.waiting() {
                        return Got3::Wait;
                    }
                    refuse(
                        s,
                        503,
                        b"ServiceUnavailable",
                        b"the request body could not be read; send it again",
                    );
                    return Got3::Done;
                }
            }
        } else if b.len() <= DOC_MAX {
            cp(&mut s.doc[..b.len()], b);
            body_len = b.len();
        }
    }
    let body = core::slice::from_raw_parts(s.doc.as_ptr(), body_len);
    if js2(body, b"spec", b"signerName") != SIGNER {
        refuse(
            s,
            403,
            b"Forbidden",
            b"a device's key is certified only by the nanocloud.io/device signer",
        );
        return Got3::Done;
    }
    let nm = js2(body, b"metadata", b"name");
    if nm.len() > NAME_MAX {
        refuse(s, 403, b"Forbidden", b"the request's name is too long");
        return Got3::Done;
    }
    cp(&mut s.req_name[..nm.len()], nm);
    s.req_name_len = nm.len() as u8;
    let mut pem = [0u8; 2048];
    let mut der = [0u8; 1024];
    let parsed = pkcs10::b64_decode(js2(body, b"spec", b"request"), &mut pem)
        .and_then(|n| pkcs10::pem_request(&pem[..n], &mut der))
        .and_then(|n| pkcs10::parse(&der[..n]));
    let Some(csr) = parsed else {
        refuse(
            s,
            400,
            b"BadRequest",
            b"spec.request is not a PEM PKCS#10 request for a P-256 key",
        );
        return Got3::Done;
    };
    let mut h = Sha256::new();
    h.update(csr.info);
    if !ecdsa_verify(csr.point, &h.finalize(), &csr.sig) {
        refuse(
            s,
            400,
            b"BadRequest",
            b"the request's signature does not verify",
        );
        return Got3::Done;
    }
    cp(&mut s.point, csr.point);
    let mut fp = [0u8; 64];
    sha256_hex(csr.point, &mut fp);
    s.fp = fp;

    Got3::Yes
}

fn quoted(fp: &[u8; 64]) -> [u8; 66] {
    let mut q = [b'"'; 66];
    cp(&mut q[1..65], fp);
    q
}

/// `/devices.nanocloud.io/<ns>/<name>`.
fn device_key(s: &State, out: &mut [u8; MAX_KEY]) -> usize {
    put(out, 0, DEVICES)
        .and_then(|p| put(out, p, &s.ns[..s.ns_len as usize]))
        .and_then(|p| put(out, p, b"/"))
        .and_then(|p| put(out, p, &s.name[..s.name_len as usize]))
        .unwrap_or(0)
}

/// `{"subject","tenant","device","public_key"}` for the authority.
fn cert_body(s: &State, out: &mut [u8]) -> Option<usize> {
    let mut k = [0u8; 96];
    let kl = b64::encode(&s.point, &mut k)?;
    let ns = &s.ns[..s.ns_len as usize];
    let nm = &s.name[..s.name_len as usize];
    let mut at = put(out, 0, b"{\"subject\":\"CN=device:")?;
    at = put(out, at, nm)?;
    at = put(out, at, b"\",\"tenant\":\"")?;
    at = put(out, at, ns)?;
    at = put(out, at, b"\",\"device\":\"")?;
    at = put(out, at, nm)?;
    at = put(out, at, b"\",\"public_key\":\"")?;
    at = put(out, at, k.get(..kl)?)?;
    at = put(out, at, b"\",\"lifetime_s\":")?;
    let mut d = [0u8; 20];
    let dn = dec_u64(u64::from(s.lifetime_s), &mut d);
    at = put(out, at, &d[..dn])?;
    put(out, at, b"}")
}

/// A Device's key, re-enrolled: no token and no valid certificate, only the
/// request's proof that it holds the key the Device records. The Device must
/// be the one the key was enrolled for (its uid), not revoked, and — when it
/// sets `spec.certificate.maxOffline` — not offline past it since its last
/// certificate expired. Answered as a redemption is, 201 and the CSR, named
/// `device-<the first 16 hex of the key's fingerprint>`; nothing is kept but
/// the claim's facts, so a lost answer is a request sent again.
unsafe fn reenroll(s: &mut State, sys: &SyscallTable) {
    let st = Store::plain(sys, &s.nag);
    let held_len = s.held_len as usize;
    match request_key(s, &st, held_len) {
        Got3::Yes => {}
        Got3::Wait | Got3::Done => return,
    }
    let fp = s.fp;
    let mut want = [0u8; 23];
    cp(&mut want[..7], b"device-");
    cp(&mut want[7..], &fp[..16]);
    if s.req_name[..s.req_name_len as usize] != want[..] {
        return refuse(
            s,
            403,
            b"Forbidden",
            b"a re-enrollment is named device-<first 16 hex of the key's SHA-256>",
        );
    }
    // The Device this key was enrolled for.
    let mut kk = [0u8; MAX_KEY];
    let kkl = put(&mut kk, 0, KEYS)
        .and_then(|p| put(&mut kk, p, &fp))
        .unwrap_or(0);
    let mut idx = [0u8; 512];
    let got = get_value(&st, &kk[..kkl], &mut idx);
    if st.waiting() {
        return;
    }
    let Some(il) = got else {
        if st.refusal().is_some() {
            return refuse(
                s,
                503,
                b"ServiceUnavailable",
                b"the key could not be looked up; send it again",
            );
        }
        return refuse(
            s,
            403,
            b"Forbidden",
            b"this key is enrolled to no Device; adopt it with a claim token",
        );
    };
    let idx = &idx[..il];
    let (ns, dn) = (js1(idx, b"ns"), js1(idx, b"name"));
    if !name_ok(ns) || !name_ok(dn) {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the key's record is not whole",
        );
    }
    cp(&mut s.ns[..ns.len()], ns);
    s.ns_len = ns.len() as u8;
    cp(&mut s.name[..dn.len()], dn);
    s.name_len = dn.len() as u8;
    let mut dk = [0u8; MAX_KEY];
    let dkl = device_key(s, &mut dk);
    let got = get_value(&st, &dk[..dkl], &mut s.doc2);
    if st.waiting() {
        return;
    }
    let Some(dl) = got else {
        if st.refusal().is_some() {
            return refuse(
                s,
                503,
                b"ServiceUnavailable",
                b"the Device could not be read; send it again",
            );
        }
        return refuse(
            s,
            403,
            b"Forbidden",
            b"the Device this key was enrolled for no longer exists",
        );
    };
    let dev = core::slice::from_raw_parts(s.doc2.as_ptr(), dl);
    let uid = js2(dev, b"metadata", b"uid");
    if js2(dev, b"spec", b"revoked") == b"true" {
        return refuse(s, 403, b"Forbidden", b"the Device is revoked");
    }
    // Its claim: the key it records, and when its last certificate expired.
    let mut ck = [0u8; MAX_KEY];
    let ckl = put(&mut ck, 0, CLAIM_OF)
        .and_then(|p| put(&mut ck, p, ns))
        .and_then(|p| put(&mut ck, p, b"/"))
        .and_then(|p| put(&mut ck, p, dn))
        .unwrap_or(0);
    let mut claim = [0u8; 1024];
    let got = get_value(&st, &ck[..ckl], &mut claim);
    if st.waiting() {
        return;
    }
    let Some(cl) = got else {
        return refuse(s, 403, b"Forbidden", b"the Device records no enrolled key");
    };
    let claim = &claim[..cl];
    if uid.is_empty() || js1(claim, b"uid") != uid || !same(js1(claim, b"fp"), &fp) {
        return refuse(
            s,
            403,
            b"Forbidden",
            b"the Device records another key (or was recreated); adopt it with a claim token",
        );
    }
    if let Some(max_off) = parse_duration(js3(dev, b"spec", b"certificate", b"maxOffline")) {
        let tt = dev_trusted_unix(sys);
        let now = u64::from_le_bytes(tt[0..8].try_into().unwrap());
        let expired = rfc3339_unix(js1(claim, b"notAfter")).unwrap_or(0);
        if now == 0 {
            return refuse(
                s,
                503,
                b"ServiceUnavailable",
                b"no trusted clock to judge spec.certificate.maxOffline by",
            );
        }
        if now > expired && now - expired > max_off {
            return refuse(s, 403, b"Forbidden", b"offline past spec.certificate.maxOffline since its certificate expired; adopt the Device again");
        }
    }
    let (id, pfx) = (js1(claim, b"id"), js1(claim, b"prefix"));
    if uid.len() > NAME_MAX || id.len() != 6 || pfx.len() > PARAM_MAX {
        return refuse(s, 503, b"ServiceUnavailable", b"the claim is not whole");
    }
    cp(&mut s.uid[..uid.len()], uid);
    s.uid_len = uid.len() as u8;
    cp(&mut s.id, id);
    cp(&mut s.prefix[..pfx.len()], pfx);
    s.prefix_len = pfx.len() as u8;
    s.lifetime_s = device_lifetime(s, dev);
    s.renew = 1;
    ask_authority(s, sys);
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
    let mut rec = [0u8; BUF];
    loop {
        let poll = (sys.channel_poll)(s.response_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.response_in, rec.as_mut_ptr(), BUF);
        if n <= 0 {
            return;
        }
        match s.call.take(&rec[..n as usize]) {
            CallEvent::Answered => {
                let (status, body) = s.call.answer();
                let body = core::slice::from_raw_parts(body.as_ptr(), body.len());
                signed(s, status, body);
            }
            CallEvent::Failed(status) => signed(s, status, &[]),
            _ => {}
        }
    }
}

/// The authority answered: the certificate is checked and the CSR built.
unsafe fn signed(s: &mut State, status: u16, body: &[u8]) {
    if s.phase != P_SIGN {
        return;
    }
    let mut der = [0u8; CERT_MAX];
    let leaf = if status == 200 {
        j_get1(body, b"certificate").and_then(|t| b64::decode(t, &mut der))
    } else {
        None
    };
    let Some(ll) = leaf else {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the authority did not certify the key; present the claim again",
        );
    };
    let der = &der[..ll];
    // The answer must certify the key that was proved.
    let mut nb = [0u8; 20];
    if !pkcs10::contains(der, &s.point)
        || !not_after(der, &mut s.not_after)
        || !validity_time(der, 0, &mut nb)
    {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the authority's answer did not certify the key proved",
        );
    }
    // What the certificate lasts, read from it: an authority that does not
    // take a requested lifetime issues its own, and the status says so.
    let span = rfc3339_unix(&s.not_after)
        .unwrap_or(0)
        .saturating_sub(rfc3339_unix(&nb).unwrap_or(0));
    s.issued_s = if span > u64::from(u32::MAX) {
        u32::MAX
    } else {
        span as u32
    };
    if s.issued_s != s.lifetime_s {
        // The authority chose its own lifetime (one that takes none per
        // request issues its fixed one): said, since the Device asked another.
        let mut m = [0u8; 128];
        let mut a = [0u8; 20];
        let mut i = [0u8; 20];
        let an = dec_u64(u64::from(s.lifetime_s), &mut a);
        let inn = dec_u64(u64::from(s.issued_s), &mut i);
        let mut w = W::new(&mut m);
        w.s(b"[device_pki] device-")
            .s(&s.id)
            .s(b": asked ")
            .s(&a[..an])
            .s(b" s, the authority issued ")
            .s(&i[..inn])
            .s(b" s");
        let n = w.end().unwrap_or(0);
        dev_log(&*s.syscalls, 1, m.as_ptr(), n);
    }
    let spiffe = j_get1(body, b"spiffe_id").unwrap_or(b"");
    let spiffe = if spiffe.len() >= 2 && spiffe[0] == b'"' {
        &spiffe[1..spiffe.len() - 1]
    } else {
        spiffe
    };
    if spiffe.len() > PARAM_MAX {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the authority's identity is too long",
        );
    }
    cp(&mut s.spiffe[..spiffe.len()], spiffe);
    s.spiffe_len = spiffe.len() as u8;
    let mut pem = [0u8; CERT_MAX];
    let built = pkcs10::pem_certificate(der, &mut pem)
        .and_then(|pn| pkcs10::b64_encode(&pem[..pn], &mut s.cert_b64));
    let Some(cn) = built else {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the certificate could not be encoded",
        );
    };
    s.cert_b64_len = cn as u16;
    let mut csr = [0u8; ANSWER_MAX];
    let Some(n) = csr_doc(s, &mut csr) else {
        return refuse(
            s,
            503,
            b"ServiceUnavailable",
            b"the certificate signing request could not be built",
        );
    };
    cp(&mut s.csr[..n], &csr[..n]);
    s.csr_len = n as u16;
    s.phase = P_FINISH;
    s.fin = 0;
}

/// The CSR as stored and answered.
fn csr_doc(s: &State, out: &mut [u8]) -> Option<usize> {
    let mut w = W::new(out);
    w.s(b"{\"apiVersion\":\"certificates.k8s.io/v1\",\"kind\":\"CertificateSigningRequest\",\"metadata\":{\"name\":\"device-");
    if s.renew != 0 {
        w.s(&s.fp[..16])
            .s(b"\"},\"spec\":{\"signerName\":\"nanocloud.io/device\",\"usages\":[\"digital signature\",\"client auth\"],\"username\":\"")
            .s(&s.spiffe[..s.spiffe_len as usize])
            .s(b"\"},\"status\":{\"conditions\":[{\"type\":\"Approved\",\"status\":\"True\",\"reason\":\"DeviceKey\",\"message\":\"re-enrolled by the key the Device records\"}],\"certificate\":\"");
    } else {
        w.s(&s.id)
            .s(b"\"},\"spec\":{\"signerName\":\"nanocloud.io/device\",\"usages\":[\"digital signature\",\"client auth\"],\"username\":\"system:bootstrap:")
            .s(&s.id)
            .s(b"\",\"groups\":[\"system:bootstrappers\",\"system:authenticated\"]},\"status\":{\"conditions\":[{\"type\":\"Approved\",\"status\":\"True\",\"reason\":\"DeviceClaim\",\"message\":\"the device claim token was redeemed by this key\"}],\"certificate\":\"");
    }
    w.s(&s.cert_b64[..s.cert_b64_len as usize]).s(b"\"}}");
    w.end()
}

/// Step 6, a write at a time: the CSR, then the claim (the token emptied,
/// the enrollment's facts kept), then the answer. The Device's own status is
/// the sweep's to write (`tend`), from the claim: a write to an API object
/// waits on the API's own store path, which this request is holding.
unsafe fn finish(s: &mut State, sys: &SyscallTable) {
    let st = Store::plain(sys, &s.nag);
    match s.fin {
        0 if s.renew != 0 => s.fin = 1, // a re-enrollment keeps no CSR
        0 => {
            let mut ck = [0u8; MAX_KEY];
            let ckl = put(&mut ck, 0, CSRS)
                .and_then(|p| put(&mut ck, p, b"device-"))
                .and_then(|p| put(&mut ck, p, &s.id))
                .unwrap_or(0);
            let csr = core::slice::from_raw_parts(s.csr.as_ptr(), s.csr_len as usize);
            match wr(s, sys, &ck[..ckl], Some(csr), PRE_ABSENT, 0) {
                // EEXIST: a concurrent redemption under this token and key
                // made it first, and theirs stands.
                WriteAnswer::Decided(0) | WriteAnswer::Decided(ERR_EEXIST) => s.fin = 1,
                WriteAnswer::Pending => {}
                _ => refuse(
                    s,
                    503,
                    b"ServiceUnavailable",
                    b"the certificate could not be kept; present the claim again",
                ),
            }
        }
        1 => {
            let mut k = [0u8; MAX_KEY];
            let kl = put(&mut k, 0, CLAIM_OF)
                .and_then(|p| put(&mut k, p, &s.ns[..s.ns_len as usize]))
                .and_then(|p| put(&mut k, p, b"/"))
                .and_then(|p| put(&mut k, p, &s.name[..s.name_len as usize]))
                .unwrap_or(0);
            let mut v = [0u8; 1024];
            let got = get_value_rev(&st, &k[..kl], &mut v);
            if st.waiting() {
                return;
            }
            let Some((n, rev)) = got else {
                s.fin = 2; // the Device went, and its claim with it
                return;
            };
            let cur = &v[..n];
            let had = js1(cur, b"fp");
            if js1(cur, b"uid") != &s.uid[..s.uid_len as usize]
                || !(had.is_empty() || same(had, &s.fp))
                || (!had.is_empty() && js1(cur, b"notAfter") == &s.not_after[..])
            {
                s.fin = 2; // another's, or written already
                return;
            }
            let mut life = [0u8; 20];
            let lifen = dec_u64(u64::from(s.issued_s), &mut life);
            let mut next = [0u8; 1024];
            let mut w = W::new(&mut next);
            w.s(b"{\"uid\":\"")
                .s(&s.uid[..s.uid_len as usize])
                .s(b"\",\"id\":\"")
                .s(&s.id)
                .s(b"\",\"token\":\"\",\"prefix\":\"")
                .s(&s.prefix[..s.prefix_len as usize])
                .s(b"\",\"fp\":\"")
                .s(&s.fp)
                .s(b"\",\"notAfter\":\"")
                .s(&s.not_after)
                .s(b"\",\"spiffe\":\"")
                .s(&s.spiffe[..s.spiffe_len as usize])
                .s(b"\",\"lifetime\":")
                .s(&life[..lifen])
                .s(b"}");
            let Some(nl) = w.end() else {
                s.fin = 2;
                return;
            };
            match wr(s, sys, &k[..kl], Some(&next[..nl]), PRE_ETAG, rev) {
                WriteAnswer::Decided(0) => s.fin = 2,
                WriteAnswer::Decided(ERR_EAGAIN) | WriteAnswer::Pending => {}
                _ => s.fin = 2,
            }
        }
        2 => {
            // The key's index, for its re-enrollment.
            let mut kk = [0u8; MAX_KEY];
            let kkl = put(&mut kk, 0, KEYS)
                .and_then(|p| put(&mut kk, p, &s.fp))
                .unwrap_or(0);
            let mut v = [0u8; 256];
            let mut w = W::new(&mut v);
            w.s(b"{\"ns\":\"")
                .s(&s.ns[..s.ns_len as usize])
                .s(b"\",\"name\":\"")
                .s(&s.name[..s.name_len as usize])
                .s(b"\"}");
            let Some(vl) = w.end() else {
                s.fin = 3;
                return;
            };
            if s.renew != 0 {
                s.fin = 3;
                return;
            }
            match wr(s, sys, &kk[..kkl], Some(&v[..vl]), PRE_ANY, 0) {
                WriteAnswer::Pending => {}
                _ => s.fin = 3,
            }
        }
        _ => {
            s.redeemed = s.redeemed.wrapping_add(1);
            let mut m = [0u8; 96];
            let w = put(
                &mut m,
                0,
                if s.renew != 0 {
                    b"[device_pki] re-enrolled device-"
                } else {
                    b"[device_pki] redeemed device-"
                },
            )
            .and_then(|p| put(&mut m, p, &s.id))
            .unwrap_or(0);
            dev_log(sys, 2, m.as_ptr(), w);
            let csr = core::slice::from_raw_parts(s.csr.as_ptr(), s.csr_len as usize);
            answer(s, 201, csr);
        }
    }
}

/// The Device's status once enrolled, from its claim.
fn enrolled_status(claim: &[u8], broker: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut life = [0u8; 24];
    let ln = fmt_duration(parse_u64(js1(claim, b"lifetime")).unwrap_or(0), &mut life);
    let mut w = W::new(out);
    w.s(b"{\"phase\":\"Enrolled\",\"identity\":{\"spiffeId\":\"").s(js1(claim, b"spiffe"))
        .s(b"\",\"keyFingerprint\":\"sha256:").s(js1(claim, b"fp"))
        .s(b"\",\"certificateNotAfter\":\"").s(js1(claim, b"notAfter"))
        .s(b"\",\"certificateLifetime\":\"").s(&life[..ln])
        .s(b"\"},\"mqtt\":{\"broker\":\"").s(broker)
        .s(b"\",\"topicPrefix\":\"").s(js1(claim, b"prefix"))
        .s(b"\"},\"conditions\":[{\"type\":\"Claimed\",\"status\":\"True\"},{\"type\":\"CertificateValid\",\"status\":\"True\"}]}");
    w.end()
}

// ── writes, under the pending rule ─────────────────────────────────────────

/// Ask the owed write, from its own buffers: the request names them by
/// address, so they are the same on every ask.
unsafe fn ask_owed(s: &mut State, sys: &SyscallTable) -> WriteAnswer {
    let key = &s.owed_key[..s.owed_key_len as usize];
    let mut arg = [0u8; MAX_KEY + 96];
    let etag_len = if s.owed_pre == PRE_ETAG { 32 } else { 0 };
    let kept = abi::contracts::storage::object::Kept::none();
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    cp(&mut arg[p..p + key.len()], key);
    p += key.len();
    if s.owed_put != 0 {
        arg[p] = 0;
        p += 1;
        arg[p..p + 8].copy_from_slice(&(s.owed_val.as_ptr() as u64).to_le_bytes());
        p += 8;
        arg[p..p + 8].copy_from_slice(&(s.owed_len as u64).to_le_bytes());
        p += 8;
    }
    arg[p] = s.owed_pre;
    p += 1;
    arg[p] = etag_len as u8;
    p += 1;
    if etag_len > 0 {
        cp(&mut arg[p..p + 32], &etag_of(s.owed_rev));
        p += 32;
    }
    if s.owed_put != 0 {
        match kept.encode(&mut arg[p..]) {
            Some(n) => p += n,
            None => return WriteAnswer::Decided(-22),
        }
    }
    arg[p..p + 8].copy_from_slice(&(s.owed_fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let op = if s.owed_put != 0 { OBJ_PUT } else { OBJ_DELETE };
    let a = write_answer((sys.provider_call)(-1, op, arg.as_mut_ptr(), p));
    if a != WriteAnswer::Pending {
        s.owed = 0;
    }
    a
}

/// Write `key` (`val` absent: delete) under a precondition. A write owed
/// already is asked first: the same write is answered by it, another waits
/// (`Pending`) until it is decided.
unsafe fn wr(
    s: &mut State,
    sys: &SyscallTable,
    key: &[u8],
    val: Option<&[u8]>,
    pre: u8,
    rev: u64,
) -> WriteAnswer {
    if s.owed != 0 {
        let same_write = &s.owed_key[..s.owed_key_len as usize] == key
            && s.owed_pre == pre
            && s.owed_rev == rev
            && match val {
                Some(v) => s.owed_put != 0 && &s.owed_val[..s.owed_len as usize] == v,
                None => s.owed_put == 0,
            };
        let a = ask_owed(s, sys);
        if same_write || a == WriteAnswer::Pending {
            return if same_write { a } else { WriteAnswer::Pending };
        }
    }
    if key.len() > MAX_KEY || val.map_or(0, |v| v.len()) > DOC_MAX {
        return WriteAnswer::Decided(-22);
    }
    cp(&mut s.owed_key, key);
    s.owed_key_len = key.len() as u8;
    match val {
        Some(v) => {
            cp(&mut s.owed_val, v);
            s.owed_len = v.len() as u16;
            s.owed_put = 1;
        }
        None => {
            s.owed_len = 0;
            s.owed_put = 0;
        }
    }
    s.owed_pre = pre;
    s.owed_rev = rev;
    s.owed = 1;
    ask_owed(s, sys)
}

// ── MINT ───────────────────────────────────────────────────────────────────

/// Every `SWEEP_MS`: the first customer Device (one with `spec.class`) whose
/// claim is not whole is brought one write closer to it.
unsafe fn sweep(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys);
    if now < s.sweep_at {
        return;
    }
    s.sweep_at = now + SWEEP_MS;
    let st = Store::plain(sys, &s.nag);
    let mut walk = ListWalk::new(DEVICES);
    let mut seen = 0u32;
    let mut key = [0u8; MAX_KEY];
    loop {
        let kl = match walk.next(&st) {
            Some(name) => {
                if name.starts_with(DEVICES) {
                    put(&mut key, 0, name).unwrap_or(0)
                } else {
                    put(&mut key, 0, DEVICES)
                        .and_then(|p| put(&mut key, p, name))
                        .unwrap_or(0)
                }
            }
            None => return,
        };
        seen += 1;
        if seen > SWEEP_MAX || st.waiting() || st.refusal().is_some() {
            return;
        }
        if kl == 0 {
            continue;
        }
        let k: [u8; MAX_KEY] = key;
        if !tend(s, sys, &st, &k[..kl]) {
            // One write asked: the rest wait for the next sweep.
            s.sweep_at = now + 50;
            return;
        }
    }
}

/// Bring Device `key`'s claim one step closer to whole. True when it already
/// is (or the Device is not a customer's), false when a write was asked or a
/// read must be asked again.
unsafe fn tend(s: &mut State, sys: &SyscallTable, st: &Store, key: &[u8]) -> bool {
    let rest = &key[DEVICES.len()..];
    let Some(slash) = rest.iter().position(|&c| c == b'/') else {
        return true;
    };
    let (ns, name) = (&rest[..slash], &rest[slash + 1..]);
    if !name_ok(ns) || !name_ok(name) {
        return true;
    }
    let Some((dl, drev)) = get_value_rev(st, key, &mut s.doc) else {
        return !st.waiting();
    };
    let dev = core::slice::from_raw_parts(s.doc.as_ptr(), dl);
    if js2(dev, b"spec", b"class").is_empty() {
        return true;
    }
    let uid = js2(dev, b"metadata", b"uid");
    if uid.is_empty() || uid.len() > NAME_MAX {
        return true;
    }
    let revoked = js2(dev, b"spec", b"revoked") == b"true";
    if revoked {
        // Revoked: no token is minted, none redeemed, no key re-enrolled.
        if js2(dev, b"status", b"phase") == b"Revoked" {
            return true;
        }
        let nl = j_set_top(
            dev,
            b"status",
            b"{\"phase\":\"Revoked\",\"conditions\":[{\"type\":\"Revoked\",\"status\":\"True\"}]}",
            &mut s.doc2,
        );
        if nl > 0 {
            let next = core::slice::from_raw_parts(s.doc2.as_ptr(), nl);
            let _ = wr(s, sys, key, Some(next), PRE_ETAG, drev);
        }
        return false;
    }

    // The claim of this Device, by uid.
    let mut ck = [0u8; MAX_KEY];
    let ckl = put(&mut ck, 0, CLAIM_OF)
        .and_then(|p| put(&mut ck, p, rest))
        .unwrap_or(0);
    let mut claim = [0u8; 1024];
    let got = get_value_rev(st, &ck[..ckl], &mut claim);
    if st.waiting() || st.refusal().is_some() {
        return false;
    }
    let mine = match got {
        Some((n, _)) if js1(&claim[..n], b"uid") == uid => Some(n),
        _ => None,
    };
    let Some(cn) = mine else {
        // None for this uid: an earlier one's offer goes first, then a token
        // is drawn (once, kept until written) and the claim written.
        if let Some((n, rev)) = got {
            let old = js1(&claim[..n], b"id");
            if old.len() == 6 {
                let mut ok = [0u8; MAX_KEY];
                let okl = put(&mut ok, 0, CLAIMS)
                    .and_then(|p| put(&mut ok, p, old))
                    .unwrap_or(0);
                if exists(st, &ok[..okl]) {
                    let _ = wr(s, sys, &ok[..okl], None, PRE_ANY, 0);
                    return false;
                }
                if st.waiting() {
                    return false;
                }
            }
            let _ = rev;
        }
        return mint(s, sys, st, &ck[..ckl], ns, name, uid);
    };
    let claim = &claim[..cn];
    if !js1(claim, b"fp").is_empty() {
        // Redeemed: the Device reads Enrolled, from the claim's facts, as of
        // its latest certificate.
        if js2(dev, b"status", b"phase") == b"Enrolled"
            && js3(dev, b"status", b"identity", b"certificateNotAfter") == js1(claim, b"notAfter")
        {
            return true;
        }
        let mut status = [0u8; 768];
        let br = &s.broker[..s.broker_len as usize];
        let Some(sl) = enrolled_status(claim, br, &mut status) else {
            return true;
        };
        let nl = j_set_top(dev, b"status", &status[..sl], &mut s.doc2);
        if nl > 0 {
            let next = core::slice::from_raw_parts(s.doc2.as_ptr(), nl);
            let _ = wr(s, sys, key, Some(next), PRE_ETAG, drev);
        }
        return false;
    }
    let id = js1(claim, b"id");
    let token = js1(claim, b"token");
    let prefix = js1(claim, b"prefix");
    if id.len() != 6 || prefix.len() > PARAM_MAX {
        return true;
    }

    // The offer.
    let mut ok = [0u8; MAX_KEY];
    let okl = put(&mut ok, 0, CLAIMS)
        .and_then(|p| put(&mut ok, p, id))
        .unwrap_or(0);
    let there = exists(st, &ok[..okl]);
    if st.waiting() {
        return false;
    }
    if !there {
        if token.len() != 23 {
            return true; // redeemed and its offer gone: nothing to make
        }
        let mut secret = [0u8; 64];
        sha256_hex(&token[7..], &mut secret);
        let tt = dev_trusted_unix(sys);
        let now = u64::from_le_bytes(tt[0..8].try_into().unwrap());
        let mut d = [0u8; 20];
        let dn = dec_u64(now + u64::from(s.claim_ttl_s), &mut d);
        let mut v = [0u8; 768];
        let vl = [
            &b"{\"ns\":\""[..],
            ns,
            b"\",\"name\":\"",
            name,
            b"\",\"uid\":\"",
            uid,
            b"\",\"secret\":\"",
            &secret,
            b"\",\"exp\":",
            &d[..dn],
            b",\"prefix\":\"",
            prefix,
            b"\",\"key\":\"\"}",
        ]
        .iter()
        .try_fold(0usize, |at, p| put(&mut v, at, p));
        if let Some(vl) = vl {
            let _ = wr(s, sys, &ok[..okl], Some(&v[..vl]), PRE_ABSENT, 0);
        }
        return false;
    }

    // The Device's status, Claimable.
    if js3(dev, b"status", b"mqtt", b"topicPrefix") == prefix {
        return true;
    }
    let mut status = [0u8; 768];
    let td = &s.trust_domain[..s.trust_domain_len as usize];
    let br = &s.broker[..s.broker_len as usize];
    let sl = [
        &b"{\"phase\":\"Claimable\",\"identity\":{\"spiffeId\":\"spiffe://"[..],
        td,
        b"/device/",
        ns,
        b"/",
        name,
        b"\"},\"mqtt\":{\"broker\":\"",
        br,
        b"\",\"topicPrefix\":\"",
        prefix,
        b"\"},\"conditions\":[{\"type\":\"Claimed\",\"status\":\"False\"}]}",
    ]
    .iter()
    .try_fold(0usize, |at, p| put(&mut status, at, p));
    let Some(sl) = sl else {
        return true;
    };
    let nl = j_set_top(dev, b"status", &status[..sl], &mut s.doc2);
    if nl > 0 {
        let next = core::slice::from_raw_parts(s.doc2.as_ptr(), nl);
        let _ = wr(s, sys, key, Some(next), PRE_ETAG, drev);
    }
    false
}

/// Write the claim of a Device: a token drawn once and kept until written.
unsafe fn mint(
    s: &mut State,
    sys: &SyscallTable,
    st: &Store,
    claim_key: &[u8],
    ns: &[u8],
    name: &[u8],
    uid: &[u8],
) -> bool {
    // The namespace's uid, hashed, for the topic prefix.
    let mut nk = [0u8; MAX_KEY];
    let nkl = put(&mut nk, 0, b"/namespaces/")
        .and_then(|p| put(&mut nk, p, ns))
        .unwrap_or(0);
    let mut nsdoc = [0u8; 2048];
    let got = get_value(st, &nk[..nkl], &mut nsdoc);
    if st.waiting() {
        return false;
    }
    let nsuid = match got {
        Some(n) => js2(&nsdoc[..n], b"metadata", b"uid"),
        None => b"",
    };
    if nsuid.is_empty() {
        return true; // no namespace (yet): nothing to scope it to
    }
    let mut opaque = [0u8; 64];
    sha256_hex(nsuid, &mut opaque);

    let fresh = s.mint_key_len as usize != claim_key.len()
        || &s.mint_key[..s.mint_key_len as usize] != claim_key
        || &s.mint_uid[..s.mint_uid_len as usize] != uid;
    if fresh {
        let mut rnd = [0u8; 22];
        if dev_csprng_fill(sys, rnd.as_mut_ptr(), rnd.len()) < 0 {
            return false;
        }
        const A: &[u8; 36] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        for (i, slot) in rnd.iter_mut().enumerate() {
            let r = *slot;
            // 252 = 7 * 36: a draw past it is taken again from its own bits
            // halved, which keeps the alphabet near-uniform without a loop.
            let c = if r < 252 { r % 36 } else { (r >> 1) % 36 };
            let at = if i < 6 { i } else { i + 1 };
            s.mint_token[at] = A[c as usize];
            *slot = 0;
        }
        s.mint_token[6] = b'.';
        cp(&mut s.mint_key[..claim_key.len()], claim_key);
        s.mint_key_len = claim_key.len() as u8;
        cp(&mut s.mint_uid[..uid.len()], uid);
        s.mint_uid_len = uid.len() as u8;
    }
    let tok = s.mint_token;
    let mut v = [0u8; 512];
    let vl = [
        &b"{\"uid\":\""[..],
        uid,
        b"\",\"id\":\"",
        &tok[..6],
        b"\",\"token\":\"",
        &tok,
        b"\",\"prefix\":\"t/",
        &opaque[..16],
        b"/devices/",
        name,
        b"/\"}",
    ]
    .iter()
    .try_fold(0usize, |at, p| put(&mut v, at, p));
    let Some(vl) = vl else {
        return true;
    };
    match wr(s, sys, claim_key, Some(&v[..vl]), PRE_ANY, 0) {
        WriteAnswer::Decided(0) => {
            s.mint_key_len = 0;
            s.minted = s.minted.wrapping_add(1);
            let mut m = [0u8; 160];
            let w = [&b"[device_pki] claim minted for "[..], ns, b"/", name]
                .iter()
                .try_fold(0usize, |at, p| put(&mut m, at, p))
                .unwrap_or(0);
            dev_log(sys, 2, m.as_ptr(), w);
        }
        WriteAnswer::Pending => {}
        _ => s.mint_key_len = 0,
    }
    false
}
