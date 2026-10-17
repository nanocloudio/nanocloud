//! The encrypted-volume pipelines' node effector: a volume effect in, the
//! volume's subgraph staged, unlocked, published and torn down on this node,
//! the typed outcome back as a record.
//!
//! See `manifest.toml` for the wire. The subgraph is `_shared/
//! volume_graph.rs`, staged as a workload; the Kagi requests are `_shared/
//! kagi_attach_ops.rs`, carried to the storage-key endpoint through the
//! graph's HTTP client. This module keeps the instances, their lanes, the
//! recipient each `crypt_block` announced, the bundles released to it and
//! the renewal chain, and runs `nbd-client` through the process executor.
//! The bundle never leaves the node: it goes from Kagi's answer to the
//! instance's control input.

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
include!("../_shared/exchange_call.rs");

#[path = "../../../target/fluxor/kagi-common/auth_wire.rs"]
mod auth_wire;
#[path = "../../../target/fluxor/fluxor-abi/sdk/platform/linux/host_process.rs"]
mod hp;
#[path = "../../../target/fluxor/kagi-common/issuer_key.rs"]
mod issuer_key;
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/key_vault.rs"]
mod key_vault;
#[path = "../../../target/fluxor/kagi-common/state_wire.rs"]
mod state_wire;
#[path = "../../../target/fluxor/kagi-common/storage_key.rs"]
mod storage_key;
#[path = "../../../target/fluxor/kagi-common/storage_key_service.rs"]
mod storage_key_service;
#[path = "../../../target/fluxor/kagi-common/time_policy.rs"]
mod time_policy;
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/workload.rs"]
mod wl;

use storage_key as sk;
use storage_key_service as svc;

include!("../_shared/kagi_attach_ops.rs");
include!("../_shared/volume_graph.rs");

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_FIELD: u8 = 254;
const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;
const CHAN_EINVAL: i32 = -22;
const EACCES: i32 = 13;

const REC: usize = 8192;
/// Volumes this node serves at once.
const MAX_INST: usize = 4;
const MAX_CLAIM: usize = 128;
const MAX_CRED: usize = 4096;
const MAX_LABEL: usize = 64;
/// The node's capability file, read whole (one `fxcap1.` chain per line):
/// as much as loam_volume takes, its `CAP_FILE_MAX` — eight chains
/// (`MAX_SESSION_GRANTS`) of at most 1375 bytes (`MAX_TEXT_LEN`) and a newline.
const CAP_TEXT_MAX: usize = 8 * (1375 + 1);
const FLXA_CAP: usize = 3 * MAX_CRED + CAP_TEXT_MAX + 2048;
const FS_OPEN: u32 = 0x0900;
const FS_READ: u32 = 0x0901;
const FS_CLOSE: u32 = 0x0903;
const FS_EAGAIN: i32 = -11;
const WL_EINPROGRESS: i32 = -36;
const WL_EEXIST: i32 = -17;
const STATUS_LEN: usize = 40;
const CMD: usize = 256;
/// The storage-key endpoint's path, on the HTTP client's pinned authority.
const KAGI_PATH: &[u8] = b"/storage-key";

// Effects.
const OP_GRAPH_START: i64 = 10;
const OP_RECIPIENT: i64 = 11;
const OP_BUNDLE_DELIVER: i64 = 12;
const OP_NBD_START: i64 = 13;
const OP_FLUSH: i64 = 14;
const OP_NBD_STOP: i64 = 15;
const OP_GRAPH_DESTROY: i64 = 16;
const OP_KAGI_RELEASE: i64 = 22;
const OP_KAGI_RENEW: i64 = 23;
const OP_CB_STATUS: i64 = 67;
const OP_CB_RECIPIENT: i64 = 68;
const OP_CB_BUNDLE: i64 = 69;
const OP_CB_ERASE: i64 = 72;
const OP_KAGI_RECOVER: i64 = 79;

fn ours(op: i64) -> bool {
    matches!(
        op,
        OP_GRAPH_START
            | OP_RECIPIENT
            | OP_BUNDLE_DELIVER
            | OP_NBD_START
            | OP_FLUSH
            | OP_NBD_STOP
            | OP_GRAPH_DESTROY
            | OP_KAGI_RELEASE
            | OP_KAGI_RENEW
            | OP_CB_STATUS
            | OP_CB_RECIPIENT
            | OP_CB_BUNDLE
            | OP_CB_ERASE
            | OP_KAGI_RECOVER
    )
}

// What an effect in flight waits for.
const W_NONE: u8 = 0;
/// An FXRK on the instance's recipient lane.
const W_RECIPIENT: u8 = 1;
/// An FXST that settles a bundle.
const W_BUNDLE: u8 = 2;
/// An FXST reporting the erase.
const W_ERASE: u8 = 3;
/// The storage-key endpoint's answer.
const W_KAGI: u8 = 4;
/// A tool's exit.
const W_TOOL: u8 = 5;

/// FXST states.
const ST_AWAITING: u8 = 1;
const ST_READY: u8 = 2;
const ST_ROTATING: u8 = 3;
const ST_FAILED: u8 = 4;
const ST_ERASED: u8 = 5;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ok,
    Ready,
    Stale,
    Eacces,
    Refused,
    Custodians,
    FenceChanged,
    Expired,
    Conflict,
    Failed,
    Unavailable,
}

fn out_name(o: Outcome) -> &'static [u8] {
    match o {
        Outcome::Ok => b"ok",
        Outcome::Ready => b"ready",
        Outcome::Stale => b"stale",
        Outcome::Eacces => b"eacces",
        Outcome::Refused => b"refused",
        Outcome::Custodians => b"custodians",
        Outcome::FenceChanged => b"fence_changed",
        Outcome::Expired => b"expired",
        Outcome::Conflict => b"conflict",
        Outcome::Failed => b"failed",
        Outcome::Unavailable => b"unavailable",
    }
}

#[derive(Clone, Copy)]
struct Inst {
    used: bool,
    gen: u16,
    claim: [u8; MAX_CLAIM],
    claim_len: u8,
    handle: i32,
    /// Lanes: crypt_block's recipient and status to this module, this
    /// module's records to its control input.
    recipient_in: i32,
    status_in: i32,
    ctrl_out: i32,
    /// The latest recipient announced, and whether it is still unspent.
    recipient: [u8; sk::RECIPIENT_LEN],
    has_recipient: bool,
    /// The latest status record.
    status: [u8; STATUS_LEN],
    has_status: bool,
    /// A bundle released for `bundle_epoch`, not yet delivered.
    bundle: [u8; sk::BUNDLE_LEN],
    bundle_epoch: u32,
    has_bundle: bool,
    /// The renewal chain's head.
    chain: [u8; sk::MAX_RECORD],
    chain_len: u16,
    nbd_index: u8,
    published: bool,
}

const NO_INST: Inst = Inst {
    used: false,
    gen: 0,
    claim: [0; MAX_CLAIM],
    claim_len: 0,
    handle: -1,
    recipient_in: -1,
    status_in: -1,
    ctrl_out: -1,
    recipient: [0; sk::RECIPIENT_LEN],
    has_recipient: false,
    status: [0; STATUS_LEN],
    has_status: false,
    bundle: [0; sk::BUNDLE_LEN],
    bundle_epoch: 0,
    has_bundle: false,
    chain: [0; sk::MAX_RECORD],
    chain_len: 0,
    nbd_index: 0,
    published: false,
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    request_out: i32,
    response_in: i32,
    resolved: u8,
    busy: u8,
    configured: u8,
    wait: u8,

    // Params.
    loam_authority: [u8; 128],
    loam_authority_len: u8,
    loam_capability: [u8; 255],
    loam_capability_len: u8,
    /// Its chains, as last read for an attach.
    cap_text: [u8; CAP_TEXT_MAX],
    issuer: [u8; sk::MAX_ID],
    issuer_len: u8,
    device: [u8; sk::MAX_ID],
    device_len: u8,
    key_label: [u8; MAX_LABEL],
    key_label_len: u8,
    device_suite: u32,
    nbd_port_base: u32,
    nbd_index_base: u32,
    lease_ttl_ms: u32,
    lifetime_s: u32,
    block_size: u32,
    unit_size: u32,
    journal_units: u32,
    timeout_ms: u32,
    clock_unchecked: u8,
    cert: [u8; MAX_CRED],
    cert_len: u16,
    key: [u8; MAX_CRED],
    key_len: u16,
    trust: [u8; MAX_CRED],
    trust_len: u16,
    verify_hostname: [u8; 128],
    verify_hostname_len: u8,

    device_key: issuer_key::IssuerKey,
    sign_scratch: [u8; sk::MAX_PROOF_INPUT + issuer_key::SIGN_SCRATCH_OVERHEAD],

    // The effect in flight.
    op: i64,
    slot: u8,
    started: u64,
    epoch: u32,
    held: [u8; REC],
    held_len: u16,
    /// Results the outcome reports.
    r_inst: [u8; 24],
    r_inst_len: u8,
    r_dev: [u8; 24],
    r_dev_len: u8,
    r_exp: i64,
    r_n: i64,
    r_1: [u8; 16],
    r_1_len: u8,
    proc_h: i32,
    drain: [u8; 512],
    attach: KagiAttach,
    /// The storage-key exchange in flight, through the HTTP client.
    call: Call,
    kin: [u8; REC],
    body: [u8; svc::MAX_REQUEST + 16],
    flxa: [u8; FLXA_CAP],

    inst: [Inst; MAX_INST],
    gen: u16,
    reply: [u8; REC],
    reply_len: u16,
    dropped: u32,
}

mod params_def {
    use super::p_u32;
    use super::p_u8;
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        // Loam's admin gate, `host:port`, which every volume's subgraph
        // dials under this node's certificate.
        1, loam_authority, str, 0
            => |s, d, len| { super::set_str(&mut s.loam_authority, &mut s.loam_authority_len, d, len); };

        // The storage-key issuer every proof is addressed to (its `iss`).
        2, issuer, str, 0
            => |s, d, len| { super::set_str(&mut s.issuer, &mut s.issuer_len, d, len); };

        // This node's Kagi device identity.
        3, device, str, 0
            => |s, d, len| { super::set_str(&mut s.device, &mut s.device_len, d, len); };

        // The vault label of this node's enrolled device key.
        4, device_key_label, str, 0
            => |s, d, len| { super::set_str(&mut s.key_label, &mut s.key_label_len, d, len); };

        // The device key's credential suite: 1 ES256, 2 Ed25519.
        5, device_suite, u32, 2
            => |s, d, len| { s.device_suite = p_u32(d, len, 0, 2); };

        // The first NBD port and /dev/nbd index this node's volumes take.
        6, nbd_port_base, u32, 10809
            => |s, d, len| { s.nbd_port_base = p_u32(d, len, 0, 10809); };
        7, nbd_index_base, u32, 0
            => |s, d, len| { s.nbd_index_base = p_u32(d, len, 0, 0); };

        8, lease_ttl_ms, u32, 30000
            => |s, d, len| { s.lease_ttl_ms = p_u32(d, len, 0, 30000); };

        // The attachment handle's lifetime asked of Kagi, seconds: within
        // the grant's.
        9, lifetime_s, u32, 3600
            => |s, d, len| { s.lifetime_s = p_u32(d, len, 0, 3600); };

        // 10..17 are the extended credential tags; the schema skips them.
        18, block_size, u32, 4096
            => |s, d, len| { s.block_size = p_u32(d, len, 0, 4096); };
        19, unit_size, u32, 4096
            => |s, d, len| { s.unit_size = p_u32(d, len, 0, 4096); };
        20, journal_units, u32, 64
            => |s, d, len| { s.journal_units = p_u32(d, len, 0, 64); };
        21, timeout_ms, u32, 30000
            => |s, d, len| { s.timeout_ms = p_u32(d, len, 0, 30000); };
        22, clock_unchecked, u8, 0
            => |s, d, len| { s.clock_unchecked = p_u8(d, len, 0, 0); };

        // The trust anchors the volume's subgraph verifies Loam under:
        // `${file:<der>}`. The builder resolves it into the extended
        // credential entries this module reads; the schema entry only
        // admits the key.
        23, trust, str, 0
            => |_s, _d, _len| {};

        // The capability file every volume's subgraph presents to Loam's
        // gate: this node's, as the operator minted it.
        24, loam_capability, str, 0
            => |s, d, len| { super::set_str(&mut s.loam_capability, &mut s.loam_capability_len, d, len); };
    }
}

/// The node's capability file into `cap_text`: its length; `Err(true)` when
/// the file system answers "not yet" (asked again with the attach),
/// `Err(false)` when it is missing, empty or larger than `CAP_TEXT_MAX`.
unsafe fn read_capability(s: &mut State, sys: &SyscallTable) -> Result<usize, bool> {
    let mut path = [0u8; 256];
    let pl = s.loam_capability_len as usize;
    path[..pl].copy_from_slice(&s.loam_capability[..pl]);
    let fd = (sys.provider_call)(-1, FS_OPEN, path.as_mut_ptr(), pl);
    if fd == FS_EAGAIN {
        return Err(true);
    }
    if fd < 0 {
        return Err(false);
    }
    let mut filled = 0usize;
    let mut again = false;
    while filled < CAP_TEXT_MAX {
        let n = (sys.provider_call)(fd, FS_READ, s.cap_text.as_mut_ptr().add(filled), CAP_TEXT_MAX - filled);
        if n == FS_EAGAIN {
            again = true;
            break;
        }
        if n <= 0 {
            break;
        }
        filled += n as usize;
    }
    // One byte past the ceiling: a file that has it is refused, not cut.
    let mut probe = [0u8; 1];
    let over = !again && filled == CAP_TEXT_MAX && (sys.provider_call)(fd, FS_READ, probe.as_mut_ptr(), 1) > 0;
    let mut z = [0u8; 4];
    (sys.provider_call)(fd, FS_CLOSE, z.as_mut_ptr(), 0);
    if again {
        return Err(true);
    }
    if over || filled == 0 {
        return Err(false);
    }
    Ok(filled)
}

unsafe fn set_str(dst: &mut [u8], dst_len: &mut u8, d: *const u8, len: usize) {
    let n = len.min(dst.len()).min(255);
    dst[..n].copy_from_slice(core::slice::from_raw_parts(d, n));
    *dst_len = n as u8;
}

/// The extended entries after the schema's end marker, as the config
/// builder lays them for any module: `cert_file` (10), `key_file` (11),
/// `trust` (12), `verify_hostname` (13), `[tag][0x00][len BE][payload]`.
unsafe fn parse_credentials(s: &mut State, params: *const u8, params_len: usize) {
    if params.is_null() || params_len < 4 {
        return;
    }
    let data = core::slice::from_raw_parts(params, params_len);
    let mut pos = 0usize;
    if data[0] == 0xFE && data[1] == 0x01 {
        let basic_end = 4 + (((data[3] as usize) << 8) | data[2] as usize);
        if basic_end <= params_len {
            pos = basic_end;
        }
    }
    while pos + 4 <= params_len {
        let tag = data[pos];
        if matches!(tag, 10..=13) && data[pos + 1] == 0 {
            let len = ((data[pos + 2] as usize) << 8) | data[pos + 3] as usize;
            let at = pos + 4;
            if at + len > params_len {
                break;
            }
            let v = &data[at..at + len];
            match tag {
                10 if len <= MAX_CRED => {
                    s.cert[..len].copy_from_slice(v);
                    s.cert_len = len as u16;
                }
                11 if len <= MAX_CRED => {
                    s.key[..len].copy_from_slice(v);
                    s.key_len = len as u16;
                }
                12 if len <= MAX_CRED => {
                    s.trust[..len].copy_from_slice(v);
                    s.trust_len = len as u16;
                }
                13 if len <= s.verify_hostname.len() => {
                    s.verify_hostname[..len].copy_from_slice(v);
                    s.verify_hostname_len = len as u8;
                }
                _ => {}
            }
            pos = at + len;
        } else {
            pos += 1;
        }
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

fn decimal_u32(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 10 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    u32::try_from(v).ok()
}

fn write_decimal(v: u64, out: &mut [u8]) -> usize {
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
    let n = (tmp.len() - i).min(out.len());
    out[..n].copy_from_slice(&tmp[i..i + n]);
    n
}

fn unhex16(h: &[u8]) -> Option<[u8; 16]> {
    if h.len() != 32 {
        return None;
    }
    let v = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    let mut out = [0u8; 16];
    for (i, o) in out.iter_mut().enumerate() {
        *o = (v(h[2 * i])? << 4) | v(h[2 * i + 1])?;
    }
    Some(out)
}

fn hex(b: &[u8], out: &mut [u8]) -> usize {
    const D: &[u8; 16] = b"0123456789abcdef";
    let n = b.len().min(out.len() / 2);
    for i in 0..n {
        out[2 * i] = D[(b[i] >> 4) as usize];
        out[2 * i + 1] = D[(b[i] & 15) as usize];
    }
    2 * n
}

/// The first 16 bytes of sha256 over the parts.
fn digest16(parts: &[&[u8]]) -> [u8; 16] {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    let d = h.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&d[..16]);
    out
}

/// The Kagi resource of a volume: sha256(name)[..16].
fn resource_of(vol: &[u8]) -> [u8; 16] {
    digest16(&[vol])
}

/// `<root>:<path>` split at its first ':'.
fn split_vol(vol: &[u8]) -> Option<(&[u8], &[u8])> {
    let at = vol.iter().position(|&b| b == b':')?;
    let (root, path) = (&vol[..at], &vol[at + 1..]);
    (!root.is_empty() && path.first() == Some(&b'/')).then_some((root, path))
}

// ---- the device key ----

struct Signer<'a> {
    sys: &'a SyscallTable,
    key: &'a issuer_key::IssuerKey,
    scratch: &'a mut [u8],
}

impl DeviceKey for Signer<'_> {
    fn sign(&mut self, input: &[u8], signature: &mut [u8]) -> Option<usize> {
        // ES256 signs a digest, Ed25519 the message: as Kagi verifies.
        let digest;
        let m: &[u8] = if self.key.suite() == auth_wire::suite::ES256 {
            digest = sha256(input);
            &digest
        } else {
            input
        };
        unsafe { self.key.sign(self.sys, self.scratch, m, signature) }
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
/// instance arena and syscall table; `params` is the config TLV blob.
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
    s.request_out = -1;
    s.response_in = -1;
    s.call.reset();
    s.proc_h = -1;
    s.device_key = issuer_key::IssuerKey::empty();
    s.attach = KagiAttach::empty();
    s.inst = [NO_INST; MAX_INST];
    params_def::set_defaults(s);
    params_def::parse_tlv(s, params, params_len);
    parse_credentials(s, params, params_len);
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
            s.resolved = 1;
        }
        if s.configured == 0 {
            configure(s, sys);
        }
        if s.configured != 1 || s.request_in < 0 || s.response_out < 0 {
            return 0;
        }
        drain_lanes(s, sys);
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

/// Open the device key (generated in the vault on first open, persisted)
/// and say its public half, which enrolment binds; check the params.
unsafe fn configure(s: &mut State, sys: &SyscallTable) {
    let suite = s.device_suite as u16;
    let ok = s.loam_authority_len != 0
        && s.loam_capability_len != 0
        && s.issuer_len != 0
        && s.device_len != 0
        && s.key_label_len != 0
        && s.cert_len != 0
        && s.key_len != 0
        && s.trust_len != 0
        && matches!(suite, auth_wire::suite::ED25519 | auth_wire::suite::ES256);
    if !ok {
        log(sys, 1, b"[volume_host] FAULT: loam_authority, loam_capability, issuer, device, device_key_label, cert_file, key_file and trust are required");
        s.configured = 2;
        return;
    }
    let label = &s.key_label[..s.key_label_len as usize];
    if !s.device_key.open(sys, suite, label) {
        // No durable vault yet: asked again next step.
        return;
    }
    let public = s.device_key.public_key();
    if !s.attach.configure(
        &s.issuer[..s.issuer_len as usize],
        &s.device[..s.device_len as usize],
        suite,
        public,
    ) {
        s.configured = 2;
        return;
    }
    let mut m = [0u8; 40 + 2 * 65];
    let head = b"[volume_host] device key ";
    m[..head.len()].copy_from_slice(head);
    let n = hex(public, &mut m[head.len()..]);
    log(sys, 3, &m[..head.len() + n]);
    s.configured = 1;
}

/// Take what every live instance's lanes carry: the latest recipient and
/// status.
unsafe fn drain_lanes(s: &mut State, sys: &SyscallTable) {
    for i in 0..MAX_INST {
        let inst = &mut s.inst[i];
        if !inst.used {
            continue;
        }
        if inst.recipient_in >= 0 {
            let mut buf = [0u8; sk::RECIPIENT_LEN];
            loop {
                let poll = (sys.channel_poll)(inst.recipient_in, POLL_IN);
                if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
                    break;
                }
                let n = (sys.channel_read)(inst.recipient_in, buf.as_mut_ptr(), buf.len());
                if n != sk::RECIPIENT_LEN as i32 {
                    break;
                }
                if sk::recipient_public(&buf).is_some() {
                    inst.recipient = buf;
                    inst.has_recipient = true;
                }
            }
        }
        if inst.status_in >= 0 {
            let mut buf = [0u8; STATUS_LEN];
            loop {
                let poll = (sys.channel_poll)(inst.status_in, POLL_IN);
                if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
                    break;
                }
                let n = (sys.channel_read)(inst.status_in, buf.as_mut_ptr(), buf.len());
                if n != STATUS_LEN as i32 {
                    break;
                }
                if &buf[..4] == b"FXST" {
                    inst.status = buf;
                    inst.has_status = true;
                }
            }
        }
    }
}

/// The instance an `inst` argument names: `<slot>.<generation>`.
fn find_inst(s: &State, name: &[u8]) -> Option<usize> {
    let dot = name.iter().position(|&b| b == b'.')?;
    let slot = decimal_u32(&name[..dot])? as usize;
    let gen = decimal_u32(&name[dot + 1..])?;
    let inst = s.inst.get(slot)?;
    (inst.used && u32::from(inst.gen) == gen).then_some(slot)
}

fn inst_name(slot: usize, gen: u16, out: &mut [u8; 24]) -> usize {
    let mut n = write_decimal(slot as u64, out);
    out[n] = b'.';
    n += 1;
    n + write_decimal(u64::from(gen), &mut out[n..])
}

/// Take one record and start the effect it names.
unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    let Some(len) = take_record(s, sys) else {
        return;
    };
    s.held_len = len as u16;
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), len);
    s.op = frame_i64(rec, 2);
    if !ours(s.op) {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    s.busy = 1;
    s.wait = W_NONE;
    s.started = dev_millis(sys);
    s.r_inst_len = 0;
    s.r_dev_len = 0;
    s.r_exp = 0;
    s.r_n = 0;
    s.r_1_len = 0;
    let a1 = frame_bytes(rec, 4);
    if s.op == OP_GRAPH_START {
        start_graph(s, sys, rec);
        return;
    }
    let Some(slot) = find_inst(s, a1) else {
        // An instance this node no longer has: nothing to flush, stop or
        // destroy; anything else cannot run.
        let gone = matches!(s.op, OP_GRAPH_DESTROY | OP_NBD_STOP | OP_FLUSH);
        answer(s, sys, if gone { Outcome::Ok } else { Outcome::Failed });
        return;
    };
    s.slot = slot as u8;
    match s.op {
        OP_RECIPIENT | OP_CB_RECIPIENT => {
            if s.op == OP_CB_RECIPIENT {
                s.inst[slot].has_recipient = false;
                if !control(s, sys, slot, b"FXRT") {
                    answer(s, sys, Outcome::Unavailable);
                    return;
                }
            }
            s.wait = W_RECIPIENT;
            pump(s, sys);
        }
        OP_KAGI_RELEASE | OP_KAGI_RENEW | OP_KAGI_RECOVER => start_kagi(s, sys, rec, slot),
        OP_BUNDLE_DELIVER | OP_CB_BUNDLE => {
            let want = if s.op == OP_CB_BUNDLE {
                decimal_u32(frame_bytes(rec, 5)).unwrap_or(0)
            } else {
                s.inst[slot].bundle_epoch
            };
            let inst = &s.inst[slot];
            if !inst.has_bundle || inst.bundle_epoch != want {
                answer(s, sys, Outcome::Failed);
                return;
            }
            s.epoch = want;
            let bundle = inst.bundle;
            let ctrl = inst.ctrl_out;
            if ctrl < 0
                || (sys.channel_write)(ctrl, bundle.as_ptr(), bundle.len()) != bundle.len() as i32
            {
                answer(s, sys, Outcome::Unavailable);
                return;
            }
            // Delivered: the bundle is the instance's now, and spent here.
            let inst = &mut s.inst[slot];
            inst.has_bundle = false;
            inst.bundle = [0; sk::BUNDLE_LEN];
            inst.has_status = false;
            s.wait = W_BUNDLE;
            pump(s, sys);
        }
        OP_CB_STATUS => {
            let want = decimal_u32(frame_bytes(rec, 5)).unwrap_or(0);
            cb_status(s, sys, slot, want);
        }
        OP_CB_ERASE => {
            s.inst[slot].has_status = false;
            if !control(s, sys, slot, b"FXER") {
                answer(s, sys, Outcome::Unavailable);
                return;
            }
            s.wait = W_ERASE;
            pump(s, sys);
        }
        OP_NBD_START => {
            let inst = s.inst[slot];
            if inst.published {
                set_dev(s, inst.nbd_index);
                answer(s, sys, Outcome::Ok);
                return;
            }
            let mut port = [0u8; 10];
            let pn = write_decimal(u64::from(s.nbd_port_base) + slot as u64, &mut port);
            let mut dev = [0u8; 24];
            let dn = dev_path(inst.nbd_index, &mut dev);
            let mut bs = [0u8; 10];
            let bn = write_decimal(u64::from(s.block_size), &mut bs);
            spawn(
                s,
                sys,
                &[
                    b"nbd-client",
                    b"-b",
                    &bs[..bn],
                    // A server that stops answering fails the device's I/O
                    // with EIO rather than holding it, and everything
                    // touching the mount, in uninterruptible sleep.
                    b"-timeout",
                    b"30",
                    b"127.0.0.1",
                    &port[..pn],
                    &dev[..dn],
                ],
            );
        }
        OP_FLUSH => {
            let inst = s.inst[slot];
            if !inst.published {
                answer(s, sys, Outcome::Ok);
                return;
            }
            let mut dev = [0u8; 24];
            let dn = dev_path(inst.nbd_index, &mut dev);
            spawn(s, sys, &[b"blockdev", b"--flushbufs", &dev[..dn]]);
        }
        OP_NBD_STOP => {
            let inst = s.inst[slot];
            if !inst.published {
                answer(s, sys, Outcome::Ok);
                return;
            }
            let mut dev = [0u8; 24];
            let dn = dev_path(inst.nbd_index, &mut dev);
            spawn(s, sys, &[b"nbd-client", b"-d", &dev[..dn]]);
        }
        OP_GRAPH_DESTROY => {
            destroy(s, sys, slot);
            answer(s, sys, Outcome::Ok);
        }
        _ => answer(s, sys, Outcome::Failed),
    }
}

fn dev_path(index: u8, out: &mut [u8; 24]) -> usize {
    let head = b"/dev/nbd";
    out[..head.len()].copy_from_slice(head);
    head.len() + write_decimal(u64::from(index), &mut out[head.len()..])
}

fn set_dev(s: &mut State, index: u8) {
    let mut d = [0u8; 24];
    let n = dev_path(index, &mut d);
    s.r_dev[..n].copy_from_slice(&d[..n]);
    s.r_dev_len = n as u8;
}

/// A four-byte control record and four zero bytes, on the control input.
unsafe fn control(s: &mut State, sys: &SyscallTable, slot: usize, magic: &[u8; 4]) -> bool {
    let ctrl = s.inst[slot].ctrl_out;
    let mut r = [0u8; 8];
    r[..4].copy_from_slice(magic);
    ctrl >= 0 && (sys.channel_write)(ctrl, r.as_ptr(), r.len()) == 8
}

/// `GRAPH_START`: the volume's subgraph, staged as a workload under its own
/// owner, started, and its lanes read back.
unsafe fn start_graph(s: &mut State, sys: &SyscallTable, rec: &[u8]) {
    let claim = frame_bytes(rec, 3);
    let vol = frame_bytes(rec, 4);
    let fresh = frame_bytes(rec, 5) == b"1";
    let recovery = frame_bytes(rec, 8) == b"recovery";
    let fence = frame_i64(rec, 9);
    let Some((root, path)) = split_vol(vol) else {
        answer(s, sys, Outcome::Failed);
        return;
    };
    if claim.is_empty() || claim.len() > MAX_CLAIM || fence < 0 {
        answer(s, sys, Outcome::Failed);
        return;
    }
    // One instance per claim: asked again, the live one answers.
    for i in 0..MAX_INST {
        let inst = &s.inst[i];
        if inst.used && &inst.claim[..inst.claim_len as usize] == claim {
            let gen = inst.gen;
            let n = inst_name(i, gen, &mut s.r_inst);
            s.r_inst_len = n as u8;
            answer(s, sys, Outcome::Ok);
            return;
        }
    }
    let Some(slot) = s.inst.iter().position(|i| !i.used) else {
        answer(s, sys, Outcome::Unavailable);
        return;
    };
    // The lease holder the attach flow acquired under: the claim's, or the
    // recovery's own owner `rc:<claim>`.
    let holder = if recovery {
        digest16(&[b"rc:", claim])
    } else {
        digest16(&[claim])
    };
    let mut holder_hex = [0u8; 32];
    hex(&holder, &mut holder_hex);
    s.gen = s.gen.wrapping_add(1).max(1);
    let gen = s.gen;
    let mut g = [0u8; 2];
    g.copy_from_slice(&gen.to_le_bytes());
    let owner = digest16(&[b"volume_host:", claim, b":", &g]);
    // The capability's chains, read now (a rotated file holds from the next
    // attach on) and handed to the subgraph: it cannot read the file itself.
    let cap_len = match read_capability(s, sys) {
        Ok(n) => n,
        Err(true) => {
            answer(s, sys, Outcome::Unavailable); // the file system: not yet
            return;
        }
        Err(false) => {
            log(sys, 1, b"[volume_host] the Loam capability file cannot be read, or is empty or too large");
            answer(s, sys, Outcome::Failed);
            return;
        }
    };
    let st = s as *mut State;
    let cap_text: &[u8] = &(&(*st).cap_text)[..cap_len];
    let graph = VolumeGraph {
        owner_uid: owner,
        state_cap: 0,
        loam_authority: &s.loam_authority[..s.loam_authority_len as usize],
        capability: cap_text,
        verify_hostname: &s.verify_hostname[..s.verify_hostname_len as usize],
        trust_der: &s.trust[..s.trust_len as usize],
        cert_der: &s.cert[..s.cert_len as usize],
        key_der: &s.key[..s.key_len as usize],
        clock_unchecked: s.clock_unchecked != 0,
        root,
        path,
        holder_hex: &holder_hex,
        fence: fence as u64,
        lease_ttl_ms: s.lease_ttl_ms,
        format: fresh,
        block_size: s.block_size,
        unit_size: s.unit_size,
        journal_units: s.journal_units,
        nbd_port: (s.nbd_port_base as u16).wrapping_add(slot as u16),
        export: &[],
    };
    // [header][FLXA]
    let hdr = wl::CREATE_HEADER_SIZE;
    let Some(n) = encode(&graph, &mut s.flxa[hdr..]) else {
        answer(s, sys, Outcome::Failed);
        return;
    };
    let h = &mut s.flxa[..hdr];
    h.fill(0);
    h[0..16].copy_from_slice(&owner);
    h[16] = wl::POSTURE_SHARED;
    h[17] = wl::SOURCE_FMOD_GRAPH;
    h[18] = wl::NET_ISO_SHARED;
    h[40..42].copy_from_slice(&(n as u16).to_le_bytes());
    // A graph backend stages the subgraph over several calls (EINPROGRESS):
    // the same spec is asked again until it answers; four modules are one
    // batch. An identity still held (EEXIST: an attach of this generation
    // that never finished) is destroyed by identity and asked once more.
    let mut handle = -1;
    for _ in 0..2 {
        for _ in 0..16 {
            handle = (sys.provider_call)(-1, wl::CREATE, s.flxa.as_mut_ptr(), hdr + n);
            if handle != WL_EINPROGRESS {
                break;
            }
        }
        if handle != WL_EEXIST {
            break;
        }
        let mut id = owner;
        (sys.provider_call)(-1, wl::DESTROY, id.as_mut_ptr(), id.len());
    }
    if handle < 0 {
        log(
            sys,
            1,
            b"[volume_host] workload CREATE refused the volume graph",
        );
        answer(s, sys, Outcome::Failed);
        return;
    }
    if (sys.provider_call)(handle, wl::START, core::ptr::null_mut(), 0) < 0 {
        (sys.provider_call)(handle, wl::DESTROY, core::ptr::null_mut(), 0);
        answer(s, sys, Outcome::Failed);
        return;
    }
    let mut lanes = [0u8; 1 + wl::MAX_CALLER_LANES * 9];
    let ln = (sys.provider_call)(handle, wl::LANES, lanes.as_mut_ptr(), lanes.len());
    let mut inst = NO_INST;
    inst.used = true;
    inst.gen = gen;
    inst.claim[..claim.len()].copy_from_slice(claim);
    inst.claim_len = claim.len() as u8;
    inst.handle = handle;
    inst.nbd_index = (s.nbd_index_base as u8).wrapping_add(slot as u8);
    if ln > 0 {
        for i in 0..lanes[0] as usize {
            let o = 1 + i * 9;
            let to = i32::from_le_bytes(lanes[o + 1..o + 5].try_into().unwrap());
            let from = i32::from_le_bytes(lanes[o + 5..o + 9].try_into().unwrap());
            match lanes[o] {
                CALLER_RECIPIENT => inst.recipient_in = to,
                CALLER_STATUS => inst.status_in = to,
                CALLER_BUNDLE => inst.ctrl_out = from,
                _ => {}
            }
        }
    }
    if inst.recipient_in < 0 || inst.status_in < 0 || inst.ctrl_out < 0 {
        (sys.provider_call)(handle, wl::DESTROY, core::ptr::null_mut(), 0);
        log(
            sys,
            1,
            b"[volume_host] the volume graph's lanes are missing",
        );
        answer(s, sys, Outcome::Failed);
        return;
    }
    s.inst[slot] = inst;
    let n = inst_name(slot, gen, &mut s.r_inst);
    s.r_inst_len = n as u8;
    answer(s, sys, Outcome::Ok);
}

/// `GRAPH_DESTROY`: the workload goes, and with it crypt_block's recipient
/// and volume-key handles. What was released and not delivered is dropped.
unsafe fn destroy(s: &mut State, sys: &SyscallTable, slot: usize) {
    let h = s.inst[slot].handle;
    if h >= 0 {
        (sys.provider_call)(h, wl::DESTROY, core::ptr::null_mut(), 0);
    }
    s.inst[slot] = NO_INST;
}

/// `CB_STATUS`: where crypt_block's rotation toward `want` stands.
unsafe fn cb_status(s: &mut State, sys: &SyscallTable, slot: usize, want: u32) {
    let inst = &s.inst[slot];
    if !inst.has_status {
        answer(s, sys, Outcome::Unavailable);
        return;
    }
    let st = &inst.status;
    let epoch = u32::from_le_bytes(st[8..12].try_into().unwrap());
    let name: &[u8] = match st[5] {
        0 if epoch >= want => b"steady",
        0 => b"none",
        1 => b"prepared",
        2 => b"active",
        3 => b"migrating",
        4 => b"retiring",
        _ => b"none",
    };
    s.r_1[..name.len()].copy_from_slice(name);
    s.r_1_len = name.len() as u8;
    answer(s, sys, Outcome::Ok);
}

/// `KAGI_RELEASE`, `KAGI_RENEW`, `KAGI_RECOVER`: the attach client's job,
/// over the instance's announced recipient.
unsafe fn start_kagi(s: &mut State, sys: &SyscallTable, rec: &[u8], slot: usize) {
    let vol = frame_bytes(rec, 5);
    let device = frame_bytes(rec, 6);
    let kvol = frame_bytes(rec, 8);
    let fence = frame_i64(rec, 9);
    let epoch = match frame_bytes(rec, 7) {
        b"" => Some(1),
        e => decimal_u32(e),
    };
    let inst = s.inst[slot];
    let (Some(epoch), true, true) = (
        epoch,
        inst.has_recipient,
        device == &s.device[..s.device_len as usize],
    ) else {
        answer(s, sys, Outcome::Failed);
        return;
    };
    if fence < 0 || vol.is_empty() {
        answer(s, sys, Outcome::Failed);
        return;
    }
    let (job, ticket, key_vol) = match s.op {
        OP_KAGI_RECOVER => match unhex16(kvol) {
            Some(t) => (AttachJob::Recover, t, vol),
            None => {
                answer(s, sys, Outcome::Failed);
                return;
            }
        },
        // A clone's key resource is its source's.
        OP_KAGI_RENEW => (
            AttachJob::Renew,
            [0; 16],
            if kvol.is_empty() { vol } else { kvol },
        ),
        _ => (
            AttachJob::Attach,
            [0; 16],
            if kvol.is_empty() { vol } else { kvol },
        ),
    };
    if job == AttachJob::Renew && inst.chain_len == 0 {
        answer(s, sys, Outcome::Failed);
        return;
    }
    s.epoch = epoch;
    let f = AttachFacts {
        resource: resource_of(key_vol),
        epoch,
        fence: fence as u64,
        lifetime_secs: s.lifetime_s,
        recipient: &inst.recipient,
        ticket,
        renewed: &inst.chain[..inst.chain_len as usize],
    };
    let next = s.attach.begin(job, &f, &mut s.body);
    s.wait = W_KAGI;
    kagi_next(s, sys, next);
}

unsafe fn kagi_next(s: &mut State, sys: &SyscallTable, next: AttachNext) {
    match next {
        AttachNext::Send(n) => {
            let port = s.request_out;
            let body = core::slice::from_raw_parts(s.body.as_ptr(), n);
            // `POST /storage-key` on the client's pinned authority: the
            // REQUEST payload is the whole body, inline.
            let opened = s.call.begin(port, |id, out| {
                call_head(id, exch::METHOD_POST, KAGI_PATH, b"", body, out)
            });
            if !opened {
                answer(s, sys, Outcome::Failed);
                return;
            }
            send_kagi(s, sys);
        }
        AttachNext::Done(code) => kagi_done(s, sys, code),
    }
}

/// Place what the call owes the HTTP client: its request (first sent, or
/// asked again after the client's `LINK UP`), or the ABORT for a call given
/// up on.
unsafe fn send_kagi(s: &mut State, sys: &SyscallTable) {
    if s.request_out < 0 {
        return;
    }
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

/// The endpoint's answer through the HTTP client: a 200 whose body is the
/// storage-key REPLY payload, or an exchange that ended without one.
unsafe fn kagi_reply(s: &mut State, sys: &SyscallTable) {
    send_kagi(s, sys);
    if s.response_in < 0 {
        return;
    }
    loop {
        let poll = (sys.channel_poll)(s.response_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.response_in, s.kin.as_mut_ptr(), REC);
        if n <= 0 {
            return;
        }
        let rec = core::slice::from_raw_parts(s.kin.as_ptr(), n as usize);
        match s.call.take(rec) {
            CallEvent::Answered => break,
            CallEvent::Failed(status) => {
                answer(s, sys, unanswered(status));
                return;
            }
            CallEvent::Nothing | CallEvent::LinkDown | CallEvent::LinkUp => {}
        }
    }
    let (status, body) = s.call.answer();
    if status != exch::status::OK || body.is_empty() {
        answer(s, sys, unanswered(status));
        return;
    }
    let mut payload = [0u8; svc::MAX_FRAME];
    let pn = body.len().min(payload.len());
    payload[..pn].copy_from_slice(&body[..pn]);
    let now = wall_ms(sys);
    let mut anti = [0u8; 16];
    dev_csprng_fill(sys, anti.as_mut_ptr(), 16);
    let mut signer = Signer {
        sys,
        key: &s.device_key,
        scratch: &mut s.sign_scratch,
    };
    let next = s
        .attach
        .on_reply(&payload[..pn], &mut signer, now, anti, &mut s.body);
    kagi_next(s, sys, next);
}

/// An exchange that ended without the endpoint's REPLY: worth asking again
/// when the endpoint, or the path to it, was busy or gone; a failure when it
/// refused the request itself.
fn unanswered(status: u16) -> Outcome {
    if status >= 500 {
        Outcome::Unavailable
    } else {
        Outcome::Failed
    }
}

/// Wall-clock Unix ms when the platform vouches for its clock for a
/// credential window, as Kagi's own modules ask; else 0.
unsafe fn wall_ms(sys: &SyscallTable) -> u64 {
    let obs = dev_trusted_unix(sys);
    if time_policy::now_for(time_policy::Decision::CredentialWindow, &obs).is_none() {
        return 0;
    }
    dev_unix_millis(sys)
}

unsafe fn kagi_done(s: &mut State, sys: &SyscallTable, code: AttachCode) {
    let slot = s.slot as usize;
    if code == AttachCode::Ok {
        let inst = &mut s.inst[slot];
        let record = s.attach.record();
        inst.chain[..record.len()].copy_from_slice(record);
        inst.chain_len = record.len() as u16;
        let bundle = s.attach.bundle();
        if bundle.len() == sk::BUNDLE_LEN {
            inst.bundle.copy_from_slice(bundle);
            inst.bundle_epoch = s.epoch;
            inst.has_bundle = true;
            // The recipient it was sealed to is spent.
            inst.has_recipient = false;
        }
        // On this node's monotonic clock, as the pipeline compares it.
        let wall = wall_ms(sys);
        let left = s.attach.expiry_ms.saturating_sub(wall);
        s.r_exp = (dev_millis(sys).saturating_add(left)) as i64;
        if s.op == OP_KAGI_RECOVER {
            s.r_n = 2;
        }
    }
    let out = match code {
        AttachCode::Ok => Outcome::Ok,
        AttachCode::Refused => Outcome::Refused,
        AttachCode::Custodians => Outcome::Custodians,
        AttachCode::FenceChanged => Outcome::FenceChanged,
        AttachCode::Expired => Outcome::Expired,
        AttachCode::Conflict => Outcome::Conflict,
        AttachCode::Failed => Outcome::Failed,
        AttachCode::Unavailable => Outcome::Unavailable,
    };
    if code != AttachCode::Ok && s.attach.refusal != 0 {
        let mut m = *b"[volume_host] op 00 refused: Kagi refusal 000";
        let at = m.len() - 3;
        m[17] = b'0' + (s.op / 10 % 10) as u8;
        m[18] = b'0' + (s.op % 10) as u8;
        let r = s.attach.refusal;
        m[at] = b'0' + r / 100;
        m[at + 1] = b'0' + r / 10 % 10;
        m[at + 2] = b'0' + r % 10;
        log(sys, 1, &m);
    }
    answer(s, sys, out);
}

/// Advance the effect in flight.
unsafe fn pump(s: &mut State, sys: &SyscallTable) {
    let slot = s.slot as usize;
    match s.wait {
        W_RECIPIENT => {
            if s.inst[slot].has_recipient {
                answer(s, sys, Outcome::Ok);
                return;
            }
        }
        W_BUNDLE => {
            let inst = &s.inst[slot];
            if inst.has_status {
                let st = &inst.status;
                let state = st[4];
                let errno = i32::from_le_bytes(st[20..24].try_into().unwrap());
                let awaited = u32::from_le_bytes(st[16..20].try_into().unwrap());
                let out = match state {
                    ST_READY | ST_ROTATING if errno == 0 => Some(Outcome::Ready),
                    ST_AWAITING | ST_READY | ST_ROTATING if errno == EACCES => {
                        Some(Outcome::Eacces)
                    }
                    // A bundle for an epoch the container no longer needs.
                    ST_READY | ST_ROTATING | ST_AWAITING
                        if errno != 0 && awaited != s.epoch && s.op == OP_CB_BUNDLE =>
                    {
                        Some(Outcome::Stale)
                    }
                    ST_FAILED if errno == EACCES => Some(Outcome::Eacces),
                    ST_FAILED => Some(Outcome::Failed),
                    _ => None,
                };
                if let Some(o) = out {
                    answer(s, sys, o);
                    return;
                }
            }
        }
        W_ERASE => {
            let inst = &s.inst[slot];
            if inst.has_status && inst.status[4] == ST_ERASED {
                answer(s, sys, Outcome::Ok);
                return;
            }
            if inst.has_status && inst.status[4] == ST_FAILED {
                answer(s, sys, Outcome::Failed);
                return;
            }
        }
        W_KAGI => kagi_reply(s, sys),
        W_TOOL => {
            poll_tool(s, sys);
            return;
        }
        _ => {}
    }
    if s.busy != 0 && dev_millis(sys).saturating_sub(s.started) > u64::from(s.timeout_ms) {
        if s.proc_h >= 0 {
            (sys.provider_call)(s.proc_h, hp::PROC_CLOSE, core::ptr::null_mut(), 0);
            s.proc_h = -1;
        }
        answer(s, sys, Outcome::Unavailable);
    }
}

/// Run a tool through the process executor; its exit decides the effect.
unsafe fn spawn(s: &mut State, sys: &SyscallTable, words: &[&[u8]]) {
    let mut cmd = [0u8; CMD];
    let mut n = 0usize;
    for w in words {
        if n + w.len() + 1 > CMD {
            answer(s, sys, Outcome::Failed);
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
        log(
            sys,
            1,
            b"[volume_host] a tool did not start (the node's allowlist?)",
        );
        answer(s, sys, Outcome::Failed);
        return;
    }
    s.proc_h = h;
    s.wait = W_TOOL;
}

unsafe fn poll_tool(s: &mut State, sys: &SyscallTable) {
    let _ = (sys.provider_call)(s.proc_h, hp::PROC_READ, s.drain.as_mut_ptr(), s.drain.len());
    let mut code = [0u8; 4];
    let running = (sys.provider_call)(s.proc_h, hp::PROC_STATUS, code.as_mut_ptr(), 4);
    if running == 1 {
        if dev_millis(sys).saturating_sub(s.started) > u64::from(s.timeout_ms) {
            (sys.provider_call)(s.proc_h, hp::PROC_CLOSE, core::ptr::null_mut(), 0);
            s.proc_h = -1;
            answer(s, sys, Outcome::Unavailable);
        }
        return;
    }
    let _ = (sys.provider_call)(s.proc_h, hp::PROC_CLOSE, core::ptr::null_mut(), 0);
    s.proc_h = -1;
    let exit = if running == 0 {
        i32::from_le_bytes(code)
    } else {
        -1
    };
    let slot = s.slot as usize;
    if exit != 0 {
        answer(s, sys, Outcome::Failed);
        return;
    }
    match s.op {
        OP_NBD_START => {
            s.inst[slot].published = true;
            let idx = s.inst[slot].nbd_index;
            set_dev(s, idx);
        }
        OP_NBD_STOP => s.inst[slot].published = false,
        _ => {}
    }
    answer(s, sys, Outcome::Ok);
}

/// Build the outcome record for the held request and end the effect.
unsafe fn answer(s: &mut State, sys: &SyscallTable, o: Outcome) {
    s.busy = 0;
    s.wait = W_NONE;
    // A call still open is given up on: a late answer is not the next
    // effect's, and the client is told.
    s.call.cancel();
    send_kagi(s, sys);
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
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
    let zero = 0i64.to_le_bytes();
    let fits = f(&mut out, 1, TY_I64, &frame_i64(held, 1).to_le_bytes())
        && f(&mut out, 2, TY_I64, &s.op.to_le_bytes())
        && f(&mut out, 3, TY_BYTES, frame_bytes(held, 3))
        && f(&mut out, 4, TY_BYTES, out_name(o))
        && f(&mut out, 5, TY_I64, &zero)
        && f(&mut out, 6, TY_I64, &zero)
        && f(&mut out, 7, TY_I64, &s.r_exp.to_le_bytes())
        && f(&mut out, 8, TY_BYTES, &s.r_inst[..s.r_inst_len as usize])
        && f(&mut out, 9, TY_BYTES, &s.r_dev[..s.r_dev_len as usize])
        && f(&mut out, 10, TY_BYTES, &[])
        && f(&mut out, 11, TY_BYTES, &[])
        && f(&mut out, 12, TY_BYTES, &[])
        && f(&mut out, 13, TY_I64, &s.r_n.to_le_bytes())
        && f(&mut out, 14, TY_BYTES, &s.r_1[..s.r_1_len as usize])
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

unsafe fn flush_reply(s: &mut State, sys: &SyscallTable) {
    let n = s.reply_len as usize;
    if (sys.channel_write)(s.response_out, s.reply.as_ptr(), n) > 0 {
        s.reply_len = 0;
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
