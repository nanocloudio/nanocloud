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
use abi::platform::linux::host_process as hp;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/argv.rs");
// `Got`, `refused` and the rate-limited `ReadNag` of every store read.
const MAX_KEY: usize = 256;
include!("../_shared/head_rev.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
// The moment a run starts, as a record keeps one (`mo_now`).
include!("../_shared/moment.rs");

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

const WL_CREATE_HEADER: usize = 76;
/// `storage_bytes` in the CREATE header: the workload's storage quota (its
/// own files and store, rooted apart from every other owner's); 0 = none set.
const STORAGE_BYTES_AT: usize = 68;
/// The workload an identity names (a caller that lost its handle).
const WL_LOOKUP: u32 = 0x1A12;
const WL_EINPROGRESS: i32 = -36;
const WL_EEXIST: i32 = -17;
const WL_ENOENT: i32 = -2;
/// `LOOKUP` answered, `DESTROY` by identity served (caps ops bit 5).
const CAPS_OP_LOOKUP: u16 = 1 << 5;
/// A CREATE the kernel says is live (`EEXIST`) but `LOOKUP` cannot find is
/// asked again this many times before it is reported failed.
const CREATE_EEXIST_RETRIES: u8 = 8;
const POSTURE_SHARED: u8 = 0;
/// A workload that is a module subgraph (an FLXA blob), realised by the
/// kernel's fmod-graph backend from the node's own modules. A bare-metal
/// node runs these and packs (`SOURCE_FMOD_PACK`), never a host process.
const SOURCE_FMOD_GRAPH: u8 = 0;
const SOURCE_HOST_PROCESS: u8 = 1;
/// A workload that brings its own modules: a pulled workload image (its
/// FLXA layer, and one `.fmod` layer per module), staged by the kernel as
/// the workload's code — each module admitted by its pin, the layer digest.
const SOURCE_FMOD_PACK: u8 = 2;
/// The pack reference ahead of the pins and the FLXA (`workload::pack_ref`).
const PACK_REF: usize = 16;
const PIN: usize = 32;
/// FXMT, as `fluxor build` lays out a module table: a 16-byte header, a
/// 16-byte entry per module, each module placed so its code (past the
/// 80-byte header) starts on a 4 KiB boundary of the table.
const FXMT_MAGIC: u32 = 0x544D_5846;
const FXMT_VERSION: u8 = 1;
const FXMT_HEADER: usize = 16;
const FXMT_ENTRY: usize = 16;
const FMOD_HEADER: usize = 80;
const FMOD_NAME_AT: usize = 28;
const FMOD_NAME_LEN: usize = 32;
const CODE_ALIGN: usize = 4096;
/// Layers of one workload image: its FLXA and up to 32 modules.
const MAX_LAYERS: usize = 33;
/// A pack is assembled in a grant from the kernel's elastic region, taken
/// at the first pack and kept for every one after: grants are not freed one
/// by one, so the node pays only when it runs a workload image, and only
/// for the largest. A grant grows by doubling (the smaller one is not
/// returned), so what it holds stays under twice the largest pack.
const ELASTIC_ALLOC: u32 = 0x0C3F;
const PACK_MIN: usize = 1 << 20;
/// The kernel's pack pool: no larger pack could be staged.
const PACK_MAX: usize = 16 << 20;
/// One read per step: a provider (fat32 on metal) answers within its budget.
const READ_CHUNK: usize = 64 * 1024;
const MANIFESTS_PREFIX: &[u8] = b"/image-manifests/";
const MAX_MANIFEST: usize = 4096;
const MAX_DIR: usize = 96;
const FS_OPEN: u32 = 0x0900;
const FS_READ: u32 = 0x0901;
const FS_CLOSE: u32 = 0x0903;
/// The fs provider never waits on its device inside a call: `EAGAIN` is
/// "not now, nothing moved" and `EINPROGRESS` "taken, ask again"; neither is
/// a failure or the end of a layer. The read resumes on a later step from
/// the FD and the bytes it already has (`Asm::fd`, `Asm::got`).
const EAGAIN: i32 = -11;
const EINPROGRESS: i32 = -36;
/// `storage.object`: the FLXA blob of a graph-backed workload is read from
/// the Graph object (`graphs.nanocloud.io/v1`, `kubectl apply`'d to the
/// plane and replicated to every node) at `/graphs.nanocloud.io/<name>`,
/// whose `spec.flxa` carries it as hex.
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const GRAPHS_PREFIX: &[u8] = b"/graphs.nanocloud.io/";
const MAX_GRAPH: usize = 64;
const FLXA_FIELD: &[u8] = b"\"flxa\":\"";
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
/// `pack=<name>;layers=<64 hex>,…;at=<index>,…` for every layer at most.
const BAD_MAX: usize = 32 + MAX_ID + MAX_LAYERS * 65 + MAX_LAYERS * 3;
/// The kernel's refusal of a pack whose module is not its pinned digest.
const WL_EACCES: i32 = -13;
const MAX_ID: usize = 128;
/// A moment's text at most (`mo_now`: `m<20 digits>@<32 hex>` fits).
const MOMENT_MAX: usize = 64;
/// Workloads held at once, a finished one until it is destroyed: a node keeps
/// a sandbox for every container of every Pod it hosts, its exit and logs
/// with it, running or not — so this is the runtime's own sandbox table
/// (fluxor's Linux host-process backend, which matches its owner table), and
/// a create past it is refused (`failed`, -12) by the runtime, not here.
const MAX_SB: usize = 64;
const MAX_TTY: usize = 4;
const MAX_SRC: usize = 4096;
const MAX_OPTS: usize = 1024;
/// The `net`/`resolver` Tier-2 entry at most: its head and the context's
/// bound.
const RESOLVER_OPT: usize = 1 + 3 + 1 + 8 + 1 + 2 + abi::contracts::workload::resolver::MAX;
const LOG_READ: usize = 512;
const EXEC_CAP: usize = 2048;
const TTY_CAP: usize = 4096;
const TTY_IN_CAP: usize = 1024;
const INPUTS: usize = 6;
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
    /// The moment START started its process (`_shared/moment.rs`), carried by
    /// every status of the run: a run's start is the runtime's to say, not
    /// something a reader infers from having seen it running.
    run_len: u8,
    run_at: [u8; MOMENT_MAX],
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
    run_len: 0,
    run_at: [0; MOMENT_MAX],
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

/// A workload image being read into a pack, a step at a time (a provider
/// may answer EAGAIN): one at a time, and no other op is taken meanwhile —
/// they wait in their channels, so none is lost or reordered behind it.
#[repr(C)]
struct Asm {
    active: bool,
    cid: i64,
    req_len: u16,
    req: [u8; REQ_BUF],
    n: u8,
    hex: [[u8; 64]; MAX_LAYERS],
    size: [u32; MAX_LAYERS],
    /// Where each layer lands in `buf`: the FLXA at the end of the CREATE
    /// arg, each module at its place in the table.
    at: [u32; MAX_LAYERS],
    buf: *mut u8,
    arg_len: u32,
    /// The options' bytes at the end of the CREATE arg (its resolver
    /// context).
    opts_len: u16,
    pack_at: u32,
    pack_len: u32,
    cur: u8,
    fd: i32,
    got: u32,
    hasher: Sha256,
    /// The manifest the pack is (its /image-manifests/ name), for a failed
    /// pack's report of the layers it could not use.
    name: [u8; MAX_ID],
    name_len: u8,
    /// The pack's CREATE is the one `creating` holds: a kernel refusal of
    /// its pins (EACCES) names every module layer.
    pending: bool,
}

/// A CREATE the kernel stages over several calls (`EINPROGRESS`): asked
/// again each step with the same spec, byte for byte, until it answers the
/// handle. The spec is the pack grant (a workload image) or `cbuf`. No other
/// op is taken meanwhile, so the spec cannot change under it.
#[repr(C)]
struct Pend {
    active: bool,
    eexist: u8,
    cid: i64,
    req_len: u16,
    req: [u8; REQ_BUF],
    arg: *mut u8,
    arg_len: u32,
}

#[repr(C)]
struct State {
    /// The host-process companion class (0x1B: READ, EXEC) answered
    /// ENOSYS: a node that runs module subgraphs, not processes (bcm2712).
    /// Asked once; a READ every tick for every live workload otherwise
    /// floods the kernel's provider log (pi5-a: 150 lines/s) and drowns
    /// every other telemetry line.
    hp_absent: bool,
    /// What the host realizes of the host-process source
    /// (`host_process::CAPS`, asked once: 0 where the class is absent):
    /// a read-only root, numeric groups, the strict group policy.
    hp_caps: u32,
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

    /// The elastic grant packs are assembled in (null until the first).
    grant: *mut u8,
    grant_len: usize,
    /// The node's blob cache, where a pulled image's layers are files named
    /// by their sha256.
    blobs: [u8; MAX_DIR],
    blobs_len: u8,
    asm: Asm,
    creating: Pend,
    /// A failed pack's cached layers, as the record its status event carries
    /// (`pack=<name>;layers=<hex>,…;at=<index>,…`).
    bad: [u8; BAD_MAX],
    /// An op whose store read was not answered yet (the store's "not yet",
    /// not absence): taken again, before any new op, on a later step.
    held_op: [u8; REQ_BUF],
    held_op_len: u16,
    /// The spec of a graph or host workload's CREATE, held while staged.
    cbuf: [u8; WL_CREATE_HEADER + MAX_SRC + MAX_OPTS],
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

mod params_def {
    use super::p_u32;
    use super::State;
    use super::MAX_DIR;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, blobs, str, 0
            => |s, d, len| {
                let n = if len > MAX_DIR { 0 } else { len };
                s.blobs_len = n as u8;
                if n > 0 { core::ptr::copy_nonoverlapping(d, s.blobs.as_mut_ptr(), n); }
            };
    }
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

/// Decimal digits as a u64, saturating; 0 for none.
fn parse_u64(b: &[u8]) -> u64 {
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    v
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
    // A status of a started workload carries its run's start.
    let mut run = [0u8; MOMENT_MAX];
    let mut rl = 0usize;
    if kind == EV_STATUS {
        if let Some(i) = find_slot(s, id) {
            let sb = &s.sandboxes[i];
            if sb.started {
                rl = (sb.run_len as usize).min(MOMENT_MAX);
                run[..rl].copy_from_slice(&sb.run_at[..rl]);
            }
        }
    }
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
    // Always present (empty when there is no run to name), as every field
    // before it: a decision reads a field the record carries.
    field!(put_field(ev, p, 9, TY_BYTES, &run[..rl]));
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

/// The host-process source (`SOURCE_HOST_PROCESS`, the linux backend's
/// format, `host_process::SOURCE_HOST_PROCESS`): `[flags:u8]` (isolated,
/// root read-only) then, each `[len:u16 LE][bytes]`, the rootfs, the argv
/// (NUL-separated), the environment (NUL-separated `NAME=VALUE`), the cwd,
/// the user and the groups (`[flags:u8][gid:u32 LE]×n`, `01 00 00` for
/// none). From the op: 4 the argv — whitespace-separated tokens, each with
/// `%XX` for a byte it cannot carry raw (a space inside `sh -c "a; b"`),
/// decoded here — 5 rootfs, 6 iso, 14 env, 15 host, 16 wd, 17 iwd, 18 user
/// (the last four `%XX`-escaped, decoded here).
///
/// The environment is the WHOLE of what the process gets — the backend adds
/// nothing of the runtime's: `HOSTNAME` (the Pod's name), then the
/// `,`-separated escaped entries of field 14 (the image's Env and the
/// container's, joined), a later entry overriding an earlier one of its
/// name, and the default `PATH` when none names one — as a container runtime
/// composes a Kubernetes container's environment. An entry with no `=` or no
/// name is not one, and is left out. The cwd is the container's workingDir,
/// else the image's (the backend's `/` when neither is set).
///
/// The identity (field 18, [`split_ident`]): the user [`compose_user`]
/// makes, the supplementary groups and their policy, and whether the root
/// is read-only. What the host cannot realize (`caps`, the host's CAPS) is
/// refused `EOPNOTSUPP` rather than run without it, as a read-only root
/// without isolation is `EINVAL`: a container is never run with less than
/// its securityContext asks.
fn spawn_src(buf: &mut [u8], req: &[u8], caps: u32) -> Result<usize, i64> {
    let mut ub = [0u8; MAX_IDENT];
    let ul = decode_text(frame_bytes(req, 18), &mut ub).ok_or(-22i64)?;
    let (user, groups, flags) = split_ident(ub.get(..ul).ok_or(-22i64)?).ok_or(-22i64)?;
    // `s` before `r`, each at most once (split_ident).
    let strict = flags.first() == Some(&b's');
    let ro_root = flags.last() == Some(&b'r');
    let iso = frame_bytes(req, 6) == b"1";
    if ro_root && !iso {
        return Err(-22);
    }
    let need = if ro_root { hp::caps::ROOT_READ_ONLY } else { 0 }
        | if groups.is_empty() {
            0
        } else {
            hp::caps::GROUPS
        }
        | if strict { hp::caps::GROUPS_STRICT } else { 0 };
    if caps & need != need {
        return Err(-95);
    }
    let mut b0 = 0u8;
    if iso {
        b0 |= hp::SRC_ISOLATED;
    }
    if ro_root {
        b0 |= hp::SRC_ROOT_READ_ONLY;
    }
    spawn_parts(buf, req, b0, user, groups, strict).ok_or(-22)
}

/// The decoded identity of field 18 can carry this much: a user and up to
/// [`MAX_GROUPS`] groups.
const MAX_IDENT: usize = 512;
const MAX_GROUPS: usize = 32;

fn spawn_parts(
    buf: &mut [u8],
    req: &[u8],
    flags: u8,
    user: &[u8],
    groups: &[u8],
    strict: bool,
) -> Option<usize> {
    let rootfs = frame_bytes(req, 5);
    *buf.get_mut(0)? = flags;
    let mut p = put_part(buf, 1, rootfs)?;
    // argv
    let at = p;
    p += 2;
    let mut any = false;
    for tok in argv_tokens(frame_bytes(req, 4)) {
        if any {
            *buf.get_mut(p)? = 0;
            p += 1;
        }
        p += decode_text(tok, buf.get_mut(p..)?)?;
        any = true;
    }
    if !any {
        return None;
    }
    p = end_part(buf, at, p)?;
    // env
    let at = p;
    p = put_env(buf, p + 2, frame_bytes(req, 14), frame_bytes(req, 15))?;
    p = end_part(buf, at, p)?;
    // cwd: the container's, else the image's
    let wd = frame_bytes(req, 16);
    let wd = if wd.is_empty() {
        frame_bytes(req, 17)
    } else {
        wd
    };
    let at = p;
    p = at + 2 + decode_text(wd, buf.get_mut(at + 2..)?)?;
    p = end_part(buf, at, p)?;
    // user
    let at = p;
    p = at + 2 + compose_user(user, buf.get_mut(at + 2..)?)?;
    p = end_part(buf, at, p)?;
    // groups: the policy, then each gid
    let at = p;
    p += 2;
    *buf.get_mut(p)? = if strict { hp::GROUPS_STRICT } else { 0 };
    p += 1;
    let mut n = 0usize;
    for g in groups.split(|&c| c == b',').filter(|g| !g.is_empty()) {
        n += 1;
        if n > MAX_GROUPS {
            return None;
        }
        p = copy_into(buf, p, &parse_gid(g)?.to_le_bytes())?;
    }
    end_part(buf, at, p)
}

/// A numeric group id, as Kubernetes allows one (0..=2147483647).
fn parse_gid(g: &[u8]) -> Option<u32> {
    if g.is_empty() || g.len() > 10 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in g {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    (v <= 0x7FFF_FFFF).then_some(v as u32)
}

/// Field 18 decoded, split: the user ([`compose_user`]'s
/// `<image's User>/<runAsUser>/<runAsGroup>`, or a user alone), then — when
/// pod_resolve states any — `/<gid>,<gid>…/<flags>`: the supplementary groups
/// (fsGroup and supplementalGroups, numeric) and `s` (supplementalGroupsPolicy
/// Strict) then `r` (readOnlyRootFilesystem). None when it is not that shape.
fn split_ident(raw: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let mut slashes = raw
        .iter()
        .enumerate()
        .filter(|(_, &c)| c == b'/')
        .map(|(i, _)| i);
    let (_, _, Some(third)) = (slashes.next(), slashes.next(), slashes.next()) else {
        return Some((raw, &[], &[]));
    };
    let fourth = slashes.next()?;
    if slashes.next().is_some() {
        return None;
    }
    let flags = raw.get(fourth + 1..)?;
    if !matches!(flags, b"" | b"s" | b"r" | b"sr") {
        return None;
    }
    Some((raw.get(..third)?, raw.get(third + 1..fourth)?, flags))
}

/// The user a container runs as, from field 18: the image's User alone, or
/// `<the image's User>/<runAsUser>/<runAsGroup>` (pod_project) composed as
/// upstream's runtime does — a runAsUser replaces the image's user (with
/// runAsGroup as its group, else the one its passwd entry names), a
/// runAsGroup alone keeps the image's uid (root when it names none) with
/// that group.
fn compose_user(raw: &[u8], out: &mut [u8]) -> Option<usize> {
    let Some(s1) = raw.iter().position(|&c| c == b'/') else {
        return copy_into(out, 0, raw);
    };
    let base = raw.get(..s1)?;
    let rest = raw.get(s1 + 1..)?;
    let s2 = rest.iter().position(|&c| c == b'/')?;
    let ruid = rest.get(..s2)?;
    let rgid = rest.get(s2 + 1..)?;
    if !ruid.is_empty() {
        let p = copy_into(out, 0, ruid)?;
        if rgid.is_empty() {
            return Some(p);
        }
        let p = copy_into(out, p, b":")?;
        return copy_into(out, p, rgid);
    }
    if rgid.is_empty() {
        return copy_into(out, 0, base);
    }
    let uid = match base.iter().position(|&c| c == b':') {
        Some(c) => base.get(..c)?,
        None => base,
    };
    let uid: &[u8] = if uid.is_empty() { b"0" } else { uid };
    let p = copy_into(out, 0, uid)?;
    let p = copy_into(out, p, b":")?;
    copy_into(out, p, rgid)
}

/// One `%XX`-escaped token or entry decoded into `out`. None when it does
/// not fit, or when it decodes a NUL: no argument, variable, path or user
/// carries one, and `%00` is how a projection says what it rendered did not
/// fit (_shared/pathmod.rs, OVERFLOW) — so a command, an env, a cwd or a
/// user cut short is refused (the CREATE fails -22, the Pod Failed) and
/// never run as something else.
fn decode_text(tok: &[u8], out: &mut [u8]) -> Option<usize> {
    let n = argv_decode(tok, out)?;
    if !out.get(..n)?.iter().all(|&b| b != 0) {
        return None;
    }
    Some(n)
}

/// `[len:u16 LE][bytes]` at `at`; returns the end.
fn put_part(buf: &mut [u8], at: usize, bytes: &[u8]) -> Option<usize> {
    let n = u16::try_from(bytes.len()).ok()?;
    let le = n.to_le_bytes();
    *buf.get_mut(at)? = le[0];
    *buf.get_mut(at + 1)? = le[1];
    let dst = buf.get_mut(at + 2..at + 2 + bytes.len())?;
    for (d, &b) in dst.iter_mut().zip(bytes) {
        *d = b;
    }
    Some(at + 2 + bytes.len())
}

/// Write the length of the part whose bytes run from `at + 2` to `end`.
fn end_part(buf: &mut [u8], at: usize, end: usize) -> Option<usize> {
    let n = u16::try_from(end.checked_sub(at + 2)?).ok()?;
    let le = n.to_le_bytes();
    *buf.get_mut(at)? = le[0];
    *buf.get_mut(at + 1)? = le[1];
    Some(end)
}

/// An escaped env entry's name (before its first `=`); None when it is not
/// `NAME=VALUE`.
fn env_name(e: &[u8]) -> Option<&[u8]> {
    let i = e.iter().position(|&c| c == b'=')?;
    (i > 0).then(|| e.get(..i)).flatten()
}

/// The environment (see [`spawn_src`]) NUL-separated at `buf[p..]`; returns
/// the end.
fn put_env(buf: &mut [u8], mut p: usize, env: &[u8], host: &[u8]) -> Option<usize> {
    let start = p;
    let named = |name: &[u8]| env.split(|&c| c == b',').any(|e| env_name(e) == Some(name));
    let sep = |buf: &mut [u8], p: &mut usize| -> Option<()> {
        if *p > start {
            *buf.get_mut(*p)? = 0;
            *p += 1;
        }
        Some(())
    };
    if !host.is_empty() && !named(b"HOSTNAME") {
        sep(buf, &mut p)?;
        p = copy_into(buf, p, b"HOSTNAME=")?;
        p = copy_into(buf, p, host)?;
    }
    for (i, e) in env.split(|&c| c == b',').enumerate() {
        let Some(name) = env_name(e) else {
            // Not an entry — but an OVERFLOW (`%00`) is a list that did not
            // fit, and is refused rather than run without it.
            if e.windows(3).any(|w| w == b"%00") {
                return None;
            }
            continue;
        };
        // A later entry of this name overrides it.
        if env
            .split(|&c| c == b',')
            .skip(i + 1)
            .any(|l| env_name(l) == Some(name))
        {
            continue;
        }
        sep(buf, &mut p)?;
        p += decode_text(e, buf.get_mut(p..)?)?;
    }
    if !named(b"PATH") {
        sep(buf, &mut p)?;
        p = copy_into(
            buf,
            p,
            b"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        )?;
    }
    Some(p)
}

/// `bytes` at `buf[p..]`; returns the end.
fn copy_into(buf: &mut [u8], p: usize, bytes: &[u8]) -> Option<usize> {
    let dst = buf.get_mut(p..p + bytes.len())?;
    for (d, &b) in dst.iter_mut().zip(bytes) {
        *d = b;
    }
    Some(p + bytes.len())
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

/// The FLXA bytes of graph `name`: the Graph object at
/// `/graphs.nanocloud.io/<name>` read through `storage.object`, its
/// `spec.flxa` decoded from hex into `out`. [`Got::Absent`] when the object
/// is absent, carries no flxa, is not hex, or is larger than `out` — a graph
/// the node was not given is a workload it cannot run, reported
/// `failed(-2)` by the caller. [`Got::Pending`] when the
/// store has not answered yet (its "not yet": [`not_yet`]): the op is asked
/// again, never failed for it. [`Got::Refused`] when the store refused the
/// read for good: the op fails with that errno, never held for it.
unsafe fn graph_source(
    sys: &SyscallTable,
    nag: &ReadNag,
    name: &[u8],
    out: &mut [u8],
) -> Got<usize> {
    if name.is_empty() || name.len() > MAX_GRAPH {
        return Got::Absent;
    }
    let mut key = [0u8; GRAPHS_PREFIX.len() + MAX_GRAPH];
    key[..GRAPHS_PREFIX.len()].copy_from_slice(GRAPHS_PREFIX);
    key[GRAPHS_PREFIX.len()..GRAPHS_PREFIX.len() + name.len()].copy_from_slice(name);
    let klen = GRAPHS_PREFIX.len() + name.len();
    let h = (sys.provider_call)(-1, OBJ_GET, key.as_mut_ptr(), klen);
    if h < 0 {
        return refusal(sys, nag, b"GET", &key[..klen], h);
    }
    // The object is JSON; its hex is twice the bytes. One read of the whole
    // record, then the `"flxa":"…"` string inside it.
    let mut rec = [0u8; 2 * MAX_SRC + 512];
    let mut rg = [0u8; 20];
    rg[0..8].copy_from_slice(&0u64.to_le_bytes());
    rg[8..12].copy_from_slice(&(rec.len() as u32).to_le_bytes());
    rg[12..20].copy_from_slice(&(rec.as_mut_ptr() as u64).to_le_bytes());
    let got = (sys.provider_call)(h, OBJ_RANGE_GET, rg.as_mut_ptr(), rg.len());
    let mut z = [0u8; 1];
    (sys.provider_call)(h, OBJ_CLOSE, z.as_mut_ptr(), 0);
    // A refused read of an open handle is never absence.
    if got < 0 {
        return range_refused(sys, nag, &key[..klen], got);
    }
    match graph_flxa(&rec, got, out) {
        Some(n) => Got::Found(n),
        None => Got::Absent,
    }
}

/// The `"flxa":"…"` hex of the Graph object `rec[..got]`, decoded into `out`.
fn graph_flxa(rec: &[u8], got: i32, out: &mut [u8]) -> Option<usize> {
    if got <= 0 {
        return None;
    }
    let rec = &rec[..(got as usize).min(rec.len())];
    let start = rec
        .windows(FLXA_FIELD.len())
        .position(|w| w == FLXA_FIELD)
        .map(|i| i + FLXA_FIELD.len())?;
    let n = rec[start..].iter().position(|&c| c == b'"')?;
    let hex = &rec[start..start + n];
    if n % 2 != 0 || n / 2 > out.len() {
        return None;
    }
    let v = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    for i in 0..n / 2 {
        match (v(hex[2 * i]), v(hex[2 * i + 1])) {
            (Some(hi), Some(lo)) => out[i] = (hi << 4) | lo,
            _ => return None,
        }
    }
    Some(n / 2)
}

/// A store value whole into `out`; [`Got::Absent`] when absent or larger
/// than `out`, [`Got::Pending`] when the store has not answered yet (its
/// "not yet": [`not_yet`]), [`Got::Refused`] when it refused the read for
/// good ([`refused`]).
unsafe fn store_get(sys: &SyscallTable, nag: &ReadNag, key: &[u8], out: &mut [u8]) -> Got<usize> {
    let mut k = [0u8; 256];
    if key.len() > k.len() {
        return Got::Absent;
    }
    k[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, k.as_mut_ptr(), key.len());
    if h < 0 {
        return refusal(sys, nag, b"GET", key, h);
    }
    let mut rg = [0u8; 20];
    rg[8..12].copy_from_slice(&(out.len() as u32).to_le_bytes());
    rg[12..20].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
    let got = (sys.provider_call)(h, OBJ_RANGE_GET, rg.as_mut_ptr(), rg.len());
    let mut z = [0u8; 1];
    (sys.provider_call)(h, OBJ_CLOSE, z.as_mut_ptr(), 0);
    if got < 0 {
        return range_refused(sys, nag, key, got);
    }
    if (got as usize) < out.len() {
        Got::Found(got as usize)
    } else {
        Got::Absent
    }
}

/// Hold op `req` for a later step: a store read it needs was not answered.
fn hold_op(s: &mut State, req: &[u8]) {
    let n = req.len().min(REQ_BUF);
    s.held_op[..n].copy_from_slice(&req[..n]);
    s.held_op_len = n as u16;
}

/// The `name=` field of a flat `k=v;` record.
fn flat_field<'a>(rec: &'a [u8], name: &[u8]) -> &'a [u8] {
    for part in rec.split(|&c| c == b';') {
        if part.len() > name.len() && part[name.len()] == b'=' && &part[..name.len()] == name {
            return &part[name.len() + 1..];
        }
    }
    &[]
}

fn hex_nib(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// The CREATE header's Tier-1 fields, shared by every source kind: identity,
/// posture, kind, the task envelope, the section lengths, the net identity.
/// False for an address that is not IPv4.
unsafe fn create_header(
    arg: &mut [u8],
    kind: u8,
    id: &[u8],
    req: &[u8],
    src_len: usize,
    opts_len: usize,
) -> bool {
    let tasks = parse_u32(frame_bytes(req, 7));
    let addr = frame_bytes(req, 8);
    let prefix = parse_u32(frame_bytes(req, 9)).min(32) as u8;
    // The identity's segment (field 22): the node's (node_net's
    // `/sandbox-net`), whose gateway the workload reaches everything
    // through; 0 none.
    let segment = parse_u32(frame_bytes(req, 22)).min(u16::MAX as u32) as u16;
    for b in arg[..WL_CREATE_HEADER].iter_mut() {
        *b = 0;
    }
    // The workload's identity, on every backend: the kernel allocates its
    // owner by it, and an owner's resources — its storage root, a volume's
    // grant — key on it. The first 16 bytes of sha256 of the sandbox id,
    // which is the Pod's UID: the same for a re-offered CREATE of one Pod,
    // distinct for every other. The null identity names no one; the kernel
    // refuses it.
    let digest = sha256(id);
    arg[..16].copy_from_slice(&digest[..16]);
    arg[16] = POSTURE_SHARED;
    arg[17] = kind;
    arg[32..36].copy_from_slice(&tasks.to_le_bytes());
    arg[40..42].copy_from_slice(&(src_len as u16).to_le_bytes());
    arg[44..48].copy_from_slice(&(opts_len as u32).to_le_bytes());
    // The envelope: the container's limits (fields 19 cpu millicores, 20
    // memory bytes, 21 ephemeral-storage bytes, decimal; empty = unlimited).
    // A Pod's workload is an owner of its own with a storage root of its
    // own; its ephemeral-storage limit is what that owner may hold in it
    // (`storage_bytes`), its files and its store.
    let milli = parse_u64(frame_bytes(req, 19)).min(u32::MAX as u64) as u32;
    arg[20..24].copy_from_slice(&milli.to_le_bytes());
    arg[24..32].copy_from_slice(&parse_u64(frame_bytes(req, 20)).to_le_bytes());
    arg[STORAGE_BYTES_AT..STORAGE_BYTES_AT + 8]
        .copy_from_slice(&parse_u64(frame_bytes(req, 21)).to_le_bytes());
    if !addr.is_empty() {
        let Some(a) = parse_ipv4(addr) else {
            return false;
        };
        arg[18] = NET_ISO_OWN;
        arg[48] = NET_FAM_IPV4;
        arg[49] = if prefix == 0 { 24 } else { prefix };
        for (d, b) in arg.iter_mut().skip(50).take(2).zip(segment.to_le_bytes()) {
            *d = b;
        }
        arg[52..56].copy_from_slice(&a);
    }
    true
}

/// The Tier-2 `net`/`resolver` entry for the resolver context `dns`
/// (`<server> <search>…`, field 23, the Pod's resolv.conf as pod_resolve
/// made it) into `out`: `[family][server:16][n_search]([len][name])×n`,
/// the search names that fit the context's bound. Advisory: a backend that
/// does not resolve for its workloads (Linux: the workload reads its own
/// resolv.conf) ignores it; one that does (bare metal) holds it for the
/// owner. 0 when there is none, or its server is not an IPv4 address.
fn resolver_option(dns: &[u8], out: &mut [u8]) -> usize {
    use abi::contracts::workload::resolver as rv;
    let mut words = dns.split(|&c| c == b' ').filter(|w| !w.is_empty());
    let Some(server) = words.next().and_then(parse_ipv4) else {
        return 0;
    };
    let head = 1 + rv::NS.len() + 1 + rv::KEY.len() + 1 + 2;
    let mut val = [0u8; rv::MAX];
    val[0] = NET_FAM_IPV4;
    for (d, &b) in val.iter_mut().skip(1).zip(server.iter()) {
        *d = b;
    }
    let mut vl = rv::HEAD;
    let mut n = 0u8;
    for w in words {
        if w.len() > 253 || vl + 1 + w.len() > rv::MAX || n == u8::MAX {
            break;
        }
        if let Some(d) = val.get_mut(vl) {
            *d = w.len() as u8;
        }
        for (d, &b) in val.iter_mut().skip(vl + 1).zip(w.iter()) {
            *d = b;
        }
        vl += 1 + w.len();
        n += 1;
    }
    val[rv::HEAD - 1] = n;
    if head + vl > out.len() {
        return 0;
    }
    let mut p = 0usize;
    let mut push = |bytes: &[u8], p: &mut usize| {
        for (d, &b) in out.iter_mut().skip(*p).zip(bytes.iter()) {
            *d = b;
        }
        *p += bytes.len();
    };
    push(&[rv::NS.len() as u8], &mut p);
    push(rv::NS, &mut p);
    push(&[rv::KEY.len() as u8], &mut p);
    push(rv::KEY, &mut p);
    push(&[0], &mut p);
    push(&(vl as u16).to_le_bytes(), &mut p);
    push(val.get(..vl).unwrap_or(&[]), &mut p);
    p
}

/// A live workload's slot, taken for `handle` and reported created.
unsafe fn seat(s: &mut State, handle: i32, cid: i64, id: &[u8], req: &[u8]) {
    let sys = &*s.syscalls;
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

/// A pulled workload image to its pack: the manifest's layers (field 13
/// names it under /image-manifests/), laid out, then read a step at a time.
/// `failed(-2)` for an image the node has not pulled whole, `-7` past the
/// pack arena, `-22` for a manifest this cannot read.
unsafe fn asm_begin(s: &mut State, req: &[u8], cid: i64, id: &[u8], name: &[u8]) {
    let sys = &*s.syscalls;
    let mut key = [0u8; MANIFESTS_PREFIX.len() + 128];
    if name.len() > MAX_ID || s.blobs_len == 0 {
        emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
        return;
    }
    key[..MANIFESTS_PREFIX.len()].copy_from_slice(MANIFESTS_PREFIX);
    key[MANIFESTS_PREFIX.len()..MANIFESTS_PREFIX.len() + name.len()].copy_from_slice(name);
    let mut rec = [0u8; MAX_MANIFEST];
    let kl = MANIFESTS_PREFIX.len() + name.len();
    let rl = match store_get(sys, &s.nag, &key[..kl], &mut rec) {
        Got::Found(rl) => rl,
        Got::Pending => {
            hold_op(s, req);
            return;
        }
        // Refused for good: failed with the store's errno, never held.
        Got::Refused(rc) => {
            emit(
                s,
                cid,
                EV_STATUS,
                id,
                b"failed",
                i64::from(rc),
                &[],
                Some(req),
            );
            return;
        }
        Got::Absent => {
            emit(s, cid, EV_STATUS, id, b"failed", -2, &[], Some(req));
            return;
        }
    };
    let rec = &rec[..rl];
    let a = &mut s.asm;
    if flat_field(rec, b"kind") != b"workload" {
        emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
        return;
    }
    // Layer 0 is the FLXA, every other a module, in manifest order.
    let mut n = 0usize;
    let mut bad = false;
    for h in flat_field(rec, b"layers").split(|&c| c == b',') {
        if n == MAX_LAYERS || h.len() != 64 || h.iter().any(|&c| hex_nib(c).is_none()) {
            bad = true;
            break;
        }
        a.hex[n].copy_from_slice(h);
        n += 1;
    }
    let mut m = 0usize;
    for z in flat_field(rec, b"sizes").split(|&c| c == b',') {
        if m == n {
            bad = true;
            break;
        }
        let v = parse_u32(z);
        if v == 0 {
            bad = true;
            break;
        }
        a.size[m] = v;
        m += 1;
    }
    if bad || n < 2 || m != n {
        emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
        return;
    }
    let mods = n - 1;
    // The CREATE arg: header, pack reference, a pin per module, the FLXA,
    // then its options (its resolver context).
    let mut ropt = [0u8; RESOLVER_OPT];
    let ol = resolver_option(frame_bytes(req, 23), &mut ropt);
    a.opts_len = ol as u16;
    let arg_len = WL_CREATE_HEADER + PACK_REF + PIN * mods + a.size[0] as usize + ol;
    a.at[0] = (WL_CREATE_HEADER + PACK_REF + PIN * mods) as u32;
    let pack_at = arg_len.div_ceil(16) * 16;
    let mut t = FXMT_HEADER + FXMT_ENTRY * mods;
    for i in 1..n {
        while !(t + FMOD_HEADER).is_multiple_of(CODE_ALIGN) {
            t += 1;
        }
        a.at[i] = (pack_at + t) as u32;
        t += a.size[i] as usize;
    }
    let total = pack_at + t;
    if total > PACK_MAX {
        emit(s, cid, EV_STATUS, id, b"failed", -7, &[], Some(req));
        return;
    }
    if total > s.grant_len {
        let mut want = PACK_MIN;
        while want < total {
            want *= 2;
        }
        let mut arg = [0u8; 8];
        arg[..4].copy_from_slice(&(want.min(PACK_MAX) as u32).to_le_bytes());
        let got = (sys.provider_call)(-1, ELASTIC_ALLOC, arg.as_mut_ptr(), arg.len());
        if got <= 0 || (got as usize) < total {
            emit(s, cid, EV_STATUS, id, b"failed", -12, &[], Some(req));
            return;
        }
        s.grant = u64::from_le_bytes(arg) as usize as *mut u8;
        s.grant_len = got as usize;
    }
    let buf = s.grant;
    core::ptr::write_bytes(buf, 0, total);
    let a = &mut s.asm;
    a.buf = buf;
    a.arg_len = arg_len as u32;
    a.pack_at = pack_at as u32;
    a.pack_len = t as u32;
    a.n = n as u8;
    a.cur = 0;
    a.fd = -1;
    a.got = 0;
    a.hasher = Sha256::new();
    a.cid = cid;
    a.req[..req.len()].copy_from_slice(req);
    a.req_len = req.len() as u16;
    a.name[..name.len()].copy_from_slice(name);
    a.name_len = name.len() as u8;
    a.pending = false;
    a.active = true;
}

/// The pack is done (or failed): the heap block goes, the slot is free.
unsafe fn asm_end(s: &mut State, rc: i64) {
    let sys = &*s.syscalls;
    if s.asm.fd >= 0 {
        let mut z = [0u8; 4];
        (sys.provider_call)(s.asm.fd, FS_CLOSE, z.as_mut_ptr(), 0);
        s.asm.fd = -1;
    }
    s.asm.buf = core::ptr::null_mut();
    s.asm.active = false;
    if rc < 0 {
        let mut req = [0u8; REQ_BUF];
        let rl = s.asm.req_len as usize;
        req[..rl].copy_from_slice(&s.asm.req[..rl]);
        let id = frame_bytes(&req[..rl], 3);
        let mut idb = [0u8; MAX_ID];
        let il = id.len().min(MAX_ID);
        idb[..il].copy_from_slice(&id[..il]);
        // A layer the cache does not hold, holds short, or holds as other
        // bytes than its digest: named, so the chain drops it from the cache
        // and has it fetched again (image_heal.uproc).
        let cur = s.asm.cur as usize;
        let bl = if rc == -2 || rc == -5 || rc == -74 {
            bad_layers(s, cur, cur + 1)
        } else {
            0
        };
        let mut bad = [0u8; BAD_MAX];
        bad[..bl].copy_from_slice(&s.bad[..bl]);
        emit(
            s,
            s.asm.cid,
            EV_STATUS,
            &idb[..il],
            b"failed",
            rc,
            &bad[..bl],
            Some(&req[..rl]),
        );
    }
}

/// `pack=<name>;layers=<hex>,…;at=<index>,…` for layers `from..to` of the
/// pack in hand, into `s.bad`; its length.
unsafe fn bad_layers(s: &mut State, from: usize, to: usize) -> usize {
    let a = &s.asm;
    let to = to.min(a.n as usize).min(MAX_LAYERS);
    let out = &mut s.bad;
    let mut p = 0usize;
    let mut put = |b: &[u8], p: &mut usize| {
        for &c in b {
            if let Some(slot) = out.get_mut(*p) {
                *slot = c;
                *p += 1;
            }
        }
    };
    put(b"pack=", &mut p);
    put(a.name.get(..a.name_len as usize).unwrap_or(&[]), &mut p);
    put(b";layers=", &mut p);
    for i in from..to {
        if i > from {
            put(b",", &mut p);
        }
        put(a.hex.get(i).map(|h| &h[..]).unwrap_or(&[]), &mut p);
    }
    put(b";at=", &mut p);
    for i in from..to {
        if i > from {
            put(b",", &mut p);
        }
        let d = [b'0' + (i / 10) as u8, b'0' + (i % 10) as u8];
        put(if i < 10 { &d[1..] } else { &d[..] }, &mut p);
    }
    p
}

/// One read of the layer in hand; at the last layer's end, the CREATE.
unsafe fn asm_step(s: &mut State) {
    let sys = &*s.syscalls;
    let a = &mut s.asm;
    let i = a.cur as usize;
    if a.fd < 0 {
        let mut path = [0u8; MAX_DIR + 1 + 64];
        let dl = s.blobs_len as usize;
        path[..dl].copy_from_slice(&s.blobs[..dl]);
        path[dl] = b'/';
        path[dl + 1..dl + 65].copy_from_slice(&a.hex[i]);
        let fd = (sys.provider_call)(-1, FS_OPEN, path.as_mut_ptr(), dl + 65);
        if fd == EAGAIN || fd == EINPROGRESS {
            return;
        }
        if fd < 0 {
            asm_end(s, -2); // a layer the cache does not hold
            return;
        }
        a.fd = fd;
    }
    let want = (a.size[i] - a.got) as usize;
    let dst = a.buf.add(a.at[i] as usize + a.got as usize);
    let n = (sys.provider_call)(a.fd, FS_READ, dst, want.min(READ_CHUNK));
    if n == EAGAIN || n == EINPROGRESS {
        return;
    }
    if n <= 0 || n as usize > want {
        asm_end(s, -5); // short, or longer than the manifest says
        return;
    }
    a.hasher
        .update(core::slice::from_raw_parts(dst, n as usize));
    a.got += n as u32;
    if a.got < a.size[i] {
        return;
    }
    let mut z = [0u8; 4];
    (sys.provider_call)(a.fd, FS_CLOSE, z.as_mut_ptr(), 0);
    a.fd = -1;
    // The cache's bytes are the layer the manifest names, or nothing runs.
    let h = core::mem::take(&mut a.hasher).finalize();
    const HX: &[u8; 16] = b"0123456789abcdef";
    for (k, b) in h.iter().enumerate() {
        if a.hex[i][2 * k] != HX[(b >> 4) as usize] || a.hex[i][2 * k + 1] != HX[(b & 15) as usize]
        {
            asm_end(s, -74);
            return;
        }
    }
    a.got = 0;
    a.cur += 1;
    if (a.cur as usize) < a.n as usize {
        return;
    }
    asm_create(s);
}

/// Every layer in: the module table's header and entries, the pins, the
/// CREATE.
unsafe fn asm_create(s: &mut State) {
    let sys = &*s.syscalls;
    let a = &s.asm;
    let n = a.n as usize;
    let mods = n - 1;
    let buf = core::slice::from_raw_parts_mut(a.buf, a.pack_at as usize + a.pack_len as usize);
    let (arg, pack) = buf.split_at_mut(a.pack_at as usize);
    pack[0..4].copy_from_slice(&FXMT_MAGIC.to_le_bytes());
    pack[4] = FXMT_VERSION;
    pack[5] = mods as u8;
    pack[6..8].copy_from_slice(&((a.pack_len & 0xFFFF) as u16).to_le_bytes());
    pack[8..10].copy_from_slice(&((a.pack_len >> 16) as u16).to_le_bytes());
    for i in 1..n {
        let off = (a.at[i] - a.pack_at) as usize;
        let fmod = &pack[off..off + a.size[i] as usize];
        if fmod.len() < FMOD_HEADER {
            asm_end(s, -22);
            return;
        }
        let name = &fmod[FMOD_NAME_AT..FMOD_NAME_AT + FMOD_NAME_LEN];
        let nl = name.iter().position(|&c| c == 0).unwrap_or(FMOD_NAME_LEN);
        let hash = fnv1a32(&name[..nl]);
        let mtype = fmod[6];
        let e = FXMT_HEADER + FXMT_ENTRY * (i - 1);
        pack[e..e + 4].copy_from_slice(&hash.to_le_bytes());
        pack[e + 4..e + 8].copy_from_slice(&(off as u32).to_le_bytes());
        pack[e + 8..e + 12].copy_from_slice(&a.size[i].to_le_bytes());
        pack[e + 12] = mtype;
    }
    let ol = a.opts_len as usize;
    let src_len = a.arg_len as usize - WL_CREATE_HEADER - ol;
    let rl = a.req_len as usize;
    let mut req = [0u8; REQ_BUF];
    req[..rl].copy_from_slice(&a.req[..rl]);
    let req = &req[..rl];
    let id = frame_bytes(req, 3);
    let mut idb = [0u8; MAX_ID];
    let il = id.len().min(MAX_ID);
    idb[..il].copy_from_slice(&id[..il]);
    let id = &idb[..il];
    if !create_header(arg, SOURCE_FMOD_PACK, id, req, src_len, ol) {
        asm_end(s, -22);
        return;
    }
    let oat = WL_CREATE_HEADER + src_len;
    let wrote = resolver_option(
        frame_bytes(req, 23),
        arg.get_mut(oat..oat + ol).unwrap_or(&mut []),
    );
    if wrote != ol {
        asm_end(s, -22);
        return;
    }
    let r = WL_CREATE_HEADER;
    arg[r..r + 8].copy_from_slice(&(pack.as_ptr() as u64).to_le_bytes());
    arg[r + 8..r + 12].copy_from_slice(&s.asm.pack_len.to_le_bytes());
    arg[r + 12..r + 14].copy_from_slice(&(mods as u16).to_le_bytes());
    for i in 1..n {
        let p = r + PACK_REF + PIN * (i - 1);
        for k in 0..PIN {
            let hx = &s.asm.hex[i];
            arg[p + k] =
                (hex_nib(hx[2 * k]).unwrap_or(0) << 4) | hex_nib(hx[2 * k + 1]).unwrap_or(0);
        }
    }
    let arg_ptr = arg.as_mut_ptr();
    let arg_len = s.asm.arg_len;
    let cid = s.asm.cid;
    // The spec lives in the grant, which stays this workload's until its
    // CREATE answers: the next pack waits behind the pending create.
    asm_end(s, 0);
    let _ = sys;
    s.asm.pending = true;
    create_begin(s, cid, req, arg_ptr, arg_len);
}

/// Hold a CREATE's spec and ask it the first time.
unsafe fn create_begin(s: &mut State, cid: i64, req: &[u8], arg: *mut u8, arg_len: u32) {
    let p = &mut s.creating;
    p.active = true;
    p.eexist = 0;
    p.cid = cid;
    p.req[..req.len()].copy_from_slice(req);
    p.req_len = req.len() as u16;
    p.arg = arg;
    p.arg_len = arg_len;
    create_step(s);
}

/// Ask the pending CREATE again: the handle seats the workload; EINPROGRESS
/// waits for the next step; EEXIST (a workload of this identity is live — a
/// CREATE that completed while this module was faulted mid-call, or a
/// restart of this module) adopts it by LOOKUP; anything else fails it.
unsafe fn create_step(s: &mut State) {
    let sys = &*s.syscalls;
    let rc = (sys.provider_call)(-1, WL_CREATE, s.creating.arg, s.creating.arg_len as usize);
    if rc == WL_EINPROGRESS {
        return;
    }
    let mut handle = rc;
    if rc == WL_EEXIST && s.caps_ops & CAPS_OP_LOOKUP != 0 {
        let mut ident = [0u8; 16];
        core::ptr::copy_nonoverlapping(s.creating.arg, ident.as_mut_ptr(), 16);
        let h = (sys.provider_call)(-1, WL_LOOKUP, ident.as_mut_ptr(), ident.len());
        if h >= 0 {
            handle = h;
        } else if h == WL_EINPROGRESS {
            return; // still being staged: the same spec continues it
        } else if s.creating.eexist < CREATE_EEXIST_RETRIES {
            s.creating.eexist += 1;
            return; // gone between the two calls: create again next step
        }
    }
    let mut req = [0u8; REQ_BUF];
    let rl = s.creating.req_len as usize;
    req[..rl].copy_from_slice(&s.creating.req[..rl]);
    let req = &req[..rl];
    let id = frame_bytes(req, 3);
    let mut idb = [0u8; MAX_ID];
    let il = id.len().min(MAX_ID);
    idb[..il].copy_from_slice(&id[..il]);
    let cid = s.creating.cid;
    s.creating.active = false;
    let pack = core::mem::replace(&mut s.asm.pending, false);
    if handle < 0 {
        // The kernel refused a module that is not its pinned digest: the
        // bytes changed after they were verified here, so none is trusted.
        let bl = if pack && handle as i64 == WL_EACCES as i64 {
            bad_layers(s, 1, MAX_LAYERS)
        } else {
            0
        };
        let mut bad = [0u8; BAD_MAX];
        bad[..bl].copy_from_slice(&s.bad[..bl]);
        emit(
            s,
            cid,
            EV_STATUS,
            &idb[..il],
            b"failed",
            handle as i64,
            &bad[..bl],
            Some(req),
        );
        return;
    }
    seat(s, handle, cid, &idb[..il], req);
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
    // A pulled workload image: read into a pack over the next steps.
    let pack = frame_bytes(req, 13);
    if !pack.is_empty() {
        asm_begin(s, req, cid, id, pack);
        return;
    }
    let graph = frame_bytes(req, 12);
    let mut src = [0u8; MAX_SRC];
    let mut opts = [0u8; MAX_OPTS];
    // A graph-backed workload: its source is the FLXA subgraph the node's
    // render seeded at `/graphs/<name>`, not a process. Its one option is
    // its resolver context: the kernel realises it as modules of the node's
    // own image.
    let (sl, ol, kind) = if !graph.is_empty() {
        match graph_source(sys, &s.nag, graph, &mut src) {
            Got::Pending => {
                hold_op(s, req);
                return;
            }
            Got::Found(n) => (
                n,
                resolver_option(frame_bytes(req, 23), &mut opts),
                SOURCE_FMOD_GRAPH,
            ),
            // Refused for good: failed with the store's errno, never held.
            Got::Refused(rc) => {
                emit(
                    s,
                    cid,
                    EV_STATUS,
                    id,
                    b"failed",
                    i64::from(rc),
                    &[],
                    Some(req),
                );
                return;
            }
            Got::Absent => {
                emit(s, cid, EV_STATUS, id, b"failed", -2, &[], Some(req));
                return;
            }
        }
    } else {
        let sl = match spawn_src(&mut src, req, s.hp_caps) {
            Ok(n) => n,
            Err(code) => {
                emit(s, cid, EV_STATUS, id, b"failed", code, &[], Some(req));
                return;
            }
        };
        let Some(ol) = bind_options(frame_bytes(req, 10), &mut opts) else {
            emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
            return;
        };
        let rl = resolver_option(frame_bytes(req, 23), opts.get_mut(ol..).unwrap_or(&mut []));
        (sl, ol + rl, SOURCE_HOST_PROCESS)
    };
    // The spec is held in `cbuf` until the CREATE answers (it may stage).
    let st = s as *mut State;
    let arg = &mut (*st).cbuf;
    if !create_header(arg, kind, id, req, sl, ol) {
        emit(s, cid, EV_STATUS, id, b"failed", -22, &[], Some(req));
        return;
    }
    arg[WL_CREATE_HEADER..WL_CREATE_HEADER + sl].copy_from_slice(&src[..sl]);
    arg[WL_CREATE_HEADER + sl..WL_CREATE_HEADER + sl + ol].copy_from_slice(&opts[..ol]);
    let _ = sys;
    create_begin(
        s,
        cid,
        req,
        arg.as_mut_ptr(),
        (WL_CREATE_HEADER + sl + ol) as u32,
    );
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
                    // The run's start, stamped as the process starts.
                    let mut at = [0u8; MOMENT_MAX];
                    let al = mo_now(sys, &mut at);
                    let sb = &mut s.sandboxes[i];
                    sb.started = true;
                    sb.run_len = al.min(MOMENT_MAX) as u8;
                    sb.run_at = at;
                    emit(s, cid, EV_STATUS, id, b"running", 0, &[], Some(req));
                }
            }
        }
        OP_DESTROY => {
            // Idempotent: a workload this runtime does not hold — one from
            // before a restart, which the restart ended — is destroyed
            // already, and is answered so; otherwise its status would stay
            // whatever the last boot recorded, and its delete never settle.
            if let Some(i) = slot {
                (sys.provider_call)(s.sandboxes[i].handle, WL_DESTROY, z.as_mut_ptr(), 0);
                s.sandboxes[i] = SB_EMPTY;
            } else if s.caps_ops & CAPS_OP_LOOKUP != 0 {
                // No handle held: a workload of this identity may still be
                // live or staged (this module faulted, or restarted, mid
                // CREATE). Destroyed by identity, or nothing carries it.
                let digest = sha256(id);
                let mut ident = [0u8; 16];
                ident.copy_from_slice(&digest[..16]);
                let _ = (sys.provider_call)(-1, WL_DESTROY, ident.as_mut_ptr(), ident.len());
                let _ = WL_ENOENT;
            }
            emit(s, cid, EV_STATUS, id, b"destroyed", 0, &[], Some(req));
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
        let n = if s.hp_absent {
            0
        } else {
            (sys.provider_call)(-1, WL_READ, arg.as_mut_ptr(), arg.len())
        };
        if n == E_NOSYS {
            s.hp_absent = true;
        }
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
        s.inputs = [in_chan, -1, -1, -1, -1, -1];
        s.event_out = out_chan;
        s.resolved = 0;
        s.caps_ops = 0;
        s.hp_caps = 0;
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
        s.blobs_len = 0;
        s.asm.active = false;
        s.asm.buf = core::ptr::null_mut();
        s.asm.fd = -1;
        s.grant = core::ptr::null_mut();
        s.grant_len = 0;
        s.creating.active = false;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
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
            let hc = (sys.provider_call)(-1, hp::CAPS, core::ptr::null_mut(), 0);
            s.hp_caps = if hc < 0 { 0 } else { hc as u32 };
            // This process's boot: a random nonce drawn at start, which no
            // restart repeats. Never the wall clock: a board reads 0 there
            // until its time protocol has answered, so every boot before
            // that was the same boot and a restart re-created nothing — the
            // last boot's statuses read as this one's, `Running` included.
            let mut nonce = [0u8; 8];
            if (sys.provider_call)(
                -1,
                abi::kernel_abi::RANDOM_FILL,
                nonce.as_mut_ptr(),
                nonce.len(),
            ) != 0
            {
                // No randomness yet: asked again next step; no boot is
                // announced (and nothing is created) without an identity.
                return 0;
            }
            let hex = |n: u8| if n < 10 { b'0' + n } else { b'a' + (n - 10) };
            let mut n = 0usize;
            for (pair, b) in s.boot.chunks_exact_mut(2).zip(nonce.iter()) {
                pair[0] = hex(*b >> 4);
                pair[1] = hex(*b & 0x0f);
                n += 2;
            }
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
        // A CREATE being staged is asked again; the rest wait their turn.
        if s.creating.active {
            if s.can_emit() {
                create_step(s);
            }
            poll_sandboxes(s);
            poll_ttys(s);
            return 0;
        }
        // A pack being read takes the step's op: the rest wait their turn.
        if s.asm.active {
            if s.can_emit() {
                asm_step(s);
            }
            poll_sandboxes(s);
            poll_ttys(s);
            return 0;
        }
        // An op held on a store read not answered yet goes before any new
        // one, asked again a step later.
        if s.held_op_len != 0 {
            if s.can_emit() {
                let n = s.held_op_len as usize;
                s.held_op_len = 0;
                let mut req = [0u8; REQ_BUF];
                req[..n].copy_from_slice(&s.held_op[..n]);
                op(s, &req[..n]);
            }
            poll_sandboxes(s);
            poll_ttys(s);
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
