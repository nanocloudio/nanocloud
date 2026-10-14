//! Store doc — the API writer's record adapter in front of lattice's JSON
//! document dialect. See `manifest.toml` for the wire and the rationale.

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* and lattice's wire via include!/mod, so each module's compile sees the full surface; this module uses a subset"
)]
#![allow(
    clippy::duplicate_mod,
    reason = "the PIC build #[path]-remounts shared SDK and lattice wire source"
)]

use core::convert::TryInto;
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/store.rs");
include!("../_shared/views.rs");

// lattice's JSON document wire, from its published source — the channel
// contract its dialect reads, not restated here.
#[path = "../../../target/fluxor/lattice-common/doc_exec.rs"]
mod doc_exec;
#[path = "../../../target/fluxor/lattice-common/doc_json.rs"]
mod doc_json;
#[path = "../../../target/fluxor/lattice-common/wire.rs"]
mod wire;

use doc_exec::{
    FieldWriter, DIALECT_KUBERNETES, FLAG_DRY_RUN, FLAG_FORCE, OUT_BUSY, OUT_COMPACTED,
    OUT_CONFLICT, OUT_EXISTS, OUT_INVALID, OUT_MALFORMED, OUT_NOT_FOUND, OUT_OK, OUT_OWNERSHIP,
    OUT_TIMEOUT, OUT_TOO_LARGE, OUT_UNAVAILABLE, RFLAG_CREATED, RFLAG_HELD,
};
use doc_json::{
    JsonResponse, JR_ERROR, JR_TEXT, J_COLLECTION, J_DOC, J_ID, J_ID_PREFIX, J_IF_REVISION,
    J_MANAGER, J_PATCH, J_SCOPE_NAME, J_VIEW,
};

// Chronicle's decision engine, from its published cores, as store_effect
// fuses it: the arriving record is the decision's input and the outcome it
// constructs is the request.
mod dec {
    include!("../../../target/fluxor/chronicle-common/vm_core.rs");
    include!("../../../target/fluxor/chronicle-common/pipeline_core.rs");
    include!("../../../target/fluxor/chronicle-common/decision_core.rs");
    include!("../../../target/fluxor/chronicle-common/blob_core.rs");
}

const DEC_CONT: usize = dec::MAX_CONTAINER_BIN_FULL;

/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const OBJ_PUT: u32 = 0x1420;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;
const MAX_PREFIX: usize = 64;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

/// store_effect's op codes, the ones this node serves.
const OP_NOOP: i64 = 0;
/// A read of the object at the key: 200 with it, 404 when there is none (a
/// TokenRequest's ServiceAccount, which must exist and is not written).
const OP_GET: i64 = 1;
const OP_PUT: i64 = 3;
const OP_DELETE: i64 = 4;
const OP_CREATE: i64 = 8;
const OP_REPLACE: i64 = 9;
const OP_PATCH_MERGE: i64 = 13;
const OP_PATCH_JSON: i64 = 14;
const OP_PATCH_SMP: i64 = 15;
const OP_APPLY: i64 = 16;
/// store_effect's TAKE: the value answered and deleted in one step,
/// conditional on the revision read — a single-use claim (a join secret).
const OP_TAKE: i64 = 17;

/// One record frame.
const BUF: usize = 8192;
/// The largest reply record the next stage reads: `se_act`, Chronicle's
/// `decision`, whose in and out record buffers are both 4096 bytes. An
/// object as stored that would carry the reply past it is sent with an
/// EMPTY value instead, which `se_ans` (`apiplane_watch.uproc ans`) reads
/// as "too large for a record" and answers with a GET registration the
/// watch chain serves. A reply past the bound is dropped by the reader, so
/// the client would wait out its timeout on a write that landed.
const REPLY_RECORD_MAX: usize = 4096;
const _: () = assert!(REPLY_RECORD_MAX <= BUF);
/// The largest staged body read whole (store_big's bound).
const DOC_MAX: usize = 3 << 20;
/// A request message (its envelope, head and fields around the body) and
/// an answer (the object as stored, rendered), each on the heap.
const MSG_CAP: usize = DOC_MAX + 65536;
/// The heap: the two message buffers, a staged body read beside them
/// (read_whole grows to DOC_MAX), and the allocator's rounding.
const ARENA: usize = 2 * MSG_CAP + DOC_MAX + (1 << 20);
/// A lost answer is a 503, never a writer stalled for good.
const ANSWER_MS: u64 = 30_000;
/// How long a deleted object's last state stays for a watch to carry.
/// How long a deleted object's tomb stays (`tomb_ms`): a watch's DELETED
/// event reads it, and a Pod's node spec lives as long (tomb.uproc).
const TOMB_MS: u32 = 300_000;

const PHASE_IDLE: u8 = 0;
const PHASE_SEND: u8 = 1;
const PHASE_WAIT: u8 = 2;

struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    doc_in: i32,
    response_out: i32,
    doc_out: i32,
    key_prefix: [u8; MAX_PREFIX],
    key_prefix_len: u8,
    /// `tomb = <prefix>`: where a completed delete leaves the object's last
    /// state for a watch's DELETED event (store_big's `tomb`).
    tomb: [u8; 48],
    tomb_len: u8,
    tomb_ms: u32,
    /// `blob = <prefix>`: a body `sha256:<hex>` is the staged body there.
    blob: [u8; 48],
    blob_len: u8,
    /// The decision container, a `blob` param arriving straight into
    /// `dec_cont`.
    dec_in: dec::BlobIn,
    dec_cont: [u8; DEC_CONT],
    dec_len: u16,
    dec_buf: [u8; BUF],
    dec_fault: u8,
    dec_dropped: u32,
    dec_failed: u32,
    in_buf: [u8; BUF],
    /// A reply the output channel has not taken yet: `out_buf[out_at..out_len]`.
    out_buf: [u8; BUF],
    out_at: u32,
    out_len: u32,
    /// The request in flight: the decided record (its cid and carries),
    /// its op, and the object's key (marks removed).
    req: [u8; BUF],
    req_len: u32,
    op: i64,
    key: [u8; MAX_KEY],
    key_len: u16,
    phase: u8,
    /// The store calls of the request being served ([`Journal`]), and
    /// whether the request is held for a write the store has not decided.
    journal: Journal,
    held: u8,
    corr: u64,
    sent_ms: u64,
    /// The request message, envelope header first (heap).
    msg: *mut u8,
    msg_len: u32,
    parts: wire::PartWriter,
    frame: [u8; wire::ENVELOPE_HDR + wire::FRAME_PAYLOAD_MAX],
    /// The answer as it arrives (heap), and its reassembly.
    ans: *mut u8,
    reasm: wire::Reassembly,
    env: [u8; wire::FRAME_PAYLOAD_MAX],
    served: u32,
    refused: u32,
    timeouts: u32,
}

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let num = *frame.get(p)?;
        let ty = *frame.get(p + 1)?;
        let len = u16::from_le_bytes(frame.get(p + 2..p + 4)?.try_into().ok()?) as usize;
        let body = frame.get(p + 4..p + 4 + len)?;
        if num == number {
            return Some((ty, body));
        }
        p += 4 + len;
    }
    None
}

fn frame_i64(frame: &[u8], number: u8) -> Option<i64> {
    match frame_field(frame, number)? {
        (TY_I64, v) if v.len() == 8 => Some(i64::from_le_bytes(v.try_into().ok()?)),
        _ => None,
    }
}

fn put_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    let len = u16::try_from(payload.len()).ok()?;
    let end = at.checked_add(4 + payload.len())?;
    let d = out.get_mut(at..end)?;
    d[0] = number;
    d[1] = ty;
    d[2..4].copy_from_slice(&len.to_le_bytes());
    d[4..].copy_from_slice(payload);
    Some(end)
}

/// `v` as decimal digits into `out`: its length.
fn dec(v: i64, out: &mut [u8; 24]) -> usize {
    let mut tmp = [0u8; 24];
    let mut u = v.unsigned_abs();
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 || i == 1 {
            break;
        }
    }
    if v < 0 {
        i -= 1;
        tmp[i] = b'-';
    }
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// A key or value part: bytes as they are, an integer as decimal.
fn part<'a>(f: Option<(u8, &'a [u8])>, digits: &'a mut [u8; 24]) -> &'a [u8] {
    match f {
        Some((TY_BYTES, b)) => b,
        Some((TY_I64, b)) if b.len() == 8 => {
            let n = dec(i64::from_le_bytes(b.try_into().unwrap_or([0; 8])), digits);
            &digits[..n]
        }
        _ => &[],
    }
}

/// Can a key segment be written inside a JSON string as it is? (A
/// Kubernetes name is a DNS label or subdomain; anything else is refused
/// rather than escaped into an id nobody named.)
fn plain_segment(s: &[u8]) -> bool {
    !s.is_empty()
        && s.iter()
            .all(|&b| b > 0x20 && b != b'"' && b != b'\\' && b < 0x7F)
}

/// The id a key's segments name, as JSON into `out`: `"name"` for one,
/// `["ns","name"]` for more. `prefix`: the leading segments only (a create
/// whose name is generated), always an array.
fn id_json(segs: &[&[u8]], prefix: bool, out: &mut [u8]) -> Option<usize> {
    let mut at = 0usize;
    let mut put = |b: &[u8], at: &mut usize| -> Option<()> {
        let end = at.checked_add(b.len())?;
        out.get_mut(*at..end)?.copy_from_slice(b);
        *at = end;
        Some(())
    };
    let array = prefix || segs.len() > 1;
    if array {
        put(b"[", &mut at)?;
    }
    for (i, s) in segs.iter().enumerate() {
        if !plain_segment(s) {
            return None;
        }
        if i > 0 {
            put(b",", &mut at)?;
        }
        put(b"\"", &mut at)?;
        put(s, &mut at)?;
        put(b"\"", &mut at)?;
    }
    if array {
        put(b"]", &mut at)?;
    }
    Some(at)
}

/// The dialect's outcome as the status store_big answers.
fn status_of(outcome: u8, flags: u8, op: i64) -> i64 {
    match outcome {
        OUT_OK if op == OP_APPLY && flags & RFLAG_CREATED != 0 => 201,
        OUT_OK => 200,
        OUT_NOT_FOUND => 404,
        OUT_EXISTS | OUT_CONFLICT | OUT_OWNERSHIP | OUT_COMPACTED => 409,
        OUT_INVALID => 422,
        OUT_MALFORMED => 400,
        OUT_TOO_LARGE => 413,
        OUT_BUSY | OUT_UNAVAILABLE | OUT_TIMEOUT => 503,
        _ => 500,
    }
}

impl State {
    fn sys(&self) -> &SyscallTable {
        // SAFETY: set once in module_new, the loader's table.
        unsafe { &*self.syscalls }
    }

    /// The store as the request being served sees it.
    fn store(&mut self) -> Store {
        // SAFETY: set once in module_new, the loader's table.
        Store::serving(unsafe { &*self.syscalls }, &mut self.journal)
    }

    fn msg_buf(&mut self) -> &mut [u8] {
        if self.msg.is_null() {
            return &mut [];
        }
        // SAFETY: MSG_CAP bytes of this module's heap, owned by `msg`.
        unsafe { core::slice::from_raw_parts_mut(self.msg, MSG_CAP) }
    }

    fn ans_buf(&mut self) -> &mut [u8] {
        if self.ans.is_null() {
            return &mut [];
        }
        // SAFETY: MSG_CAP bytes of this module's heap, owned by `ans`.
        unsafe { core::slice::from_raw_parts_mut(self.ans, MSG_CAP) }
    }

    /// The reply to the request in flight: its cid, `status`, `value` (when
    /// it fits a record; the chain reads a large object from the store),
    /// and its carries 30..39 as they came.
    fn reply(&mut self, status: i64, value: &[u8]) {
        let rl = (self.req_len as usize).min(BUF);
        // SAFETY: `rl` bytes of `req`, which nothing below writes.
        let req = unsafe { core::slice::from_raw_parts(self.req.as_ptr(), rl) };
        let cid = frame_i64(req, 1).unwrap_or(0);
        let mut carried = 0usize;
        for n in 30u8..=39 {
            if let Some((_, b)) = frame_field(req, n) {
                carried += 4 + b.len();
            }
        }
        let room = REPLY_RECORD_MAX.saturating_sub(1 + 3 * 4 + 16 + carried);
        let value = if value.len() <= room { value } else { &[] };
        let out = &mut self.out_buf;
        let mut count = 3u8;
        let p = put_field(out, 1, 1, TY_I64, &cid.to_le_bytes())
            .and_then(|p| put_field(out, p, 2, TY_I64, &status.to_le_bytes()))
            .and_then(|p| put_field(out, p, 3, TY_BYTES, value));
        let Some(mut p) = p else {
            self.out_len = 0;
            return;
        };
        for n in 30u8..=39 {
            if let Some((ty, b)) = frame_field(req, n) {
                match put_field(out, p, n, ty, b) {
                    Some(q) => {
                        p = q;
                        count += 1;
                    }
                    None => break,
                }
            }
        }
        out[0] = count;
        self.out_at = 0;
        self.out_len = p as u32;
        self.phase = PHASE_IDLE;
    }

    /// Write what is owed of the last reply. True when nothing is owed.
    fn flush_reply(&mut self) -> bool {
        if self.out_len == 0 || self.response_out < 0 {
            self.out_len = 0;
            return true;
        }
        let (at, len) = (self.out_at as usize, self.out_len as usize);
        let Some(rest) = self.out_buf.get(at..len) else {
            self.out_len = 0;
            return true;
        };
        // SAFETY: the channel reads `rest.len()` bytes at its pointer.
        let w = unsafe {
            (self.sys().channel_write)(self.response_out, rest.as_ptr() as *mut u8, rest.len())
        };
        if w > 0 {
            self.out_at += w as u32;
        }
        if self.out_at >= self.out_len {
            self.out_at = 0;
            self.out_len = 0;
            self.served = self.served.wrapping_add(1);
            true
        } else {
            false
        }
    }

    /// Read one request record into `in_buf`: its length.
    fn read_request(&mut self) -> Option<usize> {
        // SAFETY: the loader's table, set in module_new.
        let sys = unsafe { &*self.syscalls };
        let chan = self.request_in;
        // SAFETY: reads into this module's own buffers, bounded by BUF.
        unsafe {
            let poll = (sys.channel_poll)(chan, POLL_IN);
            if poll <= 0 || (poll as u32) & POLL_IN == 0 {
                return None;
            }
            let buf = self.in_buf.as_mut_ptr();
            let pk = (sys.channel_peek)(chan, buf, BUF);
            let n = if pk == CHAN_EINVAL {
                (sys.channel_read)(chan, buf, BUF)
            } else if pk <= 0 {
                return None;
            } else {
                match dec::frame_len(self.in_buf.get(..pk as usize)?) {
                    Some(fl) => (sys.channel_read)(chan, buf, fl),
                    None if (pk as usize) < BUF => return None,
                    None => {
                        (sys.channel_read)(chan, buf, BUF);
                        self.dec_failed = self.dec_failed.wrapping_add(1);
                        return None;
                    }
                }
            };
            (n > 0).then_some(n as usize)
        }
    }

    /// Run the fused decision over the `n`-byte record in `in_buf` into
    /// `dec_buf`: the outcome's length, `None` when it constructed nothing.
    fn decide(&mut self, n: usize) -> Option<usize> {
        let mut fields = [dec::Field {
            number: 0,
            value: dec::Value::Null,
        }; dec::MAX_PIPE_FIELDS];
        let raw = self.in_buf.get(..n)?;
        let Ok(nf) = dec::decode_frame(raw, &mut fields) else {
            self.dec_failed = self.dec_failed.wrapping_add(1);
            return None;
        };
        let params = [dec::Message {
            fields: &fields[..nf],
        }];
        let mut builder = dec::Builder::new();
        let mut sbuf = [0u8; dec::STAGE_SCRATCH_CAP];
        let mut scratch = dec::Scratch::new(&mut sbuf);
        let mut spent = 0u64;
        let cont = self.dec_cont.get(..self.dec_len as usize)?;
        if dec::run_decision_metered(cont, &params, &mut builder, &mut scratch, &mut spent).is_err()
        {
            self.dec_failed = self.dec_failed.wrapping_add(1);
            // Logged at the 1st, 2nd, 4th, … failure: the record is not
            // served, and nothing else would say why.
            if self.dec_failed.is_power_of_two() {
                let mut m = [0u8; 200];
                let mut n = 0usize;
                let mut put = |part: &[u8]| {
                    let k = part.len().min(m.len() - n);
                    m[n..n + k].copy_from_slice(&part[..k]);
                    n += k;
                };
                put(b"[store_doc] ");
                put(&self.key_prefix[..self.key_prefix_len as usize]);
                put(b": the fused decision failed closed: a rule read a field the record does not carry, or did not evaluate");
                unsafe { dev_log(self.sys(), 1, m.as_ptr(), n) };
            }
            return None;
        }
        if builder.message().fields.is_empty() {
            self.dec_dropped = self.dec_dropped.wrapping_add(1);
            return None;
        }
        match dec::encode_frame_scratch(&builder.message(), &scratch, &mut self.dec_buf) {
            Ok(m) => Some(m),
            Err(_) => {
                self.dec_failed = self.dec_failed.wrapping_add(1);
                None
            }
        }
    }

    /// Serve the request in `req`: answered at once (NOOP, PUT, refusals)
    /// or sent to the dialect (PHASE_SEND).
    fn serve(&mut self) {
        let rl = (self.req_len as usize).min(BUF);
        // SAFETY: `rl` bytes of `req`, which nothing below writes.
        let req = unsafe { core::slice::from_raw_parts(self.req.as_ptr(), rl) };
        let op = frame_i64(req, 2).unwrap_or(OP_NOOP);
        self.op = op;
        // The key: the prefix, then parts 3, 17, 18 (an integer as digits).
        let mut full = [0u8; MAX_KEY];
        let pl = self.key_prefix_len as usize;
        full[..pl].copy_from_slice(&self.key_prefix[..pl]);
        let mut kl = pl;
        for f in [3u8, 17, 18] {
            let mut digits = [0u8; 24];
            let p = part(frame_field(req, f), &mut digits);
            let Some(d) = full.get_mut(kl..kl + p.len()) else {
                self.reply(400, &[]);
                return;
            };
            d.copy_from_slice(p);
            kl += p.len();
        }
        let (key, marks) = view_split(&full[..kl]);
        let (view, manager, force, dry) = view_args(marks);
        self.key[..key.len()].copy_from_slice(key);
        self.key_len = key.len() as u16;
        // The value: parts 4..16 joined, an integer as digits.
        let mut value = [0u8; BUF];
        let mut vl = 0usize;
        for f in 4u8..=16 {
            let mut digits = [0u8; 24];
            let p = part(frame_field(req, f), &mut digits);
            let n = p.len().min(BUF - vl);
            value[vl..vl + n].copy_from_slice(&p[..n]);
            vl += n;
        }
        match op {
            OP_NOOP => return self.reply(204, &[]),
            OP_PUT => {
                // SAFETY: storage.object through the loader's table.
                let ok = unsafe { done(put_value(&self.store(), key, &value[..vl])) };
                return self.reply(if ok { 200 } else { 500 }, &[]);
            }
            OP_GET => {
                let mut got = [0u8; BUF];
                // SAFETY: storage.object through the loader's table.
                let read = unsafe { get_value(&self.store(), key, &mut got) };
                return match read {
                    Some(n) => self.reply(200, &got[..n]),
                    None => self.reply(404, &[]),
                };
            }
            OP_TAKE => {
                let st = self.store();
                let mut got = [0u8; BUF];
                // SAFETY: storage.object through the loader's table.
                let taken = unsafe {
                    match (head(&st, key), get_value(&st, key, &mut got)) {
                        (Some(at), Some(n)) if at > 0 => {
                            (code(delete_cond(&st, key, PRE_ETAG, at)) == 0).then_some(n)
                        }
                        _ => None,
                    }
                };
                return match taken {
                    Some(n) => self.reply(200, &got[..n]),
                    None => self.reply(404, &[]),
                };
            }
            OP_CREATE | OP_REPLACE | OP_PATCH_MERGE | OP_PATCH_JSON | OP_PATCH_SMP | OP_APPLY
            | OP_DELETE => {}
            _ => {
                self.refused = self.refused.wrapping_add(1);
                return self.reply(400, &[]);
            }
        }
        // `/<collection>/<id…>`: the path codec's key, read back.
        let Some(path) = key.strip_prefix(b"/") else {
            return self.reply(400, &[]);
        };
        let mut segs: [&[u8]; 6] = [&[]; 6];
        let mut ns = 0usize;
        for s in path.split(|b| *b == b'/') {
            if ns == segs.len() {
                return self.reply(400, &[]);
            }
            segs[ns] = s;
            ns += 1;
        }
        if ns < 2 || !plain_segment(segs[0]) {
            return self.reply(400, &[]);
        }
        let coll = segs[0];
        // A delete leaves the object's last state for the watch chain's
        // DELETED event BEFORE the delete: the chain reads the tomb when it
        // meets the delete in the change feed, which can be before this
        // node sees the answer. A delete that finds nothing, or is held by
        // a finalizer, leaves a tomb nothing reads; it expires.
        if op == OP_DELETE && self.tomb_len > 0 && view.is_empty() {
            // SAFETY: storage.object reads through the loader's table.
            if let Some((p, n)) = unsafe { read_whole(&self.store(), key, DOC_MAX) } {
                // SAFETY: `n` bytes read_whole filled at `p`, freed below.
                let obj = unsafe { core::slice::from_raw_parts(p, n) };
                self.leave_tomb(obj);
                // SAFETY: read_whole's allocation, no longer read.
                unsafe { heap_free(self.sys(), p) };
            }
        }
        // A create whose key ends at the collection or namespace names no
        // object: its name is generated, its namespace stated.
        let generated = op == OP_CREATE && segs[ns - 1].is_empty();
        let ids = if generated {
            &segs[1..ns - 1]
        } else {
            &segs[1..ns]
        };
        let mut idb = [0u8; MAX_KEY + 16];
        let il = if generated && ids.is_empty() {
            0
        } else {
            match id_json(ids, generated, &mut idb) {
                Some(n) => n,
                None => return self.reply(400, &[]),
            }
        };
        // A staged body: its bytes, read whole onto the heap.
        let mut staged: *mut u8 = core::ptr::null_mut();
        let body: &[u8] = if self.blob_len > 0 && value[..vl].starts_with(b"sha256:") {
            let mut bk = [0u8; MAX_KEY];
            let bl = self.blob_len as usize;
            let hex = &value[7..vl];
            let Some(d) = bk.get_mut(bl..bl + hex.len()) else {
                return self.reply(413, &[]);
            };
            d.copy_from_slice(hex);
            bk[..bl].copy_from_slice(&self.blob[..bl]);
            // SAFETY: storage.object reads through the loader's table.
            match unsafe { read_whole(&self.store(), &bk[..bl + hex.len()], DOC_MAX) } {
                Some((p, n)) => {
                    staged = p;
                    // SAFETY: `n` bytes read_whole filled at `p`.
                    unsafe { core::slice::from_raw_parts(p, n) }
                }
                None => {
                    let m = b"[store_doc] a staged body is gone or past the heap - REFUSED";
                    // SAFETY: logging through the loader's table.
                    unsafe { dev_log(self.sys(), 1, m.as_ptr(), m.len()) };
                    return self.reply(413, &[]);
                }
            }
        } else {
            &value[..vl]
        };
        let (dop, body_tag) = match op {
            OP_CREATE => (doc_exec::OP_INSERT, J_DOC),
            OP_REPLACE => (doc_exec::OP_REPLACE, J_DOC),
            OP_PATCH_MERGE => (doc_exec::OP_PATCH_MERGE, J_PATCH),
            OP_PATCH_JSON => (doc_exec::OP_PATCH_JSON, J_PATCH),
            OP_PATCH_SMP => (doc_exec::OP_PATCH_KEYED, J_PATCH),
            OP_APPLY => (doc_exec::OP_APPLY, J_DOC),
            _ => (doc_exec::OP_DELETE, 0),
        };
        let flags = if force { FLAG_FORCE } else { 0 } | if dry { FLAG_DRY_RUN } else { 0 };
        let manager: &[u8] = if manager.is_empty() {
            b"unknown"
        } else {
            manager
        };
        // The revision the request states (field 28); the one a body
        // states is the dialect's to read (`rv.if`, the schema's).
        let expected = frame_i64(req, 28).filter(|r| *r > 0);
        self.corr = self.corr.wrapping_add(1);
        let corr = self.corr;
        let n = {
            let out = self
                .msg_buf()
                .get_mut(wire::ENVELOPE_HDR..)
                .unwrap_or(&mut []);
            (|| {
                let mut w = FieldWriter::request(out, corr, DIALECT_KUBERNETES, 0, 0, dop, flags)?;
                w.field(J_COLLECTION, &[coll])?;
                if il > 0 {
                    let tag = if generated { J_ID_PREFIX } else { J_ID };
                    w.field(tag, &[&idb[..il]])?;
                }
                if body_tag != 0 {
                    w.field(body_tag, &[body])?;
                }
                w.field(J_MANAGER, &[manager])?;
                if !view.is_empty() {
                    w.field(J_VIEW, &[view])?;
                    w.field(J_SCOPE_NAME, &[view])?;
                }
                if let Some(r) = expected {
                    w.field(J_IF_REVISION, &[&(r as u64).to_le_bytes()])?;
                }
                Some(w.finish())
            })()
        };
        if !staged.is_null() {
            // SAFETY: read_whole's allocation, no longer read.
            unsafe { heap_free(self.sys(), staged) };
        }
        let Some(n) = n else {
            return self.reply(413, &[]);
        };
        self.msg_len = n as u32;
        self.parts = wire::PartWriter::new(corr as u32, wire::MSG_JSON_DOC_REQUEST, n);
        self.reasm.reset();
        // SAFETY: the clock through the loader's table.
        self.sent_ms = unsafe { dev_millis(self.sys()) };
        self.phase = PHASE_SEND;
    }

    /// Send what is left of the request: `true` once all of it has gone.
    fn send(&mut self) -> bool {
        let len = self.msg_len as usize;
        let chan = self.doc_out;
        let sys = self.syscalls;
        // SAFETY: the loader's table, set in module_new.
        let sys = unsafe { &*sys };
        if len <= wire::FRAME_PAYLOAD_MAX {
            let buf = self.msg_buf();
            if !wire::write_envelope_hdr(buf, wire::MSG_JSON_DOC_REQUEST, len) {
                return true;
            }
            let total = wire::ENVELOPE_HDR + len;
            // SAFETY: `total` bytes of the heap message.
            return unsafe { (sys.channel_write)(chan, buf.as_mut_ptr(), total) } == total as i32;
        }
        while !self.parts.done() {
            let (msg, frame) = (self.msg, &mut self.frame);
            // SAFETY: MSG_CAP bytes of the heap message; `len` of them written.
            let body = unsafe { core::slice::from_raw_parts(msg.add(wire::ENVELOPE_HDR), len) };
            let Some(n) = self.parts.frame(body, frame) else {
                return true;
            };
            // SAFETY: `n` bytes of the frame buffer.
            if unsafe { (sys.channel_write)(chan, frame.as_mut_ptr(), n) } != n as i32 {
                return false;
            }
            self.parts.sent();
        }
        true
    }

    /// Take the dialect's answer when it has come, and reply.
    fn receive(&mut self) {
        let sys = self.syscalls;
        // SAFETY: the loader's table, set in module_new.
        let sys = unsafe { &*sys };
        for _ in 0..64 {
            // SAFETY: reads into this module's own buffer.
            let Some((mt, n)) = (unsafe { read_envelope(sys, self.doc_in, &mut self.env) }) else {
                break;
            };
            let whole = match mt {
                wire::MSG_JSON_DOC_RESPONSE => {
                    // SAFETY: `n` bytes of `env`, which nothing below writes.
                    let env = unsafe { core::slice::from_raw_parts(self.env.as_ptr(), n) };
                    let Some(d) = self.ans_buf().get_mut(..n) else {
                        continue;
                    };
                    d.copy_from_slice(env);
                    Some(n)
                }
                wire::MSG_PART => {
                    // SAFETY: `n` bytes of `env`, which nothing below writes.
                    let env = unsafe { core::slice::from_raw_parts(self.env.as_ptr(), n) };
                    let (mut r, ans) = (self.reasm, self.ans);
                    // SAFETY: MSG_CAP bytes of the heap answer buffer.
                    let buf = unsafe { core::slice::from_raw_parts_mut(ans, MSG_CAP) };
                    let got = r.take(env, buf);
                    self.reasm = r;
                    match got {
                        Ok(Some((wire::MSG_JSON_DOC_RESPONSE, total))) => Some(total),
                        _ => None,
                    }
                }
                _ => None,
            };
            if let Some(total) = whole {
                if self.answer(total) {
                    return;
                }
            }
        }
        // SAFETY: the clock through the loader's table.
        let now = unsafe { dev_millis(sys) };
        if now.saturating_sub(self.sent_ms) > ANSWER_MS {
            self.timeouts = self.timeouts.wrapping_add(1);
            let m = b"[store_doc] the dialect did not answer in time - 503";
            // SAFETY: logging through the loader's table.
            unsafe { dev_log(self.sys(), 1, m.as_ptr(), m.len()) };
            self.reply(503, &[]);
        }
    }

    /// The answer in `ans[..total]`: replied when it is this request's.
    fn answer(&mut self, total: usize) -> bool {
        let ans = self.ans;
        if ans.is_null() {
            return false;
        }
        // SAFETY: `total` bytes of the heap answer buffer, read-only below
        // (the reply and the tomb write elsewhere).
        let body = unsafe { core::slice::from_raw_parts(ans, total) };
        let Some(a) = JsonResponse::decode_up_to(body, MSG_CAP) else {
            return false;
        };
        if a.corr_id != self.corr {
            // A late answer to a request that already timed out.
            return false;
        }
        let status = status_of(a.outcome, a.flags, self.op);
        let text = a.field(JR_TEXT).unwrap_or(&[]);
        // A refusal the client cannot act on (too large, unavailable,
        // anything internal) is said here with the engine's reason; the
        // rest (absent, conflicts, invalid) are the client's answers.
        if let Some(e) = a
            .field(JR_ERROR)
            .filter(|e| !e.is_empty() && (status == 413 || status >= 500))
        {
            let mut m = [0u8; 200];
            let mut l = append(&mut m, 0, b"[store_doc] refused: ");
            l = append(&mut m, l, e);
            // SAFETY: logging through the loader's table.
            unsafe { dev_log(self.sys(), 2, m.as_ptr(), l.min(m.len())) };
        }
        self.reply(status, text);
        true
    }

    /// Leave `obj` as the deleted key's tombstone (store_big's `tomb`).
    fn leave_tomb(&mut self, obj: &[u8]) {
        let tl = self.tomb_len as usize;
        let kl = self.key_len as usize;
        if tl == 0 || obj.is_empty() || tl + kl > MAX_KEY {
            return;
        }
        let mut tk = [0u8; MAX_KEY];
        tk[..tl].copy_from_slice(&self.tomb[..tl]);
        tk[tl..tl + kl].copy_from_slice(&self.key[..kl]);
        let mut digits = [0u8; 24];
        // SAFETY: the clock through the loader's table.
        let deadline = unsafe { dev_millis(self.sys()) }.saturating_add(self.tomb_ms as u64);
        let dl = dec(deadline as i64, &mut digits);
        // The request message is spent: the tomb is assembled there.
        let v = self.msg_buf();
        let mut l = 0usize;
        l = append(v, l, b"{\"deadline\":");
        l = append(v, l, &digits[..dl]);
        l = append(v, l, b",\"obj\":");
        l = append(v, l, obj);
        l = append(v, l, b"}");
        if l >= v.len() {
            return;
        }
        let (msg, store) = (self.msg, self.store());
        // SAFETY: `l` bytes of the heap message just written. A tomb the
        // store has not decided holds the request: served again, its
        // journal asks the same write again.
        unsafe {
            let v = core::slice::from_raw_parts(msg, l);
            let _ = put_value(&store, &tk[..tl + kl], v);
        }
    }
}

/// One envelope from `chan` into `buf`: `(type, payload length)`.
unsafe fn read_envelope(sys: &SyscallTable, chan: i32, buf: &mut [u8]) -> Option<(u8, usize)> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; wire::ENVELOPE_HDR];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), wire::ENVELOPE_HDR) < wire::ENVELOPE_HDR as i32 {
        return None;
    }
    let n = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if n > buf.len() {
        let mut sink = [0u8; 256];
        let mut left = n;
        while left > 0 {
            let k = left.min(sink.len());
            if (sys.channel_read)(chan, sink.as_mut_ptr(), k) <= 0 {
                break;
            }
            left -= k;
        }
        return None;
    }
    if n > 0 && ((sys.channel_read)(chan, buf.as_mut_ptr(), n) as usize) < n {
        return None;
    }
    Some((hdr[0], n))
}

unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    let mut i = 0usize;
    while i < n {
        *dst.add(i) = *src.add(i);
        i += 1;
    }
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PREFIX;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, key_prefix, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.key_prefix_len = n as u8;
                if n > 0 { ptr_copy(s.key_prefix.as_mut_ptr(), d, n); }
            };

        2, tomb, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.tomb_len = n as u8;
                if n > 0 { ptr_copy(s.tomb.as_mut_ptr(), d, n); }
            };

        // How long a tomb stays, ms (default TOMB_MS).
        4, tomb_ms, u32, 300000
            => |s, d, len| { s.tomb_ms = super::p_u32(d, len, 0, super::TOMB_MS); };

        3, blob, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.blob_len = n as u8;
                if n > 0 { ptr_copy(s.blob.as_mut_ptr(), d, n); }
            };

        // `blob`: the container's own bytes, in chunks of at most 255, the
        // first led by its whole length (chronicle blob_core). One cut short
        // or past its buffer is a fault, never a truncation.
        15, decision, blob, 0
            => |s, d, len| { s.dec_in.take(&mut s.dec_cont, core::slice::from_raw_parts(d, len)); };
    }
}

#[no_mangle]
#[link_section = ".text.module_arena_size"]
pub extern "C" fn module_arena_size() -> u32 {
    ARENA as u32
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
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<State>() {
            return -2;
        }
        let s = &mut *(state as *mut State);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = syscalls as *const SyscallTable;
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.doc_in = dev_channel_port(sys, PORT_INPUT, 1);
        s.doc_out = dev_channel_port(sys, PORT_OUTPUT, 1);
        s.out_at = 0;
        s.out_len = 0;
        s.phase = PHASE_IDLE;
        s.corr = 0;
        s.dec_in = dec::BlobIn::new();
        s.dec_len = 0;
        s.dec_fault = 0;
        s.key_prefix_len = 0;
        s.tomb_len = 0;
        s.blob_len = 0;
        s.parts = wire::PartWriter::new(0, 0, 0);
        s.reasm = wire::Reassembly::new();
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.dec_in.given() {
            match s.dec_in.whole() {
                Some(n) if dec::scan_decision_container(&s.dec_cont[..n]).is_ok() => {
                    s.dec_len = n as u16;
                }
                _ => s.dec_fault = 1,
            }
        }
        if s.dec_fault != 0 {
            let m = b"[store_doc] FAULT: decision param invalid, truncated, or needs a builtin not in this build";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        if s.doc_in < 0 || s.doc_out < 0 {
            return -3;
        }
        s.msg = heap_alloc(sys, MSG_CAP as u32);
        s.ans = heap_alloc(sys, MSG_CAP as u32);
        if s.msg.is_null() || s.ans.is_null() {
            let m = b"[store_doc] no heap for its message buffers";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -4;
        }
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return 0;
    }
    // SAFETY: the loader's instance arena, constructed by module_new.
    let s = unsafe { &mut *(state as *mut State) };
    if s.syscalls.is_null() || s.request_in < 0 || s.dec_fault != 0 {
        return 0;
    }
    // What is owed goes first; nothing new is read until it has.
    if !s.flush_reply() {
        return 0;
    }
    match s.phase {
        PHASE_SEND => {
            if s.send() {
                s.phase = PHASE_WAIT;
            }
            return 0;
        }
        PHASE_WAIT => {
            s.receive();
            if s.out_len > 0 {
                s.flush_reply();
            }
            return 0;
        }
        _ => {}
    }
    if s.held == 0 {
        let Some(n) = s.read_request() else {
            return 0;
        };
        let len = if s.dec_len > 0 {
            match s.decide(n) {
                Some(l) => {
                    let (d, r) = (&s.dec_buf, &mut s.req);
                    r[..l].copy_from_slice(&d[..l]);
                    l
                }
                None => return 0,
            }
        } else {
            let (d, r) = (&s.in_buf, &mut s.req);
            r[..n].copy_from_slice(&d[..n]);
            n
        };
        s.req_len = len as u32;
        s.journal.reset();
    } else {
        s.journal.begin();
    }
    s.serve();
    if s.journal.held() {
        // A write of this request is undecided: nothing the serve staged
        // goes out, and the request is served again next step.
        s.held = 1;
        s.out_len = 0;
        s.out_at = 0;
        s.phase = PHASE_IDLE;
        return 0;
    }
    s.held = 0;
    if s.phase == PHASE_SEND && s.send() {
        s.phase = PHASE_WAIT;
    }
    s.flush_reply();
    0
}
