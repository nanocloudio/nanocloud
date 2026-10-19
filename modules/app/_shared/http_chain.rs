// An HTTP application route as a Chronicle chain, without I/O: the provider
// half that sits behind Wave's `http` server (`http.request_out` ->
// `request_in`, `response_out` -> `http.response_in`), turns each exchange
// into ONE request envelope for the chain, and the chain's reply envelopes
// back into the exchange's response. Mounted by `http_chain/mod.rs` and by
// `tests/harness`; the records are the SDK's exchange contract
// (`abi::contracts::exchange`, which the includer mounts).
//
// Why a module and not the chain itself: a chain record carries its
// correlation as an integer through every stage and connector, and the
// exchange id is 14 opaque bytes; a watch answers with many records under one
// exchange, past the response credit the server grants; and a body larger
// than one record is staged in the store and handed on by reference. Each of
// those holds state across records, which a stage cannot.
//
// REQUEST ENVELOPE (chain-bound, one record, little-endian):
//
//   [cid u32][method u8][flags u8][target_len u16][hdr_len u16][body_len u16]
//   target | headers | body | peer trailer
//
//   cid      this module's correlation for the exchange, never 0 or
//            0xFFFF_FFFF (a reply to 0xFFFF_FFFF is a reply to no one)
//   method   the exchange method byte (GET 1, POST 3, ...)
//   flags    0x02 a verified mutual-TLS peer follows the body as
//            `[len u16][peer]` (the exchange's `peer`); 0x04 the body was
//            staged and `body` is `sha256:<hex>`
//
// REPLY ENVELOPE (from the chain, one record, little-endian):
//
//   [cid u32][status u16][flags u8][ct_len u8][hdr_len u16][body_len u16]
//   content type | headers | body
//
//   flags    0x01 more envelopes follow for this cid (the body continues);
//            0x04 the response is held open on purpose (a watch): the
//            exchange's HOLD
//
// The first reply envelope gives the status, content type and headers; every
// envelope's body is the next stretch of the response body. The response goes
// to the server within the credit it grants; what the server has not yet
// taken is held for the exchange, up to `stash_max` bytes, past which the
// exchange is aborted as too slow rather than holding every other exchange
// up behind it.
//
// Refusals, as the exchange contract names them: 503 when every slot is
// taken (or the store could not hold the request), 413 for a body past
// `max_body` or an envelope past one record, 500 when a body could not be
// staged.

use abi::contracts::exchange as hx;

/// Request envelope head.
const HC_REQ_HEAD: usize = 12;
/// Reply envelope head.
const HC_REPLY_HEAD: usize = 12;
/// The largest envelope either way: one channel record.
const HC_ENV_MAX: usize = 8192;
/// Request envelope flags.
const HC_PEER: u8 = 0x02;
const HC_BODY_REF: u8 = 0x04;
/// Reply envelope flags.
const HC_MORE: u8 = 0x01;
const HC_HOLD: u8 = 0x04;
/// `sha256:` and 64 hex digits.
const HC_REF_LEN: usize = 7 + 64;
/// The cid no exchange is given: a reply to it is a reply to no one.
const HC_CID_NONE: u32 = u32::MAX;
/// The largest content type and header block a response keeps.
const HC_CT_MAX: usize = 128;
const HC_HDR_MAX: usize = 512;
/// Refusals owed to exchanges that never got a slot.
const HC_REFUSALS: usize = 16;
/// How long an exchange may wait for the chain's first reply envelope,
/// from its head, before this desk answers it 503 itself (with
/// `Retry-After`). Wave's `http` server gives an application 30 s from its
/// last progress (`APP_TIMEOUT_MS`; the credit this desk grants as the head
/// arrives) and then answers 504 for it: an internal stall — a store too
/// busy to decide a 1 MiB replace in time — must reach the client as the
/// retryable 503 it is, before that. The chain's late reply answers no one.
const HC_ANSWER_MS: u64 = 28_000;
/// The late answer: upstream's Status for a request the server could not
/// decide in time, which a client retries after `Retry-After`. The write
/// may still land (as a write whose etcd request timed out may).
const HC_LATE_CT: &[u8] = b"application/json";
const HC_LATE_HDRS: &[u8] = b"Retry-After: 1\r\n";
const HC_LATE_BODY: &[u8] = b"{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"the request could not be decided in time; it may still be applied - retry\",\"reason\":\"ServiceUnavailable\",\"details\":{\"retryAfterSeconds\":1},\"code\":503}";

const HC_FREE: u8 = 0;
/// The request body is still arriving.
const HC_COLLECT: u8 = 1;
/// The body is past the inline bound: owed to the store.
const HC_STAGE: u8 = 2;
/// The envelope is owed to the chain.
const HC_READY: u8 = 3;
/// At the chain: its reply envelopes become the response.
const HC_CHAIN: u8 = 4;
/// A refusal status is owed, then the slot frees.
const HC_REFUSE: u8 = 5;
/// An ABORT is owed, then the slot frees.
const HC_ABORT: u8 = 6;

/// Where a module's growable buffers come from: its heap arena, or a host
/// suite's allocator.
trait HcHeap {
    /// Grow (or first allocate, from null) to `cap` bytes; null when the
    /// arena cannot, the old block left as it was.
    unsafe fn grow(&mut self, p: *mut u8, cap: u32) -> *mut u8;
    unsafe fn free(&mut self, p: *mut u8);
}

/// A byte buffer on the heap: `ptr[start..end]` is live.
#[derive(Clone, Copy)]
struct HcBytes {
    ptr: *mut u8,
    cap: u32,
    start: u32,
    end: u32,
}

impl HcBytes {
    const EMPTY: HcBytes = HcBytes {
        ptr: core::ptr::null_mut(),
        cap: 0,
        start: 0,
        end: 0,
    };

    fn len(&self) -> usize {
        (self.end - self.start) as usize
    }

    fn bytes(&self) -> &[u8] {
        if self.ptr.is_null() || self.end == self.start {
            return &[];
        }
        // SAFETY: `ptr[..cap]` is the block this buffer owns, and
        // `start <= end <= cap`.
        unsafe { core::slice::from_raw_parts(self.ptr.add(self.start as usize), self.len()) }
    }

    /// Append `data`, growing the block (doubling, at most `limit` live
    /// bytes). False, and nothing appended, when it would pass `limit` or the
    /// heap cannot grow it.
    fn push<H: HcHeap>(&mut self, heap: &mut H, data: &[u8], limit: usize) -> bool {
        if data.is_empty() {
            return true;
        }
        let Some(want) = self.len().checked_add(data.len()) else {
            return false;
        };
        if want > limit || want > u32::MAX as usize {
            return false;
        }
        // Reclaim what has been consumed, only when the tail has no room:
        // a stream drained as it fills never moves its bytes.
        let tail_room = (self.cap - self.end) as usize;
        if self.start != 0 && data.len() > tail_room {
            let live = self.len();
            if live != 0 {
                // SAFETY: both ranges lie inside the owned block.
                unsafe {
                    core::ptr::copy(self.ptr.add(self.start as usize), self.ptr, live);
                }
            }
            self.start = 0;
            self.end = live as u32;
        }
        if want > self.cap as usize {
            let mut cap = (self.cap as usize).max(256);
            while cap < want {
                cap = cap.saturating_mul(2);
            }
            let cap = cap.min(limit.max(want)).min(u32::MAX as usize);
            // SAFETY: `ptr` is null or this buffer's own block.
            let p = unsafe { heap.grow(self.ptr, cap as u32) };
            if p.is_null() {
                return false;
            }
            self.ptr = p;
            self.cap = cap as u32;
        }
        // SAFETY: `end + data.len() <= cap` by the growth above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.ptr.add(self.end as usize),
                data.len(),
            );
        }
        self.end += data.len() as u32;
        true
    }

    fn consume(&mut self, n: usize) {
        let n = n.min(self.len()) as u32;
        self.start += n;
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
    }

    fn release<H: HcHeap>(&mut self, heap: &mut H) {
        if !self.ptr.is_null() {
            // SAFETY: the block is this buffer's, and nothing else holds it.
            unsafe { heap.free(self.ptr) };
        }
        *self = HcBytes::EMPTY;
    }
}

#[derive(Clone, Copy)]
struct HcSlot {
    st: u8,
    id: hx::ExchangeId,
    cid: u32,
    /// Order the chain sees requests in: when they became whole.
    seq: u32,
    method: u8,
    target_len: u16,
    hdr_len: u16,
    peer_len: u16,
    /// `target | headers | peer | body`, whole once collected.
    req: HcBytes,
    /// `sha256:<hex>` once the body is staged.
    body_ref: [u8; HC_REF_LEN],
    staged: bool,
    /// Request-body credit owed to the server.
    grant: u32,
    /// The status a refusal answers with.
    refuse: u16,
    /// When the exchange's head arrived (the desk's clock, [`HcDesk::tick`]).
    opened_ms: u64,
    /// The refusal is the late answer ([`HC_ANSWER_MS`]).
    late: bool,
    abort: u8,

    // The response.
    credit: u32,
    head_set: bool,
    head_sent: bool,
    done: bool,
    status: u16,
    hold: bool,
    ct: [u8; HC_CT_MAX],
    ct_len: u8,
    hdrs: [u8; HC_HDR_MAX],
    hdrs_len: u16,
    /// Response body the server has not yet taken.
    stash: HcBytes,
}

impl HcSlot {
    const EMPTY: HcSlot = HcSlot {
        st: HC_FREE,
        id: hx::ExchangeId::NONE,
        cid: 0,
        seq: 0,
        method: 0,
        target_len: 0,
        hdr_len: 0,
        peer_len: 0,
        req: HcBytes::EMPTY,
        body_ref: [0; HC_REF_LEN],
        staged: false,
        grant: 0,
        refuse: 0,
        opened_ms: 0,
        late: false,
        abort: 0,
        credit: 0,
        head_set: false,
        head_sent: false,
        done: false,
        status: 0,
        hold: false,
        ct: [0; HC_CT_MAX],
        ct_len: 0,
        hdrs: [0; HC_HDR_MAX],
        hdrs_len: 0,
        stash: HcBytes::EMPTY,
    };

    fn parts_len(&self) -> usize {
        self.target_len as usize + self.hdr_len as usize + self.peer_len as usize
    }

    fn body(&self) -> &[u8] {
        self.req.bytes().get(self.parts_len()..).unwrap_or(&[])
    }
}

/// The transcoder's sink: the request's new buffer, grown on the heap.
struct HcSink<'a, H: HcHeap> {
    buf: &'a mut HcBytes,
    heap: &'a mut H,
    limit: usize,
    full: bool,
}

impl<H: HcHeap> PbSink for HcSink<'_, H> {
    fn put(&mut self, b: &[u8]) -> bool {
        let ok = self.buf.push(self.heap, b, self.limit);
        self.full |= !ok;
        ok
    }
}

/// The `name: value\r\n` lines of an exchange header block, each with its
/// line end.
fn hc_header_lines(h: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut at = 0usize;
    core::iter::from_fn(move || {
        if at >= h.len() {
            return None;
        }
        let rest = h.get(at..)?;
        let n = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|i| i + 2)
            .unwrap_or(rest.len());
        at += n;
        rest.get(..n)
    })
}

/// Whether header line `line` is named `name` (case aside).
fn hc_header_name_is(line: &[u8], name: &[u8]) -> bool {
    line.len() > name.len()
        && line.get(name.len()) == Some(&b':')
        && line
            .get(..name.len())
            .is_some_and(|n| n.eq_ignore_ascii_case(name))
}

/// Whether the block's content type is `ct` (parameters aside).
fn hc_content_type_is(h: &[u8], ct: &[u8]) -> bool {
    hc_header_lines(h).any(|line| {
        if !hc_header_name_is(line, b"content-type") {
            return false;
        }
        let v = line.get(b"content-type:".len()..).unwrap_or(&[]);
        let v = v.trim_ascii();
        let v = v.split(|&c| c == b';').next().unwrap_or(&[]).trim_ascii();
        v.eq_ignore_ascii_case(ct)
    })
}

/// What a record the module is to write to the server stands for, so the
/// desk commits it once the channel took it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum HcOut {
    /// A refusal owed to an exchange that never got a slot.
    Queued,
    Grant(usize),
    Refusal(usize),
    Abort(usize),
    /// `consumed` body bytes of slot `.0`'s stash; `.2` ends the response.
    Response(usize, usize, bool),
}

/// Bounds a desk enforces.
#[derive(Clone, Copy)]
struct HcConfig {
    /// The largest request body taken (413 past it).
    max_body: u32,
    /// A body past this is staged rather than inlined; 0 never stages, and a
    /// body that does not fit one envelope is then refused.
    inline_max: u32,
    /// Response bytes held for one exchange past its credit.
    stash_max: u32,
}

/// The exchanges in hand: `SLOTS` at once.
struct HcDesk<const SLOTS: usize> {
    cfg: HcConfig,
    slots: [HcSlot; SLOTS],
    next_cid: u32,
    next_seq: u32,
    /// Where the round-robin over slots starts next.
    turn: usize,
    queued: [(hx::ExchangeId, u16); HC_REFUSALS],
    queued_len: usize,
    /// The module's clock as of its last [`HcDesk::tick`]; 0 before one.
    now_ms: u64,
    /// Counters.
    taken: u32,
    refused: u32,
    stray: u32,
    aborted: u32,
}

impl<const SLOTS: usize> HcDesk<SLOTS> {
    /// Reset in place: a module's state is zeroed memory, never `new()`.
    fn reset(&mut self, cfg: HcConfig) {
        self.cfg = cfg;
        for s in self.slots.iter_mut() {
            *s = HcSlot::EMPTY;
        }
        self.next_cid = 0;
        self.next_seq = 0;
        self.turn = 0;
        self.queued_len = 0;
        self.now_ms = 0;
        self.taken = 0;
        self.refused = 0;
        self.stray = 0;
        self.aborted = 0;
    }

    /// The step's clock: every exchange the chain has not begun to answer
    /// within [`HC_ANSWER_MS`] of its head is answered 503 now. The number
    /// so answered.
    fn tick<H: HcHeap>(&mut self, heap: &mut H, now: u64) -> u32 {
        self.now_ms = now;
        let mut late = 0u32;
        for at in 0..SLOTS {
            let Some(s) = self.slots.get_mut(at) else {
                break;
            };
            if s.st == HC_FREE {
                continue;
            }
            if s.opened_ms == 0 {
                s.opened_ms = now;
            }
            let waiting = matches!(s.st, HC_STAGE | HC_READY | HC_CHAIN) && !s.head_set;
            if waiting && now.saturating_sub(s.opened_ms) >= HC_ANSWER_MS {
                s.stash.release(heap);
                self.refuse(heap, at, hx::status::BUSY);
                if let Some(s) = self.slots.get_mut(at) {
                    s.late = true;
                }
                late += 1;
            }
        }
        late
    }

    /// Whether a request record can be taken now: a refusal to an exchange
    /// that finds no slot must have somewhere to wait.
    fn can_take(&self) -> bool {
        self.queued_len < HC_REFUSALS
    }

    fn find(&self, id: &hx::ExchangeId) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.st != HC_FREE && s.id == *id)
    }

    fn find_cid(&self, cid: u32) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.st == HC_CHAIN && s.cid == cid)
    }

    fn free<H: HcHeap>(&mut self, heap: &mut H, at: usize) {
        if let Some(s) = self.slots.get_mut(at) {
            s.req.release(heap);
            s.stash.release(heap);
            *s = HcSlot::EMPTY;
        }
    }

    fn refuse<H: HcHeap>(&mut self, heap: &mut H, at: usize, status: u16) {
        if let Some(s) = self.slots.get_mut(at) {
            s.req.release(heap);
            s.st = HC_REFUSE;
            s.refuse = status;
            s.grant = 0;
        }
        self.refused = self.refused.wrapping_add(1);
    }

    fn queue_refusal(&mut self, id: hx::ExchangeId, status: u16) {
        if let Some(q) = self.queued.get_mut(self.queued_len) {
            *q = (id, status);
            self.queued_len += 1;
        }
        self.refused = self.refused.wrapping_add(1);
    }

    fn new_cid(&mut self) -> u32 {
        loop {
            self.next_cid = self.next_cid.wrapping_add(1);
            let c = self.next_cid;
            if c != 0
                && c != HC_CID_NONE
                && self.slots.iter().all(|s| s.st == HC_FREE || s.cid != c)
            {
                return c;
            }
        }
    }

    /// Take one request-direction record from the server.
    fn take_request<H: HcHeap>(&mut self, heap: &mut H, record: &[u8]) {
        let Some(rec) = hx::parse_request(record) else {
            return;
        };
        match rec {
            hx::Record::Head(h) => self.open(heap, &h),
            hx::Record::Body { id, flags, data } => {
                let Some(at) = self.find(&id) else {
                    return;
                };
                if self.slots[at].st != HC_COLLECT {
                    return;
                }
                let limit = self.slots[at].parts_len() + self.cfg.max_body as usize;
                if !self.slots[at].req.push(heap, data, limit) {
                    let status = if self.slots[at].req.len() + data.len() > limit {
                        hx::status::TOO_LARGE
                    } else {
                        hx::status::BUSY
                    };
                    self.refuse(heap, at, status);
                    return;
                }
                if flags & hx::flag::MORE == 0 {
                    self.whole(heap, at);
                }
            }
            hx::Record::Abort { id, .. } => {
                if let Some(at) = self.find(&id) {
                    self.free(heap, at);
                    self.aborted = self.aborted.wrapping_add(1);
                }
            }
            hx::Record::Credit { id, bytes } => {
                if let Some(at) = self.find(&id) {
                    let s = &mut self.slots[at];
                    s.credit = s.credit.saturating_add(bytes);
                }
            }
            hx::Record::Datagram { .. } | hx::Record::Link { .. } => {}
        }
    }

    fn open<H: HcHeap>(&mut self, heap: &mut H, h: &hx::RequestHead<'_>) {
        // A repeat HEAD for an exchange in hand restarts it.
        if let Some(at) = self.find(&h.id) {
            self.free(heap, at);
        }
        let Some(at) = self.slots.iter().position(|s| s.st == HC_FREE) else {
            self.queue_refusal(h.id, hx::status::BUSY);
            return;
        };
        let parts = h.target.len() + h.headers.len() + h.peer.len();
        // Every part must fit one envelope beside the smallest body; a body
        // past the bound is refused before any of it is held.
        if parts + HC_REQ_HEAD + 2 > HC_ENV_MAX || h.body.len() > self.cfg.max_body as usize {
            self.queue_refusal(h.id, hx::status::TOO_LARGE);
            return;
        }
        let cid = self.new_cid();
        let limit = parts + self.cfg.max_body as usize;
        let s = &mut self.slots[at];
        *s = HcSlot::EMPTY;
        s.st = HC_COLLECT;
        s.id = h.id;
        s.cid = cid;
        s.method = h.method;
        s.target_len = h.target.len() as u16;
        s.hdr_len = h.headers.len() as u16;
        s.peer_len = h.peer.len() as u16;
        s.credit = h.resp_credit;
        s.opened_ms = self.now_ms;
        let ok = s.req.push(heap, h.target, limit)
            && s.req.push(heap, h.headers, limit)
            && s.req.push(heap, h.peer, limit)
            && s.req.push(heap, h.body, limit);
        if !ok {
            self.refuse(heap, at, hx::status::BUSY);
            return;
        }
        self.taken = self.taken.wrapping_add(1);
        if h.flags & hx::flag::MORE != 0 {
            // The rest of the body this desk takes, granted at once: it hands
            // the chain whole requests and has no use for a smaller window.
            self.slots[at].grant = self.cfg.max_body - h.body.len() as u32;
            return;
        }
        self.whole(heap, at);
    }

    /// The request in slot `at` is whole: staged, or straight to the chain.
    fn whole<H: HcHeap>(&mut self, heap: &mut H, at: usize) {
        if !self.transcode(heap, at) {
            return;
        }
        let s = &self.slots[at];
        let body = s.body().len();
        let inline = self.cfg.inline_max == 0 || body <= self.cfg.inline_max as usize;
        let body_bytes = if inline { body } else { HC_REF_LEN };
        let peer = if s.peer_len > 0 {
            2 + s.peer_len as usize
        } else {
            0
        };
        let env = HC_REQ_HEAD + s.target_len as usize + s.hdr_len as usize + body_bytes + peer;
        if env > HC_ENV_MAX || body_bytes > u16::MAX as usize {
            self.refuse(heap, at, hx::status::TOO_LARGE);
            return;
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let s = &mut self.slots[at];
        s.grant = 0;
        s.seq = seq;
        s.st = if inline { HC_READY } else { HC_STAGE };
    }

    /// A Kubernetes protobuf body (kubectl's typed generators send one) as
    /// the JSON object it stands for, its content type `application/json`:
    /// the chain reads JSON alone. False when the request was refused — 415
    /// for a body that does not decode, 413 for one whose JSON passes the
    /// bound, 503 when the heap cannot hold it.
    fn transcode<H: HcHeap>(&mut self, heap: &mut H, at: usize) -> bool {
        let old = self.slots[at].req;
        let (t, h, p) = {
            let s = &self.slots[at];
            (
                s.target_len as usize,
                s.hdr_len as usize,
                s.peer_len as usize,
            )
        };
        let all = old.bytes();
        let headers = all.get(t..t + h).unwrap_or(&[]);
        if !hc_content_type_is(headers, b"application/vnd.kubernetes.protobuf") {
            return true;
        }
        let target = all.get(..t).unwrap_or(&[]);
        let peer = all.get(t + h..t + h + p).unwrap_or(&[]);
        let body = all.get(t + h + p..).unwrap_or(&[]);
        // The JSON is larger than the protobuf it decodes (keys, base64):
        // bounded at twice the body bound.
        let limit = t + h + p + 64 + self.cfg.max_body as usize * 2;
        let mut nb = HcBytes::EMPTY;
        let mut new_h = 0usize;
        let mut ok = nb.push(heap, target, limit);
        for line in hc_header_lines(headers) {
            let piece: &[u8] = if hc_header_name_is(line, b"content-type") {
                b"content-type: application/json\r\n"
            } else {
                line
            };
            ok = ok && nb.push(heap, piece, limit);
            new_h += piece.len();
        }
        ok = ok && nb.push(heap, peer, limit);
        if !ok || new_h > u16::MAX as usize {
            nb.release(heap);
            self.refuse(heap, at, hx::status::BUSY);
            return false;
        }
        let res = {
            let mut sink = HcSink {
                buf: &mut nb,
                heap,
                limit,
                full: false,
            };
            pb_to_json(body, &mut sink).map(|()| sink.full)
        };
        match res {
            Ok(_) => {
                let mut old = old;
                old.release(heap);
                let s = &mut self.slots[at];
                s.req = nb;
                s.hdr_len = new_h as u16;
                true
            }
            Err(e) => {
                nb.release(heap);
                let status = match e {
                    PbErr::Full => hx::status::TOO_LARGE,
                    _ => 415,
                };
                self.refuse(heap, at, status);
                false
            }
        }
    }

    /// A slot whose body is owed to the store: `(slot, cid)`.
    fn next_stage(&self) -> Option<(usize, u32)> {
        let at = self.slots.iter().position(|s| s.st == HC_STAGE)?;
        Some((at, self.slots[at].cid))
    }

    /// The body slot `at` owes the store, while it still does: an exchange
    /// aborted meanwhile, its slot freed or taken by another, owes nothing.
    fn stage_body(&self, at: usize, cid: u32) -> Option<&[u8]> {
        let s = self.slots.get(at)?;
        (s.st == HC_STAGE && s.cid == cid).then(|| s.body())
    }

    /// The body of slot `at` is in the store under `sha256:<hex>`; `None`
    /// when the store refused it (answered 500).
    fn staged<H: HcHeap>(&mut self, heap: &mut H, at: usize, cid: u32, hex: Option<&[u8; 64]>) {
        if self.stage_body(at, cid).is_none() {
            return;
        }
        let Some(hex) = hex else {
            self.refuse(heap, at, hx::status::FAILED);
            return;
        };
        let s = &mut self.slots[at];
        s.body_ref[..7].copy_from_slice(b"sha256:");
        s.body_ref[7..].copy_from_slice(hex);
        s.staged = true;
        s.st = HC_READY;
    }

    /// The next request envelope owed to the chain, oldest first, laid into
    /// `out`: `(slot, length)`.
    fn next_envelope(&self, out: &mut [u8]) -> Option<(usize, usize)> {
        let at = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.st == HC_READY)
            .min_by_key(|(_, s)| s.seq.wrapping_sub(self.next_seq))?
            .0;
        let s = &self.slots[at];
        let all = s.req.bytes();
        let t = s.target_len as usize;
        let h = s.hdr_len as usize;
        let p = s.peer_len as usize;
        let target = all.get(..t)?;
        let headers = all.get(t..t + h)?;
        let peer = all.get(t + h..t + h + p)?;
        let body: &[u8] = if s.staged { &s.body_ref } else { s.body() };
        let mut flags = 0u8;
        if p > 0 {
            flags |= HC_PEER;
        }
        if s.staged {
            flags |= HC_BODY_REF;
        }
        let trailer = if p > 0 { 2 + p } else { 0 };
        let total = HC_REQ_HEAD + t + h + body.len() + trailer;
        if total > out.len() {
            return None;
        }
        out[0..4].copy_from_slice(&s.cid.to_le_bytes());
        out[4] = s.method;
        out[5] = flags;
        out[6..8].copy_from_slice(&(t as u16).to_le_bytes());
        out[8..10].copy_from_slice(&(h as u16).to_le_bytes());
        out[10..12].copy_from_slice(&(body.len() as u16).to_le_bytes());
        let mut at_out = HC_REQ_HEAD;
        for part in [target, headers, body] {
            out.get_mut(at_out..at_out + part.len())?
                .copy_from_slice(part);
            at_out += part.len();
        }
        if p > 0 {
            out.get_mut(at_out..at_out + 2)?
                .copy_from_slice(&(p as u16).to_le_bytes());
            at_out += 2;
            out.get_mut(at_out..at_out + p)?.copy_from_slice(peer);
            at_out += p;
        }
        Some((at, at_out))
    }

    /// Slot `at`'s envelope reached the chain: the request is no longer
    /// held, only the exchange it answers.
    fn envelope_sent<H: HcHeap>(&mut self, heap: &mut H, at: usize) {
        if let Some(s) = self.slots.get_mut(at) {
            s.req.release(heap);
            s.st = HC_CHAIN;
        }
    }

    /// Take one reply envelope from the chain. False when it answers no
    /// exchange in hand (a request already gone, or cid 0xFFFF_FFFF), or
    /// does not parse.
    fn take_reply<H: HcHeap>(&mut self, heap: &mut H, env: &[u8]) -> bool {
        let Some(head) = env.get(..HC_REPLY_HEAD) else {
            self.stray = self.stray.wrapping_add(1);
            return false;
        };
        let cid = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        let status = u16::from_le_bytes([head[4], head[5]]);
        let flags = head[6];
        let ct_len = head[7] as usize;
        let hdr_len = u16::from_le_bytes([head[8], head[9]]) as usize;
        let body_len = u16::from_le_bytes([head[10], head[11]]) as usize;
        let ct_at = HC_REPLY_HEAD;
        let hdr_at = ct_at + ct_len;
        let body_at = hdr_at + hdr_len;
        let (Some(ct), Some(hdrs), Some(body)) = (
            env.get(ct_at..hdr_at),
            env.get(hdr_at..body_at),
            env.get(body_at..body_at + body_len),
        ) else {
            self.stray = self.stray.wrapping_add(1);
            return false;
        };
        let Some(at) = self.find_cid(cid) else {
            self.stray = self.stray.wrapping_add(1);
            return false;
        };
        let stash_max = self.cfg.stash_max as usize;
        let s = &mut self.slots[at];
        if s.done {
            self.stray = self.stray.wrapping_add(1);
            return false;
        }
        if !s.head_set {
            if ct.len() > HC_CT_MAX || hdrs.len() > HC_HDR_MAX {
                // A head this desk cannot carry: the exchange fails rather
                // than answering with part of it.
                s.st = HC_ABORT;
                s.abort = hx::abort::FAILED;
                return true;
            }
            s.head_set = true;
            s.status = status;
            s.hold = flags & HC_HOLD != 0;
            s.ct[..ct.len()].copy_from_slice(ct);
            s.ct_len = ct.len() as u8;
            s.hdrs[..hdrs.len()].copy_from_slice(hdrs);
            s.hdrs_len = hdrs.len() as u16;
            // Every 503 this front sends says when to ask again: the chain's
            // (a write the store did not decide) as much as its own.
            let n = hdrs.len();
            if status == hx::status::BUSY && hx::header(hdrs, b"retry-after").is_none() {
                if let Some(d) = s.hdrs.get_mut(n..n + HC_LATE_HDRS.len()) {
                    for (o, &b) in d.iter_mut().zip(HC_LATE_HDRS) {
                        *o = b;
                    }
                    s.hdrs_len = (n + HC_LATE_HDRS.len()) as u16;
                }
            }
        }
        if !s.stash.push(heap, body, stash_max) {
            // The server is not taking this exchange's response as fast as
            // the chain makes it: too slow, and never allowed to hold the
            // others up.
            s.stash.release(heap);
            s.st = HC_ABORT;
            s.abort = hx::abort::STALLED;
            return true;
        }
        if flags & HC_MORE == 0 {
            s.done = true;
        }
        true
    }

    /// The next record owed to the server, laid into `out`: a queued
    /// refusal, then each slot's grant, refusal, abort or response, taking
    /// slots in turn so no exchange waits behind another's credit.
    fn next_out(&mut self, out: &mut [u8]) -> Option<(HcOut, usize)> {
        if self.queued_len > 0 {
            let (id, status) = self.queued[0];
            let n = hx::write_response(&id, status, &[], &[], out)?;
            return Some((HcOut::Queued, n));
        }
        for k in 0..SLOTS {
            let at = (self.turn + k) % SLOTS;
            if let Some(r) = self.slot_out(at, out) {
                self.turn = (at + 1) % SLOTS;
                return Some(r);
            }
        }
        None
    }

    fn slot_out(&self, at: usize, out: &mut [u8]) -> Option<(HcOut, usize)> {
        let s = self.slots.get(at)?;
        match s.st {
            HC_COLLECT if s.grant > 0 => {
                let n = hx::write_credit(&s.id, s.grant, out)?;
                Some((HcOut::Grant(at), n))
            }
            HC_REFUSE if s.late => {
                let n = hx::write_response_head(
                    &hx::ResponseHead {
                        id: s.id,
                        flags: 0,
                        status: s.refuse,
                        content_type: HC_LATE_CT,
                        headers: HC_LATE_HDRS,
                        body: HC_LATE_BODY,
                    },
                    out,
                )?;
                Some((HcOut::Refusal(at), n))
            }
            HC_REFUSE if s.refuse == hx::status::BUSY => {
                // As many exchanges as this desk holds: asked again, later.
                let n = hx::write_response_head(
                    &hx::ResponseHead {
                        id: s.id,
                        flags: 0,
                        status: s.refuse,
                        content_type: &[],
                        headers: HC_LATE_HDRS,
                        body: &[],
                    },
                    out,
                )?;
                Some((HcOut::Refusal(at), n))
            }
            HC_REFUSE => {
                let n = hx::write_response(&s.id, s.refuse, &[], &[], out)?;
                Some((HcOut::Refusal(at), n))
            }
            HC_ABORT => {
                let n = hx::write_abort(&s.id, s.abort, out)?;
                Some((HcOut::Abort(at), n))
            }
            HC_CHAIN if s.head_set => self.response_out(at, s, out),
            _ => None,
        }
    }

    fn response_out(&self, at: usize, s: &HcSlot, out: &mut [u8]) -> Option<(HcOut, usize)> {
        let stash = s.stash.bytes();
        let credit = s.credit as usize;
        if !s.head_sent {
            let ct = s.ct.get(..s.ct_len as usize)?;
            let hdrs = s.hdrs.get(..s.hdrs_len as usize)?;
            let room = hx::RECORD_MAX - (hx::HDR + hx::RESP_HEAD_FIXED + ct.len() + hdrs.len());
            let n = stash.len().min(credit).min(room);
            let ends = s.done && n == stash.len();
            let mut flags = if ends { 0 } else { hx::flag::MORE };
            if s.hold {
                flags |= hx::flag::HOLD;
            }
            let len = hx::write_response_head(
                &hx::ResponseHead {
                    id: s.id,
                    flags,
                    status: s.status,
                    content_type: ct,
                    headers: hdrs,
                    body: stash.get(..n)?,
                },
                out,
            )?;
            return Some((HcOut::Response(at, n, ends), len));
        }
        let n = stash.len().min(credit).min(hx::BODY_MAX);
        let ends = s.done && n == stash.len();
        if n == 0 && !ends {
            // Nothing the credit lets go, or nothing yet.
            return None;
        }
        let flags = if ends { 0 } else { hx::flag::MORE };
        let len = hx::write_body(&s.id, flags, stash.get(..n)?, out)?;
        Some((HcOut::Response(at, n, ends), len))
    }

    /// The channel took the record `next_out` laid.
    fn out_sent<H: HcHeap>(&mut self, heap: &mut H, what: HcOut) {
        match what {
            HcOut::Queued => {
                self.queued.copy_within(1..self.queued_len, 0);
                self.queued_len -= 1;
            }
            HcOut::Grant(at) => {
                if let Some(s) = self.slots.get_mut(at) {
                    s.grant = 0;
                }
            }
            HcOut::Refusal(at) | HcOut::Abort(at) => self.free(heap, at),
            HcOut::Response(at, consumed, ends) => {
                let Some(s) = self.slots.get_mut(at) else {
                    return;
                };
                s.head_sent = true;
                s.stash.consume(consumed);
                s.credit = s.credit.saturating_sub(consumed as u32);
                if ends {
                    self.free(heap, at);
                }
            }
        }
    }

    /// Exchanges in hand.
    fn live(&self) -> usize {
        self.slots.iter().filter(|s| s.st != HC_FREE).count()
    }
}

/// Lower-case hex of a digest.
fn hc_hex(d: &[u8; 32]) -> [u8; 64] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 64];
    for (i, b) in d.iter().enumerate() {
        out[2 * i] = HEX[(b >> 4) as usize];
        out[2 * i + 1] = HEX[(b & 0x0f) as usize];
    }
    out
}
