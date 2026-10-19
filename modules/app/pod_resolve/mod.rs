//! A scheduled Pod's containers as the node's asks. See `manifest.toml` for
//! the wire and `_shared/podres.rs` for what a container becomes.
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

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_HEAD: u32 = 0x1422;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;

include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/poddns.rs");
include!("../_shared/podres.rs");
include!("../_shared/moment.rs");
include!("../_shared/podlive.rs");

const CHAN_EINVAL: i32 = -22;
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
/// One input record (a store_source record of a Pod).
const REC: usize = 8192;
/// A Pod read whole.
const POD: usize = 32 * 1024;
/// A container's node record, and the store_effect request around it.
const SPEC: usize = 7680;
const REQ: usize = 8192;
/// The requests of one container, queued.
const OUTQ: usize = 3 * REQ;
/// store_effect's PUT, DELETE and CREATE (absent precondition: 409 when
/// the key exists, which a claim made once is).
const OP_PUT: i64 = 3;
const OP_DELETE: i64 = 4;
const OP_CREATE: i64 = 8;
/// Pods kept to resolve again, and how often.
const RETRY: usize = 16;
const RETRY_REC: usize = 400;
const RETRY_MS: u64 = 3000;
/// Containers of one list a Pod may have, past which none is resolved.
const MAX_CTRS: usize = 16;
/// How long past its grace's end (`deletionTimestamp`) a Pod being deleted
/// waits for its node before it is deleted anyway: a node that stopped
/// answering never says its containers stopped.
const GRACE_SLACK_MS: u64 = 15_000;

/// Where the transcription is in the Pod at hand: the Pod's own record
/// first (its first app container — a container record follows it on the
/// node, and one written before it would be read as orphaned), then the init
/// containers, then the other app containers; `idx` the container next to
/// build.
#[derive(Clone, Copy)]
struct Cursor {
    phase: u8,
    idx: usize,
}

const C_OWN: u8 = 0;
const C_INIT: u8 = 1;
const C_APP: u8 = 2;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    request_out: i32,
    /// Every record as it came in, on to the graph-backed Pods' decision
    /// (pod_specs.uproc), which shares this source.
    forward_out: i32,
    /// The held record has been forwarded.
    forwarded: bool,
    /// The Pod record being resolved (held across steps until every
    /// container's record is out), and where in it the next container is.
    rec: [u8; REC],
    rec_len: usize,
    cur: Cursor,
    pod: [u8; POD],
    spec: [u8; SPEC],
    /// The requests built but not yet taken by the channel, in order: a
    /// container's record, its probes', the claims it needs created; `at`
    /// the first not yet written.
    outq: [u8; OUTQ],
    outq_len: usize,
    outq_at: usize,
    x: PrScratch,
    cid: i64,
    /// Pods with a container Kubernetes would not create yet (a ConfigMap or
    /// Secret not there): their record, compact (key, uid, node), resolved
    /// again every RETRY_MS — how one starts once what it waits on exists,
    /// at no store subscription of its own. `err` marks the Pod in hand.
    retry: [[u8; RETRY_REC]; RETRY],
    retry_len: [u16; RETRY],
    retry_next: usize,
    last_retry_ms: u64,
    err: bool,
    resolved: u32,
    /// Pods whose delete was finished here (nothing of them ran any more).
    finished: u32,
    refused: u32,
    waited: u32,
    last_hb_ms: u64,
    last_hb_sum: u32,
    nag: ReadNag,
}

/// The store as the transcription reads it: one pass.
struct StoreWorld<'a> {
    st: &'a Store,
}

impl PrWorld for StoreWorld<'_> {
    fn object(&mut self, coll: &[u8], ns: &[u8], name: &[u8], out: &mut [u8]) -> PrLook {
        let mut key = [0u8; MAX_KEY];
        let mut n = 0usize;
        let mut put = |b: &[u8], n: &mut usize| {
            for &c in b {
                if let Some(s) = key.get_mut(*n) {
                    *s = c;
                    *n += 1;
                }
            }
        };
        put(b"/", &mut n);
        put(coll, &mut n);
        put(b"/", &mut n);
        if !ns.is_empty() {
            put(ns, &mut n);
            put(b"/", &mut n);
        }
        put(name, &mut n);
        if name.is_empty() || n >= MAX_KEY {
            return PrLook::Absent;
        }
        // SAFETY: the pass's store over the live syscall table.
        match unsafe { get_value(self.st, key.get(..n).unwrap_or(&[]), out) } {
            Some(v) => PrLook::Found(v),
            None if self.st.waiting() => PrLook::Pending,
            None => PrLook::Absent,
        }
    }

    fn raw(&mut self, key: &[u8], out: &mut [u8]) -> PrLook {
        // SAFETY: the pass's store over the live syscall table.
        match unsafe { get_value(self.st, key, out) } {
            Some(v) => PrLook::Found(v),
            None if self.st.waiting() => PrLook::Pending,
            None => PrLook::Absent,
        }
    }

    fn services<F: FnMut(&[u8])>(&mut self, ns: &[u8], buf: &mut [u8], mut f: F) -> PrLook {
        let mut prefix = [0u8; MAX_KEY];
        let mut n = 0usize;
        for &c in b"/services/".iter().chain(ns.iter()).chain(b"/".iter()) {
            if let Some(s) = prefix.get_mut(n) {
                *s = c;
                n += 1;
            }
        }
        let mut walk = ListWalk::new(prefix.get(..n).unwrap_or(&[]));
        let mut key = [0u8; MAX_KEY];
        // SAFETY: the pass's store over the live syscall table.
        unsafe {
            while let Some(k) = walk.next(self.st) {
                let kl = k.len().min(MAX_KEY);
                for (d, &c) in key.iter_mut().zip(k) {
                    *d = c;
                }
                if let Some(v) = get_value(self.st, key.get(..kl).unwrap_or(&[]), buf) {
                    f(buf.get(..v).unwrap_or(&[]));
                }
            }
        }
        if walk.pending || self.st.waiting() {
            PrLook::Pending
        } else {
            PrLook::Found(0)
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
    s.request_out = out_chan;
    s.forward_out = dev_channel_port(&*s.syscalls, 1, 1);
    s.forwarded = false;
    s.retry_len = [0; RETRY];
    s.retry_next = 0;
    s.last_retry_ms = 0;
    s.err = false;
    s.rec_len = 0;
    s.outq_len = 0;
    s.outq_at = 0;
    s.cur = Cursor {
        phase: C_OWN,
        idx: 0,
    };
    s.cid = 0;
    s.finished = 0;
    let _ = (params, params_len);
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
        // A Pod waiting on a ConfigMap or Secret, again, now and then.
        let now = dev_millis(sys);
        if s.rec_len == 0 && now.wrapping_sub(s.last_retry_ms) >= RETRY_MS {
            s.last_retry_ms = now;
            retry_take(s);
        }
        // A few Pods a step: each is a handful of reads.
        for _ in 0..4 {
            if !flush(s, sys) {
                break;
            }
            if s.rec_len == 0 {
                match take(sys, s.request_in, &mut s.rec) {
                    Some(n) => {
                        s.rec_len = n;
                        s.forwarded = false;
                        s.err = false;
                        s.cur = Cursor {
                            phase: C_OWN,
                            idx: 0,
                        };
                    }
                    None => break,
                }
            }
            if !s.forwarded {
                if s.forward_out >= 0
                    && (sys.channel_write)(s.forward_out, s.rec.as_ptr(), s.rec_len) <= 0
                {
                    break;
                }
                s.forwarded = true;
            }
            if !resolve(s, sys) {
                // A read not answered yet: this Pod again next step.
                s.waited = s.waited.wrapping_add(1);
                break;
            }
        }
        heartbeat(s, sys);
        0
    }
}

/// Resolve the held Pod from its cursor on. True when the Pod is done (or
/// is not this module's); false when a read is not answered yet or the
/// channel is full, the Pod held for the next step.
unsafe fn resolve(s: &mut State, sys: &SyscallTable) -> bool {
    let rec_len = s.rec_len;
    let (key, uid, node, rtc, deleting) = {
        let rec = s.rec.get(..rec_len).unwrap_or(&[]);
        (
            span(rec, frame_bytes(rec, 3)),
            span(rec, frame_bytes(rec, 4)),
            span(rec, frame_bytes(rec, 5)),
            span(rec, frame_bytes(rec, 9)),
            !frame_bytes(rec, 8).is_empty(),
        )
    };
    let (kl, ul, nl) = (key.1 - key.0, uid.1 - uid.0, node.1 - node.0);
    let rtc_fluxor = s.rec.get(rtc.0..rtc.1) == Some(b"fluxor".as_slice());
    if kl == 0 || ul == 0 || nl == 0 || rtc_fluxor || ul > 96 || nl > 96 {
        s.rec_len = 0;
        return true;
    }
    let mut keyb = [0u8; MAX_KEY];
    let mut uidb = [0u8; 96];
    let mut nodeb = [0u8; 96];
    copy_span(&s.rec, key, &mut keyb);
    copy_span(&s.rec, uid, &mut uidb);
    copy_span(&s.rec, node, &mut nodeb);
    let key = keyb.get(..kl.min(MAX_KEY)).unwrap_or(&[]);
    let uid = uidb.get(..ul).unwrap_or(&[]);
    let node = nodeb.get(..nl).unwrap_or(&[]);

    let st = Store::plain(sys, &s.nag);
    // The Pod itself, whole.
    let mut pk = [0u8; MAX_KEY];
    let pkl = cat(&mut pk, &[b"/pods/", key]);
    let pod_len = match get_value(&st, pk.get(..pkl).unwrap_or(&[]), &mut s.pod) {
        Some(n) => n,
        None if st.waiting() => return false,
        None => {
            retry_keep(s, key, false);
            s.rec_len = 0;
            return true;
        }
    };
    {
        // The record is of the Pod as it is now: an older incarnation's
        // (another uid under the same name) is not resolved.
        let pod = s.pod.get(..pod_len).unwrap_or(&[]);
        let meta = pr_m(pod, b"metadata").unwrap_or(b"{}");
        if pr_ms(meta, b"uid") != uid {
            retry_keep(s, key, false);
            s.rec_len = 0;
            return true;
        }
    }
    loop {
        if s.cur.idx >= MAX_CTRS {
            if s.cur.phase == C_INIT {
                s.cur = Cursor {
                    phase: C_APP,
                    idx: 1,
                };
                continue;
            }
            break;
        }
        let init = s.cur.phase == C_INIT;
        let ask = PrAsk {
            pod: s.pod.get(..pod_len).unwrap_or(&[]),
            key,
            init,
            idx: s.cur.idx,
            deleting,
        };
        let mut world = StoreWorld { st: &st };
        let n = match pr_spec(&mut world, &mut s.x, &ask, &mut s.spec) {
            PrRes::Pending => return false,
            PrRes::NoContainer => {
                if init {
                    s.cur = Cursor {
                        phase: C_APP,
                        idx: 1,
                    };
                    continue;
                }
                break;
            }
            PrRes::Built(n) => n,
        };
        // Its key: the Pod's first app container is the Pod's own record.
        let mut id = [0u8; 160];
        let mut idx = [0u8; 20];
        let il = pm_dec(s.cur.idx as u64, &mut idx);
        let idx = idx.get(..il).unwrap_or(&[]);
        let idl = if s.cur.phase == C_OWN {
            cat(&mut id, &[uid])
        } else if init {
            cat(&mut id, &[uid, b"~i", idx])
        } else {
            cat(&mut id, &[uid, b"~c", idx])
        };
        let id = id.get(..idl).unwrap_or(&[]);
        let mut rk = [0u8; MAX_KEY];
        let rkl = cat(&mut rk, &[b"~node/", node, b"/pod-specs/", id]);
        let mut pk = [0u8; MAX_KEY];
        let pkl = cat(&mut pk, &[b"~node/", node, b"/pod-probes/", id]);
        s.cid = s.cid.wrapping_add(1);
        s.outq_len = 0;
        s.outq_at = 0;
        let spec_ok = queue(s, OP_PUT, rk.get(..rkl).unwrap_or(&[]), Src::Spec(n));
        // Its probes: their own record, or none.
        let pl = s.x.probes_len;
        if pl == 0 {
            queue(s, OP_DELETE, pk.get(..pkl).unwrap_or(&[]), Src::None);
        } else {
            queue(s, OP_PUT, pk.get(..pkl).unwrap_or(&[]), Src::Probes(pl));
        }
        // The claims its volumes need created (a StatefulSet's templates).
        let mut at = 0usize;
        while at < s.x.claims_len {
            let rest = s.x.claims.get(at..s.x.claims_len).unwrap_or(&[]);
            let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
                break;
            };
            let doc = rest.get(..nl).unwrap_or(&[]);
            let meta = pr_m(doc, b"metadata").unwrap_or(b"{}");
            let mut ck = [0u8; MAX_KEY];
            let ckl = cat(
                &mut ck,
                &[
                    b"persistentvolumeclaims/",
                    pr_ms(meta, b"namespace"),
                    b"/",
                    pr_ms(meta, b"name"),
                ],
            );
            queue(
                s,
                OP_CREATE,
                ck.get(..ckl).unwrap_or(&[]),
                Src::Claim(at, nl),
            );
            at += nl + 1;
        }
        s.err |= !s.spec.get(..n).unwrap_or(&[]).ends_with(b";cfgerr=");
        if !spec_ok {
            // A record past a store_effect request is refused, never cut.
            s.refused = s.refused.wrapping_add(1);
            let m = b"[pod_resolve] a container's record is past a request - REFUSED";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        advance(&mut s.cur);
        if !flush(s, sys) {
            return false;
        }
    }
    // A Pod being deleted (marked: _shared/podlive.rs): its containers'
    // records above say so, with the delete's grace, and its node stops
    // them — SIGTERM, then SIGKILL once the grace is spent. Once nothing of
    // it runs there, the kubelet's last word upstream: the Pod deleted
    // outright (its tomb left by the effect: its node's records end with
    // it). A finalizer holds it until it is removed, as upstream's does.
    // Until then it is looked at again every RETRY_MS.
    let mut keep = false;
    if deleting {
        let (held, until) = {
            let pod = s.pod.get(..pod_len).unwrap_or(&[]);
            let meta = pr_m(pod, b"metadata").unwrap_or(b"{}");
            (
                pr_m(meta, b"finalizers")
                    .and_then(|f| pr_at(f, 0))
                    .is_some(),
                pl_parse_time(pr_ms(meta, b"deletionTimestamp")),
            )
        };
        if !held {
            let Some(live) = pl_live(&st, node, uid) else {
                return false;
            };
            let past = match (until, mo_trusted_ms(sys)) {
                (Some(u), Some(now)) => now >= u.saturating_add(GRACE_SLACK_MS),
                _ => false,
            };
            if !live || past {
                let mut dk = [0u8; MAX_KEY];
                let dkl = cat(&mut dk, &[b"pods/", key]);
                s.cid = s.cid.wrapping_add(1);
                s.outq_len = 0;
                s.outq_at = 0;
                queue(s, OP_DELETE, dk.get(..dkl).unwrap_or(&[]), Src::None);
                s.finished = s.finished.wrapping_add(1);
                if !flush(s, sys) {
                    // Taken next step (module_step flushes first); the
                    // record is let go.
                    s.rec_len = 0;
                    return true;
                }
            } else {
                keep = true;
            }
        }
    }
    s.resolved = s.resolved.wrapping_add(1);
    retry_keep(s, key, s.err || keep);
    s.rec_len = 0;
    true
}

/// The Pod in hand kept to resolve again (`again`), or let go.
fn retry_keep(s: &mut State, key: &[u8], again: bool) {
    let found = (0..RETRY).find(|&i| {
        let l = s.retry_len[i] as usize;
        l > 0 && frame_bytes(s.retry[i].get(..l).unwrap_or(&[]), 3) == key
    });
    if !again {
        if let Some(i) = found {
            s.retry_len[i] = 0;
        }
        return;
    }
    if found.is_some() {
        return;
    }
    let Some(i) = (0..RETRY).find(|&i| s.retry_len[i] == 0) else {
        return;
    };
    // The fields resolve() reads: 3 key, 4 uid, 5 node, 8 deleting, 9 rtc.
    let rec = s.rec.get(..s.rec_len).unwrap_or(&[]);
    let mut out = [0u8; RETRY_REC];
    let mut p = 1usize;
    let mut count = 0u8;
    for num in [3u8, 4, 5, 8, 9] {
        match put_field(&mut out, p, num, TY_BYTES, frame_bytes(rec, num)) {
            Some(e) => {
                p = e;
                count += 1;
            }
            None => return,
        }
    }
    out[0] = count;
    s.retry[i] = out;
    s.retry_len[i] = p as u16;
}

/// The next kept Pod into hand, already forwarded (it is not a source
/// record).
fn retry_take(s: &mut State) {
    for k in 0..RETRY {
        let i = (s.retry_next + k) % RETRY;
        let l = s.retry_len[i] as usize;
        if l == 0 {
            continue;
        }
        s.retry_next = i + 1;
        let src = s.retry[i];
        for (d, &c) in s.rec.iter_mut().zip(src.iter().take(l)) {
            *d = c;
        }
        s.rec_len = l;
        s.forwarded = true;
        s.err = false;
        s.cur = Cursor {
            phase: C_OWN,
            idx: 0,
        };
        return;
    }
}

/// The container after this one: from the Pod's own record to the init
/// containers, from them to the other app containers.
fn advance(c: &mut Cursor) {
    if c.phase == C_OWN {
        *c = Cursor {
            phase: C_INIT,
            idx: 0,
        };
    } else {
        c.idx += 1;
    }
}

/// `parts` joined into `out`; the length (cut at `out`'s end, which a key
/// past MAX_KEY is anyway).
fn cat(out: &mut [u8], parts: &[&[u8]]) -> usize {
    let mut n = 0usize;
    for p in parts {
        for &c in p.iter() {
            if let Some(s) = out.get_mut(n) {
                *s = c;
                n += 1;
            }
        }
    }
    n
}

/// Where `sub` (a slice of `rec`) lies in it.
fn span(rec: &[u8], sub: &[u8]) -> (usize, usize) {
    if sub.is_empty() {
        return (0, 0);
    }
    let a = sub.as_ptr() as usize - rec.as_ptr() as usize;
    (a, a + sub.len())
}

fn copy_span(rec: &[u8], r: (usize, usize), out: &mut [u8]) {
    for (d, &c) in out.iter_mut().zip(rec.get(r.0..r.1).unwrap_or(&[])) {
        *d = c;
    }
}

/// Where a queued request's value comes from.
enum Src {
    None,
    Spec(usize),
    Probes(usize),
    /// A claim's document: its offset and length in `x.claims`.
    Claim(usize, usize),
}

/// A request onto the queue; false when it does not fit (refused whole).
fn queue(s: &mut State, op: i64, key: &[u8], src: Src) -> bool {
    let value: &[u8] = match src {
        Src::None => &[],
        Src::Spec(n) => s.spec.get(..n).unwrap_or(&[]),
        Src::Probes(n) => s.x.probes.get(..n).unwrap_or(&[]),
        Src::Claim(a, n) => s.x.claims.get(a..a + n).unwrap_or(&[]),
    };
    let mut req = [0u8; REQ];
    let Some(rl) = put_request(&mut req, s.cid, op, key, value) else {
        return false;
    };
    let Some(dst) = s.outq.get_mut(s.outq_len..s.outq_len + rl) else {
        return false;
    };
    for (d, &c) in dst.iter_mut().zip(req.iter()) {
        *d = c;
    }
    s.outq_len += rl;
    true
}

/// A store_effect request: 1 cid, 2 op, 3 key, 4 value.
fn put_request(out: &mut [u8; REQ], cid: i64, op: i64, key: &[u8], value: &[u8]) -> Option<usize> {
    let mut p = 1usize;
    p = put_field(out, p, 1, TY_I64, &cid.to_le_bytes())?;
    p = put_field(out, p, 2, TY_I64, &op.to_le_bytes())?;
    p = put_field(out, p, 3, TY_BYTES, key)?;
    p = put_field(out, p, 4, TY_BYTES, value)?;
    *out.get_mut(0)? = 4;
    Some(p)
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

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    let count = frame.first().copied().unwrap_or(0) as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let (Some(&num), Some(&ty), Some(&l0), Some(&l1)) = (
            frame.get(p),
            frame.get(p + 1),
            frame.get(p + 2),
            frame.get(p + 3),
        ) else {
            return &[];
        };
        let len = u16::from_le_bytes([l0, l1]) as usize;
        let Some(payload) = frame.get(p + 4..p + 4 + len) else {
            return &[];
        };
        if num == number {
            return if ty == TY_BYTES { payload } else { &[] };
        }
        p += 4 + len;
    }
    &[]
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

/// One whole record off `chan` into `out`: its length.
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

/// The queued requests, onto the channel in order, one record a write.
/// True when nothing is owed.
unsafe fn flush(s: &mut State, sys: &SyscallTable) -> bool {
    while s.outq_at < s.outq_len {
        let rest = s.outq.get(s.outq_at..s.outq_len).unwrap_or(&[]);
        let Some(fl) = frame_len(rest) else {
            s.outq_len = 0;
            s.outq_at = 0;
            return true;
        };
        if s.request_out >= 0 && (sys.channel_write)(s.request_out, rest.as_ptr(), fl) <= 0 {
            return false;
        }
        s.outq_at += fl;
    }
    s.outq_len = 0;
    s.outq_at = 0;
    true
}

/// `[pod_resolve] hb resolved=… waited=… refused=…`, every 10 s while any
/// moved, so a stalled resolver is visible.
unsafe fn heartbeat(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys);
    let sum = s
        .resolved
        .wrapping_add(s.waited)
        .wrapping_add(s.refused)
        .wrapping_add(s.finished);
    if sum == s.last_hb_sum || now.wrapping_sub(s.last_hb_ms) < 10_000 {
        return;
    }
    s.last_hb_ms = now;
    s.last_hb_sum = sum;
    let mut m = [0u8; 128];
    let head = b"[pod_resolve] hb resolved=";
    let mut n = head.len();
    for (d, &c) in m.iter_mut().zip(head.iter()) {
        *d = c;
    }
    let mut d = [0u8; 20];
    let k = pm_dec(u64::from(s.resolved), &mut d);
    n += cat(
        m.get_mut(n..).unwrap_or(&mut []),
        &[d.get(..k).unwrap_or(&[]), b" waited="],
    );
    let k = pm_dec(u64::from(s.waited), &mut d);
    n += cat(
        m.get_mut(n..).unwrap_or(&mut []),
        &[d.get(..k).unwrap_or(&[]), b" finished="],
    );
    let k = pm_dec(u64::from(s.finished), &mut d);
    n += cat(
        m.get_mut(n..).unwrap_or(&mut []),
        &[d.get(..k).unwrap_or(&[]), b" refused="],
    );
    let k = pm_dec(u64::from(s.refused), &mut d);
    n += cat(
        m.get_mut(n..).unwrap_or(&mut []),
        &[d.get(..k).unwrap_or(&[])],
    );
    dev_log(sys, 3, m.as_ptr(), n);
}
