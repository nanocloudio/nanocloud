//! Intent rows → Wormhole's v1 records, published a generation at a time.
//!
//! See `manifest.toml` for the keys, the order and the rationale, and
//! `_shared/whemit.rs` for the records themselves. The chains decide what a
//! Service and its endpoints mean to Wormhole (`wormhole.plan`); this
//! encodes them in Wormhole's own codecs and writes them in the §9.5 order,
//! activating each generation by compare-and-swap.

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
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/store.rs");
include!("../_shared/wormhole_traffic.rs");
include!("../_shared/whemit.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;

const INTENT: &[u8] = b"/wh-intent/";
const INTENT_EP: &[u8] = b"/wh-intent-ep/";
// The two subscriptions, and the reasons a status says (WhError::code
// order): names, as a PIC module cannot hold a table of pointers.
name_table!(SUBS = [b"/wh-intent/", b"/wh-intent-ep/", b"/wh-dial/"]);
/// A Service's traffic dial (a WHR1 `TrafficDial`), folded into its
/// generations (`whemit.rs`, `wh_parse_dial`).
const DIAL: &[u8] = b"/wh-dial/";
name_table!(REASONS = [b"name", b"malformed", b"too-many", b"buffer"]);
const STATUS: &[u8] = b"/wh-status/";
/// Records written per step before yielding: a generation of a large
/// Service is spread over steps rather than holding one.
const WRITE_BUDGET: usize = 32;
/// Garbage keys deleted per step.
const GC_BUDGET: usize = 32;
/// Quiet after a ring before a scan publishes: a burst of intent rows (a
/// Service and its endpoints arriving together) becomes one generation, not
/// one per row.
const SETTLE_MS: u64 = 250;
/// How often a module whose subscription the store refused looks again,
/// and how long after a failed pass it runs one again.
const POLL_MS: u64 = 2000;

/// What the module is doing between steps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Nothing in flight: a ring runs a scan.
    Idle,
    /// Staging and activating `s.publishing` for the loaded Service.
    Publishing,
    /// Deleting the loaded Service's generations other than `keep` and the
    /// one before it (`all`: other than `keep` only — a withdrawn Service's,
    /// or `only`: just generation `only`, an abandoned staging).
    Collect,
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    event: i32,
    subscribed: u32,
    /// A scan is owed: set by a ring, a poll or a retry, cleared when a
    /// scan finds nothing to do.
    dirty: u32,
    /// A ring came while a publication was in flight: its Service's inputs
    /// are read again before the next write.
    reload: u32,
    poll: u8,
    retry: u8,
    poll_ms: u64,
    /// The last ring: a scan waits SETTLE_MS past it.
    rung_ms: u64,
    nag: ReadNag,

    phase: Phase,
    publishing: WhPub,
    /// Collect: the generation kept, the one deleted alone, or every one
    /// but `keep` (withdrawal).
    keep: u64,
    only: u64,
    all: u8,
    /// The loaded Service: its key tail `<ns>/<svc>`, parsed row and
    /// endpoints (identity order).
    tail: [u8; MAX_KEY],
    tail_len: u16,
    svc: WhSvc,
    eps: [WhEp; WH_MAX_EPS],
    neps: u16,

    published: u32,
    withdrawn: u32,
    failed: u32,
}

/// `_shared/store.rs` answers, as the publication's store sees them.
struct Adapter<'a> {
    store: &'a Store,
}

impl WhStore for Adapter<'_> {
    fn put(&mut self, key: &[u8], val: &[u8]) -> WhAnswer {
        match unsafe { put_value(self.store, key, val) } {
            WriteAnswer::Decided(0) => WhAnswer::Done,
            WriteAnswer::Pending => WhAnswer::Pending,
            WriteAnswer::Decided(rc) => WhAnswer::Failed(rc),
        }
    }
    fn get(&mut self, key: &[u8], out: &mut [u8]) -> Result<Option<(usize, u64)>, WhAnswer> {
        let got = unsafe { get_value_rev(self.store, key, out) };
        if self.store.waiting() {
            return Err(WhAnswer::Pending);
        }
        if let Some(rc) = self.store.refusal() {
            return Err(WhAnswer::Failed(rc));
        }
        Ok(got)
    }
    fn cas(&mut self, key: &[u8], val: &[u8], rev: Option<u64>) -> WhAnswer {
        let (pre, r) = match rev {
            Some(r) => (PRE_ETAG, r),
            None => (PRE_ABSENT, 0),
        };
        match unsafe { put_cond(self.store, key, val, pre, r) } {
            WriteAnswer::Decided(0) => WhAnswer::Done,
            WriteAnswer::Pending => WhAnswer::Pending,
            WriteAnswer::Decided(ERR_EAGAIN | ERR_EEXIST) => WhAnswer::Lost,
            WriteAnswer::Decided(rc) => WhAnswer::Failed(rc),
        }
    }
}

/// What one pass came to.
enum Pass {
    Idle,
    Again,
    /// A write is undecided: the pass is run again next step, unchanged.
    Wait,
    Failed,
}

fn cat(out: &mut [u8], parts: &[&[u8]]) -> usize {
    let mut p = 0;
    for part in parts {
        p = append(out, p, part);
    }
    p
}

/// `<ns>/<svc>` split.
fn split_tail(tail: &[u8]) -> Option<(&[u8], &[u8])> {
    let i = tail.iter().position(|&c| c == b'/')?;
    let (ns, name) = (&tail[..i], &tail[i + 1..]);
    (!ns.is_empty() && !name.is_empty() && !wh_has(name, b'/')).then_some((ns, name))
}

/// Write `/wh-status/<tail>` put-if-changed.
unsafe fn status(store: &Store, tail: &[u8], text: &[u8]) -> WriteAnswer {
    let mut k = [0u8; MAX_KEY];
    let kl = cat(&mut k, &[STATUS, tail]);
    let mut cur = [0u8; 256];
    if let Some(n) = get_value(store, &k[..kl], &mut cur) {
        if &cur[..n] == text {
            return WriteAnswer::Decided(0);
        }
    }
    if store.stopped() {
        return WriteAnswer::Pending;
    }
    put_value(store, &k[..kl], text)
}

/// The status of a Service whose active generation says its intent:
/// `gen=<g>;written=<records>;routes=<r>;endpoints=<e>`.
unsafe fn say_current(s: &State, store: &Store, gen: u64) -> WriteAnswer {
    let eps = &s.eps[..s.neps as usize];
    let mut t = [0u8; 128];
    let n = match wh_generation(&s.svc, eps, gen) {
        Ok(g) => {
            let p = cat(&mut t, &[b"gen="]);
            let p = wh_dec(&mut t, p, gen).unwrap_or(p);
            let p = append(&mut t, p, b";written=");
            let p = wh_dec(&mut t, p, g.len() as u64).unwrap_or(p);
            let p = append(&mut t, p, b";routes=");
            let p = wh_dec(&mut t, p, g.nroutes as u64).unwrap_or(p);
            let p = append(&mut t, p, b";endpoints=");
            wh_dec(&mut t, p, eps.len() as u64).unwrap_or(p)
        }
        Err(e) => cat(&mut t, &[b"error=", REASONS.get(e.code())]),
    };
    status(store, &s.tail[..s.tail_len as usize], &t[..n])
}

/// Read the Service at `tail` (its row and its endpoints) into the state.
/// Ok(false): its row is gone. Err: a read waits or failed, or the intent
/// is one Wormhole cannot be told (said in its status).
unsafe fn load(s: &mut State, store: &Store, tail: &[u8]) -> Result<bool, Option<WhError>> {
    let kl = tail.len().min(MAX_KEY);
    s.tail[..kl].copy_from_slice(&tail[..kl]);
    s.tail_len = kl as u16;
    let (ns, name) = split_tail(tail).ok_or(Some(WhError::Name))?;
    let mut k = [0u8; MAX_KEY];
    let n = cat(&mut k, &[INTENT, tail]);
    let mut row = [0u8; MAX_VALUE];
    let Some(rl) = get_value(store, &k[..n], &mut row) else {
        return if store.stopped() {
            Err(None)
        } else {
            Ok(false)
        };
    };
    s.svc = wh_parse_svc(ns, name, &row[..rl]).map_err(Some)?;
    let n = cat(&mut k, &[DIAL, tail]);
    let mut dial = [0u8; 256];
    if let Some(dl) = get_value(store, &k[..n], &mut dial) {
        s.svc.dial = wh_parse_dial(&s.svc, &dial[..dl]);
    } else if store.stopped() {
        return Err(None);
    }
    s.neps = 0;
    let n = cat(&mut k, &[INTENT_EP, tail, b"/"]);
    let mut walk = ListWalk::new(&k[..n]);
    let mut over = false;
    while let Some(key) = walk.next(store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let pod = last_seg(&kb[..kl]);
        let mut v = [0u8; 1024];
        let Some(vl) = get_value(store, &kb[..kl], &mut v) else {
            continue;
        };
        let Some(e) = wh_parse_ep(ns, name, pod, &v[..vl]) else {
            continue;
        };
        if s.neps as usize >= WH_MAX_EPS {
            over = true;
            break;
        }
        s.eps[s.neps as usize] = e;
        s.neps += 1;
    }
    if store.stopped() || walk.failed {
        return Err(None);
    }
    if over {
        return Err(Some(WhError::TooMany));
    }
    wh_sort_eps(&mut s.eps[..s.neps as usize]);
    Ok(true)
}

fn active_key(svc: &WhSvc, out: &mut [u8; MAX_KEY]) -> usize {
    wh_active_key(&svc.tenant, &svc.service, out).unwrap_or(0)
}

/// One step of a publication in flight.
unsafe fn publish_step(s: &mut State, store: &Store) -> Pass {
    if s.reload != 0 {
        s.reload = 0;
        let mut tail = [0u8; MAX_KEY];
        let tl = s.tail_len as usize;
        tail[..tl].copy_from_slice(&s.tail[..tl]);
        match load(s, store, &tail[..tl]) {
            Ok(true)
                if wh_intent_digest(&s.svc, &s.eps[..s.neps as usize]) == s.publishing.intent => {}
            Err(None) => {
                s.reload = 1;
                return Pass::Again;
            }
            _ => {
                // The intent moved under the staging: what was staged is
                // removed (it was never active, so nobody read it) and the
                // scan stages again from the active pointer.
                s.phase = Phase::Collect;
                s.only = s.publishing.gen;
                s.all = 0;
                s.keep = 0;
                return Pass::Again;
            }
        }
    }
    let eps = &s.eps[..s.neps as usize];
    let Ok(g) = wh_generation(&s.svc, eps, s.publishing.gen) else {
        s.phase = Phase::Idle;
        return Pass::Failed;
    };
    let mut a = Adapter { store };
    match wh_publish(&mut a, &mut s.publishing, &g, WRITE_BUDGET) {
        WhStep::More => Pass::Again,
        WhStep::Wait => Pass::Wait,
        WhStep::Activated => {
            s.published = s.published.wrapping_add(1);
            s.phase = Phase::Collect;
            s.keep = s.publishing.gen;
            s.only = 0;
            s.all = 0;
            Pass::Again
        }
        WhStep::Raced => {
            // Another publisher advanced the pointer: what was staged is
            // removed, and the next generation staged after its.
            s.phase = Phase::Collect;
            s.only = s.publishing.gen;
            s.all = 0;
            s.keep = 0;
            Pass::Again
        }
        WhStep::Failed => {
            s.phase = Phase::Idle;
            s.failed = s.failed.wrapping_add(1);
            let _ = status(store, &s.tail[..s.tail_len as usize], b"error=write");
            Pass::Failed
        }
    }
}

/// One step of collection: delete up to GC_BUDGET keys of the loaded
/// Service's generations that are garbage.
unsafe fn collect_step(s: &mut State, store: &Store) -> Pass {
    let mut k = [0u8; MAX_KEY];
    let mut pfx = [0u8; MAX_KEY];
    let pl = wh_put(&mut pfx, 0, b"/wh/")
        .and_then(|p| wh_hex(&mut pfx, p, &s.svc.tenant))
        .and_then(|p| wh_put(&mut pfx, p, b"/"))
        .and_then(|p| wh_hex(&mut pfx, p, &s.svc.service))
        .and_then(|p| wh_put(&mut pfx, p, b"/gen/"))
        .unwrap_or(0);
    let mut walk = ListWalk::new(&pfx[..pl]);
    let mut deleted = 0usize;
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        k[..kl].copy_from_slice(&key[..kl]);
        let Some(gen) = wh_key_gen(&k[..kl], pl) else {
            continue;
        };
        let garbage = if s.only != 0 {
            gen == s.only
        } else if s.all != 0 {
            gen != s.keep
        } else {
            wh_collectable(gen, s.keep)
        };
        if !garbage {
            continue;
        }
        match delete_value(store, &k[..kl]) {
            WriteAnswer::Pending => return Pass::Wait,
            WriteAnswer::Decided(_) => {}
        }
        deleted += 1;
        if deleted == GC_BUDGET {
            return Pass::Again;
        }
    }
    if store.stopped() || walk.failed {
        return Pass::Failed;
    }
    if s.only != 0 {
        // An abandoned staging removed: stage again from the scan.
        s.phase = Phase::Idle;
        s.dirty = 1;
        return Pass::Again;
    }
    s.phase = Phase::Idle;
    if s.all != 0 {
        // Withdrawn and collected: its status goes last.
        let mut sk = [0u8; MAX_KEY];
        let n = cat(&mut sk, &[STATUS, &s.tail[..s.tail_len as usize]]);
        if delete_value(store, &sk[..n]) == WriteAnswer::Pending {
            s.phase = Phase::Collect;
            return Pass::Wait;
        }
        s.withdrawn = s.withdrawn.wrapping_add(1);
    } else if say_current(s, store, s.keep) == WriteAnswer::Pending {
        s.phase = Phase::Collect;
        return Pass::Wait;
    }
    s.dirty = 1;
    Pass::Again
}

/// A scan: the first Service whose active generation does not say its
/// intent starts a publication; then the first one Wormhole is no longer
/// told of is withdrawn. Pass::Idle when every Service is current.
unsafe fn scan(s: &mut State, store: &Store) -> Pass {
    let mut walk = ListWalk::new(INTENT);
    while let Some(key) = walk.next(store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let tail = &kb[INTENT.len().min(kl)..kl];
        match load(s, store, tail) {
            Ok(true) => {}
            Ok(false) => continue,
            Err(None) => {
                return if store.waiting() {
                    Pass::Again
                } else {
                    Pass::Failed
                }
            }
            Err(Some(e)) => {
                // Said, not published: the last good generation stands.
                let mut t = [0u8; 64];
                let n = cat(&mut t, &[b"error=", REASONS.get(e.code())]);
                if status(store, tail, &t[..n]) == WriteAnswer::Pending {
                    return Pass::Wait;
                }
                continue;
            }
        }
        let mut ak = [0u8; MAX_KEY];
        let al = active_key(&s.svc, &mut ak);
        let active = match wh_read_active(&mut Adapter { store }, &ak[..al]) {
            Ok(a) => a,
            Err(WhAnswer::Pending) => return Pass::Again,
            Err(_) => return Pass::Failed,
        };
        let intent = wh_intent_digest(&s.svc, &s.eps[..s.neps as usize]);
        if active.intent == Some(intent) {
            if say_current(s, store, active.gen) == WriteAnswer::Pending {
                return Pass::Wait;
            }
            continue;
        }
        s.publishing = wh_begin(&active, &intent);
        s.phase = Phase::Publishing;
        s.reload = 0;
        return Pass::Again;
    }
    if store.stopped() || walk.failed {
        return Pass::Failed;
    }
    // Withdrawal: a Service with a status here and no intent row.
    let mut walk = ListWalk::new(STATUS);
    while let Some(key) = walk.next(store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let tail = &kb[STATUS.len().min(kl)..kl];
        let mut ik = [0u8; MAX_KEY];
        let il = cat(&mut ik, &[INTENT, tail]);
        if exists(store, &ik[..il]) {
            continue;
        }
        if store.stopped() {
            return Pass::Again;
        }
        let Some((ns, name)) = split_tail(tail) else {
            continue;
        };
        s.svc.tenant = wh_tenant_id(ns);
        s.svc.service = wh_service_id(ns, name);
        s.tail[..tail.len()].copy_from_slice(tail);
        s.tail_len = tail.len() as u16;
        let mut ak = [0u8; MAX_KEY];
        let al = active_key(&s.svc, &mut ak);
        let mut cur = [0u8; 512];
        let got = get_value_rev(store, &ak[..al], &mut cur);
        if store.stopped() {
            return Pass::Again;
        }
        let keep = match got {
            Some((n, rev)) if wh_field(&cur[..n], b"withdrawn=") != b"1" => {
                let gen = wh_u64(wh_field(&cur[..n], b"gen=")).unwrap_or(0) + 1;
                let mut v = [0u8; 256];
                let vl = wh_withdrawn(gen, ns, name, &mut v).unwrap_or(0);
                match put_cond(store, &ak[..al], &v[..vl], PRE_ETAG, rev) {
                    WriteAnswer::Pending => return Pass::Wait,
                    WriteAnswer::Decided(0) => gen,
                    // Moved meanwhile: scanned again.
                    WriteAnswer::Decided(_) => return Pass::Again,
                }
            }
            Some((n, _)) => wh_u64(wh_field(&cur[..n], b"gen=")).unwrap_or(0),
            None => 0,
        };
        s.phase = Phase::Collect;
        s.keep = keep;
        s.only = 0;
        s.all = 1;
        return Pass::Again;
    }
    if store.stopped() || walk.failed {
        return Pass::Failed;
    }
    Pass::Idle
}

unsafe fn pass(s: &mut State) -> Pass {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    match s.phase {
        Phase::Publishing => publish_step(s, &store),
        Phase::Collect => collect_step(s, &store),
        Phase::Idle => scan(s, &store),
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
    _in_chan: i32,
    _out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
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
        s.event = -1;
        s.subscribed = 0;
        s.dirty = 1;
        s.reload = 0;
        s.poll = 0;
        s.retry = 0;
        s.poll_ms = 0;
        s.rung_ms = 0;
        s.nag = ReadNag::new();
        s.phase = Phase::Idle;
        s.keep = 0;
        s.only = 0;
        s.all = 0;
        s.tail_len = 0;
        s.neps = 0;
        s.published = 0;
        s.withdrawn = 0;
        s.failed = 0;
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
        if s.event < 0 && s.subscribed == 0 {
            s.event = store_event(sys);
            if s.event < 0 {
                let m = b"[wormhole_emit] ERROR no kernel event - changes will NOT wake this module, it looks again every POLL_MS";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.poll = 1;
                s.subscribed = 3;
            }
        }
        // The intent prefixes and the dials: its inputs, never its outputs.
        while s.subscribed < 3 {
            let prefix = SUBS.get(s.subscribed as usize);
            let rc = store_subscribe(sys, prefix, s.event, 0);
            if not_yet(rc) {
                return 0;
            }
            if rc < 0 {
                s.poll = 1;
            }
            s.subscribed += 1;
        }
        if s.poll != 0 || s.retry != 0 {
            let now = dev_millis(sys);
            if now.wrapping_sub(s.poll_ms) >= POLL_MS {
                s.poll_ms = now;
                s.retry = 0;
                s.dirty = 1;
            }
        }
        if store_take(sys, s.event, |_| {}) > 0 {
            s.dirty = 1;
            s.rung_ms = dev_millis(sys);
            if s.phase == Phase::Publishing {
                s.reload = 1;
            }
        }
        if s.dirty == 0 && s.phase == Phase::Idle {
            return 0;
        }
        if s.phase == Phase::Idle && dev_millis(sys).wrapping_sub(s.rung_ms) < SETTLE_MS {
            return 0;
        }
        match pass(s) {
            Pass::Idle => s.dirty = 0,
            Pass::Again | Pass::Wait => {}
            Pass::Failed => {
                let m = b"[wormhole_emit] ERROR a pass FAILED (a read refused, a listing failed, or a write refused) - run again in POLL_MS";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.dirty = 0;
                s.retry = 1;
                s.poll_ms = dev_millis(sys);
            }
        }
        0
    }
}
