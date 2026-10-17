// Shared no_std store + parsing helpers for nanocloud fmods — included via
// `include!("../_shared/store.rs")`.
//
// One implementation of the store plumbing, for every module that touches the
// store. A per-module copy is not free: this is a quarter of all module source
// in the repo, and a copied helper drifts silently — a mis-sized write buffer
// or a dropped LIST cursor loses data without erroring.
//
// Only helpers with a SINGLE implementation across the repo live here. A module
// whose variant is deliberately different keeps its own copy, so that the
// divergence stays visible instead of being flattened into a shared default
// that would quietly break it.
//
// The including module must define the opcodes and bounds these helpers name:
// `MAX_KEY`, `MAX_VALUE`, `EVENT_HEADER_SIZE`, `OBJ_GET`, `OBJ_PUT`,
// `OBJ_RANGE_GET`, `OBJ_CLOSE`, `OBJ_DELETE`, `NS_LIST` and `NS_SUBSCRIBE`.
//
// The object and namespace helpers reach the store through a [`Store`]: the
// syscall table, and — while a module serves a record — that record's
// [`Journal`]. A storage write may answer `EINPROGRESS`: taken, not yet
// decided. The record is then held and served again next step, and the
// journal answers that serve's calls as the first serve saw them, up to the
// pending write, which is asked again exactly as it was first asked. So a
// write is never made twice and a serve never sees its own write half-way.
//
// A storage READ may answer `EAGAIN`: not yet (the board's store answers it
// while its device is busy with a write). That is never absence — and nor is
// any refusal but the contract's absence (`ENXIO`, or a file-backed
// provider's `ENOENT`): a read refused otherwise is undecided, handled
// exactly as `EAGAIN` is, and said loudly but at most once per
// `REFUSAL_SAY_MS` per module (its [`ReadNag`]). In a serve
// it holds the record exactly as a pending write does: the read is not
// recorded, every later call of the serve answers without reaching the
// store, and the next serve replays what was recorded and asks that read
// again. Outside a serve the [`Store`] remembers it ([`Store::waiting`]):
// every later call through it answers without reaching the store, and the
// caller drops the pass and runs it again on a later step. The `*_live`
// readers answer a [`Got`], whose `Pending` a caller has to handle.
//
// ALL of them, even the ones that module never calls: the reference here is
// lexical, so a const that looks dead to `dead_code` is still what makes this
// file compile. Deleting one because the compiler called it unused is a build
// break, not a cleanup.

use abi::contracts::storage::object::{write_answer, WriteAnswer};

fn append(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

fn last_seg(key: &[u8]) -> &[u8] {
    match key.iter().rposition(|&b| b == b'/') {
        Some(i) => &key[i + 1..],
        None => key,
    }
}

// ── The store a serve reaches ─────────────────────────────────────────────

/// Calls one record's serve may record.
const JOURNAL_CALLS: usize = 96;
/// Bytes the recorded answers of one serve may hold: what its reads
/// returned, and a pending write's key and value.
const JOURNAL_BYTES: usize = 16 * 1024;

const J_EXISTS: u8 = 1;
const J_GET: u8 = 2;
const J_WHOLE: u8 = 3;
const J_HEAD: u8 = 4;
const J_LIST: u8 = 5;
const J_WRITE: u8 = 6;
const J_TIME: u8 = 7;
const J_GETREV: u8 = 8;

/// A write's recorded answer while it is undecided.
const J_PENDING: i64 = i64::MIN;

/// One call a serve made and what it answered.
#[derive(Clone, Copy)]
struct JCall {
    kind: u8,
    /// FNV-1a of the key the call named, to tell a serve that has taken
    /// another path from the one recorded.
    key: u32,
    rc: i64,
    off: u32,
    len: u32,
}

const JCALL_EMPTY: JCall = JCall {
    kind: 0,
    key: 0,
    rc: 0,
    off: 0,
    len: 0,
};

/// The store calls of the record a module is serving, in the order its
/// serve made them. Reset when a record is taken; kept while it is held.
struct Journal {
    calls: [JCall; JOURNAL_CALLS],
    bytes: [u8; JOURNAL_BYTES],
    /// Calls recorded, and calls this serve has made so far.
    n: u16,
    at: u16,
    used: u32,
    /// A write of this serve is undecided, or a read answered `EAGAIN`:
    /// every later call of the serve answers without reaching the store,
    /// and the record is held.
    stalled: bool,
    /// The serve outgrew the journal, or took another path than the one
    /// recorded: it cannot be served again faithfully, so it makes no
    /// further write and its record is answered as refused.
    broken: bool,
    /// The serving module's refused reads, said rate-limited. Not reset with
    /// the record: the limit is the module's, across records.
    nag: ReadNag,
}

impl Journal {
    /// A journal for a record just taken.
    fn reset(&mut self) {
        self.n = 0;
        self.at = 0;
        self.used = 0;
        self.stalled = false;
        self.broken = false;
    }

    /// A serve of the held record begins: its calls replay from the first.
    fn begin(&mut self) {
        self.at = 0;
        self.stalled = false;
    }

    /// Whether this serve's record must be held (a write is undecided, or
    /// a read has to be asked again).
    fn held(&self) -> bool {
        self.stalled && !self.broken
    }

    /// The recorded call this one replays, if any: `Err` when the serve
    /// has gone another way than the record.
    fn replay(&mut self, kind: u8, key: u32) -> Result<Option<JCall>, ()> {
        if (self.at as usize) >= self.n as usize {
            return Ok(None);
        }
        let c = self.calls[self.at as usize];
        if c.kind != kind || c.key != key {
            self.broken = true;
            return Err(());
        }
        self.at += 1;
        Ok(Some(c))
    }

    /// Record a live call and its answer, with `data` kept; false when
    /// there is no room (the journal is then broken).
    fn record(&mut self, kind: u8, key: u32, rc: i64, data: &[&[u8]]) -> bool {
        let len: usize = data.iter().map(|d| d.len()).sum();
        if self.n as usize >= JOURNAL_CALLS || self.used as usize + len > JOURNAL_BYTES {
            self.broken = true;
            return false;
        }
        let off = self.used as usize;
        let mut at = off;
        for d in data {
            self.bytes[at..at + d.len()].copy_from_slice(d);
            at += d.len();
        }
        self.calls[self.n as usize] = JCall {
            kind,
            key,
            rc,
            off: off as u32,
            len: len as u32,
        };
        self.n += 1;
        self.at = self.n;
        self.used = at as u32;
        true
    }

    fn data(&self, c: &JCall) -> &[u8] {
        &self.bytes[c.off as usize..(c.off + c.len) as usize]
    }

    /// Whether a call must answer without reaching the store.
    fn silent(&self) -> bool {
        self.stalled || self.broken
    }
}

/// The store a module reaches: the syscall table, and the journal of the
/// record it is serving (none outside a serve).
///
/// One `Store` is one pass: a caller outside a serve makes a new one each
/// step, and once a read through it has answered `EAGAIN` it is
/// [`waiting`](Store::waiting) for the rest of the pass.
struct Store {
    sys: *const SyscallTable,
    j: *mut Journal,
    /// Where this module's refused reads are counted ([`ReadNag`]): the
    /// journal's in a serve, the module state's outside one.
    nag: *const ReadNag,
    /// A read of this pass answered `EAGAIN`.
    waited: core::cell::Cell<bool>,
}

impl core::ops::Deref for Store {
    type Target = SyscallTable;
    fn deref(&self) -> &SyscallTable {
        // SAFETY: built from a live `&SyscallTable`.
        unsafe { &*self.sys }
    }
}

impl Store {
    /// The store without a journal: every call goes to it. `nag` is the
    /// module state's: it outlives the pass.
    fn plain(sys: &SyscallTable, nag: &ReadNag) -> Store {
        Store {
            sys,
            j: core::ptr::null_mut(),
            nag,
            waited: core::cell::Cell::new(false),
        }
    }

    /// The store as the record `j` journals sees it.
    fn serving(sys: &SyscallTable, j: &mut Journal) -> Store {
        let nag: *const ReadNag = &j.nag;
        Store {
            sys,
            j,
            nag,
            waited: core::cell::Cell::new(false),
        }
    }

    fn table(&self) -> &SyscallTable {
        self
    }

    fn nag(&self) -> &ReadNag {
        // SAFETY: the module state or journal it came from outlives the
        // `Store`.
        unsafe { &*self.nag }
    }

    /// Whether this pass has to be run again on a later step, deciding
    /// nothing now: a read answered `EAGAIN`, or (in a serve) a write is
    /// undecided. What the pass read since is not the store's answer —
    /// a read answers absent, a walk ends, a write answers `Pending` — so a
    /// caller that sees this drops what it concluded.
    fn waiting(&self) -> bool {
        // SAFETY: the journal, when there is one, outlives the `Store`.
        self.waited.get() || unsafe { self.j.as_ref() }.is_some_and(|j| j.held())
    }

    /// A read answered `EAGAIN` (or another undecided refusal): the pass
    /// waits, and in a serve the record is held.
    unsafe fn wait(&self) {
        self.waited.set(true);
        if let Some(j) = self.j.as_mut() {
            j.stalled = true;
        }
    }

    /// Whether a call must answer without reaching the store.
    unsafe fn silent(&self) -> bool {
        self.waited.get() || self.j.as_ref().is_some_and(|j| j.silent())
    }
}

/// FNV-1a over `key`.
fn key_hash(key: &[u8]) -> u32 {
    let mut h = 0x811c_9dc5u32;
    for &b in key {
        h = (h ^ u32::from(b)).wrapping_mul(0x0100_0193);
    }
    h
}

/// The wall clock (Unix ms), as the serve first saw it.
unsafe fn now_millis(sys: &Store) -> u64 {
    if let Some(j) = sys.j.as_mut() {
        if j.silent() {
            return 0;
        }
        if let Ok(Some(c)) = j.replay(J_TIME, 0) {
            return c.rc as u64;
        }
        let t = dev_unix_millis(sys.table());
        j.record(J_TIME, 0, t as i64, &[]);
        return t;
    }
    dev_unix_millis(sys.table())
}

/// True iff `key` currently exists (open+close a GET handle).
/// False when absent — or when the pass is [`waiting`](Store::waiting),
/// which the caller checks before acting on it.
unsafe fn exists(sys: &Store, key: &[u8]) -> bool {
    let kh = key_hash(key);
    if sys.silent() {
        return false;
    }
    if let Some(j) = sys.j.as_mut() {
        match j.replay(J_EXISTS, kh) {
            Ok(Some(c)) => return c.rc != 0,
            Ok(None) => {}
            Err(()) => return false,
        }
    }
    let found = match exists_live(sys.table(), sys.nag(), key) {
        Got::Pending => {
            sys.wait();
            return false;
        }
        Got::Found(()) => true,
        Got::Absent => false,
    };
    if let Some(j) = sys.j.as_mut() {
        j.record(J_EXISTS, kh, i64::from(found), &[]);
    }
    found
}

unsafe fn exists_live(sys: &SyscallTable, nag: &ReadNag, key: &[u8]) -> Got<()> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return Got::Absent;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return refusal(sys, nag, b"GET", key, h);
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    Got::Found(())
}

/// storage.object GET+RANGE_GET+CLOSE — read `key` into `dst`. Returns
/// length; `None` when absent — or when the pass is
/// [`waiting`](Store::waiting), which the caller checks before acting on it.
unsafe fn get_value(sys: &Store, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let kh = key_hash(key);
    if sys.silent() {
        return None;
    }
    if let Some(j) = sys.j.as_mut() {
        match j.replay(J_GET, kh) {
            Ok(Some(c)) => {
                if c.rc < 0 {
                    return None;
                }
                let d = j.data(&c);
                let n = d.len().min(dst.len());
                dst[..n].copy_from_slice(&d[..n]);
                return Some(n);
            }
            Ok(None) => {}
            Err(()) => return None,
        }
    }
    let got = match get_value_live(sys.table(), sys.nag(), key, dst) {
        Got::Pending => {
            sys.wait();
            return None;
        }
        Got::Found(n) => Some(n),
        Got::Absent => None,
    };
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some(n) => j.record(J_GET, kh, n as i64, &[&dst[..n]]),
            None => j.record(J_GET, kh, -1, &[]),
        };
    }
    got
}

unsafe fn get_value_live(
    sys: &SyscallTable,
    nag: &ReadNag,
    key: &[u8],
    dst: &mut [u8],
) -> Got<usize> {
    match get_live(sys, nag, key, dst, false) {
        Got::Found((n, _)) => Got::Found(n),
        Got::Absent => Got::Absent,
        Got::Pending => Got::Pending,
    }
}

/// storage.object GET, with the revision of the value read: the value and
/// its revision come from ONE read (the read handle's fence), so a
/// conditional write on that revision is conditional on exactly this
/// value — a separate HEAD may answer for another one, or (a board's store
/// whose device made the HEAD wait) answer nothing at all. `None` when
/// absent — or when the pass is [`waiting`](Store::waiting), which the
/// caller checks before acting on it. The revision is 0 when the provider
/// gives the handle no fence.
unsafe fn get_value_rev(sys: &Store, key: &[u8], dst: &mut [u8]) -> Option<(usize, u64)> {
    let kh = key_hash(key);
    if sys.silent() {
        return None;
    }
    if let Some(j) = sys.j.as_mut() {
        match j.replay(J_GETREV, kh) {
            Ok(Some(c)) => {
                if c.rc < 0 {
                    return None;
                }
                let d = j.data(&c);
                let rev = u64::from_le_bytes(d.get(..8)?.try_into().ok()?);
                let v = d.get(8..)?;
                let n = v.len().min(dst.len());
                dst[..n].copy_from_slice(&v[..n]);
                return Some((n, rev));
            }
            Ok(None) => {}
            Err(()) => return None,
        }
    }
    let got = match get_live(sys.table(), sys.nag(), key, dst, true) {
        Got::Pending => {
            sys.wait();
            return None;
        }
        Got::Found(v) => Some(v),
        Got::Absent => None,
    };
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some((n, rev)) => j.record(J_GETREV, kh, n as i64, &[&rev.to_le_bytes(), &dst[..n]]),
            None => j.record(J_GETREV, kh, -1, &[]),
        };
    }
    got
}

/// The revision a read handle observed (its `ViewConsistent` /
/// `RevisionMonotone` fence), 0 when the provider gives none.
unsafe fn handle_rev(sys: &SyscallTable, h: i32) -> u64 {
    use abi::fence::{Fence, QUERY_OP, WIRE_MAX_LEN};
    let mut f = [0u8; WIRE_MAX_LEN];
    let n = (sys.provider_call)(h, QUERY_OP, f.as_mut_ptr(), f.len());
    if n <= 0 {
        return 0;
    }
    match Fence::decode(f.get(..n as usize).unwrap_or(&[])) {
        Some((Fence::ViewConsistent { revision, .. }, _))
        | Some((Fence::RevisionMonotone { revision, .. }, _)) => revision,
        _ => 0,
    }
}

/// The live GET behind [`get_value_live`] and [`get_value_rev`]: the
/// value's length and, when `want_rev`, its revision ([`handle_rev`]).
unsafe fn get_live(
    sys: &SyscallTable,
    nag: &ReadNag,
    key: &[u8],
    dst: &mut [u8],
    want_rev: bool,
) -> Got<(usize, u64)> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return Got::Absent;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return refusal(sys, nag, b"GET", key, h);
    }
    let rev = if want_rev { handle_rev(sys, h) } else { 0 };
    let mut rarg = [0u8; 20];
    rarg[8..12].copy_from_slice(&(dst.len() as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
    // A value the caller's buffer cannot hold is REFUSED, not returned as a
    // prefix of itself. `RANGE_GET` reports bytes copied, so a full buffer is
    // ambiguous; one more byte past it settles it. A truncated read of a
    // compact record silently drops its trailing fields — a sandbox spec that
    // loses `;phase=start` is created and never started — so the check belongs
    // here, for every reader, rather than in each buffer's size.
    let mut truncated = false;
    // A range the provider cannot serve yet — or refuses at all: a refused
    // read of an open handle is never absence — the handle is closed and the
    // whole read asked again later (a GET answers from the key as it then
    // is, so nothing is lost by opening it again).
    let mut wait = n < 0;
    let mut rc = n;
    if n >= 0 && n as usize == dst.len() {
        let mut probe = [0u8; 1];
        let mut parg = [0u8; 20];
        parg[0..8].copy_from_slice(&(dst.len() as u64).to_le_bytes());
        parg[8..12].copy_from_slice(&1u32.to_le_bytes());
        parg[12..20].copy_from_slice(&(probe.as_mut_ptr() as u64).to_le_bytes());
        let pr = (sys.provider_call)(h, OBJ_RANGE_GET, parg.as_mut_ptr(), 20);
        if pr > 0 {
            truncated = true;
        }
        if pr < 0 {
            wait = true;
            rc = pr;
        }
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    if wait {
        if rc != READ_WAIT {
            say_refused(sys, nag, b"RANGE_GET", key, rc);
        }
        return Got::Pending;
    }
    if truncated {
        let m = b"[store] GET value larger than the reader's buffer - REFUSED, not truncated";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return Got::Absent;
    }
    Got::Found((n as usize, rev))
}

/// A value too large for a caller's fixed buffer, read WHOLE onto this
/// module's heap (the buffer doubled as it fills, up to `max`). `None` when
/// absent, past `max`, or the module has no heap (or it is exhausted) — or
/// when the pass is [`waiting`](Store::waiting). The caller frees it with
/// `heap_free`.
unsafe fn read_whole(sys: &Store, key: &[u8], max: usize) -> Option<(*mut u8, usize)> {
    let kh = key_hash(key);
    if sys.silent() {
        return None;
    }
    if let Some(j) = sys.j.as_mut() {
        match j.replay(J_WHOLE, kh) {
            Ok(Some(c)) => {
                if c.rc < 0 {
                    return None;
                }
                // The caller frees what it is handed: a copy of the record.
                let d = j.data(&c);
                let p = heap_alloc(sys.table(), d.len().max(1) as u32);
                if p.is_null() {
                    return None;
                }
                core::ptr::copy_nonoverlapping(d.as_ptr(), p, d.len());
                return Some((p, d.len()));
            }
            Ok(None) => {}
            Err(()) => return None,
        }
    }
    let got = match read_whole_live(sys.table(), sys.nag(), key, max) {
        Got::Pending => {
            sys.wait();
            return None;
        }
        Got::Found(v) => Some(v),
        Got::Absent => None,
    };
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some((p, n)) => j.record(J_WHOLE, kh, n as i64, &[core::slice::from_raw_parts(p, n)]),
            None => j.record(J_WHOLE, kh, -1, &[]),
        };
    }
    got
}

unsafe fn read_whole_live(
    sys: &SyscallTable,
    nag: &ReadNag,
    key: &[u8],
    max: usize,
) -> Got<(*mut u8, usize)> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return Got::Absent;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return refusal(sys, nag, b"GET", key, h);
    }
    let mut cap = 64 * 1024usize;
    let mut p = heap_alloc(sys, cap as u32);
    let mut len = 0usize;
    let out = loop {
        if p.is_null() {
            break Got::Absent;
        }
        let mut rarg = [0u8; 20];
        rarg[0..8].copy_from_slice(&(len as u64).to_le_bytes());
        rarg[8..12].copy_from_slice(&((cap - len) as u32).to_le_bytes());
        rarg[12..20].copy_from_slice(&(p.add(len) as u64).to_le_bytes());
        let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
        if n < 0 {
            heap_free(sys, p);
            // Not yet: closed, and the whole read asked again later. A
            // refused read of an open handle is never absence.
            if n != READ_WAIT {
                say_refused(sys, nag, b"RANGE_GET", key, n);
            }
            break Got::Pending;
        }
        len += n as usize;
        if len < cap {
            break Got::Found((p, len));
        }
        if cap >= max {
            heap_free(sys, p);
            break Got::Absent;
        }
        cap = (cap * 2).min(max);
        p = heap_realloc(sys, p, cap as u32);
    };
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    out
}

/// A key's store revision ([`head_rev`]), as the serve first saw it. `None`
/// when absent — or when the pass is [`waiting`](Store::waiting).
unsafe fn head(sys: &Store, key: &[u8]) -> Option<u64> {
    let kh = key_hash(key);
    if sys.silent() {
        return None;
    }
    if let Some(j) = sys.j.as_mut() {
        match j.replay(J_HEAD, kh) {
            Ok(Some(c)) => {
                return (c.rc >= 0).then(|| {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&j.data(&c)[..8]);
                    u64::from_le_bytes(b)
                })
            }
            Ok(None) => {}
            Err(()) => return None,
        }
    }
    let got = match head_rev(sys.table(), sys.nag(), key) {
        Got::Pending => {
            sys.wait();
            return None;
        }
        Got::Found(r) => Some(r),
        Got::Absent => None,
    };
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some(r) => j.record(J_HEAD, kh, 0, &[&r.to_le_bytes()]),
            None => j.record(J_HEAD, kh, -1, &[]),
        };
    }
    got
}

/// storage.namespace SUBSCRIBE — push namespace.change for `prefix` onto `sink`.
/// `flags` bit0 = include-initial-listing. Returns the provider rc: a
/// handle, or `READ_WAIT` (`EAGAIN`) when the provider cannot list yet — no
/// subscription was made, and the caller subscribes again on a later step.
unsafe fn store_subscribe(sys: &SyscallTable, prefix: &[u8], sink: i32, flags: u8) -> i32 {
    let mut arg = [0u8; MAX_KEY + 16];
    if 2 + prefix.len() + 4 + 1 > arg.len() {
        return -1;
    }
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    arg[p..p + 4].copy_from_slice(&(sink as u32).to_le_bytes());
    p += 4;
    arg[p] = flags;
    p += 1;
    let rc = (sys.provider_call)(-1, NS_SUBSCRIBE, arg.as_mut_ptr(), p);
    if rc < 0 && rc != READ_WAIT {
        // The store's subscription table is full (or the prefix refused):
        // a change under it will never wake this module. Said at ERROR,
        // naming the module (its scheduler index) and the prefix.
        let mut b = [0u8; 256];
        let mut n = 0usize;
        let mut put = |part: &[u8]| {
            for &c in part {
                if let Some(slot) = b.get_mut(n) {
                    *slot = c;
                    n += 1;
                }
            }
        };
        let num = |v: u32, d: &mut [u8; 10]| -> usize {
            let mut t = [0u8; 10];
            let mut i = t.len();
            let mut u = v;
            loop {
                i -= 1;
                if let Some(slot) = t.get_mut(i) {
                    *slot = b'0' + (u % 10) as u8;
                }
                u /= 10;
                if u == 0 || i == 0 {
                    break;
                }
            }
            let k = t.len() - i;
            for (o, c) in d.iter_mut().zip(t.iter().skip(i)) {
                *o = *c;
            }
            k
        };
        let mut d = [0u8; 10];
        put(b"[store] ERROR module=");
        let k = num(dev_self_index(sys).unsigned_abs(), &mut d);
        put(d.get(..k).unwrap_or(&[]));
        put(b" SUBSCRIBE ");
        put(prefix);
        put(b" refused rc=-");
        let k = num(rc.unsigned_abs(), &mut d);
        put(d.get(..k).unwrap_or(&[]));
        put(b": the store's subscription table is full - changes under it will NOT wake this module");
        dev_log(sys, 1, b.as_ptr(), n);
    }
    rc
}

/// Drain all pending namespace.change events off `sink`; returns the count.
unsafe fn drain_changes(sys: &SyscallTable, sink: i32) -> u32 {
    let mut n_ev = 0u32;
    let mut hdr = [0u8; EVENT_HEADER_SIZE];
    loop {
        let n = (sys.channel_read)(sink, hdr.as_mut_ptr(), EVENT_HEADER_SIZE);
        if n != EVENT_HEADER_SIZE as i32 {
            break; // EAGAIN / no full header → done
        }
        n_ev += 1;
        let mut left = u16::from_le_bytes([hdr[30], hdr[31]]) as usize;
        let mut discard = [0u8; 512];
        while left > 0 {
            let take = left.min(512);
            let r = (sys.channel_read)(sink, discard.as_mut_ptr(), take);
            if r <= 0 {
                break;
            }
            left -= r as usize;
        }
    }
    n_ev
}

/// A paged walk over every key under `prefix`, in key order.
///
/// Bounded memory, unbounded prefix. The walk holds ONE raw provider page and
/// hands out one key at a time; when the page is spent it fetches the next
/// from the cursor the provider returned. There is no output buffer, so there
/// is nothing for a large prefix to overflow, and the walk is complete only
/// when the PROVIDER says so — a `cursor_len` of 0 in the trailing record.
///
/// Listing a whole prefix into one fixed caller buffer is the shape that
/// truncates silently and reads as convergence: a prefix past the bound is
/// refused, or cut at page one, or cut where the repack overflows. The fix for
/// that is not a bigger buffer; it is no buffer, which is what this walk is.
///
/// A page the walk cannot parse is a provider fault, reported at error level
/// and ending the walk with `failed` set — never passed off as the end.
///
/// A page the provider answers `EAGAIN` (not yet) — or refuses at all: a
/// listing has no "absent", and a refused page is never the end — ends
/// nothing: `next` answers `None` with `pending` set, the pass is
/// [`waiting`](Store::waiting), and a walk kept to a later step resumes at
/// the same cursor. A refusal other than `EAGAIN` is said at ERROR,
/// rate-limited ([`say_refused`]). A caller that ran out of keys checks
/// `pending` (or the store's `waiting`) before it takes the listing as
/// complete.
///
/// Usage:
///     let mut walk = ListWalk::new(PREFIX);
///     while let Some(key) = walk.next(sys) { ... }
struct ListWalk {
    prefix: [u8; MAX_KEY],
    plen: usize,
    page: [u8; LIST_PAGE],
    n: usize,
    rp: usize,
    cursor: [u8; LIST_CURSOR],
    clen: usize,
    /// The provider has said the listing is complete (or the walk failed).
    done: bool,
    /// Set when a page could not be parsed. Callers that must distinguish
    /// "no keys" from "could not list" read this after the walk.
    failed: bool,
    /// The next page has not been answered yet (`EAGAIN` or another
    /// refusal, or the pass was already waiting): the walk stopped short of
    /// its end, at its cursor.
    pending: bool,
    started: bool,
}

/// Bytes offered to one LIST call. Small on purpose: the page is stack-held
/// for the duration of the walk, and it bounds nothing but the syscall count.
const LIST_PAGE: usize = 512;
/// The contract encodes the trailing cursor's length as a u8, so 255 is the
/// most a provider can ever hand back. Sized to that bound rather than to any
/// one provider's current cursor, so a provider that moves to a key-shaped
/// cursor cannot silently end every walk at page one here.
const LIST_CURSOR: usize = 255;

impl ListWalk {
    fn new(prefix: &[u8]) -> ListWalk {
        let mut w = ListWalk {
            prefix: [0u8; MAX_KEY],
            plen: 0,
            page: [0u8; LIST_PAGE],
            n: 0,
            rp: 0,
            cursor: [0u8; LIST_CURSOR],
            clen: 0,
            done: false,
            failed: false,
            pending: false,
            started: false,
        };
        let n = prefix.len().min(MAX_KEY);
        w.prefix[..n].copy_from_slice(&prefix[..n]);
        w.plen = n;
        w
    }

    /// Fetch the next page into `self.page`. Returns false when there is
    /// nothing more to fetch.
    unsafe fn fetch(&mut self, sys: &Store) -> bool {
        if self.started && self.clen == 0 {
            self.done = true;
            return false;
        }
        if sys.silent() {
            // The pass answers nothing more: stopped at the cursor.
            self.pending = true;
            return false;
        }
        self.pending = false;
        let kh = key_hash(&self.prefix[..self.plen]) ^ key_hash(&self.cursor[..self.clen]);
        if let Some(j) = sys.j.as_mut() {
            match j.replay(J_LIST, kh) {
                Ok(Some(c)) => {
                    let n = self.took_page(sys, c.rc as i32, Some(j.data(&c)));
                    return n;
                }
                Ok(None) => {}
                Err(()) => {
                    self.done = true;
                    return false;
                }
            }
        }
        let n = self.fetch_live(sys.table());
        if n < 0 {
            // Not yet: nothing recorded, the cursor kept for the ask again.
            // A refused page is not the end of the listing either.
            if n != READ_WAIT {
                say_refused(
                    sys.table(),
                    sys.nag(),
                    b"LIST",
                    &self.prefix[..self.plen],
                    n,
                );
            }
            sys.wait();
            self.pending = true;
            return false;
        }
        if let Some(j) = sys.j.as_mut() {
            let got = if n > 0 {
                &self.page[..n as usize]
            } else {
                &[][..]
            };
            j.record(J_LIST, kh, i64::from(n), &[got]);
        }
        self.took_page(sys, n, None)
    }

    /// The provider's answer to a page fetch, as fetched or replayed.
    unsafe fn took_page(&mut self, sys: &Store, n: i32, replayed: Option<&[u8]>) -> bool {
        self.started = true;
        if n < 0 {
            let m = b"[store] LIST failed - the walk is INCOMPLETE, not empty";
            dev_log(sys.table(), 1, m.as_ptr(), m.len());
            self.failed = true;
            self.done = true;
            return false;
        }
        if let Some(d) = replayed {
            let k = d.len().min(LIST_PAGE);
            self.page[..k].copy_from_slice(&d[..k]);
        }
        self.n = n as usize;
        self.rp = 0;
        self.clen = 0;
        true
    }

    /// One LIST call into `self.page`: the provider's answer.
    unsafe fn fetch_live(&mut self, sys: &SyscallTable) -> i32 {
        let mut larg = [0u8; MAX_KEY + LIST_CURSOR + 32];
        let mut p = 0;
        larg[p..p + 2].copy_from_slice(&(self.plen as u16).to_le_bytes());
        p += 2;
        larg[p..p + self.plen].copy_from_slice(&self.prefix[..self.plen]);
        p += self.plen;
        larg[p..p + 2].copy_from_slice(&(self.clen as u16).to_le_bytes());
        p += 2;
        larg[p..p + self.clen].copy_from_slice(&self.cursor[..self.clen]);
        p += self.clen;
        larg[p..p + 8].copy_from_slice(&(self.page.as_mut_ptr() as u64).to_le_bytes());
        p += 8;
        larg[p..p + 4].copy_from_slice(&(LIST_PAGE as u32).to_le_bytes());
        p += 4;
        let mut fence = [0u8; 62];
        larg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
        p += 8;
        larg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
        p += 2;
        (sys.provider_call)(-1, NS_LIST, larg.as_mut_ptr(), p)
    }

    /// The next key, or None at the end of the listing.
    unsafe fn next(&mut self, sys: &Store) -> Option<&[u8]> {
        loop {
            if self.done {
                return None;
            }
            if self.rp >= self.n && !self.fetch(sys) {
                return None;
            }
            // entries: [name_len:u8][kind:u8][name]
            // trailer: [0xFF][0xFF][cursor_len:u8][cursor]
            //
            // BOTH sentinel bytes decide. `name_len` alone is ambiguous — a
            // name of exactly 255 bytes makes it 0xFF — and reading that as
            // the trailer drops the entry and every one after it, silently.
            // `kind` has three valid values, so 0xFF in the second position
            // can never begin an entry.
            let rp = self.rp;
            if rp >= self.n {
                // An empty page with no trailer: the provider is required to
                // terminate every page. Treat as a fault, not as the end.
                let m = b"[store] LIST page carried no trailing record - walk INCOMPLETE";
                dev_log(sys.table(), 1, m.as_ptr(), m.len());
                self.failed = true;
                self.done = true;
                return None;
            }
            let name_len = self.page[rp] as usize;
            if name_len == 0xFF && rp + 1 < self.n && self.page[rp + 1] == 0xFF {
                let cl = if rp + 3 <= self.n {
                    self.page[rp + 2] as usize
                } else {
                    0
                };
                if cl > 0 && rp + 3 + cl <= self.n && cl <= self.cursor.len() {
                    self.cursor[..cl].copy_from_slice(&self.page[rp + 3..rp + 3 + cl]);
                    self.clen = cl;
                } else if cl > 0 {
                    let m = b"[store] LIST cursor malformed - walk INCOMPLETE";
                    dev_log(sys.table(), 1, m.as_ptr(), m.len());
                    self.failed = true;
                    self.done = true;
                    return None;
                }
                // Page spent: the next call fetches from the cursor, or ends.
                self.rp = self.n;
                if self.clen == 0 {
                    self.done = true;
                    return None;
                }
                continue;
            }
            if rp + 2 + name_len > self.n {
                let m = b"[store] LIST entry overruns its page - walk INCOMPLETE";
                dev_log(sys.table(), 1, m.as_ptr(), m.len());
                self.failed = true;
                self.done = true;
                return None;
            }
            self.rp = rp + 2 + name_len;
            return Some(&self.page[rp + 2..rp + 2 + name_len]);
        }
    }
}

/// Live key count under `prefix` — a full walk, counting. `Pending` when a
/// page was not answered yet (the count would be short).
unsafe fn list_count(sys: &Store, prefix: &[u8]) -> Got<usize> {
    let mut walk = ListWalk::new(prefix);
    let mut n = 0usize;
    while walk.next(sys).is_some() {
        n += 1;
    }
    if walk.pending {
        Got::Pending
    } else {
        Got::Found(n)
    }
}

/// storage.object DELETE `key` unconditionally.
unsafe fn delete_value(sys: &Store, key: &[u8]) -> WriteAnswer {
    write(sys, key, None, PRE_ANY, 0)
}

/// storage.object PUT — write `key = value` unconditionally.
unsafe fn put_value(sys: &Store, key: &[u8], value: &[u8]) -> WriteAnswer {
    write(sys, key, Some(value), PRE_ANY, 0)
}

/// Whether a write was decided and applied. Inside a serve a write still
/// pending holds the record, so the serve's own reading of it does not
/// matter; a caller outside one asks again on [`WriteAnswer::Pending`].
fn done(a: WriteAnswer) -> bool {
    a == WriteAnswer::Decided(0)
}

/// A write's answer as the errno-shaped code the conditional callers
/// branch on; a pending write is `ERR_PENDING`, which none of them takes
/// for a lost precondition.
fn code(a: WriteAnswer) -> i32 {
    match a {
        WriteAnswer::Decided(rc) => rc,
        WriteAnswer::Pending => ERR_PENDING,
    }
}

/// Preconditions a write may carry, by the contract's discriminants
/// (`storage::object::precondition`). Named here rather than in each module so
/// a conditional write reads the same wherever it is issued.
const PRE_ANY: u8 = 0;
/// Apply only if the key does not exist; the provider answers `EEXIST` (-17).
const PRE_ABSENT: u8 = 1;
/// Apply only if the key is still at the given revision; `EAGAIN` (-11) if
/// it moved — re-read and retry.
const PRE_ETAG: u8 = 2;
const ERR_EEXIST: i32 = -17;
const ERR_EAGAIN: i32 = -11;
/// `EINPROGRESS`: [`code`] of a write not yet decided.
const ERR_PENDING: i32 = -36;
/// `ENOSPC`: a write a serve's journal has no room to hold, refused unasked.
const ERR_NO_ROOM: i32 = -28;
/// `EIO`: a write of a serve that took another path than its record.
const ERR_DIVERGED: i32 = -5;

/// Encode a revision as the contract's 32-byte etag. Both stores key the etag
/// on the per-key revision, little-endian in the first eight bytes.
fn etag_of(rev: u64) -> [u8; 32] {
    let mut e = [0u8; 32];
    e[..8].copy_from_slice(&rev.to_le_bytes());
    e
}

/// storage.object PUT under a precondition, evaluated by the provider at ONE
/// linearization point — a HEAD-then-PUT is exactly the race the contract
/// forbids. Decided: 0, `ERR_EEXIST` (ABSENT lost), `ERR_EAGAIN` (ETAG
/// moved), or another negative errno.
unsafe fn put_cond(sys: &Store, key: &[u8], value: &[u8], pre: u8, rev: u64) -> WriteAnswer {
    write(sys, key, Some(value), pre, rev)
}

/// storage.object DELETE under a precondition. Same answers as [`put_cond`].
unsafe fn delete_cond(sys: &Store, key: &[u8], pre: u8, rev: u64) -> WriteAnswer {
    write(sys, key, None, pre, rev)
}

/// A PUT (`value` given) or DELETE of `key`, through the serve's journal:
/// a write it decided answers as decided, the write it holds is asked
/// again as first asked, and a new one is asked only with room to hold it.
unsafe fn write(sys: &Store, key: &[u8], value: Option<&[u8]>, pre: u8, rev: u64) -> WriteAnswer {
    let Some(j) = sys.j.as_mut() else {
        if sys.waited.get() {
            // A read of this pass is not answered yet: whatever led here is
            // not the store's answer, so nothing is asked. The caller asks
            // again (its whole pass) on a later step.
            return WriteAnswer::Pending;
        }
        return write_live(sys.table(), key, value, pre, rev);
    };
    if j.stalled {
        return WriteAnswer::Pending;
    }
    if j.broken {
        return WriteAnswer::Decided(ERR_DIVERGED);
    }
    let kh = key_hash(key);
    match j.replay(J_WRITE, kh) {
        Err(()) => return WriteAnswer::Decided(ERR_DIVERGED),
        Ok(Some(c)) if c.rc != J_PENDING => return WriteAnswer::Decided(c.rc as i32),
        Ok(Some(c)) => {
            // The write this record is held for, asked again as it was
            // first asked: `[put][pre][rev:u64][key_len:u16][key][value]`.
            let at = (j.at - 1) as usize;
            let d = j.data(&c);
            let put = d[0] != 0;
            let (p, r) = (
                d[1],
                u64::from_le_bytes(d[2..10].try_into().unwrap_or([0; 8])),
            );
            let kl = u16::from_le_bytes([d[10], d[11]]) as usize;
            let (k, v) = (&d[12..12 + kl], &d[12 + kl..]);
            let a = write_live(sys.table(), k, put.then_some(v), p, r);
            match a {
                WriteAnswer::Pending => j.stalled = true,
                WriteAnswer::Decided(rc) => j.calls[at].rc = i64::from(rc),
            }
            return a;
        }
        Ok(None) => {}
    }
    // Room to hold it must be there before it is asked: a write the
    // provider has taken is never left untracked.
    let need = 12 + key.len() + value.map_or(0, <[u8]>::len);
    if j.n as usize >= JOURNAL_CALLS || j.used as usize + need > JOURNAL_BYTES {
        j.broken = true;
        let m = b"[store] a serve outgrew its journal - its write REFUSED, not asked";
        dev_log(sys.table(), 1, m.as_ptr(), m.len());
        return WriteAnswer::Decided(ERR_NO_ROOM);
    }
    let a = write_live(sys.table(), key, value, pre, rev);
    match a {
        WriteAnswer::Pending => {
            let mut head = [0u8; 12];
            head[0] = u8::from(value.is_some());
            head[1] = pre;
            head[2..10].copy_from_slice(&rev.to_le_bytes());
            head[10..12].copy_from_slice(&(key.len() as u16).to_le_bytes());
            j.record(J_WRITE, kh, J_PENDING, &[&head, key, value.unwrap_or(&[])]);
            j.stalled = true;
        }
        WriteAnswer::Decided(rc) => {
            j.record(J_WRITE, kh, i64::from(rc), &[]);
        }
    }
    a
}

/// The write itself: a PUT of `value`, or a DELETE.
unsafe fn write_live(
    sys: &SyscallTable,
    key: &[u8],
    value: Option<&[u8]>,
    pre: u8,
    rev: u64,
) -> WriteAnswer {
    // The value goes by POINTER, so this buffer holds only the key plus the
    // fixed fields and the etag — never the value. Sizing it for the value
    // would put several KB of dead stack under every write, in a no_std
    // environment that counts it.
    let mut arg = [0u8; MAX_KEY + 96];
    let etag_len = if pre == PRE_ETAG { 32 } else { 0 };
    // `1 + 1` is the precondition PAIR — `[precondition][etag_len]`. The bound
    // counts both bytes: the buffer's slack would otherwise hide an undercount
    // until a long key made it overflow.
    if 2 + key.len() + 1 + 8 + 8 + 1 + 1 + etag_len + 8 + 2 > arg.len() {
        return WriteAnswer::Decided(-22);
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    if let Some(value) = value {
        arg[p] = 0; // content_type_len
        p += 1;
        arg[p..p + 8].copy_from_slice(&(value.as_ptr() as u64).to_le_bytes());
        p += 8;
        arg[p..p + 8].copy_from_slice(&(value.len() as u64).to_le_bytes());
        p += 8;
    }
    // Every field after the precondition pair (including `fence_out_ptr`) is
    // positioned off its width; get it wrong and the provider reads a
    // misaligned pointer and the write is silently lost. `ANY` is the
    // explicit "apply unconditionally".
    arg[p] = pre;
    p += 1;
    arg[p] = etag_len as u8;
    p += 1;
    if etag_len > 0 {
        arg[p..p + 32].copy_from_slice(&etag_of(rev));
        p += 32;
    }
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    if value.is_some() {
        write_answer((sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p))
    } else {
        write_answer((sys.provider_call)(-1, OBJ_DELETE, arg.as_mut_ptr(), p))
    }
}

// ── TAKE: read a record and remove it, once ───────────────────────────────

/// What a [`take_value`] answered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Took {
    /// Taken, by this take and no other: the value's length in `dst`.
    Taken(usize),
    /// Not there: never stored, or taken already (by any take).
    Absent,
    /// Not decided this step — a read waits (`EAGAIN`, or any refusal but
    /// `ENXIO`/`ENOENT`) or a write is pending (`EINPROGRESS`). In a serve
    /// the record is held and served again; nothing may be answered from
    /// it, least of all "absent".
    Waiting,
    /// Not taken, and not known absent: the record kept moving under every
    /// try, the store refused a write, or the record is past the reader's
    /// buffer.
    Failed,
}

/// A record a take has claimed holds this, then its take's nonce in hex. A
/// claimed record is being taken: another take of it answers it absent.
const TAKE_MARK: &[u8] = b"\0nanocloud.take:";
const TAKE_MARK_LEN: usize = 16 + 32;
/// Reads of a record that moved between its read and its claim, before a
/// take gives up ([`Took::Failed`]).
const TAKE_TRIES: usize = 4;

/// The claim a take with `nonce` writes.
fn take_mark(nonce: &[u8; 16]) -> [u8; TAKE_MARK_LEN] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut m = [0u8; TAKE_MARK_LEN];
    for (d, s) in m.iter_mut().zip(TAKE_MARK) {
        *d = *s;
    }
    for (i, b) in nonce.iter().enumerate() {
        if let Some(d) = m.get_mut(16 + 2 * i..18 + 2 * i) {
            d[0] = HEX[(b >> 4) as usize];
            d[1] = HEX[(b & 15) as usize];
        }
    }
    m
}

/// A fresh nonce for one take: the platform's CSPRNG, or — where it has
/// none — the clock and `salt` mixed. Drawn once per record, before its
/// first serve, and kept while the record is held: every serve of one take
/// must write and recognise the same claim.
unsafe fn take_nonce(sys: &SyscallTable, salt: u64) -> [u8; 16] {
    const CSPRNG_FILL: u32 = 0x0C3C;
    let mut n = [0u8; 16];
    if (sys.provider_call)(-1, CSPRNG_FILL, n.as_mut_ptr(), n.len()) >= 0 && n != [0u8; 16] {
        return n;
    }
    let t = dev_unix_millis(sys) ^ salt.rotate_left(29) ^ (sys as *const SyscallTable as u64);
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for (i, d) in n.iter_mut().enumerate() {
        h = (h ^ t.rotate_left(i as u32 * 8)).wrapping_mul(0x0100_0000_01b3);
        *d = (h >> 56) as u8;
    }
    n
}

/// TAKE `key`: answer its value into `dst` and remove it, exactly once
/// whatever the store answers — however many takes race for it, however
/// often a read waits, and whether or not a write's answer reaches us.
///
/// The value is read with its revision from one read, then CLAIMED: the
/// record is replaced, conditional on that revision, by this take's mark
/// ([`TAKE_MARK`] + `nonce`); only then is it deleted. A claim that lost
/// its revision (`EAGAIN`) is read again: the take's own mark there means
/// its claim applied although its answer was lost (a store may answer a
/// write asked again as a fresh one), another take's mark means that take
/// has it, and the record changed means it is claimed again on its new
/// revision. A take without the claim — a delete conditional on the
/// revision read — cannot tell its own lost delete from another take's,
/// and answers a secret it spent as missing.
///
/// `scratch` holds each read (at least as large as `dst`). Outside a serve
/// the caller asks again on [`Took::Waiting`] with the same nonce.
unsafe fn take_value(
    sys: &Store,
    key: &[u8],
    nonce: &[u8; 16],
    dst: &mut [u8],
    scratch: &mut [u8],
) -> Took {
    let mark = take_mark(nonce);
    let mut have: Option<usize> = None;
    for _ in 0..TAKE_TRIES {
        let Some((n, mut rev)) = get_value_rev(sys, key, scratch) else {
            if sys.waiting() {
                return Took::Waiting;
            }
            // Absent only as the store answers absence (`ENXIO`/`ENOENT`): a
            // record the read refused for itself (past `scratch`) is there,
            // and answering it absent would spend nothing and say "missing".
            let there = exists(sys, key);
            return if sys.waiting() {
                Took::Waiting
            } else if there {
                Took::Failed
            } else {
                Took::Absent
            };
        };
        let Some(v) = scratch.get(..n) else {
            return Took::Failed;
        };
        if v == mark.as_slice() {
            // Our claim stands: what it replaced was read before it.
            return match have {
                Some(len) => take_finish(sys, key, len),
                None => Took::Failed,
            };
        }
        if v.starts_with(TAKE_MARK) {
            return Took::Absent;
        }
        let Some(d) = dst.get_mut(..n) else {
            return Took::Failed;
        };
        for (d, s) in d.iter_mut().zip(v) {
            *d = *s;
        }
        have = Some(n);
        if rev == 0 {
            // A provider without read fences: its HEAD's revision.
            match head(sys, key) {
                Some(r) => rev = r,
                None if sys.waiting() => return Took::Waiting,
                None => continue,
            }
        }
        match put_cond(sys, key, &mark, PRE_ETAG, rev) {
            WriteAnswer::Decided(0) => return take_finish(sys, key, n),
            WriteAnswer::Pending => return Took::Waiting,
            // Moved since it was read (or our own claim, its answer lost):
            // read again.
            WriteAnswer::Decided(ERR_EAGAIN) => {}
            WriteAnswer::Decided(_) => return Took::Failed,
        }
    }
    Took::Failed
}

/// The claimed record deleted: the take is done. A delete asked again after
/// its answer was lost finds nothing there, which is done too.
unsafe fn take_finish(sys: &Store, key: &[u8], len: usize) -> Took {
    match delete_value(sys, key) {
        WriteAnswer::Decided(0 | -2 | -6) => Took::Taken(len),
        WriteAnswer::Pending => Took::Waiting,
        WriteAnswer::Decided(_) => Took::Failed,
    }
}

include!("head_rev.rs");
