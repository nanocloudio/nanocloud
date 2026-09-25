// Shared no_std store + parsing helpers for nanocloud fmods — included via
// `include!("../_shared/store.rs")`.
//
// One implementation of the store plumbing, for every module that touches the
// store. A per-module copy is not free: this is a quarter of all module source
// in the repo, and a copied helper drifts silently — a mis-sized write buffer
// or a dropped LIST cursor loses data without erroring.
//
// Only helpers with a SINGLE implementation across the repo live here. Where a
// module's variant is deliberately different — `image_assembler`'s `field` uses
// `>=` so a drained `pull=` reads `Some("")` where the house `>` returns None —
// it keeps its own copy, and that divergence stays visible instead of being
// flattened into a shared default that would quietly break it.
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
    /// A write of this serve is undecided: every later call of the serve
    /// answers without reaching the store, and the record is held.
    stalled: bool,
    /// The serve outgrew the journal, or took another path than the one
    /// recorded: it cannot be served again faithfully, so it makes no
    /// further write and its record is answered as refused.
    broken: bool,
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

    /// Whether this serve's record must be held (a write is undecided).
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
struct Store {
    sys: *const SyscallTable,
    j: *mut Journal,
}

impl core::ops::Deref for Store {
    type Target = SyscallTable;
    fn deref(&self) -> &SyscallTable {
        // SAFETY: built from a live `&SyscallTable`.
        unsafe { &*self.sys }
    }
}

impl Store {
    /// The store without a journal: every call goes to it.
    fn plain(sys: &SyscallTable) -> Store {
        Store {
            sys,
            j: core::ptr::null_mut(),
        }
    }

    /// The store as the record `j` journals sees it.
    fn serving(sys: &SyscallTable, j: &mut Journal) -> Store {
        Store { sys, j }
    }

    fn table(&self) -> &SyscallTable {
        self
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
unsafe fn exists(sys: &Store, key: &[u8]) -> bool {
    let kh = key_hash(key);
    if let Some(j) = sys.j.as_mut() {
        if j.silent() {
            return false;
        }
        match j.replay(J_EXISTS, kh) {
            Ok(Some(c)) => return c.rc != 0,
            Ok(None) => {}
            Err(()) => return false,
        }
    }
    let found = exists_live(sys.table(), key);
    if let Some(j) = sys.j.as_mut() {
        j.record(J_EXISTS, kh, i64::from(found), &[]);
    }
    found
}

unsafe fn exists_live(sys: &SyscallTable, key: &[u8]) -> bool {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return false;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return false;
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    true
}

/// storage.object GET+RANGE_GET+CLOSE — read `key` into `dst`. Returns length.
unsafe fn get_value(sys: &Store, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let kh = key_hash(key);
    if let Some(j) = sys.j.as_mut() {
        if j.silent() {
            return None;
        }
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
    let got = get_value_live(sys.table(), key, dst);
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some(n) => j.record(J_GET, kh, n as i64, &[&dst[..n]]),
            None => j.record(J_GET, kh, -1, &[]),
        };
    }
    got
}

unsafe fn get_value_live(sys: &SyscallTable, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return None;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return None;
    }
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
    if n >= 0 && n as usize == dst.len() {
        let mut probe = [0u8; 1];
        let mut parg = [0u8; 20];
        parg[0..8].copy_from_slice(&(dst.len() as u64).to_le_bytes());
        parg[8..12].copy_from_slice(&1u32.to_le_bytes());
        parg[12..20].copy_from_slice(&(probe.as_mut_ptr() as u64).to_le_bytes());
        if (sys.provider_call)(h, OBJ_RANGE_GET, parg.as_mut_ptr(), 20) > 0 {
            truncated = true;
        }
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    if truncated {
        let m = b"[store] GET value larger than the reader's buffer - REFUSED, not truncated";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return None;
    }
    if n < 0 {
        None
    } else {
        Some(n as usize)
    }
}

/// A value too large for a caller's fixed buffer, read WHOLE onto this
/// module's heap (the buffer doubled as it fills, up to `max`). `None` when
/// absent, past `max`, or the module has no heap (or it is exhausted). The
/// caller frees it with `heap_free`.
unsafe fn read_whole(sys: &Store, key: &[u8], max: usize) -> Option<(*mut u8, usize)> {
    let kh = key_hash(key);
    if let Some(j) = sys.j.as_mut() {
        if j.silent() {
            return None;
        }
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
    let got = read_whole_live(sys.table(), key, max);
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some((p, n)) => j.record(J_WHOLE, kh, n as i64, &[core::slice::from_raw_parts(p, n)]),
            None => j.record(J_WHOLE, kh, -1, &[]),
        };
    }
    got
}

unsafe fn read_whole_live(sys: &SyscallTable, key: &[u8], max: usize) -> Option<(*mut u8, usize)> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return None;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return None;
    }
    let mut cap = 64 * 1024usize;
    let mut p = heap_alloc(sys, cap as u32);
    let mut len = 0usize;
    let out = loop {
        if p.is_null() {
            break None;
        }
        let mut rarg = [0u8; 20];
        rarg[0..8].copy_from_slice(&(len as u64).to_le_bytes());
        rarg[8..12].copy_from_slice(&((cap - len) as u32).to_le_bytes());
        rarg[12..20].copy_from_slice(&(p.add(len) as u64).to_le_bytes());
        let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
        if n < 0 {
            heap_free(sys, p);
            break None;
        }
        len += n as usize;
        if len < cap {
            break Some((p, len));
        }
        if cap >= max {
            heap_free(sys, p);
            break None;
        }
        cap = (cap * 2).min(max);
        p = heap_realloc(sys, p, cap as u32);
    };
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    out
}

/// A key's store revision ([`head_rev`]), as the serve first saw it.
unsafe fn head(sys: &Store, key: &[u8]) -> Option<u64> {
    let kh = key_hash(key);
    if let Some(j) = sys.j.as_mut() {
        if j.silent() {
            return None;
        }
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
    let got = head_rev(sys.table(), key);
    if let Some(j) = sys.j.as_mut() {
        match got {
            Some(r) => j.record(J_HEAD, kh, 0, &[&r.to_le_bytes()]),
            None => j.record(J_HEAD, kh, -1, &[]),
        };
    }
    got
}

/// storage.namespace SUBSCRIBE — push namespace.change for `prefix` onto `sink`.
/// `flags` bit0 = include-initial-listing. Returns the provider rc.
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
    (sys.provider_call)(-1, NS_SUBSCRIBE, arg.as_mut_ptr(), p)
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
    /// Set when a page could not be parsed or fetched. Callers that must
    /// distinguish "no keys" from "could not list" read this after the walk.
    failed: bool,
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
        let kh = key_hash(&self.prefix[..self.plen]) ^ key_hash(&self.cursor[..self.clen]);
        if let Some(j) = sys.j.as_mut() {
            if j.silent() {
                self.done = true;
                return false;
            }
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

/// Live key count under `prefix` — a full walk, counting.
unsafe fn list_count(sys: &Store, prefix: &[u8]) -> usize {
    let mut walk = ListWalk::new(prefix);
    let mut n = 0usize;
    while walk.next(sys).is_some() {
        n += 1;
    }
    n
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

include!("head_rev.rs");
