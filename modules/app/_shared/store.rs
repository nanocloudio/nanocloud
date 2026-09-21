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
// The including module must define `MAX_KEY`, `OBJ_GET` and `OBJ_CLOSE`.

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

/// True iff `key` currently exists (open+close a GET handle).
unsafe fn exists(sys: &SyscallTable, key: &[u8]) -> bool {
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
unsafe fn get_value(sys: &SyscallTable, key: &[u8], dst: &mut [u8]) -> Option<usize> {
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
    unsafe fn fetch(&mut self, sys: &SyscallTable) -> bool {
        if self.started && self.clen == 0 {
            self.done = true;
            return false;
        }
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
        let n = (sys.provider_call)(-1, NS_LIST, larg.as_mut_ptr(), p);
        self.started = true;
        if n < 0 {
            let m = b"[store] LIST failed - the walk is INCOMPLETE, not empty";
            dev_log(sys, 1, m.as_ptr(), m.len());
            self.failed = true;
            self.done = true;
            return false;
        }
        self.n = n as usize;
        self.rp = 0;
        self.clen = 0;
        true
    }

    /// The next key, or None at the end of the listing.
    unsafe fn next(&mut self, sys: &SyscallTable) -> Option<&[u8]> {
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
                dev_log(sys, 1, m.as_ptr(), m.len());
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
                    dev_log(sys, 1, m.as_ptr(), m.len());
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
                dev_log(sys, 1, m.as_ptr(), m.len());
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
unsafe fn list_count(sys: &SyscallTable, prefix: &[u8]) -> usize {
    let mut walk = ListWalk::new(prefix);
    let mut n = 0usize;
    while walk.next(sys).is_some() {
        n += 1;
    }
    n
}

/// storage.object DELETE `key` unconditionally; true on success.
unsafe fn delete_value(sys: &SyscallTable, key: &[u8]) -> bool {
    let mut arg = [0u8; MAX_KEY + 32];
    if 2 + key.len() + 1 + 8 + 2 > arg.len() {
        return false;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    // storage.object writes carry a precondition PAIR — `[precondition:u8]
    // [etag_len:u8]` — two bytes, not one. Every field after it (including
    // `fence_out_ptr`) is positioned off that width, so getting it wrong hands
    // the provider a misaligned pointer and the write is silently lost. `ANY`
    // is the explicit "apply unconditionally".
    arg[p] = 0; // precondition::ANY
    p += 1;
    arg[p] = 0; // etag_len (none, under ANY)
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    (sys.provider_call)(-1, OBJ_DELETE, arg.as_mut_ptr(), p) == 0
}

/// storage.object PUT — write `key = value` unconditionally. True on success.
unsafe fn put_value(sys: &SyscallTable, key: &[u8], value: &[u8]) -> bool {
    // The value goes by POINTER, so this buffer holds only the key plus 31
    // fixed bytes — never the value. Sizing it `MAX_KEY + MAX_VALUE + 64`
    // would put several KB of dead stack under every write, in a no_std
    // environment that counts it.
    let mut arg = [0u8; MAX_KEY + 64];
    // `1 + 1` is the precondition PAIR — `[precondition][etag_len]`. The
    // bound counts both bytes: the buffer's slack would otherwise hide an
    // undercount until a long key made it overflow.
    if 2 + key.len() + 1 + 8 + 8 + 1 + 1 + 8 + 2 > arg.len() {
        return false;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p] = 0; // content_type_len
    p += 1;
    arg[p..p + 8].copy_from_slice(&(value.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(value.len() as u64).to_le_bytes());
    p += 8;
    // storage.object writes carry a precondition PAIR — `[precondition:u8]
    // [etag_len:u8]` — two bytes, not one. Every field after it (including
    // `fence_out_ptr`) is positioned off that width; get it wrong and the
    // provider reads a misaligned pointer and the write is silently lost.
    // `ANY` is the explicit "apply unconditionally".
    arg[p] = 0; // precondition::ANY
    p += 1;
    arg[p] = 0; // etag_len (none, under ANY)
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    (sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p) == 0
}
