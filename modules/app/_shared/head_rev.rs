// A key's store revision, by HEAD — its own fragment because both store
// halves need it: `store.rs` (the connectors' conditional writes) and
// `store_source` (the revision a verdict is conditional on), which carries its
// own store plumbing and does not include `store.rs`. The including module
// supplies `MAX_KEY`.
//
// It also carries [`Got`], the answer of every live read, because both halves
// read through it.

/// What a live storage read answered: the value, its absence, or not yet.
///
/// `storage.object` lets `GET`, `HEAD` and `RANGE_GET` answer `EAGAIN` while
/// the value is not available yet (a board store whose device is busy with a
/// write, or a store still recovering), and `storage.namespace` providers
/// answer `LIST` and an initial-listing `SUBSCRIBE` the same way. That is
/// "ask again", never "absent": a reader that folds it into absence answers
/// NotFound for a key it has just written. So is any other refusal that is
/// not `ENXIO`/`ENOENT` ([`refused`]). A `match` on a `Got` has to say
/// what it does with [`Got::Pending`], which is the point.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Got<T> {
    Found(T),
    /// Absent, or refused (a value past the reader's buffer, a malformed
    /// answer): decided, and not worth asking again this pass.
    Absent,
    /// `EAGAIN`, or any refusal that is not the contract's absence
    /// ([`refused`]): not decided. Asked again on a later step; nothing may be
    /// concluded from it.
    Pending,
}

impl<T> Got<T> {
    /// The value, `None` for both other answers: only for a caller that has
    /// already handled [`Got::Pending`].
    fn found(self) -> Option<T> {
        match self {
            Got::Found(v) => Some(v),
            _ => None,
        }
    }
}

/// `EAGAIN` from a read: ask again.
const READ_WAIT: i32 = abi::kernel_abi::errno::EAGAIN;

/// What a refused read means. Only the contract's absence — `ENXIO` (an
/// absent key, as storage.object answers it) or `ENOENT` (a file-backed
/// provider) — is absence. Any other refusal is undecided and asked again
/// on a later step: an error is not evidence that a key is missing (a busy
/// store has answered a HEAD `-1`), and folding it into absence answers
/// NotFound for a record that is there.
fn refused<T>(rc: i32) -> Got<T> {
    if rc == abi::kernel_abi::errno::ENXIO || rc == abi::kernel_abi::errno::ENOENT {
        Got::Absent
    } else {
        Got::Pending
    }
}

/// How often one module says that the store refuses its reads.
const REFUSAL_SAY_MS: u64 = 10_000;

/// When this module last said a refused read, and how many refusals it has
/// kept quiet since. A store that keeps refusing is asked again every step,
/// so saying each refusal would be a log storm; saying none hides a broken
/// store behind reads that never end. Kept in the module's state (all zero
/// is a fresh one) because a [`Got::Pending`] read is asked again on a later
/// step, by a pass that is new.
struct ReadNag {
    said_ms: core::cell::Cell<u64>,
    said: core::cell::Cell<bool>,
    quiet: core::cell::Cell<u32>,
    /// The same, for a value past the reader's buffer: a module that reads
    /// such values whole on its heap meets one on every walk past it.
    past_ms: core::cell::Cell<u64>,
    past: core::cell::Cell<bool>,
    past_quiet: core::cell::Cell<u32>,
}

impl ReadNag {
    const fn new() -> ReadNag {
        ReadNag {
            said_ms: core::cell::Cell::new(0),
            said: core::cell::Cell::new(false),
            quiet: core::cell::Cell::new(0),
            past_ms: core::cell::Cell::new(0),
            past: core::cell::Cell::new(false),
            past_quiet: core::cell::Cell::new(0),
        }
    }
}

/// Say a value past the reader's buffer — refused, never read cut — at
/// ERROR, with the module and the key, at most once per [`REFUSAL_SAY_MS`]
/// per module. A reader with a heap reads it whole next (store_stamped,
/// store_big) and meets one on every walk past it: said each time, that is
/// a log storm that slows every module of the process.
unsafe fn say_past_buffer(sys: &SyscallTable, nag: &ReadNag, key: &[u8]) {
    let now = dev_millis(sys);
    if nag.past.get() && now.wrapping_sub(nag.past_ms.get()) < REFUSAL_SAY_MS {
        nag.past_quiet.set(nag.past_quiet.get().saturating_add(1));
        return;
    }
    let quiet = nag.past_quiet.get();
    nag.past.set(true);
    nag.past_ms.set(now);
    nag.past_quiet.set(0);
    let mut b = [0u8; 384];
    let mut n = 0usize;
    let mut put = |part: &[u8]| {
        for &c in part {
            if let Some(slot) = b.get_mut(n) {
                *slot = c;
                n += 1;
            }
        }
    };
    let mut d = [0u8; 20];
    put(b"[store] module=");
    let idx = dev_self_index(sys);
    if idx < 0 {
        put(b"-");
    }
    put(nag_digits(u64::from(idx.unsigned_abs()), &mut d));
    put(b" GET ");
    put(key);
    put(b": value larger than the reader's buffer - REFUSED, not truncated (read whole where this module has a heap; ");
    put(nag_digits(u64::from(quiet), &mut d));
    put(b" more since the last said)");
    dev_log(sys, 1, b.as_ptr(), n);
}

/// A read the store refused with `rc`, as [`refused`] classifies it. An
/// undecided refusal that is not `EAGAIN` (the contract's "not yet") is a
/// store answering out of contract: said at ERROR — the module, the op, the
/// key and the errno — at most once per [`REFUSAL_SAY_MS`] per module.
unsafe fn refusal<T>(sys: &SyscallTable, nag: &ReadNag, op: &[u8], key: &[u8], rc: i32) -> Got<T> {
    let got = refused(rc);
    if matches!(got, Got::Pending) && rc != READ_WAIT {
        say_refused(sys, nag, op, key, rc);
    }
    got
}

/// Say a refused read at ERROR, rate-limited by `nag` ([`refusal`]).
unsafe fn say_refused(sys: &SyscallTable, nag: &ReadNag, op: &[u8], key: &[u8], rc: i32) {
    let now = dev_millis(sys);
    if nag.said.get() && now.wrapping_sub(nag.said_ms.get()) < REFUSAL_SAY_MS {
        nag.quiet.set(nag.quiet.get().saturating_add(1));
        return;
    }
    let quiet = nag.quiet.get();
    nag.said.set(true);
    nag.said_ms.set(now);
    nag.quiet.set(0);
    let mut b = [0u8; 384];
    let mut n = 0usize;
    let mut put = |part: &[u8]| {
        for &c in part {
            if let Some(slot) = b.get_mut(n) {
                *slot = c;
                n += 1;
            }
        }
    };
    let mut d = [0u8; 20];
    put(b"[store] ERROR module=");
    let idx = dev_self_index(sys);
    if idx < 0 {
        put(b"-");
    }
    put(nag_digits(u64::from(idx.unsigned_abs()), &mut d));
    put(b" ");
    put(op);
    put(b" ");
    put(key);
    put(b" refused rc=");
    if rc < 0 {
        put(b"-");
    }
    put(nag_digits(u64::from(rc.unsigned_abs()), &mut d));
    put(b": undecided, NOT absent - asked again on a later step (");
    put(nag_digits(u64::from(quiet), &mut d));
    put(b" more refused since the last said)");
    dev_log(sys, 1, b.as_ptr(), n);
}

/// `v` in decimal, written into the end of `d`.
fn nag_digits(mut v: u64, d: &mut [u8; 20]) -> &[u8] {
    let mut i = d.len();
    loop {
        i -= 1;
        if let Some(slot) = d.get_mut(i) {
            *slot = b'0' + (v % 10) as u8;
        }
        v /= 10;
        if v == 0 || i == 0 {
            break;
        }
    }
    d.get(i..).unwrap_or(&[])
}

/// storage.object HEAD — the key's current revision (the etag's first eight
/// bytes), [`Got::Absent`] when the key is absent (`ENXIO`/`ENOENT`),
/// [`Got::Pending`] for any other refusal ([`refusal`]). The opcode is local because only a
/// conditional writer needs it, and the fragment's contract is that an
/// including module names only the opcodes every module shares.
unsafe fn head_rev(sys: &SyscallTable, nag: &ReadNag, key: &[u8]) -> Got<u64> {
    const OBJ_HEAD: u32 = 0x1422;
    let mut arg = [0u8; MAX_KEY + 32];
    if 2 + key.len() + 8 + 4 + 8 + 2 > arg.len() {
        return Got::Absent;
    }
    // `[size:u64][mtime:u64][ct_len:u8][ct][etag_len:u8][etag]` — sized for a
    // content type as long as the u8 prefix allows, so a provider that tags
    // its objects cannot push the etag off the end.
    let mut rec = [0u8; 8 + 8 + 1 + 255 + 1 + 32];
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p..p + 8].copy_from_slice(&(rec.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(rec.len() as u32).to_le_bytes());
    p += 4;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, OBJ_HEAD, arg.as_mut_ptr(), p);
    if n < 0 {
        return refusal(sys, nag, b"HEAD", key, n);
    }
    if n < 18 {
        return Got::Absent; // a malformed record: decided, not worth asking again
    }
    let n = n as usize;
    let ct = rec[16] as usize;
    let at = 17 + ct;
    if at + 1 > n || (rec[at] as usize) < 8 || at + 1 + 8 > n {
        return Got::Absent;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&rec[at + 1..at + 9]);
    Got::Found(u64::from_le_bytes(b))
}
