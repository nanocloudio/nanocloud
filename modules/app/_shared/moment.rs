// A moment, as a node records one before it knows what time it is.
// `include!`d by store_effect (which stamps them, `wall = 2`) and
// store_source (which reads them back as wall-clock time, `!wall`).
//
// A board has no RTC: until SNTP anchors its clock, the wall clock reads 0
// (or the time since boot), and a moment stamped with it is 1970 — wrong,
// and indistinguishable from right. So a moment is stamped as what IS known:
//
//   <unix ms>             the wall clock, when the platform says it is
//                         TRUSTED (`TIMER::TRUSTED_UNIX`: synchronised)
//   m<mono ms>@<boot>     otherwise: the monotonic clock and the boot it
//                         counts from (this boot's incarnation, hex)
//
// and read back (`!wall`) as Unix ms once the clock is trusted — the wall
// clock now, less how long ago it was on the monotonic clock — provided it
// is this boot's (another boot's monotonic clock means nothing here). Until
// then, and forever for another boot's, it reads EMPTY: absent, never wrong.

/// The trusted wall clock in Unix ms, or None.
unsafe fn mo_trusted_ms(sys: &SyscallTable) -> Option<u64> {
    let r = dev_trusted_unix(sys);
    let flags = *r.get(33)?;
    if flags & 0x02 == 0 {
        return None;
    }
    let secs = u64::from_le_bytes(r.get(0..8)?.try_into().ok()?);
    let nanos = u32::from_le_bytes(r.get(8..12)?.try_into().ok()?);
    if secs == 0 {
        return None;
    }
    Some(
        secs.saturating_mul(1000)
            .saturating_add(u64::from(nanos / 1_000_000)),
    )
}

fn mo_dec(v: u64, out: &mut [u8], at: usize) -> usize {
    let mut t = [0u8; 20];
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
    let mut p = at;
    for &c in t.iter().skip(i) {
        if let Some(slot) = out.get_mut(p) {
            *slot = c;
            p += 1;
        }
    }
    p
}

/// This boot's incarnation as 32 hex digits into `out` at `at`; `-` when the
/// kernel has none yet.
unsafe fn mo_boot(sys: &SyscallTable, out: &mut [u8], at: usize) -> usize {
    let hexd = b"0123456789abcdef";
    let mut p = at;
    match dev_boot_incarnation(sys) {
        Some(b) => {
            for x in b {
                for nib in [x >> 4, x & 15] {
                    if let Some(slot) = out.get_mut(p) {
                        *slot = *hexd.get((nib & 15) as usize).unwrap_or(&b'0');
                        p += 1;
                    }
                }
            }
        }
        None => {
            if let Some(slot) = out.get_mut(p) {
                *slot = b'-';
                p += 1;
            }
        }
    }
    p
}

/// The moment now, as the header says; its length in `out` (64 bytes hold
/// either form).
unsafe fn mo_now(sys: &SyscallTable, out: &mut [u8; 64]) -> usize {
    match mo_trusted_ms(sys) {
        Some(ms) => mo_dec(ms, out, 0),
        None => {
            out[0] = b'm';
            let p = mo_dec(dev_millis(sys), out, 1);
            let p = match out.get_mut(p) {
                Some(slot) => {
                    *slot = b'@';
                    p + 1
                }
                None => p,
            };
            mo_boot(sys, out, p)
        }
    }
}

/// `!wall`: a recorded moment as Unix ms (the header), into `out`; its
/// length, 0 when it cannot be said truthfully.
unsafe fn mo_wall(sys: &SyscallTable, src: &[u8], out: &mut [u8]) -> usize {
    if !src.is_empty() && src.iter().all(|c| c.is_ascii_digit()) {
        let n = src.len().min(out.len());
        for (o, &c) in out.iter_mut().zip(src.iter()) {
            *o = c;
        }
        return n;
    }
    let Some(rest) = src.strip_prefix(b"m") else {
        return 0;
    };
    let Some(at) = rest.iter().position(|&c| c == b'@') else {
        return 0;
    };
    let (Some(mono_s), Some(boot_s)) = (rest.get(..at), rest.get(at + 1..)) else {
        return 0;
    };
    if mono_s.is_empty() || mono_s.len() > 19 || !mono_s.iter().all(|c| c.is_ascii_digit()) {
        return 0;
    }
    let mut mono = 0u64;
    for &c in mono_s {
        mono = mono.saturating_mul(10).saturating_add(u64::from(c - b'0'));
    }
    let mut boot = [0u8; 40];
    let bl = mo_boot(sys, &mut boot, 0);
    if boot.get(..bl) != Some(boot_s) || boot_s == b"-" {
        return 0;
    }
    let Some(now_wall) = mo_trusted_ms(sys) else {
        return 0;
    };
    let ago = dev_millis(sys).saturating_sub(mono);
    mo_dec(now_wall.saturating_sub(ago), out, 0)
}

/// The moment `ms` from now, in the header's form; its length in `out`. A
/// deadline stamped so is a WALL time when the clock is trusted — it means
/// the same after a restart — and this boot's monotonic time otherwise.
unsafe fn mo_after(sys: &SyscallTable, ms: u64, out: &mut [u8; 64]) -> usize {
    match mo_trusted_ms(sys) {
        Some(now) => mo_dec(now.saturating_add(ms), out, 0),
        None => {
            out[0] = b'm';
            let p = mo_dec(dev_millis(sys).saturating_add(ms), out, 1);
            let p = match out.get_mut(p) {
                Some(slot) => {
                    *slot = b'@';
                    p + 1
                }
                None => p,
            };
            mo_boot(sys, out, p)
        }
    }
}

/// A recorded deadline moment as THIS boot's monotonic ms (the clock a
/// source's `now` and its timer run on), and whether it had to be re-based.
///
///   <unix ms>          the clock trusted now: now, plus how far ahead it is
///                      (a deadline passed is due now)
///   m<mono>@<boot>     this boot's: as it is
///
/// A deadline that cannot be said on this boot — another boot's monotonic
/// time, or a wall time while the clock is not trusted — is RE-BASED to
/// `ttl` past this boot's origin: it ends within `ttl` of the restart,
/// never kept for ever and never ended early by a clock that means nothing
/// here. The re-base is the same on every read of this boot (it moves with
/// nothing), so the deadline is deterministic. An empty or malformed
/// deadline is 0 (none).
unsafe fn mo_due(sys: &SyscallTable, src: &[u8], ttl: u64) -> (i64, bool) {
    if src.is_empty() {
        return (0, false);
    }
    if src.iter().all(|c| c.is_ascii_digit()) {
        let mut wall = 0u64;
        for &c in src.iter().take(19) {
            wall = wall.saturating_mul(10).saturating_add(u64::from(c - b'0'));
        }
        return match mo_trusted_ms(sys) {
            Some(now_wall) => {
                let now = dev_millis(sys);
                let d = if wall <= now_wall {
                    now
                } else {
                    now.saturating_add(wall - now_wall)
                };
                (d.max(1).min(i64::MAX as u64) as i64, false)
            }
            None => (ttl.max(1).min(i64::MAX as u64) as i64, true),
        };
    }
    let Some(rest) = src.strip_prefix(b"m") else {
        return (0, false);
    };
    let Some(at) = rest.iter().position(|&c| c == b'@') else {
        return (0, false);
    };
    let (Some(mono_s), Some(boot_s)) = (rest.get(..at), rest.get(at + 1..)) else {
        return (0, false);
    };
    if mono_s.is_empty() || mono_s.len() > 19 || !mono_s.iter().all(|c| c.is_ascii_digit()) {
        return (0, false);
    }
    let mut mono = 0u64;
    for &c in mono_s {
        mono = mono.saturating_mul(10).saturating_add(u64::from(c - b'0'));
    }
    let mut boot = [0u8; 40];
    let bl = mo_boot(sys, &mut boot, 0);
    if boot.get(..bl) == Some(boot_s) && boot_s != b"-" {
        return (mono.max(1).min(i64::MAX as u64) as i64, false);
    }
    (ttl.max(1).min(i64::MAX as u64) as i64, true)
}

/// A recorded moment PLUS `off` ms, as THIS boot's monotonic ms — a deadline
/// measured from a moment a record kept (a Job's start plus its
/// activeDeadlineSeconds, a failure plus its backoff), and whether it had to
/// be re-based.
///
///   <unix ms>          the clock trusted now: the deadline on this boot's
///                      monotonic clock — now, plus how far ahead it is, or
///                      less how long ago it passed (never past now, so a
///                      record read at `now` compares `now >= deadline`
///                      truly however many ms the read took)
///   m<mono>@<boot>     this boot's: `mono + off`
///
/// A moment this boot cannot say (another boot's monotonic time, a wall
/// time while the clock is untrusted) is re-based to `off` past this boot's
/// origin, as [`mo_due`] re-bases to its ttl: the wait runs again from the
/// restart, never for ever and never cut short by a clock that means
/// nothing here. An empty or malformed moment is 0 (none).
unsafe fn mo_due_after(sys: &SyscallTable, src: &[u8], off: u64) -> (i64, bool) {
    if src.is_empty() {
        return (0, false);
    }
    if src.iter().all(|c| c.is_ascii_digit()) {
        let mut wall = 0u64;
        for &c in src.iter().take(19) {
            wall = wall.saturating_mul(10).saturating_add(u64::from(c - b'0'));
        }
        let wall = wall.saturating_add(off);
        return match mo_trusted_ms(sys) {
            Some(now_wall) => {
                let now = dev_millis(sys);
                let d = if wall <= now_wall {
                    now.saturating_sub(now_wall - wall)
                } else {
                    now.saturating_add(wall - now_wall)
                };
                (d.max(1).min(i64::MAX as u64) as i64, false)
            }
            None => (off.max(1).min(i64::MAX as u64) as i64, true),
        };
    }
    let Some(rest) = src.strip_prefix(b"m") else {
        return (0, false);
    };
    let Some(at) = rest.iter().position(|&c| c == b'@') else {
        return (0, false);
    };
    let (Some(mono_s), Some(boot_s)) = (rest.get(..at), rest.get(at + 1..)) else {
        return (0, false);
    };
    if mono_s.is_empty() || mono_s.len() > 19 || !mono_s.iter().all(|c| c.is_ascii_digit()) {
        return (0, false);
    }
    let mut mono = 0u64;
    for &c in mono_s {
        mono = mono.saturating_mul(10).saturating_add(u64::from(c - b'0'));
    }
    let mut boot = [0u8; 40];
    let bl = mo_boot(sys, &mut boot, 0);
    if boot.get(..bl) == Some(boot_s) && boot_s != b"-" {
        let d = mono.saturating_add(off);
        return (d.max(1).min(i64::MAX as u64) as i64, false);
    }
    (off.max(1).min(i64::MAX as u64) as i64, true)
}
