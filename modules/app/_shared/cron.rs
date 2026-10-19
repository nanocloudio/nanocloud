// Cron schedules, as a CronJob states them — `include!`d by store_source
// (its `cron` param). Self-contained: civil-date arithmetic and the parser.
//
// The standard five fields (minute hour day-of-month month day-of-week),
// each `*` (or `?`), a value, a range `a-b`, a step `*/n` / `a-b/n` / `a/n`,
// or a comma list of those; month and weekday names (JAN…DEC, SUN…SAT);
// weekday 7 is Sunday; and the descriptors @yearly @annually @monthly
// @weekly @daily @midnight @hourly. A day matches as cron's does: when both
// day fields are restricted, either may match; when one is `*`, the other
// must.
//
// A time zone is the CronJob's spec.timeZone: unset, UTC (and its aliases),
// or a fixed `Etc/GMT±N` offset. A zone with daylight saving needs the tz
// database, which this module does not carry: such a zone is refused (the
// CronJob is not scheduled) rather than scheduled in the wrong hour.

#[derive(Clone, Copy)]
struct CronSpec {
    min: u64,
    hour: u32,
    dom: u32,
    month: u16,
    dow: u8,
    dom_star: bool,
    dow_star: bool,
}

/// Days since 1970-01-01 of a civil date (proleptic Gregorian).
fn cr_days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil (month, day) of a day number.
fn cr_civil_md(days: i64) -> (i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (m, d)
}

fn cr_num(t: &[u8]) -> Option<u32> {
    if t.is_empty() || t.len() > 4 {
        return None;
    }
    let mut v = 0u32;
    for &c in t {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u32::from(c - b'0');
    }
    Some(v)
}

/// A value: a number, or (month, weekday fields) a three-letter name —
/// `names` the names run together, three bytes each (a table of slices
/// would be a table of pointers, which a module cannot hold).
fn cr_value(t: &[u8], names: &[u8], base: u32) -> Option<u32> {
    if let Some(v) = cr_num(t) {
        return Some(v);
    }
    if t.len() != 3 {
        return None;
    }
    let mut i = 0usize;
    while let Some(n) = names.get(i * 3..i * 3 + 3) {
        if t.iter()
            .zip(n.iter())
            .all(|(a, b)| a.to_ascii_uppercase() == *b)
        {
            return Some(base + i as u32);
        }
        i += 1;
    }
    None
}

/// One field into a bit set over lo..=hi; whether it was `*`/`?`.
fn cr_field(f: &[u8], lo: u32, hi: u32, names: &[u8], base: u32) -> Option<(u64, bool)> {
    let mut bits = 0u64;
    let star = f == b"*" || f == b"?";
    for part in f.split(|&c| c == b',') {
        let (range, step) = match part.iter().position(|&c| c == b'/') {
            Some(i) => (part.get(..i)?, cr_num(part.get(i + 1..)?)?),
            None => (part, 0),
        };
        let (a, b) = if range == b"*" || range == b"?" {
            (lo, hi)
        } else if let Some(i) = range.iter().position(|&c| c == b'-') {
            (
                cr_value(range.get(..i)?, names, base)?,
                cr_value(range.get(i + 1..)?, names, base)?,
            )
        } else {
            let v = cr_value(range, names, base)?;
            // `a/n`: from a to the end, every n.
            (v, if step > 0 { hi } else { v })
        };
        if a < lo || b > hi || a > b || b > 63 || (step == 0 && !part.iter().all(|&c| c != b'/')) {
            return None;
        }
        let step = step.max(1);
        let mut v = a;
        while v <= b {
            bits |= 1u64.wrapping_shl(v);
            v += step;
        }
    }
    Some((bits, star))
}

/// Parse a schedule; None when it is not one.
fn cr_parse(spec: &[u8]) -> Option<CronSpec> {
    // The descriptors, each its own call: a `match` answering slices would
    // lower to a table of pointers.
    if spec == b"@yearly" || spec == b"@annually" {
        return cr_parse5(b"0 0 1 1 *");
    }
    if spec == b"@monthly" {
        return cr_parse5(b"0 0 1 * *");
    }
    if spec == b"@weekly" {
        return cr_parse5(b"0 0 * * 0");
    }
    if spec == b"@daily" || spec == b"@midnight" {
        return cr_parse5(b"0 0 * * *");
    }
    if spec == b"@hourly" {
        return cr_parse5(b"0 * * * *");
    }
    cr_parse5(spec)
}

fn cr_parse5(spec: &[u8]) -> Option<CronSpec> {
    let mut fields: [&[u8]; 5] = [&[]; 5];
    let mut n = 0usize;
    for f in spec
        .split(|&c| c == b' ' || c == b'\t')
        .filter(|f| !f.is_empty())
    {
        let slot = fields.get_mut(n)?;
        *slot = f;
        n += 1;
    }
    if n != 5 {
        return None;
    }
    let (min, _) = cr_field(fields[0], 0, 59, b"", 0)?;
    let (hour, _) = cr_field(fields[1], 0, 23, b"", 0)?;
    let (dom, dom_star) = cr_field(fields[2], 1, 31, b"", 0)?;
    let (month, _) = cr_field(fields[3], 1, 12, b"JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC", 1)?;
    let (dow, dow_star) = cr_field(fields[4], 0, 7, b"SUNMONTUEWEDTHUFRISAT", 0)?;
    // 7 is Sunday too.
    let dow = (dow | dow.wrapping_shr(7)) & 0x7f;
    Some(CronSpec {
        min,
        hour: hour as u32,
        dom: dom as u32,
        month: month as u16,
        dow: dow as u8,
        dom_star,
        dow_star,
    })
}

/// A time zone's offset from UTC in seconds; None for a zone not carried.
fn cr_zone(tz: &[u8]) -> Option<i64> {
    match tz {
        b"" | b"UTC" | b"Etc/UTC" | b"GMT" | b"Etc/GMT" | b"UCT" | b"Etc/UCT" | b"Zulu"
        | b"Etc/Zulu" | b"Universal" | b"Etc/Universal" | b"Etc/Greenwich" | b"Greenwich"
        | b"Etc/GMT0" | b"Etc/GMT+0" | b"Etc/GMT-0" | b"GMT0" => Some(0),
        _ => {
            // `Etc/GMT+N` is N hours WEST of Greenwich (POSIX's sign).
            let rest = tz.strip_prefix(b"Etc/GMT")?;
            let (sign, num) = match rest.first() {
                Some(&b'+') => (-1i64, rest.get(1..)?),
                Some(&b'-') => (1i64, rest.get(1..)?),
                _ => return None,
            };
            let h = i64::from(cr_num(num)?);
            if h > 14 {
                return None;
            }
            Some(sign * h * 3600)
        }
    }
}

/// Bit `i` of `v` (false out of range).
fn cr_bit(v: u64, i: i64) -> bool {
    (0..64).contains(&i) && v.wrapping_shr(i as u32) & 1 == 1
}

fn cr_day_ok(c: &CronSpec, day: i64) -> bool {
    let (m, d) = cr_civil_md(day);
    if !cr_bit(u64::from(c.month), m) {
        return false;
    }
    let dow = (day + 4).rem_euclid(7);
    let dom_ok = cr_bit(u64::from(c.dom), d);
    let dow_ok = cr_bit(u64::from(c.dow), dow);
    if c.dom_star || c.dow_star {
        dom_ok && dow_ok
    } else {
        dom_ok || dow_ok
    }
}

/// How far a search walks, in days: a 29 February schedule fires once in
/// four years (eight across a skipped leap year, as 2100 is).
const CR_SPAN_DAYS: i64 = 8 * 366;

/// The latest scheduled minute at or before `now` (Unix seconds), in Unix
/// seconds; None when there is none within the span.
fn cr_prev(c: &CronSpec, now: i64, off: i64) -> Option<i64> {
    let local = now + off;
    let day0 = local.div_euclid(86_400);
    let sod = local.rem_euclid(86_400);
    for k in 0..=CR_SPAN_DAYS {
        let day = day0 - k;
        if !cr_day_ok(c, day) {
            continue;
        }
        let top_h = if k == 0 { sod / 3600 } else { 23 };
        let mut h = top_h;
        while h >= 0 {
            if cr_bit(u64::from(c.hour), h) {
                let top_m = if k == 0 && h == top_h {
                    (sod % 3600) / 60
                } else {
                    59
                };
                let mut m = top_m;
                while m >= 0 {
                    if cr_bit(c.min, m) {
                        return Some(day * 86_400 + h * 3600 + m * 60 - off);
                    }
                    m -= 1;
                }
            }
            h -= 1;
        }
    }
    None
}

/// The earliest scheduled minute strictly after `now` (Unix seconds).
fn cr_next(c: &CronSpec, now: i64, off: i64) -> Option<i64> {
    let local = now + off;
    let day0 = local.div_euclid(86_400);
    let sod = local.rem_euclid(86_400);
    for k in 0..=CR_SPAN_DAYS {
        let day = day0 + k;
        if !cr_day_ok(c, day) {
            continue;
        }
        let from_h = if k == 0 { sod / 3600 } else { 0 };
        for h in from_h..24 {
            if !cr_bit(u64::from(c.hour), h) {
                continue;
            }
            let from_m = if k == 0 && h == from_h {
                (sod % 3600) / 60 + 1
            } else {
                0
            };
            for m in from_m..60 {
                if cr_bit(c.min, m) {
                    return Some(day * 86_400 + h * 3600 + m * 60 - off);
                }
            }
        }
    }
    None
}
