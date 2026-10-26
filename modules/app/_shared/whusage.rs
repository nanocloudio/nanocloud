// Tenant usage ingest (rfc_lb_vip §19, §22 "Nanocloud: ingest
// `usage_record.v1`"): Wormhole's exporters' records applied in order by
// Wormhole's own ledger (`usage_core.rs` `UsageLedger`: chained, gaps
// recorded and never skipped, duplicates and conflicts told apart, a gap
// past the replay ring closed from running totals and marked so), and
// each stream projected to one store row a bill reads:
//
//   /wh-usage/<tenant hex>/<subject hex>/<exporter hex>
//     = seq=<n>;chain=<hex>;last=<hex>;connections=<n>;bytes_in=<n>;
//       bytes_out=<n>;refusals=<n>;gaps=<open>;from_totals=<n>
//
// A restarted ingest takes its streams back from those rows
// (`usage_restore`): the sequence, chain and last digest are what the
// ledger continues from, so a record already applied before the restart is
// a duplicate after it, and the next one applies. I/O-free and
// allocation-free; needs `_shared/wormhole_portfolio.rs` first.

const UH: &[u8; 16] = b"0123456789abcdef";

fn put(out: &mut [u8], n: &mut usize, b: &[u8]) -> Option<()> {
    let end = *n + b.len();
    out.get_mut(*n..end)?.copy_from_slice(b);
    *n = end;
    Some(())
}

fn put_hex(out: &mut [u8], n: &mut usize, b: &[u8]) -> Option<()> {
    for v in b {
        put(out, n, &[UH[usize::from(v >> 4)], UH[usize::from(v & 15)]])?;
    }
    Some(())
}

fn put_u64(out: &mut [u8], n: &mut usize, v: u64) -> Option<()> {
    let mut d = [0u8; 20];
    let mut k = 0;
    let mut x = v;
    loop {
        d[k] = b'0' + (x % 10) as u8;
        k += 1;
        x /= 10;
        if x == 0 {
            break;
        }
    }
    d[..k].reverse();
    put(out, n, &d[..k])
}

/// The row key of a stream; the length written.
pub fn usage_key(s: &traffic::LedgerStream, out: &mut [u8]) -> Option<usize> {
    let mut n = 0;
    put(out, &mut n, b"/wh-usage/")?;
    put_hex(out, &mut n, &s.tenant)?;
    put(out, &mut n, b"/")?;
    put_hex(out, &mut n, &s.subject)?;
    put(out, &mut n, b"/")?;
    put_hex(out, &mut n, &s.exporter)?;
    Some(n)
}

/// The row value of a stream; the length written.
pub fn usage_row(s: &traffic::LedgerStream, out: &mut [u8]) -> Option<usize> {
    let mut n = 0;
    put(out, &mut n, b"seq=")?;
    put_u64(out, &mut n, s.seq)?;
    put(out, &mut n, b";chain=")?;
    put_hex(out, &mut n, &s.chain)?;
    put(out, &mut n, b";last=")?;
    put_hex(out, &mut n, &s.last)?;
    for (name, v) in [
        (&b";connections="[..], s.sum.connections),
        (b";bytes_in=", s.sum.bytes_in),
        (b";bytes_out=", s.sum.bytes_out),
        (b";refusals=", s.sum.refusals),
        (b";gaps=", u64::from(s.n_gaps)),
        (b";from_totals=", s.reconciled_from_totals),
    ] {
        put(out, &mut n, name)?;
        put_u64(out, &mut n, v)?;
    }
    Some(n)
}

/// One field of a row.
fn field<'a>(row: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    row.split(|c| *c == b';').find_map(|kv| {
        let eq = kv.iter().position(|c| *c == b'=')?;
        (&kv[..eq] == name).then(|| &kv[eq + 1..])
    })
}

fn parse_u64(v: &[u8]) -> Option<u64> {
    if v.is_empty() || v.len() > 20 {
        return None;
    }
    let mut n: u64 = 0;
    for c in v {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(n)
}

fn parse_hex16(v: &[u8]) -> Option<[u8; 16]> {
    if v.len() != 32 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut o = [0u8; 16];
    for i in 0..16 {
        o[i] = (nib(v[2 * i])? << 4) | nib(v[2 * i + 1])?;
    }
    Some(o)
}

/// Take a stream back from its key and row after a restart. Open gaps are
/// not carried: a gap open at the restart is closed from the next record's
/// totals and marked so, never assumed filled.
pub fn usage_restore(ledger: &mut traffic::UsageLedger, key: &[u8], row: &[u8]) -> bool {
    let rest = match key.strip_prefix(b"/wh-usage/") {
        Some(r) if r.len() == 32 * 3 + 2 => r,
        _ => return false,
    };
    let (Some(tenant), Some(subject), Some(exporter)) = (
        parse_hex16(&rest[0..32]),
        parse_hex16(&rest[33..65]),
        parse_hex16(&rest[66..98]),
    ) else {
        return false;
    };
    let get = |n: &[u8]| field(row, n).and_then(parse_u64);
    let (Some(seq), Some(chain), Some(last)) = (
        get(b"seq"),
        field(row, b"chain").and_then(parse_hex16),
        field(row, b"last").and_then(parse_hex16),
    ) else {
        return false;
    };
    let sum = traffic::UsageCounters {
        connections: get(b"connections").unwrap_or(0),
        bytes_in: get(b"bytes_in").unwrap_or(0),
        bytes_out: get(b"bytes_out").unwrap_or(0),
        refusals: get(b"refusals").unwrap_or(0),
    };
    let Some(slot) = ledger.streams.iter_mut().find(|s| s.used == 0) else {
        return false;
    };
    *slot = traffic::LedgerStream {
        used: 1,
        exporter,
        tenant,
        subject,
        seq,
        chain,
        sum,
        last,
        reconciled_from_totals: get(b"from_totals").unwrap_or(0),
        ..traffic::LedgerStream::default()
    };
    true
}

/// Apply one `usage_record.v1` (144 bytes) and name the stream it moved,
/// whose row is then written; `None` for bytes that are not a record.
pub fn usage_ingest(
    ledger: &mut traffic::UsageLedger,
    rec: &[u8],
) -> Option<(traffic::LedgerOutcome, Option<traffic::LedgerStream>)> {
    let r = traffic::UsageRecord::decode(rec).ok()?;
    let outcome = ledger.accept(&r);
    let moved = matches!(
        outcome,
        traffic::LedgerOutcome::Applied
            | traffic::LedgerOutcome::Gap(_)
            | traffic::LedgerOutcome::Filled
    );
    let s = if moved {
        ledger.stream(&r.exporter, &r.tenant, &r.subject).copied()
    } else {
        None
    };
    Some((outcome, s))
}
