// Explaining failovers from the CLI (rfc_lb_vip §19.1, §19.3, §22
// "Nanocloud: a CLI over the explain library"): the region controller's
// hash-chained `FailoverExplain` ledger (`explain.v1` 0xC1, its
// `ledger_out`), one row per entry,
//
//   /wh-ledger/<service hex>/<seq, 20 digits>  = the explain.v1 record
//
// read back in order, every entry checked against its predecessor with
// Wormhole's own `LedgerCheck` (the first gap, fork or rewritten record is
// named and nothing after it is vouched for), and rendered one line each.
// The rendering names the operation, its step and status, the regions, the
// fence that allowed it, the global binding's epochs and DR generation
// before and after, and when each stage was reached. I/O-free and
// allocation-free; needs `_shared/wormhole_portfolio.rs` first.

const XH: &[u8; 16] = b"0123456789abcdef";

fn xput(out: &mut [u8], n: usize, b: &[u8]) -> usize {
    let end = (n + b.len()).min(out.len());
    out[n..end].copy_from_slice(&b[..end - n]);
    end
}

fn xnum(out: &mut [u8], n: usize, v: u64) -> usize {
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
    xput(out, n, &d[..k])
}

/// The ledger row prefix of a service (`/wh-ledger/<service hex>/`).
pub fn ledger_prefix(service: &[u8; 16], out: &mut [u8]) -> usize {
    let mut n = xput(out, 0, b"/wh-ledger/");
    for v in service {
        n = xput(out, n, &[XH[usize::from(v >> 4)], XH[usize::from(v & 15)]]);
    }
    xput(out, n, b"/")
}

/// The ledger row key of entry `seq` (zero-padded, so keys sort in order).
pub fn ledger_key(service: &[u8; 16], seq: u64, out: &mut [u8]) -> usize {
    let mut n = ledger_prefix(service, out);
    let mut d = [b'0'; 20];
    let mut x = seq;
    for i in (0..20).rev() {
        d[i] = b'0' + (x % 10) as u8;
        x /= 10;
    }
    n = xput(out, n, &d);
    n
}

fn fx_kind_name(k: u8) -> &'static [u8] {
    match k {
        traffic::fx_kind::ADMIT => b"admit",
        traffic::fx_kind::SITE_LOSS => b"site-loss",
        traffic::fx_kind::REGIONAL_DRAIN => b"regional-drain",
        traffic::fx_kind::RESTORE => b"restore",
        _ => b"other",
    }
}

fn fx_status_name(s: u8) -> &'static [u8] {
    match s {
        traffic::fx_status::STEP => b"step",
        traffic::fx_status::DONE => b"done",
        traffic::fx_status::BLOCKED_FENCE => b"blocked-fence",
        traffic::fx_status::BLOCKED_QUORUM => b"blocked-quorum",
        _ => b"other",
    }
}

fn fence_name(k: u8) -> &'static [u8] {
    match k {
        traffic::region_fence::UPSTREAM_CUT => b"upstream-cut",
        traffic::region_fence::POWER => b"power",
        traffic::region_fence::OPERATOR => b"operator",
        traffic::region_fence::SELF => b"self",
        _ => b"none",
    }
}

/// One entry as a line; the length written.
pub fn explain_failover_line(e: &traffic::FailoverExplain, out: &mut [u8]) -> usize {
    let mut n = xput(out, 0, b"#");
    n = xnum(out, n, e.seq);
    n = xput(out, n, b" ");
    n = xput(out, n, fx_kind_name(e.kind));
    n = xput(out, n, b" ");
    n = xput(out, n, fx_status_name(e.status));
    n = xput(out, n, b" step=");
    n = xnum(out, n, u64::from(e.step));
    n = xput(out, n, b" region=");
    n = xnum(out, n, u64::from(e.region));
    n = xput(out, n, b" target=");
    n = xnum(out, n, u64::from(e.target));
    n = xput(out, n, b" evidence=");
    n = xnum(out, n, u64::from(e.evidence_count));
    n = xput(out, n, b" fence=");
    n = xput(out, n, fence_name(e.fence_kind));
    n = xput(out, n, b"/");
    n = xnum(out, n, u64::from(e.fence_generation));
    n = xput(out, n, b" epoch=");
    n = xnum(out, n, e.epoch_before);
    n = xput(out, n, b"->");
    n = xnum(out, n, e.epoch_after);
    n = xput(out, n, b" dr=");
    n = xnum(out, n, e.dr_before);
    n = xput(out, n, b"->");
    n = xnum(out, n, e.dr_after);
    n = xput(out, n, b" manifest=");
    n = xnum(out, n, e.manifest_generation);
    for (name, t) in [
        (&b" t_detect="[..], e.t_detect),
        (b" t_fence=", e.t_fence),
        (b" t_commit=", e.t_commit),
        (b" t_publish=", e.t_publish),
        (b" t_done=", e.t_done),
    ] {
        n = xput(out, n, name);
        n = xnum(out, n, u64::from(t));
    }
    xput(out, n, b"\n")
}

/// A ledger being read back: each row checked against its predecessor.
#[derive(Default)]
pub struct LedgerReader {
    pub check: traffic::LedgerCheck,
    pub malformed: u32,
}

/// What one row was.
pub enum LedgerRow {
    /// An entry in order: its line is written.
    Entry(usize),
    /// Not an explain.v1 failover record.
    Malformed,
    /// The chain broke here (and stays broken).
    Fault(traffic::LedgerFault),
}

impl LedgerReader {
    pub fn row(&mut self, value: &[u8], out: &mut [u8]) -> LedgerRow {
        let entry = traffic::decode_explain_envelope(value)
            .ok()
            .and_then(|env| {
                if env.kind == traffic::ExplainKind::Failover {
                    traffic::FailoverExplain::decode(env.payload).ok()
                } else {
                    None
                }
            });
        let Some(e) = entry else {
            self.malformed += 1;
            return LedgerRow::Malformed;
        };
        match self.check.feed(&e) {
            Ok(()) => LedgerRow::Entry(explain_failover_line(&e, out)),
            Err(f) => LedgerRow::Fault(f),
        }
    }

    /// The closing verdict; the length written.
    pub fn verdict(&self, out: &mut [u8]) -> usize {
        let mut n = xput(out, 0, b"ledger: ");
        n = xnum(out, n, self.check.entries);
        n = xput(out, n, b" entries, ");
        match self.check.fault {
            None if self.malformed == 0 => xput(out, n, b"chain intact\n"),
            None => {
                n = xnum(out, n, u64::from(self.malformed));
                xput(out, n, b" malformed rows\n")
            }
            Some(traffic::LedgerFault::Gap { expected, got }) => {
                n = xput(out, n, b"GAP: expected #");
                n = xnum(out, n, expected);
                n = xput(out, n, b", found #");
                n = xnum(out, n, got);
                xput(out, n, b"\n")
            }
            Some(traffic::LedgerFault::Fork { seq }) => {
                n = xput(out, n, b"FORK or rewrite at #");
                n = xnum(out, n, seq);
                xput(out, n, b"\n")
            }
        }
    }
}
