// Minimal core/v1 Event emission, shared by the reconcilers that record events
// (the k8s Events surface). `include!`d after the module's own
// helpers; the including module must provide `put_value(sys, key, value) -> bool`.
//
// Events are stored at `/events/<ns>/<name>` and served by the apiserver's
// opaque object CRUD, so `kubectl get events` / `describe` see them. They
// carry NO timestamps, so the reaper in `garbage_collector` is a coarse count
// cap rather than the age-based GC k8s does. `seq` disambiguates names for recurring reasons; a
// once-per-object reason (e.g. Scheduled) can pass 0.

fn ev_cp(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

fn ev_u32(dst: &mut [u8], at: usize, mut n: u32) -> usize {
    if at >= dst.len() {
        return at;
    }
    if n == 0 {
        dst[at] = b'0';
        return at + 1;
    }
    let mut tmp = [0u8; 10];
    let mut i = 0;
    while n > 0 && i < tmp.len() {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    let mut p = at;
    while i > 0 && p < dst.len() {
        i -= 1;
        dst[p] = tmp[i];
        p += 1;
    }
    p
}

/// Event `type` inferred from the reason (Warning for failure-ish reasons).
fn ev_type(reason: &[u8]) -> &'static [u8] {
    if reason.starts_with(b"Failed")
        || reason == b"BackOff"
        || reason == b"Killing"
        || reason == b"Unhealthy"
        || reason == b"Evicted"
    {
        b"Warning"
    } else {
        b"Normal"
    }
}

/// Emit a core/v1 Event about `<kind> <ns>/<obj>`; the type is inferred from the
/// reason. `seq` disambiguates the event name for recurring reasons.
unsafe fn emit_event(
    sys: &SyscallTable,
    ns: &[u8],
    kind: &[u8],
    obj: &[u8],
    reason: &[u8],
    message: &[u8],
    seq: u32,
) {
    let etype = ev_type(reason);
    // name = <obj>.<reason>.<seq>
    let mut name = [0u8; 160];
    let mut nl = ev_cp(&mut name, 0, obj);
    nl = ev_cp(&mut name, nl, b".");
    nl = ev_cp(&mut name, nl, reason);
    nl = ev_cp(&mut name, nl, b".");
    nl = ev_u32(&mut name, nl, seq);

    // key = /events/<ns>/<name>
    let mut key = [0u8; 256];
    let mut kl = ev_cp(&mut key, 0, b"/events/");
    kl = ev_cp(&mut key, kl, ns);
    kl = ev_cp(&mut key, kl, b"/");
    kl = ev_cp(&mut key, kl, &name[..nl]);

    // A minimal Event object.
    let mut doc = [0u8; 512];
    let mut d = ev_cp(&mut doc, 0, b"{\"metadata\":{\"name\":\"");
    d = ev_cp(&mut doc, d, &name[..nl]);
    d = ev_cp(&mut doc, d, b"\",\"namespace\":\"");
    d = ev_cp(&mut doc, d, ns);
    d = ev_cp(&mut doc, d, b"\"},\"involvedObject\":{\"kind\":\"");
    d = ev_cp(&mut doc, d, kind);
    d = ev_cp(&mut doc, d, b"\",\"namespace\":\"");
    d = ev_cp(&mut doc, d, ns);
    d = ev_cp(&mut doc, d, b"\",\"name\":\"");
    d = ev_cp(&mut doc, d, obj);
    d = ev_cp(&mut doc, d, b"\"},\"reason\":\"");
    d = ev_cp(&mut doc, d, reason);
    d = ev_cp(&mut doc, d, b"\",\"message\":\"");
    d = ev_cp(&mut doc, d, message);
    d = ev_cp(&mut doc, d, b"\",\"type\":\"");
    d = ev_cp(&mut doc, d, etype);
    d = ev_cp(&mut doc, d, b"\",\"count\":1}");
    put_value(sys, &key[..kl], &doc[..d]);
}

/// Split a `/pods/<ns>/<name>`-style key (after `prefix`) into `(ns, name)`.
fn ev_ns_name<'a>(key: &'a [u8], prefix: &[u8]) -> (&'a [u8], &'a [u8]) {
    let rest = if key.len() >= prefix.len() {
        &key[prefix.len()..]
    } else {
        key
    };
    match rest.iter().position(|&b| b == b'/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (b"default", rest),
    }
}
