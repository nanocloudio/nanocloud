// A deleted object's tomb, and when it ends — `include!`d (after store.rs,
// json.rs and moment.rs) by the writers that leave one: store_doc and
// store_effect (`tomb`).
//
// The tomb is `<tomb><key>` = `{"deadline":"<moment>","obj":<object>}`: the
// object's last state, for a watch's DELETED event, as large as the object.
// Its END is indexed apart, at `<tomb>-at<key>`:
//
//   {"deadline":"<moment>","ttl":<ms>,"obj":<identity>}
//
// small whatever the object's size, so every reader holds it in one record
// (a store_source refuses a value past its record; a tomb it had to read
// would never end). `<identity>` is the object pruned to what the tomb
// chains read — metadata name, namespace and uid; a Pod's node, runtime
// class, first image and grace (the delete's
// `metadata.deletionGracePeriodSeconds`, its own
// `spec.terminationGracePeriodSeconds`) — at the same paths, so a chain
// reads `obj.spec.nodeName` from the index exactly as it did from the tomb.
// A writer that knows the delete's grace (store_doc: the request's
// DeleteOptions) states it; the rest read the object's. The
// tomb chain scans the index and, at the deadline, drops both together
// (store_effect `OP_DROP_EACH` over `tomb,tomb-at`).
//
// The deadline is a MOMENT (moment.rs): wall-clock ms when the clock is
// trusted — it means the same after a restart — else this boot's monotonic
// ms. A source reads it back (`!due:ttl`, moment.rs `mo_due`) as this
// boot's monotonic time; one it cannot say on this boot (another boot's
// monotonic time, a wall time with the clock untrusted) is re-based to
// `ttl` past this boot's origin, and said: a tomb ends within its ttl of a
// restart, never lives for ever. The moment is drawn through the serve's
// journal ([`drawn_bytes`]), so a held delete asks again the bytes it first
// asked.
//
// The index is written FIRST: a tomb is never there without the entry that
// ends it (an entry without its tomb ends nothing, harmlessly).

/// The index entry's room: the deadline, the ttl and the identity.
const TOMB_AT_MAX: usize = 2048;

/// A deadline `ttl` ms from now, as a moment, drawn through the journal.
unsafe fn tomb_deadline(sys: &Store, ttl: u64, out: &mut [u8; 64]) -> usize {
    drawn_bytes(sys, out, |o| {
        let mut m = [0u8; 64];
        let n = mo_after(sys.table(), ttl, &mut m);
        for (d, s) in o.iter_mut().zip(m.iter().take(n)) {
            *d = *s;
        }
        n.min(o.len())
    })
}

/// `"k":"v"` (or `,"k":"v"`) into `out` at `l` when `v` is there.
fn tomb_member(out: &mut [u8], l: usize, first: &mut bool, k: &[u8], v: Option<&[u8]>) -> usize {
    let Some(v) = v else {
        return l;
    };
    let mut l = l;
    if !*first {
        l = append(out, l, b",");
    }
    *first = false;
    l = append(out, l, b"\"");
    l = append(out, l, k);
    l = append(out, l, b"\":\"");
    l = append(out, l, v);
    append(out, l, b"\"")
}

/// `"k":<n>` (or `,"k":<n>`) into `out` at `l` when `v` is there.
fn tomb_number(out: &mut [u8], l: usize, first: &mut bool, k: &[u8], v: Option<u64>) -> usize {
    let Some(v) = v else {
        return l;
    };
    let mut l = l;
    if !*first {
        l = append(out, l, b",");
    }
    *first = false;
    l = append(out, l, b"\"");
    l = append(out, l, k);
    l = append(out, l, b"\":");
    let mut d = [0u8; 20];
    let n = mo_dec(v, &mut d, 0);
    append(out, l, d.get(..n).unwrap_or(&[]))
}

/// A JSON number's digits as a count; `None` for anything else.
fn tomb_count(v: Option<&[u8]>) -> Option<u64> {
    let v = v?;
    if v.is_empty() || v.len() > 12 || v.iter().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    Some(v.iter().fold(0u64, |n, &c| n * 10 + u64::from(c - b'0')))
}

/// The object pruned to what the tomb chains read, at the same paths. Each
/// path by its own call (`j_get2`): an array of byte-string literals is a
/// table of unrelocated pointers in a PIC module. `grace`: the delete's
/// grace when the writer knows it, written as the object's
/// `metadata.deletionGracePeriodSeconds` (otherwise the object's own).
fn tomb_identity(obj: &[u8], grace: Option<u64>, out: &mut [u8]) -> usize {
    let mut l = append(out, 0, b"{\"metadata\":{");
    let mut first = true;
    l = tomb_member(
        out,
        l,
        &mut first,
        b"name",
        j_get2(obj, b"metadata", b"name"),
    );
    l = tomb_member(
        out,
        l,
        &mut first,
        b"namespace",
        j_get2(obj, b"metadata", b"namespace"),
    );
    l = tomb_member(out, l, &mut first, b"uid", j_get2(obj, b"metadata", b"uid"));
    let dgrace =
        grace.or_else(|| tomb_count(j_get2(obj, b"metadata", b"deletionGracePeriodSeconds")));
    l = tomb_number(out, l, &mut first, b"deletionGracePeriodSeconds", dgrace);
    l = append(out, l, b"},\"spec\":{");
    let mut first = true;
    l = tomb_member(
        out,
        l,
        &mut first,
        b"nodeName",
        j_get2(obj, b"spec", b"nodeName"),
    );
    l = tomb_member(
        out,
        l,
        &mut first,
        b"runtimeClassName",
        j_get2(obj, b"spec", b"runtimeClassName"),
    );
    l = tomb_number(
        out,
        l,
        &mut first,
        b"terminationGracePeriodSeconds",
        tomb_count(j_get2(obj, b"spec", b"terminationGracePeriodSeconds")),
    );
    if let Some(img) = j_get4(obj, b"spec", b"containers", b"0", b"image") {
        if !first {
            l = append(out, l, b",");
        }
        l = append(out, l, b"\"containers\":[{\"image\":\"");
        l = append(out, l, img);
        l = append(out, l, b"\"}]");
    }
    append(out, l, b"}}")
}

/// A key and its family gone: every `<base>~…` (a Pod's other containers'
/// node specs, `<uid>~c<n>` and `<uid>~i<n>`: _shared/podres.rs) first,
/// then `<base>` itself — each delete-if-present (a record already gone,
/// or never written, is the end reached). One listing of the base's
/// prefix, whatever the Pod's container count. The caller reads
/// `sys.waiting()` after: a page or a write not answered yet holds its
/// serve, which replays these calls. A listing that failed (a page refused
/// for good: `sys.refusal()`, or one that could not be parsed) leaves
/// `<base>` too — removing it with its family unknown would orphan that
/// family — and answers `false`.
unsafe fn drop_family(sys: &Store, base: &[u8]) -> bool {
    if base.is_empty() || base.len() + 1 > MAX_KEY {
        return true;
    }
    let mut pre = [0u8; MAX_KEY];
    let n = append(&mut pre, 0, base);
    let n = append(&mut pre, n, b"~");
    let mut walk = ListWalk::new(pre.get(..n).unwrap_or(&[]));
    let mut k = [0u8; MAX_KEY];
    while let Some(one) = walk.next(sys) {
        let kl = one.len().min(MAX_KEY);
        for (d, &c) in k.iter_mut().zip(one.iter()) {
            *d = c;
        }
        let _ = delete_value(sys, k.get(..kl).unwrap_or(&[]));
    }
    if walk.failed || sys.refusal().is_some() {
        return false;
    }
    if walk.pending || sys.waiting() {
        return true;
    }
    if exists(sys, base) {
        let _ = delete_value(sys, base);
    }
    sys.refusal().is_none()
}

/// A Pod's tomb about to REPLACE another incarnation's (the same name,
/// deleted again within the ttl — a ReplicaSet's `<rs>-<i>` rolled, a
/// StatefulSet's `db-0`): the replaced tomb's end is ended now, as its
/// deadline would have ended it — its node specs
/// `/~node/<node>/pod-specs/<uid>` and every `<uid>~…` of its other
/// containers deleted (tomb.uproc `spec`), so the node drops every record
/// of those (a live sandbox stopped first). The index names one uid; once
/// overwritten, nothing else would ever remove the old one's records.
/// `false` while a read is asked again or the delete is undecided: the
/// serve is held, and served again it replays these calls and goes on to
/// leave the tomb. `false` too when the store refused a read for good: the
/// serve has failed, and leaves no tomb.
unsafe fn end_replaced_pod(sys: &Store, at_key: &[u8], key: &[u8], obj: &[u8]) -> bool {
    if !key.starts_with(b"/pods/") {
        return true;
    }
    let mut old = [0u8; TOMB_AT_MAX];
    let n = match get_value(sys, at_key, &mut old) {
        Some(n) => n,
        None if sys.waiting() || sys.refusal().is_some() => return false,
        None => return true,
    };
    let old = old.get(..n).unwrap_or(&[]);
    let (Some(uid), Some(node)) = (
        j_get3(old, b"obj", b"metadata", b"uid"),
        j_get3(old, b"obj", b"spec", b"nodeName"),
    ) else {
        return true;
    };
    if uid.is_empty() || node.is_empty() || j_get2(obj, b"metadata", b"uid") == Some(uid) {
        return true;
    }
    let mut sk = [0u8; MAX_KEY];
    let mut sl = append(&mut sk, 0, b"/~node/");
    sl = append(&mut sk, sl, node);
    sl = append(&mut sk, sl, b"/pod-specs/");
    if sl + uid.len() > sk.len() {
        return true;
    }
    sl = append(&mut sk, sl, uid);
    let spec = sk.get(..sl).unwrap_or(&[]);
    // Delete-if-present, as tomb.uproc's end does: a spec already gone (or
    // never written: a Pod no node ran) is the end reached. A read asked
    // again holds the serve (the tomb's own writes after it wait with it).
    drop_family(sys, spec);
    !sys.waiting() && sys.refusal().is_none()
}

/// Write the tomb's index entry `<tomb>-at<key>`: `false` when it was
/// refused or could not be built (the caller then leaves no tomb). `grace`:
/// the delete's grace, when the writer knows it ([`tomb_identity`]). A write
/// the store has not decided holds the serve; the tomb's write after it
/// waits with it.
unsafe fn put_tomb_at(
    sys: &Store,
    tomb: &[u8],
    key: &[u8],
    deadline: &[u8],
    ttl: u64,
    obj: &[u8],
    grace: Option<u64>,
) -> bool {
    let mut k = [0u8; MAX_KEY];
    let mut kl = append(&mut k, 0, tomb);
    kl = append(&mut k, kl, b"-at");
    if kl + key.len() > k.len() || deadline.is_empty() {
        return false;
    }
    kl = append(&mut k, kl, key);
    let mut digits = [0u8; 64];
    let dl = mo_dec(ttl, &mut digits, 0);
    let mut v = [0u8; TOMB_AT_MAX];
    let mut l = append(&mut v, 0, b"{\"deadline\":\"");
    l = append(&mut v, l, deadline);
    l = append(&mut v, l, b"\",\"ttl\":");
    l = append(&mut v, l, digits.get(..dl).unwrap_or(&[]));
    l = append(&mut v, l, b",\"obj\":");
    let mut id = [0u8; TOMB_AT_MAX];
    let il = tomb_identity(obj, grace, &mut id);
    if il >= id.len() {
        return false;
    }
    l = append(&mut v, l, id.get(..il).unwrap_or(&[]));
    l = append(&mut v, l, b"}");
    if l >= v.len() {
        return false;
    }
    if !end_replaced_pod(sys, k.get(..kl).unwrap_or(&[]), key, obj) {
        return false;
    }
    !matches!(
        put_value(sys, k.get(..kl).unwrap_or(&[]), v.get(..l).unwrap_or(&[])),
        WriteAnswer::Decided(rc) if rc != 0
    )
}
