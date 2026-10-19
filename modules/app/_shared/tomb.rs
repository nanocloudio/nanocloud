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
// class and first image — at the same paths, so a chain reads
// `obj.spec.nodeName` from the index exactly as it did from the tomb. The
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

/// The object pruned to what the tomb chains read, at the same paths. Each
/// path by its own call (`j_get2`): an array of byte-string literals is a
/// table of unrelocated pointers in a PIC module.
fn tomb_identity(obj: &[u8], out: &mut [u8]) -> usize {
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

/// Write the tomb's index entry `<tomb>-at<key>`: `false` when it was
/// refused or could not be built (the caller then leaves no tomb). A write
/// the store has not decided holds the serve; the tomb's write after it
/// waits with it.
unsafe fn put_tomb_at(
    sys: &Store,
    tomb: &[u8],
    key: &[u8],
    deadline: &[u8],
    ttl: u64,
    obj: &[u8],
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
    let il = tomb_identity(obj, &mut id);
    if il >= id.len() {
        return false;
    }
    l = append(&mut v, l, id.get(..il).unwrap_or(&[]));
    l = append(&mut v, l, b"}");
    if l >= v.len() {
        return false;
    }
    !matches!(
        put_value(sys, k.get(..kl).unwrap_or(&[]), v.get(..l).unwrap_or(&[])),
        WriteAnswer::Decided(rc) if rc != 0
    )
}
