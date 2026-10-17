// A Kubernetes protobuf request body, as the JSON the API reads.
//
// kubectl's typed generators (`kubectl create configmap|secret|deployment|
// …`, `kubectl create token`) send `application/vnd.kubernetes.protobuf`:
// the magic `k8s\0`, then a `runtime.Unknown` — `typeMeta {apiVersion=1,
// kind=2}`, `raw` (the object's own message), `contentEncoding`,
// `contentType`. The object is decoded by the field schema
// `k8s_proto_schema.rs` holds (generated from Kubernetes' generated.proto by
// scripts/support/k8s_proto_schema.py) and written as the JSON the same
// object has: field names are the proto names, which Kubernetes keeps equal
// to the JSON keys; maps are objects, bytes base64; and the types whose JSON
// is not their message shape are written as Kubernetes writes them (Time,
// MicroTime, Duration, Quantity, IntOrString, RawExtension/FieldsV1, and
// the `items` string-list wrappers).
//
// Fields are written in schema order. A field the schema does not know (a
// newer client's) is skipped: the API would refuse an unknown JSON field
// under strict validation anyway, and nothing here can name it.
//
// No allocation and no panic path: the includer supplies a sink, every
// index is checked, and nesting is bounded.
//
// Mounted by `http_chain` (the API's ingress) and by tests/harness.

/// Where the JSON goes. False when it cannot take more (the body is refused).
trait PbSink {
    fn put(&mut self, b: &[u8]) -> bool;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PbErr {
    /// Not a Kubernetes protobuf body (no `k8s\0` magic).
    NotProto,
    /// A kind the schema does not carry.
    UnknownKind,
    /// The wire bytes do not decode as the schema says.
    Malformed,
    /// An encoded `raw` (contentEncoding) or nesting past the bound.
    Unsupported,
    /// The sink is full.
    Full,
}

const PB_MAGIC: &[u8; 4] = b"k8s\0";
const PB_MAX_DEPTH: u32 = 32;

const PB_K_STRING: u8 = 1;
const PB_K_INT32: u8 = 2;
const PB_K_INT64: u8 = 3;
const PB_K_BOOL: u8 = 4;
const PB_K_BYTES: u8 = 5;
const PB_K_MESSAGE: u8 = 6;
const PB_K_MAP: u8 = 7;

const PB_S_TIME: u16 = 1;
const PB_S_MICROTIME: u16 = 2;
const PB_S_DURATION: u16 = 3;
const PB_S_QUANTITY: u16 = 4;
const PB_S_INTORSTR: u16 = 5;
const PB_S_RAWJSON: u16 = 6;
const PB_S_STRLIST: u16 = 7;

// ── The schema ───────────────────────────────────────────────────────

fn sch_u16(at: usize) -> Option<u16> {
    let b = K8S_PB_SCHEMA.get(at..at + 2)?;
    Some(u16::from_le_bytes([b[0], b[1]]))
}

fn sch_u32(at: usize) -> Option<u32> {
    let b = K8S_PB_SCHEMA.get(at..at + 4)?;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn sch_str(off: u16, len: u8) -> &'static [u8] {
    let base = sch_u32(16).unwrap_or(0) as usize + off as usize;
    K8S_PB_SCHEMA.get(base..base + len as usize).unwrap_or(&[])
}

/// One field of a message's schema.
#[derive(Clone, Copy)]
struct PbField {
    num: u16,
    kind: u8,
    repeated: bool,
    /// A non-pointer `omitempty` field: gogo-protobuf writes it even at its
    /// zero value, where the object's JSON has nothing.
    drop_zero: bool,
    /// An embedded struct (`json:",inline"`): its members are written into
    /// the enclosing object.
    inline: bool,
    vkind: u8,
    reference: u16,
    name: &'static [u8],
}

/// Message `m`: `(special, field count, first field's byte offset)`.
fn sch_msg(m: u16) -> Option<(u16, u16, usize)> {
    let n_msgs = sch_u16(4)?;
    if m >= n_msgs {
        return None;
    }
    let at = sch_u32(8)? as usize + m as usize * 8;
    let fields_base = sch_u32(8)? as usize + n_msgs as usize * 8;
    Some((
        sch_u16(at)?,
        sch_u16(at + 2)?,
        fields_base + sch_u32(at + 4)? as usize,
    ))
}

fn sch_field(first: usize, i: u16) -> Option<PbField> {
    let at = first + i as usize * 10;
    let b = K8S_PB_SCHEMA.get(at..at + 10)?;
    Some(PbField {
        num: u16::from_le_bytes([b[0], b[1]]),
        kind: b[2],
        repeated: b[3] & 1 != 0,
        drop_zero: b[3] & 2 != 0,
        inline: b[3] & 4 != 0,
        vkind: b[4],
        reference: u16::from_le_bytes([b[6], b[7]]),
        name: sch_str(u16::from_le_bytes([b[8], b[9]]), b[5]),
    })
}

/// The message for `(apiVersion, kind)`; options a typed client sends under
/// any group (`DeleteOptions`) by kind alone.
fn sch_kind(api_version: &[u8], kind: &[u8]) -> Option<u16> {
    let n = sch_u16(6)?;
    let base = sch_u32(12)? as usize;
    let mut by_kind = None;
    for i in 0..n as usize {
        let at = base + i * 8;
        let b = K8S_PB_SCHEMA.get(at..at + 8)?;
        let gv = sch_str(u16::from_le_bytes([b[0], b[1]]), b[2]);
        let k = sch_str(u16::from_le_bytes([b[4], b[5]]), b[3]);
        let m = u16::from_le_bytes([b[6], b[7]]);
        if k == kind {
            if gv == api_version {
                return Some(m);
            }
            if gv == b"meta.k8s.io/v1" {
                by_kind = Some(m);
            }
        }
    }
    by_kind
}

// ── The wire ─────────────────────────────────────────────────────────

/// One field on the wire: its number, wire type and value (a varint's
/// value, or the bytes of a length-delimited field).
#[derive(Clone, Copy)]
struct PbWire<'a> {
    num: u32,
    wt: u8,
    varint: u64,
    bytes: &'a [u8],
}

fn pb_varint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let c = *b.get(*at)?;
        *at += 1;
        if shift >= 64 {
            return None;
        }
        v |= u64::from(c & 0x7F) << shift;
        if c & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
    }
}

/// The next field of `b` from `at`; `None` at the end, `Some(Err)` when
/// the bytes do not decode.
fn pb_next<'a>(b: &'a [u8], at: &mut usize) -> Option<Result<PbWire<'a>, PbErr>> {
    if *at >= b.len() {
        return None;
    }
    let Some(key) = pb_varint(b, at) else {
        return Some(Err(PbErr::Malformed));
    };
    let wt = (key & 7) as u8;
    let num = (key >> 3) as u32;
    let mut w = PbWire {
        num,
        wt,
        varint: 0,
        bytes: &[],
    };
    match wt {
        0 => match pb_varint(b, at) {
            Some(v) => w.varint = v,
            None => return Some(Err(PbErr::Malformed)),
        },
        1 | 5 => {
            let n = if wt == 1 { 8 } else { 4 };
            let Some(s) = b.get(*at..*at + n) else {
                return Some(Err(PbErr::Malformed));
            };
            let mut v = 0u64;
            for (i, &c) in s.iter().enumerate() {
                v |= u64::from(c) << (8 * i);
            }
            w.varint = v;
            *at += n;
        }
        2 => {
            let Some(len) = pb_varint(b, at) else {
                return Some(Err(PbErr::Malformed));
            };
            let end = (*at).checked_add(len as usize);
            let Some(s) = end.and_then(|e| b.get(*at..e)) else {
                return Some(Err(PbErr::Malformed));
            };
            w.bytes = s;
            *at += len as usize;
        }
        _ => return Some(Err(PbErr::Malformed)),
    }
    Some(Ok(w))
}

/// The last occurrence of field `num` in `b` (proto: the last one wins).
fn pb_last<'a>(b: &'a [u8], num: u32) -> Result<Option<PbWire<'a>>, PbErr> {
    let mut at = 0usize;
    let mut found = None;
    while let Some(w) = pb_next(b, &mut at) {
        let w = w?;
        if w.num == num {
            found = Some(w);
        }
    }
    Ok(found)
}

// ── JSON ─────────────────────────────────────────────────────────────

fn j_put<S: PbSink>(out: &mut S, b: &[u8]) -> Result<(), PbErr> {
    if out.put(b) {
        Ok(())
    } else {
        Err(PbErr::Full)
    }
}

fn j_string<S: PbSink>(out: &mut S, s: &[u8]) -> Result<(), PbErr> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    j_put(out, b"\"")?;
    let mut run = 0usize;
    for (i, &c) in s.iter().enumerate() {
        let esc: Option<[u8; 6]> = match c {
            b'"' | b'\\' => Some([b'\\', c, 0, 0, 0, 0]),
            b'\n' => Some([b'\\', b'n', 0, 0, 0, 0]),
            b'\r' => Some([b'\\', b'r', 0, 0, 0, 0]),
            b'\t' => Some([b'\\', b't', 0, 0, 0, 0]),
            0..=0x1F => Some([
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(c >> 4) as usize],
                HEX[(c & 15) as usize],
            ]),
            _ => None,
        };
        if let Some(e) = esc {
            j_put(out, s.get(run..i).unwrap_or(&[]))?;
            let n = if e[2] == 0 { 2 } else { 6 };
            j_put(out, e.get(..n).unwrap_or(&[]))?;
            run = i + 1;
        }
    }
    j_put(out, s.get(run..).unwrap_or(&[]))?;
    j_put(out, b"\"")
}

fn j_i64<S: PbSink>(out: &mut S, v: i64) -> Result<(), PbErr> {
    let mut buf = [0u8; 20];
    let mut at = buf.len();
    let neg = v < 0;
    let mut u = v.unsigned_abs();
    loop {
        at -= 1;
        buf[at] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    if neg {
        j_put(out, b"-")?;
    }
    j_put(out, buf.get(at..).unwrap_or(&[]))
}

fn j_base64<S: PbSink>(out: &mut S, b: &[u8]) -> Result<(), PbErr> {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    j_put(out, b"\"")?;
    for chunk in b.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let q = [
            T[(b0 >> 2) as usize],
            T[(((b0 & 3) << 4) | (b1 >> 4)) as usize],
            if chunk.len() > 1 {
                T[(((b1 & 15) << 2) | (b2 >> 6)) as usize]
            } else {
                b'='
            },
            if chunk.len() > 2 {
                T[(b2 & 63) as usize]
            } else {
                b'='
            },
        ];
        j_put(out, &q)?;
    }
    j_put(out, b"\"")
}

/// `seconds` since the epoch as RFC 3339 UTC, with `micros` six fractional
/// digits when given (metav1.Time / MicroTime).
fn j_time<S: PbSink>(out: &mut S, seconds: i64, micros: Option<u32>) -> Result<(), PbErr> {
    let days = seconds.div_euclid(86_400);
    let secs = seconds.rem_euclid(86_400);
    // Civil from days (H. Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let mut b = [0u8; 27];
    let mut put2 = |at: usize, v: i64| {
        b[at] = b'0' + (v / 10 % 10) as u8;
        b[at + 1] = b'0' + (v % 10) as u8;
    };
    put2(0, y / 100);
    put2(2, y % 100);
    put2(5, m);
    put2(8, d);
    put2(11, secs / 3600);
    put2(14, secs / 60 % 60);
    put2(17, secs % 60);
    b[4] = b'-';
    b[7] = b'-';
    b[10] = b'T';
    b[13] = b':';
    b[16] = b':';
    let mut n = 19;
    if let Some(us) = micros {
        b[19] = b'.';
        let mut v = us;
        for i in (20..26).rev() {
            b[i] = b'0' + (v % 10) as u8;
            v /= 10;
        }
        n = 26;
    }
    b[n] = b'Z';
    j_put(out, b"\"")?;
    j_put(out, b.get(..n + 1).unwrap_or(&[]))?;
    j_put(out, b"\"")
}

/// A duration in nanoseconds as Go's `time.Duration.String` writes it
/// (metav1.Duration's JSON).
fn j_duration<S: PbSink>(out: &mut S, d: i64) -> Result<(), PbErr> {
    let mut buf = [0u8; 32];
    let mut w = buf.len();
    let neg = d < 0;
    let mut u = d.unsigned_abs();
    fn frac(buf: &mut [u8; 32], w: &mut usize, v: u64, prec: usize) -> u64 {
        let mut v = v;
        let mut print = false;
        for _ in 0..prec {
            let digit = v % 10;
            print = print || digit != 0;
            if print {
                *w -= 1;
                buf[*w] = b'0' + digit as u8;
            }
            v /= 10;
        }
        if print {
            *w -= 1;
            buf[*w] = b'.';
        }
        v
    }
    fn int(buf: &mut [u8; 32], w: &mut usize, v: u64) {
        let mut v = v;
        if v == 0 {
            *w -= 1;
            buf[*w] = b'0';
            return;
        }
        while v > 0 {
            *w -= 1;
            buf[*w] = b'0' + (v % 10) as u8;
            v /= 10;
        }
    }
    if u < 1_000_000_000 {
        if u == 0 {
            return j_put(out, b"\"0s\"");
        }
        w -= 1;
        buf[w] = b's';
        let prec = if u < 1_000 {
            w -= 1;
            buf[w] = b'n';
            0
        } else if u < 1_000_000 {
            w -= 2;
            buf[w] = 0xC2; // µ
            buf[w + 1] = 0xB5;
            3
        } else {
            w -= 1;
            buf[w] = b'm';
            6
        };
        u = frac(&mut buf, &mut w, u, prec);
        int(&mut buf, &mut w, u);
    } else {
        w -= 1;
        buf[w] = b's';
        u = frac(&mut buf, &mut w, u, 9);
        int(&mut buf, &mut w, u % 60);
        u /= 60;
        if u > 0 {
            w -= 1;
            buf[w] = b'm';
            int(&mut buf, &mut w, u % 60);
            u /= 60;
            if u > 0 {
                w -= 1;
                buf[w] = b'h';
                int(&mut buf, &mut w, u);
            }
        }
    }
    if neg {
        w -= 1;
        buf[w] = b'-';
    }
    j_put(out, b"\"")?;
    j_put(out, buf.get(w..).unwrap_or(&[]))?;
    j_put(out, b"\"")
}

// ── Messages ─────────────────────────────────────────────────────────

/// A message's own JSON: its special form, or an object of its fields.
fn pb_message<S: PbSink>(out: &mut S, b: &[u8], m: u16, depth: u32) -> Result<(), PbErr> {
    if depth > PB_MAX_DEPTH {
        return Err(PbErr::Unsupported);
    }
    let (special, _, _) = sch_msg(m).ok_or(PbErr::Malformed)?;
    match special {
        PB_S_TIME | PB_S_MICROTIME => {
            let secs = pb_last(b, 1)?.map(|w| w.varint as i64);
            let nanos = pb_last(b, 2)?.map(|w| w.varint as i64 as i32).unwrap_or(0);
            match secs {
                None if nanos == 0 => j_put(out, b"null"),
                s => j_time(
                    out,
                    s.unwrap_or(0),
                    (special == PB_S_MICROTIME).then_some((nanos.max(0) as u32) / 1000),
                ),
            }
        }
        PB_S_DURATION => j_duration(out, pb_last(b, 1)?.map(|w| w.varint as i64).unwrap_or(0)),
        PB_S_QUANTITY => j_string(out, pb_last(b, 1)?.map(|w| w.bytes).unwrap_or(&[])),
        PB_S_INTORSTR => {
            let ty = pb_last(b, 1)?.map(|w| w.varint).unwrap_or(0);
            if ty == 1 {
                j_string(out, pb_last(b, 3)?.map(|w| w.bytes).unwrap_or(&[]))
            } else {
                j_i64(
                    out,
                    pb_last(b, 2)?
                        .map(|w| w.varint as i64 as i32)
                        .unwrap_or(0)
                        .into(),
                )
            }
        }
        PB_S_RAWJSON => match pb_last(b, 1)? {
            Some(w) if !w.bytes.is_empty() => j_put(out, w.bytes),
            _ => j_put(out, b"null"),
        },
        PB_S_STRLIST => {
            j_put(out, b"[")?;
            let mut at = 0usize;
            let mut first = true;
            while let Some(w) = pb_next(b, &mut at) {
                let w = w?;
                if w.num == 1 && w.wt == 2 {
                    if !first {
                        j_put(out, b",")?;
                    }
                    first = false;
                    j_string(out, w.bytes)?;
                }
            }
            j_put(out, b"]")
        }
        _ => {
            j_put(out, b"{")?;
            pb_fields(out, b, m, depth, true)?;
            j_put(out, b"}")
        }
    }
}

/// Message `m`'s fields present in `b`, as `"name":value` members in schema
/// order, each led by a comma unless `first` still holds. Whether `first`
/// still holds after (nothing was written).
fn pb_fields<S: PbSink>(
    out: &mut S,
    b: &[u8],
    m: u16,
    depth: u32,
    first: bool,
) -> Result<bool, PbErr> {
    if depth > PB_MAX_DEPTH {
        return Err(PbErr::Unsupported);
    }
    let (_, n, base) = sch_msg(m).ok_or(PbErr::Malformed)?;
    let mut first = first;
    for i in 0..n {
        let f = sch_field(base, i).ok_or(PbErr::Malformed)?;
        let num = u32::from(f.num);
        if f.kind == PB_K_MAP || f.repeated {
            // Every occurrence, in order: elements of a repeated field (a
            // packed scalar run expands), entries of a map.
            let mut at = 0usize;
            let mut any = false;
            while let Some(w) = pb_next(b, &mut at) {
                let w = w?;
                if w.num != num {
                    continue;
                }
                let packed = w.wt == 2 && matches!(f.kind, PB_K_INT32 | PB_K_INT64 | PB_K_BOOL);
                if !any {
                    if !first {
                        j_put(out, b",")?;
                    }
                    first = false;
                    j_string(out, f.name)?;
                    j_put(out, if f.kind == PB_K_MAP { b":{" } else { b":[" })?;
                } else if !packed {
                    j_put(out, b",")?;
                }
                if f.kind == PB_K_MAP {
                    pb_map_entry(out, w.bytes, f, depth)?;
                } else if packed {
                    let mut p = 0usize;
                    let mut lead = !any;
                    while p < w.bytes.len() {
                        let v = pb_varint(w.bytes, &mut p).ok_or(PbErr::Malformed)?;
                        if !lead {
                            j_put(out, b",")?;
                        }
                        lead = false;
                        pb_scalar(
                            out,
                            f.kind,
                            PbWire {
                                num,
                                wt: 0,
                                varint: v,
                                bytes: &[],
                            },
                        )?;
                    }
                } else {
                    pb_value(out, f.kind, f.reference, w, depth)?;
                }
                any = true;
            }
            if any {
                j_put(out, if f.kind == PB_K_MAP { b"}" } else { b"]" })?;
            }
            continue;
        }
        if let Some(w) = pb_last(b, num)? {
            if f.inline && f.kind == PB_K_MESSAGE && w.wt == 2 {
                first = pb_fields(out, w.bytes, f.reference, depth + 1, first)?;
                continue;
            }
            let zero = match f.kind {
                PB_K_STRING | PB_K_BYTES => w.bytes.is_empty(),
                PB_K_INT32 | PB_K_INT64 | PB_K_BOOL => w.varint == 0,
                _ => false,
            };
            if f.drop_zero && zero {
                continue;
            }
            if !first {
                j_put(out, b",")?;
            }
            first = false;
            j_string(out, f.name)?;
            j_put(out, b":")?;
            pb_value(out, f.kind, f.reference, w, depth)?;
        }
    }
    Ok(first)
}

/// One map entry (`key` = 1, `value` = 2) as `"key":value`.
fn pb_map_entry<S: PbSink>(out: &mut S, b: &[u8], f: PbField, depth: u32) -> Result<(), PbErr> {
    let key = pb_last(b, 1)?.map(|w| w.bytes).unwrap_or(&[]);
    j_string(out, key)?;
    j_put(out, b":")?;
    match pb_last(b, 2)? {
        Some(w) => pb_value(out, f.vkind, f.reference, w, depth),
        // An absent value is the type's zero.
        None => match f.vkind {
            PB_K_STRING => j_put(out, b"\"\""),
            PB_K_BYTES => j_put(out, b"\"\""),
            PB_K_BOOL => j_put(out, b"false"),
            PB_K_INT32 | PB_K_INT64 => j_put(out, b"0"),
            _ => pb_message(out, &[], f.reference, depth + 1),
        },
    }
}

fn pb_value<S: PbSink>(
    out: &mut S,
    kind: u8,
    reference: u16,
    w: PbWire<'_>,
    depth: u32,
) -> Result<(), PbErr> {
    match kind {
        PB_K_MESSAGE => {
            if w.wt != 2 {
                return Err(PbErr::Malformed);
            }
            pb_message(out, w.bytes, reference, depth + 1)
        }
        _ => pb_scalar(out, kind, w),
    }
}

fn pb_scalar<S: PbSink>(out: &mut S, kind: u8, w: PbWire<'_>) -> Result<(), PbErr> {
    match kind {
        PB_K_STRING => {
            if w.wt != 2 {
                return Err(PbErr::Malformed);
            }
            j_string(out, w.bytes)
        }
        PB_K_BYTES => {
            if w.wt != 2 {
                return Err(PbErr::Malformed);
            }
            j_base64(out, w.bytes)
        }
        PB_K_INT32 => j_i64(out, i64::from(w.varint as i64 as i32)),
        PB_K_INT64 => j_i64(out, w.varint as i64),
        PB_K_BOOL => j_put(out, if w.varint != 0 { b"true" } else { b"false" }),
        _ => Err(PbErr::Malformed),
    }
}

/// `body` (a Kubernetes protobuf body) as the JSON object it stands for.
fn pb_to_json<S: PbSink>(body: &[u8], out: &mut S) -> Result<(), PbErr> {
    if body.get(..4) != Some(&PB_MAGIC[..]) {
        return Err(PbErr::NotProto);
    }
    let unknown = body.get(4..).unwrap_or(&[]);
    let type_meta = pb_last(unknown, 1)?.map(|w| w.bytes).unwrap_or(&[]);
    let api_version = pb_last(type_meta, 1)?.map(|w| w.bytes).unwrap_or(&[]);
    let kind = pb_last(type_meta, 2)?.map(|w| w.bytes).unwrap_or(&[]);
    if pb_last(unknown, 3)?.is_some_and(|w| !w.bytes.is_empty()) {
        return Err(PbErr::Unsupported);
    }
    let raw = pb_last(unknown, 2)?.map(|w| w.bytes).unwrap_or(&[]);
    let m = sch_kind(api_version, kind).ok_or(PbErr::UnknownKind)?;
    j_put(out, b"{\"kind\":")?;
    j_string(out, kind)?;
    j_put(out, b",\"apiVersion\":")?;
    j_string(out, api_version)?;
    pb_fields(out, raw, m, 0, false)?;
    j_put(out, b"}")
}
