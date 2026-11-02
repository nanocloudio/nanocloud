// http_fetch's reply record: the frame codec it reads a request with, and
// the answer it writes back — the JSON members a request asked of a body,
// within the bound every hop after it carries. Pure (no syscalls), so the
// harness drives the same code the module runs (tests/harness/tests/
// fetch_answer.rs). Needs `jq` (_shared/jsonq.rs) in scope.

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
/// The record's carried context (chronicle `CARRY_FIELD`): one nested
/// message, echoed unchanged beside the legacy 30..=39 range.
const CARRY_FIELD: u8 = 254;
/// The JSON queries a request asks (fields 14..21), answered at 40..47.
const MAX_QUERIES: u8 = 8;

/// The most layers an image the node pulls has: a Fluxor workload image is
/// its graph (the FLXA) and one `.fmod` per module, and one subgraph holds
/// at most 64 modules (Fluxor's `MAX_ADD_MODULES`). A manifest with more is
/// refused by the image plane with its count (image.plan `ans`), never left
/// to fail on a buffer.
const IMAGE_LAYERS_MAX: usize = 65;
/// One answer to one query. Sized for the longest list a pull asks whole:
/// a manifest's layer digests (`layers[].digest!tag`), 64 hex each,
/// `,`-joined — 65 * 65 - 1 = 4224 bytes — with room for an answer a few
/// layers past the bound, so the plane refuses it by its count.
const ANSWER: usize = 4352;
const _: () = assert!(ANSWER >= IMAGE_LAYERS_MAX * 65 - 1);
/// The status of an answer this connector cannot carry whole: an answer
/// past `ANSWER`, or a record past the reply buffer. Never cut, never sent
/// EMPTY as if the member were absent — a chain reading an empty answer as
/// "not asked" would retry it forever.
const ANSWER_TOO_LARGE: u16 = 413;

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        if p + 4 > frame.len() {
            return None;
        }
        let num = frame[p];
        let ty = frame[p + 1];
        let len = u16::from_le_bytes([frame[p + 2], frame[p + 3]]) as usize;
        if p + 4 + len > frame.len() {
            return None;
        }
        if num == number {
            return Some((ty, &frame[p + 4..p + 4 + len]));
        }
        p += 4 + len;
    }
    None
}

fn frame_i64(frame: &[u8], number: u8) -> Option<i64> {
    match frame_field(frame, number) {
        Some((TY_I64, b)) if b.len() == 8 => {
            let mut v = [0u8; 8];
            v.copy_from_slice(b);
            Some(i64::from_le_bytes(v))
        }
        _ => None,
    }
}

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, b)) => b,
        _ => &[],
    }
}

fn put_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    if payload.len() > u16::MAX as usize || at + 4 + payload.len() > out.len() {
        return None;
    }
    out[at] = number;
    out[at + 1] = ty;
    out[at + 2..at + 4].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    out[at + 4..at + 4 + payload.len()].copy_from_slice(payload);
    Some(at + 4 + payload.len())
}

/// What the reply to a request says of its body, beside the answers.
struct FetchOutcome<'a> {
    status: u16,
    received: i64,
    ctype: &'a [u8],
    /// The body's sha256 as 64 hex, for a 200 answered in the reply.
    sha: &'a [u8],
    /// The body answered in the reply (sink 1): its JSON members asked.
    replied: bool,
}

/// The reply record for the request `req` into `out`: correlation, status,
/// bytes received, Content-Type, the body's digest, every answer field
/// (40..47, empty when not asked) and the request's carry. Its length and
/// the status it carries: `o.status`, or `ANSWER_TOO_LARGE` when an asked
/// member's answer is past `ANSWER` (the answers that fit given, the one
/// that does not empty, the digest withheld) or the record is past `out`
/// (no answers at all). None only when even that record does not fit — the
/// request's own carry is past `out`.
fn fetch_reply(out: &mut [u8], req: &[u8], o: &FetchOutcome, body: &[u8]) -> Option<(usize, u16)> {
    let asked = o.status == 200 && o.replied;
    let mut status = o.status;
    if asked {
        let mut probe = [0u8; ANSWER];
        for q in 0..MAX_QUERIES {
            let query = frame_bytes(req, 14 + q);
            if !query.is_empty() && jq(body, query, &mut probe).is_none() {
                status = ANSWER_TOO_LARGE;
            }
        }
    }
    let sha: &[u8] = if status == 200 && o.replied {
        o.sha
    } else {
        &[]
    };
    if let Some(n) = fetch_record(
        out,
        req,
        status,
        o,
        sha,
        if asked { body } else { &[] },
        asked,
    ) {
        return Some((n, status));
    }
    fetch_record(out, req, ANSWER_TOO_LARGE, o, &[], &[], false).map(|n| (n, ANSWER_TOO_LARGE))
}

fn fetch_record(
    out: &mut [u8],
    req: &[u8],
    status: u16,
    o: &FetchOutcome,
    sha: &[u8],
    body: &[u8],
    answers: bool,
) -> Option<usize> {
    let mut p = 1usize;
    let mut n = 0u8;
    let cid = frame_i64(req, 1).unwrap_or(0);
    p = put_field(out, p, 1, TY_I64, &cid.to_le_bytes())?;
    p = put_field(out, p, 2, TY_I64, &(status as i64).to_le_bytes())?;
    p = put_field(out, p, 3, TY_I64, &o.received.to_le_bytes())?;
    p = put_field(out, p, 4, TY_BYTES, o.ctype)?;
    p = put_field(out, p, 5, TY_BYTES, sha)?;
    n += 5;
    // Every answer field, always — empty when not asked or not answered: a
    // decision reading a field the record lacks produces nothing at all.
    for q in 0..MAX_QUERIES {
        let query = frame_bytes(req, 14 + q);
        let mut ans = [0u8; ANSWER];
        let len = if answers && !query.is_empty() {
            jq(body, query, &mut ans).unwrap_or(0)
        } else {
            0
        };
        p = put_field(out, p, 40 + q, TY_BYTES, &ans[..len])?;
        n += 1;
    }
    for f in (CARRY_LO..=CARRY_HI).chain(core::iter::once(CARRY_FIELD)) {
        if let Some((ty, v)) = frame_field(req, f) {
            p = put_field(out, p, f, ty, v)?;
            n += 1;
        }
    }
    out[0] = n;
    Some(p)
}
