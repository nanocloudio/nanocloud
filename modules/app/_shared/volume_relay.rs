// The encrypted-volume pipelines' relay between the control plane and a
// node, as the store carries it (`volume_relay`): the pure half — which ops
// it carries, where a request goes, how a request and its answer sit in a
// record, and how a served request's correlation id is kept apart from the
// node's own. Mounted by `volume_relay/mod.rs` and by `tests/harness`.
//
// A request is the effect record, whole, as hex: `req=<hex>`; its answer is
// the effector's outcome record, whole, as hex, appended: `;out=<hex>`.

/// The most ops a relay carries are numbered below this.
const RL_OPS: usize = 128;

/// The ops a relay carries, from `"10,11,22"`. `None` for anything else.
fn rl_ops(list: &[u8]) -> Option<[bool; RL_OPS]> {
    let mut set = [false; RL_OPS];
    for part in list.split(|&b| b == b',') {
        if part.is_empty() || part.len() > 3 {
            return None;
        }
        let mut n = 0usize;
        for &b in part {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n * 10 + (b - b'0') as usize;
        }
        *set.get_mut(n)? = true;
    }
    Some(set)
}

fn rl_carries(set: &[bool; RL_OPS], op: i64) -> bool {
    usize::try_from(op)
        .ok()
        .and_then(|i| set.get(i).copied())
        .unwrap_or(false)
}

const RL_HEX: &[u8; 16] = b"0123456789abcdef";

/// `src` as lowercase hex into `out`: its length, or `None` when it does
/// not fit.
fn rl_hex(src: &[u8], out: &mut [u8]) -> Option<usize> {
    let n = src.len() * 2;
    if n > out.len() {
        return None;
    }
    for (i, &b) in src.iter().enumerate() {
        out[2 * i] = RL_HEX[(b >> 4) as usize];
        out[2 * i + 1] = RL_HEX[(b & 15) as usize];
    }
    Some(n)
}

fn rl_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// Hex `src` back into bytes in `out`: their length.
fn rl_unhex(src: &[u8], out: &mut [u8]) -> Option<usize> {
    if !src.len().is_multiple_of(2) || src.len() / 2 > out.len() {
        return None;
    }
    for i in 0..src.len() / 2 {
        out[i] = (rl_nibble(src[2 * i])? << 4) | rl_nibble(src[2 * i + 1])?;
    }
    Some(src.len() / 2)
}

/// A request record's value: `req=<hex>` (and, answered, `;out=<hex>`).
fn rl_request(rec: &[u8], out: &mut [u8]) -> Option<usize> {
    let head = b"req=";
    if out.len() < head.len() {
        return None;
    }
    out[..head.len()].copy_from_slice(head);
    Some(head.len() + rl_hex(rec, &mut out[head.len()..])?)
}

/// `value` with the answer `outcome` appended: its length in `out`.
fn rl_answered(value: &[u8], outcome: &[u8], out: &mut [u8]) -> Option<usize> {
    let mid = b";out=";
    let at = value.len() + mid.len();
    if at > out.len() {
        return None;
    }
    out[..value.len()].copy_from_slice(value);
    out[value.len()..at].copy_from_slice(mid);
    Some(at + rl_hex(outcome, &mut out[at..])?)
}

/// A request record's parts: the request's hex, and its answer's once it
/// has one.
fn rl_parts(value: &[u8]) -> Option<(&[u8], Option<&[u8]>)> {
    let rest = value.strip_prefix(b"req=")?;
    match rest.iter().position(|&b| b == b';') {
        None => Some((rest, None)),
        Some(i) => Some((&rest[..i], Some(rest[i + 1..].strip_prefix(b"out=")?))),
    }
}

// ---- record frames: `[count][number type len:u16 value]…` ----

const RL_TY_BYTES: u8 = 0;
const RL_TY_I64: u8 = 1;
/// A nested record: the carry.
const RL_TY_MSG: u8 = 3;

fn rl_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let num = *frame.get(p)?;
        let ty = *frame.get(p + 1)?;
        let len = u16::from_le_bytes([*frame.get(p + 2)?, *frame.get(p + 3)?]) as usize;
        let v = frame.get(p + 4..p + 4 + len)?;
        if num == number {
            return Some((ty, v));
        }
        p += 4 + len;
    }
    None
}

fn rl_i64(frame: &[u8], number: u8) -> i64 {
    match rl_field(frame, number) {
        Some((RL_TY_I64, b)) if b.len() == 8 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(b);
            i64::from_le_bytes(a)
        }
        _ => 0,
    }
}

fn rl_bytes(frame: &[u8], number: u8) -> &[u8] {
    match rl_field(frame, number) {
        Some((RL_TY_BYTES, b)) => b,
        _ => &[],
    }
}

/// `rec` with its correlation id (field 1) replaced by `cid`, into `out`.
fn rl_with_cid(rec: &[u8], cid: i64, out: &mut [u8]) -> Option<usize> {
    let count = *rec.first()? as usize;
    if out.is_empty() {
        return None;
    }
    out[0] = rec[0];
    let (mut p, mut w) = (1usize, 1usize);
    for _ in 0..count {
        let num = *rec.get(p)?;
        let ty = *rec.get(p + 1)?;
        let len = u16::from_le_bytes([*rec.get(p + 2)?, *rec.get(p + 3)?]) as usize;
        let v = rec.get(p + 4..p + 4 + len)?;
        let cid_bytes = cid.to_le_bytes();
        let v: &[u8] = if num == 1 && ty == RL_TY_I64 {
            &cid_bytes
        } else {
            v
        };
        let end = w + 4 + v.len();
        if end > out.len() {
            return None;
        }
        out[w] = num;
        out[w + 1] = ty;
        out[w + 2..w + 4].copy_from_slice(&(v.len() as u16).to_le_bytes());
        out[w + 4..end].copy_from_slice(v);
        p += 4 + len;
        w = end;
    }
    Some(w)
}

/// The carry's field holding the node an effect names (`volume_effect.uproc`
/// `Ctx.device`).
const RL_CARRY_DEVICE: u8 = 36;
const RL_CARRY: u8 = 254;

/// The node an effect record names itself: the device argument of the ops
/// that carry one (GRAPH_START 10, KAGI_RELEASE 22, KAGI_RENEW 23,
/// KAGI_RECOVER 79: a3), else the device its carry names (a recovery's,
/// a snapshot's flush).
/// `None`: the claim's attachment names it.
fn rl_named_device(rec: &[u8]) -> Option<&[u8]> {
    let a3 = rl_bytes(rec, 6);
    if matches!(rl_i64(rec, 2), 10 | 22 | 23 | 79) && !a3.is_empty() {
        return Some(a3);
    }
    let carry = match rl_field(rec, RL_CARRY) {
        Some((RL_TY_MSG, c)) => c,
        _ => &[],
    };
    let d = rl_bytes(carry, RL_CARRY_DEVICE);
    (!d.is_empty()).then_some(d)
}

/// A node name a key can hold: no `/`, no `;`.
fn rl_node_ok(n: &[u8]) -> bool {
    !n.is_empty() && n.len() <= 63 && !n.iter().any(|&b| b == b'/' || b == b';' || b < 0x21)
}

/// `device=<v>` out of a flat record.
fn rl_flat<'a>(v: &'a [u8], k: &[u8]) -> Option<&'a [u8]> {
    for part in v.split(|&b| b == b';') {
        let Some(eq) = part.iter().position(|&b| b == b'=') else {
            continue;
        };
        if &part[..eq] == k {
            return Some(&part[eq + 1..]);
        }
    }
    None
}
