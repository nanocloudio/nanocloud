//! The encrypted-volume pipelines' relay between the control plane and a
//! node, carried by the replicated store (`docs/design/encrypted-volumes.md`,
//! multi-node placement). See `manifest.toml` for the wire.
//!
//! As an effector it takes the effects its graph's chains issue that only
//! the other side can answer, writes each into the store addressed to that
//! side, and answers the chain with the outcome the other side writes back.
//! As a server it runs the requests addressed to its own side through its
//! graph's effectors and writes their outcomes back. Records are the
//! pipelines' own, whole (`_shared/volume_relay.rs`).
#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_HEAD: u32 = 0x1422;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;

include!("../_shared/store.rs");
include!("../_shared/volume_relay.rs");

const CHAN_EINVAL: i32 = -22;
const REC: usize = 4096;
/// A request record's value: `req=` and the effect, then `;out=` and the
/// outcome, both as hex.
const VALUE: usize = 4 * REC + 16;

/// Requests in flight each way.
const SLOTS: usize = 8;

/// Where requests go: a node's are in its scope (its store names them
/// `/volume-ops/`), the control plane's under `/volume-asks/`.
const TO_NODE: &[u8] = b"/~node/";
const NODE_OPS: &[u8] = b"/volume-ops/";
const ASKS: &[u8] = b"/volume-asks/";
const ATTACHMENTS: &[u8] = b"/volume-attachments/";

/// `side`: the control plane relays to the node holding a claim and serves
/// the nodes' asks; a node relays to the control plane and serves its own
/// requests.
const SIDE_PLANE: u8 = 0;
const SIDE_NODE: u8 = 1;

/// A served request's correlation id on its way through this graph's
/// effectors: apart from any its chains use.
const SERVED_CID: i64 = 0x5652_0000_0000_0000;

/// How often the counters are logged, and how soon after they change.
const HB_MS: u64 = 10_000;
const HB_CHANGED_MS: u64 = 1_000;

const PHASE_FREE: u8 = 0;
/// Issued: the request is being written.
const PHASE_WRITE: u8 = 1;
/// Issued: written, its answer awaited.
const PHASE_WAIT: u8 = 2;
/// Issued: answered (or given up); the request is being withdrawn.
const PHASE_WITHDRAW: u8 = 3;
/// Served: sent to the effectors, its outcome awaited.
const PHASE_SENT: u8 = 4;
/// Served: the outcome is to be written back, over the request as it stands.
const PHASE_ANSWER: u8 = 5;
/// Served: the outcome's write is in flight, conditioned on `rev`; asked
/// again as it was until the store decides it.
const PHASE_ANSWERING: u8 = 6;

#[derive(Clone, Copy)]
struct Slot {
    phase: u8,
    key: [u8; MAX_KEY],
    key_len: u8,
    since_ms: u64,
    /// Served: the request's own correlation id (restored on its outcome).
    cid: i64,
    /// The record's value: what is being written, or what was read.
    value_len: u16,
    /// Served: the request's revision its answer is conditioned on.
    rev: u64,
}

const SLOT_FREE: Slot = Slot {
    phase: PHASE_FREE,
    key: [0; MAX_KEY],
    key_len: 0,
    since_ms: 0,
    cid: 0,
    value_len: 0,
    rev: 0,
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    answer_in: i32,
    sink: i32,
    response_out: i32,
    effect_out: i32,
    side: u8,
    ops: [bool; RL_OPS],
    ops_text: [u8; 256],
    ops_len: u16,
    poll_ms: u32,
    timeout_ms: u32,
    started: u8,
    refused: u8,
    nonce: [u8; 8],
    seq: u32,
    last_sweep_ms: u64,
    woken: u8,
    issued: [Slot; SLOTS],
    issued_values: [[u8; VALUE]; SLOTS],
    served: [Slot; SLOTS],
    served_values: [[u8; VALUE]; SLOTS],
    /// The outcome on its way to the chains, or the request on its way to
    /// the effectors.
    reply: [u8; REC],
    reply_len: u16,
    effect: [u8; REC],
    effect_len: u16,
    /// An effect taken from the chains whose address read was not answered
    /// yet (`EAGAIN`): addressed again next step, before any new one.
    held: [u8; REC],
    held_len: u16,
    /// Scratch, kept off the stack.
    rec: [u8; REC],
    rec2: [u8; REC],
    buf: [u8; VALUE],
    att: [u8; MAX_VALUE],
    relayed: u32,
    answered: u32,
    served_n: u32,
    given_up: u32,
    dropped: u32,
    last_hb_ms: u64,
    /// The counters' sum at the last heartbeat.
    last_hb_sum: u32,
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

define_params! {
    State;

    // Comma-separated: the ops this graph's chains issue that only the
    // other side answers.
    1, relay_ops, str, 0
        => |s, d, len| {
            if len <= s.ops_text.len() {
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                s.ops_text[..len].copy_from_slice(src);
                s.ops_len = len as u16;
            }
        };
    2, side, u8, 0, enum { plane=0, node=1 }
        => |s, d, len| { s.side = p_u8(d, len, 0, 0); };
    // How often waiting requests are read again (a change wakes them
    // sooner).
    3, poll_ms, u32, 200
        => |s, d, len| { s.poll_ms = p_u32(d, len, 0, 200); };
    // An issued request unanswered this long is withdrawn: the chain's own
    // timeout redrives it.
    4, timeout_ms, u32, 60000
        => |s, d, len| { s.timeout_ms = p_u32(d, len, 0, 60_000); };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

/// # Safety
/// Kernel module-ABI entry point: `state`/`syscalls` are the loader-owned
/// instance arena and syscall table.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if syscalls.is_null() || state.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<State>() {
        return -2;
    }
    let s = &mut *(state as *mut State);
    let sys = &*(syscalls as *const SyscallTable);
    s.syscalls = sys;
    s.request_in = in_chan;
    s.answer_in = dev_channel_port(sys, 0, 1);
    s.sink = dev_channel_port(sys, 0, 2);
    s.response_out = out_chan;
    s.effect_out = dev_channel_port(sys, 1, 1);
    s.issued = [SLOT_FREE; SLOTS];
    s.served = [SLOT_FREE; SLOTS];
    set_defaults(s);
    if !params.is_null() && params_len >= 4 {
        parse_tlv(s, params, params_len);
    }
    let list = &s.ops_text[..s.ops_len as usize];
    match if list.is_empty() {
        Some([false; RL_OPS])
    } else {
        rl_ops(list)
    } {
        Some(set) => s.ops = set,
        None => {
            let m = b"[volume_relay] relay_ops is not a list of op numbers - REFUSED";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -22;
        }
    }
    // Request keys are this incarnation's own: a nonce, then a sequence.
    if (sys.provider_call)(-1, abi::kernel_abi::RANDOM_FILL, s.nonce.as_mut_ptr(), 8) != 0 {
        let m = b"[volume_relay] no random nonce - REFUSED";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return -5;
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return 0;
        }
        let s = &mut *(state as *mut State);
        if s.syscalls.is_null() {
            return 0;
        }
        let sys = &*s.syscalls;
        let now = dev_millis(sys);
        if s.started == 0 {
            s.started = 1;
            let prefix = if s.side == SIDE_NODE { NODE_OPS } else { ASKS };
            if s.sink >= 0 {
                store_subscribe(sys, prefix, s.sink, 0);
            }
            s.woken = 1;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.woken = 1;
        }
        let swept = now.wrapping_sub(s.last_sweep_ms) >= u64::from(s.poll_ms.max(10));
        if swept {
            s.last_sweep_ms = now;
        }
        if s.reply_len != 0 && !flush(sys, s.response_out, &mut s.reply, &mut s.reply_len) {
            return 0;
        }
        if s.effect_len != 0 && !flush(sys, s.effect_out, &mut s.effect, &mut s.effect_len) {
            return 0;
        }
        let waited = issue(s, sys, now, swept);
        let waited = serve(s, sys, now, swept) | waited;
        // A pass whose reads were not all answered (`EAGAIN`) runs again
        // next step rather than at the next sweep.
        s.woken = u8::from(waited);
        let sum = s
            .relayed
            .wrapping_add(s.answered)
            .wrapping_add(s.given_up)
            .wrapping_add(s.dropped)
            .wrapping_add(s.served_n);
        let since = now.wrapping_sub(s.last_hb_ms);
        if since >= HB_MS || (sum != s.last_hb_sum && since >= HB_CHANGED_MS) {
            s.last_hb_ms = now;
            s.last_hb_sum = sum;
            heartbeat(s, sys);
        }
        0
    }
}

/// `[volume_relay] hb relayed=… answered=… given_up=… dropped=… served=…`
unsafe fn heartbeat(s: &State, sys: &SyscallTable) {
    let mut m = [0u8; 160];
    let mut n = 0usize;
    let mut put = |b: &[u8]| {
        let k = b.len().min(m.len() - n);
        m[n..n + k].copy_from_slice(&b[..k]);
        n += k;
    };
    put(b"[volume_relay] hb");
    for (name, v) in [
        (&b" relayed="[..], s.relayed),
        (b" answered=", s.answered),
        (b" given_up=", s.given_up),
        (b" dropped=", s.dropped),
        (b" served=", s.served_n),
    ] {
        put(name);
        let mut d = [0u8; 10];
        let mut i = d.len();
        let mut x = v;
        loop {
            i -= 1;
            d[i] = b'0' + (x % 10) as u8;
            x /= 10;
            if x == 0 {
                break;
            }
        }
        put(&d[i..]);
    }
    dev_log(sys, 3, m.as_ptr(), n);
}

unsafe fn flush(sys: &SyscallTable, chan: i32, buf: &mut [u8; REC], len: &mut u16) -> bool {
    if chan < 0 {
        *len = 0;
        return true;
    }
    if (sys.channel_write)(chan, buf.as_ptr(), *len as usize) > 0 {
        *len = 0;
        return true;
    }
    false
}

/// One whole record off `chan` into `out`: its length.
unsafe fn take(
    sys: &SyscallTable,
    chan: i32,
    out: &mut [u8; REC],
    dropped: &mut u32,
) -> Option<usize> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(chan, out.as_mut_ptr(), REC);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(chan, out.as_mut_ptr(), REC)
    } else if pk <= 0 {
        return None;
    } else {
        match rl_frame_len(&out[..pk as usize]) {
            Some(fl) => (sys.channel_read)(chan, out.as_mut_ptr(), fl),
            None if (pk as usize) < REC => return None,
            None => {
                (sys.channel_read)(chan, out.as_mut_ptr(), REC);
                *dropped = dropped.wrapping_add(1);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}

fn rl_frame_len(data: &[u8]) -> Option<usize> {
    let count = *data.first()? as usize;
    let mut off = 1usize;
    for _ in 0..count {
        let l = u16::from_le_bytes([*data.get(off + 2)?, *data.get(off + 3)?]) as usize;
        off += 4 + l;
    }
    (off <= data.len()).then_some(off)
}

// ── issuing: this graph's chains' effects, to the other side ─────────

/// True when a read of the pass was not answered yet (`EAGAIN`).
unsafe fn issue(s: &mut State, sys: &SyscallTable, now: u64, swept: bool) -> bool {
    let store = Store::plain(sys, &s.nag);
    // A new effect, into a free slot: the held one first.
    if let Some(i) = s.issued.iter().position(|x| x.phase == PHASE_FREE) {
        let got = if s.held_len != 0 {
            let n = s.held_len as usize;
            s.rec[..n].copy_from_slice(&s.held[..n]);
            s.held_len = 0;
            Some(n)
        } else {
            take(sys, s.request_in, &mut s.rec, &mut s.dropped)
        };
        if let Some(n) = got {
            let rec = &s.rec[..n];
            if rl_carries(&s.ops, rl_i64(rec, 2)) && !start_issue(s, &store, i, n, now) {
                s.held[..n].copy_from_slice(&s.rec[..n]);
                s.held_len = n as u16;
            }
        }
    }
    for i in 0..SLOTS {
        match s.issued[i].phase {
            PHASE_WRITE => {
                let x = &s.issued[i];
                let key = &x.key[..x.key_len as usize];
                let value = &s.issued_values[i][..x.value_len as usize];
                match put_value(&store, key, value) {
                    WriteAnswer::Pending => {}
                    WriteAnswer::Decided(0) => s.issued[i].phase = PHASE_WAIT,
                    WriteAnswer::Decided(_) => give_up(s, sys, i),
                }
            }
            PHASE_WAIT if s.woken != 0 || swept => {
                let x = &s.issued[i];
                let key = &x.key[..x.key_len as usize];
                let Some(n) = get_value(&store, key, &mut s.buf) else {
                    if store.waiting() {
                        continue; // not answered yet, not withdrawn
                    }
                    // Withdrawn under it (a restart's sweep): asked again by
                    // the chain.
                    s.issued[i] = SLOT_FREE;
                    continue;
                };
                match rl_parts(&s.buf[..n]) {
                    Some((_, Some(out))) => {
                        if s.reply_len != 0 {
                            continue;
                        }
                        if let Some(m) = rl_unhex(out, &mut s.reply) {
                            {
                                s.reply_len = m as u16;
                                s.answered = s.answered.wrapping_add(1);
                                s.issued[i].phase = PHASE_WITHDRAW;
                            }
                        } else {
                            s.issued[i].phase = PHASE_WITHDRAW;
                        }
                    }
                    _ if now.wrapping_sub(s.issued[i].since_ms) >= u64::from(s.timeout_ms) => {
                        s.given_up = s.given_up.wrapping_add(1);
                        s.issued[i].phase = PHASE_WITHDRAW;
                    }
                    _ => {}
                }
            }
            PHASE_WITHDRAW => {
                let x = &s.issued[i];
                let key = &x.key[..x.key_len as usize];
                if delete_value(&store, key) != WriteAnswer::Pending {
                    s.issued[i] = SLOT_FREE;
                }
            }
            _ => {}
        }
    }
    if s.reply_len != 0 {
        flush(sys, s.response_out, &mut s.reply, &mut s.reply_len);
    }
    store.waiting()
}

/// Address effect `s.rec[..n]` and stage its request in slot `i`. False
/// when the address could not be read yet (`EAGAIN`): nothing was staged,
/// and the caller holds the effect for the next step.
unsafe fn start_issue(s: &mut State, store: &Store, i: usize, n: usize, now: u64) -> bool {
    let rec = &s.rec[..n];
    let mut key = [0u8; MAX_KEY];
    let mut k = 0usize;
    let mut add = |key: &mut [u8; MAX_KEY], part: &[u8]| -> bool {
        if k + part.len() > MAX_KEY {
            return false;
        }
        key[k..k + part.len()].copy_from_slice(part);
        k += part.len();
        true
    };
    let ok = if s.side == SIDE_NODE {
        add(&mut key, ASKS)
    } else {
        // The node holding the claim: named by the effect, or by the
        // claim's attachment.
        let att = &mut s.att;
        let mut node = [0u8; 64];
        let nl = match rl_named_device(rec) {
            Some(d) if d.len() <= node.len() => {
                node[..d.len()].copy_from_slice(d);
                d.len()
            }
            _ => {
                let claim = rl_bytes(rec, 3);
                let mut ak = [0u8; MAX_KEY];
                let akl = ATTACHMENTS.len() + claim.len();
                if akl > MAX_KEY {
                    0
                } else {
                    ak[..ATTACHMENTS.len()].copy_from_slice(ATTACHMENTS);
                    ak[ATTACHMENTS.len()..akl].copy_from_slice(claim);
                    match get_value(store, &ak[..akl], att) {
                        Some(m) => match rl_flat(&att[..m], b"device") {
                            Some(d) if d.len() <= node.len() => {
                                node[..d.len()].copy_from_slice(d);
                                d.len()
                            }
                            _ => 0,
                        },
                        None => 0,
                    }
                }
            }
        };
        if store.waiting() {
            return false;
        }
        let name = &node[..nl];
        rl_node_ok(name)
            && add(&mut key, TO_NODE)
            && add(&mut key, name)
            && add(&mut key, &NODE_OPS[..NODE_OPS.len()])
    };
    let mut id = [0u8; 16 + 1 + 8];
    let hl = rl_hex(&s.nonce, &mut id).unwrap_or(0);
    id[hl] = b'-';
    let seq = s.seq;
    s.seq = s.seq.wrapping_add(1);
    let sl = rl_hex(&seq.to_be_bytes(), &mut id[hl + 1..]).unwrap_or(0);
    let id_len = hl + 1 + sl;
    let ok = ok && add(&mut key, &id[..id_len]);
    let x = &mut s.issued[i];
    if !ok {
        // Nowhere to send it: the chain's own timeout answers it.
        s.dropped = s.dropped.wrapping_add(1);
        let m = b"[volume_relay] an effect names no node this relay can reach - dropped";
        dev_log(store.table(), 2, m.as_ptr(), m.len());
        return true;
    }
    let Some(vl) = rl_request(rec, &mut s.issued_values[i]) else {
        s.dropped = s.dropped.wrapping_add(1);
        return true;
    };
    x.key[..k].copy_from_slice(&key[..k]);
    x.key_len = k as u8;
    x.value_len = vl as u16;
    x.since_ms = now;
    x.phase = PHASE_WRITE;
    s.relayed = s.relayed.wrapping_add(1);
    true
}

unsafe fn give_up(s: &mut State, sys: &SyscallTable, i: usize) {
    s.given_up = s.given_up.wrapping_add(1);
    s.issued[i] = SLOT_FREE;
    let m = b"[volume_relay] a request could not be written - the chain redrives it";
    dev_log(sys, 2, m.as_ptr(), m.len());
}

// ── serving: the other side's requests, through this graph's effectors ─

/// True when a read of the pass was not answered yet (`EAGAIN`).
unsafe fn serve(s: &mut State, sys: &SyscallTable, now: u64, swept: bool) -> bool {
    let store = Store::plain(sys, &s.nag);
    // Outcomes from the effectors: a served request's (by its id) is
    // written back; the chains' own pass by.
    while let Some(n) = take(sys, s.answer_in, &mut s.rec, &mut s.dropped) {
        let cid = rl_i64(&s.rec[..n], 1);
        let Some(i) = s
            .served
            .iter()
            .position(|x| x.phase == PHASE_SENT && SERVED_CID | i64::from(slot_tag(x)) == cid)
        else {
            continue;
        };
        let Some(m) = rl_with_cid(&s.rec[..n], s.served[i].cid, &mut s.rec2) else {
            continue;
        };
        let vl = s.served[i].value_len as usize;
        s.buf[..vl].copy_from_slice(&s.served_values[i][..vl]);
        match rl_answered(&s.buf[..vl], &s.rec2[..m], &mut s.served_values[i]) {
            Some(al) => {
                s.served[i].value_len = al as u16;
                s.served[i].phase = PHASE_ANSWER;
            }
            None => s.served[i] = SLOT_FREE,
        }
    }
    for i in 0..SLOTS {
        if s.served[i].phase == PHASE_ANSWER {
            // Written back only while the request stands: one withdrawn is
            // not brought back.
            let x = &s.served[i];
            match head(&store, &x.key[..x.key_len as usize]) {
                Some(rev) => {
                    s.served[i].rev = rev;
                    s.served[i].phase = PHASE_ANSWERING;
                }
                None if store.waiting() => continue, // not answered yet
                None => {
                    s.served[i] = SLOT_FREE;
                    continue;
                }
            }
        }
        if s.served[i].phase != PHASE_ANSWERING {
            continue;
        }
        let x = &s.served[i];
        let key = &x.key[..x.key_len as usize];
        let value = &s.served_values[i][..x.value_len as usize];
        match put_cond(&store, key, value, PRE_ETAG, x.rev) {
            WriteAnswer::Pending => {}
            WriteAnswer::Decided(rc) => {
                if rc == 0 {
                    s.served_n = s.served_n.wrapping_add(1);
                }
                s.served[i] = SLOT_FREE;
            }
        }
    }
    // New requests: on a change, and on the sweep.
    if !(s.woken != 0 || swept) || s.effect_len != 0 {
        return store.waiting();
    }
    let prefix = if s.side == SIDE_NODE { NODE_OPS } else { ASKS };
    let mut walk = ListWalk::new(prefix);
    while let Some(k) = walk.next(&store) {
        if s.effect_len != 0 {
            break;
        }
        let Some(i) = s.served.iter().position(|x| x.phase == PHASE_FREE) else {
            break;
        };
        if k.len() > MAX_KEY
            || s.served
                .iter()
                .any(|x| x.phase != PHASE_FREE && &x.key[..x.key_len as usize] == k)
        {
            continue;
        }
        let mut key = [0u8; MAX_KEY];
        key[..k.len()].copy_from_slice(k);
        let kl = k.len();
        let Some(n) = get_value(&store, &key[..kl], &mut s.served_values[i]) else {
            continue;
        };
        let (req, out) = match rl_parts(&s.served_values[i][..n]) {
            Some(p) => p,
            None => continue,
        };
        if out.is_some() {
            continue;
        }
        let Some(m) = rl_unhex(req, &mut s.rec) else {
            continue;
        };
        let x = &mut s.served[i];
        x.key[..kl].copy_from_slice(&key[..kl]);
        x.key_len = kl as u8;
        x.value_len = n as u16;
        x.since_ms = now;
        x.cid = rl_i64(&s.rec[..m], 1);
        let tag = slot_tag(x);
        let Some(el) = rl_with_cid(&s.rec[..m], SERVED_CID | i64::from(tag), &mut s.effect) else {
            *x = SLOT_FREE;
            continue;
        };
        x.phase = PHASE_SENT;
        s.effect_len = el as u16;
        flush(sys, s.effect_out, &mut s.effect, &mut s.effect_len);
    }
    // A page or a value not answered yet: the requests past it are found
    // on the next pass.
    store.waiting()
}

/// A served slot's tag in the correlation id its effect carries: its key's
/// hash, so a slot reused for another request answers only its own.
fn slot_tag(x: &Slot) -> u32 {
    key_hash(&x.key[..x.key_len as usize])
}
