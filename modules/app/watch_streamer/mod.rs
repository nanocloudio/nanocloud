//! watch_streamer — `?watch=true` as a PIC module. The Kubernetes watch
//! contract is a direct projection of the store contract: LIST at a fence
//! revision, then the changes since it. This module serves it as long-poll over
//! the request/response seam — given a prefix + a `since` revision it returns
//! the entries whose revision is newer than `since` (as PUT events) plus the
//! new fence; the client re-requests with the fence for the next batch. A pure
//! store transform.
//!
//! Data model:
//!   /watch-req/<reqid>  = "prefix=<p>;since=<rev>"
//!   /<p>...             = the watched objects (each LIST entry carries its rev)
//!   /watch-resp/<reqid> = "rev=<fence>;events=PUT:<name>:<rev>,DELETE:<name>:<rev>,..."
//!
//! `since=0` yields the full initial state (every entry, as PUT). `since>0` is
//! incremental: `PUT:<name>:<rev>` for adds/updates and `DELETE:<name>:<rev>`
//! for removals, from the store's bounded change history (the CHANGES op). The
//! fence is the store's authoritative watermark — the client's next `since` —
//! correct even for an empty batch (a settled watch returns `events=` with the
//! current fence, so the client resumes without re-listing).

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::convert::TryInto;
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");

// The control-plane store via the standard fluxor storage contracts. The watch
// server is a direct projection of storage.namespace: LIST-at-a-fence then the
// changes since it. storage.object (0x14) for the request/response keys;
// storage.namespace (0x13) for the /watch-req/ change watch (SUBSCRIBE onto our
// self-edge sink) and the synchronous windowed change-read CHANGES (0x1307) that
// answers "what changed under <prefix> since <rev>?" in one call.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const NS_CHANGES: u32 = 0x1307;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;
// CHANGES event kind (the store's ChangeKind, wire-encoded): Deleted = 2.
const CHANGE_DELETED: u8 = 2;
// CHANGES status byte: window preceded retained history (client must relist).
const CHANGES_LOST: u8 = 1;

const REQ_PREFIX: &[u8] = b"/watch-req/";
const RESP_PREFIX: &[u8] = b"/watch-resp/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 1024;
const LIST_BUF: usize = 4096;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the /watch-req/ SUBSCRIBE + cold-start reconcile have run.
    subscribed: u8,
    batches: u32,
}

// ---- storage.object / storage.namespace ops ----

/// storage.namespace CHANGES — synchronous windowed change-read of `prefix`
/// since revision `since` (0 = full snapshot). Fills `out` with
/// `[status:u8][count:u32][events: [rev:u64][kind:u8][key_len:u16][val_len:u32]
/// [key][val]...]`. Returns bytes written, or None on error.
unsafe fn changes_query(
    sys: &SyscallTable,
    prefix: &[u8],
    since: u64,
    out: &mut [u8],
) -> Option<usize> {
    let mut arg = [0u8; MAX_KEY + 40];
    let mut fence = [0u8; 62];
    if 2 + prefix.len() + 8 + 8 + 4 + 8 + 2 > arg.len() {
        return None;
    }
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    arg[p..p + 8].copy_from_slice(&since.to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(out.len() as u32).to_le_bytes());
    p += 4;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, NS_CHANGES, arg.as_mut_ptr(), p);
    if n < 0 {
        None
    } else {
        Some(n as usize)
    }
}

// ---- helpers ----

fn parse_u64(b: &[u8]) -> u64 {
    let mut n: u64 = 0;
    for &c in b {
        if c.is_ascii_digit() {
            n = n.wrapping_mul(10).wrapping_add((c - b'0') as u64);
        } else {
            break;
        }
    }
    n
}

fn write_u64(dst: &mut [u8], at: usize, mut n: u64) -> usize {
    if at >= dst.len() {
        return at;
    }
    if n == 0 {
        dst[at] = b'0';
        return at + 1;
    }
    let mut tmp = [0u8; 20];
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

/// Build the watch batch for one request into `doc`. `since=0` is a full
/// snapshot (every live entry as PUT — the list half of list/watch, and the
/// resync a client relists into). `since>0` is incremental: emit `PUT:<name>:
/// <rev>` for adds/updates and `DELETE:<name>:<rev>` for removals. Both are one
/// synchronous storage.namespace CHANGES call. If the store's bounded history
/// can't reach `since` (LOST), fall back to a snapshot resync.
unsafe fn watch_batch(sys: &SyscallTable, req: &[u8], doc: &mut [u8]) -> usize {
    let prefix = field(req, b"prefix=").unwrap_or(b"");
    let since = field(req, b"since=").map(parse_u64).unwrap_or(0);

    let mut cbuf = [0u8; LIST_BUF];
    let n = changes_query(sys, prefix, since, &mut cbuf).unwrap_or(0);
    if n >= 5 && cbuf[0] == CHANGES_LOST {
        // resourceVersion too old — the client relists (a full snapshot).
        let n2 = changes_query(sys, prefix, 0, &mut cbuf).unwrap_or(0);
        return format_events(&cbuf[..n2], doc);
    }
    format_events(&cbuf[..n], doc)
}

/// Format a CHANGES batch (`[status:u8][count:u32][events...]`) into
/// `rev=<fence>;events=PUT|DELETE:<name>:<rev>,...`. The fence is the highest
/// revision covered — the client's next `since`. A settled watch (no events)
/// yields `events=` with the fence unchanged from the batch (0 if empty).
fn format_events(c: &[u8], doc: &mut [u8]) -> usize {
    let mut events = [0u8; MAX_VALUE];
    let mut el = 0usize;
    let mut first = true;
    // The fence is the highest watched-prefix revision in the batch — the
    // client's next `since`. There are no watched changes above it at snapshot
    // time, so resuming from it misses nothing. An empty batch yields 0 (a
    // client watching an empty collection simply re-snapshots until it fills).
    let mut fence = 0u64;

    if c.len() >= 5 {
        let count = u32::from_le_bytes(c[1..5].try_into().unwrap());
        // Event: [rev:u64][kind:u8][key_len:u16][val_len:u32][key][val]
        let mut q = 5usize;
        for _ in 0..count {
            if q + 15 > c.len() {
                break;
            }
            let rev = u64::from_le_bytes(c[q..q + 8].try_into().unwrap());
            let kind = c[q + 8];
            let klen = u16::from_le_bytes(c[q + 9..q + 11].try_into().unwrap()) as usize;
            let vlen = u32::from_le_bytes(c[q + 11..q + 15].try_into().unwrap()) as usize;
            let kstart = q + 15;
            if kstart + klen + vlen > c.len() || klen > MAX_KEY {
                break;
            }
            let name = last_seg(&c[kstart..kstart + klen]);
            let mut nbuf = [0u8; MAX_KEY];
            let nl = name.len().min(MAX_KEY);
            nbuf[..nl].copy_from_slice(&name[..nl]);
            q = kstart + klen + vlen;

            if rev > fence {
                fence = rev;
            }
            if !first {
                el = append(&mut events, el, b",");
            }
            first = false;
            let verb: &[u8] = if kind == CHANGE_DELETED {
                b"DELETE:"
            } else {
                b"PUT:"
            };
            el = append(&mut events, el, verb);
            el = append(&mut events, el, &nbuf[..nl]);
            el = append(&mut events, el, b":");
            el = write_u64(&mut events, el, rev);
        }
    }

    let mut d = append(doc, 0, b"rev=");
    d = write_u64(doc, d, fence);
    d = append(doc, d, b";events=");
    append(doc, d, &events[..el])
}

/// Service every /watch-req/ without a response yet.
unsafe fn reconcile(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut served = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        if klen <= REQ_PREFIX.len() {
            continue;
        }
        let tail = &key[REQ_PREFIX.len()..];
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);

        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        let mut req = [0u8; MAX_VALUE];
        let Some(rqlen) = get_value(sys, key, &mut req) else {
            continue;
        };

        let mut doc = [0u8; MAX_VALUE];
        let dlen = watch_batch(sys, &req[..rqlen], &mut doc);
        if put_value(sys, &rkey[..rlen], &doc[..dlen]) {
            served += 1;
        }
    }
    served
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<State>() {
            return -2;
        }
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.out_chan = out_chan;
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.batches = 0;
        0
    }
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

        // Cold start: resolve the change-sink channel (self-edge allocated),
        // SUBSCRIBE /watch-req/ onto it (live-only — cold-start reconcile below
        // services any request that predates the watch), then reconcile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.batches = s.batches.wrapping_add(reconcile(sys));
            return 0;
        }
        // A new /watch-req/ arrived → service the pending requests.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.batches = s.batches.wrapping_add(reconcile(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
