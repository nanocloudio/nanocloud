//! Image puller — the pull-SCHEDULING decision as a PIC module. Pulling an
//! image splits into a decision (which layers actually need fetching) and an
//! effect (the HTTP fetch + tar/gzip unpack). The decision is a pure store
//! transform — the manifest's layer digests minus what the content store
//! already holds — so it lives here; `image_fetcher` performs the effect the
//! plan names.
//!
//! Data model:
//!   /image-manifests/<image> = "layers=<digest>,<digest>,..."  (image_fetcher, from the manifest it GET)
//!   /blobs/<digest>          = <anything>                       (a present content blob)
//!   /image-pull-plan/<image> = "pull=<digest>,<digest>"         (the missing layers; empty = complete)
//!
//! Loop (level-triggered): watch /image-manifests/ + /blobs/; on change, for
//! each image recompute the missing set and PUT the plan when it changed.
//! `image_fetcher` fetches whatever `pull=` lists; when a blob lands under
//! /blobs/, the plan shrinks — a settled, fully-present image reads `pull=`.

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

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes + CAS, storage.namespace
// (0x13) prefix LIST + change SUBSCRIBE. Changes are pushed to us as
// namespace.change on the `changes` input channel (allocated by a graph
// self-edge); we drain them to detect movement, then re-reconcile.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const MANIFESTS_PREFIX: &[u8] = b"/image-manifests/";
const BLOBS_PREFIX: &[u8] = b"/blobs/";
const PLAN_PREFIX: &[u8] = b"/image-pull-plan/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 1024;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    plans: u32,
}

// ---- helpers ----

fn tail_after<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    key.get(prefix.len()..)
}

/// Whether a blob with `digest` is present in the content store.
unsafe fn blob_present(sys: &SyscallTable, digest: &[u8]) -> bool {
    let mut key = [0u8; MAX_KEY];
    let klen = BLOBS_PREFIX.len() + digest.len();
    if klen > key.len() {
        return false;
    }
    key[..BLOBS_PREFIX.len()].copy_from_slice(BLOBS_PREFIX);
    key[BLOBS_PREFIX.len()..klen].copy_from_slice(digest);
    exists(sys, &key[..klen])
}

/// Compute the pull plan for one manifest: the comma-separated missing digests.
unsafe fn compute_plan(sys: &SyscallTable, manifest: &[u8], plan: &mut [u8]) -> usize {
    let mut p = append(plan, 0, b"pull=");
    let layers = field(manifest, b"layers=").unwrap_or(b"");
    let mut first = true;
    let mut start = 0;
    while start <= layers.len() {
        let end = layers[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(layers.len());
        let digest = &layers[start..end];
        if !digest.is_empty() && !blob_present(sys, digest) {
            if !first {
                p = append(plan, p, b",");
            }
            first = false;
            p = append(plan, p, digest);
        }
        if end >= layers.len() {
            break;
        }
        start = end + 1;
    }
    p
}

/// Recompute every image's pull plan; PUT only what changed.
unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(MANIFESTS_PREFIX);
    let mut wrote = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        let mut valbuf = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut valbuf) else {
            continue;
        };

        let mut plan = [0u8; MAX_VALUE];
        let plen = compute_plan(sys, &valbuf[..vlen], &mut plan);

        let Some(tail) = tail_after(key, MANIFESTS_PREFIX) else {
            continue;
        };
        let mut pkey = [0u8; MAX_KEY];
        let pkl = PLAN_PREFIX.len() + tail.len();
        if pkl > pkey.len() {
            continue;
        }
        pkey[..PLAN_PREFIX.len()].copy_from_slice(PLAN_PREFIX);
        pkey[PLAN_PREFIX.len()..pkl].copy_from_slice(tail);

        let mut cur = [0u8; MAX_VALUE];
        if let Some(clen) = get_value(sys, &pkey[..pkl], &mut cur) {
            if cur[..clen] == plan[..plen] {
                continue;
            }
        }
        if put_value(sys, &pkey[..pkl], &plan[..plen]) {
            wrote += 1;
        }
    }
    wrote
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
        s.plans = 0;
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

        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, MANIFESTS_PREFIX, s.sink, 0);
                store_subscribe(sys, BLOBS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.plans = s.plans.wrapping_add(reconcile_all(sys));
            return 0;
        }

        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.plans = s.plans.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
