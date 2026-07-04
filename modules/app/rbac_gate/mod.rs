//! RBAC gate — the authorization decision as a PIC module (the API plane's auth
//! chain). Given an authenticated identity plus the (verb, resource) it wants,
//! decide allow/deny against the RBAC objects in the store: resolve
//! RoleBindings whose subjects include the identity to their Roles, and allow
//! if any Role rule matches the verb+resource (with `*` wildcards). A pure
//! store transform — no host facts.
//!
//! Data model (request/response over the store seam):
//!   /authz-req/<reqid>    = "id=<identity>;verb=<verb>;resource=<resource>"
//!   /rolebindings/<name>  = "subjects=<id>,<id>,...;role=<rolename>"
//!   /roles/<name>         = "rules=<verb>:<resource>,<verb>:<resource>,..."
//!   /authz-resp/<reqid>   = "200;allow"  |  "403;deny"
//!
//! A rule token `<verb>:<resource>` matches when its verb is the request verb or
//! `*`, and its resource is the request resource or `*` (so `*:*` is cluster
//! admin). Deny by default — no matching binding→role→rule means 403.

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
// self-edge); we drain them to detect movement, then re-service.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const REQ_PREFIX: &[u8] = b"/authz-req/";
const RESP_PREFIX: &[u8] = b"/authz-resp/";
const RB_PREFIX: &[u8] = b"/rolebindings/";
const ROLE_PREFIX: &[u8] = b"/roles/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 512;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start pass have run.
    subscribed: u8,
    decisions: u32,
}

// ---- helpers ----

/// Does the comma-separated `list` contain the exact token `item`?
fn csv_contains(list: &[u8], item: &[u8]) -> bool {
    let mut start = 0;
    while start <= list.len() {
        let end = list[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(list.len());
        if &list[start..end] == item {
            return true;
        }
        if end >= list.len() {
            break;
        }
        start = end + 1;
    }
    false
}

/// Whether a comma-separated `rules` list (`<verb>:<resource>` tokens) permits
/// `verb` on `resource`, honouring `*` wildcards on either side.
fn rules_match(rules: &[u8], verb: &[u8], resource: &[u8]) -> bool {
    let mut start = 0;
    while start <= rules.len() {
        let end = rules[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(rules.len());
        let rule = &rules[start..end];
        if let Some(colon) = rule.iter().position(|&b| b == b':') {
            let rv = &rule[..colon];
            let rr = &rule[colon + 1..];
            let verb_ok = rv == b"*" || rv == verb;
            let res_ok = rr == b"*" || rr == resource;
            if verb_ok && res_ok {
                return true;
            }
        }
        if end >= rules.len() {
            break;
        }
        start = end + 1;
    }
    false
}

/// Authorize (id, verb, resource): allow if any RoleBinding whose subjects
/// include `id` references a Role whose rules match. Deny by default.
unsafe fn authorize(sys: &SyscallTable, id: &[u8], verb: &[u8], resource: &[u8]) -> bool {
    let mut walk = ListWalk::new(RB_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut rbkey = [0u8; MAX_KEY];
        rbkey[..klen].copy_from_slice(key);

        let mut rbval = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &rbkey[..klen], &mut rbval) else {
            continue;
        };
        let subjects = field(&rbval[..vlen], b"subjects=").unwrap_or(b"");
        if !csv_contains(subjects, id) {
            continue;
        }
        let Some(role) = field(&rbval[..vlen], b"role=") else {
            continue;
        };

        // Resolve the Role's rules.
        let mut role_key = [0u8; MAX_KEY];
        let rkl = ROLE_PREFIX.len() + role.len();
        if rkl > role_key.len() {
            continue;
        }
        role_key[..ROLE_PREFIX.len()].copy_from_slice(ROLE_PREFIX);
        role_key[ROLE_PREFIX.len()..rkl].copy_from_slice(role);
        let mut rbuf = [0u8; MAX_VALUE];
        let Some(rlen) = get_value(sys, &role_key[..rkl], &mut rbuf) else {
            continue;
        };
        let rules = field(&rbuf[..rlen], b"rules=").unwrap_or(b"");
        if rules_match(rules, verb, resource) {
            return true;
        }
    }
    false
}

/// Service every /authz-req/ without a response yet.
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
        let id = field(&req[..rqlen], b"id=").unwrap_or(b"");
        let verb = field(&req[..rqlen], b"verb=").unwrap_or(b"");
        let resource = field(&req[..rqlen], b"resource=").unwrap_or(b"");

        let allowed = !id.is_empty() && authorize(sys, id, verb, resource);
        let resp: &[u8] = if allowed { b"200;allow" } else { b"403;deny" };
        if put_value(sys, &rkey[..rlen], resp) {
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
        s.decisions = 0;
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

        // Cold start: resolve the change-sink channel (self-edge allocated), then
        // SUBSCRIBE the watched prefix onto it, then service pre-existing requests.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.decisions = s.decisions.wrapping_add(reconcile(sys));
        }
        // A pushed namespace.change means new /authz-req/ to service.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.decisions = s.decisions.wrapping_add(reconcile(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
