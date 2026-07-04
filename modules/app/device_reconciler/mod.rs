//! Device reconciler — provision a SPIFFE identity for every Device CR.
//!
//! The identity half of the Device lifecycle, as a PIC module in the reconciler
//! tier (mirrors `deployment_reconciler`). It watches the Device custom
//! resource and drives the in-tree `cert_manager` (over the store seam, not
//! a direct channel) to issue a family-canonical SPIFFE leaf, then records the
//! provisioned identity back onto a status key.
//!
//! The SPIFFE naming is the nanocloud-family scheme
//! (`spiffe://<trust-domain>/device/<namespace>/<name>`) shared with kagi and
//! sector — see `../kagi/docs/identity-provisioning.md`
//!
//! Data model:
//!
//!   /devices.nanocloud.io/<ns>/<name>       = <DeviceSpec JSON>   (input)
//!   /cert-req/<ns>-<name>                    = "cn=…;dns=…;spiffe=…"  (to cert_manager)
//!   /cert-resp/<ns>-<name>                   = "crt=<hex>;key=<hex>"  (from cert_manager)
//!   /ca-cert                                 = "<CA cert DER hex>"    (from cert_manager)
//!   /deviceidentities.nanocloud.io/<ns>/<name> = "spiffe=…;crt=<hex>;ca=<hex>"  (output)
//!
//! Loop shape (level-triggered): first step SUBSCRIBEs the Device prefix AND the
//! /cert-resp/ prefix (so a mint completing also wakes us), then a cold-start
//! full pass. Each device: ensure its /cert-req/ exists (write on change), and
//! once /cert-resp/ appears, project the identity status — a GET-compare guards
//! every PUT so a quiet cluster spends no revisions. We never watch our own
//! output prefixes (/cert-req/, /deviceidentities…), so there is no self-wake.

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

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const DEVICES_PREFIX: &[u8] = b"/devices.nanocloud.io/";
const IDENTITY_PREFIX: &[u8] = b"/deviceidentities.nanocloud.io/";
const REQ_PREFIX: &[u8] = b"/cert-req/";
const RESP_PREFIX: &[u8] = b"/cert-resp/";
const CA_CERT_KEY: &[u8] = b"/ca-cert";

/// Family-canonical SPIFFE trust domain for locally-provisioned devices. A real
/// deployment would parameterize this (module params); the local/dev default is
/// `nanocloud.local` per the identity-provisioning contract.
const TRUST_DOMAIN: &[u8] = b"nanocloud.local";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 256;
const RESP_MAX: usize = 2048;
const CA_MAX: usize = 1024;
const MAX_DOC: usize = 4096;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Input-port channel the store pushes `namespace.change` events onto.
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// Count of docs (cert-reqs + identities) (re)written — progress observable.
    reconciles: u32,
}

// ---- storage.object / storage.namespace ops (identical seam to the sibling
//      reconcilers) ----

unsafe fn store_put(sys: &SyscallTable, key: &[u8], value: &[u8]) -> bool {
    let mut arg = [0u8; MAX_KEY + 64];
    // `1 + 1` is the precondition PAIR — `[precondition][etag_len]`. The
    // bound counts both bytes: the buffer's slack would otherwise hide an
    // undercount until a long key made it overflow.
    if 2 + key.len() + 1 + 8 + 8 + 1 + 1 + 8 + 2 > arg.len() {
        return false;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p] = 0;
    p += 1;
    arg[p..p + 8].copy_from_slice(&(value.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(value.len() as u64).to_le_bytes());
    p += 8;
    // storage.object writes carry a precondition PAIR — `[precondition:u8]
    // [etag_len:u8]` — two bytes, not one. Every field after it (including
    // `fence_out_ptr`) is positioned off that width; get it wrong and the
    // provider reads a misaligned pointer and the write is silently lost —
    // the module loads, reaches ready, and produces nothing.
    arg[p] = 0; // precondition::ANY
    p += 1;
    arg[p] = 0; // etag_len (none, under ANY)
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    (sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p) == 0
}

unsafe fn store_get(sys: &SyscallTable, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return None;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return None;
    }
    let mut rarg = [0u8; 20];
    rarg[8..12].copy_from_slice(&(dst.len() as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    if n < 0 {
        None
    } else {
        Some(n as usize)
    }
}

unsafe fn store_list(sys: &SyscallTable, prefix: &[u8], out: &mut [u8]) -> usize {
    let mut larg = [0u8; MAX_KEY + 32];
    if 2 + prefix.len() + 2 + 8 + 4 + 8 + 2 > larg.len() {
        return 0;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    larg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    larg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    larg[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;
    larg[p..p + 8].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    larg[p..p + 4].copy_from_slice(&(out.len() as u32).to_le_bytes());
    p += 4;
    larg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    larg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, NS_LIST, larg.as_mut_ptr(), p);
    if n < 0 {
        0
    } else {
        n as usize
    }
}

// ---- helpers ----

fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';' || b == b',')
            .map(|i| start + i)
            .unwrap_or(value.len());
        let seg = &value[start..end];
        if seg.len() >= tag.len() && &seg[..tag.len()] == tag {
            return Some(&seg[tag.len()..]);
        }
        if end >= value.len() {
            break;
        }
        start = end + 1;
    }
    None
}

/// The `<ns>` segment (WITHOUT trailing slash) after `prefix` in `key`.
fn namespace<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let rest = key.get(prefix.len()..)?;
    let slash = rest.iter().position(|&b| b == b'/')?;
    Some(&rest[..slash])
}

/// The `<ns>-<name>` request id used for the cert-req / cert-resp keys.
fn req_id(dst: &mut [u8], ns: &[u8], name: &[u8]) -> usize {
    let mut p = append(dst, 0, ns);
    p = append(dst, p, b"-");
    append(dst, p, name)
}

/// Provision one Device: ensure its cert-req exists, and once minted, project
/// the identity status. Returns the number of docs (re)written (0..=2).
unsafe fn reconcile_device(sys: &SyscallTable, dev_key: &[u8]) -> u32 {
    let Some(ns) = namespace(dev_key, DEVICES_PREFIX) else {
        return 0;
    };
    let name = last_seg(dev_key);
    if name.is_empty() {
        return 0;
    }

    // spiffe://<td>/device/<ns>/<name>
    let mut spiffe = [0u8; MAX_KEY];
    let mut sp = append(&mut spiffe, 0, b"spiffe://");
    sp = append(&mut spiffe, sp, TRUST_DOMAIN);
    sp = append(&mut spiffe, sp, b"/device/");
    sp = append(&mut spiffe, sp, ns);
    sp = append(&mut spiffe, sp, b"/");
    sp = append(&mut spiffe, sp, name);
    let spiffe = &spiffe[..sp];

    // request id + cert-req / cert-resp keys.
    let mut idbuf = [0u8; MAX_KEY];
    let idl = req_id(&mut idbuf, ns, name);
    let id = &idbuf[..idl];

    let mut reqkey = [0u8; MAX_KEY];
    let rkl = append(&mut reqkey, 0, REQ_PREFIX);
    let rkl = append(&mut reqkey, rkl, id);
    let reqkey = &reqkey[..rkl];

    // cert-req value: "cn=device:<name>;dns=<name>;spiffe=<id>"
    let mut reqval = [0u8; MAX_VALUE];
    let mut vp = append(&mut reqval, 0, b"cn=device:");
    vp = append(&mut reqval, vp, name);
    vp = append(&mut reqval, vp, b";dns=");
    vp = append(&mut reqval, vp, name);
    vp = append(&mut reqval, vp, b";spiffe=");
    vp = append(&mut reqval, vp, spiffe);
    let reqval = &reqval[..vp];

    let mut wrote = 0u32;
    let mut cur = [0u8; MAX_VALUE];
    let changed = match store_get(sys, reqkey, &mut cur) {
        Some(n) => &cur[..n] != reqval,
        None => true,
    };
    if changed && store_put(sys, reqkey, reqval) {
        wrote += 1;
    }

    // If cert_manager has minted the response, project the identity status.
    let mut respkey = [0u8; MAX_KEY];
    let pk = append(&mut respkey, 0, RESP_PREFIX);
    let pk = append(&mut respkey, pk, id);
    let respkey = &respkey[..pk];

    let mut resp = [0u8; RESP_MAX];
    let Some(rn) = store_get(sys, respkey, &mut resp) else {
        return wrote;
    };
    let Some(crt) = field(&resp[..rn], b"crt=") else {
        return wrote;
    };
    let mut ca = [0u8; CA_MAX];
    let ca = match store_get(sys, CA_CERT_KEY, &mut ca) {
        Some(n) => &ca[..n],
        None => &[],
    };

    // identity doc: "spiffe=<id>;crt=<hex>;ca=<hex>"
    let mut doc = [0u8; MAX_DOC];
    let mut dp = append(&mut doc, 0, b"spiffe=");
    dp = append(&mut doc, dp, spiffe);
    dp = append(&mut doc, dp, b";crt=");
    dp = append(&mut doc, dp, crt);
    dp = append(&mut doc, dp, b";ca=");
    dp = append(&mut doc, dp, ca);
    let doc = &doc[..dp];

    // /deviceidentities.nanocloud.io/<ns>/<name>
    let mut idkey = [0u8; MAX_KEY];
    let mut kp = append(&mut idkey, 0, IDENTITY_PREFIX);
    kp = append(&mut idkey, kp, ns);
    kp = append(&mut idkey, kp, b"/");
    kp = append(&mut idkey, kp, name);
    let idkey = &idkey[..kp];

    let mut curid = [0u8; MAX_DOC];
    let id_changed = match store_get(sys, idkey, &mut curid) {
        Some(n) => &curid[..n] != doc,
        None => true,
    };
    if id_changed && store_put(sys, idkey, doc) {
        wrote += 1;
    }
    wrote
}

/// Reconcile every Device (level-triggered full pass).
unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut lout = [0u8; LIST_BUF];
    let n = store_list(sys, DEVICES_PREFIX, &mut lout);
    let mut wrote = 0u32;
    let mut p = 0;
    while p < n {
        let name_len = lout[p] as usize;
        if name_len == 0xFF {
            break;
        }
        if p + 2 + name_len > n || name_len > MAX_KEY {
            break;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..name_len].copy_from_slice(&lout[p + 2..p + 2 + name_len]);
        p += 2 + name_len;
        // Confirm the device still exists (a value is present) before acting.
        let mut probe = [0u8; 8];
        if store_get(sys, &keybuf[..name_len], &mut probe).is_none() {
            continue;
        }
        wrote += reconcile_device(sys, &keybuf[..name_len]);
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
        s.sink = in_chan;
        s.subscribed = 0;
        s.reconciles = 0;
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
                // Watch the Device prefix AND /cert-resp/ (a completing mint must
                // wake us to project the identity). We never watch our own output
                // prefixes, so no self-wake.
                store_subscribe(sys, DEVICES_PREFIX, s.sink, 0);
                store_subscribe(sys, RESP_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.reconciles = s.reconciles.wrapping_add(reconcile_all(sys));
            return 0;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.reconciles = s.reconciles.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
