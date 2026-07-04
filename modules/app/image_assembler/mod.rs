//! Image assembler — the rootfs-ASSEMBLY decision as a PIC module
//! (manifest → missing digests → fetch → verify →
//! **assemble**, container-rootfs backend). image_fetcher lands the verified
//! layer blobs in the content-addressed cache; this module turns a fully
//! cached image into a runnable rootfs by projecting an ASSEMBLY JOB through
//! the existing sandbox machinery — `sandbox_runner` is the only module that
//! touches the privileged workload surface, so the extraction effect runs as
//! a host workload (the assembly script), not as new module privilege.
//!
//! Data model:
//!   /image-pull-plan/<name>      = "pull="                (drained = all layers cached; the trigger)
//!   /image-manifests/<name>      = "layers=<hex64>,..;.." (fetcher-written; the layer order)
//!   /sandboxes/img-asm-<name>    = "cmd=<sh script rootfs blobdir hex..>;phase=start|delete"  (out → sandbox_runner)
//!   /sandbox-status/img-asm-<name> = "state=..;code=<n>"  (in ← sandbox_runner)
//!   /image-rootfs/<name>         = "path=<dir>;state=<assembling|ready|failed>"  (this module's output;
//!                                   pod_lifecycle gates image-backed pods on state=ready)
//!
//! Loop (level-triggered): watch /image-pull-plan/ + /sandbox-status/; for
//! each drained plan with no rootfs record, project the assembly job; map the
//! job's terminal status to ready/failed and delete the job. A `failed`
//! record is terminal-visible (no retry loop) — deleting it re-triggers.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
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

const PLAN_PREFIX: &[u8] = b"/image-pull-plan/";
const MANIFESTS_PREFIX: &[u8] = b"/image-manifests/";
const ROOTFS_PREFIX: &[u8] = b"/image-rootfs/";
const SANDBOXES_PREFIX: &[u8] = b"/sandboxes/img-asm-";
const STATUS_PREFIX: &[u8] = b"/sandbox-status/img-asm-";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const MAX_NAME: usize = 64;
const MAX_PATH: usize = 96;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    sink: i32,
    subscribed: u8,
    assemblies: u32,
    script: [u8; MAX_PATH],
    script_len: u8,
    rootfs_base: [u8; MAX_PATH],
    rootfs_base_len: u8,
    blob_dir: [u8; MAX_PATH],
    blob_dir_len: u8,
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PATH;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, script, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATH { MAX_PATH } else { len };
                s.script_len = n as u8;
                if n > 0 { ptr_copy(s.script.as_mut_ptr(), d, n); }
            };

        2, rootfs_base, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATH { MAX_PATH } else { len };
                s.rootfs_base_len = n as u8;
                if n > 0 { ptr_copy(s.rootfs_base.as_mut_ptr(), d, n); }
            };

        3, blob_dir, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATH { MAX_PATH } else { len };
                s.blob_dir_len = n as u8;
                if n > 0 { ptr_copy(s.blob_dir.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

// ---- storage helpers (house pattern) ----

unsafe fn put_if_changed(sys: &SyscallTable, key: &[u8], value: &[u8]) -> bool {
    let mut cur = [0u8; MAX_VALUE];
    if let Some(n) = get_value(sys, key, &mut cur) {
        if &cur[..n] == value {
            return false;
        }
    }
    put_value(sys, key, value)
}

/// `;`-separated field lookup. UNLIKE the house helper this matches an EMPTY
/// value too (`>=`, not `>`): the drained-plan trigger is exactly `pull=`
/// with nothing after it, which must read as Some(b""), not None.
fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';')
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

fn make_key(dst: &mut [u8], prefix: &[u8], name: &[u8]) -> usize {
    if prefix.len() + name.len() > dst.len() {
        return 0;
    }
    let mut p = append(dst, 0, prefix);
    p = append(dst, p, name);
    p
}

/// Drive one drained image toward a ready rootfs. Returns writes made.
unsafe fn drive_one(s: &mut State, sys: &SyscallTable, name: &[u8]) -> u32 {
    let mut driven = 0u32;

    // Current rootfs record.
    let mut rkey = [0u8; MAX_KEY];
    let rkl = make_key(&mut rkey, ROOTFS_PREFIX, name);
    if rkl == 0 {
        return 0;
    }
    let mut rbuf = [0u8; 256];
    let rstate = get_value(sys, &rkey[..rkl], &mut rbuf).and_then(|n| {
        field(&rbuf[..n], b"state=").map(|f| {
            let mut st = [0u8; 16];
            let m = f.len().min(16);
            st[..m].copy_from_slice(&f[..m]);
            (st, m)
        })
    });

    // Job status key.
    let mut skey = [0u8; MAX_KEY];
    let skl = make_key(&mut skey, STATUS_PREFIX, name);
    let mut sbuf = [0u8; 128];
    let jstate = if skl != 0 {
        get_value(sys, &skey[..skl], &mut sbuf)
    } else {
        None
    };

    match rstate {
        None => {
            // No record yet: project the assembly job from the manifest.
            let mut mkey = [0u8; MAX_KEY];
            let mkl = make_key(&mut mkey, MANIFESTS_PREFIX, name);
            if mkl == 0 {
                return 0;
            }
            let mut mval = [0u8; MAX_VALUE];
            let Some(mlen) = get_value(sys, &mkey[..mkl], &mut mval) else {
                return 0;
            };
            let layers = field(&mval[..mlen], b"layers=").unwrap_or(b"");
            if layers.is_empty() {
                return 0;
            }
            // cmd = /bin/sh <script> <rootfs_base>/<name> <blob_dir> <hex...>
            let mut doc = [0u8; MAX_VALUE];
            let mut d = append(&mut doc, 0, b"cmd=/bin/sh ");
            d = append(&mut doc, d, &s.script[..s.script_len as usize]);
            d = append(&mut doc, d, b" ");
            d = append(&mut doc, d, &s.rootfs_base[..s.rootfs_base_len as usize]);
            d = append(&mut doc, d, b"/");
            d = append(&mut doc, d, name);
            d = append(&mut doc, d, b" ");
            d = append(&mut doc, d, &s.blob_dir[..s.blob_dir_len as usize]);
            // Layers in manifest order, space-separated argv.
            let mut start = 0usize;
            while start <= layers.len() {
                let end = layers[start..]
                    .iter()
                    .position(|&b| b == b',')
                    .map(|i| start + i)
                    .unwrap_or(layers.len());
                let hexd = &layers[start..end];
                if hexd.len() == 64 {
                    d = append(&mut doc, d, b" ");
                    d = append(&mut doc, d, hexd);
                }
                if end >= layers.len() {
                    break;
                }
                start = end + 1;
            }
            d = append(&mut doc, d, b";phase=start");
            let mut jkey = [0u8; MAX_KEY];
            let jkl = make_key(&mut jkey, SANDBOXES_PREFIX, name);
            if jkl == 0 {
                return 0;
            }
            // Record first (assembling), then the job — so a crash between the
            // two leaves a visible assembling record, not an orphan job.
            let mut rdoc = [0u8; 256];
            let mut rd = append(&mut rdoc, 0, b"path=");
            rd = append(&mut rdoc, rd, &s.rootfs_base[..s.rootfs_base_len as usize]);
            rd = append(&mut rdoc, rd, b"/");
            rd = append(&mut rdoc, rd, name);
            rd = append(&mut rdoc, rd, b";state=assembling");
            if put_if_changed(sys, &rkey[..rkl], &rdoc[..rd]) {
                driven += 1;
            }
            if put_if_changed(sys, &jkey[..jkl], &doc[..d]) {
                driven += 1;
                s.assemblies = s.assemblies.wrapping_add(1);
            }
        }
        Some((st, m)) if &st[..m] == b"assembling" => {
            // Map the job's terminal state.
            let Some(sn) = jstate else { return 0 };
            let jv = &sbuf[..sn];
            let state = field(jv, b"state=").unwrap_or(b"");
            let code = field(jv, b"code=").unwrap_or(b"");
            let terminal = state == b"exited" || state == b"failed" || state == b"killed";
            if !terminal {
                return 0;
            }
            let ok = state == b"exited" && code == b"0";
            let mut rdoc = [0u8; 256];
            let mut rd = append(&mut rdoc, 0, b"path=");
            rd = append(&mut rdoc, rd, &s.rootfs_base[..s.rootfs_base_len as usize]);
            rd = append(&mut rdoc, rd, b"/");
            rd = append(&mut rdoc, rd, name);
            rd = append(
                &mut rdoc,
                rd,
                if ok {
                    b";state=ready"
                } else {
                    b";state=failed"
                },
            );
            if put_if_changed(sys, &rkey[..rkl], &rdoc[..rd]) {
                driven += 1;
            }
            // Tear the job down (runner DESTROYs and marks destroyed).
            let mut jkey = [0u8; MAX_KEY];
            let jkl = make_key(&mut jkey, SANDBOXES_PREFIX, name);
            let mut jbuf = [0u8; MAX_VALUE];
            if jkl != 0 {
                if let Some(n) = get_value(sys, &jkey[..jkl], &mut jbuf) {
                    let cur = &jbuf[..n];
                    if field(cur, b"phase=") != Some(b"delete") {
                        // Rewrite with phase=delete, preserving cmd.
                        let cmd = field(cur, b"cmd=").unwrap_or(b"");
                        let mut doc = [0u8; MAX_VALUE];
                        let mut d = append(&mut doc, 0, b"cmd=");
                        d = append(&mut doc, d, cmd);
                        d = append(&mut doc, d, b";phase=delete");
                        if put_if_changed(sys, &jkey[..jkl], &doc[..d]) {
                            driven += 1;
                        }
                    }
                }
            }
        }
        _ => {} // ready / failed: settled (delete the record to re-trigger)
    }
    driven
}

/// Walk every drained pull plan and drive its assembly.
unsafe fn reconcile(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut driven = 0u32;
    let mut walk = ListWalk::new(PLAN_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        if klen <= PLAN_PREFIX.len() || key.len() - PLAN_PREFIX.len() > MAX_NAME {
            continue;
        }
        let name_buf = {
            let tail = &key[PLAN_PREFIX.len()..];
            let mut nb = [0u8; MAX_NAME];
            nb[..tail.len()].copy_from_slice(tail);
            (nb, tail.len())
        };
        // Only drained plans (pull= empty) assemble.
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut val) else {
            continue;
        };
        let pull = field(&val[..vlen], b"pull=").unwrap_or(b"x");
        if !pull.is_empty() {
            continue;
        }
        driven += drive_one(s, sys, &name_buf.0[..name_buf.1]);
    }
    driven
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
/// instance arena and syscall table; `params` is the config TLV blob (may be
/// null when `params_len == 0`). All are valid for the lifetime the loader
/// guarantees; never called concurrently.
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
        s.assemblies = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.script_len == 0 {
            let d = b"/usr/lib/nanocloud/assemble-image.sh";
            s.script[..d.len()].copy_from_slice(d);
            s.script_len = d.len() as u8;
        }
        if s.rootfs_base_len == 0 {
            let d = b"/var/lib/nanocloud.io/image/rootfs";
            s.rootfs_base[..d.len()].copy_from_slice(d);
            s.rootfs_base_len = d.len() as u8;
        }
        if s.blob_dir_len == 0 {
            let d = b"/var/lib/nanocloud.io/image/blobs";
            s.blob_dir[..d.len()].copy_from_slice(d);
            s.blob_dir_len = d.len() as u8;
        }
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
                store_subscribe(sys, PLAN_PREFIX, s.sink, 0);
                // The WHOLE status namespace, not the img-asm- key prefix — a
                // subscription binds a namespace, and the job-completion event
                // is what re-reconciles us out of `assembling`.
                store_subscribe(sys, b"/sandbox-status/", s.sink, 0);
            }
            s.subscribed = 1;
            reconcile(s, sys);
            return 0;
        }

        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            reconcile(s, sys);
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
