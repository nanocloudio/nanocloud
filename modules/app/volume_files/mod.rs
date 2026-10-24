//! A Pod's own volumes on its node. See `manifest.toml` for the wire.
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
use abi::platform::linux::host_mount as hm;
use abi::platform::linux::host_process as hp;
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
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;

include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/poddns.rs");
include!("../_shared/podres.rs");

const FS_CLOSE: u32 = 0x0903;
const FS_OPENDIR: u32 = 0x0907;
const FS_READDIR: u32 = 0x0908;
const FS_MKDIR: u32 = 0x090B;
const EAGAIN: i32 = -11;
const EINPROGRESS: i32 = -36;
const ENOENT: i32 = -2;

const CHAN_EINVAL: i32 = -22;
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const REC: usize = 8192;
/// A ConfigMap or Secret, read whole.
const OBJ: usize = 64 * 1024;
/// One file's content, decoded.
const FILE: usize = 64 * 1024;
/// A projection as one `PROJECT` argument: every file of the volume.
const HM_ARG: usize = 72 * 1024;
const PATH: usize = 256;
const CMD: usize = 512;
/// A PREPARE's volume spec (field 3).
const SPEC: usize = 2048;
const OP_NOOP: i64 = 0;
const OP_PREPARE: i64 = 40;
/// The chain's carry field the directory rides back in.
const DIR_FIELD: u8 = 39;
/// Directories whose contents are known written, by (volume, content).
const CACHE: usize = 64;
/// Gone Pods' directories found by one sweep.
const SWEEP: usize = 16;

/// Where a PREPARE (or the sweep) is.
const J_IDLE: u8 = 0;
/// Reading what the volume holds, and asking the platform for it.
const J_RUN: u8 = 1;
/// An emptyDir's mode, through the process executor.
const J_CHMOD: u8 = 2;
/// A `host_mount` op of the PREPARE in flight, asked again byte-identical.
const J_HM: u8 = 3;
/// A gone Pod's directory being removed (`REMOVE_TREE`).
const J_SWEEP: u8 = 4;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    request_out: i32,
    /// The Pods' directories, relative to the platform's mount root.
    root: [u8; 128],
    root_len: u8,
    /// The mount root (`host_mount::ROOT`), absolute; 0 until asked.
    mroot: [u8; 128],
    mroot_len: u8,
    /// `host_mount::PRIV` (-1 until asked).
    privs: i32,
    gc_ms: u32,
    /// The request held while it is served.
    rec: [u8; REC],
    rec_len: usize,
    /// The record on its way out.
    out: [u8; REC],
    out_len: usize,
    job: u8,
    /// The held PREPARE's content digest, cached once done.
    digest: u64,
    /// What the held PREPARE binds once its op succeeds.
    bind: [u8; PATH],
    bind_len: usize,
    proc_h: i32,
    proc_ok: bool,
    /// The `host_mount` op in flight and its argument.
    hm_op: u32,
    hm_len: usize,
    obj: [u8; OBJ],
    obj_len: usize,
    file: [u8; FILE],
    tmp: [u8; FILE],
    hm: [u8; HM_ARG],
    cache: [[u64; 2]; CACHE],
    cache_next: usize,
    /// The sweep: gone Pods' names, and the next to remove.
    gone: [[u8; 64]; SWEEP],
    gone_len: [u8; SWEEP],
    gone_n: usize,
    last_gc_ms: u64,
    prepared: u32,
    failed: u32,
    removed: u32,
    last_hb_ms: u64,
    last_hb_sum: u32,
    said_proc: bool,
    said_root: bool,
    said_memory: bool,
    said_outside: bool,
    said_rc: bool,
    drain: [u8; 256],
    nag: ReadNag,
}

define_params! {
    State;

    1, root, str, 0
        => |s, d, len| {
            if len < s.root.len() {
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                for (o, &c) in s.root.iter_mut().zip(src) {
                    *o = c;
                }
                s.root_len = len as u8;
            }
        };
    2, gc_ms, u32, 30000
        => |s, d, len| { s.gc_ms = p_u32(d, len, 0, 30_000); };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

/// A root the platform can take: relative, no empty, `.` or `..` part.
fn rel_ok(p: &[u8]) -> bool {
    !p.is_empty()
        && p.first() != Some(&b'/')
        && p.split(|&c| c == b'/')
            .all(|c| !c.is_empty() && c != b"." && c != b"..")
}

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
    s.syscalls = syscalls as *const SyscallTable;
    s.request_in = in_chan;
    s.request_out = out_chan;
    s.rec_len = 0;
    s.out_len = 0;
    s.job = J_IDLE;
    s.proc_h = -1;
    s.gone_n = 0;
    s.cache_next = 0;
    s.mroot_len = 0;
    s.privs = -1;
    set_defaults(s);
    if !params.is_null() && params_len >= 4 {
        parse_tlv(s, params, params_len);
    }
    if s.root_len == 0 {
        let d = b"pods";
        for (o, &c) in s.root.iter_mut().zip(d.iter()) {
            *o = c;
        }
        s.root_len = d.len() as u8;
    }
    if !rel_ok(s.root.get(..s.root_len as usize).unwrap_or(&[])) {
        let m = b"[volume_files] root is not a path relative to the mount root - REFUSED";
        dev_log(&*s.syscalls, 1, m.as_ptr(), m.len());
        return -22;
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
        step(s, sys);
        heartbeat(s, sys);
        0
    }
}

unsafe fn step(s: &mut State, sys: &SyscallTable) {
    if s.out_len != 0 {
        if (sys.channel_write)(s.request_out, s.out.as_ptr(), s.out_len) <= 0 {
            return;
        }
        s.out_len = 0;
    }
    if s.proc_h >= 0 {
        if !poll_proc(s, sys) {
            return;
        }
        if s.job == J_CHMOD {
            let ok = s.proc_ok;
            return prepared(s, sys, ok);
        }
    }
    if s.job == J_HM || s.job == J_SWEEP {
        let rc = (sys.provider_call)(-1, s.hm_op, s.hm.as_mut_ptr(), s.hm_len);
        if later(rc) {
            return;
        }
        return hm_done(s, sys, rc);
    }
    if s.job == J_IDLE && s.rec_len == 0 {
        if sweep(s, sys) {
            return;
        }
        match take(sys, s.request_in, &mut s.rec) {
            Some(n) => s.rec_len = n,
            None => return,
        }
        if frame_i64(s.rec.get(..n_of(s)).unwrap_or(&[]), 2) != Some(OP_PREPARE) {
            // Not this module's: on to the store connector, untouched.
            forward(s, None);
            return;
        }
        s.job = J_RUN;
    }
    if s.job == J_RUN {
        run(s, sys);
    }
}

fn n_of(s: &State) -> usize {
    s.rec_len.min(REC)
}

/// What a PREPARE names.
struct Vol<'a> {
    kind: u8,
    vol: &'a [u8],
    ns: &'a [u8],
    name: &'a [u8],
    mode: &'a [u8],
    optional: bool,
    items: &'a [u8],
    uid: &'a [u8],
    /// The group the volume is given (the Pod's fsGroup), if any.
    group: Option<u32>,
    /// fsGroupChangePolicy OnRootMismatch (`r`), else Always.
    on_mismatch: bool,
}

fn seg(s: &[u8], n: usize) -> &[u8] {
    s.split(|&c| c == b'/').nth(n).unwrap_or(&[])
}

fn name_ok(n: &[u8]) -> bool {
    !n.is_empty()
        && n.len() < 64
        && n.iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'-' || c == b'.')
        && n != b"."
        && n != b".."
}

/// A group ID as pod_resolve writes one (decimal, 0..=2147483647).
fn gid_of(g: &[u8]) -> Option<u32> {
    if g.is_empty() || g.len() > 10 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in g {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    (v <= 0x7FFF_FFFF).then_some(v as u32)
}

/// `<gid>/<a|r>` at segments `at`, `at + 1`: the fsGroup and its policy.
fn fs_group(spec: &[u8], at: usize) -> Option<(Option<u32>, bool)> {
    let g = seg(spec, at);
    if g.is_empty() {
        return Some((None, false));
    }
    let pol = seg(spec, at + 1);
    if pol != b"a" && pol != b"r" {
        return None;
    }
    Some((Some(gid_of(g)?), pol == b"r"))
}

fn parse_vol<'a>(spec: &'a [u8], id: &'a [u8]) -> Option<Vol<'a>> {
    let uid = match id.iter().position(|&c| c == b'~') {
        Some(t) => id.get(..t)?,
        None => id,
    };
    if !name_ok(uid) || spec.first() != Some(&b'@') {
        return None;
    }
    let kind = *spec.get(1)?;
    let vol = seg(spec, 1);
    if !name_ok(vol) {
        return None;
    }
    let bare = Vol {
        kind,
        vol,
        ns: &[],
        name: &[],
        mode: &[],
        optional: false,
        items: &[],
        uid,
        group: None,
        on_mismatch: false,
    };
    match kind {
        b'e' => {
            let (group, on_mismatch) = fs_group(spec, 2)?;
            Some(Vol {
                group,
                on_mismatch,
                ..bare
            })
        }
        // A file the Pod's spec states whole (its resolv.conf): its text
        // `%XX`-escaped, the rest of the spec.
        b'd' => {
            let at = 3 + vol.len() + 1;
            Some(Vol {
                items: spec.get(at..).unwrap_or(&[]),
                ..bare
            })
        }
        // A claim the fsGroup owns: `@g/<vol>/<ns>/<claim>/<gid>/<a|r>`.
        b'g' => {
            let (ns, name) = (seg(spec, 2), seg(spec, 3));
            let (group, on_mismatch) = fs_group(spec, 4)?;
            if !name_ok(ns) || !name_ok(name) || group.is_none() {
                return None;
            }
            Some(Vol {
                ns,
                name,
                group,
                on_mismatch,
                ..bare
            })
        }
        // A downwardAPI (`@w`) or projected (`@p`) volume:
        // `/<ns>/<mode>/<gid>/<item>|…`, each item `<path>=<mode>=<source>`.
        b'w' | b'p' => {
            let (ns, mode, g) = (seg(spec, 2), seg(spec, 3), seg(spec, 4));
            if !name_ok(ns) || mode.len() != 3 || !mode.iter().all(|c| (b'0'..=b'7').contains(c)) {
                return None;
            }
            let group = if g.is_empty() { None } else { Some(gid_of(g)?) };
            let mut at = 0usize;
            let mut n = 0usize;
            for (i, &c) in spec.iter().enumerate() {
                if c == b'/' {
                    n += 1;
                    if n == 5 {
                        at = i + 1;
                        break;
                    }
                }
            }
            if at == 0 {
                return None;
            }
            Some(Vol {
                ns,
                mode,
                items: spec.get(at..)?,
                group,
                ..bare
            })
        }
        b'c' | b's' => {
            let (ns, name, mode, opt) = (seg(spec, 2), seg(spec, 3), seg(spec, 4), seg(spec, 5));
            if !name_ok(ns)
                || !name_ok(name)
                || mode.len() != 3
                || !mode.iter().all(|c| (b'0'..=b'7').contains(c))
            {
                return None;
            }
            let g = seg(spec, 6);
            let group = if g.is_empty() { None } else { Some(gid_of(g)?) };
            // Items run to the end (their paths hold `/`).
            let mut at = 0usize;
            let mut n = 0usize;
            for (i, &c) in spec.iter().enumerate() {
                if c == b'/' {
                    n += 1;
                    if n == 7 {
                        at = i + 1;
                        break;
                    }
                }
            }
            let items = if at == 0 { &[][..] } else { spec.get(at..)? };
            Some(Vol {
                ns,
                name,
                mode,
                optional: opt == b"1",
                items,
                group,
                ..bare
            })
        }
        _ => None,
    }
}

/// `<root>/<uid>/volumes/<vol>`, relative to the mount root, into `out`.
fn vol_rel(s: &State, v: &Vol, out: &mut [u8; PATH]) -> Option<usize> {
    let mut n = 0usize;
    for &c in s
        .root
        .get(..s.root_len as usize)?
        .iter()
        .chain(b"/")
        .chain(v.uid)
        .chain(b"/volumes/")
        .chain(v.vol)
    {
        *out.get_mut(n)? = c;
        n += 1;
    }
    Some(n)
}

/// `rel` (relative to the mount root) made absolute, `/<leaf>` after it
/// when one is given.
fn abs_of(s: &State, rel: &[u8], leaf: &[u8], out: &mut [u8; PATH]) -> Option<usize> {
    let mut n = 0usize;
    let sep: &[u8] = if leaf.is_empty() { b"" } else { b"/" };
    for &c in s
        .mroot
        .get(..s.mroot_len as usize)?
        .iter()
        .chain(b"/")
        .chain(rel)
        .chain(sep)
        .chain(leaf)
    {
        *out.get_mut(n)? = c;
        n += 1;
    }
    Some(n)
}

/// An fs or platform answer that is not yet a decision.
fn later(rc: i32) -> bool {
    rc == EAGAIN || rc == EINPROGRESS
}

/// Every directory from the root down to `path` (inclusive), made. False
/// when the provider has not answered yet; an error is the next op's to
/// meet.
unsafe fn mkdirs(sys: &SyscallTable, path: &[u8]) -> bool {
    let mut buf = [0u8; PATH];
    for (i, &c) in path.iter().enumerate() {
        if (c == b'/' && i > 0) || i + 1 == path.len() {
            let end = if c == b'/' { i } else { i + 1 };
            for (d, &b) in buf.iter_mut().zip(path.iter()) {
                *d = b;
            }
            if later((sys.provider_call)(-1, FS_MKDIR, buf.as_mut_ptr(), end)) {
                return false;
            }
        }
    }
    true
}

/// The platform's mount root and privileges, asked once. False (said once)
/// when the platform has no mount root: no volume of a Pod's own can be
/// made on this node.
unsafe fn platform(s: &mut State, sys: &SyscallTable) -> bool {
    if s.mroot_len == 0 {
        let rc = (sys.provider_call)(-1, hm::ROOT, s.mroot.as_mut_ptr(), s.mroot.len());
        if rc <= 0 || rc as usize >= s.mroot.len() || s.mroot.first() != Some(&b'/') {
            if !s.said_root {
                s.said_root = true;
                let m = b"[volume_files] the platform names no mount root (host_mount ROOT) - no Pod volume can be made";
                dev_log(sys, 1, m.as_ptr(), m.len());
            }
            return false;
        }
        // The root without a trailing `/`.
        let mut n = rc as usize;
        while n > 1 && s.mroot.get(n - 1) == Some(&b'/') {
            n -= 1;
        }
        s.mroot_len = n as u8;
    }
    if s.privs < 0 {
        let p = (sys.provider_call)(-1, hm::PRIV, core::ptr::null_mut(), 0);
        s.privs = p.max(0);
    }
    true
}

/// `bytes` at `buf[at..]`; the end.
fn put_at(buf: &mut [u8], at: usize, bytes: &[u8]) -> Option<usize> {
    let end = at.checked_add(bytes.len())?;
    let dst = buf.get_mut(at..end)?;
    for (d, &c) in dst.iter_mut().zip(bytes) {
        *d = c;
    }
    Some(end)
}

/// The `host_mount` op `op` over `s.hm[..len]`, asked from the next step
/// until it answers; what the PREPARE binds once it does is `s.bind`.
fn hm_begin(s: &mut State, op: u32, len: usize) {
    s.hm_op = op;
    s.hm_len = len;
    s.job = J_HM;
}

/// `GROUP_TREE` of `rel` (relative to the mount root) to the volume's group.
fn group_tree(s: &mut State, v: &Vol, rel: &[u8]) -> Option<()> {
    let group = v.group?;
    let dl = u16::try_from(rel.len()).ok()?;
    let b = &mut s.hm;
    let mut p = put_at(b, 0, &dl.to_le_bytes())?;
    p = put_at(b, p, rel)?;
    p = put_at(b, p, &group.to_le_bytes())?;
    let pol = if v.on_mismatch {
        hm::GT_ON_ROOT_MISMATCH
    } else {
        hm::GT_ALWAYS
    };
    p = put_at(b, p, &[pol])?;
    hm_begin(s, hm::GROUP_TREE, p);
    Some(())
}

/// The held PREPARE's bind: `path`.
fn set_bind(s: &mut State, path: &[u8]) -> Option<()> {
    let dst = s.bind.get_mut(..path.len())?;
    for (d, &c) in dst.iter_mut().zip(path) {
        *d = c;
    }
    s.bind_len = path.len();
    Some(())
}

/// The volume's octal mode digits as a mode.
fn mode_of(m: &[u8; 3]) -> u16 {
    m.iter()
        .fold(0u16, |v, &c| (v << 3) | u16::from(c.wrapping_sub(b'0') & 7))
}

/// The field `name` of a flat `k=v;…` record.
fn flat_field<'a>(rec: &'a [u8], name: &[u8]) -> &'a [u8] {
    for kv in rec.split(|&c| c == b';') {
        if kv.len() > name.len()
            && kv.get(..name.len()) == Some(name)
            && kv.get(name.len()) == Some(&b'=')
        {
            return kv.get(name.len() + 1..).unwrap_or(&[]);
        }
    }
    &[]
}

/// The held PREPARE's spec (field 3) and sandbox id (field 17), copied out.
fn held(s: &State, spec: &mut [u8; SPEC], id: &mut [u8; 160]) -> (usize, usize) {
    let rec = s.rec.get(..n_of(s)).unwrap_or(&[]);
    let a = frame_bytes(rec, 3);
    let b = frame_bytes(rec, 17);
    if a.len() > SPEC || b.len() > 160 {
        return (0, 0);
    }
    for (d, &c) in spec.iter_mut().zip(a) {
        *d = c;
    }
    for (d, &c) in id.iter_mut().zip(b) {
        *d = c;
    }
    (a.len(), b.len())
}

/// The file `rel` of volume `v`: what it holds (into `s.file`) and its mode,
/// for file `i` of the volume. `Ok(None)` past the last; `Err(())` when the
/// volume cannot be projected (a key an item names is missing).
fn file_at(
    s: &mut State,
    v: &Vol,
    i: usize,
    rel: &mut [u8; PATH],
) -> Result<Option<(usize, usize, [u8; 3])>, ()> {
    let secret = v.kind == b's';
    if s.obj_len == 0 {
        // An optional source that is not there: an empty volume.
        return Ok(None);
    }
    let obj = s.obj.get(..s.obj_len).unwrap_or(&[]);
    let mut dm = [0u8; 3];
    for (d, &c) in dm.iter_mut().zip(v.mode) {
        *d = c;
    }
    if !v.items.is_empty() {
        let Some(item) = v.items.split(|&c| c == b'|').nth(i) else {
            return Ok(None);
        };
        let mut parts = item.split(|&c| c == b'=');
        let key = parts.next().unwrap_or(&[]);
        let path = parts.next().unwrap_or(&[]);
        let mode = parts.next().unwrap_or(&[]);
        let mut m = dm;
        if mode.len() == 3 {
            for (d, &c) in m.iter_mut().zip(mode) {
                *d = c;
            }
        }
        let rl = path.len().min(PATH);
        for (d, &c) in rel.iter_mut().zip(path) {
            *d = c;
        }
        let got = value_of(obj, secret, key, &mut s.file, &mut s.tmp);
        return match got {
            Some(n) => Ok(Some((rl, n, m))),
            None if v.optional => Ok(Some((rl, 0, m))),
            None => Err(()),
        };
    }
    // Every key: the data members first, then binaryData / stringData.
    let mut k = 0usize;
    for member in 0..2u8 {
        let mname: &[u8] = match (member, secret) {
            (0, _) => b"data",
            (_, false) => b"binaryData",
            (_, true) => b"stringData",
        };
        let Some(data) = pr_m(obj, mname) else {
            continue;
        };
        let mut j = j_ws(data, 1);
        while data.get(j) == Some(&b'"') {
            let ks = j + 1;
            let mut ke = ks;
            while let Some(&c) = data.get(ke) {
                if c == b'"' {
                    break;
                }
                ke += if c == b'\\' { 2 } else { 1 };
            }
            let key = data.get(ks..ke).unwrap_or(&[]);
            j = j_ws(data, ke + 1);
            if data.get(j) != Some(&b':') {
                break;
            }
            let vs = j_ws(data, j + 1);
            let ve = j_skip(data, vs);
            let keyok = !key.is_empty()
                && key != b"."
                && key != b".."
                && key
                    .iter()
                    .all(|&c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'));
            if keyok {
                if k == i {
                    let raw = j_scalar(data, vs);
                    let b64 = (secret && member == 0) || (!secret && member == 1);
                    let n = if b64 {
                        pr_unescape(raw, &mut s.tmp)
                            .and_then(|m| pr_b64(s.tmp.get(..m)?, &mut s.file))
                    } else {
                        pr_unescape(raw, &mut s.file)
                    };
                    let rl = key.len().min(PATH);
                    for (d, &c) in rel.iter_mut().zip(key) {
                        *d = c;
                    }
                    return n.map(|n| Some((rl, n, dm))).ok_or(());
                }
                k += 1;
            }
            j = j_ws(data, ve);
            if data.get(j) == Some(&b',') {
                j = j_ws(data, j + 1);
            }
        }
    }
    Ok(None)
}

/// One key's value, decoded: a ConfigMap's `data` (or base64 `binaryData`),
/// a Secret's base64 `data` (or `stringData`).
fn value_of(obj: &[u8], secret: bool, key: &[u8], out: &mut [u8], tmp: &mut [u8]) -> Option<usize> {
    if let Some(v) = pr_m(obj, b"data").and_then(|d| pr_m(d, key)) {
        let raw = pr_s(Some(v));
        if secret {
            let m = pr_unescape(raw, tmp)?;
            return pr_b64(tmp.get(..m)?, out);
        }
        return pr_unescape(raw, out);
    }
    let other: &[u8] = if secret { b"stringData" } else { b"binaryData" };
    let v = pr_m(obj, other).and_then(|d| pr_m(d, key))?;
    let raw = pr_s(Some(v));
    if secret {
        pr_unescape(raw, out)
    } else {
        let m = pr_unescape(raw, tmp)?;
        pr_b64(tmp.get(..m)?, out)
    }
}

fn fnv(h: u64, b: &[u8]) -> u64 {
    let mut h = h;
    for &c in b {
        h ^= u64::from(c);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Run the held PREPARE as far as it goes this step.
unsafe fn run(s: &mut State, sys: &SyscallTable) {
    let mut spec = [0u8; SPEC];
    let mut id = [0u8; 160];
    let (sl, il) = held(s, &mut spec, &mut id);
    let Some(v) = parse_vol(spec.get(..sl).unwrap_or(&[]), id.get(..il).unwrap_or(&[])) else {
        return finish(s, sys, false);
    };
    if !platform(s, sys) {
        return finish(s, sys, false);
    }
    let mut rel = [0u8; PATH];
    let Some(rl) = vol_rel(s, &v, &mut rel) else {
        return finish(s, sys, false);
    };
    let rel = rel.get(..rl).unwrap_or(&[]);
    let mut abs = [0u8; PATH];
    let Some(al) = abs_of(s, rel, &[], &mut abs) else {
        return finish(s, sys, false);
    };
    let abs = abs.get(..al).unwrap_or(&[]);
    let ckey = fnv(fnv(0xcbf2_9ce4_8422_2325, v.uid), v.vol);
    match v.kind {
        b'e' => {
            s.digest = fnv(0x0e, spec.get(..sl).unwrap_or(&[]));
            if set_bind(s, abs).is_none() {
                return finish(s, sys, false);
            }
            if cached(s, ckey, s.digest) {
                return finish(s, sys, true);
            }
            if !mkdirs(sys, abs) {
                return;
            }
            if v.group.is_some() {
                // The fsGroup's: group-owned, setgid, group-writable — what
                // every container of the Pod (each holding the fsGroup) may
                // write, as upstream's ownership management leaves it.
                if group_tree(s, &v, rel).is_none() {
                    finish(s, sys, false);
                }
                return;
            }
            // An emptyDir any user may write (mode 0777): the one mode no
            // platform op sets, so through the process executor.
            if !spawn_chmod(s, sys, b"777", abs) {
                finish(s, sys, false);
            }
        }
        b'd' => {
            let mut text = [0u8; 1024];
            let Some(tn) = argv_decode(v.items, &mut text) else {
                return finish(s, sys, false);
            };
            let text = text.get(..tn).unwrap_or(&[]);
            let mut fp = [0u8; PATH];
            let Some(fl) = abs_of(s, rel, b"resolv.conf", &mut fp) else {
                return finish(s, sys, false);
            };
            if set_bind(s, fp.get(..fl).unwrap_or(&[])).is_none() {
                return finish(s, sys, false);
            }
            s.digest = fnv(0xcbf2_9ce4_8422_2325, text);
            if cached(s, ckey, s.digest) {
                return finish(s, sys, true);
            }
            let Some(mut p) = project_head(&mut s.hm, rel, hm::NO_GROUP, 0, 1) else {
                return finish(s, sys, false);
            };
            match project_entry(&mut s.hm, p, b"resolv.conf", 0o644, text) {
                Some(e) => p = e,
                None => return finish(s, sys, false),
            }
            hm_begin(s, hm::PROJECT, p);
        }
        b'g' => claim(s, sys, &v, spec.get(..sl).unwrap_or(&[]), ckey),
        b'w' | b'p' => projected(s, sys, &v, spec.get(..sl).unwrap_or(&[]), rel, abs, ckey),
        _ => source(s, sys, &v, spec.get(..sl).unwrap_or(&[]), rel, abs, ckey),
    }
}

/// A claim the fsGroup owns (`@g`): its directory (the `path` its
/// `/volumes/<ns>/<claim>` record names once bound), given the group before
/// it is bound. A directory outside the mount root (a PersistentVolume's
/// own host directory) is bound as it is, as upstream leaves a hostPath:
/// the platform changes nothing outside its root.
unsafe fn claim(s: &mut State, sys: &SyscallTable, v: &Vol, spec: &[u8], ckey: u64) {
    let st = Store::plain(sys, &s.nag);
    let mut key = [0u8; MAX_KEY];
    let mut kl = 0usize;
    for &c in b"/volumes/"
        .iter()
        .chain(v.ns.iter())
        .chain(b"/".iter())
        .chain(v.name.iter())
    {
        if let Some(d) = key.get_mut(kl) {
            *d = c;
            kl += 1;
        }
    }
    let n = match get_value(&st, key.get(..kl).unwrap_or(&[]), &mut s.obj) {
        Some(n) => n,
        None if st.waiting() => return,
        // Unbound (or refused): nothing to bind yet; the claim's record
        // changing asks again.
        None => return finish(s, sys, false),
    };
    let mut path = [0u8; PATH];
    let pl = {
        let p = flat_field(s.obj.get(..n).unwrap_or(&[]), b"path");
        if p.first() != Some(&b'/') || p.len() > PATH {
            return finish(s, sys, false);
        }
        for (d, &c) in path.iter_mut().zip(p) {
            *d = c;
        }
        p.len()
    };
    let path = path.get(..pl).unwrap_or(&[]);
    if set_bind(s, path).is_none() {
        return finish(s, sys, false);
    }
    s.digest = fnv(fnv(0x67, path), spec);
    if cached(s, ckey, s.digest) {
        return finish(s, sys, true);
    }
    let ml = s.mroot_len as usize;
    let under =
        path.len() > ml + 1 && path.get(..ml) == s.mroot.get(..ml) && path.get(ml) == Some(&b'/');
    if !under {
        if !s.said_outside {
            s.said_outside = true;
            let m = b"[volume_files] a claim outside the mount root keeps its ownership (fsGroup not applied)";
            dev_log(sys, 2, m.as_ptr(), m.len());
        }
        remember(s, ckey, s.digest);
        return finish(s, sys, true);
    }
    let mut rel = [0u8; PATH];
    let rl = path.len() - ml - 1;
    for (d, &c) in rel.iter_mut().zip(path.get(ml + 1..).unwrap_or(&[])) {
        *d = c;
    }
    if group_tree(s, v, rel.get(..rl).unwrap_or(&[])).is_none() {
        finish(s, sys, false);
    }
}

/// A ConfigMap or Secret (`@c`, `@s`): every file it projects, written by
/// the platform as one projection (`PROJECT`, upstream's AtomicWriter
/// layout) — each file at its mode (`defaultMode`, an item's `mode`), the
/// fsGroup's group with group read (and, read-only as these volumes are,
/// `0440` added, as upstream's ownership management does), a Secret on a
/// memory filesystem — swapped in whole for a reader.
unsafe fn source(
    s: &mut State,
    sys: &SyscallTable,
    v: &Vol,
    spec: &[u8],
    rel: &[u8],
    abs: &[u8],
    ckey: u64,
) {
    let st = Store::plain(sys, &s.nag);
    let mut key = [0u8; MAX_KEY];
    let mut kl = 0usize;
    let coll: &[u8] = if v.kind == b's' {
        b"/secrets/"
    } else {
        b"/configmaps/"
    };
    for &c in coll
        .iter()
        .chain(v.ns.iter())
        .chain(b"/".iter())
        .chain(v.name.iter())
    {
        if let Some(d) = key.get_mut(kl) {
            *d = c;
            kl += 1;
        }
    }
    s.obj_len = match get_value(&st, key.get(..kl).unwrap_or(&[]), &mut s.obj) {
        Some(n) => n,
        None if st.waiting() => return,
        // Refused for good (said by the read): neither absent nor empty, so
        // nothing is written, and the volume fails.
        None if st.refusal().is_some() => return finish(s, sys, false),
        None if v.optional => 0,
        None => return finish(s, sys, false),
    };
    if set_bind(s, abs).is_none() {
        return finish(s, sys, false);
    }
    // What the directory should hold: projected only when it changed.
    let digest = fnv(
        fnv(0xcbf2_9ce4_8422_2325, s.obj.get(..s.obj_len).unwrap_or(&[])),
        spec,
    );
    s.digest = digest;
    if cached(s, ckey, digest) {
        return finish(s, sys, true);
    }
    let group = v.group.unwrap_or(hm::NO_GROUP);
    let mut flags = 0u8;
    if v.group.is_some() {
        flags |= hm::P_READ_ONLY;
    }
    if v.kind == b's' {
        if s.privs & hm::PRIV_SYS_ADMIN != 0 {
            flags |= hm::P_MEMORY;
        } else if !s.said_memory {
            s.said_memory = true;
            let m =
                b"[volume_files] no CAP_SYS_ADMIN: Secret volumes are written to disk, not memory";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
    }
    // The count is written once every file is in.
    let Some(head) = project_head(&mut s.hm, rel, group, flags, 0) else {
        return finish(s, sys, false);
    };
    let mut p = head;
    let mut i = 0usize;
    loop {
        let mut path = [0u8; PATH];
        let (pl, n, mode) = match file_at(s, v, i, &mut path) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(()) => return finish(s, sys, false),
        };
        let entry = project_entry(
            &mut s.hm,
            p,
            path.get(..pl).unwrap_or(&[]),
            mode_of(&mode),
            s.file.get(..n).unwrap_or(&[]),
        );
        match entry {
            Some(e) => p = e,
            None => return finish(s, sys, false),
        }
        i += 1;
    }
    let Ok(count) = u16::try_from(i) else {
        return finish(s, sys, false);
    };
    if put_at(&mut s.hm, head - 2, &count.to_le_bytes()).is_none() {
        return finish(s, sys, false);
    }
    hm_begin(s, hm::PROJECT, p);
}

/// A downwardAPI (`@w`) or projected (`@p`) volume: each item's file —
/// a downward-API value pod_resolve rendered (`t~<text>`), or a ConfigMap's
/// or Secret's key (`c~<name>~<key>~<optional>`, `s~…`; no key: every key at
/// its own name), read here — at the item's mode (else the volume's
/// `defaultMode`), written as one projection as [`source`] writes one. A
/// projected volume is on a memory filesystem, as upstream backs it (it may
/// hold a Secret).
unsafe fn projected(
    s: &mut State,
    sys: &SyscallTable,
    v: &Vol,
    spec: &[u8],
    rel: &[u8],
    abs: &[u8],
    ckey: u64,
) {
    if set_bind(s, abs).is_none() {
        return finish(s, sys, false);
    }
    let group = v.group.unwrap_or(hm::NO_GROUP);
    let mut flags = 0u8;
    if v.group.is_some() {
        flags |= hm::P_READ_ONLY;
    }
    if v.kind == b'p' {
        if s.privs & hm::PRIV_SYS_ADMIN != 0 {
            flags |= hm::P_MEMORY;
        } else if !s.said_memory {
            s.said_memory = true;
            let m = b"[volume_files] no CAP_SYS_ADMIN: projected volumes are written to disk, not memory";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
    }
    let Some(head) = project_head(&mut s.hm, rel, group, flags, 0) else {
        return finish(s, sys, false);
    };
    let mut p = head;
    let mut count = 0usize;
    let mut digest = fnv(0xcbf2_9ce4_8422_2325, spec);
    // The object in `s.obj` (its key's hash): read once for its items.
    let mut loaded = 0u64;
    let st = Store::plain(sys, &s.nag);
    let mut dm = [0u8; 3];
    for (d, &c) in dm.iter_mut().zip(v.mode) {
        *d = c;
    }
    for item in v.items.split(|&c| c == b'|') {
        let mut parts = item.splitn(3, |&c| c == b'=');
        let (path, mode, src) = (
            parts.next().unwrap_or(&[]),
            parts.next().unwrap_or(&[]),
            parts.next().unwrap_or(&[]),
        );
        let mut m = dm;
        if mode.len() == 3 && mode.iter().all(|c| (b'0'..=b'7').contains(c)) {
            for (d, &c) in m.iter_mut().zip(mode) {
                *d = c;
            }
        } else if !mode.is_empty() {
            return finish(s, sys, false);
        }
        if let Some(text) = src.strip_prefix(b"t~") {
            let Some(n) = argv_decode(text, &mut s.file) else {
                return finish(s, sys, false);
            };
            match project_entry(
                &mut s.hm,
                p,
                path,
                mode_of(&m),
                s.file.get(..n).unwrap_or(&[]),
            ) {
                Some(e) => p = e,
                None => return finish(s, sys, false),
            }
            count += 1;
            continue;
        }
        // `c~<name>~<key>~<optional>` / `s~…`.
        let mut f = src.split(|&c| c == b'~');
        let (kind, name, key, opt) = (
            f.next().unwrap_or(&[]),
            f.next().unwrap_or(&[]),
            f.next().unwrap_or(&[]),
            f.next().unwrap_or(&[]),
        );
        let kind = match kind {
            b"c" => b'c',
            b"s" => b's',
            _ => return finish(s, sys, false),
        };
        if !name_ok(name) || (key.is_empty() != path.is_empty()) {
            return finish(s, sys, false);
        }
        let mut okey = [0u8; MAX_KEY];
        let mut kl = 0usize;
        let coll: &[u8] = if kind == b's' {
            b"/secrets/"
        } else {
            b"/configmaps/"
        };
        for &c in coll.iter().chain(v.ns).chain(b"/").chain(name) {
            if let Some(d) = okey.get_mut(kl) {
                *d = c;
                kl += 1;
            }
        }
        let okey = okey.get(..kl).unwrap_or(&[]);
        let h = fnv(0xcbf2_9ce4_8422_2325, okey);
        if h != loaded {
            s.obj_len = match get_value(&st, okey, &mut s.obj) {
                Some(n) => n,
                None if st.waiting() => return,
                None if st.refusal().is_some() => return finish(s, sys, false),
                None if opt == b"1" => 0,
                None => return finish(s, sys, false),
            };
            loaded = h;
            digest = fnv(fnv(digest, okey), s.obj.get(..s.obj_len).unwrap_or(&[]));
        }
        // This source's files, as a ConfigMap or Secret volume of one item
        // (or of every key) lays them out.
        let mut one = [0u8; PATH + 64];
        let mut ol = 0usize;
        if !key.is_empty() {
            for &c in key.iter().chain(b"=").chain(path) {
                match one.get_mut(ol) {
                    Some(d) => {
                        *d = c;
                        ol += 1;
                    }
                    None => return finish(s, sys, false),
                }
            }
        }
        let tv = Vol {
            kind,
            vol: v.vol,
            ns: v.ns,
            name,
            mode: &m,
            optional: opt == b"1",
            items: one.get(..ol).unwrap_or(&[]),
            uid: v.uid,
            group: v.group,
            on_mismatch: false,
        };
        let mut i = 0usize;
        loop {
            let mut fpath = [0u8; PATH];
            let (pl, n, fm) = match file_at(s, &tv, i, &mut fpath) {
                Ok(Some(f)) => f,
                Ok(None) => break,
                Err(()) => return finish(s, sys, false),
            };
            let entry = project_entry(
                &mut s.hm,
                p,
                fpath.get(..pl).unwrap_or(&[]),
                mode_of(&fm),
                s.file.get(..n).unwrap_or(&[]),
            );
            match entry {
                Some(e) => p = e,
                None => return finish(s, sys, false),
            }
            count += 1;
            i += 1;
        }
    }
    s.digest = digest;
    if cached(s, ckey, digest) {
        return finish(s, sys, true);
    }
    let Ok(count) = u16::try_from(count) else {
        return finish(s, sys, false);
    };
    if put_at(&mut s.hm, head - 2, &count.to_le_bytes()).is_none() {
        return finish(s, sys, false);
    }
    hm_begin(s, hm::PROJECT, p);
}

/// `PROJECT`'s head at `b[0..]`: `[dir_len:u16][dir][group:u32]
/// [flags:u8][count:u16]`; the end.
fn project_head(b: &mut [u8], rel: &[u8], group: u32, flags: u8, count: u16) -> Option<usize> {
    let dl = u16::try_from(rel.len()).ok()?;
    let mut p = put_at(b, 0, &dl.to_le_bytes())?;
    p = put_at(b, p, rel)?;
    p = put_at(b, p, &group.to_le_bytes())?;
    p = put_at(b, p, &[flags])?;
    put_at(b, p, &count.to_le_bytes())
}

/// One entry `[path_len:u16][path][mode:u16][data_len:u32][data]` at `at`;
/// the end.
fn project_entry(b: &mut [u8], at: usize, path: &[u8], mode: u16, data: &[u8]) -> Option<usize> {
    let pl = u16::try_from(path.len()).ok()?;
    let dl = u32::try_from(data.len()).ok()?;
    let mut p = put_at(b, at, &pl.to_le_bytes())?;
    p = put_at(b, p, path)?;
    p = put_at(b, p, &mode.to_le_bytes())?;
    p = put_at(b, p, &dl.to_le_bytes())?;
    put_at(b, p, data)
}

/// A platform op answered: the PREPARE it served finished (its content
/// remembered when it succeeded), or the sweep's removal counted.
unsafe fn hm_done(s: &mut State, sys: &SyscallTable, rc: i32) {
    if rc != 0 && !s.said_rc {
        s.said_rc = true;
        let mut m = [0u8; 96];
        let mut n = 0usize;
        let mut d = [0u8; 20];
        let k = pm_dec(u64::from(rc.unsigned_abs()), &mut d);
        let mut o = [0u8; 20];
        let ok = pm_dec(u64::from(s.hm_op), &mut o);
        for &c in b"[volume_files] host_mount op "
            .iter()
            .chain(o.get(..ok).unwrap_or(&[]))
            .chain(b" refused: -")
            .chain(d.get(..k).unwrap_or(&[]))
        {
            if let Some(o) = m.get_mut(n) {
                *o = c;
                n += 1;
            }
        }
        dev_log(sys, 1, m.as_ptr(), n);
    }
    if s.job == J_SWEEP {
        s.job = J_IDLE;
        if rc == 0 {
            s.removed = s.removed.wrapping_add(1);
        }
        return;
    }
    prepared(s, sys, rc == 0);
}

/// The held PREPARE's last op ended: on success its content is remembered
/// and its bind offered.
unsafe fn prepared(s: &mut State, sys: &SyscallTable, ok: bool) {
    if ok {
        let mut spec = [0u8; SPEC];
        let mut id = [0u8; 160];
        let (sl, il) = held(s, &mut spec, &mut id);
        if let Some(v) = parse_vol(spec.get(..sl).unwrap_or(&[]), id.get(..il).unwrap_or(&[])) {
            let ckey = fnv(fnv(0xcbf2_9ce4_8422_2325, v.uid), v.vol);
            remember(s, ckey, s.digest);
        }
    }
    finish(s, sys, ok);
}

/// `chmod <mode> <path>` through the process executor.
unsafe fn spawn_chmod(s: &mut State, sys: &SyscallTable, mode: &[u8], path: &[u8]) -> bool {
    let mut cmd = [0u8; CMD];
    let mut n = 0usize;
    for &c in b"chmod "
        .iter()
        .chain(mode.iter())
        .chain(b" ".iter())
        .chain(path.iter())
    {
        match cmd.get_mut(n) {
            Some(d) => {
                *d = c;
                n += 1;
            }
            None => return false,
        }
    }
    let h = (sys.provider_call)(-1, hp::PROC_SPAWN, cmd.as_mut_ptr(), n);
    if h < 0 {
        if !s.said_proc {
            s.said_proc = true;
            let m = b"[volume_files] the process executor refused chmod - add it to SECTOR_PROC_ALLOW (an emptyDir without fsGroup)";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        return false;
    }
    s.proc_h = h;
    s.proc_ok = false;
    s.job = J_CHMOD;
    true
}

/// The running tool, polled. True once it has finished (`proc_ok` its
/// success).
unsafe fn poll_proc(s: &mut State, sys: &SyscallTable) -> bool {
    let _ = (sys.provider_call)(s.proc_h, hp::PROC_READ, s.drain.as_mut_ptr(), s.drain.len());
    let mut code = [0u8; 4];
    let running = (sys.provider_call)(s.proc_h, hp::PROC_STATUS, code.as_mut_ptr(), 4);
    if running == 1 {
        return false;
    }
    let _ = (sys.provider_call)(s.proc_h, hp::PROC_CLOSE, core::ptr::null_mut(), 0);
    s.proc_h = -1;
    s.proc_ok = running == 0 && i32::from_le_bytes(code) == 0;
    true
}

fn cached(s: &State, key: u64, digest: u64) -> bool {
    s.cache.iter().any(|e| e[0] == key && e[1] == digest)
}

fn remember(s: &mut State, key: u64, digest: u64) {
    if let Some(e) = s.cache.iter_mut().find(|e| e[0] == key) {
        e[1] = digest;
        return;
    }
    if let Some(e) = s.cache.get_mut(s.cache_next % CACHE) {
        *e = [key, digest];
    }
    s.cache_next = s.cache_next.wrapping_add(1);
}

/// The held request on to the store connector: a PREPARE as a NOOP carrying
/// its bind (`s.bind`, when `ok`), or nothing when it could not be
/// prepared.
unsafe fn finish(s: &mut State, sys: &SyscallTable, ok: bool) {
    let mut dir = [0u8; PATH];
    let mut dl = 0usize;
    if ok && s.bind_len > 0 {
        dl = s.bind_len.min(PATH);
        for (d, &c) in dir.iter_mut().zip(s.bind.iter().take(dl)) {
            *d = c;
        }
        s.prepared = s.prepared.wrapping_add(1);
    } else {
        s.failed = s.failed.wrapping_add(1);
    }
    s.job = J_IDLE;
    s.bind_len = 0;
    forward(
        s,
        if dl > 0 {
            Some(dir.get(..dl).unwrap_or(&[]))
        } else {
            None
        },
    );
    let _ = sys;
}

/// The held record onto `out`: as it is, or (a PREPARE) as a NOOP with the
/// directory at DIR_FIELD.
fn forward(s: &mut State, dir: Option<&[u8]>) {
    let n = n_of(s);
    let rec = s.rec.get(..n).unwrap_or(&[]);
    let prepare = frame_i64(rec, 2) == Some(OP_PREPARE);
    if !prepare {
        for (d, &c) in s.out.iter_mut().zip(rec) {
            *d = c;
        }
        s.out_len = n;
        s.rec_len = 0;
        return;
    }
    let mut p = 1usize;
    let mut count = 0u8;
    let mut ok = true;
    let count_in = rec.first().copied().unwrap_or(0) as usize;
    let mut q = 1usize;
    for _ in 0..count_in {
        let (Some(&num), Some(&ty), Some(&l0), Some(&l1)) =
            (rec.get(q), rec.get(q + 1), rec.get(q + 2), rec.get(q + 3))
        else {
            break;
        };
        let len = u16::from_le_bytes([l0, l1]) as usize;
        let payload = rec.get(q + 4..q + 4 + len).unwrap_or(&[]);
        q += 4 + len;
        if num == 2 || num == DIR_FIELD {
            continue;
        }
        match put_field(&mut s.out, p, num, ty, payload) {
            Some(e) => {
                p = e;
                count += 1;
            }
            None => ok = false,
        }
    }
    match put_field(&mut s.out, p, 2, TY_I64, &OP_NOOP.to_le_bytes()) {
        Some(e) => {
            p = e;
            count += 1;
        }
        None => ok = false,
    }
    if let Some(d) = dir {
        match put_field(&mut s.out, p, DIR_FIELD, TY_BYTES, d) {
            Some(e) => {
                p = e;
                count += 1;
            }
            None => ok = false,
        }
    }
    if let Some(c) = s.out.get_mut(0) {
        *c = count;
    }
    s.out_len = if ok { p } else { 0 };
    s.rec_len = 0;
}

/// Every `gc_ms`: the root's Pod directories whose Pod record is gone, and
/// one of them removed at a time (`REMOVE_TREE`: what the Pod's volumes
/// held, a Secret's memory filesystem unmounted first; a directory still
/// holding a mounted volume is left whole and asked again next sweep).
/// True while a removal is under way.
unsafe fn sweep(s: &mut State, sys: &SyscallTable) -> bool {
    if s.gone_n > 0 {
        s.gone_n -= 1;
        let i = s.gone_n;
        let name_len = s.gone_len.get(i).copied().unwrap_or(0) as usize;
        let name = s.gone.get(i).copied().unwrap_or([0; 64]);
        let rl = s.root_len as usize;
        let root = s.root;
        let mut p = 0usize;
        for &c in root
            .get(..rl)
            .unwrap_or(&[])
            .iter()
            .chain(b"/".iter())
            .chain(name.get(..name_len).unwrap_or(&[]).iter())
        {
            match s.hm.get_mut(p) {
                Some(d) => {
                    *d = c;
                    p += 1;
                }
                None => return false,
            }
        }
        if name_len == 0 {
            return false;
        }
        s.hm_op = hm::REMOVE_TREE;
        s.hm_len = p;
        s.job = J_SWEEP;
        return true;
    }
    let now = dev_millis(sys);
    if now.wrapping_sub(s.last_gc_ms) < u64::from(s.gc_ms.max(1000)) {
        return false;
    }
    s.last_gc_ms = now;
    if !platform(s, sys) {
        return false;
    }
    let mut root = [0u8; PATH];
    let r = s.root;
    let Some(rl) = abs_of(
        s,
        r.get(..s.root_len as usize).unwrap_or(&[]),
        &[],
        &mut root,
    ) else {
        return false;
    };
    let dh = (sys.provider_call)(-1, FS_OPENDIR, root.as_mut_ptr(), rl);
    if dh < 0 {
        return false;
    }
    let st = Store::plain(sys, &s.nag);
    let mut buf = [0u8; 1024];
    loop {
        let got = (sys.provider_call)(dh, FS_READDIR, buf.as_mut_ptr(), buf.len());
        if got <= 0 {
            break;
        }
        let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        let mut p = 2usize;
        for _ in 0..count {
            let (Some(&nl), Some(&ty)) = (buf.get(p), buf.get(p + 1)) else {
                break;
            };
            let name = buf.get(p + 2..p + 2 + nl as usize).unwrap_or(&[]);
            p += 2 + nl as usize;
            if ty != 1 || !name_ok(name) || s.gone_n >= SWEEP {
                continue;
            }
            let mut key = [0u8; MAX_KEY];
            let mut kl = 0usize;
            for &c in b"/pod-specs/".iter().chain(name.iter()) {
                if let Some(d) = key.get_mut(kl) {
                    *d = c;
                    kl += 1;
                }
            }
            if exists(&st, key.get(..kl).unwrap_or(&[])) || st.stopped() {
                continue;
            }
            if let (Some(g), Some(l)) = (s.gone.get_mut(s.gone_n), s.gone_len.get_mut(s.gone_n)) {
                for (d, &c) in g.iter_mut().zip(name) {
                    *d = c;
                }
                *l = name.len() as u8;
                s.gone_n += 1;
            }
        }
    }
    let mut z = [0u8; 4];
    (sys.provider_call)(dh, FS_CLOSE, z.as_mut_ptr(), 0);
    // A sweep whose reads were not all answered (or were refused) removes
    // nothing: it is taken again next time.
    if st.stopped() {
        s.gone_n = 0;
    }
    false
}

fn put_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    let len = u16::try_from(payload.len()).ok()?;
    let end = at.checked_add(4 + payload.len())?;
    let dst = out.get_mut(at..end)?;
    let le = len.to_le_bytes();
    let head = [number, ty, le[0], le[1]];
    for (d, &c) in dst.iter_mut().zip(head.iter().chain(payload.iter())) {
        *d = c;
    }
    Some(end)
}

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let num = *frame.get(p)?;
        let ty = *frame.get(p + 1)?;
        let len = u16::from_le_bytes([*frame.get(p + 2)?, *frame.get(p + 3)?]) as usize;
        let payload = frame.get(p + 4..p + 4 + len)?;
        if num == number {
            return Some((ty, payload));
        }
        p += 4 + len;
    }
    None
}

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, b)) => b,
        _ => &[],
    }
}

fn frame_i64(frame: &[u8], number: u8) -> Option<i64> {
    match frame_field(frame, number) {
        Some((TY_I64, b)) => <[u8; 8]>::try_from(b).ok().map(i64::from_le_bytes),
        _ => None,
    }
}

fn frame_len(data: &[u8]) -> Option<usize> {
    let count = *data.first()? as usize;
    let mut off = 1usize;
    for _ in 0..count {
        let l = u16::from_le_bytes([*data.get(off + 2)?, *data.get(off + 3)?]) as usize;
        off += 4 + l;
    }
    (off <= data.len()).then_some(off)
}

unsafe fn take(sys: &SyscallTable, chan: i32, out: &mut [u8; REC]) -> Option<usize> {
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
        match frame_len(out.get(..pk as usize).unwrap_or(&[])) {
            Some(fl) => (sys.channel_read)(chan, out.as_mut_ptr(), fl),
            None if (pk as usize) < REC => return None,
            None => {
                (sys.channel_read)(chan, out.as_mut_ptr(), REC);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}

/// `[volume_files] hb prepared=… failed=… removed=…` when they moved.
unsafe fn heartbeat(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys);
    let sum = s.prepared.wrapping_add(s.failed).wrapping_add(s.removed);
    if sum == s.last_hb_sum || now.wrapping_sub(s.last_hb_ms) < 10_000 {
        return;
    }
    s.last_hb_ms = now;
    s.last_hb_sum = sum;
    let mut m = [0u8; 128];
    let mut n = 0usize;
    let mut put = |b: &[u8], n: &mut usize| {
        for &c in b {
            if let Some(d) = m.get_mut(*n) {
                *d = c;
                *n += 1;
            }
        }
    };
    let mut d = [0u8; 20];
    put(b"[volume_files] hb prepared=", &mut n);
    let k = pm_dec(u64::from(s.prepared), &mut d);
    put(d.get(..k).unwrap_or(&[]), &mut n);
    put(b" failed=", &mut n);
    let k = pm_dec(u64::from(s.failed), &mut d);
    put(d.get(..k).unwrap_or(&[]), &mut n);
    put(b" removed=", &mut n);
    let k = pm_dec(u64::from(s.removed), &mut d);
    put(d.get(..k).unwrap_or(&[]), &mut n);
    dev_log(sys, 3, m.as_ptr(), n);
}
