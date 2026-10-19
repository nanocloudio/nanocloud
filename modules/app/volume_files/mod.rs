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
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;

include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/poddns.rs");
include!("../_shared/podres.rs");

const FS_CLOSE: u32 = 0x0903;
const FS_WRITE: u32 = 0x0906;
const FS_OPENDIR: u32 = 0x0907;
const FS_READDIR: u32 = 0x0908;
const FS_OPEN_CREATE: u32 = 0x0909;
const FS_UNLINK: u32 = 0x090A;
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
const PATH: usize = 256;
const CMD: usize = 512;
const OP_NOOP: i64 = 0;
const OP_PREPARE: i64 = 40;
/// The chain's carry field the directory rides back in.
const DIR_FIELD: u8 = 39;
/// Directories whose contents are known written, by (volume, content).
const CACHE: usize = 64;
/// Gone Pods' directories found by one sweep.
const SWEEP: usize = 16;

/// Where a PREPARE is.
const J_IDLE: u8 = 0;
const J_RUN: u8 = 1;
const J_CHMOD: u8 = 2;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    request_out: i32,
    root: [u8; 128],
    root_len: u8,
    gc_ms: u32,
    /// The request held while it is served.
    rec: [u8; REC],
    rec_len: usize,
    /// The record on its way out.
    out: [u8; REC],
    out_len: usize,
    job: u8,
    /// Files whose mode is set (the chmod pass's cursor) and how many
    /// there are; the content's digest, cached once done.
    next_mode: usize,
    files: usize,
    digest: u64,
    proc_h: i32,
    proc_ok: bool,
    obj: [u8; OBJ],
    obj_len: usize,
    file: [u8; FILE],
    tmp: [u8; FILE],
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
    set_defaults(s);
    if !params.is_null() && params_len >= 4 {
        parse_tlv(s, params, params_len);
    }
    if s.root_len == 0 {
        let d = b"/var/lib/nanocloud.io/pods";
        for (o, &c) in s.root.iter_mut().zip(d.iter()) {
            *o = c;
        }
        s.root_len = d.len() as u8;
    }
    if s.root.first() != Some(&b'/') {
        let m = b"[volume_files] root is not an absolute path - REFUSED";
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
    if s.proc_h >= 0 && !poll_proc(s, sys) {
        return;
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
    match s.job {
        J_RUN => run(s, sys),
        J_CHMOD => chmod_next(s, sys),
        _ => {}
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
    match kind {
        b'e' => Some(Vol {
            kind,
            vol,
            ns: &[],
            name: &[],
            mode: &[],
            optional: false,
            items: &[],
            uid,
        }),
        // A file the Pod's spec states whole (its resolv.conf): its text
        // `%XX`-escaped, the rest of the spec.
        b'd' => {
            let at = 3 + vol.len() + 1;
            Some(Vol {
                kind,
                vol,
                ns: &[],
                name: &[],
                mode: &[],
                optional: false,
                items: spec.get(at..).unwrap_or(&[]),
                uid,
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
            // Items run to the end (their paths hold `/`).
            let mut at = 0usize;
            let mut n = 0usize;
            for (i, &c) in spec.iter().enumerate() {
                if c == b'/' {
                    n += 1;
                    if n == 6 {
                        at = i + 1;
                        break;
                    }
                }
            }
            let items = if at == 0 { &[][..] } else { spec.get(at..)? };
            Some(Vol {
                kind,
                vol,
                ns,
                name,
                mode,
                optional: opt == b"1",
                items,
                uid,
            })
        }
        _ => None,
    }
}

/// What a volume binds: its directory, or (`@d`) its one file.
fn target(s: &State, v: &Vol, out: &mut [u8; PATH]) -> Option<usize> {
    if v.kind == b'd' {
        vol_path(s, v, b"resolv.conf", out)
    } else {
        vol_path(s, v, &[], out)
    }
}

/// `root/<uid>/volumes/<vol>[/<rel>]` into `out`.
fn vol_path(s: &State, v: &Vol, rel: &[u8], out: &mut [u8; PATH]) -> Option<usize> {
    let mut n = 0usize;
    let mut put = |b: &[u8], n: &mut usize| -> Option<()> {
        for &c in b {
            *out.get_mut(*n)? = c;
            *n += 1;
        }
        Some(())
    };
    put(s.root.get(..s.root_len as usize)?, &mut n)?;
    put(b"/", &mut n)?;
    put(v.uid, &mut n)?;
    put(b"/volumes/", &mut n)?;
    put(v.vol, &mut n)?;
    if !rel.is_empty() {
        put(b"/", &mut n)?;
        put(rel, &mut n)?;
    }
    Some(n)
}

/// An fs answer that is not yet a decision.
fn later(rc: i32) -> bool {
    rc == EAGAIN || rc == EINPROGRESS
}

/// Every directory from the root down to `path` (inclusive), made. False
/// when the provider has not answered yet; an error is the write's to meet.
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
    let n = n_of(s);
    let mut spec = [0u8; 512];
    let mut id = [0u8; 160];
    let (sl, il) = {
        let rec = s.rec.get(..n).unwrap_or(&[]);
        let a = frame_bytes(rec, 3);
        let b = frame_bytes(rec, 17);
        for (d, &c) in spec.iter_mut().zip(a) {
            *d = c;
        }
        for (d, &c) in id.iter_mut().zip(b) {
            *d = c;
        }
        (a.len().min(512), b.len().min(160))
    };
    let Some(v) = parse_vol(spec.get(..sl).unwrap_or(&[]), id.get(..il).unwrap_or(&[])) else {
        return finish(s, sys, None);
    };
    let mut dir = [0u8; PATH];
    let Some(dl) = vol_path(s, &v, &[], &mut dir) else {
        return finish(s, sys, None);
    };
    if !mkdirs(sys, dir.get(..dl).unwrap_or(&[])) {
        return;
    }
    let ckey = fnv(fnv(0xcbf2_9ce4_8422_2325, v.uid), v.vol);
    if v.kind == b'd' {
        let mut text = [0u8; 1024];
        let Some(tn) = argv_decode(v.items, &mut text) else {
            return finish(s, sys, None);
        };
        let mut fp = [0u8; PATH];
        let Some(fl) = target(s, &v, &mut fp) else {
            return finish(s, sys, None);
        };
        s.digest = fnv(0xcbf2_9ce4_8422_2325, text.get(..tn).unwrap_or(&[]));
        if cached(s, ckey, s.digest) {
            return finish(s, sys, Some(fl));
        }
        match write_file(
            sys,
            fp.get(..fl).unwrap_or(&[]),
            text.get(..tn).unwrap_or(&[]),
        ) {
            Some(true) => {}
            Some(false) => return,
            None => return finish(s, sys, None),
        }
        s.files = 0;
        s.next_mode = 0;
        if !spawn_chmod(s, sys, b"644", fp.get(..fl).unwrap_or(&[])) {
            return finish(s, sys, None);
        }
        s.job = J_CHMOD;
        return;
    }
    if v.kind == b'e' {
        s.digest = 0x0e;
        if cached(s, ckey, s.digest) {
            return finish(s, sys, Some(dl));
        }
        // An emptyDir any user may write.
        s.files = 0;
        s.next_mode = 0;
        if !spawn_chmod(s, sys, b"777", dir.get(..dl).unwrap_or(&[])) {
            return finish(s, sys, None);
        }
        s.job = J_CHMOD;
        return;
    }
    // A ConfigMap or Secret.
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
        None if v.optional => 0,
        None => return finish(s, sys, None),
    };
    // What the directory should hold: written only when it changed.
    let mut digest = fnv(0xcbf2_9ce4_8422_2325, s.obj.get(..s.obj_len).unwrap_or(&[]));
    digest = fnv(digest, spec.get(..sl).unwrap_or(&[]));
    s.digest = digest;
    if cached(s, ckey, digest) {
        return finish(s, sys, Some(dl));
    }
    let mut i = 0usize;
    loop {
        let mut rel = [0u8; PATH];
        let (rl, n, _) = match file_at(s, &v, i, &mut rel) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(()) => return finish(s, sys, None),
        };
        let mut path = [0u8; PATH];
        let Some(pl) = vol_path(s, &v, rel.get(..rl).unwrap_or(&[]), &mut path) else {
            return finish(s, sys, None);
        };
        // An item's own subdirectories.
        if let Some(last) = path
            .get(..pl)
            .and_then(|p| p.iter().rposition(|&c| c == b'/'))
        {
            if last > dl && !mkdirs(sys, path.get(..last).unwrap_or(&[])) {
                return;
            }
        }
        match write_file(
            sys,
            path.get(..pl).unwrap_or(&[]),
            s.file.get(..n).unwrap_or(&[]),
        ) {
            Some(true) => {}
            Some(false) => return,
            None => return finish(s, sys, None),
        }
        i += 1;
    }
    s.files = i;
    s.next_mode = 0;
    s.job = J_CHMOD;
    chmod_next(s, sys);
}

/// `bytes` as the whole of the file at `path`. `Some(false)` when the
/// provider has not answered yet (asked again from the start next step:
/// every write here is idempotent), `None` when it refused.
unsafe fn write_file(sys: &SyscallTable, path: &[u8], bytes: &[u8]) -> Option<bool> {
    let mut p = [0u8; PATH];
    for (d, &c) in p.iter_mut().zip(path) {
        *d = c;
    }
    let rc = (sys.provider_call)(-1, FS_UNLINK, p.as_mut_ptr(), path.len());
    if later(rc) {
        return Some(false);
    }
    let fd = (sys.provider_call)(-1, FS_OPEN_CREATE, p.as_mut_ptr(), path.len());
    if later(fd) {
        return Some(false);
    }
    if fd < 0 {
        return None;
    }
    let mut off = 0usize;
    let mut ok = Some(true);
    while off < bytes.len() {
        let rest = bytes.get(off..).unwrap_or(&[]);
        let w = (sys.provider_call)(fd, FS_WRITE, rest.as_ptr() as *mut u8, rest.len());
        if w > 0 {
            off += w as usize;
        } else if later(w) {
            ok = Some(false);
            break;
        } else {
            ok = None;
            break;
        }
    }
    let mut z = [0u8; 4];
    (sys.provider_call)(fd, FS_CLOSE, z.as_mut_ptr(), 0);
    ok
}

/// Set the next file's mode (one `chmod` each), then finish.
unsafe fn chmod_next(s: &mut State, sys: &SyscallTable) {
    let n = n_of(s);
    let mut spec = [0u8; 512];
    let mut id = [0u8; 160];
    let (sl, il) = {
        let rec = s.rec.get(..n).unwrap_or(&[]);
        let a = frame_bytes(rec, 3);
        let b = frame_bytes(rec, 17);
        for (d, &c) in spec.iter_mut().zip(a) {
            *d = c;
        }
        for (d, &c) in id.iter_mut().zip(b) {
            *d = c;
        }
        (a.len().min(512), b.len().min(160))
    };
    let Some(v) = parse_vol(spec.get(..sl).unwrap_or(&[]), id.get(..il).unwrap_or(&[])) else {
        return finish(s, sys, None);
    };
    let mut dir = [0u8; PATH];
    let Some(dl) = vol_path(s, &v, &[], &mut dir) else {
        return finish(s, sys, None);
    };
    if (v.kind == b'd' || v.kind == b'e' || s.next_mode > 0) && !s.proc_ok {
        return finish(s, sys, None);
    }
    if s.next_mode >= s.files {
        let ckey = fnv(fnv(0xcbf2_9ce4_8422_2325, v.uid), v.vol);
        remember(s, ckey, s.digest);
        let mut t = [0u8; PATH];
        let tl = target(s, &v, &mut t).unwrap_or(dl);
        return finish(s, sys, Some(tl));
    }
    let mut rel = [0u8; PATH];
    let (rl, _, mode) = match file_at(s, &v, s.next_mode, &mut rel) {
        Ok(Some(f)) => f,
        _ => return finish(s, sys, None),
    };
    let mut path = [0u8; PATH];
    let Some(pl) = vol_path(s, &v, rel.get(..rl).unwrap_or(&[]), &mut path) else {
        return finish(s, sys, None);
    };
    s.next_mode += 1;
    if !spawn_chmod(s, sys, &mode, path.get(..pl).unwrap_or(&[])) {
        finish(s, sys, None);
    }
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
    spawn(s, sys, &mut cmd, n)
}

unsafe fn spawn(s: &mut State, sys: &SyscallTable, cmd: &mut [u8; CMD], n: usize) -> bool {
    let h = (sys.provider_call)(-1, hp::PROC_SPAWN, cmd.as_mut_ptr(), n);
    if h < 0 {
        if !s.said_proc {
            s.said_proc = true;
            let m = b"[volume_files] the process executor refused chmod/rm - add them to SECTOR_PROC_ALLOW";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        return false;
    }
    s.proc_h = h;
    s.proc_ok = false;
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
    if !s.proc_ok && s.job == J_CHMOD {
        s.next_mode = usize::MAX;
    }
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
/// its directory (`dir_len` of `root/…`), or nothing when it could not be
/// prepared.
unsafe fn finish(s: &mut State, sys: &SyscallTable, dir_len: Option<usize>) {
    let n = n_of(s);
    let mut dir = [0u8; PATH];
    let mut dl = 0usize;
    if let Some(l) = dir_len {
        let rec = s.rec.get(..n).unwrap_or(&[]);
        let mut spec = [0u8; 512];
        let mut id = [0u8; 160];
        let a = frame_bytes(rec, 3);
        let b = frame_bytes(rec, 17);
        for (d, &c) in spec.iter_mut().zip(a) {
            *d = c;
        }
        for (d, &c) in id.iter_mut().zip(b) {
            *d = c;
        }
        if let Some(v) = parse_vol(
            spec.get(..a.len().min(512)).unwrap_or(&[]),
            id.get(..b.len().min(160)).unwrap_or(&[]),
        ) {
            if target(s, &v, &mut dir) == Some(l) {
                dl = l;
            }
        }
        s.prepared = s.prepared.wrapping_add(1);
    } else {
        s.failed = s.failed.wrapping_add(1);
    }
    s.job = J_IDLE;
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
/// one of them removed a step. True while a removal is under way.
unsafe fn sweep(s: &mut State, sys: &SyscallTable) -> bool {
    if s.gone_n > 0 {
        s.gone_n -= 1;
        let i = s.gone_n;
        let name_len = s.gone_len.get(i).copied().unwrap_or(0) as usize;
        let mut cmd = [0u8; CMD];
        let mut n = 0usize;
        let name = s.gone.get(i).copied().unwrap_or([0; 64]);
        let root = s.root;
        for &c in b"rm -rf "
            .iter()
            .chain(root.get(..s.root_len as usize).unwrap_or(&[]).iter())
            .chain(b"/".iter())
            .chain(name.get(..name_len).unwrap_or(&[]).iter())
        {
            if let Some(d) = cmd.get_mut(n) {
                *d = c;
                n += 1;
            }
        }
        if name_len > 0 && spawn(s, sys, &mut cmd, n) {
            s.removed = s.removed.wrapping_add(1);
            return true;
        }
        return false;
    }
    let now = dev_millis(sys);
    if now.wrapping_sub(s.last_gc_ms) < u64::from(s.gc_ms.max(1000)) {
        return false;
    }
    s.last_gc_ms = now;
    let mut root = [0u8; PATH];
    let rl = s.root_len as usize;
    for (d, &c) in root.iter_mut().zip(s.root.iter().take(rl)) {
        *d = c;
    }
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
            if exists(&st, key.get(..kl).unwrap_or(&[])) || st.waiting() {
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
    // A sweep whose reads were not all answered is taken again next time.
    if st.waiting() {
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
