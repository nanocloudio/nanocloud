//! Image fetcher — the pull EFFECT as a PIC module. Pulling an image is one
//! algorithm — manifest → missing digests → fetch → verify → assemble —
//! differing only in the assemble backend. `image_puller` computes the *plan*
//! (the missing-layer decision); this module performs the fetch: it speaks
//! HTTP/1.1 plus the OCI distribution API on its net pair (wire it through
//! `tls` in client mode for a CA-pinned registry, or straight to `linux_net`
//! for plaintext), digest-verifies every blob, and lands the bytes as
//! content-addressed files in the node blob cache via the `fs` contract. The
//! store carries only markers, never blob bytes.
//!
//! Data model (extends the image_puller chain):
//!   /image-requests/<name>  = "repo=<repo>;tag=<tag>"        (what to pull; seeded by the pod path / CLI)
//!   /image-manifests/<name> = "layers=<hex64>,..;sizes=<n>,..;config=<hex64>;configsize=<n>"
//!                                                            (written here after the manifest GET; image_puller reads `layers=` only)
//!   /image-config/<name>    = "entrypoint=..;cmd=..;env=..;workdir=..;user=.."
//!                                                            (the RUNTIME contract, out of the image config blob)
//!   /image-pull-plan/<name> = "pull=<hex64>,..."             (image_puller's missing set — this module's work queue)
//!   /blobs/<hex64>          = "size=<n>"                     (marker: the blob file is on disk, digest-verified)
//!   <blob_dir>/<hex64>                                        (the bytes, via fs OPEN_CREATE/WRITE/FSYNC)
//!
//! Loop: watch /image-requests/ + /image-manifests/ + /image-pull-plan/. A
//! request without a manifest triggers a manifest GET; a *tag* on a real
//! registry answers with an image INDEX, so a document carrying `manifests`
//! instead of `layers` selects the entry matching the `arch`/`os` params and
//! re-GETs by that digest (one hop, then it must be an image manifest). A
//! manifest whose config descriptor has no /image-config/ record triggers a
//! config-blob GET — same blob endpoint, same digest verification, but the
//! bytes are JSON parsed in memory, never a cached layer and never in the pull
//! plan (the assembler untars everything the plan names). A plan with a
//! non-empty `pull=` triggers a layer GET (optionally in bounded Range chunks
//! — `chunk_bytes` — the posture the pi5 rig converges with). One request in
//! flight at a time, one connection per request (`Connection: close`),
//! exponential backoff on failure. A settled image is: config recorded, plan
//! `pull=` empty, every layer a verified file in the cache.
//!
//! Not yet handled: a registry that answers 401 with a `WWW-Authenticate`
//! bearer challenge — the token exchange is the next layer of this module.

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
use abi::contracts::net::net_proto::{
    write_connect_to, Target, CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CONNECT_TO_MAX,
    REQUESTER_TAG_NONE,
};
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");

// ---- store ops (storage.object 0x14 / storage.namespace 0x13) ----
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;

// ---- fs contract (contracts/storage/fs.rs) ----
const FS_CLOSE: u32 = 0x0903;
const FS_FSYNC: u32 = 0x0905;
const FS_WRITE: u32 = 0x0906;
const FS_OPEN_CREATE: u32 = 0x0909;
const FS_UNLINK: u32 = 0x090A;
const FS_MKDIR: u32 = 0x090B;

// ---- net_proto vocabulary (contracts/net/net_proto.rs) ----
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;

/// Input-port kind for `dev_channel_port` (resolving the changes port).
const PORT_INPUT: u8 = 0;

const REQUESTS_PREFIX: &[u8] = b"/image-requests/";
const MANIFESTS_PREFIX: &[u8] = b"/image-manifests/";
const PLAN_PREFIX: &[u8] = b"/image-pull-plan/";
const BLOBS_PREFIX: &[u8] = b"/blobs/";
const CONFIG_PREFIX: &[u8] = b"/image-config/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const MAX_NAME: usize = 64;
const MAX_REPO: usize = 64;
const MAX_TAG: usize = 32;
/// A manifest reference is either a tag or `sha256:<hex64>` (71 bytes).
const MAX_REF: usize = 72;
/// `platform.architecture` / `platform.os` match tokens.
const MAX_PLAT: usize = 16;
/// The registry authority, `host[:port]`. A longer one is refused at
/// construction: a prefix of a name is a different host.
const MAX_AUTHORITY: usize = 64;
/// The port an authority that names none is dialled on.
const REGISTRY_PORT: u16 = 5000;
const MAX_DIR: usize = 96;
const MAX_LAYERS: usize = 32;

/// One MSG_DATA fragment + conn id + frame header.
const NET_BUF_SIZE: usize = 3 + 2 + 8192;
const HDR_BUF_SIZE: usize = 2048;
/// Whole-manifest buffer — also the image-config JSON landing zone, which is
/// the larger of the two (a real image config carries a `history` array).
const MANIFEST_BUF_SIZE: usize = 32768;
const TX_BUF_SIZE: usize = 512;

const CONNECT_TIMEOUT_MS: u64 = 10_000;
const RESPONSE_TIMEOUT_MS: u64 = 30_000;
const BACKOFF_INIT_MS: u64 = 2_000;
const BACKOFF_MAX_MS: u64 = 60_000;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum Phase {
    /// Waiting out the boot delay, then a cold-start scan.
    Init = 0,
    /// CMD_CONNECT_TO queued for the current request.
    Connecting = 1,
    /// Waiting for MSG_CONNECTED with our requester tag.
    WaitConnect = 2,
    /// Request bytes queued; response header not complete yet.
    RecvHeader = 3,
    /// Response body streaming (manifest buffer or blob file).
    RecvBody = 4,
    /// No fetch in flight; scans run on store changes.
    Idle = 5,
    /// Waiting out a failure backoff, then rescan.
    Backoff = 6,
}

const FETCH_MANIFEST: u8 = 0;
const FETCH_BLOB: u8 = 1;
/// The image *config* blob: fetched over the blob endpoint, but buffered and
/// parsed like a manifest (it is JSON, not a tar layer) and never planned by
/// `image_puller` — the layer plan must stay layers-only or the assembler
/// would try to untar it.
const FETCH_CONFIG: u8 = 2;

/// `manifest_complete` outcomes.
const MC_FAIL: u8 = 0;
const MC_DONE: u8 = 1;
const MC_INDEX: u8 = 2;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here);
    /// input port index 1, allocated by the graph's status→changes self-edge.
    sink: i32,
    resolved: u8,
    subscribed: u8,
    /// A store change landed while a fetch was in flight — rescan at Idle.
    dirty: u8,

    // Params.
    /// The registry as configured, `host[:port]`: the `CMD_CONNECT_TO`
    /// target and, verbatim, the HTTP `Host:` header.
    authority: [u8; MAX_AUTHORITY],
    authority_len: u8,
    /// The configured authority was longer than the buffer; construction
    /// refuses rather than dial a prefix of the name.
    authority_over: u8,
    /// The authority's port, or `REGISTRY_PORT` when it names none.
    port: u16,
    chunk_bytes: u32,
    boot_delay_ms: u32,
    blob_dir: [u8; MAX_DIR],
    blob_dir_len: u8,
    /// Platform selector applied to a manifest index.
    arch: [u8; MAX_PLAT],
    arch_len: u8,
    os: [u8; MAX_PLAT],
    os_len: u8,

    phase: Phase,
    fetching: u8,
    conn_id: u16,
    prev_conn_id: u16,
    conn_present: u8,

    state_start_ms: u64,
    backoff_ms: u64,

    // The image being worked (request name + repo/tag).
    name: [u8; MAX_NAME],
    name_len: u8,
    repo: [u8; MAX_REPO],
    repo_len: u8,
    tag: [u8; MAX_TAG],
    tag_len: u8,
    /// The reference the manifest GET actually asks for: the tag on the first
    /// hop, then `sha256:<hex64>` after an index selects a platform manifest.
    mref: [u8; MAX_REF],
    mref_len: u8,
    /// Index hops taken for the current image (bounded at one — an index
    /// pointing at another index is not a shape any registry produces).
    index_hops: u8,

    // The blob being fetched.
    blob_hex: [u8; 64],
    blob_size: u32,
    blob_pos: u32,
    file_fd: i32,
    hasher: Sha256,

    // HTTP response parsing.
    hdr_fill: u32,
    content_length: u32,
    body_received: u32,
    manifest_fill: u32,

    // TX request in flight.
    tx_len: u16,
    tx_sent: u16,

    hdr_buf: [u8; HDR_BUF_SIZE],
    manifest_buf: [u8; MANIFEST_BUF_SIZE],
    tx_buf: [u8; TX_BUF_SIZE],
    net_buf: [u8; NET_BUF_SIZE],
    scratch: [u8; MAX_VALUE],
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::SCHEMA_MAX;
    use super::{MAX_AUTHORITY, MAX_DIR, MAX_PLAT};

    define_params! {
        State;

        // Tags 1, 2 and 3 are retired.
        9, authority, str, 0
            => |s, d, len| {
                if len > MAX_AUTHORITY {
                    s.authority_over = 1;
                    return;
                }
                s.authority_len = len as u8;
                if len > 0 { ptr_copy(s.authority.as_mut_ptr(), d, len); }
            };

        4, blob_dir, str, 0
            => |s, d, len| {
                let n = if len > MAX_DIR { MAX_DIR } else { len };
                s.blob_dir_len = n as u8;
                if n > 0 { ptr_copy(s.blob_dir.as_mut_ptr(), d, n); }
            };

        5, chunk_bytes, u32, 0
            => |s, d, len| { s.chunk_bytes = p_u32(d, len, 0, 0); };

        6, boot_delay_ms, u32, 500
            => |s, d, len| { s.boot_delay_ms = p_u32(d, len, 0, 500); };

        7, arch, str, 0
            => |s, d, len| {
                let n = if len > MAX_PLAT { MAX_PLAT } else { len };
                s.arch_len = n as u8;
                if n > 0 { ptr_copy(s.arch.as_mut_ptr(), d, n); }
            };

        8, os, str, 0
            => |s, d, len| {
                let n = if len > MAX_PLAT { MAX_PLAT } else { len };
                s.os_len = n as u8;
                if n > 0 { ptr_copy(s.os.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

// ---- storage.object / storage.namespace helpers (house pattern) ----

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    let mut i = from;
    while i + needle.len() <= hay.len() {
        if &hay[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_dec(b: &[u8], at: usize) -> (u64, usize) {
    let mut v: u64 = 0;
    let mut n = 0usize;
    let mut i = at;
    while i < b.len() && b[i].is_ascii_digit() {
        v = v.wrapping_mul(10).wrapping_add((b[i] - b'0') as u64);
        i += 1;
        n += 1;
    }
    (v, n)
}

fn write_dec(dst: &mut [u8], at: usize, mut v: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut d = 0;
    loop {
        digits[d] = b'0' + (v % 10) as u8;
        v /= 10;
        d += 1;
        if v == 0 {
            break;
        }
    }
    let mut at = at;
    while d > 0 {
        d -= 1;
        at = append(dst, at, &digits[d..d + 1]);
    }
    at
}

unsafe fn log_msg(s: &State, msg: &[u8]) {
    let sys = &*s.syscalls;
    dev_log(sys, 3, msg.as_ptr(), msg.len());
}

unsafe fn log_err(s: &State, msg: &[u8]) {
    let sys = &*s.syscalls;
    dev_log(sys, 2, msg.as_ptr(), msg.len());
}

/// The registry as configured: what is dialled and what the `Host:` header
/// carries are the same bytes.
fn authority(s: &State) -> &[u8] {
    &s.authority[..s.authority_len as usize]
}

/// Compose the registry dial into `buf`, answering its length. A name
/// travels as a name for the network provider to resolve, a literal as its
/// address, on the authority's port or `REGISTRY_PORT`.
fn connect_record(s: &State, buf: &mut [u8], tag: u8) -> usize {
    let Some((target, _)) = Target::parse(authority(s)) else {
        return 0;
    };
    write_connect_to(buf, SOCK_TYPE_STREAM, s.port, &target, Some(tag))
}

// ---- fetch machine ----

unsafe fn enter_backoff(s: &mut State) {
    let sys = &*s.syscalls;
    if s.conn_present == 1 {
        let mut payload = [0u8; 2];
        payload[..2].copy_from_slice(&s.conn_id.to_le_bytes());
        let _ = net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            payload.as_ptr(),
            2,
            s.net_buf.as_mut_ptr(),
            NET_BUF_SIZE,
        );
        s.prev_conn_id = s.conn_id;
        s.conn_present = 0;
    }
    close_blob_file(s, true);
    s.backoff_ms = if s.backoff_ms == 0 {
        BACKOFF_INIT_MS
    } else {
        (s.backoff_ms * 2).min(BACKOFF_MAX_MS)
    };
    s.state_start_ms = dev_millis(&*s.syscalls);
    s.phase = Phase::Backoff;
}

/// Close (and on `discard` also unlink) the in-progress blob file.
unsafe fn close_blob_file(s: &mut State, discard: bool) {
    let sys = &*s.syscalls;
    if s.file_fd >= 0 {
        let mut z = [0u8; 4];
        (sys.provider_call)(s.file_fd, FS_CLOSE, z.as_mut_ptr(), 0);
        s.file_fd = -1;
        if discard {
            let mut path = [0u8; MAX_DIR + 1 + 64];
            let plen = blob_path(s, &mut path);
            let _ = (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), plen);
        }
    }
}

/// `<blob_dir>/<hex64>` into `out`; returns the length.
unsafe fn blob_path(s: &State, out: &mut [u8]) -> usize {
    let mut p = append(out, 0, &s.blob_dir[..s.blob_dir_len as usize]);
    p = append(out, p, b"/");
    p = append(out, p, &s.blob_hex);
    p
}

unsafe fn start_request(s: &mut State, fetching: u8) {
    s.fetching = fetching;
    s.hdr_fill = 0;
    s.content_length = 0;
    s.body_received = 0;
    s.tx_len = 0;
    s.tx_sent = 0;
    s.conn_present = 0;
    if fetching != FETCH_BLOB {
        s.manifest_fill = 0; // manifest and config both land in manifest_buf
    }
    s.phase = Phase::Connecting;
}

/// Build the GET for the current fetch into tx_buf.
unsafe fn build_request(s: &mut State) -> usize {
    let mut buf = [0u8; TX_BUF_SIZE];
    let mut o = 0usize;
    o = append(&mut buf, o, b"GET /v2/");
    o = append(&mut buf, o, &s.repo[..s.repo_len as usize]);
    if s.fetching == FETCH_MANIFEST {
        o = append(&mut buf, o, b"/manifests/");
        o = append(&mut buf, o, &s.mref[..s.mref_len as usize]);
    } else {
        o = append(&mut buf, o, b"/blobs/sha256:");
        o = append(&mut buf, o, &s.blob_hex);
    }
    o = append(&mut buf, o, b" HTTP/1.1\r\nHost: ");
    o = append(&mut buf, o, authority(s));
    if s.fetching == FETCH_MANIFEST {
        // Both manifest flavours AND both index flavours: a real registry
        // answers a tag with an index (the multi-arch shape), and only offers
        // the platform manifest behind the digest the index names.
        o = append(
            &mut buf,
            o,
            b"\r\nAccept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json",
        );
    } else if s.fetching == FETCH_BLOB && (s.blob_pos > 0 || s.chunk_bytes > 0) {
        // Bounded Range chunk within the blob (resume + constrained bearers).
        o = append(&mut buf, o, b"\r\nRange: bytes=");
        o = write_dec(&mut buf, o, s.blob_pos as u64);
        o = append(&mut buf, o, b"-");
        if s.chunk_bytes > 0 {
            let end = (s.blob_pos + s.chunk_bytes).min(s.blob_size) - 1;
            o = write_dec(&mut buf, o, end as u64);
        }
    }
    o = append(&mut buf, o, b"\r\nConnection: close\r\n\r\n");
    s.tx_buf[..o].copy_from_slice(&buf[..o]);
    o
}

/// Flush pending request bytes as CMD_SEND frames.
unsafe fn flush_tx(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    while s.tx_sent < s.tx_len {
        let remaining = (s.tx_len - s.tx_sent) as usize;
        let chunk = remaining.min(1024);
        let mut payload = [0u8; 2 + 1024];
        payload[..2].copy_from_slice(&s.conn_id.to_le_bytes());
        payload[2..2 + chunk]
            .copy_from_slice(&s.tx_buf[s.tx_sent as usize..s.tx_sent as usize + chunk]);
        let wrote = net_write_frame(
            sys,
            s.net_out,
            NET_CMD_SEND,
            payload.as_ptr(),
            2 + chunk,
            s.net_buf.as_mut_ptr(),
            NET_BUF_SIZE,
        );
        if wrote == 0 {
            return false;
        }
        s.tx_sent += chunk as u16;
    }
    true
}

unsafe fn parse_response_header(s: &mut State, header_end: usize) -> bool {
    let h = &s.hdr_buf[..header_end];
    let (status, _) = if h.len() > 9 { parse_dec(h, 9) } else { (0, 0) };
    let ranged = s.fetching == FETCH_BLOB && (s.blob_pos > 0 || s.chunk_bytes > 0);
    let ok_status = if ranged { 206 } else { 200 };
    if status != ok_status {
        // A server ignoring Range answers 200 with the full body; restart
        // the blob from zero rather than mis-append.
        if ranged && status == 200 && s.blob_pos == 0 {
            // Full body from the start — acceptable, just no Range.
        } else {
            log_err(s, b"[image_fetcher] http status not ok");
            return false;
        }
    }
    let mut cl: u64 = 0;
    let mut have_cl = false;
    let mut i = 0;
    while i + 16 <= h.len() {
        if h[i] == b'\n' {
            let line = &h[i + 1..];
            if line.len() >= 15 && line[..15].eq_ignore_ascii_case(b"content-length:") {
                let mut j = 15;
                while j < line.len() && line[j] == b' ' {
                    j += 1;
                }
                let (v, n) = parse_dec(line, j);
                if n > 0 {
                    cl = v;
                    have_cl = true;
                }
            }
            if line.len() >= 18 && line[..18].eq_ignore_ascii_case(b"transfer-encoding:") {
                log_err(s, b"[image_fetcher] chunked response unsupported");
                return false;
            }
        }
        i += 1;
    }
    if !have_cl {
        log_err(s, b"[image_fetcher] response missing content-length");
        return false;
    }
    if s.fetching == FETCH_MANIFEST {
        if cl == 0 || cl > MANIFEST_BUF_SIZE as u64 {
            log_err(s, b"[image_fetcher] manifest too large");
            return false;
        }
    } else if s.fetching == FETCH_CONFIG {
        if cl == 0 || cl > MANIFEST_BUF_SIZE as u64 {
            log_err(s, b"[image_fetcher] image config too large");
            return false;
        }
        if cl != s.blob_size as u64 {
            log_err(s, b"[image_fetcher] config size mismatch vs manifest");
            return false;
        }
    } else {
        let mut expect = s.blob_size - s.blob_pos;
        if s.chunk_bytes > 0 && s.chunk_bytes < expect {
            expect = s.chunk_bytes;
        }
        if cl != expect as u64 {
            log_err(s, b"[image_fetcher] blob size mismatch vs manifest");
            return false;
        }
    }
    s.content_length = cl as u32;
    true
}

/// Feed body bytes into the manifest buffer or the blob file + hasher.
unsafe fn body_bytes(s: &mut State, data: &[u8]) -> bool {
    if data.is_empty() {
        return true;
    }
    if s.fetching != FETCH_BLOB {
        let fill = s.manifest_fill as usize;
        if fill + data.len() > MANIFEST_BUF_SIZE {
            log_err(s, b"[image_fetcher] json body overran the buffer");
            return false;
        }
        s.manifest_buf[fill..fill + data.len()].copy_from_slice(data);
        s.manifest_fill += data.len() as u32;
        // The config blob is content-addressed like any other: hash it as it
        // streams so `config_complete` can verify the digest the manifest named.
        if s.fetching == FETCH_CONFIG {
            s.hasher.update(data);
        }
    } else {
        let sys = &*s.syscalls;
        if s.file_fd < 0 {
            return false;
        }
        s.hasher.update(data);
        let mut off = 0usize;
        while off < data.len() {
            let chunk = data.len() - off;
            let wrote = (sys.provider_call)(
                s.file_fd,
                FS_WRITE,
                data.as_ptr().add(off) as *mut u8,
                chunk,
            );
            if wrote <= 0 {
                log_err(s, b"[image_fetcher] blob file write failed");
                return false;
            }
            off += wrote as usize;
        }
        s.blob_pos += data.len() as u32;
    }
    s.body_received += data.len() as u32;
    true
}

/// Lowercase hex of the running hash, without consuming it.
fn hasher_hex(h: &Sha256) -> [u8; 64] {
    let digest = h.clone().finalize();
    let mut hex = [0u8; 64];
    let mut i = 0;
    while i < 32 {
        const H: &[u8; 16] = b"0123456789abcdef";
        hex[i * 2] = H[(digest[i] >> 4) as usize];
        hex[i * 2 + 1] = H[(digest[i] & 0x0F) as usize];
        i += 1;
    }
    hex
}

/// Append `src` to `dst` with the three bytes that would break the record
/// grammar escaped: `%` (the escape itself), `,` (list separator) and `;`
/// (field separator). Image config values are arbitrary shell strings —
/// `CMD ["nginx", "-g", "daemon off;"]` is the normal case, not a corner one.
fn append_esc(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let mut p = at;
    for &b in src {
        p = match b {
            b'%' => append(dst, p, b"%25"),
            b',' => append(dst, p, b"%2C"),
            b';' => append(dst, p, b"%3B"),
            _ => append(dst, p, &[b]),
        };
    }
    p
}

/// Write the `,`-joined, escaped elements of the JSON string array at
/// `obj.<key>` into `doc` under `tag`. No-op (returns `at`) when the key is
/// absent or empty — an image config omits what it does not set.
fn append_json_list(doc: &mut [u8], at: usize, obj: &[u8], key: &[u8], tag: &[u8]) -> usize {
    let Some(arr) = j_sub1(obj, key) else {
        return at;
    };
    let Some(first) = j_idx(arr, 0) else {
        return at;
    };
    let mut p = append(doc, at, tag);
    p = append_esc(doc, p, j_scalar(first, 0));
    let mut i = 1usize;
    while let Some(elem) = j_idx(arr, i) {
        p = append(doc, p, b",");
        p = append_esc(doc, p, j_scalar(elem, 0));
        i += 1;
    }
    p
}

/// Select the platform manifest out of an image index / manifest list: the
/// first entry whose `platform.architecture` + `platform.os` match ours. On a
/// hit, `s.mref` becomes that entry's `sha256:<hex64>` digest so the next
/// manifest GET fetches the real thing. Returns false when no entry matches.
unsafe fn index_select(s: &mut State, list: &[u8]) -> bool {
    let arch = &s.arch[..s.arch_len as usize];
    let os = &s.os[..s.os_len as usize];
    let mut i = 0usize;
    while let Some(elem) = j_idx(list, i) {
        i += 1;
        // An entry with no platform block is unusable for selection (an
        // attestation manifest in a buildkit index looks like this); skip it.
        let Some(a) = j_get2(elem, b"platform", b"architecture") else {
            continue;
        };
        let Some(o) = j_get2(elem, b"platform", b"os") else {
            continue;
        };
        if a != arch || o != os {
            continue;
        }
        let Some(digest) = j_get1(elem, b"digest") else {
            continue;
        };
        if digest.len() != 7 + 64 || &digest[..7] != b"sha256:" {
            continue;
        }
        s.mref_len = digest.len() as u8;
        s.mref[..digest.len()].copy_from_slice(digest);
        return true;
    }
    false
}

/// Parse the docker/OCI image manifest JSON in manifest_buf and write
/// `/image-manifests/<name> = layers=<hex>,..;sizes=<n>,..;config=<hex>;configsize=<n>`.
///
/// Returns `MC_INDEX` when the document was an image index rather than an image
/// manifest — the caller re-issues the manifest GET against the selected
/// platform digest.
unsafe fn manifest_complete(s: &mut State) -> u8 {
    let sys = &*s.syscalls;
    let m_len = s.manifest_fill as usize;
    // Borrow-split: copy the manifest to a local view via raw parts (the
    // buffer is state-resident; we only read it).
    let m = core::slice::from_raw_parts(s.manifest_buf.as_ptr(), m_len);
    let Some(layers) = j_sub1(m, b"layers") else {
        // No `layers` — an image index (`manifests`) is the other legal shape,
        // and the one every multi-arch tag actually returns.
        if let Some(list) = j_sub1(m, b"manifests") {
            if s.index_hops > 0 {
                log_err(s, b"[image_fetcher] image index nested past one hop");
                return MC_FAIL;
            }
            if !index_select(s, list) {
                log_err(s, b"[image_fetcher] no index entry for this platform");
                return MC_FAIL;
            }
            s.index_hops += 1;
            log_msg(s, b"[image_fetcher] index resolved to a platform manifest");
            return MC_INDEX;
        }
        log_err(s, b"[image_fetcher] manifest has no layers array");
        return MC_FAIL;
    };
    let mut doc = [0u8; MAX_VALUE];
    let mut p = append(&mut doc, 0, b"layers=");
    let mut sizes = [0u8; 512];
    let mut sp = 0usize;
    let mut count = 0usize;
    let mut i = 0usize;
    while i < MAX_LAYERS {
        let Some(elem) = j_idx(layers, i) else { break };
        let Some(digest) = j_get1(elem, b"digest") else {
            log_err(s, b"[image_fetcher] layer missing digest");
            return MC_FAIL;
        };
        // "sha256:<hex64>" → bare hex64.
        if digest.len() != 7 + 64 || &digest[..7] != b"sha256:" {
            log_err(s, b"[image_fetcher] unsupported layer digest algo");
            return MC_FAIL;
        }
        let size = match j_get1(elem, b"size") {
            Some(v) => parse_dec(v, 0).0,
            None => 0,
        };
        if count > 0 {
            p = append(&mut doc, p, b",");
            sp = append(&mut sizes, sp, b",");
        }
        p = append(&mut doc, p, &digest[7..]);
        sp = write_dec(&mut sizes, sp, size);
        count += 1;
        i += 1;
    }
    if count == 0 {
        log_err(s, b"[image_fetcher] manifest has zero layers");
        return MC_FAIL;
    }
    p = append(&mut doc, p, b";sizes=");
    p = append(&mut doc, p, &sizes[..sp]);
    // The config descriptor: recorded here, fetched on the next scan pass. It
    // is NOT added to `layers=` — `image_puller` plans that list and
    // `image_assembler` untars every entry of it.
    if let Some(cfg) = j_sub1(m, b"config") {
        if let Some(digest) = j_get1(cfg, b"digest") {
            let size = j_get1(cfg, b"size").map(|v| parse_dec(v, 0).0).unwrap_or(0);
            if digest.len() == 7 + 64 && &digest[..7] == b"sha256:" && size > 0 {
                p = append(&mut doc, p, b";config=");
                p = append(&mut doc, p, &digest[7..]);
                p = append(&mut doc, p, b";configsize=");
                p = write_dec(&mut doc, p, size);
            }
        }
    }
    let mut key = [0u8; MAX_KEY];
    let mut kl = append(&mut key, 0, MANIFESTS_PREFIX);
    kl = append(&mut key, kl, &s.name[..s.name_len as usize]);
    if !put_value(sys, &key[..kl], &doc[..p]) {
        log_err(s, b"[image_fetcher] manifest PUT failed");
        return MC_FAIL;
    }
    log_msg(s, b"[image_fetcher] manifest recorded");
    MC_DONE
}

/// The image config blob landed in manifest_buf — verify its digest, then
/// project the runtime contract the pod path needs:
///   `/image-config/<name> = entrypoint=..;cmd=..;env=..;workdir=..;user=..`
/// Lists are `,`-joined with `%`-escaping (see `append_esc`). Absent keys are
/// omitted rather than written empty, so a consumer can tell "the image sets
/// no Entrypoint" from "the image sets an empty one".
unsafe fn config_complete(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let hex = hasher_hex(&s.hasher);
    if hex != s.blob_hex {
        log_err(s, b"[image_fetcher] image config sha256 mismatch");
        return false;
    }
    let m_len = s.manifest_fill as usize;
    let m = core::slice::from_raw_parts(s.manifest_buf.as_ptr(), m_len);
    // The runtime block is nested under `config` in both the docker v1 and the
    // OCI image-config schemas; an image with no block sets nothing.
    let cfg = j_sub1(m, b"config").unwrap_or(&[]);
    let mut doc = [0u8; MAX_VALUE];
    let mut p = append_json_list(&mut doc, 0, cfg, b"Entrypoint", b"entrypoint=");
    if p > 0 {
        p = append(&mut doc, p, b";");
    }
    let before = p;
    p = append_json_list(&mut doc, p, cfg, b"Cmd", b"cmd=");
    if p > before {
        p = append(&mut doc, p, b";");
    }
    let before = p;
    p = append_json_list(&mut doc, p, cfg, b"Env", b"env=");
    if p > before {
        p = append(&mut doc, p, b";");
    }
    if let Some(wd) = j_get1(cfg, b"WorkingDir") {
        if !wd.is_empty() {
            p = append(&mut doc, p, b"workdir=");
            p = append_esc(&mut doc, p, wd);
            p = append(&mut doc, p, b";");
        }
    }
    if let Some(u) = j_get1(cfg, b"User") {
        if !u.is_empty() {
            p = append(&mut doc, p, b"user=");
            p = append_esc(&mut doc, p, u);
            p = append(&mut doc, p, b";");
        }
    }
    // Trim the trailing separator so the record round-trips through `field`.
    if p > 0 && doc[p - 1] == b';' {
        p -= 1;
    }
    let mut key = [0u8; MAX_KEY];
    let mut kl = append(&mut key, 0, CONFIG_PREFIX);
    kl = append(&mut key, kl, &s.name[..s.name_len as usize]);
    if !put_value(sys, &key[..kl], &doc[..p]) {
        log_err(s, b"[image_fetcher] image config PUT failed");
        return false;
    }
    log_msg(s, b"[image_fetcher] image config recorded");
    true
}

/// The blob landed and hashed — verify + fsync + marker.
unsafe fn blob_complete(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    // Digest check over everything written.
    let hex = hasher_hex(&s.hasher);
    if hex != s.blob_hex {
        log_err(s, b"[image_fetcher] blob sha256 mismatch; discarding");
        close_blob_file(s, true);
        return false;
    }
    if s.file_fd >= 0 {
        let mut z = [0u8; 4];
        let rc = (sys.provider_call)(s.file_fd, FS_FSYNC, z.as_mut_ptr(), 0);
        if rc < 0 {
            log_err(s, b"[image_fetcher] blob fsync failed");
            close_blob_file(s, true);
            return false;
        }
    }
    close_blob_file(s, false);
    // Marker: /blobs/<hex> = size=<n> — image_puller's plan shrinks on it.
    let mut key = [0u8; MAX_KEY];
    let mut kl = append(&mut key, 0, BLOBS_PREFIX);
    kl = append(&mut key, kl, &s.blob_hex);
    let mut val = [0u8; 32];
    let mut vl = append(&mut val, 0, b"size=");
    vl = write_dec(&mut val, vl, s.blob_size as u64);
    if !put_value(sys, &key[..kl], &val[..vl]) {
        log_err(s, b"[image_fetcher] blob marker PUT failed");
        return false;
    }
    log_msg(s, b"[image_fetcher] blob verified + cached");
    true
}

/// The current response body is complete — advance.
unsafe fn body_complete(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    if s.conn_present == 1 {
        let mut payload = [0u8; 2];
        payload[..2].copy_from_slice(&s.conn_id.to_le_bytes());
        let _ = net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            payload.as_ptr(),
            2,
            s.net_buf.as_mut_ptr(),
            NET_BUF_SIZE,
        );
        s.prev_conn_id = s.conn_id;
        s.conn_present = 0;
    }
    if s.fetching == FETCH_MANIFEST {
        match manifest_complete(s) {
            MC_FAIL => return false,
            MC_INDEX => {
                // The tag held an index; `s.mref` now names the platform
                // manifest. Chain straight into the second GET.
                s.backoff_ms = 0;
                start_request(s, FETCH_MANIFEST);
                return true;
            }
            _ => {}
        }
        s.backoff_ms = 0;
        s.phase = Phase::Idle;
        s.dirty = 1; // the new manifest changes the plan → rescan
        true
    } else if s.fetching == FETCH_CONFIG {
        if !config_complete(s) {
            return false;
        }
        s.backoff_ms = 0;
        s.phase = Phase::Idle;
        s.dirty = 1; // the config gates the pod → let the chain re-reconcile
        true
    } else {
        // More of the blob remains (chunked Range) — chain the next chunk.
        if s.blob_pos < s.blob_size {
            s.backoff_ms = 0;
            start_request(s, FETCH_BLOB);
            return true;
        }
        if !blob_complete(s) {
            return false;
        }
        s.backoff_ms = 0;
        s.phase = Phase::Idle;
        s.dirty = 1; // marker shrinks the plan; more blobs may remain
        true
    }
}

/// Drain one inbound net frame. Returns false when nothing was read.
unsafe fn pump_net(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    if s.net_in < 0 {
        return false;
    }
    let poll = (sys.channel_poll)(s.net_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return false;
    }
    let nbuf = s.net_buf.as_mut_ptr();
    let (msg_type, payload_len) = net_read_frame(sys, s.net_in, nbuf, NET_BUF_SIZE);
    if msg_type == 0 {
        return false;
    }
    let payload = core::slice::from_raw_parts(nbuf.add(3), payload_len);
    match msg_type {
        NET_MSG_CONNECTED => {
            if s.phase == Phase::WaitConnect && payload_len >= 2 {
                let tag = if payload_len >= 3 {
                    payload[2]
                } else {
                    REQUESTER_TAG_NONE
                };
                if tag == REQUESTER_TAG_NONE || tag == dev_requester_tag(sys) {
                    s.conn_id = u16::from_le_bytes([payload[0], payload[1]]);
                    s.conn_present = 1;
                    let len = build_request(s);
                    s.tx_len = len as u16;
                    s.tx_sent = 0;
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::RecvHeader;
                    let _ = flush_tx(s);
                }
            }
        }
        NET_MSG_DATA => {
            if payload_len < 2 {
                return true;
            }
            let cid = u16::from_le_bytes([payload[0], payload[1]]);
            if s.conn_present == 0 || cid != s.conn_id {
                return true;
            }
            let mut data = &payload[2..];
            if s.phase == Phase::RecvHeader {
                let fill = s.hdr_fill as usize;
                let n = data.len().min(HDR_BUF_SIZE - fill);
                s.hdr_buf[fill..fill + n].copy_from_slice(&data[..n]);
                s.hdr_fill += n as u32;
                let have = core::slice::from_raw_parts(s.hdr_buf.as_ptr(), s.hdr_fill as usize);
                if let Some(hdr_end) = find(have, b"\r\n\r\n", 0) {
                    if !parse_response_header(s, hdr_end) {
                        enter_backoff(s);
                        return true;
                    }
                    s.phase = Phase::RecvBody;
                    let body_in_hdr_start = hdr_end + 4;
                    let body_in_hdr_len = s.hdr_fill as usize - body_in_hdr_start;
                    if body_in_hdr_len > 0 {
                        let mut tmp = [0u8; HDR_BUF_SIZE];
                        tmp[..body_in_hdr_len]
                            .copy_from_slice(&s.hdr_buf[body_in_hdr_start..s.hdr_fill as usize]);
                        if !body_bytes(s, &tmp[..body_in_hdr_len]) {
                            enter_backoff(s);
                            return true;
                        }
                    }
                    if n < data.len() {
                        data = &data[n..];
                        let mut tmp = [0u8; 8192];
                        let m = data.len().min(8192);
                        tmp[..m].copy_from_slice(&data[..m]);
                        if !body_bytes(s, &tmp[..m]) {
                            enter_backoff(s);
                            return true;
                        }
                    }
                    if s.body_received >= s.content_length && !body_complete(s) {
                        enter_backoff(s);
                    }
                } else if s.hdr_fill as usize >= HDR_BUF_SIZE {
                    log_err(s, b"[image_fetcher] oversized response header");
                    enter_backoff(s);
                }
            } else if s.phase == Phase::RecvBody {
                let remaining = (s.content_length - s.body_received) as usize;
                let take = data.len().min(remaining);
                let mut tmp = [0u8; 8192];
                let m = take.min(8192);
                tmp[..m].copy_from_slice(&data[..m]);
                if !body_bytes(s, &tmp[..m]) {
                    enter_backoff(s);
                    return true;
                }
                if s.body_received >= s.content_length && !body_complete(s) {
                    enter_backoff(s);
                }
            }
        }
        NET_MSG_CLOSED => {
            if payload_len >= 2 {
                let cid = u16::from_le_bytes([payload[0], payload[1]]);
                if s.conn_present == 1 && cid == s.conn_id {
                    s.conn_present = 0;
                    if s.phase == Phase::RecvHeader || s.phase == Phase::RecvBody {
                        log_err(s, b"[image_fetcher] connection closed mid-response");
                        enter_backoff(s);
                    }
                }
            }
        }
        NET_MSG_ERROR => {
            let cid = if payload_len >= 2 {
                u16::from_le_bytes([payload[0], payload[1]])
            } else {
                0
            };
            let stale = payload_len >= 2 && cid == s.prev_conn_id && cid != s.conn_id;
            if (s.conn_present == 1 && cid == s.conn_id)
                || (s.phase == Phase::WaitConnect && !stale)
            {
                log_err(s, b"[image_fetcher] net error");
                enter_backoff(s);
            }
        }
        _ => {}
    }
    true
}

// ---- work discovery ----

/// Walk one LIST page; call `f(key_tail)` for each entry under `prefix`.
/// Returns entries visited.
unsafe fn for_each_key(
    sys: &SyscallTable,
    prefix: &[u8],
    mut f: impl FnMut(&[u8]) -> bool,
) -> usize {
    let mut walk = ListWalk::new(prefix);
    let mut visited = 0usize;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        visited += 1;
        if klen > prefix.len() && &key[..prefix.len()] == prefix && !f(&key[prefix.len()..]) {
            break;
        }
    }
    visited
}

/// Scan for work: a request without a manifest → manifest fetch; else a plan
/// with a non-empty `pull=` → blob fetch. Starts at most ONE fetch.
unsafe fn scan_for_work(s: &mut State) -> bool {
    let sys = &*s.syscalls;

    // 1. Requests without manifests.
    let mut req_name = [0u8; MAX_NAME];
    let mut req_name_len = 0usize;
    for_each_key(sys, REQUESTS_PREFIX, |tail| {
        if tail.len() > MAX_NAME {
            return true;
        }
        let mut mkey = [0u8; MAX_KEY];
        let mut ml = append(&mut mkey, 0, MANIFESTS_PREFIX);
        ml = append(&mut mkey, ml, tail);
        if !exists(sys, &mkey[..ml]) {
            req_name[..tail.len()].copy_from_slice(tail);
            req_name_len = tail.len();
            return false; // found one
        }
        true
    });
    if req_name_len > 0 {
        // Parse repo/tag from the request record.
        let mut rkey = [0u8; MAX_KEY];
        let mut rl = append(&mut rkey, 0, REQUESTS_PREFIX);
        rl = append(&mut rkey, rl, &req_name[..req_name_len]);
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &rkey[..rl], &mut val) else {
            return false;
        };
        let v = &val[..vlen];
        let repo = field(v, b"repo=").unwrap_or(b"");
        let tag = field(v, b"tag=").unwrap_or(b"latest");
        if repo.is_empty() || repo.len() > MAX_REPO || tag.len() > MAX_TAG {
            log_err(s, b"[image_fetcher] bad image request record");
            return false;
        }
        s.name[..req_name_len].copy_from_slice(&req_name[..req_name_len]);
        s.name_len = req_name_len as u8;
        s.repo[..repo.len()].copy_from_slice(repo);
        s.repo_len = repo.len() as u8;
        s.tag[..tag.len()].copy_from_slice(tag);
        s.tag_len = tag.len() as u8;
        // First hop asks by tag; an index answer rewrites this to a digest.
        s.mref[..tag.len()].copy_from_slice(tag);
        s.mref_len = tag.len() as u8;
        s.index_hops = 0;
        log_msg(s, b"[image_fetcher] fetching manifest");
        start_request(s, FETCH_MANIFEST);
        return true;
    }

    // 2. Manifests whose config blob has not been fetched yet. Ahead of the
    // layers: it is small, and the pod path gates on it just like the rootfs.
    let mut cfg_name = [0u8; MAX_NAME];
    let mut cfg_name_len = 0usize;
    let mut cfg_hex = [0u8; 64];
    let mut cfg_size: u64 = 0;
    for_each_key(sys, MANIFESTS_PREFIX, |tail| {
        if tail.len() > MAX_NAME {
            return true;
        }
        let mut ckey = [0u8; MAX_KEY];
        let mut cl = append(&mut ckey, 0, CONFIG_PREFIX);
        cl = append(&mut ckey, cl, tail);
        if exists(sys, &ckey[..cl]) {
            return true;
        }
        let mut mkey = [0u8; MAX_KEY];
        let mut ml = append(&mut mkey, 0, MANIFESTS_PREFIX);
        ml = append(&mut mkey, ml, tail);
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &mkey[..ml], &mut val) else {
            return true;
        };
        let v = &val[..vlen];
        let Some(digest) = field(v, b"config=") else {
            return true; // manifest recorded no usable config descriptor
        };
        if digest.len() != 64 {
            return true;
        }
        let size = field(v, b"configsize=")
            .map(|b| parse_dec(b, 0).0)
            .unwrap_or(0);
        if size == 0 || size > u32::MAX as u64 {
            return true;
        }
        cfg_name[..tail.len()].copy_from_slice(tail);
        cfg_name_len = tail.len();
        cfg_hex.copy_from_slice(digest);
        cfg_size = size;
        false
    });
    if cfg_name_len > 0 {
        let mut rkey = [0u8; MAX_KEY];
        let mut rl = append(&mut rkey, 0, REQUESTS_PREFIX);
        rl = append(&mut rkey, rl, &cfg_name[..cfg_name_len]);
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &rkey[..rl], &mut val) else {
            return false;
        };
        let repo = field(&val[..vlen], b"repo=").unwrap_or(b"");
        if repo.is_empty() || repo.len() > MAX_REPO {
            return false;
        }
        s.name[..cfg_name_len].copy_from_slice(&cfg_name[..cfg_name_len]);
        s.name_len = cfg_name_len as u8;
        s.repo[..repo.len()].copy_from_slice(repo);
        s.repo_len = repo.len() as u8;
        // Reuses the blob machinery: same URL shape, same digest verification,
        // only the landing zone differs (manifest_buf, not a cache file).
        s.blob_hex.copy_from_slice(&cfg_hex);
        s.blob_size = cfg_size as u32;
        s.blob_pos = 0;
        s.hasher = Sha256::new();
        log_msg(s, b"[image_fetcher] fetching image config");
        start_request(s, FETCH_CONFIG);
        return true;
    }

    // 3. Plans with missing blobs.
    let mut plan_name = [0u8; MAX_NAME];
    let mut plan_name_len = 0usize;
    let mut first_digest = [0u8; 64];
    let mut have_digest = false;
    for_each_key(sys, PLAN_PREFIX, |tail| {
        if tail.len() > MAX_NAME {
            return true;
        }
        let mut pkey = [0u8; MAX_KEY];
        let mut pl = append(&mut pkey, 0, PLAN_PREFIX);
        pl = append(&mut pkey, pl, tail);
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &pkey[..pl], &mut val) else {
            return true;
        };
        let pull = field(&val[..vlen], b"pull=").unwrap_or(b"");
        // First digest whose /blobs/ marker is ABSENT. The plan lags the
        // markers (image_puller shrinks it on its own change event), and a
        // re-fetch of a completed blob would UNLINK the verified file out
        // from under a concurrent assembly — the marker is the truth.
        let mut start = 0usize;
        while start <= pull.len() {
            let end = pull[start..]
                .iter()
                .position(|&b| b == b',')
                .map(|i| start + i)
                .unwrap_or(pull.len());
            let d = &pull[start..end];
            if d.len() == 64 {
                let mut bk = [0u8; MAX_KEY];
                let mut bl = append(&mut bk, 0, BLOBS_PREFIX);
                bl = append(&mut bk, bl, d);
                if !exists(sys, &bk[..bl]) {
                    plan_name[..tail.len()].copy_from_slice(tail);
                    plan_name_len = tail.len();
                    first_digest.copy_from_slice(d);
                    have_digest = true;
                    return false;
                }
            }
            if end >= pull.len() {
                break;
            }
            start = end + 1;
        }
        true
    });
    if have_digest {
        // repo comes from the image's request record; size from its manifest.
        let mut rkey = [0u8; MAX_KEY];
        let mut rl = append(&mut rkey, 0, REQUESTS_PREFIX);
        rl = append(&mut rkey, rl, &plan_name[..plan_name_len]);
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &rkey[..rl], &mut val) else {
            return false;
        };
        let repo = field(&val[..vlen], b"repo=").unwrap_or(b"");
        if repo.is_empty() || repo.len() > MAX_REPO {
            return false;
        }
        let repo_len = repo.len();
        let mut repo_copy = [0u8; MAX_REPO];
        repo_copy[..repo_len].copy_from_slice(repo);

        let mut mkey = [0u8; MAX_KEY];
        let mut ml = append(&mut mkey, 0, MANIFESTS_PREFIX);
        ml = append(&mut mkey, ml, &plan_name[..plan_name_len]);
        let mut mval = [0u8; MAX_VALUE];
        let Some(mlen) = get_value(sys, &mkey[..ml], &mut mval) else {
            return false;
        };
        let mv = &mval[..mlen];
        let layers = field(mv, b"layers=").unwrap_or(b"");
        let sizes = field(mv, b"sizes=").unwrap_or(b"");
        // Locate the digest's index in layers=, then its size in sizes=.
        let mut size: u64 = 0;
        let mut li = 0usize;
        let mut lstart = 0usize;
        let mut found = false;
        while lstart <= layers.len() {
            let lend = layers[lstart..]
                .iter()
                .position(|&b| b == b',')
                .map(|i| lstart + i)
                .unwrap_or(layers.len());
            if layers[lstart..lend] == first_digest[..] {
                // li-th entry of sizes=
                let mut si = 0usize;
                let mut sstart = 0usize;
                while sstart <= sizes.len() {
                    let send = sizes[sstart..]
                        .iter()
                        .position(|&b| b == b',')
                        .map(|i| sstart + i)
                        .unwrap_or(sizes.len());
                    if si == li {
                        size = parse_dec(sizes, sstart).0;
                        break;
                    }
                    if send >= sizes.len() {
                        break;
                    }
                    si += 1;
                    sstart = send + 1;
                }
                found = true;
                break;
            }
            if lend >= layers.len() {
                break;
            }
            li += 1;
            lstart = lend + 1;
        }
        if !found || size == 0 || size > u32::MAX as u64 {
            log_err(
                s,
                b"[image_fetcher] plan digest missing from manifest sizes",
            );
            return false;
        }

        s.name[..plan_name_len].copy_from_slice(&plan_name[..plan_name_len]);
        s.name_len = plan_name_len as u8;
        s.repo[..repo_len].copy_from_slice(&repo_copy[..repo_len]);
        s.repo_len = repo_len as u8;
        s.blob_hex.copy_from_slice(&first_digest);
        s.blob_size = size as u32;
        s.blob_pos = 0;
        s.hasher = Sha256::new();

        // Fresh file: best-effort unlink, then open-create.
        let mut path = [0u8; MAX_DIR + 1 + 64];
        let plen = blob_path(s, &mut path);
        let _ = (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), plen);
        let fd = (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), plen);
        if fd < 0 {
            log_err(s, b"[image_fetcher] blob file open failed");
            return false;
        }
        s.file_fd = fd;
        log_msg(s, b"[image_fetcher] fetching blob");
        start_request(s, FETCH_BLOB);
        return true;
    }
    false
}

// ---- module ABI ----

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
        // Port 0 pair = the net stream (through tls or straight to linux_net).
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.sink = -1;
        s.resolved = 0;
        s.subscribed = 0;
        s.dirty = 0;
        s.phase = Phase::Init;
        s.conn_present = 0;
        s.file_fd = -1;
        s.backoff_ms = 0;
        s.authority_len = 0;
        s.authority_over = 0;
        s.port = 0;
        // Defaults, then TLV params.
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.authority_over == 1 || s.authority_len == 0 {
            log_err(s, b"[image_fetcher] authority (host[:port]) is required");
            return -2;
        }
        let Some((_, port)) = Target::parse(authority(s)) else {
            log_err(s, b"[image_fetcher] authority is not host[:port]");
            return -2;
        };
        s.port = port.unwrap_or(REGISTRY_PORT);
        if s.arch_len == 0 {
            let a = b"arm64"; // the nanocloud node fleet is aarch64
            s.arch[..a.len()].copy_from_slice(a);
            s.arch_len = a.len() as u8;
        }
        if s.os_len == 0 {
            let o = b"linux";
            s.os[..o.len()].copy_from_slice(o);
            s.os_len = o.len() as u8;
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

        // Cold start: resolve the change-sink port (input index 1), subscribe
        // the work prefixes, best-effort mkdir of the blob cache dir.
        if s.resolved == 0 {
            s.sink = dev_channel_port(sys, PORT_INPUT, 1);
            if s.sink >= 0 {
                store_subscribe(sys, REQUESTS_PREFIX, s.sink, 0);
                store_subscribe(sys, PLAN_PREFIX, s.sink, 0);
                store_subscribe(sys, MANIFESTS_PREFIX, s.sink, 0);
                s.subscribed = 1;
            }
            let mut dir = [0u8; MAX_DIR];
            let dl = s.blob_dir_len as usize;
            dir[..dl].copy_from_slice(&s.blob_dir[..dl]);
            let _ = (sys.provider_call)(-1, FS_MKDIR, dir.as_mut_ptr(), dl);
            s.resolved = 1;
            s.dirty = 1; // cold-start scan once the boot delay passes
        }

        // Store changes mark the work set dirty (drained even mid-fetch so
        // the sink never backs up).
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.dirty = 1;
        }

        // Drain inbound net frames (bounded per step — covers one TLS-module
        // step burst so the clear ring never backs up during a blob stream).
        let mut drained = 0;
        while drained < 64 && pump_net(s) {
            drained += 1;
        }

        match s.phase {
            Phase::Init => {
                if dev_millis(sys) >= s.boot_delay_ms as u64 {
                    s.phase = Phase::Idle;
                }
                0
            }
            Phase::Connecting => {
                if s.net_out < 0 {
                    return 0;
                }
                let mut payload = [0u8; CONNECT_TO_MAX];
                let n = connect_record(s, &mut payload, dev_requester_tag(sys));
                if n == 0 {
                    log_err(s, b"[image_fetcher] authority is not dialable");
                    enter_backoff(s);
                    return 0;
                }
                let wrote = net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CONNECT_TO,
                    payload.as_ptr(),
                    n,
                    s.net_buf.as_mut_ptr(),
                    NET_BUF_SIZE,
                );
                if wrote > 0 {
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::WaitConnect;
                }
                0
            }
            Phase::WaitConnect => {
                if dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS {
                    log_err(s, b"[image_fetcher] connect timeout");
                    enter_backoff(s);
                }
                0
            }
            Phase::RecvHeader | Phase::RecvBody => {
                if s.tx_sent < s.tx_len {
                    let _ = flush_tx(s);
                }
                if dev_millis(sys).wrapping_sub(s.state_start_ms) > RESPONSE_TIMEOUT_MS {
                    log_err(s, b"[image_fetcher] response timeout");
                    enter_backoff(s);
                }
                0
            }
            Phase::Idle => {
                if s.dirty == 1 {
                    s.dirty = 0;
                    let _ = scan_for_work(s);
                }
                0
            }
            Phase::Backoff => {
                if dev_millis(sys).wrapping_sub(s.state_start_ms) >= s.backoff_ms {
                    s.phase = Phase::Idle;
                    s.dirty = 1;
                }
                0
            }
        }
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
