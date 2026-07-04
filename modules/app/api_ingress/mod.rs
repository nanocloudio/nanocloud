//! API ingress — the k8s apiserver front door, as a PIC module. It terminates
//! HTTP/1.1 directly off the net_proto stream (from `tls`/`linux_net`), routes
//! each request, runs the authn→authz→admission→core pipeline over the
//! control-plane store seam, marshals the result to k8s JSON, and writes the
//! HTTP response. It is also the store's single writer on the request path.
//!
//! This is the module that lets nanocloud carry no host code at all: the whole
//! HTTP+JSON edge is `no_std` PIC. The graph is
//! `linux_net → tls → api_ingress → [api fmods]`.
//!
//! The API fmods (authn, rbac_gate, admission, core_api, api_responder,
//! watch_streamer) are reached over the store seam: this module writes
//! `/<lane>-req/<corr>` and reads back `/<lane>-resp/<corr>`. Because the store
//! is single-writer and the responder fmods run in the SAME cooperative
//! scheduler, the per-connection pipeline is a state machine that advances one
//! store round-trip at a time, yielding between stages so the responders run.
//!
//! The full apiserver edge lives here: HTTP/1.1 termination, the static
//! discovery/version/health surface, the authenticated CRUD + watch pipeline
//! (authn → authz → admission → core, one store round-trip per stage), and the
//! k8s JSON ⇄ store marshalling — all on this one connection state machine.

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

// The control-plane store: storage.object (0x14) keyed bytes. We only touch keys
// we minted (the /<lane>-req|resp/ seam), so storage.namespace is not needed.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const LIST_BUF: usize = 2048;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;

// Stream Surface v1 (net_proto) — the subset this module uses.
const NET_MSG_ACCEPTED: u8 = 0x01;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_BOUND: u8 = 0x04;
const NET_MSG_ERROR: u8 = 0x06;
const NET_MSG_BIND_REFUSED: u8 = 0x07;
const NET_CMD_BIND: u8 = 0x10;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;

/// Input-port kind for `dev_channel_port` (resolving the peer_identity port).
const PORT_INPUT: u8 = 0;

/// The port the apiserver listens on. In the graph, `tls` terminates TLS and
/// hands cleartext here; `linux_net` owns the actual bind.
const LISTEN_PORT: u16 = 7443;

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 1024;
/// Max stored object size (JSON). k8s objects are a few KB; larger truncate.
const MAX_OBJ: usize = 4096;
/// One HTTP request (request line + headers + body). k8s objects are a few KB.
const REQ_BUF: usize = 8192;
/// One HTTP response (status line + headers + body).
const RESP_BUF: usize = 4096;
const NET_BUF_SIZE: usize = 3 + REQ_BUF + 64;
const MAX_CONNS: usize = 8;
/// net_proto frames drained per tick — bounded so one busy connection can't
/// starve the pipeline poll below it.
const FRAMES_PER_TICK: u32 = 16;

const NO_CONN: u8 = 0xFF;

// ---- pipeline lanes (store seam prefixes) ---------------------------------

static REQ_API: &[u8] = b"/api-req/";
static RESP_API: &[u8] = b"/api-resp/";
static REQ_AUTHN: &[u8] = b"/authn-req/";
static RESP_AUTHN: &[u8] = b"/authn-resp/";
static REQ_AUTHZ: &[u8] = b"/authz-req/";
static RESP_AUTHZ: &[u8] = b"/authz-resp/";
static REQ_ADMIT: &[u8] = b"/admit-req/";
static RESP_ADMIT: &[u8] = b"/admit-resp/";
static REQ_CORE: &[u8] = b"/core-req/";
static RESP_CORE: &[u8] = b"/core-resp/";
static REQ_WATCH: &[u8] = b"/watch-req/";
static RESP_WATCH: &[u8] = b"/watch-resp/";

// ---- connection stages ----------------------------------------------------

/// Accumulating the HTTP request off the net stream.
const STAGE_READING: u8 = 0;
/// A store round-trip is in flight for the responder lane.
const STAGE_RESPONDER: u8 = 1;
/// The authenticated pipeline stages — each is one store round-trip, advanced
/// one at a time so the responder fmods run between them.
const STAGE_AUTHN: u8 = 2;
const STAGE_AUTHZ: u8 = 3;
const STAGE_ADMIT: u8 = 4;
const STAGE_CORE: u8 = 5;
const STAGE_WATCH: u8 = 6;
/// Response assembled; flush + close.
const STAGE_DONE: u8 = 9;

// ---- route classification -------------------------------------------------

const ROUTE_NONE: u8 = 0; // 404
const ROUTE_RESPONDER: u8 = 1; // health / count / getjson (unauthenticated)
const ROUTE_CORE: u8 = 2; // CRUD (authenticated) — pipeline slice
const ROUTE_WATCH: u8 = 3; // long-poll (authenticated) — pipeline slice

const VERB_NONE: u8 = 0;
const VERB_GET: u8 = 1;
const VERB_LIST: u8 = 2;
const VERB_CREATE: u8 = 3;
const VERB_UPDATE: u8 = 4;
const VERB_DELETE: u8 = 5;

// ---- state ----------------------------------------------------------------

/// One accepted connection: an HTTP request being read, then a pipeline being
/// driven, then a response flushed. `id == NO_CONN` marks the slot free.
#[repr(C)]
struct Conn {
    id: u8,
    stage: u8,
    /// Route classification (`ROUTE_*`).
    kind: u8,
    /// CRUD verb (`VERB_*`) for `ROUTE_CORE`.
    verb: u8,
    /// Correlation id of the in-flight store round-trip.
    corr: u32,
    /// Bytes accumulated into `req`.
    req_len: u32,
    /// Index just past the `\r\n\r\n` header terminator (0 = not seen yet).
    body_at: u32,
    /// Declared Content-Length (0 if absent).
    content_len: u32,
    // ---- pipeline carry (resource/ns/name are re-derived by re-parsing `req`;
    // these are the values a stage produces that a later stage consumes) ----
    /// The caller's credential (`peer=<svid>` | `token=<t>` | empty).
    cred_len: u16,
    cred: [u8; 192],
    /// The identity resolved by authn (`system:...` or `anonymous`).
    ident_len: u16,
    identity: [u8; 128],
    /// The request object in compact form; replaced by admission's defaulted
    /// object before the core write.
    obj_len: u16,
    obj: [u8; MAX_OBJ],
    /// Watch fence (`resourceVersion`) requested.
    since: u64,
    req: [u8; REQ_BUF],
}

impl Conn {
    const EMPTY: Self = Self {
        id: NO_CONN,
        stage: STAGE_READING,
        kind: ROUTE_NONE,
        verb: VERB_NONE,
        corr: 0,
        req_len: 0,
        body_at: 0,
        content_len: 0,
        cred_len: 0,
        cred: [0u8; 192],
        ident_len: 0,
        identity: [0u8; 128],
        obj_len: 0,
        obj: [0u8; MAX_OBJ],
        since: 0,
        req: [0u8; REQ_BUF],
    };
}

/// net_proto message type for the tls `peer_identity` envelope.
const MSG_PEER_IDENTITY: u8 = 0x5A;

/// One conn_id → verified-peer-SVID binding (hash of the peer cert pubkey),
/// delivered by `tls.peer_identity`. `conn_id == NO_CONN` marks the slot free.
#[repr(C)]
#[derive(Clone, Copy)]
struct PeerId {
    conn_id: u8,
    len: u8,
    svid: [u8; 32],
}

impl PeerId {
    const EMPTY: Self = Self {
        conn_id: NO_CONN,
        len: 0,
        svid: [0u8; 32],
    };
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    net_in: i32,
    /// The `tls.peer_identity` input (port 1); -1 when unwired (plaintext graph).
    peer_in: i32,
    net_out: i32,
    /// 0 = bind not sent, 1 = sent, 2 = MSG_BOUND seen.
    bound: u8,
    /// 0 until the extra input ports have been resolved (cold start).
    resolved: u8,
    seq: u32,
    served: u32,
    conns: [Conn; MAX_CONNS],
    peer_ids: [PeerId; MAX_CONNS],
    net_buf: [u8; NET_BUF_SIZE],
}

// ---- peer identity (conn_id → SVID) ---------------------------------------

/// Record a verified peer SVID for `conn_id`.
fn peer_set(s: &mut State, conn_id: u8, svid: &[u8]) {
    let len = svid.len().min(32);
    let mut idx = (0..MAX_CONNS).find(|&i| s.peer_ids[i].conn_id == conn_id);
    if idx.is_none() {
        idx = (0..MAX_CONNS).find(|&i| s.peer_ids[i].conn_id == NO_CONN);
    }
    if let Some(i) = idx {
        s.peer_ids[i].conn_id = conn_id;
        s.peer_ids[i].len = len as u8;
        s.peer_ids[i].svid[..len].copy_from_slice(&svid[..len]);
    }
}

/// Forget any SVID bound to `conn_id` (on connection close).
fn peer_clear(s: &mut State, conn_id: u8) {
    for i in 0..MAX_CONNS {
        if s.peer_ids[i].conn_id == conn_id {
            s.peer_ids[i].conn_id = NO_CONN;
            s.peer_ids[i].len = 0;
        }
    }
}

/// Hex-encode the SVID bound to `conn_id` into `out`; returns the hex length
/// (0 when no verified peer). The hex form is the `/peer-ids/<svid>` key the
/// authn fmod resolves.
fn peer_hex(s: &State, conn_id: u8, out: &mut [u8]) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for i in 0..MAX_CONNS {
        if s.peer_ids[i].conn_id == conn_id && s.peer_ids[i].len > 0 {
            let len = s.peer_ids[i].len as usize;
            let mut wp = 0;
            for &byte in &s.peer_ids[i].svid[..len] {
                if wp + 2 > out.len() {
                    break;
                }
                out[wp] = HEX[(byte >> 4) as usize];
                out[wp + 1] = HEX[(byte & 0x0F) as usize];
                wp += 2;
            }
            return wp;
        }
    }
    0
}

// ---- storage.object helpers -----------------------------------------------

// ---- small helpers --------------------------------------------------------

fn write_u32(dst: &mut [u8], at: usize, mut n: u32) -> usize {
    if at >= dst.len() {
        return at;
    }
    if n == 0 {
        dst[at] = b'0';
        return at + 1;
    }
    let mut tmp = [0u8; 10];
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

/// Build `<prefix><corr>` into `dst`; returns the length.
fn corr_key(dst: &mut [u8], prefix: &[u8], corr: u32) -> usize {
    let p = append(dst, 0, prefix);
    write_u32(dst, p, corr)
}

/// Case-insensitive ASCII byte compare.
fn eq_ascii_ci(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Find the first occurrence of `needle` in `hay`; returns its start index.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    let mut i = 0;
    while i + needle.len() <= hay.len() {
        if &hay[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

// ---- HTTP request parsing -------------------------------------------------

/// A parsed request line + the header span, borrowing from the connection's
/// accumulated bytes.
struct HttpReq<'a> {
    method: &'a [u8],
    path: &'a [u8],
    query: &'a [u8],
    headers: &'a [u8],
    body: &'a [u8],
}

/// Parse the request once headers + declared body are fully present. `body_at`
/// is the index past `\r\n\r\n`; `content_len` the declared body length.
fn parse_http<'a>(buf: &'a [u8], body_at: usize, content_len: usize) -> Option<HttpReq<'a>> {
    // Request line ends at the first CRLF.
    let line_end = find(buf, b"\r\n")?;
    let line = &buf[..line_end];
    let sp1 = line.iter().position(|&b| b == b' ')?;
    let method = &line[..sp1];
    let rest = &line[sp1 + 1..];
    let sp2 = rest.iter().position(|&b| b == b' ')?;
    let target = &rest[..sp2];
    let (path, query) = match target.iter().position(|&b| b == b'?') {
        Some(q) => (&target[..q], &target[q + 1..]),
        None => (target, &target[target.len()..]),
    };
    let headers = &buf[line_end + 2..body_at.saturating_sub(4).max(line_end + 2)];
    let body_end = (body_at + content_len).min(buf.len());
    let body = &buf[body_at.min(buf.len())..body_end];
    Some(HttpReq {
        method,
        path,
        query,
        headers,
        body,
    })
}

/// Read `Content-Length` from a header block (case-insensitive). 0 if absent.
fn content_length(headers: &[u8]) -> u32 {
    let mut start = 0;
    while start < headers.len() {
        let end = find(&headers[start..], b"\r\n")
            .map(|i| start + i)
            .unwrap_or(headers.len());
        let line = &headers[start..end];
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            if eq_ascii_ci(&line[..colon], b"content-length") {
                let mut v = &line[colon + 1..];
                while !v.is_empty() && (v[0] == b' ' || v[0] == b'\t') {
                    v = &v[1..];
                }
                let mut n: u32 = 0;
                for &b in v {
                    if b.is_ascii_digit() {
                        n = n.saturating_mul(10).saturating_add((b - b'0') as u32);
                    } else {
                        break;
                    }
                }
                return n;
            }
        }
        start = end + 2;
    }
    0
}

/// Read the value of `Authorization: Bearer <tok>`; empty slice if absent.
fn bearer(headers: &[u8]) -> &[u8] {
    let mut start = 0;
    while start < headers.len() {
        let end = find(&headers[start..], b"\r\n")
            .map(|i| start + i)
            .unwrap_or(headers.len());
        let line = &headers[start..end];
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            if eq_ascii_ci(&line[..colon], b"authorization") {
                let mut v = &line[colon + 1..];
                while !v.is_empty() && (v[0] == b' ' || v[0] == b'\t') {
                    v = &v[1..];
                }
                if v.len() > 7 && eq_ascii_ci(&v[..7], b"bearer ") {
                    return &v[7..];
                }
            }
        }
        start = end + 2;
    }
    &headers[headers.len()..]
}

// ---- route classification --------------------------------------------------

/// Query lookup: value of `key` in a `k=v&k2=v2` string.
fn query_get<'a>(query: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start < query.len() {
        let amp = find(&query[start..], b"&")
            .map(|i| start + i)
            .unwrap_or(query.len());
        let pair = &query[start..amp];
        if let Some(eq) = pair.iter().position(|&b| b == b'=') {
            if &pair[..eq] == key {
                return Some(&pair[eq + 1..]);
            }
        }
        start = amp + 1;
    }
    None
}

/// The routed request, with spans copied into the connection's `op`/scratch by
/// the caller. This slice classifies; the pipeline slice consumes the CRUD/watch
/// fields.
#[derive(Clone, Copy)]
struct Routed<'a> {
    kind: u8,
    verb: u8,
    resource: &'a [u8],
    namespace: &'a [u8],
    name: &'a [u8],
    /// Responder op string (`ROUTE_RESPONDER`), built into `scratch`.
    op: &'a [u8],
}

/// Strip a trailing slash (except root).
fn normalize(path: &[u8]) -> &[u8] {
    if path.len() > 1 && path[path.len() - 1] == b'/' {
        &path[..path.len() - 1]
    } else {
        path
    }
}

include!("../_shared/kube_path.rs");

/// Extract `metadata.name` from a flat JSON body (`"metadata":{"name":"X"}`).
fn body_name(body: &[u8]) -> &[u8] {
    let Some(mi) = find(body, b"\"metadata\"") else {
        return &body[body.len()..];
    };
    let rest = &body[mi..];
    let Some(ni) = find(rest, b"\"name\"") else {
        return &body[body.len()..];
    };
    let after = &rest[ni + 6..];
    // skip to the opening quote of the value
    let Some(q1) = after.iter().position(|&b| b == b'"') else {
        return &body[body.len()..];
    };
    let val = &after[q1 + 1..];
    let Some(q2) = val.iter().position(|&b| b == b'"') else {
        return &body[body.len()..];
    };
    &val[..q2]
}

// ---- static discovery documents (served directly, no store round-trip) -----
//
// These are the k8s API discovery docs kubectl fetches to learn the served
// resources. They are static for a given API surface, so they live as
// constants here rather than as a store lookup.

static DOC_VERSION: &[u8] = br#"{"major":"1","minor":"0","gitVersion":"v0.0.1-nanocloud","gitCommit":"unknown","gitTreeState":"clean","buildDate":"1970-01-01T00:00:00Z","compiler":"rustc","platform":"linux/arm64"}"#;
static DOC_API: &[u8] = br#"{"kind":"APIVersions","versions":["v1"]}"#;
static DOC_APIS: &[u8] = br#"{"apiVersion":"v1","kind":"APIGroupList","groups":[{"name":"apps","versions":[{"groupVersion":"apps/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apps/v1","version":"v1"}},{"name":"discovery.k8s.io","versions":[{"groupVersion":"discovery.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"discovery.k8s.io/v1","version":"v1"}},{"name":"node.k8s.io","versions":[{"groupVersion":"node.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"node.k8s.io/v1","version":"v1"}},{"name":"nanocloud.io","versions":[{"groupVersion":"nanocloud.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"nanocloud.io/v1","version":"v1"}}]}"#;
static DOC_G_NANOCLOUD: &[u8] = br#"{"name":"nanocloud.io","versions":[{"groupVersion":"nanocloud.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"nanocloud.io/v1","version":"v1"}}"#;
static DOC_G_APPS: &[u8] = br#"{"name":"apps","versions":[{"groupVersion":"apps/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apps/v1","version":"v1"}}"#;
static DOC_G_NODE: &[u8] = br#"{"name":"node.k8s.io","versions":[{"groupVersion":"node.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"node.k8s.io/v1","version":"v1"}}"#;
static DOC_G_DISCOVERY: &[u8] = br#"{"name":"discovery.k8s.io","versions":[{"groupVersion":"discovery.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"discovery.k8s.io/v1","version":"v1"}}"#;
static DOC_R_CORE: &[u8] = br#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"pods","singularName":"","namespaced":true,"kind":"Pod","verbs":["get","list","watch"],"shortNames":["po"]},{"name":"pods/log","singularName":"","namespaced":true,"kind":"PodLogOptions","verbs":["get"]},{"name":"pods/exec","singularName":"","namespaced":true,"kind":"PodExecOptions","verbs":["create"]},{"name":"configmaps","singularName":"","namespaced":true,"kind":"ConfigMap","verbs":["get","list","watch","create","delete","update","deletecollection"],"shortNames":["cm"]},{"name":"secrets","singularName":"","namespaced":true,"kind":"Secret","verbs":["get","list","watch","create","delete","update","deletecollection"],"shortNames":["sec"]},{"name":"events","singularName":"","namespaced":true,"kind":"Event","verbs":["get","list","watch"],"shortNames":["ev"]},{"name":"persistentvolumeclaims","singularName":"","namespaced":true,"kind":"PersistentVolumeClaim","verbs":["get","list"],"shortNames":["pvc"]},{"name":"services","singularName":"","namespaced":true,"kind":"Service","verbs":["get","list","watch","create","delete"],"shortNames":["svc"]},{"name":"endpoints","singularName":"","namespaced":true,"kind":"Endpoints","verbs":["get","list","watch"],"shortNames":["ep"]}]}"#;
static DOC_R_APPS: &[u8] = br#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"apps/v1","resources":[{"name":"statefulsets","singularName":"","namespaced":true,"kind":"StatefulSet","verbs":["get","list"],"shortNames":["sts"]},{"name":"replicasets","singularName":"","namespaced":true,"kind":"ReplicaSet","verbs":["get","list"],"shortNames":["rs"]}]}"#;
static DOC_R_NANOCLOUD: &[u8] = br#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"nanocloud.io/v1","resources":[{"name":"certificates","singularName":"","namespaced":false,"kind":"Certificate","verbs":["create"]},{"name":"bundles","singularName":"","namespaced":true,"kind":"Bundle","verbs":["get","list","watch","create","update","patch","delete","deletecollection"],"shortNames":["bdl"],"categories":["nanocloud"]},{"name":"roles","singularName":"","namespaced":true,"kind":"Role","verbs":["get","list"],"shortNames":["r"],"categories":["nanocloud"]},{"name":"rolebindings","singularName":"","namespaced":true,"kind":"RoleBinding","verbs":["get","list"],"shortNames":["rb"],"categories":["nanocloud"]},{"name":"volumesnapshots","singularName":"","namespaced":true,"kind":"VolumeSnapshot","verbs":["get","list","create","delete"],"shortNames":["vsnap"],"categories":["nanocloud"]}]}"#;
static DOC_R_NODE: &[u8] = br#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"node.k8s.io/v1","resources":[{"name":"runtimeclasses","singularName":"","namespaced":false,"kind":"RuntimeClass","verbs":["get","list"]}]}"#;
static DOC_R_DISCOVERY: &[u8] = br#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"discovery.k8s.io/v1","resources":[{"name":"endpointslices","singularName":"","namespaced":true,"kind":"EndpointSlice","verbs":["get","list","watch"]}]}"#;

/// Minimal OpenAPI doc — kubectl fetches it for client-side validation and
/// degrades gracefully; a minimal valid document keeps it from erroring.
static DOC_OPENAPI: &[u8] =
    br#"{"swagger":"2.0","info":{"title":"nanocloud","version":"v1"},"paths":{}}"#;
/// Minimal Prometheus exposition — the apiserver's own liveness gauge. Richer
/// metrics ride fluxor's telemetry, not this endpoint.
static DOC_METRICS: &[u8] = b"# HELP nanocloud_apiserver_up API server is serving.\n# TYPE nanocloud_apiserver_up gauge\nnanocloud_apiserver_up 1\n";

/// Return the static document + content-type for `path`, if any. Exact-path GETs
/// only — `/api/v1` is the core resource list, while `/api/v1/...` is CRUD.
fn static_doc(method: &[u8], path: &[u8]) -> Option<(&'static [u8], &'static [u8])> {
    if method != b"GET" {
        return None;
    }
    let json: &[u8] = b"application/json";
    let p = normalize(path);
    if p == b"/version" {
        Some((DOC_VERSION, json))
    } else if p == b"/api" {
        Some((DOC_API, json))
    } else if p == b"/apis" {
        Some((DOC_APIS, json))
    } else if p == b"/apis/nanocloud.io" {
        Some((DOC_G_NANOCLOUD, json))
    } else if p == b"/apis/apps" {
        Some((DOC_G_APPS, json))
    } else if p == b"/apis/node.k8s.io" {
        Some((DOC_G_NODE, json))
    } else if p == b"/apis/discovery.k8s.io" {
        Some((DOC_G_DISCOVERY, json))
    } else if p == b"/api/v1" {
        Some((DOC_R_CORE, json))
    } else if p == b"/apis/apps/v1" {
        Some((DOC_R_APPS, json))
    } else if p == b"/apis/nanocloud.io/v1" {
        Some((DOC_R_NANOCLOUD, json))
    } else if p == b"/apis/node.k8s.io/v1" {
        Some((DOC_R_NODE, json))
    } else if p == b"/apis/discovery.k8s.io/v1" {
        Some((DOC_R_DISCOVERY, json))
    } else if p == b"/openapi/v2" || p == b"/openapi.json" {
        Some((DOC_OPENAPI, json))
    } else if p == b"/metrics" {
        Some((DOC_METRICS, b"text/plain; version=0.0.4"))
    } else {
        None
    }
}

/// Classify a request. `scratch` receives a built responder op when applicable.
fn route<'a>(
    method: &[u8],
    path: &'a [u8],
    query: &[u8],
    body: &'a [u8],
    scratch: &'a mut [u8],
) -> Routed<'a> {
    let none = Routed {
        kind: ROUTE_NONE,
        verb: VERB_NONE,
        resource: b"",
        namespace: b"",
        name: b"",
        op: b"",
    };
    let path = normalize(path);

    // --- api_responder ops (unauthenticated) ---
    if method == b"GET" && (path == b"/healthz" || path == b"/livez" || path == b"/readyz") {
        let n = append(scratch, 0, b"healthz");
        return Routed {
            kind: ROUTE_RESPONDER,
            op: &scratch[..n],
            ..none
        };
    }
    if method == b"GET" && path.starts_with(b"/apis/nanocloud.io/v1/counts/") {
        let res = &path[b"/apis/nanocloud.io/v1/counts/".len()..];
        #[allow(
            clippy::manual_contains,
            reason = "contains(&b'/') links core::slice::memchr, which is unavailable in PIC no_std — keep the explicit byte scan"
        )]
        let has_slash = res.iter().any(|&c| c == b'/');
        if !res.is_empty() && !has_slash {
            let mut p = append(scratch, 0, b"count:/");
            p = append(scratch, p, res);
            p = append(scratch, p, b"/");
            return Routed {
                kind: ROUTE_RESPONDER,
                op: &scratch[..p],
                ..none
            };
        }
        return none;
    }
    if method == b"GET" && path.starts_with(b"/apis/nanocloud.io/v1/objects/") {
        let rest = &path[b"/apis/nanocloud.io/v1/objects/".len()..];
        if !rest.is_empty() {
            let mut p = append(scratch, 0, b"getjson:/");
            p = append(scratch, p, rest);
            return Routed {
                kind: ROUTE_RESPONDER,
                op: &scratch[..p],
                ..none
            };
        }
        return none;
    }

    // --- CRUD + watch (authenticated; consumed by the pipeline slice) ---
    let Some((resource, namespace, name)) = parse_rest_path(path) else {
        return none;
    };
    let watch = query_get(query, b"watch") == Some(b"true");
    match method {
        b"GET" if watch && name.is_empty() => Routed {
            kind: ROUTE_WATCH,
            verb: VERB_NONE,
            resource,
            namespace,
            name: b"",
            op: b"",
        },
        b"GET" if !name.is_empty() => Routed {
            kind: ROUTE_CORE,
            verb: VERB_GET,
            resource,
            namespace,
            name,
            op: b"",
        },
        b"GET" => Routed {
            kind: ROUTE_CORE,
            verb: VERB_LIST,
            resource,
            namespace,
            name: b"",
            op: b"",
        },
        b"POST" => Routed {
            kind: ROUTE_CORE,
            verb: VERB_CREATE,
            resource,
            namespace,
            name: body_name(body),
            op: b"",
        },
        b"PUT" if !name.is_empty() => Routed {
            kind: ROUTE_CORE,
            verb: VERB_UPDATE,
            resource,
            namespace,
            name,
            op: b"",
        },
        b"DELETE" if !name.is_empty() => Routed {
            kind: ROUTE_CORE,
            verb: VERB_DELETE,
            resource,
            namespace,
            name,
            op: b"",
        },
        _ => none,
    }
}

// ---- net_proto emitters ---------------------------------------------------

unsafe fn net_send_bind(s: &mut State) -> bool {
    if s.net_out < 0 {
        return false;
    }
    let port = LISTEN_PORT.to_le_bytes();
    let scratch = s.net_buf.as_mut_ptr();
    net_write_frame(
        &*s.syscalls,
        s.net_out,
        NET_CMD_BIND,
        port.as_ptr(),
        2,
        scratch,
        NET_BUF_SIZE,
    ) > 0
}

/// Emit CMD_SEND. Payload: `[conn_id:2 LE][data:n]`. Chunks data larger than one
/// net_proto frame across multiple sends. `conn_id` is u16 on the wire.
unsafe fn net_send_data(s: &mut State, conn_id: u8, data: &[u8]) -> bool {
    if s.net_out < 0 {
        return false;
    }
    let sys = s.syscalls;
    let out_chan = s.net_out;
    let mut off = 0;
    let cap = NET_BUF_SIZE - 5; // frame header (3) + conn_id (2 LE)
    while off < data.len() {
        let chunk = (data.len() - off).min(cap);
        let payload_len = 2 + chunk;
        let scratch = s.net_buf.as_mut_ptr();
        *scratch = NET_CMD_SEND;
        *scratch.add(1) = (payload_len & 0xFF) as u8;
        *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
        *scratch.add(3) = conn_id;
        *scratch.add(4) = 0; // conn_id high byte (u16 LE; slot ids < 256)
        core::ptr::copy_nonoverlapping(data.as_ptr().add(off), scratch.add(5), chunk);
        if ((*sys).channel_write)(out_chan, scratch, 3 + payload_len) <= 0 {
            return false;
        }
        off += chunk;
    }
    true
}

unsafe fn net_send_close(s: &mut State, conn_id: u8) {
    if s.net_out < 0 {
        return;
    }
    // CMD_CLOSE payload: [conn_id:2 LE]; slot ids are < 256, so the high
    // byte is always 0.
    let payload = [conn_id, 0];
    let scratch = s.net_buf.as_mut_ptr();
    net_write_frame(
        &*s.syscalls,
        s.net_out,
        NET_CMD_CLOSE,
        payload.as_ptr(),
        2,
        scratch,
        NET_BUF_SIZE,
    );
}

// ---- HTTP response --------------------------------------------------------

/// Write an HTTP/1.1 response to `conn_id` and close it. `ctype` is the
/// Content-Type; `body` the payload.
unsafe fn http_respond(s: &mut State, conn_id: u8, status: u16, ctype: &[u8], body: &[u8]) {
    let mut hdr = [0u8; 160];
    let mut p = append(&mut hdr, 0, b"HTTP/1.1 ");
    p = write_u32(&mut hdr, p, status as u32);
    p = append(&mut hdr, p, b" ");
    p = append(&mut hdr, p, reason_phrase(status));
    p = append(&mut hdr, p, b"\r\nContent-Type: ");
    p = append(&mut hdr, p, ctype);
    p = append(&mut hdr, p, b"\r\nContent-Length: ");
    p = write_u32(&mut hdr, p, body.len() as u32);
    p = append(&mut hdr, p, b"\r\nConnection: close\r\n\r\n");
    net_send_data(s, conn_id, &hdr[..p]);
    if !body.is_empty() {
        net_send_data(s, conn_id, body);
    }
    net_send_close(s, conn_id);
}

fn reason_phrase(status: u16) -> &'static [u8] {
    match status {
        200 => b"OK",
        201 => b"Created",
        400 => b"Bad Request",
        401 => b"Unauthorized",
        403 => b"Forbidden",
        404 => b"Not Found",
        409 => b"Conflict",
        500 => b"Internal Server Error",
        501 => b"Not Implemented",
        502 => b"Bad Gateway",
        _ => b"Status",
    }
}

/// Split a responder `<status>;<body>` doc into `(status, body)`.
fn split_status_body(doc: &[u8]) -> (u16, &[u8]) {
    let Some(sc) = doc.iter().position(|&b| b == b';') else {
        return (502, b"malformed module response");
    };
    let mut status: u16 = 0;
    for &b in &doc[..sc] {
        if b.is_ascii_digit() {
            status = status.saturating_mul(10).saturating_add((b - b'0') as u16);
        }
    }
    (status, &doc[sc + 1..])
}

// ---- JSON marshal (no_std) -------------------------------------------------

/// Iterate `,`-separated fields of `compact`, calling `f(field)`.
fn for_each_field(compact: &[u8], mut f: impl FnMut(&[u8])) {
    let mut start = 0;
    while start <= compact.len() {
        let end = find(&compact[start..], b",")
            .map(|x| start + x)
            .unwrap_or(compact.len());
        if end > start {
            f(&compact[start..end]);
        }
        if end >= compact.len() {
            break;
        }
        start = end + 1;
    }
}

/// Emit a compact scalar as JSON: a number (all digits, optional leading `-`) or
/// `true`/`false` unquoted; anything else quoted.
fn write_scalar_json(out: &mut [u8], at: usize, v: &[u8]) -> usize {
    let numeric = !v.is_empty()
        && v.iter()
            .enumerate()
            .all(|(k, &c)| c.is_ascii_digit() || (k == 0 && c == b'-' && v.len() > 1));
    if numeric || v == b"true" || v == b"false" {
        append(out, at, v)
    } else {
        let p = append(out, at, b"\"");
        let p = append(out, p, v);
        append(out, p, b"\"")
    }
}

/// Render a compact object (`name=X,k=v`) as a JSON object, `name` nested under
/// `metadata`. Returns the length written to `out`.
fn object_to_json(compact: &[u8], out: &mut [u8]) -> usize {
    let mut wp = append(out, 0, b"{");
    let mut first = true;
    // The closure can't capture `out`+`wp` by mutable ref through FnMut twice, so
    // walk fields inline.
    let mut start = 0;
    while start <= compact.len() {
        let end = find(&compact[start..], b",")
            .map(|x| start + x)
            .unwrap_or(compact.len());
        let field = &compact[start..end];
        if !field.is_empty() {
            if let Some(eq) = field.iter().position(|&c| c == b'=') {
                let k = &field[..eq];
                let v = &field[eq + 1..];
                if !first {
                    wp = append(out, wp, b",");
                }
                first = false;
                if k == b"name" {
                    wp = append(out, wp, b"\"metadata\":{\"name\":\"");
                    wp = append(out, wp, v);
                    wp = append(out, wp, b"\"}");
                } else {
                    wp = append(out, wp, b"\"");
                    wp = append(out, wp, k);
                    wp = append(out, wp, b"\":");
                    wp = write_scalar_json(out, wp, v);
                }
            }
        }
        if end >= compact.len() {
            break;
        }
        start = end + 1;
    }
    append(out, wp, b"}")
}

/// Render a compact name CSV (`a,b,c`) as a k8s List object.
fn list_to_json(csv: &[u8], out: &mut [u8]) -> usize {
    let mut wp = append(out, 0, b"{\"kind\":\"List\",\"items\":[");
    let mut first = true;
    let mut start = 0;
    while start <= csv.len() {
        let end = find(&csv[start..], b",")
            .map(|x| start + x)
            .unwrap_or(csv.len());
        let name = &csv[start..end];
        if !name.is_empty() {
            if !first {
                wp = append(out, wp, b",");
            }
            first = false;
            wp = append(out, wp, b"{\"metadata\":{\"name\":\"");
            wp = append(out, wp, name);
            wp = append(out, wp, b"\"}}");
        }
        if end >= csv.len() {
            break;
        }
        start = end + 1;
    }
    append(out, wp, b"]}")
}

/// Render a k8s Status object.
fn status_to_json(code: u16, success: bool, message: &[u8], out: &mut [u8]) -> usize {
    let mut wp = append(out, 0, b"{\"kind\":\"Status\",\"status\":\"");
    wp = append(out, wp, if success { b"Success" } else { b"Failure" });
    wp = append(out, wp, b"\",\"message\":\"");
    wp = append(out, wp, message);
    wp = append(out, wp, b"\",\"code\":");
    wp = write_u32(out, wp, code as u32);
    append(out, wp, b"}")
}

/// Render a watch response (`rev=<fence>;events=PUT:name:rev,DELETE:name:rev,…`)
/// as `{"resourceVersion":<f>,"events":[{"type":..,"object":..,"resourceVersion":..}]}`.
fn watch_to_json(watchbody: &[u8], out: &mut [u8]) -> usize {
    let (fence, events) = match find(watchbody, b";") {
        Some(sc) => (&watchbody[..sc], &watchbody[sc + 1..]),
        None => (&watchbody[..0], watchbody),
    };
    let fence_val = fence.strip_prefix(b"rev=").unwrap_or(b"0");
    let events = events.strip_prefix(b"events=").unwrap_or(events);
    let mut wp = append(out, 0, b"{\"resourceVersion\":");
    wp = append(
        out,
        wp,
        if fence_val.is_empty() {
            b"0"
        } else {
            fence_val
        },
    );
    wp = append(out, wp, b",\"events\":[");
    let mut first = true;
    let mut start = 0;
    while start <= events.len() {
        let end = find(&events[start..], b",")
            .map(|x| start + x)
            .unwrap_or(events.len());
        let ev = &events[start..end];
        if !ev.is_empty() {
            // "PUT:<name>:<rev>" | "DELETE:<name>:<rev>"
            let (etype, rest): (&[u8], &[u8]) = if let Some(r) = ev.strip_prefix(b"PUT:") {
                (b"ADDED", r)
            } else if let Some(r) = ev.strip_prefix(b"DELETE:") {
                (b"DELETED", r)
            } else {
                (b"", &ev[..0])
            };
            if !etype.is_empty() {
                // rest = "<name>:<rev>" — split on the LAST ':'.
                let (name, rev) = match rest.iter().rposition(|&c| c == b':') {
                    Some(p) => (&rest[..p], &rest[p + 1..]),
                    None => (rest, &rest[rest.len()..]),
                };
                if !first {
                    wp = append(out, wp, b",");
                }
                first = false;
                wp = append(out, wp, b"{\"type\":\"");
                wp = append(out, wp, etype);
                wp = append(out, wp, b"\",\"object\":{\"metadata\":{\"name\":\"");
                wp = append(out, wp, name);
                wp = append(out, wp, b"\"}},\"resourceVersion\":");
                wp = append(out, wp, if rev.is_empty() { b"0" } else { rev });
                wp = append(out, wp, b"}");
            }
        }
        if end >= events.len() {
            break;
        }
        start = end + 1;
    }
    append(out, wp, b"]}")
}

// ---- pipeline op builders -------------------------------------------------

fn verb_str(verb: u8) -> &'static [u8] {
    match verb {
        VERB_GET => b"get",
        VERB_LIST => b"list",
        VERB_CREATE => b"create",
        VERB_UPDATE => b"update",
        VERB_DELETE => b"delete",
        _ => b"",
    }
}

fn is_mutating(verb: u8) -> bool {
    matches!(verb, VERB_CREATE | VERB_UPDATE | VERB_DELETE)
}

/// The verb name the authz gate keys on. A watch authorizes as `watch`.
fn authz_verb(kind: u8, verb: u8) -> &'static [u8] {
    if kind == ROUTE_WATCH {
        b"watch"
    } else {
        verb_str(verb)
    }
}

// ---- pipeline state machine ----------------------------------------------
//
// A connection routed to CRUD/watch walks authn → authz → (admit) → core|watch,
// each stage one store round-trip. `dispatch` starts it at authn; `advance` runs
// one stage transition per call from `module_step`, yielding between so the
// responder fmods run.

/// Re-derive `(resource, namespace, name)` from the buffered request into the
/// caller's fixed buffers; returns their lengths. Re-parsing avoids stashing the
/// spans on the connection across stages.
fn route_parts(
    s: &State,
    slot: usize,
    res: &mut [u8],
    ns: &mut [u8],
    nm: &mut [u8],
) -> (usize, usize, usize) {
    let conn = &s.conns[slot];
    let req = &conn.req[..conn.req_len as usize];
    let mut scratch = [0u8; MAX_VALUE];
    let Some(http) = parse_http(req, conn.body_at as usize, conn.content_len as usize) else {
        return (0, 0, 0);
    };
    let routed = route(http.method, http.path, http.query, http.body, &mut scratch);
    let rl = routed.resource.len().min(res.len());
    res[..rl].copy_from_slice(&routed.resource[..rl]);
    let nsl = routed.namespace.len().min(ns.len());
    ns[..nsl].copy_from_slice(&routed.namespace[..nsl]);
    let ml = routed.name.len().min(nm.len());
    nm[..ml].copy_from_slice(&routed.name[..ml]);
    (rl, nsl, ml)
}

/// Mint a correlation, write `/<prefix>/<corr> = op`, and advance to `stage`.
unsafe fn pipeline_send(s: &mut State, slot: usize, prefix: &[u8], op: &[u8], stage: u8) -> bool {
    let corr = s.seq;
    s.seq = s.seq.wrapping_add(1);
    let mut key = [0u8; MAX_KEY];
    let klen = corr_key(&mut key, prefix, corr);
    let sys = &*s.syscalls;
    if put_value(sys, &key[..klen], op) {
        s.conns[slot].corr = corr;
        s.conns[slot].stage = stage;
        true
    } else {
        false
    }
}

/// Poll a stage's response; on arrival copy the body into `out`, retire both
/// keys, and return `(status, body_len)`.
unsafe fn pipeline_poll(
    s: &mut State,
    slot: usize,
    req_prefix: &[u8],
    resp_prefix: &[u8],
    out: &mut [u8],
) -> Option<(u16, usize)> {
    let corr = s.conns[slot].corr;
    let mut rkey = [0u8; MAX_KEY];
    let rlen = corr_key(&mut rkey, resp_prefix, corr);
    let mut val = [0u8; MAX_VALUE];
    let sys = &*s.syscalls;
    let n = get_value(sys, &rkey[..rlen], &mut val)?;
    let (status, body) = split_status_body(&val[..n]);
    let bl = body.len().min(out.len());
    out[..bl].copy_from_slice(&body[..bl]);
    let mut qkey = [0u8; MAX_KEY];
    let qlen = corr_key(&mut qkey, req_prefix, corr);
    let sys = &*s.syscalls;
    delete_value(sys, &qkey[..qlen]);
    delete_value(sys, &rkey[..rlen]);
    Some((status, bl))
}

/// Start the authn stage: `/authn-req/<corr> = <credential>`.
unsafe fn start_authn(s: &mut State, slot: usize) {
    let cl = s.conns[slot].cred_len as usize;
    let mut op = [0u8; 192];
    let n = append(&mut op, 0, &s.conns[slot].cred[..cl]);
    if !pipeline_send(s, slot, REQ_AUTHN, &op[..n], STAGE_AUTHN) {
        let id = s.conns[slot].id;
        http_respond(s, id, 500, b"text/plain", b"store write failed");
        conn_free(s, slot);
    }
}

/// Start the authz stage: `id=<identity>;verb=<verb>;resource=<resource>`.
unsafe fn start_authz(s: &mut State, slot: usize) {
    let mut res = [0u8; 48];
    let mut ns = [0u8; 64];
    let mut nm = [0u8; 128];
    let (rl, _nsl, _ml) = route_parts(s, slot, &mut res, &mut ns, &mut nm);
    let il = s.conns[slot].ident_len as usize;
    let kind = s.conns[slot].kind;
    let verb = s.conns[slot].verb;
    let mut op = [0u8; 256];
    let mut p = append(&mut op, 0, b"id=");
    p = append(&mut op, p, &s.conns[slot].identity[..il]);
    p = append(&mut op, p, b";verb=");
    p = append(&mut op, p, authz_verb(kind, verb));
    p = append(&mut op, p, b";resource=");
    p = append(&mut op, p, &res[..rl]);
    pipeline_send(s, slot, REQ_AUTHZ, &op[..p], STAGE_AUTHZ);
}

/// Start the admission stage: `verb=..;resource=..;ns=..;obj=<compact>`.
unsafe fn start_admit(s: &mut State, slot: usize) {
    let mut res = [0u8; 48];
    let mut ns = [0u8; 64];
    let mut nm = [0u8; 128];
    let (rl, nsl, _ml) = route_parts(s, slot, &mut res, &mut ns, &mut nm);
    let verb = s.conns[slot].verb;
    let ol = s.conns[slot].obj_len as usize;
    let mut op = [0u8; MAX_OBJ + 512];
    let mut p = append(&mut op, 0, b"verb=");
    p = append(&mut op, p, verb_str(verb));
    p = append(&mut op, p, b";resource=");
    p = append(&mut op, p, &res[..rl]);
    p = append(&mut op, p, b";ns=");
    p = append(&mut op, p, &ns[..nsl]);
    p = append(&mut op, p, b";obj=");
    p = append(&mut op, p, &s.conns[slot].obj[..ol]);
    pipeline_send(s, slot, REQ_ADMIT, &op[..p], STAGE_ADMIT);
}

/// Start the core stage: `verb=..;resource=..;ns=..;name=..;obj=<compact>`.
unsafe fn start_core(s: &mut State, slot: usize) {
    let mut res = [0u8; 48];
    let mut ns = [0u8; 64];
    let mut nm = [0u8; 128];
    let (rl, nsl, ml) = route_parts(s, slot, &mut res, &mut ns, &mut nm);
    let verb = s.conns[slot].verb;
    let ol = s.conns[slot].obj_len as usize;
    let mut op = [0u8; MAX_OBJ + 512];
    let mut p = append(&mut op, 0, b"verb=");
    p = append(&mut op, p, verb_str(verb));
    p = append(&mut op, p, b";resource=");
    p = append(&mut op, p, &res[..rl]);
    p = append(&mut op, p, b";ns=");
    p = append(&mut op, p, &ns[..nsl]);
    p = append(&mut op, p, b";name=");
    p = append(&mut op, p, &nm[..ml]);
    p = append(&mut op, p, b";obj=");
    p = append(&mut op, p, &s.conns[slot].obj[..ol]);
    pipeline_send(s, slot, REQ_CORE, &op[..p], STAGE_CORE);
}

/// Start the watch stage: `prefix=/<resource>/<ns>/;since=<n>`.
unsafe fn start_watch(s: &mut State, slot: usize) {
    let mut res = [0u8; 48];
    let mut ns = [0u8; 64];
    let mut nm = [0u8; 128];
    let (rl, nsl, _ml) = route_parts(s, slot, &mut res, &mut ns, &mut nm);
    let since = s.conns[slot].since;
    let mut op = [0u8; 256];
    let mut p = append(&mut op, 0, b"prefix=/");
    p = append(&mut op, p, &res[..rl]);
    p = append(&mut op, p, b"/");
    p = append(&mut op, p, &ns[..nsl]);
    p = append(&mut op, p, b"/;since=");
    // u64 → decimal
    p = write_u64(&mut op, p, since);
    pipeline_send(s, slot, REQ_WATCH, &op[..p], STAGE_WATCH);
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

/// Render a pipeline error `(status, reason)` as a k8s Status and respond.
unsafe fn respond_error(s: &mut State, slot: usize, status: u16, body: &[u8]) {
    let id = s.conns[slot].id;
    let mut json = [0u8; 512];
    let jl = status_to_json(status, false, body, &mut json);
    http_respond(s, id, status, b"application/json", &json[..jl]);
    conn_free(s, slot);
}

/// Render the core backend result by verb and respond.
unsafe fn respond_core(s: &mut State, slot: usize, status: u16, body: &[u8]) {
    let id = s.conns[slot].id;
    let verb = s.conns[slot].verb;
    // get/create/update return the stored object verbatim (real nested JSON);
    // list wraps names as a k8s List; delete and errors render a Status.
    if status >= 400 {
        let mut json = [0u8; 512];
        let jl = status_to_json(status, false, body, &mut json);
        http_respond(s, id, status, b"application/json", &json[..jl]);
    } else if verb == VERB_LIST {
        let mut json = [0u8; MAX_OBJ];
        let jl = list_to_json(body, &mut json);
        http_respond(s, id, status, b"application/json", &json[..jl]);
    } else if verb == VERB_DELETE {
        let mut json = [0u8; 512];
        let jl = status_to_json(status, true, body, &mut json);
        http_respond(s, id, status, b"application/json", &json[..jl]);
    } else {
        http_respond(s, id, status, b"application/json", body);
    }
    conn_free(s, slot);
}

/// Render the watch batch and respond.
unsafe fn respond_watch(s: &mut State, slot: usize, body: &[u8]) {
    let id = s.conns[slot].id;
    let mut json = [0u8; 2048];
    let jl = watch_to_json(body, &mut json);
    http_respond(s, id, 200, b"application/json", &json[..jl]);
    conn_free(s, slot);
}

/// Advance one pipeline stage for a connection waiting on a store round-trip.
unsafe fn advance_pipeline(s: &mut State, slot: usize) {
    let mut buf = [0u8; MAX_OBJ];
    match s.conns[slot].stage {
        STAGE_AUTHN => {
            if let Some((_st, bl)) = pipeline_poll(s, slot, REQ_AUTHN, RESP_AUTHN, &mut buf) {
                // The authn body is the identity (`system:...` or `anonymous`);
                // authz decides on it either way.
                let il = bl.min(s.conns[slot].identity.len());
                s.conns[slot].identity[..il].copy_from_slice(&buf[..il]);
                s.conns[slot].ident_len = il as u16;
                start_authz(s, slot);
            }
        }
        STAGE_AUTHZ => {
            if let Some((st, bl)) = pipeline_poll(s, slot, REQ_AUTHZ, RESP_AUTHZ, &mut buf) {
                if st != 200 {
                    respond_error(s, slot, st, &buf[..bl]);
                } else if s.conns[slot].kind == ROUTE_WATCH {
                    start_watch(s, slot);
                } else if is_mutating(s.conns[slot].verb) {
                    start_admit(s, slot);
                } else {
                    start_core(s, slot);
                }
            }
        }
        STAGE_ADMIT => {
            if let Some((st, bl)) = pipeline_poll(s, slot, REQ_ADMIT, RESP_ADMIT, &mut buf) {
                if st != 200 {
                    respond_error(s, slot, st, &buf[..bl]);
                } else {
                    // Admission may default the object; carry it to the core write.
                    let ol = bl.min(s.conns[slot].obj.len());
                    s.conns[slot].obj[..ol].copy_from_slice(&buf[..ol]);
                    s.conns[slot].obj_len = ol as u16;
                    start_core(s, slot);
                }
            }
        }
        STAGE_CORE => {
            if let Some((st, bl)) = pipeline_poll(s, slot, REQ_CORE, RESP_CORE, &mut buf) {
                respond_core(s, slot, st, &buf[..bl]);
            }
        }
        STAGE_WATCH => {
            if let Some((_st, bl)) = pipeline_poll(s, slot, REQ_WATCH, RESP_WATCH, &mut buf) {
                respond_watch(s, slot, &buf[..bl]);
            }
        }
        _ => {}
    }
}

// ---- connection table -----------------------------------------------------

fn conn_slot(s: &mut State, id: u8) -> Option<usize> {
    (0..MAX_CONNS).find(|&i| s.conns[i].id == id)
}

fn conn_alloc(s: &mut State, id: u8) -> Option<usize> {
    let i = (0..MAX_CONNS).find(|&i| s.conns[i].id == NO_CONN)?;
    // Reset only the scalar cursor fields; the large buffers are overwritten as
    // used, never read past their live length.
    s.conns[i].id = id;
    s.conns[i].stage = STAGE_READING;
    s.conns[i].kind = ROUTE_NONE;
    s.conns[i].verb = VERB_NONE;
    s.conns[i].corr = 0;
    s.conns[i].req_len = 0;
    s.conns[i].body_at = 0;
    s.conns[i].content_len = 0;
    s.conns[i].cred_len = 0;
    s.conns[i].ident_len = 0;
    s.conns[i].obj_len = 0;
    s.conns[i].since = 0;
    Some(i)
}

fn conn_free(s: &mut State, slot: usize) {
    s.conns[slot].id = NO_CONN;
    s.conns[slot].stage = STAGE_READING;
    s.conns[slot].req_len = 0;
}

// ---- request handling -----------------------------------------------------

/// Once a full HTTP request is buffered, classify it and either dispatch a
/// responder round-trip or answer inline (404 / 501). Returns true if a store
/// round-trip is now in flight (stage advanced), false if already responded.
unsafe fn dispatch(s: &mut State, slot: usize) {
    let id = s.conns[slot].id;
    let body_at = s.conns[slot].body_at as usize;
    let content_len = s.conns[slot].content_len as usize;

    // Parse + route against the buffered request, extracting everything the
    // later stages need into locals so the borrow of `s.conns[slot].req` ends
    // before we mutate the connection or the store.
    let mut op_out = [0u8; MAX_VALUE];
    let mut cred_local = [0u8; 192];
    let mut obj_local = [0u8; MAX_OBJ];
    let mut op_len = 0usize;
    let mut cred_len = 0usize;
    let mut obj_len = 0usize;
    let mut since_val = 0u64;
    let mut kind = ROUTE_NONE;
    let mut verb = VERB_NONE;
    let mut malformed = false;
    let mut bad_body = false;
    let mut static_body: &'static [u8] = b"";
    let mut static_ct: &'static [u8] = b"application/json";
    {
        let mut scratch = [0u8; MAX_VALUE];
        let req_len = s.conns[slot].req_len as usize;
        match parse_http(&s.conns[slot].req[..req_len], body_at, content_len) {
            None => malformed = true,
            Some(http) if static_doc(http.method, http.path).is_some() => {
                if let Some((doc, ct)) = static_doc(http.method, http.path) {
                    static_body = doc;
                    static_ct = ct;
                }
            }
            Some(http) => {
                let routed = route(http.method, http.path, http.query, http.body, &mut scratch);
                kind = routed.kind;
                verb = routed.verb;
                if kind == ROUTE_RESPONDER {
                    op_len = routed.op.len().min(MAX_VALUE);
                    op_out[..op_len].copy_from_slice(&routed.op[..op_len]);
                } else if kind == ROUTE_CORE || kind == ROUTE_WATCH {
                    // Credential: a verified mTLS peer SVID wins over any bearer
                    // token (the stronger proof); absent one, the bearer; absent
                    // both, anonymous.
                    let mut phex = [0u8; 64];
                    let pl = peer_hex(s, s.conns[slot].id, &mut phex);
                    if pl > 0 {
                        let mut p = append(&mut cred_local, 0, b"peer=");
                        p = append(&mut cred_local, p, &phex[..pl]);
                        cred_len = p;
                    } else {
                        let tok = bearer(http.headers);
                        if !tok.is_empty() {
                            let mut p = append(&mut cred_local, 0, b"token=");
                            p = append(&mut cred_local, p, tok);
                            cred_len = p;
                        }
                    }
                    // Object body (create/update) → compact wire form.
                    if (verb == VERB_CREATE || verb == VERB_UPDATE) && !http.body.is_empty() {
                        // Store the object as raw JSON (real nested k8s objects).
                        let bl = http.body.len();
                        if bl > MAX_OBJ || http.body[0] != b'{' {
                            bad_body = true;
                        } else {
                            obj_local[..bl].copy_from_slice(http.body);
                            obj_len = bl;
                        }
                    }
                    // Watch fence.
                    if kind == ROUTE_WATCH {
                        if let Some(rv) = query_get(http.query, b"resourceVersion") {
                            let mut v = 0u64;
                            for &c in rv {
                                if c.is_ascii_digit() {
                                    v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
                                }
                            }
                            since_val = v;
                        }
                    }
                }
            }
        }
    }
    if malformed {
        http_respond(s, id, 400, b"text/plain", b"bad request");
        conn_free(s, slot);
        return;
    }
    if !static_body.is_empty() {
        // A discovery/version/openapi/metrics document — served directly,
        // unauthenticated.
        http_respond(s, id, 200, static_ct, static_body);
        conn_free(s, slot);
        return;
    }
    if bad_body {
        http_respond(s, id, 400, b"text/plain", b"invalid object body");
        conn_free(s, slot);
        return;
    }
    s.conns[slot].kind = kind;
    s.conns[slot].verb = verb;

    match kind {
        ROUTE_RESPONDER => {
            // Project /api-req/<corr> = <op>; poll for /api-resp/<corr>.
            let corr = s.seq;
            s.seq = s.seq.wrapping_add(1);
            s.conns[slot].corr = corr;
            let mut key = [0u8; MAX_KEY];
            let klen = corr_key(&mut key, REQ_API, corr);
            let sys = &*s.syscalls;
            if put_value(sys, &key[..klen], &op_out[..op_len]) {
                s.conns[slot].stage = STAGE_RESPONDER;
            } else {
                http_respond(s, id, 500, b"text/plain", b"store write failed");
                conn_free(s, slot);
            }
        }
        ROUTE_CORE | ROUTE_WATCH => {
            // Stash the pipeline carry on the connection, then start authn.
            s.conns[slot].cred[..cred_len].copy_from_slice(&cred_local[..cred_len]);
            s.conns[slot].cred_len = cred_len as u16;
            s.conns[slot].obj[..obj_len].copy_from_slice(&obj_local[..obj_len]);
            s.conns[slot].obj_len = obj_len as u16;
            s.conns[slot].since = since_val;
            start_authn(s, slot);
        }
        _ => {
            http_respond(s, id, 404, b"text/plain", b"not found");
            conn_free(s, slot);
        }
    }
}

/// Poll the responder response for a connection in STAGE_RESPONDER; when it
/// lands, render the HTTP response and retire the correlation.
unsafe fn poll_responder(s: &mut State, slot: usize) {
    let corr = s.conns[slot].corr;
    let id = s.conns[slot].id;
    let mut rkey = [0u8; MAX_KEY];
    let rlen = corr_key(&mut rkey, RESP_API, corr);
    let mut val = [0u8; MAX_VALUE];
    let sys = &*s.syscalls;
    let Some(n) = get_value(sys, &rkey[..rlen], &mut val) else {
        return; // not answered yet; try next tick
    };

    let (status, body) = split_status_body(&val[..n]);
    // Responder bodies are text/number/JSON already (getjson emits JSON).
    let ctype: &[u8] = if !body.is_empty() && body[0] == b'{' {
        b"application/json"
    } else {
        b"text/plain"
    };
    // Copy the body out before we start emitting (val is about to be reused).
    let mut bodybuf = [0u8; MAX_VALUE];
    let bl = body.len().min(MAX_VALUE);
    bodybuf[..bl].copy_from_slice(&body[..bl]);
    http_respond(s, id, status, ctype, &bodybuf[..bl]);

    // Retire both halves of the correlation.
    let mut qkey = [0u8; MAX_KEY];
    let qlen = corr_key(&mut qkey, REQ_API, corr);
    let sys = &*s.syscalls;
    delete_value(sys, &qkey[..qlen]);
    delete_value(sys, &rkey[..rlen]);

    s.served = s.served.wrapping_add(1);
    conn_free(s, slot);
}

// ---- net_in drain ---------------------------------------------------------

/// Drain `tls.peer_identity` envelopes, recording each conn_id → SVID binding.
unsafe fn poll_peer_in(s: &mut State) {
    if s.peer_in < 0 {
        return;
    }
    let sys = s.syscalls;
    let chan = s.peer_in;
    for _ in 0..FRAMES_PER_TICK {
        let poll = ((*sys).channel_poll)(chan, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        // Sized for the WHOLE peer-identity record: the fixed part, a
        // 64-byte fingerprint and a 256-byte principal. A buffer that cannot
        // hold one is not a short read — the frame header takes three bytes
        // of it, the length check then fails, no identity binds (every mTLS
        // client is anonymous), and the undelivered bytes stay in the FIFO
        // and desync the next frame.
        let mut buf = [0u8; 512];
        let (msg_type, plen) = net_read_frame(&*sys, chan, buf.as_mut_ptr(), buf.len());
        if msg_type == 0 {
            return;
        }
        // The payload (at buf[3]) is a record of typed facts — see
        // `MSG_PEER_IDENTITY` in fluxor's tls module:
        //   [session_id:4][result:1][credential_kind:1][profile_id:2]
        //   [not_before:8][not_after:8][flags:4][fp_alg:1][fp_len:1]
        //   [principal_len:2][fingerprint][principal]
        //
        // Typed facts rather than a `verified` boolean, which could not say
        // WHICH checks ran. A connection binds to a key fingerprint only when
        // the chain was validated and key possession proved: a fingerprint
        // from an untrusted or merely-copied certificate is not an identity.
        const PEER_FIXED: usize = 4 + 1 + 1 + 2 + 8 + 8 + 4 + 1 + 1 + 2;
        const RESULT_OK: u8 = 0;
        const CHECK_CHAIN: u32 = 0x0000_0001;
        const CHECK_KEY_POSSESSION: u32 = 0x0000_0010;
        if msg_type == MSG_PEER_IDENTITY && plen >= PEER_FIXED {
            let at = 3usize;
            // Slot ids are < 256, so the low byte is the connection id.
            let conn_id = buf[at];
            let result = buf[at + 4];
            let flags =
                u32::from_le_bytes([buf[at + 24], buf[at + 25], buf[at + 26], buf[at + 27]]);
            let fp_len = buf[at + 29] as usize;
            let fp_at = at + PEER_FIXED;
            let established = result == RESULT_OK
                && (flags & CHECK_CHAIN) != 0
                && (flags & CHECK_KEY_POSSESSION) != 0;
            if established
                && fp_len > 0
                && PEER_FIXED + fp_len <= plen
                && fp_at + fp_len <= buf.len()
            {
                let mut svid = [0u8; 32];
                let n = fp_len.min(svid.len());
                svid[..n].copy_from_slice(&buf[fp_at..fp_at + n]);
                peer_set(s, conn_id, &svid[..n]);
            }
        }
    }
}

unsafe fn poll_net_in(s: &mut State) {
    if s.net_in < 0 {
        return;
    }
    for _ in 0..FRAMES_PER_TICK {
        let sys = s.syscalls;
        let chan = s.net_in;
        let poll = ((*sys).channel_poll)(chan, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let buf = s.net_buf.as_mut_ptr();
        let (msg_type, payload_len) = net_read_frame(&*sys, chan, buf, NET_BUF_SIZE);
        if msg_type == 0 {
            return;
        }
        match msg_type {
            NET_MSG_BOUND => s.bound = 2,
            NET_MSG_BIND_REFUSED | NET_MSG_ERROR => s.bound = 0,
            NET_MSG_ACCEPTED => {
                // Payload: [conn_id:2 LE][listener_port:2 LE]. conn_id ids < 256
                // so the low byte is the id; the high byte is ignored.
                if payload_len >= 2 {
                    let id = *buf.add(3);
                    conn_alloc(s, id);
                }
            }
            NET_MSG_CLOSED => {
                if payload_len >= 2 {
                    let id = *buf.add(3);
                    if let Some(i) = conn_slot(s, id) {
                        conn_free(s, i);
                    }
                    peer_clear(s, id);
                }
            }
            NET_MSG_DATA => {
                if payload_len < 2 {
                    continue;
                }
                let id = *buf.add(3);
                let Some(slot) = conn_slot(s, id) else {
                    continue;
                };
                if s.conns[slot].stage != STAGE_READING {
                    continue; // already dispatched; ignore trailing bytes
                }
                // Append the data payload (past the 2-byte conn_id) to the request.
                let n = payload_len - 2;
                let at = s.conns[slot].req_len as usize;
                let take = n.min(REQ_BUF - at);
                for i in 0..take {
                    s.conns[slot].req[at + i] = *buf.add(5 + i);
                }
                s.conns[slot].req_len = (at + take) as u32;

                // Have we seen the header terminator yet?
                if s.conns[slot].body_at == 0 {
                    let rl = s.conns[slot].req_len as usize;
                    if let Some(h) = find(&s.conns[slot].req[..rl], b"\r\n\r\n") {
                        s.conns[slot].body_at = (h + 4) as u32;
                        let headers = &s.conns[slot].req[..h + 2];
                        s.conns[slot].content_len = content_length(headers);
                    }
                }
                // Full request present (headers + declared body)?
                if s.conns[slot].body_at != 0 {
                    let need = s.conns[slot].body_at + s.conns[slot].content_len;
                    if s.conns[slot].req_len >= need {
                        dispatch(s, slot);
                    }
                }
            }
            _ => {}
        }
    }
}

// ---- module entry points --------------------------------------------------

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
        s.net_in = in_chan;
        s.peer_in = -1;
        s.net_out = out_chan;
        s.bound = 0;
        s.resolved = 0;
        s.seq = 0;
        s.served = 0;
        for i in 0..MAX_CONNS {
            s.conns[i].id = NO_CONN;
            s.conns[i].stage = STAGE_READING;
            s.conns[i].req_len = 0;
            s.conns[i].body_at = 0;
            s.peer_ids[i] = PeerId::EMPTY;
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

        // Cold start: resolve the optional peer_identity input (port 1). Unwired
        // in the plaintext graph → stays -1, no mTLS peer identity.
        if s.resolved == 0 {
            s.peer_in = dev_channel_port(&*s.syscalls, PORT_INPUT, 1);
            s.resolved = 1;
        }

        if s.bound == 0 && net_send_bind(s) {
            s.bound = 1;
        }

        poll_peer_in(s);
        poll_net_in(s);

        // Advance any connection waiting on a store round-trip.
        for slot in 0..MAX_CONNS {
            if s.conns[slot].id == NO_CONN {
                continue;
            }
            match s.conns[slot].stage {
                STAGE_RESPONDER => poll_responder(s, slot),
                STAGE_AUTHN | STAGE_AUTHZ | STAGE_ADMIT | STAGE_CORE | STAGE_WATCH => {
                    advance_pipeline(s, slot)
                }
                _ => {}
            }
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
