//! The nanocloud CLI as a fluxor cli-applet fmod: ONE PIC module dispatches
//! nanocloud subcommands. The `cli` stack injects the host I/O surface
//! (cli_in/cli_out); this module reads argv (NUL-separated, from `args`),
//! routes on the subcommand, reads and writes the control-plane store
//! directly, writes output to `stdout` (→ cli_out.bytes_in), latches an exit
//! code on `exit` (→ cli_out.exit_in), and returns Done so the
//! run-to-completion CLI exits.
//!
//!   nanocloud status                     # cluster object counts
//!   nanocloud get <r> [<ns/name>]        # list object names, or show one object
//!   nanocloud describe <r> <ns/name>     # labelled read of one object
//!   nanocloud create <r> <ns/name> <json>  # store a new object (fails if present)
//!   nanocloud apply  <r> <ns/name> <json>  # upsert an object (create or replace)
//!   nanocloud delete <r> <ns/name>       # remove one object (a store mutation)
//!   nanocloud scale  <r> <ns/name> <n>   # set desired replicas
//!   nanocloud rollout status|undo <ns/name>  # rollout progress / rollback
//!   nanocloud watch <r>                  # stream a resource listing on change
//!   nanocloud logs [-f] <id>             # a sandbox's stdout/stderr (-f = follow)
//!   nanocloud exec [-it] <id> [--] <cmd> # run a command inside a sandbox (-it = pty)
//!   nanocloud diagnostics                # object counts across every resource
//!   nanocloud policy                     # NetworkPolicies + the compiled dataplane
//!   nanocloud volume                     # PersistentVolumeClaims + bound volumes
//!   nanocloud config                     # the CLI's effective context
//!   nanocloud bundle export|apply        # dump / restore every object as key+JSON
//!   nanocloud ca                         # print the cluster CA certificate
//!   nanocloud token <sa> [<ns>]          # mint a ServiceAccount token (JWT)
//!   nanocloud help                       # lists commands

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
include!("../_shared/json.rs");
include!("../_shared/store.rs");

const NS_LIST: u32 = 0x1302;
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const PORT_OUTPUT: u8 = 1;
const STEP_DONE: i32 = 1;

const MAX_VALUE: usize = 4096;

// Buffers live on the PIC module's (small) stack, so they stay modest and are
// never nested at full size — an over-large `out` + a nested `raw` overflows.
const MAX_KEY: usize = 128;
const LIST_BUF: usize = 1024;
const OUT_BUF: usize = 2048;
const ARGV_BUF: usize = 1024;
/// Steps to wait for the argv record before defaulting to `help` (cli_in emits
/// it early; an empty argv — no `--` — never arrives, so we fall through).
const ARGV_WAIT: u32 = 2000;

// Store prefixes (the group-qualified keys the API plane writes). Kept as
// direct `&[u8]` statics used inline — a PIC module does NOT relocate the inner
// pointers of a nested const table (`&[(&[u8], &[u8])]`), so iterating such a
// table dereferences unrelocated (garbage) pointers and faults.
const PODS_PREFIX: &[u8] = b"/pods/";
const DEPLOYMENTS_PREFIX: &[u8] = b"/deployments.apps/";
const REPLICASETS_PREFIX: &[u8] = b"/replicasets.apps/";
const DAEMONSETS_PREFIX: &[u8] = b"/daemonsets.apps/";
const STATEFULSETS_PREFIX: &[u8] = b"/statefulsets.apps/";
const JOBS_PREFIX: &[u8] = b"/jobs.batch/";
const NODES_PREFIX: &[u8] = b"/nodes/";
const SERVICES_PREFIX: &[u8] = b"/services/";
const ENDPOINTS_PREFIX: &[u8] = b"/endpoints/";
const NAMESPACES_PREFIX: &[u8] = b"/namespaces/";
const EVENTS_PREFIX: &[u8] = b"/events/";
const CONFIGMAPS_PREFIX: &[u8] = b"/configmaps/";
const SECRETS_PREFIX: &[u8] = b"/secrets/";
const SERVICEACCOUNTS_PREFIX: &[u8] = b"/serviceaccounts/";
const VOLUMESNAPSHOTS_PREFIX: &[u8] = b"/volumesnapshots.snapshot.storage.k8s.io/";
const PVCS_PREFIX: &[u8] = b"/persistentvolumeclaims/";
const DEVICES_PREFIX: &[u8] = b"/devices.nanocloud.io/";
const NETWORKPOLICIES_PREFIX: &[u8] = b"/networkpolicies/";
const BOUND_VOLUMES_PREFIX: &[u8] = b"/volumes/";
const DATAPLANE_NETPOLICY_KEY: &[u8] = b"/dataplane/netpolicy";
// Pod logs (published by sandbox_runner draining the workload READ op) + the
// sandbox lifecycle status (`state=<label>;code=<n>`) keyed by the same id.
const LOGS_PREFIX: &[u8] = b"/sandbox-logs/";
const SANDBOX_STATUS_PREFIX: &[u8] = b"/sandbox-status/";
/// Step backstop for `logs -f` so a never-terminating sandbox can't spin the
/// applet forever (each step is a cheap store read).
const STREAM_MAX: u32 = 600_000;

// The exec request/response seam (serviced by sandbox_runner running on the
// node): the CLI writes `/sandbox-exec/<id>/cli = <cmd>`, then polls
// `/sandbox-exec-result/<id>/cli` (exit code) + `/sandbox-exec-output/<id>/cli`
// (captured stdout/stderr), and deletes the trio when done.
const EXEC_REQ_PREFIX: &[u8] = b"/sandbox-exec/";
const EXEC_RESULT_PREFIX: &[u8] = b"/sandbox-exec-result/";
const EXEC_OUTPUT_PREFIX: &[u8] = b"/sandbox-exec-output/";
const EXEC_REQID: &[u8] = b"cli";
/// Poll backstop for `exec`: if no runner answers within this many steps the CLI
/// reports a timeout (rather than hang) — the node graph likely isn't running.
const EXEC_POLL_MAX: u32 = 5000;

// Interactive PTY exec seam (kubectl exec -it). The CLI opens
// `/tty/<id>/cli/ctl`, streams stdin → `/tty/<id>/cli/in`, PTY output ←
// `/tty/<id>/cli/out`, and finishes on `/tty/<id>/cli/status` = `exited;…`.
const TTY_PREFIX: &[u8] = b"/tty/";
const TTY_SID: &[u8] = b"cli";
const PORT_INPUT: u8 = 0;

// CA cert (published by cert_manager) + SA-token seam (serviced by sa_token).
const CA_CERT_KEY: &[u8] = b"/ca-cert";
const TOKEN_REQ_KEY: &[u8] = b"/token-req/cli";
const TOKEN_RESP_KEY: &[u8] = b"/token-resp/cli";

/// Stream modes for the persistent (multi-step) commands.
const STREAM_LOGS_FOLLOW: u8 = 1;
const STREAM_EXEC_POLL: u8 = 2;
const STREAM_TTY: u8 = 3;
const STREAM_TOKEN_POLL: u8 = 4;
const STREAM_WATCH: u8 = 5;
const STREAM_BUNDLE_APPLY: u8 = 6;
/// Sentinel prior-hash for `watch` so the first listing always emits.
const WATCH_HASH_INIT: u32 = 0xFFFF_FFFF;
/// Idle steps with no new stdin before `bundle apply` treats it as EOF + applies.
const BUNDLE_IDLE_EOF: u32 = 400;
const BUNDLE_BUF: usize = 8192;

/// Map a `get <resource>` name to its store prefix (direct literals — no table).
fn resource_prefix(resource: &[u8]) -> Option<&'static [u8]> {
    if resource == b"pods" {
        Some(PODS_PREFIX)
    } else if resource == b"deployments" {
        Some(DEPLOYMENTS_PREFIX)
    } else if resource == b"replicasets" {
        Some(REPLICASETS_PREFIX)
    } else if resource == b"daemonsets" {
        Some(DAEMONSETS_PREFIX)
    } else if resource == b"statefulsets" {
        Some(STATEFULSETS_PREFIX)
    } else if resource == b"jobs" {
        Some(JOBS_PREFIX)
    } else if resource == b"nodes" {
        Some(NODES_PREFIX)
    } else if resource == b"services" {
        Some(SERVICES_PREFIX)
    } else if resource == b"endpoints" {
        Some(ENDPOINTS_PREFIX)
    } else if resource == b"namespaces" {
        Some(NAMESPACES_PREFIX)
    } else if resource == b"events" {
        Some(EVENTS_PREFIX)
    } else if resource == b"configmaps" {
        Some(CONFIGMAPS_PREFIX)
    } else if resource == b"secrets" {
        Some(SECRETS_PREFIX)
    } else if resource == b"serviceaccounts" {
        Some(SERVICEACCOUNTS_PREFIX)
    } else if resource == b"volumesnapshots" {
        Some(VOLUMESNAPSHOTS_PREFIX)
    } else if resource == b"pvcs" || resource == b"persistentvolumeclaims" {
        Some(PVCS_PREFIX)
    } else if resource == b"devices" {
        Some(DEVICES_PREFIX)
    } else if resource == b"networkpolicies" || resource == b"netpol" {
        Some(NETWORKPOLICIES_PREFIX)
    } else {
        None
    }
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    /// argv channel (input port 0 ← cli_in.args_out).
    args_chan: i32,
    /// stdout channel (output port 0 → cli_out.bytes_in).
    out_chan: i32,
    /// exit-code channel (output port 1 → cli_out.exit_in).
    exit_chan: i32,
    waited: u32,
    done: u8,
    /// `logs -f` streaming: 1 = active. `stream_off` = bytes already emitted;
    /// `stream_steps` = backstop counter; `stream_id` = the sandbox/pod id.
    stream_mode: u8,
    stream_idlen: u8,
    stream_off: u32,
    stream_steps: u32,
    stream_id: [u8; 96],
    /// `exec -it` stdin: the stdin input port + a local buffer holding bytes not
    /// yet flushed to `/tty/<id>/cli/in` (flushed once the runner consumes prior).
    stdin_chan: i32,
    tty_pending_len: u16,
    tty_pending: [u8; 512],
    /// `watch`: hash of the last emitted listing (emit again only on change).
    watch_hash: u32,
    /// `bundle apply`: stdin bundle accumulated until EOF, then applied.
    bundle_len: u16,
    bundle_buf: [u8; BUNDLE_BUF],
}

// ---- store ----

/// storage.namespace LIST into `raw`; returns bytes written, or 0. Entries are
/// `[name_len:u8][kind:u8][name]`, closed by a `[0xFF][0xFF][cursor_len:u8]`
/// trailer — read them with `list_entry`, never by testing `name_len` alone.
unsafe fn ns_list(sys: &SyscallTable, prefix: &[u8], raw: &mut [u8]) -> usize {
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
    larg[p..p + 8].copy_from_slice(&(raw.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    larg[p..p + 4].copy_from_slice(&(raw.len() as u32).to_le_bytes());
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

unsafe fn count_prefix(sys: &SyscallTable, prefix: &[u8]) -> u32 {
    let mut raw = [0u8; LIST_BUF];
    let n = ns_list(sys, prefix, &mut raw);
    let mut rp = 0usize;
    let mut count = 0u32;
    while let Some((_, next)) = list_entry(&raw[..n], rp) {
        rp = next;
        count += 1;
    }
    count
}

/// The entry at `rp` in a LIST page, and the offset just past it; `None` at
/// the trailing record or the end of the page.
///
/// BOTH sentinel bytes decide the trailer. A name of exactly 255 bytes makes
/// `name_len` 0xFF as well, so testing that byte alone ends the listing one
/// entry early and loses every entry after it; `kind` never takes the value
/// 0xFF. One reader for every walk below, so the rule is stated once.
fn list_entry(page: &[u8], rp: usize) -> Option<(&[u8], usize)> {
    if rp >= page.len() {
        return None;
    }
    let nl = page[rp] as usize;
    if nl == 0xFF && rp + 1 < page.len() && page[rp + 1] == 0xFF {
        return None;
    }
    if rp + 2 + nl > page.len() {
        return None;
    }
    Some((&page[rp + 2..rp + 2 + nl], rp + 2 + nl))
}

/// Build a full store key `<prefix><subpath>` into `buf`; returns its length
/// (0 on overflow). e.g. prefix `/pods/` + subpath `default/web-0`.
fn build_key(prefix: &[u8], subpath: &[u8], buf: &mut [u8]) -> usize {
    let n = prefix.len() + subpath.len();
    if n > buf.len() {
        return 0;
    }
    buf[..prefix.len()].copy_from_slice(prefix);
    buf[prefix.len()..n].copy_from_slice(subpath);
    n
}

// ---- text helpers ----

fn append_u32(dst: &mut [u8], at: usize, mut n: u32) -> usize {
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

// ---- commands ----

unsafe fn cmd_status(sys: &SyscallTable, out: &mut [u8]) -> usize {
    let mut p = append(out, 0, b"nanocloud status\n");
    p = append(out, p, b"  nodes:        ");
    p = append_u32(out, p, count_prefix(sys, NODES_PREFIX));
    p = append(out, p, b"\n  deployments:  ");
    p = append_u32(out, p, count_prefix(sys, DEPLOYMENTS_PREFIX));
    p = append(out, p, b"\n  replicasets:  ");
    p = append_u32(out, p, count_prefix(sys, REPLICASETS_PREFIX));
    p = append(out, p, b"\n  pods:         ");
    p = append_u32(out, p, count_prefix(sys, PODS_PREFIX));
    p = append(out, p, b"\n");
    p
}

/// `diagnostics` — object counts across every resource type + a health line.
unsafe fn cmd_diagnostics(sys: &SyscallTable, out: &mut [u8]) -> usize {
    let rows: [(&[u8], &[u8]); 16] = [
        (b"nodes", NODES_PREFIX),
        (b"namespaces", NAMESPACES_PREFIX),
        (b"deployments", DEPLOYMENTS_PREFIX),
        (b"replicasets", REPLICASETS_PREFIX),
        (b"daemonsets", DAEMONSETS_PREFIX),
        (b"statefulsets", STATEFULSETS_PREFIX),
        (b"jobs", JOBS_PREFIX),
        (b"pods", PODS_PREFIX),
        (b"services", SERVICES_PREFIX),
        (b"endpoints", ENDPOINTS_PREFIX),
        (b"configmaps", CONFIGMAPS_PREFIX),
        (b"secrets", SECRETS_PREFIX),
        (b"serviceaccounts", SERVICEACCOUNTS_PREFIX),
        (b"networkpolicies", NETWORKPOLICIES_PREFIX),
        (b"volumesnapshots", VOLUMESNAPSHOTS_PREFIX),
        (b"events", EVENTS_PREFIX),
    ];
    let mut p = append(out, 0, b"nanocloud diagnostics\n");
    let ready = count_prefix(sys, NODES_PREFIX) > 0;
    p = append(out, p, b"  control-plane store: reachable\n");
    p = append(out, p, b"  nodes ready:         ");
    p = append(out, p, if ready { b"yes" } else { b"no" });
    p = append(out, p, b"\n  ---- object counts ----\n");
    for (label, prefix) in rows.iter() {
        p = append(out, p, b"  ");
        p = append(out, p, label);
        // pad to a column.
        let mut pad = 18usize.saturating_sub(label.len());
        while pad > 0 {
            p = append(out, p, b" ");
            pad -= 1;
        }
        p = append_u32(out, p, count_prefix(sys, prefix));
        p = append(out, p, b"\n");
    }
    p
}

/// `policy` — list NetworkPolicies + print the compiled dataplane ruleset.
unsafe fn cmd_policy(sys: &SyscallTable, out: &mut [u8]) -> usize {
    let (np, _) = cmd_get(sys, b"networkpolicies", out);
    let mut p = np;
    p = append(
        out,
        p,
        b"---- compiled dataplane (/dataplane/netpolicy) ----\n",
    );
    let mut val = [0u8; MAX_VALUE];
    match get_value(sys, DATAPLANE_NETPOLICY_KEY, &mut val) {
        Some(n) if n > 0 => {
            p = append(out, p, &val[..n]);
            append(out, p, b"\n")
        }
        _ => append(out, p, b"(not compiled yet)\n"),
    }
}

/// `volume` — list PersistentVolumeClaims and bound volumes.
unsafe fn cmd_volume(sys: &SyscallTable, out: &mut [u8]) -> usize {
    let mut p = append(out, 0, b"persistentvolumeclaims:\n");
    p = list_names_into(sys, PVCS_PREFIX, out, p);
    p = append(out, p, b"bound volumes:\n");
    list_names_into(sys, BOUND_VOLUMES_PREFIX, out, p)
}

/// List the final path segment of every key under `prefix`, one per line.
unsafe fn list_names_into(
    sys: &SyscallTable,
    prefix: &[u8],
    out: &mut [u8],
    mut p: usize,
) -> usize {
    let mut raw = [0u8; LIST_BUF];
    let n = ns_list(sys, prefix, &mut raw);
    let mut rp = 0usize;
    let mut any = false;
    while let Some((key, next)) = list_entry(&raw[..n], rp) {
        p = append(out, p, b"  ");
        p = append(out, p, last_seg(key));
        p = append(out, p, b"\n");
        any = true;
        rp = next;
    }
    if !any {
        p = append(out, p, b"  (none)\n");
    }
    p
}

/// `config` — the CLI's effective context (direct in-cluster store access).
fn cmd_config(out: &mut [u8]) -> usize {
    append(
        out,
        0,
        b"context:    in-cluster (direct control-plane store)\n\
          apiVersion: nanocloud.io/v1\n\
          note:       on-node CLI reads/writes the store directly; remote access is via the apiserver (HTTPS)\n",
    )
}

/// `rollout status <ns/name>` — how many pods have rolled to the current
/// pod-template-hash of the Deployment's ReplicaSet.
unsafe fn cmd_rollout_status(sys: &SyscallTable, subpath: &[u8], out: &mut [u8]) -> (usize, i32) {
    let mut rskey = [0u8; MAX_KEY];
    let rl = build_key(REPLICASETS_PREFIX, subpath, &mut rskey);
    if rl == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let mut rsval = [0u8; MAX_VALUE];
    let Some(rslen) = get_value(sys, &rskey[..rl], &mut rsval) else {
        return (
            append(out, 0, b"error: no replicaset for that deployment\n"),
            1,
        );
    };
    let Some(hash) = j_get3(
        &rsval[..rslen],
        b"metadata",
        b"labels",
        b"pod-template-hash",
    ) else {
        return (
            append(
                out,
                0,
                b"error: deployment has no rollout (no pod-template-hash)\n",
            ),
            1,
        );
    };
    let mut hbuf = [0u8; 16];
    let hl = hash.len().min(16);
    hbuf[..hl].copy_from_slice(&hash[..hl]);
    let replicas = j_u32_2(&rsval[..rslen], b"spec", b"replicas");
    let (ns, name) = split_ns_name(subpath);

    let mut updated = 0u32;
    for i in 0..replicas.min(64) {
        let mut pk = [0u8; MAX_KEY];
        let mut pp = append(&mut pk, 0, PODS_PREFIX);
        pp = append(&mut pk, pp, ns);
        pp = append(&mut pk, pp, b"/");
        pp = append(&mut pk, pp, name);
        pp = append(&mut pk, pp, b"-");
        pp = append_u32(&mut pk, pp, i);
        let mut pv = [0u8; MAX_VALUE];
        if let Some(pl) = get_value(sys, &pk[..pp], &mut pv) {
            if j_get3(&pv[..pl], b"metadata", b"labels", b"pod-template-hash") == Some(&hbuf[..hl])
            {
                updated += 1;
            }
        }
    }
    let mut p = append(out, 0, b"deployment ");
    p = append(out, p, name);
    p = append(out, p, b": ");
    p = append_u32(out, p, updated);
    p = append(out, p, b"/");
    p = append_u32(out, p, replicas);
    p = append(out, p, b" pods at revision ");
    p = append(out, p, &hbuf[..hl]);
    if updated >= replicas {
        (append(out, p, b"\nstatus: complete\n"), 0)
    } else {
        (append(out, p, b"\nstatus: progressing\n"), 0)
    }
}

/// `rollout undo <ns/name>` — restore the Deployment's previous template
/// (archived by deployment_reconciler at .../previous on the last rollout).
unsafe fn cmd_rollout_undo(sys: &SyscallTable, subpath: &[u8], out: &mut [u8]) -> (usize, i32) {
    let (ns, name) = split_ns_name(subpath);
    let mut ck = [0u8; MAX_KEY];
    let mut cl = append(&mut ck, 0, b"/controllerrevisions.apps/");
    cl = append(&mut ck, cl, ns);
    cl = append(&mut ck, cl, b"/");
    cl = append(&mut ck, cl, name);
    cl = append(&mut ck, cl, b"/previous");
    let mut cv = [0u8; MAX_VALUE];
    let Some(cvl) = get_value(sys, &ck[..cl], &mut cv) else {
        return (
            append(out, 0, b"error: no previous revision to roll back to\n"),
            1,
        );
    };
    let mut dk = [0u8; MAX_KEY];
    let dl = build_key(DEPLOYMENTS_PREFIX, subpath, &mut dk);
    if dl == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let mut dv = [0u8; MAX_VALUE];
    let Some(dvl) = get_value(sys, &dk[..dl], &mut dv) else {
        return (append(out, 0, b"error: deployment not found\n"), 1);
    };
    let mut nd = [0u8; MAX_VALUE];
    let ndl = j_set_top(&dv[..dvl], b"spec", &cv[..cvl], &mut nd);
    if ndl == 0 {
        return (append(out, 0, b"error: rollback failed\n"), 1);
    }
    if put_value(sys, &dk[..dl], &nd[..ndl]) {
        let mut p = append(out, 0, b"deployment ");
        p = append(out, p, name);
        (
            append(out, p, b" rolled back to the previous revision\n"),
            0,
        )
    } else {
        (append(out, 0, b"error: rollback write failed\n"), 1)
    }
}

/// Emit `key\tvalue\n` for every object under `prefix`, written directly to the
/// stdout channel (a full-cluster dump exceeds the fixed out buffer).
unsafe fn export_prefix(sys: &SyscallTable, out_chan: i32, prefix: &[u8]) {
    let mut raw = [0u8; LIST_BUF];
    let n = ns_list(sys, prefix, &mut raw);
    let mut rp = 0usize;
    while let Some((key, next)) = list_entry(&raw[..n], rp) {
        let mut val = [0u8; MAX_VALUE];
        if let Some(vlen) = get_value(sys, key, &mut val) {
            if out_chan >= 0 {
                let _ = (sys.channel_write)(out_chan, key.as_ptr(), key.len());
                let _ = (sys.channel_write)(out_chan, b"\t".as_ptr(), 1);
                let _ = (sys.channel_write)(out_chan, val.as_ptr(), vlen);
                let _ = (sys.channel_write)(out_chan, b"\n".as_ptr(), 1);
            }
        }
        rp = next;
    }
}

/// `bundle export` — dump every object across all resource types as
/// `<store-key>\t<json>` lines, streamed to stdout (a restorable snapshot).
unsafe fn cmd_export(s: &State, sys: &SyscallTable) {
    let prefixes: [&[u8]; 17] = [
        NAMESPACES_PREFIX,
        NODES_PREFIX,
        DEPLOYMENTS_PREFIX,
        REPLICASETS_PREFIX,
        DAEMONSETS_PREFIX,
        STATEFULSETS_PREFIX,
        JOBS_PREFIX,
        PODS_PREFIX,
        SERVICES_PREFIX,
        ENDPOINTS_PREFIX,
        CONFIGMAPS_PREFIX,
        SECRETS_PREFIX,
        SERVICEACCOUNTS_PREFIX,
        NETWORKPOLICIES_PREFIX,
        PVCS_PREFIX,
        VOLUMESNAPSHOTS_PREFIX,
        DEVICES_PREFIX,
    ];
    for prefix in prefixes {
        export_prefix(sys, s.out_chan, prefix);
    }
}

/// One step of `watch <resource>`: re-list the resource and, when the listing
/// changed, emit a fresh snapshot of names. Runs until the step backstop (or the
/// process is killed) — `kubectl get -w` semantics.
unsafe fn watch_step(s: &mut State, sys: &SyscallTable) -> i32 {
    s.stream_steps += 1;
    let reslen = s.stream_idlen as usize;
    let mut resbuf = [0u8; 96];
    resbuf[..reslen].copy_from_slice(&s.stream_id[..reslen]);
    let res = &resbuf[..reslen];
    let Some(prefix) = resource_prefix(res) else {
        if s.out_chan >= 0 {
            let m = b"error: unknown resource\n";
            let _ = (sys.channel_write)(s.out_chan, m.as_ptr(), m.len());
        }
        if s.exit_chan >= 0 {
            let c = 1i32.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    };
    let mut raw = [0u8; LIST_BUF];
    let n = ns_list(sys, prefix, &mut raw);
    let h = fnv1a(&raw[..n]);
    if h != s.watch_hash {
        s.watch_hash = h;
        if s.out_chan >= 0 {
            let hdr = b"---- ";
            let _ = (sys.channel_write)(s.out_chan, hdr.as_ptr(), hdr.len());
            let _ = (sys.channel_write)(s.out_chan, res.as_ptr(), res.len());
            let _ = (sys.channel_write)(s.out_chan, b" ----\n".as_ptr(), 6);
            let mut rp = 0usize;
            let mut any = false;
            while let Some((key, next)) = list_entry(&raw[..n], rp) {
                let name = last_seg(key);
                let _ = (sys.channel_write)(s.out_chan, name.as_ptr(), name.len());
                let _ = (sys.channel_write)(s.out_chan, b"\n".as_ptr(), 1);
                any = true;
                rp = next;
            }
            if !any {
                let e = b"(none)\n";
                let _ = (sys.channel_write)(s.out_chan, e.as_ptr(), e.len());
            }
        }
    }
    if s.stream_steps >= STREAM_MAX {
        if s.exit_chan >= 0 {
            let c = 0i32.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    }
    0
}

/// One step of `bundle apply` / `restore`: accumulate the stdin bundle, and once
/// stdin goes idle (EOF), parse each `<store-key>\t<json>` line and PUT the
/// object. Emits a count and exits.
unsafe fn bundle_apply_step(s: &mut State, sys: &SyscallTable) -> i32 {
    // Drain stdin into the bundle buffer.
    let mut got = false;
    if s.stdin_chan >= 0 {
        let mut buf = [0u8; 512];
        let n = (sys.channel_read)(s.stdin_chan, buf.as_mut_ptr(), buf.len());
        if n > 0 {
            let off = s.bundle_len as usize;
            let take = (n as usize).min(s.bundle_buf.len() - off);
            s.bundle_buf[off..off + take].copy_from_slice(&buf[..take]);
            s.bundle_len += take as u16;
            got = true;
            s.stream_steps = 0; // reset idle counter
        }
    }
    if got {
        return 0;
    }
    s.stream_steps += 1;
    if s.stream_steps < BUNDLE_IDLE_EOF {
        return 0; // wait for more stdin
    }

    // EOF: apply each `<key>\t<json>` line.
    let blen = s.bundle_len as usize;
    let mut applied = 0u32;
    let mut i = 0usize;
    while i < blen {
        // one line
        let end = {
            let mut j = i;
            while j < blen && s.bundle_buf[j] != b'\n' {
                j += 1;
            }
            j
        };
        let line_start = i;
        let line_end = end;
        i = end + 1;
        if line_end <= line_start {
            continue;
        }
        // split on the first tab
        let mut tab = line_start;
        while tab < line_end && s.bundle_buf[tab] != b'\t' {
            tab += 1;
        }
        if tab >= line_end {
            continue; // no tab → not a bundle line
        }
        // Copy key + value out (the buffer is borrowed for both put args).
        let mut key = [0u8; MAX_KEY];
        let klen = (tab - line_start).min(MAX_KEY);
        key[..klen].copy_from_slice(&s.bundle_buf[line_start..line_start + klen]);
        let mut val = [0u8; MAX_VALUE];
        let vsrc = tab + 1;
        let vlen = (line_end - vsrc).min(MAX_VALUE);
        val[..vlen].copy_from_slice(&s.bundle_buf[vsrc..vsrc + vlen]);
        if put_value(sys, &key[..klen], &val[..vlen]) {
            applied += 1;
        }
    }

    let mut out = [0u8; 64];
    let mut p = append(&mut out, 0, b"applied ");
    p = append_u32(&mut out, p, applied);
    p = append(&mut out, p, b" objects\n");
    if s.out_chan >= 0 {
        let _ = (sys.channel_write)(s.out_chan, out.as_ptr(), p);
    }
    if s.exit_chan >= 0 {
        let c = 0i32.to_le_bytes();
        let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
    }
    s.done = 1;
    STEP_DONE
}

/// `get <resource>` — one object name per line (the final key segment).
unsafe fn cmd_get(sys: &SyscallTable, resource: &[u8], out: &mut [u8]) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        p = append(out, p, b"'\n");
        return (p, 1);
    };
    let mut raw = [0u8; LIST_BUF];
    let n = ns_list(sys, prefix, &mut raw);
    let mut p = 0usize;
    let mut rp = 0usize;
    let mut any = false;
    while let Some((key, next)) = list_entry(&raw[..n], rp) {
        let name = last_seg(key);
        p = append(out, p, name);
        p = append(out, p, b"\n");
        any = true;
        rp = next;
    }
    if !any {
        p = append(out, p, b"(none)\n");
    }
    (p, 0)
}

/// `get <resource> <subpath>` — show one object's stored value (its fields).
unsafe fn cmd_get_one(
    sys: &SyscallTable,
    resource: &[u8],
    subpath: &[u8],
    out: &mut [u8],
) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        return (append(out, p, b"'\n"), 1);
    };
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(prefix, subpath, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let mut val = [0u8; MAX_VALUE];
    match get_value(sys, &key[..klen], &mut val) {
        Some(vlen) => {
            let mut p = append(out, 0, subpath);
            p = append(out, p, b"\t");
            p = append(out, p, &val[..vlen]);
            (append(out, p, b"\n"), 0)
        }
        None => {
            let mut p = append(out, 0, b"error: not found: ");
            p = append(out, p, &key[..klen]);
            (append(out, p, b"\n"), 1)
        }
    }
}

/// `delete <resource> <subpath>` — remove one object from the store (a mutation).
unsafe fn cmd_delete(
    sys: &SyscallTable,
    resource: &[u8],
    subpath: &[u8],
    out: &mut [u8],
) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        return (append(out, p, b"'\n"), 1);
    };
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(prefix, subpath, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    if delete_value(sys, &key[..klen]) {
        let mut p = append(out, 0, resource);
        p = append(out, p, b" \"");
        p = append(out, p, subpath);
        (append(out, p, b"\" deleted\n"), 0)
    } else {
        let mut p = append(out, 0, b"error: delete failed: ");
        p = append(out, p, &key[..klen]);
        (append(out, p, b"\n"), 1)
    }
}

/// Rewrite the `replicas=` field of a `;`-separated doc to `new`, preserving the
/// other fields (appends `replicas=` if absent); returns the new length.
fn set_replicas(value: &[u8], new: &[u8], out: &mut [u8]) -> usize {
    let mut p = 0usize;
    let mut start = 0usize;
    let mut first = true;
    let mut replaced = false;
    let mut i = 0usize;
    while i <= value.len() {
        if i == value.len() || value[i] == b';' {
            let field = &value[start..i];
            if !first {
                p = append(out, p, b";");
            }
            first = false;
            if field.len() >= 9 && &field[..9] == b"replicas=" {
                p = append(out, p, b"replicas=");
                p = append(out, p, new);
                replaced = true;
            } else {
                p = append(out, p, field);
            }
            start = i + 1;
        }
        i += 1;
    }
    if !replaced {
        if !first {
            p = append(out, p, b";");
        }
        p = append(out, p, b"replicas=");
        p = append(out, p, new);
    }
    p
}

/// `scale <resource> <ns/name> <replicas>` — read-modify-write of desired state:
/// GET the object, rewrite `replicas=`, PUT it back. The reconciler cascade
/// (deployment → replicaset → pods) then converges on the new count.
unsafe fn cmd_scale(
    sys: &SyscallTable,
    resource: &[u8],
    subpath: &[u8],
    replicas: &[u8],
    out: &mut [u8],
) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        return (append(out, p, b"'\n"), 1);
    };
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(prefix, subpath, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let mut val = [0u8; MAX_VALUE];
    let Some(vlen) = get_value(sys, &key[..klen], &mut val) else {
        let mut p = append(out, 0, b"error: not found: ");
        p = append(out, p, &key[..klen]);
        return (append(out, p, b"\n"), 1);
    };
    let mut newdoc = [0u8; MAX_VALUE];
    // Set spec.replicas (a numeric JSON value) on the JSON object.
    let nlen = j_set2(&val[..vlen], b"spec", b"replicas", replicas, &mut newdoc);
    if nlen == 0 {
        return (append(out, 0, b"error: object has no spec\n"), 1);
    }
    if put_value(sys, &key[..klen], &newdoc[..nlen]) {
        let mut p = append(out, 0, resource);
        p = append(out, p, b" \"");
        p = append(out, p, subpath);
        p = append(out, p, b"\" scaled to ");
        p = append(out, p, replicas);
        (append(out, p, b" replicas\n"), 0)
    } else {
        (append(out, 0, b"error: scale write failed\n"), 1)
    }
}

/// `logs <id>` — print the sandbox's captured stdout/stderr (published by
/// sandbox_runner at `/sandbox-logs/<id>`). One-shot; `logs -f` streams instead.
unsafe fn cmd_logs(sys: &SyscallTable, id: &[u8], out: &mut [u8]) -> (usize, i32) {
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(LOGS_PREFIX, id, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let mut val = [0u8; MAX_VALUE];
    match get_value(sys, &key[..klen], &mut val) {
        Some(vlen) if vlen > 0 => (append(out, 0, &val[..vlen]), 0),
        _ => (append(out, 0, b"(no logs)\n"), 0),
    }
}

/// Is a sandbox status doc (`state=<label>;code=<n>`) in a terminal state? A
/// terminal sandbox emits no more logs, so `logs -f` stops.
fn status_is_terminal(doc: &[u8]) -> bool {
    // Extract the `state=` field value (up to the first `;`).
    let after = match doc.windows(6).position(|w| w == b"state=") {
        Some(i) => &doc[i + 6..],
        None => return false,
    };
    let label = match after.iter().position(|&b| b == b';') {
        Some(i) => &after[..i],
        None => after,
    };
    label == b"exited" || label == b"failed" || label == b"killed" || label == b"destroyed"
}

/// One step of `logs -f`: emit any log bytes past `stream_off`, then stop once
/// the sandbox status is terminal (or the step backstop trips). Returns the
/// module_step result (0 = keep streaming, STEP_DONE = finished).
unsafe fn stream_step(s: &mut State, sys: &SyscallTable) -> i32 {
    s.stream_steps += 1;
    let idlen = s.stream_idlen as usize;
    let mut idbuf = [0u8; 96];
    idbuf[..idlen].copy_from_slice(&s.stream_id[..idlen]);
    let id = &idbuf[..idlen];

    // Emit new log bytes.
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(LOGS_PREFIX, id, &mut key);
    let mut val = [0u8; MAX_VALUE];
    let vlen = if klen == 0 {
        0
    } else {
        get_value(sys, &key[..klen], &mut val).unwrap_or(0)
    };
    let off = (s.stream_off as usize).min(vlen);
    if vlen > off && s.out_chan >= 0 {
        let _ = (sys.channel_write)(s.out_chan, val[off..vlen].as_ptr(), vlen - off);
    }
    s.stream_off = vlen as u32;

    // Terminal? then flush is complete — latch exit 0 and finish.
    let mut skey = [0u8; MAX_KEY];
    let sklen = build_key(SANDBOX_STATUS_PREFIX, id, &mut skey);
    let mut sval = [0u8; MAX_VALUE];
    let terminal = sklen != 0
        && match get_value(sys, &skey[..sklen], &mut sval) {
            Some(sl) => status_is_terminal(&sval[..sl]),
            None => false,
        };
    if terminal || s.stream_steps >= STREAM_MAX {
        if s.exit_chan >= 0 {
            let c = 0i32.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    }
    0
}

/// Build an exec seam key `<prefix><id>/cli` into `buf`; returns the length
/// (0 on overflow). The reqid is fixed (`cli`) — a single-node admin applet.
fn build_exec_key(prefix: &[u8], id: &[u8], buf: &mut [u8]) -> usize {
    let n = prefix.len() + id.len() + 1 + EXEC_REQID.len();
    if n > buf.len() {
        return 0;
    }
    let mut p = 0;
    buf[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    buf[p..p + id.len()].copy_from_slice(id);
    p += id.len();
    buf[p] = b'/';
    p += 1;
    buf[p..p + EXEC_REQID.len()].copy_from_slice(EXEC_REQID);
    p += EXEC_REQID.len();
    p
}

/// Parse a decimal (optionally negative) exit code.
fn parse_int(b: &[u8]) -> i32 {
    let (neg, mut i) = if !b.is_empty() && b[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };
    let mut v: i32 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        v = v.saturating_mul(10).saturating_add((b[i] - b'0') as i32);
        i += 1;
    }
    if neg {
        -v
    } else {
        v
    }
}

/// One step of `exec`: poll for the runner's result. When it appears, emit the
/// captured output, delete the request/result/output trio, latch the command's
/// exit code, and finish. Times out (exit 1) if no runner answers — the node
/// graph (with sandbox_runner) probably isn't running.
unsafe fn exec_poll_step(s: &mut State, sys: &SyscallTable) -> i32 {
    s.stream_steps += 1;
    let idlen = s.stream_idlen as usize;
    let mut idbuf = [0u8; 96];
    idbuf[..idlen].copy_from_slice(&s.stream_id[..idlen]);
    let id = &idbuf[..idlen];

    let mut rkey = [0u8; MAX_KEY];
    let rl = build_exec_key(EXEC_RESULT_PREFIX, id, &mut rkey);
    let mut rval = [0u8; 32];
    if rl != 0 {
        if let Some(vlen) = get_value(sys, &rkey[..rl], &mut rval) {
            // Emit the captured output.
            let mut okey = [0u8; MAX_KEY];
            let ol = build_exec_key(EXEC_OUTPUT_PREFIX, id, &mut okey);
            let mut oval = [0u8; MAX_VALUE];
            if ol != 0 {
                if let Some(olen) = get_value(sys, &okey[..ol], &mut oval) {
                    if olen > 0 && s.out_chan >= 0 {
                        let _ = (sys.channel_write)(s.out_chan, oval.as_ptr(), olen);
                    }
                }
            }
            let code = parse_int(&rval[..vlen]);
            // Clean up the seam so requests don't accumulate.
            let mut qkey = [0u8; MAX_KEY];
            let ql = build_exec_key(EXEC_REQ_PREFIX, id, &mut qkey);
            if ql != 0 {
                delete_value(sys, &qkey[..ql]);
            }
            delete_value(sys, &rkey[..rl]);
            if ol != 0 {
                delete_value(sys, &okey[..ol]);
            }
            if s.exit_chan >= 0 {
                let c = code.to_le_bytes();
                let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
            }
            s.done = 1;
            return STEP_DONE;
        }
    }
    if s.stream_steps >= EXEC_POLL_MAX {
        let msg = b"error: exec timed out (is the control-plane/node graph running?)\n";
        if s.out_chan >= 0 {
            let _ = (sys.channel_write)(s.out_chan, msg.as_ptr(), msg.len());
        }
        if s.exit_chan >= 0 {
            let c = 1i32.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    }
    0
}

/// Build a PTY session key `/tty/<id>/cli/<leaf>` into `buf`; length or 0.
fn build_tty_key(id: &[u8], leaf: &[u8], buf: &mut [u8]) -> usize {
    let n = TTY_PREFIX.len() + id.len() + 1 + TTY_SID.len() + 1 + leaf.len();
    if n > buf.len() {
        return 0;
    }
    let mut p = 0;
    buf[p..p + TTY_PREFIX.len()].copy_from_slice(TTY_PREFIX);
    p += TTY_PREFIX.len();
    buf[p..p + id.len()].copy_from_slice(id);
    p += id.len();
    buf[p] = b'/';
    p += 1;
    buf[p..p + TTY_SID.len()].copy_from_slice(TTY_SID);
    p += TTY_SID.len();
    buf[p] = b'/';
    p += 1;
    buf[p..p + leaf.len()].copy_from_slice(leaf);
    p += leaf.len();
    p
}

/// Extract the exit code from a `…;code=N` status doc.
fn status_code(v: &[u8]) -> i32 {
    match v.windows(5).position(|w| w == b"code=") {
        Some(i) => parse_int(&v[i + 5..]),
        None => 0,
    }
}

/// One step of `exec -it`: pump the interactive PTY session. Reads stdin from the
/// stdin port (buffering until the runner consumes the last chunk), flushes it to
/// `/tty/<id>/cli/in`, emits new PTY output from `/tty/<id>/cli/out`, and finishes
/// when the session status is terminal (exiting with the shell's code).
unsafe fn tty_step(s: &mut State, sys: &SyscallTable) -> i32 {
    s.stream_steps += 1;
    let idlen = s.stream_idlen as usize;
    let mut idbuf = [0u8; 96];
    idbuf[..idlen].copy_from_slice(&s.stream_id[..idlen]);
    let id = &idbuf[..idlen];

    // 1. Drain the stdin port into the local pending buffer.
    if s.stdin_chan >= 0 {
        let mut buf = [0u8; 512];
        let n = (sys.channel_read)(s.stdin_chan, buf.as_mut_ptr(), buf.len());
        if n > 0 {
            let off = s.tty_pending_len as usize;
            let take = (n as usize).min(s.tty_pending.len() - off);
            s.tty_pending[off..off + take].copy_from_slice(&buf[..take]);
            s.tty_pending_len += take as u16;
        }
    }
    // 2. Flush pending stdin once the runner has consumed the previous chunk.
    if s.tty_pending_len > 0 {
        let mut inkey = [0u8; MAX_KEY];
        let ik = build_tty_key(id, b"in", &mut inkey);
        if ik != 0 && !exists(sys, &inkey[..ik]) {
            let pl = s.tty_pending_len as usize;
            put_value(sys, &inkey[..ik], &s.tty_pending[..pl]);
            s.tty_pending_len = 0;
        }
    }
    // 3. Emit new PTY output.
    let mut okey = [0u8; MAX_KEY];
    let ok = build_tty_key(id, b"out", &mut okey);
    let mut oval = [0u8; MAX_VALUE];
    let olen = if ok != 0 {
        get_value(sys, &okey[..ok], &mut oval).unwrap_or(0)
    } else {
        0
    };
    let off = (s.stream_off as usize).min(olen);
    if olen > off && s.out_chan >= 0 {
        let _ = (sys.channel_write)(s.out_chan, oval[off..olen].as_ptr(), olen - off);
    }
    s.stream_off = olen as u32;
    // 4. Poll session status.
    let mut skey = [0u8; MAX_KEY];
    let sk = build_tty_key(id, b"status", &mut skey);
    let mut sval = [0u8; 32];
    let (terminal, code) = if sk != 0 {
        match get_value(sys, &skey[..sk], &mut sval) {
            Some(n) if sval[..n].starts_with(b"exited") || sval[..n].starts_with(b"failed") => {
                (true, status_code(&sval[..n]))
            }
            _ => (false, 0),
        }
    } else {
        (false, 0)
    };
    if terminal || s.stream_steps >= STREAM_MAX {
        if s.exit_chan >= 0 {
            let c = code.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    }
    0
}

/// One step of `token`: poll `/token-resp/cli` for the minted JWT. When it
/// appears, emit the token, delete the request/response, and exit. Times out
/// (exit 1) if no sa_token runner answers.
unsafe fn token_poll_step(s: &mut State, sys: &SyscallTable) -> i32 {
    s.stream_steps += 1;
    let mut rval = [0u8; MAX_VALUE];
    if let Some(n) = get_value(sys, TOKEN_RESP_KEY, &mut rval) {
        let v = &rval[..n];
        let tok = if v.starts_with(b"token=") { &v[6..] } else { v };
        if s.out_chan >= 0 {
            let _ = (sys.channel_write)(s.out_chan, tok.as_ptr(), tok.len());
            let _ = (sys.channel_write)(s.out_chan, b"\n".as_ptr(), 1);
        }
        delete_value(sys, TOKEN_REQ_KEY);
        delete_value(sys, TOKEN_RESP_KEY);
        if s.exit_chan >= 0 {
            let c = 0i32.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    }
    if s.stream_steps >= EXEC_POLL_MAX {
        let msg = b"error: token mint timed out (is the control-plane graph running?)\n";
        if s.out_chan >= 0 {
            let _ = (sys.channel_write)(s.out_chan, msg.as_ptr(), msg.len());
        }
        if s.exit_chan >= 0 {
            let c = 1i32.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        return STEP_DONE;
    }
    0
}

/// `ca` — print the cluster CA certificate (DER hex, published by cert_manager).
unsafe fn cmd_ca(sys: &SyscallTable, out: &mut [u8]) -> (usize, i32) {
    let mut val = [0u8; MAX_VALUE];
    match get_value(sys, CA_CERT_KEY, &mut val) {
        Some(n) if n > 0 => {
            let p = append(out, 0, &val[..n]);
            (append(out, p, b"\n"), 0)
        }
        _ => (
            append(out, 0, b"(no CA certificate; is cert_manager running?)\n"),
            1,
        ),
    }
}

/// Split a `<namespace>/<name>` subpath into its two segments. Cluster-scoped
/// objects (nodes, namespaces) have no `/`, so the namespace is empty.
fn split_ns_name(subpath: &[u8]) -> (&[u8], &[u8]) {
    match subpath.iter().position(|&b| b == b'/') {
        Some(i) => (&subpath[..i], &subpath[i + 1..]),
        None => (&[], subpath),
    }
}

/// `describe <resource> <ns/name>` — a labelled read: Name / Namespace headers
/// followed by the object's stored JSON body. A friendlier `get <r> <ns/name>`.
unsafe fn cmd_describe(
    sys: &SyscallTable,
    resource: &[u8],
    subpath: &[u8],
    out: &mut [u8],
) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        return (append(out, p, b"'\n"), 1);
    };
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(prefix, subpath, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let mut val = [0u8; MAX_VALUE];
    let Some(vlen) = get_value(sys, &key[..klen], &mut val) else {
        let mut p = append(out, 0, b"error: not found: ");
        p = append(out, p, &key[..klen]);
        return (append(out, p, b"\n"), 1);
    };
    let (ns, name) = split_ns_name(subpath);
    let mut p = append(out, 0, b"Name:       ");
    p = append(out, p, name);
    p = append(out, p, b"\nNamespace:  ");
    p = append(out, p, if ns.is_empty() { b"(cluster)" } else { ns });
    p = append(out, p, b"\nResource:   ");
    p = append(out, p, resource);
    p = append(out, p, b"\n\n");
    p = append(out, p, &val[..vlen]);
    (append(out, p, b"\n"), 0)
}

/// Validate a JSON-object body — must be non-empty and open with `{`. Cheap
/// guard so `create`/`apply` don't store obvious garbage as an object.
fn is_json_object(body: &[u8]) -> bool {
    let t = j_ws(body, 0);
    t < body.len() && body[t] == b'{'
}

/// `create <resource> <ns/name> <json>` — store a new object, failing if the key
/// already exists (like `kubectl create`). A direct store write, same model as
/// `delete`/`scale`; the reconcilers then act on it.
unsafe fn cmd_create(
    sys: &SyscallTable,
    resource: &[u8],
    subpath: &[u8],
    body: &[u8],
    out: &mut [u8],
) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        return (append(out, p, b"'\n"), 1);
    };
    if !is_json_object(body) {
        return (append(out, 0, b"error: body must be a JSON object\n"), 1);
    }
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(prefix, subpath, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    if exists(sys, &key[..klen]) {
        let mut p = append(out, 0, resource);
        p = append(out, p, b" \"");
        p = append(out, p, subpath);
        return (append(out, p, b"\" already exists\n"), 1);
    }
    if put_value(sys, &key[..klen], body) {
        let mut p = append(out, 0, resource);
        p = append(out, p, b" \"");
        p = append(out, p, subpath);
        (append(out, p, b"\" created\n"), 0)
    } else {
        (append(out, 0, b"error: create write failed\n"), 1)
    }
}

/// `apply <resource> <ns/name> <json>` — upsert the object (create or replace),
/// like `kubectl apply`. Reports created vs configured by probing first.
unsafe fn cmd_apply(
    sys: &SyscallTable,
    resource: &[u8],
    subpath: &[u8],
    body: &[u8],
    out: &mut [u8],
) -> (usize, i32) {
    let Some(prefix) = resource_prefix(resource) else {
        let mut p = append(out, 0, b"error: unknown resource '");
        p = append(out, p, resource);
        return (append(out, p, b"'\n"), 1);
    };
    if !is_json_object(body) {
        return (append(out, 0, b"error: body must be a JSON object\n"), 1);
    }
    let mut key = [0u8; MAX_KEY];
    let klen = build_key(prefix, subpath, &mut key);
    if klen == 0 {
        return (append(out, 0, b"error: name too long\n"), 1);
    }
    let existed = exists(sys, &key[..klen]);
    if put_value(sys, &key[..klen], body) {
        let mut p = append(out, 0, resource);
        p = append(out, p, b" \"");
        p = append(out, p, subpath);
        (
            append(
                out,
                p,
                if existed {
                    b"\" configured\n"
                } else {
                    b"\" created\n"
                },
            ),
            0,
        )
    } else {
        (append(out, 0, b"error: apply write failed\n"), 1)
    }
}

fn cmd_help(out: &mut [u8]) -> usize {
    append(
        out,
        0,
        b"nanocloud (fluxor cli-applet)\n\
          commands:\n\
          \x20 status                          cluster object counts\n\
          \x20 get <resource>                  list object names (pods, deployments, nodes, ...)\n\
          \x20 get <resource> <ns/name>        show one object's stored fields\n\
          \x20 describe <resource> <ns/name>   labelled read of one object\n\
          \x20 create <resource> <ns/name> <json>  store a new object (fails if it exists)\n\
          \x20 apply <resource> <ns/name> <json>   upsert an object (create or replace)\n\
          \x20 delete <resource> <ns/name>     remove one object (a store mutation)\n\
          \x20 scale <resource> <ns/name> <n>  set desired replicas (read-modify-write)\n\
          \x20 rollout status|undo <ns/name>   deployment rollout status / rollback\n\
          \x20 logs [-f] <id>                  a sandbox's stdout/stderr (-f = follow)\n\
          \x20 exec [-it] <id> [--] <command>  run a command inside a sandbox (-it = pty)\n\
          \x20 ca                              print the cluster CA certificate\n\
          \x20 token <serviceaccount> [<ns>]  mint a ServiceAccount token (JWT)\n\
          \x20 diagnostics                     object counts across every resource\n\
          \x20 policy                          NetworkPolicies + compiled dataplane\n\
          \x20 volume                          PersistentVolumeClaims + bound volumes\n\
          \x20 config                          the CLI's effective context\n\
          \x20 watch <resource>                stream a resource listing on change\n\
          \x20 bundle export                   dump every object (key\\tjson lines)\n\
          \x20 bundle apply / restore          create objects from a bundle on stdin\n\
          \x20 help                            this message\n",
    )
}

/// Split a NUL-separated argv record into slices (up to `MAXARGS`).
fn split_argv(rec: &[u8], out: &mut [(usize, usize); 16]) -> usize {
    let mut n = 0;
    let mut start = 0;
    let mut i = 0;
    while i <= rec.len() && n < out.len() {
        if i == rec.len() || rec[i] == 0 {
            if i > start {
                out[n] = (start, i);
                n += 1;
            }
            start = i + 1;
        }
        i += 1;
    }
    n
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
        s.args_chan = in_chan; // input port 0 ← cli_in.args_out
        s.out_chan = out_chan; // output port 0 → cli_out.bytes_in
        s.exit_chan = -1; // output port 1 → cli_out.exit_in (resolved lazily)
        s.waited = 0;
        s.done = 0;
        s.stream_mode = 0;
        s.stream_idlen = 0;
        s.stream_off = 0;
        s.stream_steps = 0;
        s.stream_id = [0u8; 96];
        s.stdin_chan = -1; // input port 1 ← cli_in.stdin_out (resolved lazily)
        s.tty_pending_len = 0;
        s.tty_pending = [0u8; 512];
        s.watch_hash = WATCH_HASH_INIT;
        s.bundle_len = 0;
        s.bundle_buf = [0u8; BUNDLE_BUF];
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return STEP_DONE;
        }
        let s = &mut *(state as *mut State);
        if s.syscalls.is_null() {
            return STEP_DONE;
        }
        let sys = &*s.syscalls;
        if s.done != 0 {
            return STEP_DONE;
        }
        if s.exit_chan < 0 {
            s.exit_chan = dev_channel_port(sys, PORT_OUTPUT, 1);
        }

        if s.stdin_chan < 0 {
            s.stdin_chan = dev_channel_port(sys, PORT_INPUT, 1);
        }

        // Persistent (multi-step) commands: once armed, each step advances the
        // stream — `logs -f` emits log bytes; `exec` polls; `exec -it` pumps a pty.
        if s.stream_mode != 0 {
            return match s.stream_mode {
                STREAM_EXEC_POLL => exec_poll_step(s, sys),
                STREAM_TTY => tty_step(s, sys),
                STREAM_TOKEN_POLL => token_poll_step(s, sys),
                STREAM_WATCH => watch_step(s, sys),
                STREAM_BUNDLE_APPLY => bundle_apply_step(s, sys),
                _ => stream_step(s, sys),
            };
        }

        // Read the argv record (one NUL-separated record from cli_in). Retry a
        // bounded number of steps; an empty argv (no `--`) never arrives, so we
        // fall through to `help`.
        let mut arec = [0u8; ARGV_BUF];
        let an = if s.args_chan >= 0 {
            (sys.channel_read)(s.args_chan, arec.as_mut_ptr(), arec.len())
        } else {
            0
        };
        if an <= 0 {
            s.waited += 1;
            if s.waited < ARGV_WAIT {
                return 0; // keep waiting for argv
            }
        }
        let alen = if an > 0 { an as usize } else { 0 };

        let mut argv = [(0usize, 0usize); 16];
        let argc = split_argv(&arec[..alen], &mut argv);

        let mut out = [0u8; OUT_BUF];
        let (olen, code): (usize, i32) = if argc == 0 {
            (cmd_help(&mut out), 0)
        } else {
            let (s0, e0) = argv[0];
            let sub = &arec[s0..e0];
            if sub == b"status" {
                (cmd_status(sys, &mut out), 0)
            } else if sub == b"get" {
                if argc >= 3 {
                    let (s1, e1) = argv[1];
                    let (s2, e2) = argv[2];
                    cmd_get_one(sys, &arec[s1..e1], &arec[s2..e2], &mut out)
                } else if argc >= 2 {
                    let (s1, e1) = argv[1];
                    cmd_get(sys, &arec[s1..e1], &mut out)
                } else {
                    (append(&mut out, 0, b"error: get needs a resource\n"), 1)
                }
            } else if sub == b"describe" {
                if argc >= 3 {
                    let (s1, e1) = argv[1];
                    let (s2, e2) = argv[2];
                    cmd_describe(sys, &arec[s1..e1], &arec[s2..e2], &mut out)
                } else {
                    (
                        append(&mut out, 0, b"error: describe needs <resource> <ns/name>\n"),
                        1,
                    )
                }
            } else if sub == b"create" {
                if argc >= 4 {
                    let (s1, e1) = argv[1];
                    let (s2, e2) = argv[2];
                    let (s3, e3) = argv[3];
                    cmd_create(sys, &arec[s1..e1], &arec[s2..e2], &arec[s3..e3], &mut out)
                } else {
                    (
                        append(
                            &mut out,
                            0,
                            b"error: create needs <resource> <ns/name> <json>\n",
                        ),
                        1,
                    )
                }
            } else if sub == b"apply" {
                if argc >= 4 {
                    let (s1, e1) = argv[1];
                    let (s2, e2) = argv[2];
                    let (s3, e3) = argv[3];
                    cmd_apply(sys, &arec[s1..e1], &arec[s2..e2], &arec[s3..e3], &mut out)
                } else {
                    (
                        append(
                            &mut out,
                            0,
                            b"error: apply needs <resource> <ns/name> <json>\n",
                        ),
                        1,
                    )
                }
            } else if sub == b"delete" {
                if argc >= 3 {
                    let (s1, e1) = argv[1];
                    let (s2, e2) = argv[2];
                    cmd_delete(sys, &arec[s1..e1], &arec[s2..e2], &mut out)
                } else {
                    (
                        append(&mut out, 0, b"error: delete needs <resource> <ns/name>\n"),
                        1,
                    )
                }
            } else if sub == b"scale" {
                if argc >= 4 {
                    let (s1, e1) = argv[1];
                    let (s2, e2) = argv[2];
                    let (s3, e3) = argv[3];
                    cmd_scale(sys, &arec[s1..e1], &arec[s2..e2], &arec[s3..e3], &mut out)
                } else {
                    (
                        append(
                            &mut out,
                            0,
                            b"error: scale needs <resource> <ns/name> <replicas>\n",
                        ),
                        1,
                    )
                }
            } else if sub == b"logs" {
                // logs [-f] <id>
                let follow = argc >= 2 && &arec[argv[1].0..argv[1].1] == b"-f";
                let id_idx = if follow { 2 } else { 1 };
                if argc > id_idx {
                    let (is, ie) = argv[id_idx];
                    let id = &arec[is..ie];
                    if follow {
                        // Arm streaming — stream_step emits from the next step on.
                        let n = id.len().min(s.stream_id.len());
                        s.stream_id[..n].copy_from_slice(&id[..n]);
                        s.stream_idlen = n as u8;
                        s.stream_off = 0;
                        s.stream_steps = 0;
                        s.stream_mode = STREAM_LOGS_FOLLOW;
                        (0usize, 0i32)
                    } else {
                        cmd_logs(sys, id, &mut out)
                    }
                } else {
                    (append(&mut out, 0, b"error: logs needs [-f] <id>\n"), 1)
                }
            } else if sub == b"exec" {
                // exec [-it|-i|-t] <id> [--] <command...>
                let mut ai = 1;
                let mut interactive = false;
                if ai < argc {
                    let a1 = &arec[argv[ai].0..argv[ai].1];
                    if a1.first() == Some(&b'-') {
                        interactive = a1.iter().any(|&c| c == b't' || c == b'i');
                        ai += 1;
                    }
                }
                if ai >= argc {
                    (
                        append(&mut out, 0, b"error: exec needs <id> [--] <command>\n"),
                        1,
                    )
                } else {
                    let (is, ie) = argv[ai];
                    let id = &arec[is..ie];
                    let mut ti = ai + 1;
                    if ti < argc && &arec[argv[ti].0..argv[ti].1] == b"--" {
                        ti += 1;
                    }
                    if ti >= argc {
                        (append(&mut out, 0, b"error: exec needs a command\n"), 1)
                    } else {
                        // Assemble the space-joined command line.
                        let mut cbuf = [0u8; MAX_VALUE];
                        let mut cl = 0usize;
                        let mut first = true;
                        for &(s2, e2) in &argv[ti..argc] {
                            if !first && cl < cbuf.len() {
                                cbuf[cl] = b' ';
                                cl += 1;
                            }
                            first = false;
                            let tok = &arec[s2..e2];
                            if cl + tok.len() > cbuf.len() {
                                break;
                            }
                            cbuf[cl..cl + tok.len()].copy_from_slice(tok);
                            cl += tok.len();
                        }
                        if interactive {
                            // Open a PTY session and stream it (tty_step drives it).
                            let mut ctl = [0u8; MAX_VALUE];
                            let mut cp = append(&mut ctl, 0, b"open;cmd=");
                            cp = append(&mut ctl, cp, &cbuf[..cl]);
                            let mut ckey = [0u8; MAX_KEY];
                            let ckl = build_tty_key(id, b"ctl", &mut ckey);
                            if ckl == 0 {
                                (append(&mut out, 0, b"error: name too long\n"), 1)
                            } else if put_value(sys, &ckey[..ckl], &ctl[..cp]) {
                                let n = id.len().min(s.stream_id.len());
                                s.stream_id[..n].copy_from_slice(&id[..n]);
                                s.stream_idlen = n as u8;
                                s.stream_off = 0;
                                s.stream_steps = 0;
                                s.tty_pending_len = 0;
                                s.stream_mode = STREAM_TTY;
                                (0usize, 0i32)
                            } else {
                                (append(&mut out, 0, b"error: tty open write failed\n"), 1)
                            }
                        } else {
                            let mut rkey = [0u8; MAX_KEY];
                            let rl = build_exec_key(EXEC_REQ_PREFIX, id, &mut rkey);
                            if rl == 0 {
                                (append(&mut out, 0, b"error: name too long\n"), 1)
                            } else if put_value(sys, &rkey[..rl], &cbuf[..cl]) {
                                // Arm exec-poll mode; exec_poll_step drives the rest.
                                let n = id.len().min(s.stream_id.len());
                                s.stream_id[..n].copy_from_slice(&id[..n]);
                                s.stream_idlen = n as u8;
                                s.stream_steps = 0;
                                s.stream_mode = STREAM_EXEC_POLL;
                                (0usize, 0i32)
                            } else {
                                (
                                    append(&mut out, 0, b"error: exec request write failed\n"),
                                    1,
                                )
                            }
                        }
                    }
                }
            } else if sub == b"bundle" {
                // bundle export | bundle apply (stdin)
                let action = if argc >= 2 {
                    &arec[argv[1].0..argv[1].1]
                } else {
                    b"".as_slice()
                };
                if action == b"export" {
                    cmd_export(s, sys);
                    (0usize, 0i32)
                } else if action == b"apply" {
                    s.stream_steps = 0;
                    s.bundle_len = 0;
                    s.stream_mode = STREAM_BUNDLE_APPLY;
                    (0usize, 0i32)
                } else {
                    (append(&mut out, 0, b"usage: bundle export|apply\n"), 1)
                }
            } else if sub == b"restore" {
                // restore = apply a bundle (from `bundle export`) on stdin.
                s.stream_steps = 0;
                s.bundle_len = 0;
                s.stream_mode = STREAM_BUNDLE_APPLY;
                (0usize, 0i32)
            } else if sub == b"watch" {
                // watch <resource>  (stream a resource's listing on change)
                if argc >= 2 {
                    let (s1, e1) = argv[1];
                    let res = &arec[s1..e1];
                    let n = res.len().min(s.stream_id.len());
                    s.stream_id[..n].copy_from_slice(&res[..n]);
                    s.stream_idlen = n as u8;
                    s.stream_steps = 0;
                    s.watch_hash = WATCH_HASH_INIT;
                    s.stream_mode = STREAM_WATCH;
                    (0usize, 0i32)
                } else {
                    (append(&mut out, 0, b"error: watch needs a resource\n"), 1)
                }
            } else if sub == b"rollout" {
                // rollout status <ns/name> | rollout undo <ns/name>
                if argc >= 3 {
                    let action = &arec[argv[1].0..argv[1].1];
                    let (s2, e2) = argv[2];
                    let subpath = &arec[s2..e2];
                    if action == b"status" {
                        cmd_rollout_status(sys, subpath, &mut out)
                    } else if action == b"undo" {
                        cmd_rollout_undo(sys, subpath, &mut out)
                    } else {
                        (
                            append(&mut out, 0, b"usage: rollout status|undo <ns/name>\n"),
                            1,
                        )
                    }
                } else {
                    (
                        append(&mut out, 0, b"usage: rollout status|undo <ns/name>\n"),
                        1,
                    )
                }
            } else if sub == b"diagnostics" {
                (cmd_diagnostics(sys, &mut out), 0)
            } else if sub == b"policy" {
                (cmd_policy(sys, &mut out), 0)
            } else if sub == b"volume" {
                (cmd_volume(sys, &mut out), 0)
            } else if sub == b"config" {
                (cmd_config(&mut out), 0)
            } else if sub == b"install" || sub == b"uninstall" {
                (
                    append(
                        &mut out,
                        0,
                        b"nanocloud is fluxor-native and installs as a Debian package:\n\
                          \x20 install:   apt install nanocloud   (control plane = the `nanocloud` systemd unit,\n\
                          \x20                                     `fluxor run controlplane.yaml`; CLI = `fluxor exec nanocloud`)\n\
                          \x20 uninstall: apt remove nanocloud\n",
                    ),
                    0,
                )
            } else if sub == b"ca" {
                cmd_ca(sys, &mut out)
            } else if sub == b"token" {
                // token <serviceaccount> [<namespace>]
                if argc >= 2 {
                    let (s1, e1) = argv[1];
                    let sa = &arec[s1..e1];
                    let ns: &[u8] = if argc >= 3 {
                        let (s2, e2) = argv[2];
                        &arec[s2..e2]
                    } else {
                        b"default"
                    };
                    let mut req = [0u8; MAX_VALUE];
                    let mut rp = append(&mut req, 0, b"ns=");
                    rp = append(&mut req, rp, ns);
                    rp = append(&mut req, rp, b";sa=");
                    rp = append(&mut req, rp, sa);
                    if put_value(sys, TOKEN_REQ_KEY, &req[..rp]) {
                        s.stream_steps = 0;
                        s.stream_mode = STREAM_TOKEN_POLL;
                        (0usize, 0i32)
                    } else {
                        (
                            append(&mut out, 0, b"error: token request write failed\n"),
                            1,
                        )
                    }
                } else {
                    (
                        append(
                            &mut out,
                            0,
                            b"error: token needs <serviceaccount> [<namespace>]\n",
                        ),
                        1,
                    )
                }
            } else if sub == b"help" {
                (cmd_help(&mut out), 0)
            } else {
                let mut p = append(&mut out, 0, b"error: unknown command '");
                p = append(&mut out, p, sub);
                p = append(&mut out, p, b"' (try `help`)\n");
                (p, 1)
            }
        };

        // `logs -f` armed streaming instead of a one-shot answer: don't finalize
        // (no exit latch / Done yet) — subsequent steps stream via stream_step.
        if s.stream_mode != 0 {
            return 0;
        }

        if s.out_chan >= 0 && olen > 0 {
            let _ = (sys.channel_write)(s.out_chan, out.as_ptr(), olen);
        }
        if s.exit_chan >= 0 {
            let c = code.to_le_bytes();
            let _ = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
        }
        s.done = 1;
        STEP_DONE
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
