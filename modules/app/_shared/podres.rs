// A Pod's container as its node runs it: the kubelet's half of the
// Kubernetes semantics a flat node record cannot say by itself, transcribed
// once on the control plane (`pod_resolve`), `include!`d after `json.rs` and
// `pathmod.rs` (whose `ArgvOut`, `pm_quantity`, `pm_json_argv`, `pm_dec` and
// `pm_overflow` it uses). Pure: what it reads past the Pod comes through a
// [`PrWorld`] — the store, for the module; fixtures, for the harness.
//
// What it decides, by upstream's rules (kubelet `makeEnvironmentVariables`,
// `ExpandContainerCommandAndArgs`, the API server's defaults):
//
//   env       the Service links of the Pod's namespace (`<SVC>_SERVICE_HOST`,
//             `_SERVICE_PORT`, `_SERVICE_PORT_<NAME>`, `<SVC>_PORT`,
//             `_PORT_<p>_<PROTO>[_PROTO|_PORT|_ADDR]`, only for a Service
//             with a cluster IP; the default namespace's `kubernetes` always,
//             the rest unless `enableServiceLinks: false`), then `envFrom`
//             (a ConfigMap's `data`, a Secret's decoded `data`, `prefix`ed),
//             then `env` in order — `value` with `$(VAR)` expanded against
//             what is defined so far and the Service links (`$$` is `$`, an
//             unknown reference is left as written), or `valueFrom` (a
//             ConfigMap or Secret key, the downward API's fieldRef and
//             resourceFieldRef). A later definition overrides an earlier one
//             of its name; a Service link never overrides the container's own.
//             A source that is missing and not `optional` is
//             CreateContainerConfigError, the container not run
//   cmd/args  each element JSON-decoded, then `$(VAR)` expanded against the
//             container's environment (not the image's), as an argv
//   user      securityContext `runAsUser` / `runAsGroup` / `runAsNonRoot`, the
//             container's over the Pod's (`ruid`, `rgid`, `nonroot`: the node
//             composes them with the image's User and refuses root);
//             `readOnlyRootFilesystem`, `supplementalGroups` and `privileged`
//             the node cannot give, so a container asking for them is refused
//             rather than run without them
//   volumes   each volumeMount against the Pod's volumes, as the node's list
//             (`volume.uproc`): a PersistentVolumeClaim `<ns>/<claim>`, a
//             hostPath `<path>`, an emptyDir `@e/<vol>`, a ConfigMap
//             `@c/<vol>/<ns>/<name>/<mode>/<optional>[/<key>=<path>|…]`, a
//             Secret `@s/…` the same way — each `:<mountPath>[:ro]`
//   restart   the Pod's policy (`always` unless stated); an init container's
//             is `onfailure` unless the Pod's is `never`
//
// A field is never cut short: what does not fit is OVERFLOW (`%00`), which the
// node refuses (`pathmod.rs`).

const PR_MAX_ENV: usize = 160;
const PR_ENV_ARENA: usize = 12 * 1024;
/// A ConfigMap, Secret, Namespace or Node read whole.
const PR_OBJ: usize = 16 * 1024;
/// One Service.
const PR_SVC: usize = 4096;
/// A value decoded, or expanded.
const PR_VAL: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PrLook {
    Found(usize),
    Absent,
    /// Not answered yet: the transcription is asked again later.
    Pending,
}

/// What a transcription reads past the Pod.
trait PrWorld {
    /// `/<coll>/<ns>/<name>` (`/<coll>/<name>` when `ns` is empty) into `out`.
    fn object(&mut self, coll: &[u8], ns: &[u8], name: &[u8], out: &mut [u8]) -> PrLook;
    /// The value at `key`, whole (a record that is not an object's: the
    /// cluster DNS's address at `/cluster-dns`).
    fn raw(&mut self, key: &[u8], out: &mut [u8]) -> PrLook;
    /// Each Service of namespace `ns`, read into `buf` and handed to `f`.
    /// `Pending` when the listing (or a Service in it) is not answered yet.
    fn services<F: FnMut(&[u8])>(&mut self, ns: &[u8], buf: &mut [u8], f: F) -> PrLook;
}

/// An environment being built: entries in definition order, a later one of
/// a name overriding an earlier.
struct PrEnv {
    arena: [u8; PR_ENV_ARENA],
    used: usize,
    /// `[offset, name length, value length]`.
    ent: [[u16; 3]; PR_MAX_ENV],
    n: usize,
    over: bool,
}

impl PrEnv {
    fn clear(&mut self) {
        self.used = 0;
        self.n = 0;
        self.over = false;
    }

    fn push(&mut self, name: &[u8], value: &[u8]) {
        let need = name.len() + value.len();
        if self.n >= PR_MAX_ENV || self.used + need > PR_ENV_ARENA {
            self.over = true;
            return;
        }
        let at = self.used;
        let dst = match self.arena.get_mut(at..at + need) {
            Some(d) => d,
            None => {
                self.over = true;
                return;
            }
        };
        for (d, &c) in dst.iter_mut().zip(name.iter().chain(value.iter())) {
            *d = c;
        }
        if let Some(e) = self.ent.get_mut(self.n) {
            *e = [at as u16, name.len() as u16, value.len() as u16];
        }
        self.used += need;
        self.n += 1;
    }

    fn name(&self, i: usize) -> &[u8] {
        let e = self.ent.get(i).copied().unwrap_or([0, 0, 0]);
        let a = e[0] as usize;
        self.arena.get(a..a + e[1] as usize).unwrap_or(&[])
    }

    fn value(&self, i: usize) -> &[u8] {
        let e = self.ent.get(i).copied().unwrap_or([0, 0, 0]);
        let a = e[0] as usize + e[1] as usize;
        self.arena.get(a..a + e[2] as usize).unwrap_or(&[])
    }

    /// The latest definition of `name`.
    fn get(&self, name: &[u8]) -> Option<&[u8]> {
        (0..self.n)
            .rev()
            .find(|&i| self.name(i) == name)
            .map(|i| self.value(i))
    }

    /// Whether entry `i` is overridden by a later one of its name.
    fn shadowed(&self, i: usize) -> bool {
        let n = self.name(i);
        (i + 1..self.n).any(|k| self.name(k) == n)
    }
}

/// Everything a transcription works in, kept off the stack.
struct PrScratch {
    env: PrEnv,
    svc: PrEnv,
    obj: [u8; PR_OBJ],
    sbuf: [u8; PR_SVC],
    val: [u8; PR_VAL],
    val2: [u8; PR_VAL],
    /// The container's probes record (`/pod-probes/<id>`), as [`pr_spec`]
    /// left it: 0 when it has none (or is being deleted).
    probes: [u8; PR_PROBES],
    probes_len: usize,
    /// PersistentVolumeClaims the container's volumes need created (a
    /// StatefulSet's claim templates, [`pr_sts_claim`]): JSON documents, one
    /// a line.
    claims: [u8; PR_CLAIMS],
    claims_len: usize,
}

const PR_CLAIMS: usize = 4096;

/// A container's probes record: three probes' argvs and parameters.
const PR_PROBES: usize = 2048;

/// A probe as the node's probe chain runs it: an argv on the exec seam. An
/// `exec` probe's command; an `httpGet` as `@http <host> <port> <path>
/// [<name>:<value>…]` (its headers) and a `tcpSocket` as `@tcp <host>
/// <port>`, which the node's dialer (probe_dial) answers rather than the
/// sandbox. `<host>` is the probe's, else the Pod's IP — its node's, whose
/// network it shares — a named port the container's port of that name.
/// Nothing for a probe the node cannot run (grpc, an HTTPS httpGet, a port
/// that names no port): it is not run, as before.
fn pr_probe_cmd(ctr: &[u8], probe: &[u8], host_ip: &[u8], o: &mut PrOut) {
    if let Some(cmd) = pr_m(pr_m(probe, b"exec").unwrap_or(b"{}"), b"command") {
        let start = o.n;
        let rest = o.b.get_mut(start..).unwrap_or(&mut []);
        let n = pm_json_argv(cmd, rest).unwrap_or(0);
        o.n = start + n;
        return;
    }
    let (http, h) = match (pr_m(probe, b"httpGet"), pr_m(probe, b"tcpSocket")) {
        (Some(h), _) => (true, h),
        (None, Some(t)) => (false, t),
        _ => return,
    };
    let scheme = pr_ms(h, b"scheme");
    if http && !scheme.is_empty() && scheme != b"HTTP" {
        return;
    }
    let port_raw = pr_m(h, b"port").unwrap_or(b"");
    let mut pb = [0u8; 8];
    let port: &[u8] = if port_raw.first() == Some(&b'"') {
        let name = pr_s(Some(port_raw));
        let ports = pr_m(ctr, b"ports").unwrap_or(b"[]");
        match pr_named(Some(ports), name) {
            Some(p) => pr_ms(p, b"containerPort"),
            None if name.iter().all(|c| c.is_ascii_digit()) => name,
            None => return,
        }
    } else {
        let d = pr_s(Some(port_raw));
        let n = d.len().min(8);
        for (o, &c) in pb.iter_mut().zip(d) {
            *o = c;
        }
        pb.get(..n).unwrap_or(&[])
    };
    if port.is_empty() || !port.iter().all(|c| c.is_ascii_digit()) {
        return;
    }
    let host = pr_ms(h, b"host");
    let host: &[u8] = if !host.is_empty() {
        host
    } else if !host_ip.is_empty() {
        host_ip
    } else {
        b"127.0.0.1"
    };
    let start = o.n;
    let mut full = false;
    {
        let rest = o.b.get_mut(start..).unwrap_or(&mut []);
        let mut aw = ArgvOut::new(rest);
        let tok = |aw: &mut ArgvOut, t: &[u8]| {
            for &c in t {
                aw.byte(c);
            }
            aw.next_token();
        };
        if http {
            tok(&mut aw, b"@http");
        } else {
            tok(&mut aw, b"@tcp");
        }
        tok(&mut aw, host);
        tok(&mut aw, port);
        if http {
            let path = pr_ms(h, b"path");
            if path.is_empty() {
                tok(&mut aw, b"/");
            } else {
                tok(&mut aw, path);
            }
            let hdrs = pr_m(h, b"httpHeaders").unwrap_or(b"[]");
            let mut k = 0usize;
            while let Some(hd) = pr_at(hdrs, k) {
                k += 1;
                for &c in pr_ms(hd, b"name")
                    .iter()
                    .chain(b":".iter())
                    .chain(pr_ms(hd, b"value").iter())
                {
                    aw.byte(c);
                }
                aw.next_token();
            }
        }
        if aw.full {
            full = true;
        } else {
            o.n = start + aw.len();
        }
    }
    if full {
        o.n = start;
        o.lit(b"%00");
    }
}

/// The container's probes record: `rp_*` (readiness), `lp_*` (liveness),
/// `sp_*` (startup) — each its argv ([`pr_probe_cmd`]) and periodSeconds,
/// timeoutSeconds, failureThreshold, successThreshold, initialDelaySeconds
/// as stated (empty: the probe chain's defaults). Nothing when the container
/// has no probe the node runs.
fn pr_probes(ctr: &[u8], host_ip: &[u8], o: &mut PrOut) {
    let start = o.n;
    let mut any = pr_probe_fields(ctr, b"readinessProbe", b"rp_", host_ip, o);
    o.byte(b';');
    any |= pr_probe_fields(ctr, b"livenessProbe", b"lp_", host_ip, o);
    o.byte(b';');
    any |= pr_probe_fields(ctr, b"startupProbe", b"sp_", host_ip, o);
    if !any {
        o.n = start;
    }
}

/// One probe's fields (see [`pr_probes`]); whether it has a command.
fn pr_probe_fields(ctr: &[u8], member: &[u8], pre: &[u8], host_ip: &[u8], o: &mut PrOut) -> bool {
    let probe = pr_m(ctr, member).unwrap_or(b"{}");
    o.lit(pre);
    o.lit(b"cmd=");
    let at = o.n;
    pr_probe_cmd(ctr, probe, host_ip, o);
    let any = o.n > at;
    o.byte(b';');
    o.lit(pre);
    o.lit(b"period=");
    o.lit(pr_ms(probe, b"periodSeconds"));
    o.byte(b';');
    o.lit(pre);
    o.lit(b"timeout=");
    o.lit(pr_ms(probe, b"timeoutSeconds"));
    o.byte(b';');
    o.lit(pre);
    o.lit(b"fail=");
    o.lit(pr_ms(probe, b"failureThreshold"));
    o.byte(b';');
    o.lit(pre);
    o.lit(b"succ=");
    o.lit(pr_ms(probe, b"successThreshold"));
    o.byte(b';');
    o.lit(pre);
    o.lit(b"delay=");
    o.lit(pr_ms(probe, b"initialDelaySeconds"));
    any
}

/// A record being written: bytes, or OVERFLOW once it does not fit.
struct PrOut<'a> {
    b: &'a mut [u8],
    n: usize,
    over: bool,
}

impl PrOut<'_> {
    fn lit(&mut self, s: &[u8]) {
        for &c in s {
            self.byte(c);
        }
    }

    fn byte(&mut self, c: u8) {
        match self.b.get_mut(self.n) {
            Some(slot) => {
                *slot = c;
                self.n += 1;
            }
            None => self.over = true,
        }
    }

    /// `%`, `,` and `;` escaped (`!esc`): a scalar inside a `k=v;` record or
    /// a `,` list.
    fn esc(&mut self, s: &[u8]) {
        for &c in s {
            match c {
                b'%' => self.lit(b"%25"),
                b',' => self.lit(b"%2C"),
                b';' => self.lit(b"%3B"),
                _ => self.byte(c),
            }
        }
    }

    fn dec(&mut self, v: u64) {
        let mut d = [0u8; 20];
        let n = pm_dec(v, &mut d);
        self.lit(d.get(..n).unwrap_or(&[]));
    }
}

/// The raw value of member `k` of the object `b` (a string with its quotes,
/// an object with its braces).
fn pr_m<'a>(b: &'a [u8], k: &[u8]) -> Option<&'a [u8]> {
    let i = j_obj_get(b, 0, k)?;
    let e = j_skip(b, i);
    b.get(i..e)
}

/// Element `n` of the array `b`, raw.
fn pr_at(b: &[u8], n: usize) -> Option<&[u8]> {
    let i = j_arr_get(b, 0, n)?;
    let e = j_skip(b, i);
    b.get(i..e)
}

/// A raw member's scalar: a string's content (still escaped), a literal's
/// bytes; EMPTY for an object or array.
fn pr_s(raw: Option<&[u8]>) -> &[u8] {
    match raw {
        Some(r) if !matches!(r.first(), Some(b'{') | Some(b'[')) => j_scalar(r, 0),
        _ => &[],
    }
}

/// Member `k` of `b` as a scalar (see [`pr_s`]).
/// The longest command or args rendering the node's chains carry (a
/// projected field, a decision's scratch): past it the field is OVERFLOW
/// (`%00`), which the spawn refuses (CreateContainerError), never a cut
/// command.
const PR_ARGV_MAX: usize = 512;

fn pr_ms<'a>(b: &'a [u8], k: &[u8]) -> &'a [u8] {
    pr_s(pr_m(b, k))
}

fn pr_true(b: &[u8], k: &[u8]) -> bool {
    pr_ms(b, k) == b"true"
}

/// A JSON string's escaped content decoded into `out`. `None` when it does
/// not fit.
fn pr_unescape(s: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut o = 0usize;
    let mut i = 0usize;
    let mut put = |o: &mut usize, c: u8| -> Option<()> {
        *out.get_mut(*o)? = c;
        *o += 1;
        Some(())
    };
    while i < s.len() {
        let c = *s.get(i)?;
        if c != b'\\' {
            put(&mut o, c)?;
            i += 1;
            continue;
        }
        let e = *s.get(i + 1)?;
        i += 2;
        match e {
            b'n' => put(&mut o, b'\n')?,
            b't' => put(&mut o, b'\t')?,
            b'r' => put(&mut o, b'\r')?,
            b'b' => put(&mut o, 0x08)?,
            b'f' => put(&mut o, 0x0c)?,
            b'u' => {
                let mut v = 0u32;
                for k in 0..4 {
                    v = (v << 4) | u32::from(argv_hex(*s.get(i + k)?)?);
                }
                i += 4;
                // A surrogate pair: the low half follows as `\uDCxx`.
                if (0xD800..0xDC00).contains(&v)
                    && s.get(i) == Some(&b'\\')
                    && s.get(i + 1) == Some(&b'u')
                {
                    let mut lo = 0u32;
                    for k in 0..4 {
                        lo = (lo << 4) | u32::from(argv_hex(*s.get(i + 2 + k)?)?);
                    }
                    if (0xDC00..0xE000).contains(&lo) {
                        v = 0x10000 + ((v - 0xD800) << 10) + (lo - 0xDC00);
                        i += 6;
                    }
                }
                let ch = char::from_u32(v).unwrap_or('\u{FFFD}');
                let mut utf = [0u8; 4];
                for &b in ch.encode_utf8(&mut utf).as_bytes() {
                    put(&mut o, b)?;
                }
            }
            other => put(&mut o, other)?,
        }
    }
    Some(o)
}

/// Standard base64 (a Secret's `data`) decoded into `out`; `None` when it is
/// not base64 or does not fit.
fn pr_b64(s: &[u8], out: &mut [u8]) -> Option<usize> {
    let val = |c: u8| -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    };
    let mut o = 0usize;
    let mut acc = 0u32;
    let mut bits = 0u32;
    for &c in s {
        if c == b'=' || c == b'\n' || c == b'\r' {
            continue;
        }
        acc = (acc << 6) | val(c)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            *out.get_mut(o)? = (acc >> bits) as u8;
            o += 1;
            acc &= (1 << bits) - 1;
        }
    }
    Some(o)
}

/// `$(VAR)` expansion (upstream's `third_party/forked/golang/expansion`):
/// `$$` is a literal `$`, `$(NAME)` the value `look` finds — or itself, as
/// written, when it finds none — and `$` before anything else is itself.
/// `None` when the result does not fit `out`.
fn pr_expand<'v, L: Fn(&[u8]) -> Option<&'v [u8]>>(
    src: &[u8],
    look: L,
    out: &mut [u8],
) -> Option<usize> {
    let mut o = 0usize;
    let mut put = |o: &mut usize, s: &[u8]| -> Option<()> {
        for &c in s {
            *out.get_mut(*o)? = c;
            *o += 1;
        }
        Some(())
    };
    let mut i = 0usize;
    while i < src.len() {
        let c = *src.get(i)?;
        if c != b'$' || i + 1 >= src.len() {
            put(&mut o, core::slice::from_ref(&c))?;
            i += 1;
            continue;
        }
        let nx = *src.get(i + 1)?;
        if nx == b'$' {
            put(&mut o, b"$")?;
            i += 2;
        } else if nx == b'(' {
            match src.get(i + 2..)?.iter().position(|&b| b == b')') {
                Some(r) => {
                    let name = src.get(i + 2..i + 2 + r)?;
                    match look(name) {
                        Some(v) => put(&mut o, v)?,
                        None => put(&mut o, src.get(i..i + 3 + r)?)?,
                    }
                    i += 3 + r;
                }
                None => {
                    put(&mut o, b"$(")?;
                    i += 2;
                }
            }
        } else {
            put(&mut o, src.get(i..i + 2)?)?;
            i += 2;
        }
    }
    Some(o)
}

/// A Service's name as upstream spells its variables: upper case, `-` as `_`.
fn pr_svc_name(name: &[u8], out: &mut [u8]) -> usize {
    let mut n = 0usize;
    for (o, &c) in out.iter_mut().zip(name) {
        *o = if c == b'-' {
            b'_'
        } else {
            c.to_ascii_uppercase()
        };
        n += 1;
    }
    n
}

/// One Service's link variables into `env` (kubelet `envvars.FromServices`).
fn pr_svc_env(svc: &[u8], env: &mut PrEnv) {
    let Some(meta) = pr_m(svc, b"metadata") else {
        return;
    };
    let Some(spec) = pr_m(svc, b"spec") else {
        return;
    };
    let ip = pr_ms(spec, b"clusterIP");
    if ip.is_empty() || ip == b"None" {
        return;
    }
    let mut up = [0u8; 80];
    let un = pr_svc_name(pr_ms(meta, b"name"), &mut up);
    let base = up.get(..un).unwrap_or(&[]);
    let ports = pr_m(spec, b"ports").unwrap_or(b"[]");
    let mut nm = [0u8; 160];
    let mut vb = [0u8; 96];
    let name = |parts: &[&[u8]], nm: &mut [u8; 160]| -> usize {
        let mut n = 0usize;
        for p in parts {
            for &c in p.iter() {
                if let Some(s) = nm.get_mut(n) {
                    *s = c;
                    n += 1;
                }
            }
        }
        n
    };
    let mut k = 0usize;
    while let Some(p) = pr_at(ports, k) {
        let port = pr_ms(p, b"port");
        let proto_raw = pr_ms(p, b"protocol");
        let mut proto = [0u8; 8];
        let pl = if proto_raw.is_empty() {
            proto[..3].copy_from_slice(b"TCP");
            3
        } else {
            let n = proto_raw.len().min(8);
            for (d, &c) in proto.iter_mut().zip(proto_raw) {
                *d = c.to_ascii_uppercase();
            }
            n
        };
        let proto_u = proto.get(..pl).unwrap_or(&[]);
        let mut lower = [0u8; 8];
        for (d, &c) in lower.iter_mut().zip(proto_u) {
            *d = c.to_ascii_lowercase();
        }
        let proto_l = lower.get(..pl).unwrap_or(&[]);
        // <proto>://<ip>:<port>
        let mut ul = 0usize;
        for &c in proto_l
            .iter()
            .chain(b"://".iter())
            .chain(ip.iter())
            .chain(b":".iter())
            .chain(port.iter())
        {
            if let Some(s) = vb.get_mut(ul) {
                *s = c;
                ul += 1;
            }
        }
        let url = vb;
        let url = url.get(..ul).unwrap_or(&[]);
        if k == 0 {
            let n = name(&[base, b"_SERVICE_HOST"], &mut nm);
            env.push(nm.get(..n).unwrap_or(&[]), ip);
            let n = name(&[base, b"_SERVICE_PORT"], &mut nm);
            env.push(nm.get(..n).unwrap_or(&[]), port);
            let n = name(&[base, b"_PORT"], &mut nm);
            env.push(nm.get(..n).unwrap_or(&[]), url);
        }
        let pname = pr_ms(p, b"name");
        if !pname.is_empty() {
            let mut pu = [0u8; 64];
            let pn = pr_svc_name(pname, &mut pu);
            let n = name(
                &[base, b"_SERVICE_PORT_", pu.get(..pn).unwrap_or(&[])],
                &mut nm,
            );
            env.push(nm.get(..n).unwrap_or(&[]), port);
        }
        let n = name(&[base, b"_PORT_", port, b"_", proto_u], &mut nm);
        env.push(nm.get(..n).unwrap_or(&[]), url);
        let n = name(&[base, b"_PORT_", port, b"_", proto_u, b"_PROTO"], &mut nm);
        env.push(nm.get(..n).unwrap_or(&[]), proto_l);
        let n = name(&[base, b"_PORT_", port, b"_", proto_u, b"_PORT"], &mut nm);
        env.push(nm.get(..n).unwrap_or(&[]), port);
        let n = name(&[base, b"_PORT_", port, b"_", proto_u, b"_ADDR"], &mut nm);
        env.push(nm.get(..n).unwrap_or(&[]), ip);
        k += 1;
    }
}

/// Why a container cannot be created as asked (CreateContainerConfigError),
/// as upstream words it: written once, the first reason found, already
/// escaped as a JSON string's content (the Pod's status quotes it).
struct PrErr {
    msg: [u8; 192],
    n: usize,
}

impl PrErr {
    fn set(&mut self, parts: &[&[u8]]) {
        if self.n != 0 {
            return;
        }
        for p in parts {
            for &c in p.iter() {
                // The record's own separators never reach the message, and it
                // is written as the JSON string the Pod's status will quote.
                let c = match c {
                    b';' | b',' | b'%' | b'\n' | b'\r' | b'\t' => b' ',
                    _ => c,
                };
                if c == b'"' || c == b'\\' {
                    if self.n + 2 > self.msg.len() {
                        return;
                    }
                    if let Some(s) = self.msg.get_mut(self.n) {
                        *s = b'\\';
                        self.n += 1;
                    }
                }
                if let Some(s) = self.msg.get_mut(self.n) {
                    *s = c;
                    self.n += 1;
                }
            }
        }
    }

    fn set1(&mut self, a: &[u8]) {
        let mut one: [&[u8]; 1] = [&[]];
        one[0] = a;
        self.set(&one);
    }

    fn get(&self) -> &[u8] {
        self.msg.get(..self.n).unwrap_or(&[])
    }
}

/// An env var's name is any printable text without `=` (upstream's
/// relaxed rule); anything else, from an `envFrom` source, is skipped.
fn pr_env_name_ok(n: &[u8]) -> bool {
    !n.is_empty() && n.iter().all(|&c| (0x21..0x7f).contains(&c) && c != b'=')
}

/// One ConfigMap's or Secret's `data` member value, decoded: a ConfigMap's
/// as the string it is, a Secret's from base64 (`stringData`, which the API
/// folds into `data`, is read as written when a store kept it). `None` when
/// it does not decode or fit.
fn pr_data_value(obj: &[u8], secret: bool, key: &[u8], out: &mut [u8]) -> Option<Option<usize>> {
    if let Some(data) = pr_m(obj, b"data") {
        if let Some(v) = pr_m(data, key) {
            let s = pr_s(Some(v));
            if secret {
                let mut tmp = [0u8; PR_VAL];
                let n = pr_unescape(s, &mut tmp)?;
                return Some(Some(pr_b64(tmp.get(..n)?, out)?));
            }
            return Some(Some(pr_unescape(s, out)?));
        }
    }
    if secret {
        if let Some(sd) = pr_m(obj, b"stringData") {
            if let Some(v) = pr_m(sd, key) {
                return Some(Some(pr_unescape(pr_s(Some(v)), out)?));
            }
        }
    }
    Some(None)
}

/// A downward-API `fieldRef` path's value for this Pod; `None` for a path
/// upstream does not offer to env.
fn pr_field<'a>(
    pod: &'a [u8],
    path: &[u8],
    host_ip: &'a [u8],
    out: &'a mut [u8],
) -> Option<&'a [u8]> {
    let meta = pr_m(pod, b"metadata").unwrap_or(b"{}");
    let spec = pr_m(pod, b"spec").unwrap_or(b"{}");
    let raw: &[u8] = match path {
        b"metadata.name" => pr_ms(meta, b"name"),
        b"metadata.namespace" => pr_ms(meta, b"namespace"),
        b"metadata.uid" => pr_ms(meta, b"uid"),
        b"spec.nodeName" => pr_ms(spec, b"nodeName"),
        b"spec.serviceAccountName" => pr_ms(spec, b"serviceAccountName"),
        // A Pod shares its node's network unless it has an address of its
        // own: its IP is its node's (pod_status.uproc).
        b"status.hostIP" | b"status.hostIPs" | b"status.podIP" | b"status.podIPs" => host_ip,
        _ => {
            let (coll, rest): (&[u8], &[u8]) =
                if let Some(r) = path.strip_prefix(b"metadata.labels['") {
                    (b"labels", r)
                } else if let Some(r) = path.strip_prefix(b"metadata.annotations['") {
                    (b"annotations", r)
                } else {
                    return None;
                };
            let key = rest.strip_suffix(b"']")?;
            let m = pr_m(meta, coll).unwrap_or(b"{}");
            pr_ms(m, key)
        }
    };
    let n = pr_unescape(raw, out)?;
    let done: &'a [u8] = out;
    done.get(..n)
}

/// A `resourceFieldRef`'s value: the container's limit or request divided by
/// `divisor` (default 1), rounded up; a limit not set is the node's
/// allocatable (upstream merges it in), a request not set is its limit or 0.
fn pr_resource(pod: &[u8], ctr: &[u8], r: &[u8], node: &[u8], out: &mut PrOut) -> bool {
    let res = pr_ms(r, b"resource");
    let cname = pr_ms(r, b"containerName");
    let target = if cname.is_empty() {
        ctr
    } else {
        let spec = pr_m(pod, b"spec").unwrap_or(b"{}");
        pr_named(pr_m(spec, b"containers"), cname)
            .or_else(|| pr_named(pr_m(spec, b"initContainers"), cname))
            .unwrap_or(ctr)
    };
    let (kind, what): (&[u8], &[u8]) = match res.iter().position(|&c| c == b'.') {
        Some(d) => (res.get(..d).unwrap_or(&[]), res.get(d + 1..).unwrap_or(&[])),
        None => return false,
    };
    let cpu = what == b"cpu";
    if !(cpu || what == b"memory" || what == b"ephemeral-storage")
        || !(kind == b"limits" || kind == b"requests")
    {
        return false;
    }
    let scale = if cpu { 3 } else { 0 };
    let resources = pr_m(target, b"resources").unwrap_or(b"{}");
    let limits = pr_m(resources, b"limits").unwrap_or(b"{}");
    let requests = pr_m(resources, b"requests").unwrap_or(b"{}");
    let alloc = pr_m(pr_m(node, b"status").unwrap_or(b"{}"), b"allocatable")
        .or_else(|| pr_m(pr_m(node, b"status").unwrap_or(b"{}"), b"capacity"))
        .unwrap_or(b"{}");
    let q = |obj: &[u8]| pm_quantity(pr_ms(obj, what), scale);
    let v = if kind == b"limits" {
        q(limits).or_else(|| q(alloc)).unwrap_or(0)
    } else {
        q(requests).or_else(|| q(limits)).unwrap_or(0)
    };
    let div_raw = pr_ms(r, b"divisor");
    let div = if div_raw.is_empty() {
        pm_quantity(b"1", scale)
    } else {
        pm_quantity(div_raw, scale)
    };
    let div = match div {
        Some(d) if d > 0 => d,
        _ => return false,
    };
    out.dec(v.div_ceil(div));
    true
}

/// The element of the array `list` whose `name` is `name`.
fn pr_named<'a>(list: Option<&'a [u8]>, name: &[u8]) -> Option<&'a [u8]> {
    let list = list?;
    let mut i = 0usize;
    while let Some(c) = pr_at(list, i) {
        if pr_ms(c, b"name") == name {
            return Some(c);
        }
        i += 1;
    }
    None
}

/// Whether the namespace forbids hostPath volumes: Pod Security's `baseline`
/// and `restricted` levels do (`pod-security.kubernetes.io/enforce`), the
/// default `privileged` level allows them.
fn pr_host_path_forbidden(ns_obj: &[u8]) -> bool {
    let labels = pr_m(pr_m(ns_obj, b"metadata").unwrap_or(b"{}"), b"labels").unwrap_or(b"{}");
    let level = pm_pick_raw(labels, b"pod-security.kubernetes.io/enforce");
    level == b"baseline" || level == b"restricted"
}

/// A member of an object by its whole name (as `!pick=` does), its scalar.
fn pm_pick_raw<'a>(obj: &'a [u8], name: &[u8]) -> &'a [u8] {
    pr_ms(obj, name)
}

/// A path the node's record can carry as a volume's: absolute (or, for an
/// item, relative), no `..`, none of the record's separators.
fn pr_path_ok(p: &[u8], absolute: bool) -> bool {
    !p.is_empty()
        && (p.first() == Some(&b'/')) == absolute
        && p.iter()
            .all(|&c| c > 0x20 && c < 0x7f && !matches!(c, b',' | b';' | b':' | b'%' | b'|' | b'='))
        && !p.split(|&c| c == b'/').any(|s| s == b"..")
}

/// A DNS name (a volume's, a claim's, a ConfigMap's): what the record can
/// carry between `/`.
fn pr_name_ok(n: &[u8]) -> bool {
    !n.is_empty()
        && n.iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'-' || c == b'.')
}

/// A file mode (`defaultMode`, an item's `mode`: decimal in JSON) as the
/// octal digits `chmod` takes; 0644 when unstated.
fn pr_mode(raw: &[u8], out: &mut PrOut) -> bool {
    let mut v: u32 = 0;
    if raw.is_empty() {
        v = 0o644;
    } else {
        for &c in raw {
            if !c.is_ascii_digit() {
                return false;
            }
            v = v.saturating_mul(10).saturating_add(u32::from(c - b'0'));
        }
        if v > 0o777 {
            return false;
        }
    }
    out.byte(b'0' + ((v >> 6) & 7) as u8);
    out.byte(b'0' + ((v >> 3) & 7) as u8);
    out.byte(b'0' + (v & 7) as u8);
    true
}

/// A StatefulSet's Pod mounting one of the set's volumeClaimTemplates: the
/// claim upstream's StatefulSet controller gives each ordinal,
/// `<template>-<pod>` (the Pod's name is `<set>-<ordinal>`), stable across
/// the Pod's recreations. Its PersistentVolumeClaim is asked for (into
/// `x.claims`, created when not there yet: never replaced, never deleted
/// with the Pod). `Some(false)` when the Pod has no such template; `None`
/// when the set's read is not answered yet.
fn pr_sts_claim<W: PrWorld>(
    w: &mut W,
    x: &mut PrScratch,
    pod: &[u8],
    key: &[u8],
    vname: &[u8],
) -> Option<bool> {
    let slash = key.iter().position(|&c| c == b'/').unwrap_or(0);
    let ns = key.get(..slash).unwrap_or(&[]);
    let pname = key.get(slash + 1..).unwrap_or(&[]);
    let meta = pr_m(pod, b"metadata").unwrap_or(b"{}");
    let owners = pr_m(meta, b"ownerReferences").unwrap_or(b"[]");
    let mut k = 0usize;
    let set = loop {
        match pr_at(owners, k) {
            Some(o) if pr_ms(o, b"kind") == b"StatefulSet" => break pr_ms(o, b"name"),
            Some(_) => k += 1,
            None => return Some(false),
        }
    };
    let n = match w.object(b"statefulsets.apps", ns, set, &mut x.obj) {
        PrLook::Pending => return None,
        PrLook::Absent => return Some(false),
        PrLook::Found(n) => n,
    };
    let obj = x.obj.get(..n).unwrap_or(&[]);
    let sspec = pr_m(obj, b"spec").unwrap_or(b"{}");
    let tmpl = pr_named_meta(pr_m(sspec, b"volumeClaimTemplates"), vname);
    let Some(tmpl) = tmpl else {
        return Some(false);
    };
    let tmeta = pr_m(tmpl, b"metadata").unwrap_or(b"{}");
    let labels = pr_m(tmeta, b"labels")
        .or_else(|| pr_m(pr_m(sspec, b"selector").unwrap_or(b"{}"), b"matchLabels"))
        .unwrap_or(b"{}");
    let at = x.claims_len;
    let mut o = PrOut {
        b: x.claims.get_mut(at..).unwrap_or(&mut []),
        n: 0,
        over: false,
    };
    o.lit(b"{\"apiVersion\":\"v1\",\"kind\":\"PersistentVolumeClaim\",\"metadata\":{\"name\":\"");
    o.lit(vname);
    o.byte(b'-');
    o.lit(pname);
    o.lit(b"\",\"namespace\":\"");
    o.lit(ns);
    o.lit(b"\",\"labels\":");
    o.lit(labels);
    o.lit(b"},\"spec\":");
    o.lit(pr_m(tmpl, b"spec").unwrap_or(b"{}"));
    o.lit(b"}\n");
    if !o.over {
        x.claims_len = at + o.n;
    }
    Some(true)
}

/// The element of the array `list` whose `metadata.name` is `name`.
fn pr_named_meta<'a>(list: Option<&'a [u8]>, name: &[u8]) -> Option<&'a [u8]> {
    let list = list?;
    let mut i = 0usize;
    while let Some(c) = pr_at(list, i) {
        if pr_ms(pr_m(c, b"metadata").unwrap_or(b"{}"), b"name") == name {
            return Some(c);
        }
        i += 1;
    }
    None
}

/// The volume list entry for one volumeMount (see the header).
fn pr_volume<W: PrWorld>(
    w: &mut W,
    x: &mut PrScratch,
    pod: &[u8],
    key: &[u8],
    mount: &[u8],
    err: &mut PrErr,
    out: &mut PrOut,
) -> Option<()> {
    let slash = key.iter().position(|&c| c == b'/').unwrap_or(0);
    let ns = key.get(..slash).unwrap_or(&[]);
    let spec = pr_m(pod, b"spec").unwrap_or(b"{}");
    let vname = pr_ms(mount, b"name");
    let mp = pr_ms(mount, b"mountPath");
    if !pr_path_ok(mp, true) {
        err.set(&[
            b"volumeMount \"",
            vname,
            b"\": mountPath \"",
            mp,
            b"\" cannot be mounted by this node",
        ]);
        return Some(());
    }
    if !pr_ms(mount, b"subPath").is_empty() || !pr_ms(mount, b"subPathExpr").is_empty() {
        err.set(&[
            b"volumeMount \"",
            vname,
            b"\": subPath is not supported by this node",
        ]);
        return Some(());
    }
    let mut ro = pr_true(mount, b"readOnly");
    let vols = pr_m(spec, b"volumes").unwrap_or(b"[]");
    let mut i = 0usize;
    let vol = loop {
        match pr_at(vols, i) {
            Some(v) if pr_ms(v, b"name") == vname => break v,
            Some(_) => i += 1,
            None => {
                if pr_sts_claim(w, x, pod, key, vname)? && pr_name_ok(vname) {
                    out.lit(ns);
                    out.byte(b'/');
                    out.lit(vname);
                    out.byte(b'-');
                    out.lit(key.get(slash + 1..).unwrap_or(&[]));
                    out.byte(b':');
                    out.lit(mp);
                    if ro {
                        out.lit(b":ro");
                    }
                    return Some(());
                }
                err.set(&[b"volumeMount \"", vname, b"\" names no volume of the Pod"]);
                return Some(());
            }
        }
    };
    if !pr_name_ok(vname) {
        err.set(&[b"volume \"", vname, b"\": a name this node cannot carry"]);
        return Some(());
    }
    if let Some(pvc) = pr_m(vol, b"persistentVolumeClaim") {
        let claim = pr_ms(pvc, b"claimName");
        if !pr_name_ok(claim) {
            err.set(&[
                b"volume \"",
                vname,
                b"\": claimName \"",
                claim,
                b"\" is not a claim name",
            ]);
            return Some(());
        }
        ro |= pr_true(pvc, b"readOnly");
        out.lit(ns);
        out.byte(b'/');
        out.lit(claim);
    } else if let Some(hp) = pr_m(vol, b"hostPath") {
        let path = pr_ms(hp, b"path");
        if !pr_path_ok(path, true) {
            err.set(&[
                b"volume \"",
                vname,
                b"\": hostPath \"",
                path,
                b"\" cannot be mounted by this node",
            ]);
            return Some(());
        }
        match w.object(b"namespaces", b"", ns, &mut x.obj) {
            PrLook::Pending => return None,
            PrLook::Found(n) => {
                if pr_host_path_forbidden(x.obj.get(..n).unwrap_or(&[])) {
                    err.set(&[
                        b"volume \"",
                        vname,
                        b"\": hostPath volumes are forbidden by namespace \"",
                        ns,
                        b"\"'s Pod Security level",
                    ]);
                    return Some(());
                }
            }
            PrLook::Absent => {}
        }
        out.lit(path);
    } else if pr_m(vol, b"emptyDir").is_some() {
        out.lit(b"@e/");
        out.lit(vname);
    } else {
        let (kind, src, name): (&[u8], &[u8], &[u8]) = if let Some(c) = pr_m(vol, b"configMap") {
            (b"@c/", c, pr_ms(c, b"name"))
        } else if let Some(s) = pr_m(vol, b"secret") {
            (b"@s/", s, pr_ms(s, b"secretName"))
        } else {
            err.set(&[
                b"volume \"",
                vname,
                b"\": its type is not supported by this node",
            ]);
            return Some(());
        };
        if !pr_name_ok(name) {
            err.set(&[
                b"volume \"",
                vname,
                b"\": \"",
                name,
                b"\" is not an object name",
            ]);
            return Some(());
        }
        out.lit(kind);
        out.lit(vname);
        out.byte(b'/');
        out.lit(ns);
        out.byte(b'/');
        out.lit(name);
        out.byte(b'/');
        if !pr_mode(pr_ms(src, b"defaultMode"), out) {
            err.set(&[b"volume \"", vname, b"\": defaultMode is not a file mode"]);
            return Some(());
        }
        out.lit(if pr_true(src, b"optional") {
            b"/1"
        } else {
            b"/0"
        });
        if let Some(items) = pr_m(src, b"items") {
            let mut k = 0usize;
            while let Some(it) = pr_at(items, k) {
                let key = pr_ms(it, b"key");
                let path = pr_ms(it, b"path");
                if !pr_path_ok(path, false)
                    || key.is_empty()
                    || !key
                        .iter()
                        .all(|&c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
                {
                    err.set(&[
                        b"volume \"",
                        vname,
                        b"\": item \"",
                        key,
                        b"\" cannot be projected by this node",
                    ]);
                    return Some(());
                }
                out.byte(if k == 0 { b'/' } else { b'|' });
                out.lit(key);
                out.byte(b'=');
                out.lit(path);
                let m = pr_ms(it, b"mode");
                if !m.is_empty() {
                    out.byte(b'=');
                    if !pr_mode(m, out) {
                        err.set(&[b"volume \"", vname, b"\": item mode is not a file mode"]);
                        return Some(());
                    }
                }
                k += 1;
            }
        }
        // ConfigMap and Secret volumes are always mounted read-only.
        ro = true;
    }
    out.byte(b':');
    out.lit(mp);
    if ro {
        out.lit(b":ro");
    }
    Some(())
}

/// What [`pr_spec`] made of a container.
#[derive(Debug, PartialEq, Eq)]
enum PrRes {
    /// The node's record, this long.
    Built(usize),
    /// The Pod has no such container.
    NoContainer,
    /// A read is not answered yet: ask again later.
    Pending,
}

/// Which container of which Pod a record is asked for: init container
/// `idx` (`init`) or app container `idx` of the Pod `pod` (`<ns>/<name>` at
/// `key`), its desired state `deleted` once `deleting`.
struct PrAsk<'a> {
    pod: &'a [u8],
    key: &'a [u8],
    init: bool,
    idx: usize,
    deleting: bool,
}

/// The node's record (`/pod-specs/<uid>[~<container>]`) for the container
/// `ask` names.
fn pr_spec<W: PrWorld>(w: &mut W, x: &mut PrScratch, ask: &PrAsk, out: &mut [u8]) -> PrRes {
    let (pod, key, init, idx, deleting) = (ask.pod, ask.key, ask.init, ask.idx, ask.deleting);
    let slash = key.iter().position(|&c| c == b'/').unwrap_or(0);
    let ns = key.get(..slash).unwrap_or(&[]);
    let spec = pr_m(pod, b"spec").unwrap_or(b"{}");
    let list: &[u8] = if init {
        b"initContainers"
    } else {
        b"containers"
    };
    let Some(ctr) = pr_m(spec, list).and_then(|l| pr_at(l, idx)) else {
        return PrRes::NoContainer;
    };
    let node = pr_ms(spec, b"nodeName");
    let mut err = PrErr {
        msg: [0; 192],
        n: 0,
    };
    x.env.clear();
    x.svc.clear();
    x.claims_len = 0;

    // The node: its address (the downward API's hostIP/podIP) and allocatable.
    let mut node_len = 0usize;
    match w.object(b"nodes", b"", node, &mut x.obj) {
        PrLook::Pending => return PrRes::Pending,
        PrLook::Found(n) => node_len = n,
        PrLook::Absent => {}
    }
    let mut host_ip = [0u8; 64];
    let mut hl = 0usize;
    let mut node_alloc = [0u8; 512];
    let nal = {
        let nobj = x.obj.get(..node_len).unwrap_or(&[]);
        let status = pr_m(nobj, b"status").unwrap_or(b"{}");
        let addrs = pr_m(status, b"addresses").unwrap_or(b"[]");
        let mut k = 0usize;
        while let Some(a) = pr_at(addrs, k) {
            if pr_ms(a, b"type") == b"InternalIP" {
                let ip = pr_ms(a, b"address");
                hl = ip.len().min(host_ip.len());
                for (d, &c) in host_ip.iter_mut().zip(ip) {
                    *d = c;
                }
                break;
            }
            k += 1;
        }
        // Keep only `status.allocatable`/`capacity` for resourceFieldRef.
        let mut o = PrOut {
            b: &mut node_alloc,
            n: 0,
            over: false,
        };
        o.lit(b"{\"status\":{");
        if let Some(v) = pr_m(status, b"allocatable") {
            o.lit(b"\"allocatable\":");
            o.lit(v);
            o.byte(b',');
        }
        if let Some(v) = pr_m(status, b"capacity") {
            o.lit(b"\"capacity\":");
            o.lit(v);
            o.byte(b',');
        }
        o.lit(b"\"_\":0}}");
        if o.over {
            0
        } else {
            o.n
        }
    };
    let host_ip = host_ip.get(..hl).unwrap_or(&[]);
    let node_obj = node_alloc.get(..nal).unwrap_or(&[]);

    // Service links: the namespace's (unless enableServiceLinks: false), and
    // the default namespace's `kubernetes` always.
    let links = pr_ms(spec, b"enableServiceLinks") != b"false";
    if links {
        let svc = &mut x.svc;
        let r = w.services(ns, &mut x.sbuf, |s| pr_svc_env(s, svc));
        if r == PrLook::Pending {
            return PrRes::Pending;
        }
    }
    if !links || ns != b"default" {
        match w.object(b"services", b"default", b"kubernetes", &mut x.sbuf) {
            PrLook::Pending => return PrRes::Pending,
            PrLook::Found(n) => pr_svc_env(x.sbuf.get(..n).unwrap_or(&[]), &mut x.svc),
            PrLook::Absent => {}
        }
    }

    // envFrom.
    let env_from = pr_m(ctr, b"envFrom").unwrap_or(b"[]");
    let mut k = 0usize;
    while let Some(src) = pr_at(env_from, k) {
        k += 1;
        let prefix = pr_ms(src, b"prefix");
        let (secret, r) = match (pr_m(src, b"configMapRef"), pr_m(src, b"secretRef")) {
            (Some(r), _) => (false, r),
            (None, Some(r)) => (true, r),
            _ => continue,
        };
        let name = pr_ms(r, b"name");
        let coll: &[u8] = if secret { b"secrets" } else { b"configmaps" };
        let n = match w.object(coll, ns, name, &mut x.obj) {
            PrLook::Pending => return PrRes::Pending,
            PrLook::Found(n) => n,
            PrLook::Absent => {
                if !pr_true(r, b"optional") {
                    err.set(&[
                        if secret {
                            b"secret \""
                        } else {
                            b"configmap \""
                        },
                        name,
                        b"\" not found",
                    ]);
                }
                continue;
            }
        };
        let obj_len = n;
        for pass in 0..2u8 {
            if pass == 1 && !secret {
                continue;
            }
            let obj = x.obj.get(..obj_len).unwrap_or(&[]);
            let member: &[u8] = if pass == 0 { b"data" } else { b"stringData" };
            let Some(data) = pr_m(obj, member) else {
                continue;
            };
            let mut i = j_ws(data, 1);
            while data.get(i) == Some(&b'"') {
                let ks = i + 1;
                let mut ke = ks;
                while let Some(&c) = data.get(ke) {
                    if c == b'"' {
                        break;
                    }
                    ke += if c == b'\\' { 2 } else { 1 };
                }
                let dkey = data.get(ks..ke).unwrap_or(&[]);
                i = j_ws(data, ke + 1);
                if data.get(i) != Some(&b':') {
                    break;
                }
                let vs = j_ws(data, i + 1);
                let ve = j_skip(data, vs);
                let raw = j_scalar(data, vs);
                let decoded = if secret && pass == 0 {
                    pr_unescape(raw, &mut x.val2).and_then(|m| pr_b64(x.val2.get(..m)?, &mut x.val))
                } else {
                    pr_unescape(raw, &mut x.val)
                };
                let mut nm = [0u8; 256];
                let mut nl = 0usize;
                for &c in prefix.iter().chain(dkey.iter()) {
                    if let Some(s) = nm.get_mut(nl) {
                        *s = c;
                        nl += 1;
                    }
                }
                let nmv = nm.get(..nl).unwrap_or(&[]);
                if let Some(vl) = decoded {
                    if pr_env_name_ok(nmv) {
                        x.env.push(nmv, x.val.get(..vl).unwrap_or(&[]));
                    }
                }
                i = j_ws(data, ve);
                if data.get(i) == Some(&b',') {
                    i = j_ws(data, i + 1);
                }
            }
        }
    }

    // env.
    let envs = pr_m(ctr, b"env").unwrap_or(b"[]");
    let mut k = 0usize;
    while let Some(e) = pr_at(envs, k) {
        k += 1;
        let name = pr_ms(e, b"name");
        if name.is_empty() {
            continue;
        }
        let value = pr_ms(e, b"value");
        if let Some(from) = pr_m(e, b"valueFrom") {
            if value.is_empty() {
                if let Some(fr) = pr_m(from, b"fieldRef") {
                    let path = pr_ms(fr, b"fieldPath");
                    let mut fv = [0u8; 512];
                    match pr_field(pod, path, host_ip, &mut fv) {
                        Some(v) => {
                            let n = v.len();
                            let mut tmp = [0u8; 512];
                            for (d, &c) in tmp.iter_mut().zip(v) {
                                *d = c;
                            }
                            x.env.push(name, tmp.get(..n).unwrap_or(&[]));
                        }
                        None => err.set(&[
                            b"env \"",
                            name,
                            b"\": fieldRef \"",
                            path,
                            b"\" is not supported",
                        ]),
                    }
                    continue;
                }
                if let Some(rf) = pr_m(from, b"resourceFieldRef") {
                    let mut vb = [0u8; 32];
                    let mut o = PrOut {
                        b: &mut vb,
                        n: 0,
                        over: false,
                    };
                    if pr_resource(pod, ctr, rf, node_obj, &mut o) {
                        let n = o.n;
                        x.env.push(name, vb.get(..n).unwrap_or(&[]));
                    } else {
                        err.set(&[
                            b"env \"",
                            name,
                            b"\": resourceFieldRef \"",
                            pr_ms(rf, b"resource"),
                            b"\" is not supported",
                        ]);
                    }
                    continue;
                }
                let (secret, r) =
                    match (pr_m(from, b"configMapKeyRef"), pr_m(from, b"secretKeyRef")) {
                        (Some(r), _) => (false, r),
                        (None, Some(r)) => (true, r),
                        _ => continue,
                    };
                let oname = pr_ms(r, b"name");
                let okey = pr_ms(r, b"key");
                let optional = pr_true(r, b"optional");
                let coll: &[u8] = if secret { b"secrets" } else { b"configmaps" };
                let n = match w.object(coll, ns, oname, &mut x.obj) {
                    PrLook::Pending => return PrRes::Pending,
                    PrLook::Found(n) => n,
                    PrLook::Absent => {
                        if !optional {
                            err.set(&[
                                if secret {
                                    b"secret \""
                                } else {
                                    b"configmap \""
                                },
                                oname,
                                b"\" not found",
                            ]);
                        }
                        continue;
                    }
                };
                let obj = x.obj.get(..n).unwrap_or(&[]);
                match pr_data_value(obj, secret, okey, &mut x.val) {
                    Some(Some(vl)) => x.env.push(name, x.val.get(..vl).unwrap_or(&[])),
                    Some(None) => {
                        if !optional {
                            err.set(&[
                                b"couldn't find key ",
                                okey,
                                if secret {
                                    b" in Secret "
                                } else {
                                    b" in ConfigMap "
                                },
                                ns,
                                b"/",
                                oname,
                            ]);
                        }
                    }
                    None => err.set(&[
                        b"env \"",
                        name,
                        b"\": the value of ",
                        okey,
                        b" cannot be read",
                    ]),
                }
                continue;
            }
        }
        // A value, `$(VAR)` expanded against what is defined so far.
        let Some(un) = pr_unescape(value, &mut x.val2) else {
            err.set(&[b"env \"", name, b"\": its value is too long"]);
            continue;
        };
        let (env, svc) = (&x.env, &x.svc);
        let look = |n: &[u8]| env.get(n).or_else(|| svc.get(n));
        match pr_expand(x.val2.get(..un).unwrap_or(&[]), look, &mut x.val) {
            Some(vl) => {
                let mut tmp = [0u8; PR_VAL];
                for (d, &c) in tmp.iter_mut().zip(x.val.get(..vl).unwrap_or(&[])) {
                    *d = c;
                }
                x.env.push(name, tmp.get(..vl).unwrap_or(&[]));
            }
            None => err.set(&[b"env \"", name, b"\": its value is too long"]),
        }
    }
    if x.env.over || x.svc.over {
        err.set1(b"the container's environment is larger than this node carries");
    }

    // The record.
    let meta = pr_m(pod, b"metadata").unwrap_or(b"{}");
    let mut o = PrOut {
        b: out,
        n: 0,
        over: false,
    };
    o.lit(b"node=");
    o.lit(node);
    o.lit(b";image=");
    o.esc(pr_ms(ctr, b"image"));
    o.lit(b";pull=");
    o.esc(pr_ms(ctr, b"imagePullPolicy"));
    let restart = pr_ms(spec, b"restartPolicy");
    let mut rl = [0u8; 16];
    let rn = restart.len().min(16);
    for (d, &c) in rl.iter_mut().zip(restart) {
        *d = c.to_ascii_lowercase();
    }
    let restart = rl.get(..rn).unwrap_or(&[]);
    if !deleting {
        o.lit(b";restart=");
        if init {
            o.lit(if restart == b"never" {
                b"never"
            } else {
                b"onfailure"
            });
        } else if restart.is_empty() {
            o.lit(b"always");
        } else {
            o.lit(restart);
        }
    }
    // The grace: a delete under way's (`deletionGracePeriodSeconds`, what
    // its DeleteOptions asked: _shared/podlive.rs), else the Pod's own,
    // else upstream's default.
    o.lit(b";grace=");
    let asked = pr_ms(meta, b"deletionGracePeriodSeconds");
    let grace = if deleting && !asked.is_empty() {
        asked
    } else {
        pr_ms(spec, b"terminationGracePeriodSeconds")
    };
    o.lit(if grace.is_empty() { b"30" } else { grace });
    o.lit(if deleting {
        b";desired=deleted"
    } else {
        b";desired=running"
    });

    // command and args, against the container's environment.
    for pass in 0..2u8 {
        o.lit(if pass == 0 { b";cmd=" } else { b";args=" });
        let member: &[u8] = if pass == 0 { b"command" } else { b"args" };
        let Some(arr) = pr_m(ctr, member) else {
            continue;
        };
        let start = o.n;
        let mut full = false;
        {
            let rest = o.b.get_mut(start..).unwrap_or(&mut []);
            let mut aw = ArgvOut::new(rest);
            let mut i = 0usize;
            while let Some(el) = pr_at(arr, i) {
                i += 1;
                let raw = pr_s(Some(el));
                let Some(un) = pr_unescape(raw, &mut x.val2) else {
                    full = true;
                    break;
                };
                let (env, svc) = (&x.env, &x.svc);
                let look = |n: &[u8]| env.get(n).or_else(|| svc.get(n));
                let Some(vl) = pr_expand(x.val2.get(..un).unwrap_or(&[]), look, &mut x.val) else {
                    full = true;
                    break;
                };
                for &c in x.val.get(..vl).unwrap_or(&[]) {
                    aw.byte(c);
                }
                aw.next_token();
            }
            if aw.full || full || aw.len() > PR_ARGV_MAX {
                full = true;
            } else {
                o.n = start + aw.len();
            }
        }
        if full {
            o.n = start;
            o.lit(b"%00");
        }
    }

    // env: the Service links the container does not define, then its own.
    o.lit(b";env=");
    let mut first = true;
    for (tab, links) in [(&x.svc, true), (&x.env, false)] {
        for i in 0..tab.n {
            if tab.shadowed(i) || (links && x.env.get(tab.name(i)).is_some()) {
                continue;
            }
            if !first {
                o.byte(b',');
            }
            first = false;
            o.esc(tab.name(i));
            o.byte(b'=');
            o.esc(tab.value(i));
        }
    }

    o.lit(b";workdir=");
    if let Some(un) = pr_unescape(pr_ms(ctr, b"workingDir"), &mut x.val) {
        o.esc(x.val.get(..un).unwrap_or(&[]));
    }
    let limits = pr_m(pr_m(ctr, b"resources").unwrap_or(b"{}"), b"limits").unwrap_or(b"{}");
    o.lit(b";cpu=");
    if let Some(v) = pm_quantity(pr_ms(limits, b"cpu"), 3) {
        o.dec(v);
    }
    o.lit(b";mem=");
    if let Some(v) = pm_quantity(pr_ms(limits, b"memory"), 0) {
        o.dec(v);
    }
    o.lit(b";eph=");
    if let Some(v) = pm_quantity(pr_ms(limits, b"ephemeral-storage"), 0) {
        o.dec(v);
    }
    // A cgroup of its own for every container, as upstream's runtime gives
    // it — what the limits and the freezer (nanocloud's pause) act on. The
    // runtime makes one for a workload with an envelope, so the envelope
    // always has pids.max: the kernel's own ceiling (pid_max at most
    // 4194304), which bounds nothing.
    o.lit(b";tasks=4194304");
    o.lit(b";pod=");
    o.lit(key);
    if !deleting {
        // The readiness gate the node's status reads (`rp_cmd`: whether the
        // probe chain's `ready` says it): the readiness probe, else the
        // startup probe — a container with one is not ready until it passed.
        o.lit(b";rp_cmd=");
        let at = o.n;
        pr_probe_cmd(
            ctr,
            pr_m(ctr, b"readinessProbe").unwrap_or(b"{}"),
            host_ip,
            &mut o,
        );
        if o.n == at {
            pr_probe_cmd(
                ctr,
                pr_m(ctr, b"startupProbe").unwrap_or(b"{}"),
                host_ip,
                &mut o,
            );
        }
    }
    // The container's probes, for its own record (pr_probes).
    x.probes_len = if deleting {
        0
    } else {
        let mut po = PrOut {
            b: &mut x.probes,
            n: 0,
            over: false,
        };
        pr_probes(ctr, host_ip, &mut po);
        if po.over {
            0
        } else {
            po.n
        }
    };

    // securityContext: the container's over the Pod's.
    let psc = pr_m(spec, b"securityContext").unwrap_or(b"{}");
    let csc = pr_m(ctr, b"securityContext").unwrap_or(b"{}");
    let pick = |k: &[u8]| -> &[u8] {
        let c = pr_ms(csc, k);
        if c.is_empty() {
            pr_ms(psc, k)
        } else {
            c
        }
    };
    let ruid = pick(b"runAsUser");
    let rgid = pick(b"runAsGroup");
    let nonroot = pick(b"runAsNonRoot") == b"true";
    let cname = pr_ms(ctr, b"name");
    let digits = |s: &[u8]| s.iter().all(|c| c.is_ascii_digit());
    if !digits(ruid) || !digits(rgid) {
        err.set(&[
            b"container \"",
            cname,
            b"\": runAsUser/runAsGroup is not a number",
        ]);
    }
    if nonroot && ruid == b"0" {
        err.set(&[
            b"container's runAsUser breaks non-root policy (pod: \"",
            key,
            b"\", container: ",
            cname,
            b")",
        ]);
    }
    if pr_true(csc, b"readOnlyRootFilesystem") {
        err.set(&[
            b"container \"",
            cname,
            b"\": readOnlyRootFilesystem is not supported by this node's runtime",
        ]);
    }
    if pr_true(csc, b"privileged") {
        err.set(&[
            b"container \"",
            cname,
            b"\": privileged containers are not supported by this node's runtime",
        ]);
    }
    if pr_at(pr_m(psc, b"supplementalGroups").unwrap_or(b"[]"), 0).is_some() {
        err.set1(b"supplementalGroups are not supported by this node's runtime");
    }
    o.lit(b";ruid=");
    o.lit(ruid);
    o.lit(b";rgid=");
    o.lit(rgid);
    o.lit(b";nonroot=");
    if nonroot {
        o.byte(b'1');
    }

    // volumes.
    o.lit(b";volumes=");
    let mounts = pr_m(ctr, b"volumeMounts").unwrap_or(b"[]");
    let mut k = 0usize;
    while let Some(m) = pr_at(mounts, k) {
        if k > 0 {
            o.byte(b',');
        }
        k += 1;
        if pr_volume(w, x, pod, key, m, &mut err, &mut o).is_none() {
            return PrRes::Pending;
        }
    }

    // The Pod's /etc/resolv.conf by its dnsPolicy (_shared/poddns.rs), unless
    // a mount of the container's own puts one there: the node's own file
    // (Default), or the file the policy and dnsConfig make (ClusterFirst:
    // the cluster DNS at /cluster-dns, the search path of the namespace) —
    // a container of an image only (a host command has no root of its own
    // to bind into).
    let mut has_resolv = false;
    let mut k2 = 0usize;
    while let Some(m) = pr_at(mounts, k2) {
        has_resolv |= pr_ms(m, b"mountPath") == b"/etc/resolv.conf";
        k2 += 1;
    }
    if !has_resolv && !pr_ms(ctr, b"image").is_empty() {
        let mut dns = [0u8; 64];
        let dl = match w.raw(b"/cluster-dns", &mut dns) {
            PrLook::Pending => return PrRes::Pending,
            PrLook::Found(n) => n,
            PrLook::Absent => 0,
        };
        let mut rc = [0u8; 1024];
        let sep = k > 0;
        // No cluster DNS to name: a ClusterFirst Pod gets the node's own
        // file, as upstream's kubelet falls back (MissingClusterDNS).
        let pol = pr_ms(spec, b"dnsPolicy");
        let fallback = dl == 0 && matches!(pol, b"" | b"ClusterFirst" | b"ClusterFirstWithHostNet");
        let made = if fallback {
            PodDns::Node
        } else {
            pod_dns(
                pod,
                ns,
                dns.get(..dl).unwrap_or(&[]),
                b"cluster.local",
                &mut rc,
            )
        };
        match made {
            PodDns::Node => {
                if sep {
                    o.byte(b',');
                }
                o.lit(b"/etc/resolv.conf:/etc/resolv.conf:ro");
            }
            PodDns::File(n) => {
                if sep {
                    o.byte(b',');
                }
                o.lit(b"@d/resolv/");
                for &c in rc.get(..n).unwrap_or(&[]) {
                    match c {
                        b'%' => o.lit(b"%25"),
                        b',' => o.lit(b"%2C"),
                        b';' => o.lit(b"%3B"),
                        b':' => o.lit(b"%3A"),
                        b'\n' => o.lit(b"%0A"),
                        b'\t' => o.lit(b"%09"),
                        _ => o.byte(c),
                    }
                }
                o.lit(b":/etc/resolv.conf:ro");
            }
            PodDns::Refused => err.set(&[
                b"dnsPolicy \"",
                pr_ms(spec, b"dnsPolicy"),
                b"\" is not a policy this node writes",
            ]),
        }
    }

    // Ordering: an init container waits for the ones before it, an app
    // container for all of them.
    o.lit(b";gate=");
    if init {
        o.dec(idx as u64);
    } else {
        let n = pr_m(spec, b"initContainers")
            .map(|l| {
                let mut c = 0usize;
                while pr_at(l, c).is_some() {
                    c += 1;
                }
                c
            })
            .unwrap_or(0);
        o.dec(n as u64);
    }
    // An init container's place in the order, counted from 1 (the node
    // records how many have completed: /pod-init/).
    o.lit(b";ord=");
    if init {
        o.dec(idx as u64 + 1);
    }
    o.lit(b";ctr=");
    o.esc(cname);
    o.lit(b";paused=");
    let ann = pr_m(meta, b"annotations").unwrap_or(b"{}");
    if pr_ms(ann, b"nanocloud.io/paused") == b"true" {
        o.lit(b"true");
    }
    o.lit(b";cfgerr=");
    o.esc(err.get());
    if o.over {
        return PrRes::Built(pm_overflow(o.b));
    }
    PrRes::Built(o.n)
}
