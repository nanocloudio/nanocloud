#!/usr/bin/env python3
"""Generate modules/app/_shared/k8s_proto_schema.rs from Kubernetes'
generated.proto files: the field schema the API's ingress transcodes a
protobuf request body to JSON with (_shared/k8s_proto.rs).

kubectl's typed generators (`kubectl create configmap|secret|deployment|…`,
`kubectl create token`) send `application/vnd.kubernetes.protobuf`; the API
reads JSON. The table holds every message reachable from the top-level kinds
of the packages below, so a body of any field those kinds can carry decodes.

    python3 scripts/support/k8s_proto_schema.py <out.rs> [proto-dir go-dir]

Without the dirs the protos and the Go types are fetched for API_VERSION
(k8s.io/api and k8s.io/apimachinery tags), the version the table claims to
cover. The Go types say which zero values the JSON leaves out: gogo-protobuf
writes a non-pointer scalar even at its zero value, where the object's JSON
(`omitempty`) has nothing — so such a field at zero is marked to be dropped.
A pointer field keeps its zero (`replicas: 0` is not "unset").

The output is ONE byte array (a PIC module never relocates the pointers in
an array of slices), little-endian:

    "K8PB" | n_msgs u16 | n_kinds u16 | off_msgs u32 | off_kinds u32 | off_str u32
    msgs:   n_msgs  x [special u16][n_fields u16][fields_off u32]
    fields:           [num u16][kind u8][label u8][vkind u8][name_len u8][ref u16][name_off u16]
                      (label: bit 0 repeated, bit 1 dropped at its zero value,
                       bit 2 inline: a message whose members sit in the parent's JSON)
    kinds:  n_kinds x [gv_off u16][gv_len u8][kind_len u8][kind_off u16][msg u16]
    strings
"""
import re
import struct
import sys
import urllib.request
from pathlib import Path

API_VERSION = "v0.34.1"
API = {  # package -> (k8s.io/api path, groupVersion)
    "k8s.io.api.core.v1": ("core/v1", "v1"),
    "k8s.io.api.apps.v1": ("apps/v1", "apps/v1"),
    "k8s.io.api.batch.v1": ("batch/v1", "batch/v1"),
    "k8s.io.api.rbac.v1": ("rbac/v1", "rbac.authorization.k8s.io/v1"),
    "k8s.io.api.authentication.v1": ("authentication/v1", "authentication.k8s.io/v1"),
    "k8s.io.api.policy.v1": ("policy/v1", "policy/v1"),
    "k8s.io.api.scheduling.v1": ("scheduling/v1", "scheduling.k8s.io/v1"),
    "k8s.io.api.networking.v1": ("networking/v1", "networking.k8s.io/v1"),
    "k8s.io.api.autoscaling.v1": ("autoscaling/v1", "autoscaling/v1"),
}
MACHINERY = ["pkg/apis/meta/v1", "pkg/runtime", "pkg/util/intstr", "pkg/api/resource"]

META = "k8s.io.apimachinery.pkg.apis.meta.v1"
SPECIAL = {  # message -> how its JSON is written
    f"{META}.Time": 1,
    f"{META}.MicroTime": 2,
    f"{META}.Duration": 3,
    "k8s.io.apimachinery.pkg.api.resource.Quantity": 4,
    "k8s.io.apimachinery.pkg.api.resource.QuantityValue": 4,
    "k8s.io.apimachinery.pkg.util.intstr.IntOrString": 5,
    "k8s.io.apimachinery.pkg.runtime.RawExtension": 6,
    f"{META}.FieldsV1": 6,
}
STRLIST = 7  # a message of one `repeated string items = 1`: a JSON array
SCALAR = {"string": 1, "int32": 2, "int64": 3, "bool": 4, "bytes": 5}
K_MESSAGE, K_MAP = 6, 7


GO_FILES = [f"{p}/types.go" for p, _ in API.values()]
GO_MACHINERY = ["pkg/apis/meta/v1/types.go", "pkg/runtime/types.go", "pkg/util/intstr/intstr.go"]


def fetch_go(dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    base_api = f"https://raw.githubusercontent.com/kubernetes/api/{API_VERSION}"
    base_mach = f"https://raw.githubusercontent.com/kubernetes/apimachinery/{API_VERSION}"
    for f in GO_FILES:
        with urllib.request.urlopen(f"{base_api}/{f}") as r:
            (dst / f.replace("/", "_")).write_bytes(r.read())
    for f in GO_MACHINERY:
        with urllib.request.urlopen(f"{base_mach}/{f}") as r:
            (dst / f.replace("/", "_")).write_bytes(r.read())


def parse_go(dirpath: Path):
    """struct name -> {protobuf field number: (drop-at-zero, inline)}:
    drop-at-zero a non-pointer field whose json tag says omitempty; inline
    an embedded struct (`json:",inline"`), its members the parent's."""
    out = {}
    for f in sorted(dirpath.glob("*.go")):
        text = f.read_text()
        for m in re.finditer(r"^type\s+(\w+)\s+struct\s*\{(.*?)^\}", text, re.M | re.S):
            fields = {}
            for line in m.group(2).splitlines():
                # `Name Type `tags`` or an embedded `Type `tags``.
                t = re.match(r"\s*\w+\s+(\S+)\s+`([^`]*)`", line) or re.match(r"\s*(\*?[\w.]+)\s+`([^`]*)`", line)
                if not t:
                    continue
                pb = re.search(r'protobuf:"\w+,(\d+),', t.group(2))
                js = re.search(r'json:"([^"]*)"', t.group(2))
                if not pb or not js:
                    continue
                tag = js.group(1)
                fields[int(pb.group(1))] = (
                    (not t.group(1).startswith("*")) and "omitempty" in tag,
                    tag.split(",")[0] == "" and "inline" in tag.split(",")[1:],
                )
            out.setdefault(m.group(1), {}).update(fields)
    return out


def fetch(dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    base_api = f"https://raw.githubusercontent.com/kubernetes/api/{API_VERSION}"
    base_mach = f"https://raw.githubusercontent.com/kubernetes/apimachinery/{API_VERSION}"
    for path, _ in API.values():
        with urllib.request.urlopen(f"{base_api}/{path}/generated.proto") as r:
            (dst / (path.replace("/", "_") + ".proto")).write_bytes(r.read())
    for path in MACHINERY:
        with urllib.request.urlopen(f"{base_mach}/{path}/generated.proto") as r:
            (dst / (path.replace("/", "_") + ".proto")).write_bytes(r.read())


def parse(dirpath: Path):
    msgs = {}
    for f in sorted(dirpath.glob("*.proto")):
        text = re.sub(r"//[^\n]*", "", f.read_text())
        pkg = re.search(r"^package\s+([\w.]+);", text, re.M).group(1)
        for m in re.finditer(r"^message\s+(\w+)\s*\{(.*?)^\}", text, re.M | re.S):
            fields = []
            for line in m.group(2).split(";"):
                line = " ".join(line.split())
                if not line:
                    continue
                fm = re.match(r"(optional|repeated)?\s*(map<\s*string\s*,\s*([\w.]+)\s*>|[\w.]+)\s+(\w+)\s*=\s*(\d+)", line)
                if not fm:
                    raise SystemExit(f"{f.name}: {m.group(1)}: cannot read `{line}`")
                fields.append((fm.group(1) or "optional", fm.group(2), fm.group(3), fm.group(4), int(fm.group(5))))
            msgs[f"{pkg}.{m.group(1)}"] = (pkg, fields)
    return msgs


def resolve(msgs, pkg, t):
    if t in SCALAR:
        return t
    t = t.lstrip(".")
    for cand in (t, f"{pkg}.{t}"):
        if cand in msgs:
            return cand
    raise SystemExit(f"unresolved type {t} in {pkg}")


def main():
    out = Path(sys.argv[1])
    if len(sys.argv) > 3:
        src, gosrc = Path(sys.argv[2]), Path(sys.argv[3])
    else:
        src = Path("/tmp") / f"k8s-proto-{API_VERSION}"
        gosrc = Path("/tmp") / f"k8s-go-{API_VERSION}"
        fetch(src)
        fetch_go(gosrc)
    msgs = parse(src)
    gotypes = parse_go(gosrc)
    objmeta = f"{META}.ObjectMeta"
    kinds = []
    for name, (pkg, fields) in sorted(msgs.items()):
        if pkg in API and any(fn == "metadata" and resolve(msgs, pkg, t) == objmeta for _, t, _, fn, _ in fields):
            kinds.append((API[pkg][1], name.rsplit(".", 1)[1], name))
    # Options a typed client sends as a body under any group.
    kinds.append(("meta.k8s.io/v1", "DeleteOptions", f"{META}.DeleteOptions"))
    # Every message reachable from the kinds.
    order, seen, todo = [], set(), [k[2] for k in kinds]
    while todo:
        n = todo.pop()
        if n in seen:
            continue
        seen.add(n)
        order.append(n)
        pkg, fields = msgs[n]
        if n in SPECIAL:
            continue
        for _, t, vt, _, _ in fields:
            for x in ((vt,) if vt else (t,)):
                r = resolve(msgs, pkg, x)
                if r not in SCALAR:
                    todo.append(r)
    order.sort()
    index = {n: i for i, n in enumerate(order)}
    strings = bytearray()
    spool = {}

    def s(v: str):
        b = v.encode()
        if b not in spool:
            spool[b] = len(strings)
            strings.extend(b)
        if len(strings) > 0xFFFF or len(b) > 255:
            raise SystemExit("string pool past its u16 offsets")
        return spool[b], len(b)

    msg_tab, field_tab = bytearray(), bytearray()
    unmatched = []
    for n in order:
        pkg, fields = msgs[n]
        go = gotypes.get(n.rsplit(".", 1)[1])
        if go is None and n not in SPECIAL:
            unmatched.append(n)
        go = go or {}
        special = SPECIAL.get(n, 0)
        if not special and len(fields) == 1 and fields[0][0] == "repeated" and fields[0][1] == "string" and fields[0][3] == "items":
            special = STRLIST
        msg_tab += struct.pack("<HHI", special, len(fields), len(field_tab))
        for label, t, vt, fname, num in fields:
            off, ln = s(fname)
            if vt:
                v = resolve(msgs, pkg, vt)
                vkind = SCALAR.get(v, K_MESSAGE)
                ref = index.get(v, 0xFFFF) if vkind == K_MESSAGE else 0xFFFF
                field_tab += struct.pack("<HBBBBHH", num, K_MAP, 0, vkind, ln, ref, off)
            else:
                r = resolve(msgs, pkg, t)
                kind = SCALAR.get(r, K_MESSAGE)
                ref = index[r] if kind == K_MESSAGE else 0xFFFF
                drop, inline = go.get(num, (False, False))
                bits = (1 if label == "repeated" else 0) | (2 if drop and kind != K_MESSAGE else 0) | (4 if inline else 0)
                field_tab += struct.pack("<HBBBBHH", num, kind, bits, 0, ln, ref, off)
    kind_tab = bytearray()
    for gv, kind, n in kinds:
        go, gl = s(gv)
        ko, kl = s(kind)
        kind_tab += struct.pack("<HBBHH", go, gl, kl, ko, index[n])
    hdr = 4 + 2 + 2 + 4 + 4 + 4
    off_msgs = hdr
    off_fields = off_msgs + len(msg_tab)
    # fields_off in each message entry is relative to the field table.
    off_kinds = off_fields + len(field_tab)
    off_str = off_kinds + len(kind_tab)
    blob = (b"K8PB" + struct.pack("<HHIII", len(order), len(kinds), off_msgs, off_kinds, off_str)
            + msg_tab + field_tab + kind_tab + bytes(strings))
    # The field table follows the message table: fields_off is relative to it.
    assert off_fields == off_msgs + len(order) * 8
    body = ",".join(str(b) for b in blob)
    out.write_text(
        "// GENERATED by scripts/support/k8s_proto_schema.py from Kubernetes\n"
        f"// {API_VERSION} generated.proto (k8s.io/api, k8s.io/apimachinery). Do not edit.\n"
        f"// {len(order)} messages, {len(kinds)} kinds, {len(blob)} bytes.\n"
        # A const slice: never a `static` (a PIC module's statics are not
        # loaded where its code finds them: a static here segfaulted the
        # runtime on the first protobuf request), and not a large const array.
        f"pub const K8S_PB_SCHEMA: &[u8] = &[{body}];\n"
    )
    print(f"{out}: {len(order)} messages, {len(kinds)} kinds, {len(blob)} bytes")
    if unmatched:
        print("no Go type (zeros kept):", ", ".join(unmatched))


if __name__ == "__main__":
    main()
