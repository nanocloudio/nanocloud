#!/usr/bin/env python3
"""An FLXA subgraph, encoded off-node.

A graph-backed Pod names a subgraph the node realises as modules of its own
image (fluxor's `workload` contract, `SOURCE_FMOD_GRAPH`). The node reads the
blob from `/graphs/<name>` as hex; this writes it, in the layout
`fluxor/src/kernel/exec/scheduler/live.rs` decodes (big-endian):

    magic "FLXA" (4)  version u16 = 1  reserved u16  owner_uid (16, zero: the
    kernel copies the CREATE header's identity in)  state_cap u32  buffer_cap
    u32  module_count u8  edge_count u8
    module: type_hash u32 (FNV-1a of the type name)  domain u8  params_len
            u16  params (the SDK TLV: FE 01 [len LE16] [tag len value]... FF 00)
    edge:   from (kind u8, index u16, port u8)  to (kind u8, index u16,
            port u8)  bytes u32

    flxa.py <module>[:<tag>=<value>,...]... [--edge FROM TO BYTES]   -> hex on stdout

A value is a u8 (`1=0`), a string (`1=s:nanocloud`), a u32 (`3=u32:30000`),
or an extended entry past the end marker read from a file (`10=f:cert.der`
— `[tag][0x00][len BE][bytes]`, what `tls` takes its DER credentials as).
An edge endpoint is `kind:index:port` (kind 0 = a module of this graph,
3 = a network lane, 4 = the caller's lane).

A type resolves only against the module table packed into the image: list it
under the graph's `workload_modules:`.
"""
import struct
import sys

MAGIC = 0x464C5841
VERSION = 1


def type_hash(name: str) -> int:
    h = 0x811C9DC5
    for c in name.encode():
        h ^= c
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def params_tlv(entries, ext=()) -> bytes:
    body = b"".join(bytes([tag, len(val)]) + val for tag, val in entries)
    tail = b"".join(bytes([tag, 0]) + struct.pack(">H", len(val)) + val for tag, val in ext)
    return b"\xfe\x01" + struct.pack("<H", len(body)) + body + b"\xff\x00" + tail


def value(val: str):
    """(entry bytes, is_extended)"""
    if val.startswith("s:"):
        v = val[2:].encode()
    elif val.startswith("u32:"):
        v = struct.pack("<I", int(val[4:]))
    elif val.startswith("u16:"):
        v = struct.pack("<H", int(val[4:]))
    elif val.startswith("f:"):
        return open(val[2:], "rb").read(), True
    else:
        v = bytes([int(val) & 0xFF])
    if len(v) > 255:
        raise SystemExit(f"flxa: a parameter entry is at most 255 bytes ({val[:20]!r})")
    return v, False


def module(spec: str) -> bytes:
    name, _, rest = spec.partition(":")
    entries, ext = [], []
    if rest:
        for kv in rest.split(","):
            tag, _, val = kv.partition("=")
            v, is_ext = value(val)
            (ext if is_ext else entries).append((int(tag), v))
    p = params_tlv(entries, ext) if entries or ext else b""
    return struct.pack(">IBH", type_hash(name), 0, len(p)) + p


def edge(frm: str, to: str, nbytes: int) -> bytes:
    def ep(s):  # kind:index:port
        k, i, port = (int(x) for x in s.split(":"))
        return struct.pack(">BHB", k, i, port)
    return ep(frm) + ep(to) + struct.pack(">I", nbytes)


def encode(modules, edges=()) -> bytes:
    hdr = struct.pack(">IHH", MAGIC, VERSION, 0) + bytes(16) + struct.pack(">IIBB", 0, 0, len(modules), len(edges))
    return hdr + b"".join(modules) + b"".join(edges)


if __name__ == "__main__":
    args = sys.argv[1:]
    mods, edges = [], []
    while args:
        a = args.pop(0)
        if a == "--edge":
            edges.append(edge(args.pop(0), args.pop(0), int(args.pop(0))))
        else:
            mods.append(module(a))
    if not mods:
        sys.exit(__doc__)
    print(encode(mods, edges).hex())
