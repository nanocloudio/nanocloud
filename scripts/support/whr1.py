#!/usr/bin/env python3
"""A reader of Wormhole's WHR1 records as nanocloud's projection writes them,
written from the layout alone (rfc_lb_vip §9.4; wormhole-common
traffic/records.rs; docs/architecture/wormhole-projection.md), so the E2E
checks the bytes independently of the Rust that wrote them.

    whr1.py <store.log> check <ns> <svc>          verify and describe the active generation
    whr1.py <store.log> active-key <ns> <svc>     the Service's active-pointer key
    whr1.py <store.log> gen-prefix <ns> <svc> <g> its generation's key prefix
    whr1.py <store.log> put-vip <ns> <svc> <4|6> <phase> <address>
                                                  append a VipBinding (kind 5, §11.6
                                                  layout) at /wh/<t>/<s>/vip/<4|6>, as
                                                  Wormhole's VIP controller would
"""
import hashlib
import ipaddress
import struct
import sys

HEADER = 36


def ident(label, *parts):
    h = hashlib.sha256(label.encode())
    for p in parts:
        h.update(b"\0")
        h.update(p if isinstance(p, bytes) else p.encode())
    return h.digest()[:16]


def live(path):
    data = open(path, "rb").read()
    p, kv = 0, {}
    while p + 15 <= len(data):
        _rev, op, kl, vl = struct.unpack("<QBHI", data[p:p + 15])
        if p + 15 + kl + vl > len(data):
            break
        k = data[p + 15:p + 15 + kl]
        if op == 1:
            kv[k] = data[p + 15 + kl:p + 15 + kl + vl]
        else:
            kv.pop(k, None)
        p += 15 + kl + vl
    return kv


def envelope(rec, key):
    if rec[:4] != b"WHR1":
        sys.exit(f"bad magic at {key}")
    kind, flags, n, gen = struct.unpack("<HHIQ", rec[4:20])
    if flags != 0 or len(rec) != HEADER + n + 32:
        sys.exit(f"bad envelope at {key}")
    if hashlib.sha256(rec[:HEADER + n]).digest() != rec[HEADER + n:]:
        sys.exit(f"content digest mismatch at {key}")
    return kind, gen, rec[HEADER:HEADER + n], rec[HEADER + n:]


def addr(family, raw):
    return str(ipaddress.ip_address(raw[12:] if family == 4 else raw))


STATES = ["unknown", "ready", "degraded", "draining", "suspect", "unreachable", "fenced"]
SELECTION = ["maglev", "rendezvous", "least_loaded_bounded"]
CONTINUITY = ["reroutable", "drain_only", "resumable", "edge_anchored", "transport_migratable"]
AFFINITY = ["flow", "application_key", "none"]


def keys(ns, svc):
    t, s = ident("nc-tenant", ns).hex(), ident("nc-service", ns, svc).hex()
    return f"/wh/{t}/{s}/"


def check(kv, ns, svc):
    base = keys(ns, svc)
    act = kv.get((base + "active").encode())
    if act is None:
        sys.exit("no active pointer")
    a = dict(f.split("=", 1) for f in act.decode().split(";"))
    g = int(a["gen"])
    pre = f"{base}gen/{g}/".encode()
    recs = {k: v for k, v in kv.items() if k.startswith(pre)}
    seal = recs.pop(pre + b"seal", None)
    if seal is None:
        sys.exit("no seal")
    out = [f"active gen={g} routes={a['routes']} records={a['records']}"]
    if len(recs) + 1 != int(a["records"]):
        sys.exit(f"{len(recs) + 1} records under the generation, the pointer says {a['records']}")
    # The seal: SHA-256 over (key length, key, content digest), key order.
    kind, sgen, sp, sdig = envelope(seal, "seal")
    h = hashlib.sha256()
    digests = {}
    for k in sorted(recs):
        _kind, rgen, payload, d = envelope(recs[k], k.decode())
        if rgen != g:
            sys.exit(f"{k.decode()}: envelope generation {rgen}, want {g}")
        digests[k] = (d, payload, _kind)
        h.update(struct.pack("<H", len(k)) + k + d)
    if kind != 0x82 or sp[:16] != ident("nc-service", ns, svc) or struct.unpack("<Q", sp[16:24])[0] != g:
        sys.exit("bad seal header")
    if sp[32:64] != h.digest():
        sys.exit("the seal's digest does not match the records")
    if a["seal"] != sdig.hex():
        sys.exit("the active pointer names another seal")
    routes = sorted({k[len(pre):].split(b"/")[0] for k in recs if k[len(pre):].startswith(b"r")})
    for r in routes:
        rp = pre + r + b"/"
        eps = sorted(k for k in recs if k.startswith(rp + b"ep/"))
        md, mp, mk = digests[rp + b"manifest"]
        count = struct.unpack("<I", mp[24:28])[0]
        if mk != 3 or count != len(eps):
            sys.exit(f"{r.decode()}: manifest count {count}, {len(eps)} endpoint records")
        if hashlib.sha256(b"".join(digests[e][0] for e in eps)).digest() != mp[28:60]:
            sys.exit(f"{r.decode()}: the manifest digest is not its endpoints'")
        rd, rpay, rk = digests[rp + b"route"]
        if rk != 1 or rpay[212:228] != md[:16]:
            sys.exit(f"{r.decode()}: the route does not name its manifest")
        fam, transport, port = rpay[40], rpay[41], struct.unpack("<H", rpay[42:44])[0]
        out.append(f"route {r.decode()} {'tcp' if transport == 6 else 'udp'} {addr(fam, rpay[44:60])}:{port} "
                   f"{SELECTION[rpay[62]]} {CONTINUITY[rpay[63]]} {AFFINITY[rpay[65]]} endpoints={len(eps)}")
        for e in eps:
            _d, ep, ek = digests[e]
            if ek != 2 or ep[16:24] == b"" or ep[24:40] != ident("nc-service", ns, svc):
                sys.exit(f"{e.decode()}: not this Service's endpoint")
            if e.split(b"/")[-1].decode() != ep[:16].hex():
                sys.exit(f"{e.decode()}: keyed by another identity")
            egen = struct.unpack("<Q", ep[16:24])[0]
            eport = struct.unpack("<H", ep[42:44])[0]
            out.append(f"endpoint {r.decode()} {addr(ep[40], ep[44:60])}:{eport} {STATES[ep[41]]} gen={egen}")
    adm = digests.get(pre + b"admission")
    if adm:
        p = adm[1]
        for i in range(struct.unpack("<H", p[16:18])[0]):
            e = p[20 + 20 * i:40 + 20 * i]
            out.append(f"admission {addr(e[0], e[4:20])}/{e[1]}")
    print("\n".join(out))


def put_vip(path, ns, svc, fam, phase, address):
    a = ipaddress.ip_address(address)
    raw = (b"\0" * 12 + a.packed) if fam == 4 else a.packed
    epoch = 3
    # service_id family prefix_len mode phase prefix epoch counts(3)+reserved
    p = ident("nc-service", ns, svc) + bytes([fam, 32 if fam == 4 else 128, 0, phase]) + raw
    p += struct.pack("<Q", epoch) + bytes(4)
    p += bytes(8 * 24) + bytes(8 * 16) + bytes(8 * 16) + bytes(16) + struct.pack("<Q", 0)
    assert len(p) == 520
    head = b"WHR1" + struct.pack("<HHIQ", 5, 0, len(p), epoch) + bytes(16)
    rec = head + p
    rec += hashlib.sha256(rec).digest()
    key = (keys(ns, svc) + f"vip/{fam}").encode()
    with open(path, "r+b") as f:
        data = f.read()
        q = last = 0
        while q + 15 <= len(data):
            rev, _op, kl, vl = struct.unpack("<QBHI", data[q:q + 15])
            if q + 15 + kl + vl > len(data):
                break
            last, q = rev, q + 15 + kl + vl
        f.truncate(q)
        f.seek(0, 2)
        f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(rec)) + key + rec)


def main():
    if sys.argv[2] == "put-vip":
        put_vip(sys.argv[1], sys.argv[3], sys.argv[4], int(sys.argv[5]), int(sys.argv[6]), sys.argv[7])
        return
    kv = live(sys.argv[1])
    cmd = sys.argv[2]
    if cmd == "check":
        check(kv, sys.argv[3], sys.argv[4])
    elif cmd == "active-key":
        print(keys(sys.argv[3], sys.argv[4]) + "active")
    elif cmd == "gen-prefix":
        print(f"{keys(sys.argv[3], sys.argv[4])}gen/{sys.argv[5]}/")
    else:
        sys.exit(__doc__)


main()
