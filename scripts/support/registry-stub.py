#!/usr/bin/env python3
"""A registry for the image-plane E2Es: the OCI distribution API's two GETs,
with private repositories behind Basic or Bearer (token) auth.

    registry-stub.py <dir> <port> [--private <repo-prefix>] [--auth basic|bearer]
                     [--user <user:password>] [--realm-port <port>]

<dir> holds content-addressed blobs (`<dir>/<hex>`: layers, configs,
manifests and indexes alike) and `<dir>/tags.json`, {"<repo>:<tag>": "<hex>"},
re-read on every request so a test can move a tag. A manifest's media type is
its own `mediaType`. Every request is logged to stderr as
`<status> GET <path>` (`TOKEN <basic ok|no>` for the realm's).

A repository under --private answers 401 to a request without credentials:
`WWW-Authenticate: Basic realm="stub"` (basic), or a Bearer challenge naming
the token realm on --realm-port (bearer), whose tokens it then accepts. Either
way the credentials that serve are --user's.
"""
import argparse
import base64
import http.server
import json
import os
import secrets
import socketserver
import sys
import threading
import urllib.parse

ap = argparse.ArgumentParser()
ap.add_argument("dir")
ap.add_argument("port", type=int)
ap.add_argument("--private", default="")
ap.add_argument("--auth", default="basic")
ap.add_argument("--user", default="puller:s3cret")
ap.add_argument("--realm-port", type=int, default=0)
a = ap.parse_args()
BASIC = "Basic " + base64.b64encode(a.user.encode()).decode()
tokens, lock = set(), threading.Lock()


def log(m):
    sys.stderr.write(m + "\n")
    sys.stderr.flush()


class Base(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def answer(self, status, body=b"", ctype="application/octet-stream", extra=()):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        for k, v in extra:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class Realm(Base):
    def do_GET(self):
        ok = self.headers.get("Authorization", "") == BASIC
        log(f"TOKEN basic {'ok' if ok else 'no'} {self.path}")
        if not ok:
            return self.answer(401, b"{}", "application/json")
        t = secrets.token_urlsafe(32)
        with lock:
            tokens.add(t)
        self.answer(200, json.dumps({"token": t}).encode(), "application/json")


class Reg(Base):
    def authorized(self):
        auth = self.headers.get("Authorization", "")
        if a.auth == "basic":
            return auth == BASIC
        with lock:
            return auth.startswith("Bearer ") and auth[7:] in tokens

    def do_GET(self):
        parts = self.path.strip("/").split("/")
        if len(parts) < 4 or parts[0] != "v2" or parts[-2] not in ("manifests", "blobs"):
            log(f"404 GET {self.path}")
            return self.answer(404)
        repo, kind, ref = "/".join(parts[1:-2]), parts[-2], parts[-1]
        if a.private and repo.startswith(a.private) and not self.authorized():
            log(f"401 GET {self.path}")
            if a.auth == "basic":
                ch = 'Basic realm="stub"'
            else:
                ch = (f'Bearer realm="http://127.0.0.1:{a.realm_port}/token",'
                      f'service="stub",scope="repository:{repo}:pull"')
            return self.answer(401, b'{"errors":[{"code":"UNAUTHORIZED"}]}', "application/json",
                               extra=[("WWW-Authenticate", ch)])
        hexd = ref.split(":")[-1] if ref.startswith("sha256:") else ""
        if kind == "manifests" and not hexd:
            try:
                tags = json.load(open(os.path.join(a.dir, "tags.json")))
            except (OSError, ValueError):
                tags = {}
            hexd = tags.get(f"{repo}:{ref}", "")
        p = os.path.join(a.dir, hexd) if hexd else ""
        if not p or not os.path.exists(p):
            log(f"404 GET {self.path}")
            return self.answer(404)
        body = open(p, "rb").read()
        ctype = "application/octet-stream"
        if kind == "manifests":
            ctype = json.loads(body).get("mediaType", "application/vnd.oci.image.manifest.v1+json")
        log(f"200 GET {self.path}")
        self.answer(200, body, ctype)


class S(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if a.realm_port:
    r = S(("127.0.0.1", a.realm_port), Realm)
    threading.Thread(target=r.serve_forever, daemon=True).start()
with S(("127.0.0.1", a.port), Reg) as srv:
    srv.serve_forever()
