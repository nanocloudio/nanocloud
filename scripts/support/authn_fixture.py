#!/usr/bin/env python3
"""Mint what `authn`'s e2e needs: a kagi key frame and two signed JWTs.

Prints three lines: a live JWT, the `MSG_KEY_ADD` frame as hex, and an
expired JWT.

`authn` VERIFIES bearer credentials rather than looking them up, so a fixture
has to be a real credential: a bare string in the store is not a test of a
verifier — a module that accepted anything would pass it. Everything here is
signed with a real P-256 key, and the expired token is signed by the SAME key,
so the only thing separating it from the live one is its window.
"""
import base64, os, struct, subprocess, sys, tempfile, time

MSG_KEY_ADD = 0x22
SUITE_ES256 = 1
PROFILE_ACCESS_TOKEN = 1
KEY_STATE_ACTIVE = 1
KEY_USE_VERIFY = 0x01
ISSUER = b"https://nanocloud.local"
KID = b"k1"
SUBJECT = b"system:serviceaccount:default:api"


def b64u(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=")


def f8(b):
    return bytes([len(b)]) + b


def f16(b):
    return struct.pack("<H", len(b)) + b


def openssl(args):
    return subprocess.run(["openssl", *args], check=True, capture_output=True).stdout


def public_point(key_path):
    """The uncompressed SEC1 point, read out of `openssl ec -text`."""
    txt = openssl(["ec", "-in", key_path, "-text", "-noout"]).decode()
    take, hexes = False, []
    for line in txt.splitlines():
        if line.strip().startswith("pub:"):
            take = True
            continue
        if take:
            if ":" not in line:
                break
            hexes += [x for x in line.strip().split(":") if x]
            if len(bytes.fromhex("".join(hexes))) >= 65:
                break
    pub = bytes.fromhex("".join(hexes))[:65]
    if len(pub) != 65 or pub[0] != 0x04:
        sys.exit("could not read an uncompressed P-256 public point")
    return pub


def key_add(pub):
    """kagi `MSG_KEY_ADD` — a KeyRecord carrying the PUBLIC half only."""
    body = (
        f8(ISSUER)
        + struct.pack("<H", PROFILE_ACCESS_TOKEN)
        + f8(KID)
        + struct.pack("<H", SUITE_ES256)
        + bytes([KEY_STATE_ACTIVE, KEY_USE_VERIFY])
        + struct.pack("<I", 1)
        + struct.pack("<Q", 0)
        + struct.pack("<Q", 0)
        + f16(pub)
    )
    return bytes([MSG_KEY_ADD]) + struct.pack("<H", len(body)) + body


def der_to_raw(der):
    """DER SEQUENCE{INTEGER r, INTEGER s} -> the 64-byte r||s JOSE form."""
    assert der[0] == 0x30
    i = 2 if der[1] < 0x80 else 3 + (der[1] & 0x7F) - 1
    out = b""
    for _ in range(2):
        assert der[i] == 0x02
        n = der[i + 1]
        v = der[i + 2 : i + 2 + n]
        i += 2 + n
        out += v.lstrip(b"\x00").rjust(32, b"\x00")
    return out


def sign_jws(key_path, iat, exp):
    hdr = b'{"alg":"ES256","typ":"JWT","kid":"' + KID + b'"}'
    payload = (
        b'{"iss":"' + ISSUER + b'","sub":"' + SUBJECT + b'"'
        b',"aud":"nanocloud","iat":' + str(iat).encode()
        + b',"exp":' + str(exp).encode() + b"}"
    )
    signing_input = b64u(hdr) + b"." + b64u(payload)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(signing_input)
        tmp = f.name
    try:
        der = openssl(["dgst", "-sha256", "-sign", key_path, tmp])
    finally:
        os.unlink(tmp)
    return (signing_input + b"." + b64u(der_to_raw(der))).decode()


if __name__ == "__main__":
    key_path = sys.argv[1]
    now = int(time.time())
    print(sign_jws(key_path, now, now + 3600))
    print(key_add(public_point(key_path)).hex())
    print(sign_jws(key_path, now - 7200, now - 3600))
