//! cert_manager — nanocloud's ONE-SHOT certificate minting service as a PIC
//! module. Unlike the reconciler-style cert-manager.io controller, a request
//! MINTS a fresh certificate + key on the spot and returns it (request/response
//! over the store seam, the `crypto_signer` shape). On start it self-generates
//! a CA keypair, deposits the CA private key in the kernel KEY_VAULT (contract
//! 0x0010, an infra contract — no `requires_contract`), wipes the in-module
//! copy, and publishes a self-signed CA certificate. Each request mints a leaf
//! keypair, builds a real X.509 certificate in-module (ASN.1 DER), signs the
//! TBSCertificate with the CA key BY HANDLE (the CA private key never re-enters
//! the module), and returns the cert + key.
//!
//! Data model (request/response over the store):
//!
//!   /ca-cert            = "<self-signed CA cert, DER hex>"        (on start)
//!   /cert-req/<id>      = "cn=<commonName>;dns=<dnsName>"
//!   /cert-resp/<id>     = "crt=<leaf cert, DER hex>;key=<leaf P-256 scalar, hex>"
//!
//! Proves nanocloud can issue real, openssl-verifiable X.509 certificates as a
//! module on EXISTING fluxor crypto — no new capability surface. Validity is a
//! fixed 2025–2035 window; a real notBefore/notAfter would be read from the
//! clock, which is one of the reasons this module is development-only.

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
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/store.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes + CAS, storage.namespace
// (0x13) prefix LIST + change SUBSCRIBE. Changes are pushed to us as
// namespace.change on the `changes` input channel (allocated by a graph
// self-edge); we drain them to detect movement, then re-service.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const KV_STORE: u32 = 0x1001;
const KV_SIGN: u32 = 0x1003;
/// `key_vault::suite::P256` — the suite id the key is created under.
const KV_SUITE_P256: u16 = 1;
/// `usage::SIGN`, sealed at creation and checked per operation.
const KV_USAGE_SIGN: u32 = 1;
/// `sign_mode::DIGEST` — P-256 signs the digest, stated on the wire rather
/// than inferred from the key type.
const KV_SIGN_MODE_DIGEST: u8 = 1;

const REQ_PREFIX: &[u8] = b"/cert-req/";
const RESP_PREFIX: &[u8] = b"/cert-resp/";
const CA_CERT_KEY: &[u8] = b"/ca-cert";
const CA_CN: &[u8] = b"nanocloud-ca";

// ASN.1 OIDs (encoded content bytes).
const OID_CN: &[u8] = &[0x55, 0x04, 0x03]; // 2.5.4.3 id-at-commonName
const OID_EC_PUBKEY: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01]; // 1.2.840.10045.2.1
const OID_PRIME256V1: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x03, 0x01, 0x07]; // 1.2.840.10045.3.1.7
const OID_ECDSA_SHA256: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x02]; // 1.2.840.10045.4.3.2
const OID_SAN: &[u8] = &[0x55, 0x1D, 0x11]; // 2.5.29.17
const OID_BASIC_CONSTRAINTS: &[u8] = &[0x55, 0x1D, 0x13]; // 2.5.29.19

// Fixed validity window (UTCTime YYMMDDHHMMSSZ): 2025-01-01 .. 2035-01-01.
const NOT_BEFORE: &[u8] = b"250101000000Z";
const NOT_AFTER: &[u8] = b"350101000000Z";

const MAX_KEY: usize = 96;
const MAX_VALUE: usize = 256;
const RESP_MAX: usize = 1600;
const DER_CAP: usize = 900;
// ── Development gate ───────────────────────────────────────────────────────
//
// This module REFUSES TO CONSTRUCT unless the graph sets `development: 1`.
//
// Not deprecated, not merely discouraged: a production graph that names it
// gets a module that will not start, which is the difference between a rule
// and a note in a document. The end state is deletion — kagi's
// `certificate_endpoint` is the issuer a deployment uses — and it stands only
// because nothing else answers the `/cert-req/` seam `device_reconciler`
// writes to, and removing an issuer before its replacement exists is a
// regression rather than a fix.
//
// What is wrong with it, recorded here so the gate is not mistaken for
// caution:
//
//   * The CA keypair is REGENERATED AT STARTUP, so every restart is a new
//     trust root and every certificate it ever issued stops verifying, with
//     nothing saying so.
//   * The validity window is a FIXED 2025-2035 — a certificate cannot expire
//     within a decade, and cannot be issued outside one.
//   * The leaf PRIVATE KEY is returned as PLAINTEXT HEX through shared object
//     storage, where anything that can read the store can take it. A
//     private key that travels is not a private key.
//   * The caller chooses its own CN, DNS name and SPIFFE URI, so the
//     certificate asserts whatever the requester asked to be called.
//   * Serials derive from the CN, so two requests for the same subject
//     collide — and revocation is keyed on exactly that.
//
// A parameter rather than a build feature, because a graph is what a
// deployment writes and reviews, and this is a statement about a deployment.

mod params_def {
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, development, u8, 0
            => |s, d, len| { if len >= 1 { s.development = *d; } };
    }
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    /// 1 when the graph declared `development: 1`. See the development gate.
    development: u8,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE has run.
    subscribed: u8,
    ca_handle: i32,
    ca_pub: [u8; 65],
    ready: u8,
    minted: u32,
}

// ---- KEY_VAULT (in-place arg convention) ----

unsafe fn kv_store(sys: &SyscallTable, scalar: &[u8; 32]) -> i32 {
    // KEY_VAULT: `[suite:u16][usage_mask:u32][key_len:u32][key]`. The shape
    // is exact — a call built to a different one is refused with EINVAL, and
    // the module then publishes no CA certificate at all.
    let mut arg = [0u8; 10 + 32];
    arg[0..2].copy_from_slice(&KV_SUITE_P256.to_le_bytes());
    arg[2..6].copy_from_slice(&KV_USAGE_SIGN.to_le_bytes());
    arg[6..10].copy_from_slice(&32u32.to_le_bytes());
    arg[10..42].copy_from_slice(scalar);
    (sys.provider_call)(-1, KV_STORE, arg.as_mut_ptr(), arg.len())
}

unsafe fn kv_sign(sys: &SyscallTable, handle: i32, hash: &[u8; 32]) -> Option<[u8; 64]> {
    // KEY_VAULT: `[sign_mode:u8][_pad:u8][input_len:u32][input]
    // [sig_out_ptr:u64][sig_out_cap:u16][sig_len_out:u16]`. The signature is
    // variable-length through a caller pointer: 64 bytes is right for ES256
    // and wrong for every suite past it, so the caller sizes the buffer.
    let mut sig = [0u8; 64];
    let mut arg = [0u8; 1 + 1 + 4 + 32 + 8 + 2 + 2];
    arg[0] = KV_SIGN_MODE_DIGEST;
    arg[2..6].copy_from_slice(&32u32.to_le_bytes());
    arg[6..38].copy_from_slice(hash);
    arg[38..46].copy_from_slice(&(sig.as_mut_ptr() as u64).to_le_bytes());
    arg[46..48].copy_from_slice(&64u16.to_le_bytes());
    let rc = (sys.provider_call)(handle, KV_SIGN, arg.as_mut_ptr(), arg.len());
    if rc != 0 {
        return None;
    }
    // A P-256 slot that produced anything but 64 bytes is not a P-256 slot.
    if u16::from_le_bytes([arg[48], arg[49]]) != 64 {
        return None;
    }
    Some(sig)
}

// ---- ASN.1 DER back-to-front writer ----

struct Der {
    buf: [u8; DER_CAP],
    pos: usize,
}

impl Der {
    fn new() -> Self {
        Der {
            buf: [0u8; DER_CAP],
            pos: DER_CAP,
        }
    }
    fn bytes(&self) -> &[u8] {
        &self.buf[self.pos..]
    }
    fn push(&mut self, b: &[u8]) {
        self.pos -= b.len();
        self.buf[self.pos..self.pos + b.len()].copy_from_slice(b);
    }
    fn push_byte(&mut self, x: u8) {
        self.pos -= 1;
        self.buf[self.pos] = x;
    }
    /// Prepend a DER length (short or long form).
    fn push_len(&mut self, len: usize) {
        if len < 0x80 {
            self.push_byte(len as u8);
        } else {
            let mut tmp = [0u8; 5];
            let mut n = len;
            let mut k = 0;
            while n > 0 {
                tmp[k] = (n & 0xFF) as u8;
                n >>= 8;
                k += 1;
            }
            for &b in &tmp[..k] {
                self.push_byte(b); // LSB..MSB pushed → buffer reads MSB..LSB
            }
            self.push_byte(0x80 | k as u8);
        }
    }
    /// Wrap the content currently at `buf[pos..end]` in `tag` + length.
    fn tlv(&mut self, tag: u8, end: usize) {
        let len = end - self.pos;
        self.push_len(len);
        self.push_byte(tag);
    }
    fn oid(&mut self, oid: &[u8]) {
        let end = self.pos;
        self.push(oid);
        self.tlv(0x06, end);
    }
    fn utf8(&mut self, s: &[u8]) {
        let end = self.pos;
        self.push(s);
        self.tlv(0x0C, end);
    }
    /// AlgorithmIdentifier { ecdsa-with-SHA256 }.
    fn alg_ecdsa_sha256(&mut self) {
        let end = self.pos;
        self.oid(OID_ECDSA_SHA256);
        self.tlv(0x30, end);
    }
    /// Name ::= SEQUENCE { SET { SEQUENCE { OID CN, UTF8String cn } } }.
    fn name_cn(&mut self, cn: &[u8]) {
        let n = self.pos;
        {
            let s = self.pos;
            {
                let a = self.pos;
                self.utf8(cn);
                self.oid(OID_CN);
                self.tlv(0x30, a); // ATV SEQUENCE
            }
            self.tlv(0x31, s); // SET
        }
        self.tlv(0x30, n); // Name SEQUENCE
    }
    /// validity SEQUENCE { UTCTime notBefore, UTCTime notAfter }.
    fn validity(&mut self) {
        let end = self.pos;
        {
            let e = self.pos;
            self.push(NOT_AFTER);
            self.tlv(0x17, e);
        }
        {
            let e = self.pos;
            self.push(NOT_BEFORE);
            self.tlv(0x17, e);
        }
        self.tlv(0x30, end);
    }
    /// SubjectPublicKeyInfo for an uncompressed P-256 point.
    fn spki(&mut self, pubkey: &[u8; 65]) {
        let end = self.pos;
        // subjectPublicKey BIT STRING: [unused=0x00][point]
        {
            let e = self.pos;
            self.push(pubkey);
            self.push_byte(0x00);
            self.tlv(0x03, e);
        }
        // algorithm SEQUENCE { id-ecPublicKey, prime256v1 }
        {
            let e = self.pos;
            self.oid(OID_PRIME256V1);
            self.oid(OID_EC_PUBKEY);
            self.tlv(0x30, e);
        }
        self.tlv(0x30, end);
    }
    /// extensions [3] { SEQUENCE OF Extension } — a SAN carrying the SPIFFE
    /// `uniformResourceIdentifier` (when `spiffe` is non-empty) plus a
    /// `dNSName`. The SPIFFE URI is the family-canonical identity a relying
    /// party (e.g. fluxor's `tls` module) authorizes on; the SVID it derives is
    /// `SHA-256(subjectPublicKey)`, so nothing is stamped for it here.
    fn extensions_san(&mut self, spiffe: &[u8], dns: &[u8]) {
        let ctx = self.pos;
        {
            let seq = self.pos;
            {
                let ext = self.pos;
                // extnValue OCTET STRING wrapping GeneralNames { [URI [6],] dNSName [2] }
                {
                    let oct = self.pos;
                    {
                        let gn = self.pos;
                        // dNSName [2] (pushed first → last in the output SEQUENCE)
                        {
                            let e = self.pos;
                            self.push(dns);
                            self.tlv(0x82, e); // dNSName [2] IMPLICIT IA5String
                        }
                        // uniformResourceIdentifier [6] — the SPIFFE id (pushed
                        // last → first in output). Omitted when empty.
                        if !spiffe.is_empty() {
                            let e = self.pos;
                            self.push(spiffe);
                            self.tlv(0x86, e); // URI [6] IMPLICIT IA5String
                        }
                        self.tlv(0x30, gn); // GeneralNames SEQUENCE
                    }
                    self.tlv(0x04, oct); // OCTET STRING extnValue
                }
                self.oid(OID_SAN); // extnID
                self.tlv(0x30, ext); // Extension SEQUENCE
            }
            self.tlv(0x30, seq); // SEQUENCE OF Extension
        }
        self.tlv(0xA3, ctx); // [3] EXPLICIT
    }
    /// extensions [3] { basicConstraints cA=TRUE critical } — for the CA cert.
    fn extensions_ca(&mut self) {
        let ctx = self.pos;
        {
            let seq = self.pos;
            {
                let ext = self.pos;
                // extnValue OCTET STRING wrapping SEQUENCE { BOOLEAN cA=TRUE }
                {
                    let oct = self.pos;
                    {
                        let bc = self.pos;
                        self.push(&[0x01, 0x01, 0xFF]); // BOOLEAN TRUE
                        self.tlv(0x30, bc); // BasicConstraints SEQUENCE
                    }
                    self.tlv(0x04, oct); // OCTET STRING extnValue
                }
                self.push(&[0x01, 0x01, 0xFF]); // critical BOOLEAN TRUE
                self.oid(OID_BASIC_CONSTRAINTS); // extnID
                self.tlv(0x30, ext); // Extension SEQUENCE
            }
            self.tlv(0x30, seq); // SEQUENCE OF Extension
        }
        self.tlv(0xA3, ctx); // [3] EXPLICIT
    }
    /// serialNumber INTEGER (positive).
    fn serial(&mut self, bytes: &[u8]) {
        let end = self.pos;
        self.push(bytes);
        if bytes[0] & 0x80 != 0 {
            self.push_byte(0x00);
        }
        self.tlv(0x02, end);
    }
    /// version [0] EXPLICIT INTEGER 2 (v3).
    fn version_v3(&mut self) {
        let end = self.pos;
        {
            let e = self.pos;
            self.push_byte(0x02);
            self.tlv(0x02, e); // INTEGER 2
        }
        self.tlv(0xA0, end); // [0] EXPLICIT
    }
}

/// Build a TBSCertificate into `out`; returns its length. `is_ca` selects the
/// basicConstraints vs SAN extension. Subject/issuer are CNs; the subject key is
/// `subject_pub`.
#[allow(
    clippy::too_many_arguments,
    reason = "in-module DER builder entry point"
)]
fn build_tbs(
    out: &mut [u8],
    serial: &[u8],
    issuer_cn: &[u8],
    subject_cn: &[u8],
    subject_pub: &[u8; 65],
    spiffe: &[u8],
    dns: &[u8],
    is_ca: bool,
) -> usize {
    let mut d = Der::new();
    let end = d.pos;
    if is_ca {
        d.extensions_ca();
    } else {
        d.extensions_san(spiffe, dns);
    }
    d.spki(subject_pub);
    d.name_cn(subject_cn);
    d.validity();
    d.name_cn(issuer_cn);
    d.alg_ecdsa_sha256();
    d.serial(serial);
    d.version_v3();
    d.tlv(0x30, end); // TBSCertificate SEQUENCE
    let b = d.bytes();
    let n = b.len().min(out.len());
    out[..n].copy_from_slice(&b[..n]);
    n
}

/// Assemble a Certificate (tbs ++ sigAlg ++ signatureValue) into `out`.
fn build_cert(out: &mut [u8], tbs: &[u8], der_sig: &[u8]) -> usize {
    let mut d = Der::new();
    let end = d.pos;
    // signatureValue BIT STRING: [unused=0x00][DER ECDSA-Sig-Value]
    {
        let e = d.pos;
        d.push(der_sig);
        d.push_byte(0x00);
        d.tlv(0x03, e);
    }
    d.alg_ecdsa_sha256();
    d.push(tbs); // tbsCertificate (already a SEQUENCE)
    d.tlv(0x30, end); // Certificate SEQUENCE
    let b = d.bytes();
    let n = b.len().min(out.len());
    out[..n].copy_from_slice(&b[..n]);
    n
}

// ---- helpers ----

fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';' || b == b',')
            .map(|i| start + i)
            .unwrap_or(value.len());
        let seg = &value[start..end];
        if seg.len() >= tag.len() && &seg[..tag.len()] == tag {
            return Some(&seg[tag.len()..]);
        }
        if end >= value.len() {
            break;
        }
        start = end + 1;
    }
    None
}

fn hex_encode(src: &[u8], dst: &mut [u8]) -> usize {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut p = 0;
    for &b in src {
        if p + 2 > dst.len() {
            break;
        }
        dst[p] = H[(b >> 4) as usize];
        dst[p + 1] = H[(b & 0x0f) as usize];
        p += 2;
    }
    p
}

/// Sign a TBS with the CA key and assemble the full cert into `out`. Returns len.
unsafe fn sign_and_assemble(sys: &SyscallTable, handle: i32, tbs: &[u8], out: &mut [u8]) -> usize {
    let hash = sha256(tbs);
    let Some(sig) = kv_sign(sys, handle, &hash) else {
        return 0;
    };
    let (der_sig, der_len) = encode_der_signature(&sig);
    build_cert(out, tbs, &der_sig[..der_len])
}

/// Mint a leaf certificate for one request. Returns bytes written to `resp`
/// ("crt=<hex>;key=<hex>"), or 0 on failure.
unsafe fn mint(sys: &SyscallTable, s: &State, req: &[u8], resp: &mut [u8]) -> usize {
    let cn = field(req, b"cn=").unwrap_or(b"leaf");
    let dns = field(req, b"dns=").unwrap_or(cn);
    // Optional SPIFFE id (`spiffe=spiffe://<td>/...`). When present it is
    // stamped as a URI SAN — the family-canonical identity.
    let spiffe = field(req, b"spiffe=").unwrap_or(b"");

    // Leaf keypair.
    let mut random = [0u8; 32];
    if dev_csprng_fill(sys, random.as_mut_ptr(), 32) < 0 {
        return 0;
    }
    let (mut leaf_priv, leaf_pub) = ecdh_keygen(&random);
    for b in random.iter_mut() {
        core::ptr::write_volatile(b, 0);
    }

    // Serial from a hash of the subject (positive).
    let serialh = sha256(cn);
    let serial = &serialh[..8];

    let mut tbs = [0u8; DER_CAP];
    let tl = build_tbs(&mut tbs, serial, CA_CN, cn, &leaf_pub, spiffe, dns, false);
    let mut cert = [0u8; DER_CAP];
    let cl = sign_and_assemble(sys, s.ca_handle, &tbs[..tl], &mut cert);
    if cl == 0 {
        for b in leaf_priv.iter_mut() {
            core::ptr::write_volatile(b, 0);
        }
        return 0;
    }

    // Response: "crt=<cert hex>;key=<priv hex>".
    let mut p = 0;
    resp[p..p + 4].copy_from_slice(b"crt=");
    p += 4;
    p += hex_encode(&cert[..cl], &mut resp[p..]);
    resp[p..p + 5].copy_from_slice(b";key=");
    p += 5;
    p += hex_encode(&leaf_priv, &mut resp[p..]);
    for b in leaf_priv.iter_mut() {
        core::ptr::write_volatile(b, 0);
    }
    p
}

/// Mint a leaf for every /cert-req/ without a response yet.
unsafe fn reconcile(sys: &SyscallTable, s: &State) -> u32 {
    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut served = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        if klen <= REQ_PREFIX.len() {
            continue;
        }
        let tail = &key[REQ_PREFIX.len()..];

        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);
        let mut probe = [0u8; RESP_MAX];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        let mut req = [0u8; MAX_VALUE];
        let Some(rqlen) = get_value(sys, key, &mut req) else {
            continue;
        };

        let mut resp = [0u8; RESP_MAX];
        let n = mint(sys, s, &req[..rqlen], &mut resp);
        if n > 0 && put_value(sys, &rkey[..rlen], &resp[..n]) {
            served += 1;
        }
    }
    served
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

/// Kernel module-ABI entry point.
///
/// # Safety
///
/// `state`/`syscalls` are the loader-owned instance arena and syscall table,
/// and `params` is the config TLV blob (may be null when `params_len == 0`).
/// All are valid for the lifetime the loader guarantees and are never called
/// concurrently. `unsafe` because it READS `params` — the development gate
/// below is driven from it.
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
        // The development gate — see the block above `struct State`. Read the
        // params BEFORE anything else touches the world: this module's next
        // act is to generate a CA keypair and publish it, and a refusal that
        // happened after that would have already done the thing it refuses.
        s.development = 0;
        params_def::parse_tlv(s, params, params_len);
        if s.development != 1 {
            let m: &[u8] = b"[cert_manager] REFUSING TO START: development-only module (CA regenerated at startup, plaintext leaf key in the store, caller-chosen SAN). Set `development: 1` in the graph, or use kagi's certificate_endpoint.";
            dev_log(&*s.syscalls, 1, m.as_ptr(), m.len());
            return -3;
        }
        s.out_chan = out_chan;
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.ca_handle = -1;
        s.ca_pub = [0u8; 65];
        s.ready = 0;
        s.minted = 0;
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

        // One-time CA setup: generate CA keypair → vault → self-signed CA cert.
        if s.ready == 0 {
            let mut random = [0u8; 32];
            if dev_csprng_fill(sys, random.as_mut_ptr(), 32) < 0 {
                return 0;
            }
            let (mut ca_priv, ca_pub) = ecdh_keygen(&random);
            let handle = kv_store(sys, &ca_priv);
            for b in ca_priv.iter_mut() {
                core::ptr::write_volatile(b, 0);
            }
            for b in random.iter_mut() {
                core::ptr::write_volatile(b, 0);
            }
            if handle < 0 {
                return 0;
            }
            s.ca_handle = handle;
            s.ca_pub = ca_pub;

            // Self-signed CA cert (issuer == subject == CA_CN, basicConstraints CA).
            let mut tbs = [0u8; DER_CAP];
            let tl = build_tbs(&mut tbs, &[0x01], CA_CN, CA_CN, &ca_pub, b"", b"", true);
            let mut cert = [0u8; DER_CAP];
            let cl = sign_and_assemble(sys, s.ca_handle, &tbs[..tl], &mut cert);
            if cl > 0 {
                let mut hex = [0u8; DER_CAP * 2];
                let n = hex_encode(&cert[..cl], &mut hex);
                put_value(sys, CA_CERT_KEY, &hex[..n]);
            }

            // Resolve the change-sink channel (self-edge allocated) and SUBSCRIBE
            // /cert-req/ onto it, then mint for any pre-existing requests.
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.ready = 1;
            s.minted = s.minted.wrapping_add(reconcile(sys, s));
            return 0;
        }

        // A pushed namespace.change means new /cert-req/ to mint.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.minted = s.minted.wrapping_add(reconcile(sys, s));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
