// The per-volume node graph a graph host stages as a workload (fluxor
// `workload` CREATE, fmod-graph source): the FLXA subgraph blob and its
// modules' parameters, built at run time from one volume's facts.
//
//   nbd_serve.blocks  <- crypt_block.blocks
//   crypt_block.lower <- loam_volume.blocks
//   loam_volume admin plane <-> tls clear <-> network (loam's admin_gate)
//   nbd_serve net     <-> network
//   crypt_block recipient, status -> the host; the host -> crypt_block bundle
//
// Two network lanes (`Net` keys) and three lanes of the host's own
// (`Caller` keys): the kernel opens every channel, and the host reads its
// lanes back with `workload::LANES`. FLXA is big-endian; module parameters
// are the SDK TLV (little-endian lengths), with `tls`'s credentials in the
// extended entries after the end marker, as the config builder lays them.

/// FLXA magic, "FLXA", and wire version.
pub const FLXA_MAGIC: u32 = 0x464C_5841;
pub const FLXA_VERSION: u16 = 1;

const EP_NEW: u8 = 0;
const EP_NET: u8 = 3;
const EP_CALLER: u8 = 4;
/// `to_port_index` bit addressing a control input.
const CTRL: u8 = 0x80;

/// Module positions in the blob.
pub const M_TLS: u16 = 0;
pub const M_VOLUME: u16 = 1;
pub const M_CRYPT: u16 = 2;
pub const M_NBD: u16 = 3;

/// Network lanes: the Loam admin connection, and the NBD listener.
pub const NET_LOAM: u16 = 0;
pub const NET_NBD: u16 = 1;
/// The host's lanes: crypt_block's recipient announcement and status record
/// to the host, and the host's bundle and control records to crypt_block.
pub const CALLER_RECIPIENT: u8 = 0;
pub const CALLER_STATUS: u8 = 1;
pub const CALLER_BUNDLE: u8 = 2;

/// One volume's node graph.
pub struct VolumeGraph<'a> {
    /// The workload's identity: its owner in the node's graph.
    pub owner_uid: [u8; 16],
    /// Admitted module-state bytes (0 = unlimited).
    pub state_cap: u32,
    /// Loam: the admin gate's `host:port` to dial, the capability file whose
    /// chains the volume presents to it, the name its certificate must carry
    /// (empty to check the dialled host), the trust anchors (DER), and the
    /// node's certificate and key (DER; the key PKCS#8). The gate binds the
    /// writer lease to this certificate, so the node's lease ops run under
    /// the same one.
    pub loam_authority: &'a [u8],
    pub capability: &'a [u8],
    pub verify_hostname: &'a [u8],
    pub trust_der: &'a [u8],
    pub cert_der: &'a [u8],
    pub key_der: &'a [u8],
    /// Accept certificates without a trusted clock.
    pub clock_unchecked: bool,
    /// The volume: its namespace root and path, the attach flow's lease
    /// holder (32 hex digits, empty for none) and fence (0 for none), and
    /// the lease's time to live.
    pub root: &'a [u8],
    pub path: &'a [u8],
    pub holder_hex: &'a [u8],
    pub fence: u64,
    pub lease_ttl_ms: u32,
    /// Lay a new container on a volume that holds none.
    pub format: bool,
    /// The published device's block size, crypt_block's unit and journal.
    pub block_size: u32,
    pub unit_size: u32,
    pub journal_units: u32,
    /// The NBD listener's port and export name (empty accepts any).
    pub nbd_port: u16,
    pub export: &'a [u8],
}

/// FNV-1a over a module type name: the hash a module is resolved by.
pub const fn type_hash(name: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < name.len() {
        h ^= name[i] as u32;
        h = h.wrapping_mul(0x0100_0193);
        i += 1;
    }
    h
}

/// A cursor over a fixed buffer that reports overflow instead of panicking.
struct Out<'b> {
    buf: &'b mut [u8],
    n: usize,
    ok: bool,
}

impl<'b> Out<'b> {
    fn new(buf: &'b mut [u8]) -> Self {
        Out {
            buf,
            n: 0,
            ok: true,
        }
    }
    fn put(&mut self, b: &[u8]) {
        if !self.ok || self.n + b.len() > self.buf.len() {
            self.ok = false;
            return;
        }
        self.buf[self.n..self.n + b.len()].copy_from_slice(b);
        self.n += b.len();
    }
    fn u8(&mut self, v: u8) {
        self.put(&[v]);
    }
    fn be16(&mut self, v: u16) {
        self.put(&v.to_be_bytes());
    }
    fn be32(&mut self, v: u32) {
        self.put(&v.to_be_bytes());
    }
}

/// Module parameters in the SDK TLV layout: `[0xFE 0x01][payload_len LE]`,
/// `[tag][len][value]` entries, `[0xFF 0x00]`, then extended entries
/// `[tag][0x00][len BE]` that only a module scanning past the end reads.
struct Params<'b, 'c> {
    o: &'c mut Out<'b>,
    start: usize,
}

impl<'b, 'c> Params<'b, 'c> {
    fn begin(o: &'c mut Out<'b>) -> Self {
        let start = o.n;
        o.put(&[0xFE, 0x01, 0, 0]);
        Params { o, start }
    }
    fn entry(&mut self, tag: u8, v: &[u8]) {
        if v.len() > 255 {
            self.o.ok = false;
            return;
        }
        self.o.u8(tag);
        self.o.u8(v.len() as u8);
        self.o.put(v);
    }
    fn u8(&mut self, tag: u8, v: u8) {
        self.entry(tag, &[v]);
    }
    fn u16(&mut self, tag: u8, v: u16) {
        self.entry(tag, &v.to_le_bytes());
    }
    fn u32(&mut self, tag: u8, v: u32) {
        self.entry(tag, &v.to_le_bytes());
    }
    /// Close the basic entries; `payload_len` runs from byte 4 through the
    /// end marker.
    fn end(&mut self) {
        self.o.put(&[0xFF, 0x00]);
        if !self.o.ok {
            return;
        }
        let len = self.o.n - self.start - 4;
        if len > u16::MAX as usize {
            self.o.ok = false;
            return;
        }
        let l = (len as u16).to_le_bytes();
        self.o.buf[self.start + 2] = l[0];
        self.o.buf[self.start + 3] = l[1];
    }
    fn ext(&mut self, tag: u8, v: &[u8]) {
        if v.is_empty() {
            return;
        }
        if v.len() > u16::MAX as usize {
            self.o.ok = false;
            return;
        }
        self.o.u8(tag);
        self.o.u8(0);
        self.o.put(&(v.len() as u16).to_be_bytes());
        self.o.put(v);
    }
}

/// Decimal digits of `v`.
fn decimal(v: u64, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// One module: `[name_hash BE][domain][params_len BE][params]`. The params
/// length is patched once they are written.
fn module(o: &mut Out<'_>, name: &[u8], params: impl FnOnce(&mut Out<'_>)) {
    o.be32(type_hash(name));
    o.u8(0);
    let len_at = o.n;
    o.be16(0);
    let start = o.n;
    params(o);
    if !o.ok {
        return;
    }
    let len = o.n - start;
    if len > u16::MAX as usize {
        o.ok = false;
        return;
    }
    o.buf[len_at..len_at + 2].copy_from_slice(&(len as u16).to_be_bytes());
}

/// One edge: `[kind][idx BE][port]` from, the same to, `[buffer_bytes BE]`.
/// `bytes` is the channel the ports' manifests declare, as a graph's build
/// sizes it; 0 leaves the kernel's default. A record larger than its channel
/// can never be written: the admin wire's frames need the room.
fn edge(o: &mut Out<'_>, from: (u8, u16, u8), to: (u8, u16, u8), bytes: u32) {
    o.u8(from.0);
    o.be16(from.1);
    o.u8(from.2);
    o.u8(to.0);
    o.be16(to.1);
    o.u8(to.2);
    o.be32(bytes);
}

/// `loam_volume`'s admin ports, which `tls`'s clear side does not size.
const ADMIN_BYTES: u32 = 131072;
/// `crypt_block`'s control input: a bundle and the records after it.
const CONTROL_BYTES: u32 = 1024;

fn new(m: u16, port: u8) -> (u8, u16, u8) {
    (EP_NEW, m, port)
}

/// The edges, in blob order.
const EDGES: usize = 11;

/// Encode `g` as an FLXA blob into `out`; its length, or `None` when it does
/// not fit or a value is out of range (a parameter over 255 bytes, a
/// credential over 64 KiB).
pub fn encode(g: &VolumeGraph<'_>, out: &mut [u8]) -> Option<usize> {
    let mut o = Out::new(out);
    o.be32(FLXA_MAGIC);
    o.be16(FLXA_VERSION);
    o.be16(0);
    o.put(&g.owner_uid);
    o.be32(g.state_cap);
    o.be32(0);
    o.u8(4);
    o.u8(EDGES as u8);

    module(&mut o, b"tls", |o| {
        let mut p = Params::begin(o);
        p.u8(1, 0); // mode: client
        p.u8(11, 2); // peer_auth: ca_dns
        p.u8(12, u8::from(g.clock_unchecked));
        p.end();
        p.ext(10, g.cert_der);
        p.ext(11, g.key_der);
        p.ext(12, g.trust_der);
        p.ext(13, g.verify_hostname);
    });
    module(&mut o, b"loam_volume", |o| {
        let mut p = Params::begin(o);
        p.entry(1, g.root);
        p.entry(2, g.path);
        p.u32(3, g.lease_ttl_ms);
        p.u32(4, g.block_size);
        if !g.holder_hex.is_empty() {
            p.entry(5, g.holder_hex);
        }
        if g.fence != 0 {
            let mut d = [0u8; 20];
            let n = decimal(g.fence, &mut d);
            p.entry(6, &d[..n]);
        }
        p.entry(7, g.capability);
        p.entry(8, g.loam_authority);
        p.end();
    });
    module(&mut o, b"crypt_block", |o| {
        let mut p = Params::begin(o);
        p.u8(2, u8::from(g.format));
        p.u32(3, g.block_size);
        p.u32(4, g.unit_size);
        p.u32(5, g.journal_units);
        p.u8(6, 1); // attach: the master comes from a bundle
        p.end();
    });
    module(&mut o, b"nbd_serve", |o| {
        let mut p = Params::begin(o);
        p.u16(1, g.nbd_port);
        if !g.export.is_empty() {
            p.entry(2, g.export);
        }
        p.end();
    });

    // tls: cipher_in in0, clear_in in1; cipher_out out0, clear_out out1.
    edge(&mut o, new(M_TLS, 0), (EP_NET, NET_LOAM, 0), 0);
    edge(&mut o, (EP_NET, NET_LOAM, 0), new(M_TLS, 0), 0);
    // loam_volume: admin_in in0; blocks out0, admin_out out1 — net_proto
    // to and from tls's clear side.
    edge(&mut o, new(M_TLS, 1), new(M_VOLUME, 0), ADMIN_BYTES);
    edge(&mut o, new(M_VOLUME, 1), new(M_TLS, 1), ADMIN_BYTES);
    // crypt_block: lower in0, bundle ctrl0; blocks out0, recipient out1,
    // status out2.
    edge(&mut o, new(M_VOLUME, 0), new(M_CRYPT, 0), 0);
    edge(
        &mut o,
        new(M_CRYPT, 1),
        (EP_CALLER, u16::from(CALLER_RECIPIENT), 0),
        0,
    );
    edge(
        &mut o,
        new(M_CRYPT, 2),
        (EP_CALLER, u16::from(CALLER_STATUS), 0),
        0,
    );
    edge(
        &mut o,
        (EP_CALLER, u16::from(CALLER_BUNDLE), 0),
        new(M_CRYPT, CTRL),
        CONTROL_BYTES,
    );
    // nbd_serve: blocks in0, net_in in1; net_out out0.
    edge(&mut o, new(M_CRYPT, 0), new(M_NBD, 0), 0);
    edge(&mut o, new(M_NBD, 0), (EP_NET, NET_NBD, 0), 0);
    edge(&mut o, (EP_NET, NET_NBD, 0), new(M_NBD, 1), 0);

    if o.ok {
        Some(o.n)
    } else {
        None
    }
}
