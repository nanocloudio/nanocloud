// One Loam volume operation as a sequence of admin-wire exchanges, without
// I/O: the caller sends each request `request` builds and hands each whole
// ack to `on_ack` (loam_admin_wire's `response_len` delimits them), one
// exchange in flight. It is Loam's own wire (`admin`, the mounted
// loam_admin_wire; `wire`, loam_wire) and map-page codec (`map`), so nothing
// here restates a Loam format.
//
// | Job      | Exchanges                                                   |
// |----------|-------------------------------------------------------------|
// | Acquire  | lease acquire                                               |
// | Renew    | lease renew                                                 |
// | Release  | lease release                                               |
// | Create   | lease, begin, the zero root page, commit at revision 0, release |
// | Delete   | lookup, lease, delete at the bound revision, release        |
// | Bind     | lookup the target (already bound to the source: done), lookup the source, lease, begin, commit the source's root, release |
//
// `Bind` is a snapshot (a volume's root pinned at a snapshot path) and a
// restore (a snapshot's root bound at a new volume path): no bytes move.
// Every job is safe to run again after an answer was lost: a create finds
// its path bound, a delete finds it gone, a bind finds its target already
// bound to the same root.
//
// Expiries are on the caller's clock: a lease answered OK lives at least
// `ttl` from when the request was sent, whatever the server's clock says.

/// How a job ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Code {
    Ok,
    /// Another holder has the lease.
    Held,
    /// The lease was lost (expired or taken).
    Lost,
    /// The server refused this identity the operation (no grant).
    Refused,
    /// The path is bound to something else, or the source is missing.
    Failed,
    /// The server is busy or answered something this job cannot read; worth
    /// running again.
    Unavailable,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Job {
    Acquire,
    Renew,
    Release,
    Create,
    Delete,
    Bind,
}

/// Where a job stands after an ack.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Progress {
    /// Send the next request.
    Next,
    Done(Code),
}

const S_LOOKUP_DST: u8 = 1;
const S_LOOKUP_SRC: u8 = 2;
const S_LEASE: u8 = 3;
const S_BEGIN: u8 = 4;
const S_PAGE: u8 = 5;
const S_COMMIT: u8 = 6;
const S_DELETE: u8 = 7;
const S_RELEASE: u8 = 8;
const S_DONE: u8 = 9;

/// Longest root or path a job carries.
pub const KEY_CAP: usize = 128;
/// `sha256:` and 64 hex digits.
const OID_LEN: usize = 71;
/// Times a busy begin is asked again before the job gives up.
const BEGIN_TRIES: u8 = 8;

pub struct LoamOp {
    pub job: Job,
    root: [u8; KEY_CAP],
    root_len: u8,
    path: [u8; KEY_CAP],
    path_len: u8,
    /// `Bind`: the root and path whose committed root is bound.
    src_root: [u8; KEY_CAP],
    src_root_len: u8,
    src_path: [u8; KEY_CAP],
    src_path_len: u8,
    pub holder: [u8; 16],
    pub ttl_ms: u32,
    /// The fence the lease answered (Acquire), or the one renewed under.
    pub fence: u64,
    /// When the lease answered lives until, on the caller's clock.
    pub lease_exp: i64,
    /// `Create`: the volume's geometry and identity.
    pub size: u64,
    pub extent: u32,
    pub volume_id: [u8; 16],
    /// The revision committed (Create, Bind) or deleted (Delete).
    pub revision: u64,
    /// `Bind`: the root bound; `Create`: the new root.
    pub digest: [u8; 32],
    step: u8,
    cid: u32,
    sent_at: i64,
    tries: u8,
    /// The code a job that failed after taking its lease reports once the
    /// lease is released.
    pending: Code,
}

impl LoamOp {
    pub const fn empty() -> Self {
        LoamOp {
            job: Job::Acquire,
            root: [0; KEY_CAP],
            root_len: 0,
            path: [0; KEY_CAP],
            path_len: 0,
            src_root: [0; KEY_CAP],
            src_root_len: 0,
            src_path: [0; KEY_CAP],
            src_path_len: 0,
            holder: [0; 16],
            ttl_ms: 0,
            fence: 0,
            lease_exp: 0,
            size: 0,
            extent: 0,
            volume_id: [0; 16],
            revision: 0,
            digest: [0; 32],
            step: S_DONE,
            cid: 0,
            sent_at: 0,
            tries: 0,
            pending: Code::Ok,
        }
    }

    /// Begin `job` on `(root, path)`. False when a key is too long.
    pub fn start(&mut self, job: Job, root: &[u8], path: &[u8]) -> bool {
        if root.len() > KEY_CAP || path.len() > KEY_CAP {
            return false;
        }
        self.job = job;
        self.root[..root.len()].copy_from_slice(root);
        self.root_len = root.len() as u8;
        self.path[..path.len()].copy_from_slice(path);
        self.path_len = path.len() as u8;
        self.revision = 0;
        self.tries = 0;
        self.pending = Code::Ok;
        self.step = match job {
            Job::Acquire | Job::Renew | Job::Release => S_LEASE,
            Job::Create => S_LOOKUP_DST,
            Job::Delete => S_LOOKUP_DST,
            Job::Bind => S_LOOKUP_DST,
        };
        if job == Job::Release {
            self.step = S_RELEASE;
        }
        true
    }

    /// `Bind`: the volume whose committed root is bound at the job's path.
    pub fn set_source(&mut self, root: &[u8], path: &[u8]) -> bool {
        if root.len() > KEY_CAP || path.len() > KEY_CAP {
            return false;
        }
        self.src_root[..root.len()].copy_from_slice(root);
        self.src_root_len = root.len() as u8;
        self.src_path[..path.len()].copy_from_slice(path);
        self.src_path_len = path.len() as u8;
        true
    }

    pub fn finished(&self) -> bool {
        self.step == S_DONE
    }

    fn root(&self) -> &[u8] {
        &self.root[..self.root_len as usize]
    }
    fn path(&self) -> &[u8] {
        &self.path[..self.path_len as usize]
    }
    fn src_root(&self) -> &[u8] {
        &self.src_root[..self.src_root_len as usize]
    }
    fn src_path(&self) -> &[u8] {
        &self.src_path[..self.src_path_len as usize]
    }

    /// The next request into `out`, its correlation id `cid`; its length, or
    /// `None` when it does not fit. `zeros` is zeroed scratch the size of a
    /// map page's children, for a new volume's root.
    pub fn request(
        &mut self,
        out: &mut [u8],
        cid: u32,
        now: i64,
        zeros: &[[u8; 32]],
    ) -> Option<usize> {
        self.cid = cid;
        self.sent_at = now;
        let n = match self.step {
            S_LOOKUP_DST => admin::encode_admin_lookup(out, cid, self.root(), self.path()).ok()?,
            S_LOOKUP_SRC => {
                admin::encode_admin_lookup(out, cid, self.src_root(), self.src_path()).ok()?
            }
            S_LEASE => {
                let mode = match self.job {
                    Job::Renew => admin::LEASE_RENEW,
                    _ => admin::LEASE_ACQUIRE,
                };
                admin::encode_admin_lease(
                    out,
                    cid,
                    mode,
                    self.root(),
                    self.path(),
                    &self.holder,
                    self.ttl_ms,
                )
                .ok()?
            }
            S_RELEASE => admin::encode_admin_lease(
                out,
                cid,
                admin::LEASE_RELEASE,
                self.root(),
                self.path(),
                &self.holder,
                0,
            )
            .ok()?,
            S_BEGIN | S_COMMIT | S_DELETE => {
                let mut oid = [0u8; OID_LEN];
                let (mode, expected, oid_len) = match self.step {
                    S_BEGIN => (admin::VOLUME_BEGIN, 0, 0),
                    S_DELETE => (admin::VOLUME_DELETE, self.revision, 0),
                    _ => (admin::VOLUME_COMMIT, 0, object_id(&self.digest, &mut oid)),
                };
                let r = admin::DecodedAdminVolume {
                    correlation_id: cid,
                    mode,
                    namespace_root: self.root(),
                    path: self.path(),
                    object_id: &oid[..oid_len],
                    holder: self.holder,
                    fence: self.fence,
                    expected,
                };
                admin::encode_admin_volume(out, &r).ok()?
            }
            S_PAGE => {
                let depth = map::select_depth(self.size, self.extent).ok()?;
                let extents = self.size.div_ceil(u64::from(self.extent));
                let children = if depth == 1 {
                    extents
                } else {
                    extents.div_ceil(map::PAGE_ENTRIES as u64)
                } as usize;
                let at =
                    admin::encode_admin_put_body_header(out, cid, map::ROOT_HDR + children * 32)
                        .ok()?;
                let page = &mut out[at..];
                let n = map::encode_root(
                    page,
                    &self.volume_id,
                    self.size,
                    self.extent,
                    zeros.get(..children)?,
                )
                .ok()?;
                at + n
            }
            _ => return None,
        };
        Some(n)
    }

    /// Take the ack to the request in flight.
    pub fn on_ack(&mut self, ack: &[u8]) -> Progress {
        let r = match self.step {
            S_LOOKUP_DST | S_LOOKUP_SRC => self.on_lookup(ack),
            S_LEASE => self.on_lease(ack),
            S_RELEASE => self.on_release(ack),
            S_BEGIN | S_COMMIT | S_DELETE => self.on_volume(ack),
            S_PAGE => self.on_page(ack),
            _ => Err(Code::Unavailable),
        };
        match r {
            Ok(()) if self.step == S_DONE => Progress::Done(self.pending),
            Ok(()) => Progress::Next,
            Err(code) => self.fail(code),
        }
    }

    /// A failure: once a job holds its lease it releases it first, and then
    /// reports.
    fn fail(&mut self, code: Code) -> Progress {
        let holds = matches!(self.job, Job::Create | Job::Delete | Job::Bind)
            && matches!(self.step, S_BEGIN | S_PAGE | S_COMMIT | S_DELETE);
        if holds {
            self.pending = code;
            self.step = S_RELEASE;
            return Progress::Next;
        }
        self.step = S_DONE;
        Progress::Done(code)
    }

    fn on_lookup(&mut self, ack: &[u8]) -> Result<(), Code> {
        let (cid, status, binding) =
            admin::decode_admin_lookup_ack(ack).map_err(|_| Code::Unavailable)?;
        if cid != self.cid {
            return Err(Code::Unavailable);
        }
        let bound = match (status, binding) {
            (admin::STATUS_OK, Some(b)) => Some((b.kind, b.revision, digest_of(b.object_id))),
            (admin::STATUS_OK | admin::STATUS_NOT_FOUND, None) | (admin::STATUS_NOT_FOUND, _) => {
                None
            }
            (admin::STATUS_FORBIDDEN, _) => return Err(Code::Refused),
            _ => return Err(Code::Unavailable),
        };
        let is_volume = |kind: u8| kind == wire::KIND_VOLUME;
        if self.step == S_LOOKUP_SRC {
            match bound {
                Some((k, _, Some(d))) if is_volume(k) => {
                    self.digest = d;
                    self.step = S_LEASE;
                    Ok(())
                }
                _ => Err(Code::Failed),
            }
        } else {
            match bound {
                // A path bound to anything but a volume is not this job's.
                Some((k, _, _)) if !is_volume(k) => Err(Code::Failed),
                // Create: a volume already there is this create, answered
                // before. Bind: a bind only ever writes the source's root, so
                // a bound target is this bind's.
                Some((_, rev, Some(d))) if matches!(self.job, Job::Create | Job::Bind) => {
                    self.revision = rev;
                    self.digest = d;
                    self.step = S_DONE;
                    Ok(())
                }
                Some((_, rev, _)) if self.job == Job::Delete => {
                    self.revision = rev;
                    self.step = S_LEASE;
                    Ok(())
                }
                None => {
                    self.step = match self.job {
                        Job::Create => S_LEASE,
                        Job::Bind => S_LOOKUP_SRC,
                        _ => S_DONE,
                    };
                    Ok(())
                }
                _ => Err(Code::Failed),
            }
        }
    }

    fn on_lease(&mut self, ack: &[u8]) -> Result<(), Code> {
        let (cid, status, fence, _server_exp) =
            admin::decode_admin_lease_ack(ack).map_err(|_| Code::Unavailable)?;
        if cid != self.cid {
            return Err(Code::Unavailable);
        }
        match status {
            admin::STATUS_OK => {
                if self.job != Job::Renew || fence != 0 {
                    self.fence = fence;
                }
                self.lease_exp = self.sent_at + i64::from(self.ttl_ms);
                self.step = match self.job {
                    Job::Acquire | Job::Renew => S_DONE,
                    Job::Delete => S_DELETE,
                    _ => S_BEGIN,
                };
                Ok(())
            }
            admin::STATUS_LEASE_HELD => Err(Code::Held),
            admin::STATUS_LEASE_LOST | admin::STATUS_NOT_FOUND => Err(Code::Lost),
            admin::STATUS_FORBIDDEN => Err(Code::Refused),
            _ => Err(Code::Unavailable),
        }
    }

    fn on_release(&mut self, ack: &[u8]) -> Result<(), Code> {
        let (cid, status, _, _) =
            admin::decode_admin_lease_ack(ack).map_err(|_| Code::Unavailable)?;
        if cid != self.cid {
            return Err(Code::Unavailable);
        }
        self.step = S_DONE;
        // A compound job's own release: its outcome is the job's.
        if self.job != Job::Release {
            return Ok(());
        }
        match status {
            admin::STATUS_OK => Ok(()),
            admin::STATUS_LEASE_LOST | admin::STATUS_NOT_FOUND => {
                self.pending = Code::Lost;
                Ok(())
            }
            admin::STATUS_FORBIDDEN => {
                self.pending = Code::Refused;
                Ok(())
            }
            _ => {
                self.pending = Code::Unavailable;
                Ok(())
            }
        }
    }

    fn on_volume(&mut self, ack: &[u8]) -> Result<(), Code> {
        let (cid, status, revision) =
            admin::decode_admin_volume_ack(ack).map_err(|_| Code::Unavailable)?;
        if cid != self.cid {
            return Err(Code::Unavailable);
        }
        match (self.step, status) {
            (S_BEGIN, admin::STATUS_OK) => {
                self.step = if self.job == Job::Create {
                    S_PAGE
                } else {
                    S_COMMIT
                };
                Ok(())
            }
            (S_BEGIN, admin::STATUS_BUSY) if self.tries < BEGIN_TRIES => {
                self.tries += 1;
                Ok(())
            }
            (S_COMMIT | S_DELETE, admin::STATUS_OK) => {
                if self.step == S_COMMIT {
                    self.revision = revision;
                }
                self.step = S_RELEASE;
                Ok(())
            }
            (_, admin::STATUS_LEASE_HELD) => Err(Code::Held),
            (_, admin::STATUS_LEASE_LOST) => Err(Code::Lost),
            (_, admin::STATUS_FORBIDDEN) => Err(Code::Refused),
            (_, admin::STATUS_CONFLICT) => Err(Code::Failed),
            _ => Err(Code::Unavailable),
        }
    }

    fn on_page(&mut self, ack: &[u8]) -> Result<(), Code> {
        let a = admin::decode_admin_put_body_ack(ack).map_err(|_| Code::Unavailable)?;
        if a.correlation_id != self.cid {
            return Err(Code::Unavailable);
        }
        match (a.status, a.digest) {
            (admin::STATUS_OK, Some(d)) if d.len() == 32 => {
                self.digest.copy_from_slice(d);
                self.step = S_COMMIT;
                Ok(())
            }
            (admin::STATUS_FORBIDDEN, _) => Err(Code::Refused),
            (admin::STATUS_QUOTA, _) => Err(Code::Failed),
            _ => Err(Code::Unavailable),
        }
    }
}

/// `sha256:` and the lowercase hex of `digest` into `out`; its length.
fn object_id(digest: &[u8; 32], out: &mut [u8; OID_LEN]) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out[..7].copy_from_slice(b"sha256:");
    for (i, b) in digest.iter().enumerate() {
        out[7 + 2 * i] = HEX[(b >> 4) as usize];
        out[8 + 2 * i] = HEX[(b & 0x0f) as usize];
    }
    OID_LEN
}

/// The digest an object id names, if it is a `sha256:` content id.
fn digest_of(oid: &[u8]) -> Option<[u8; 32]> {
    let hex = oid.strip_prefix(b"sha256:")?;
    if hex.len() != 64 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = (nib(hex[2 * i])? << 4) | nib(hex[2 * i + 1])?;
    }
    Some(out)
}
