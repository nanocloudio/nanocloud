// A node's storage-key requests, without I/O: a challenge, then the attach,
// recovery or renewal it admits, each proven by the node's enrolled device
// key. It is Kagi's own wire, mounted by the includer as `sk`
// (storage_key) and `svc` (storage_key_service), so nothing here restates a
// Kagi format.
//
// | Job | Kagi | What the node holds after |
// |---|---|---|
// | Attach (KAGI_RELEASE) | 1 challenge, 5 attach | the release (the renewal chain's head) and the `FXSB` bundle |
// | Recover (KAGI_RECOVER) | 1 challenge, 12 recover, naming the ticket | the release and the bundle |
// | Renew (KAGI_RENEW) | 1 challenge, 13 renew, naming the attachment and the record renewed | the renewal (the chain's new head) |
//
// A request is the storage-key REQUEST payload, `[op][resource kind]
// [resource][body]`, as `POST /storage-key` carries it; its answer is the
// REPLY payload. Each is one exchange, and its exchange id is the
// correlation: the includer matches an answer to the request it sent. The
// recipient is `crypt_block`'s `FXRK` announcement: the proof binds it, the
// bundle is sealed to it, and only that instance's vault opens the bundle. A renewal carries the same recipient, fence and epoch as
// the attachment it extends.

/// How a job ended, as the pipelines name it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AttachCode {
    Ok,
    Refused,
    /// A recovery that fewer than two custodians answered.
    Custodians,
    /// A renewal under a lease fence the attachment no longer holds.
    FenceChanged,
    /// A renewal of an attachment already past its expiry.
    Expired,
    /// A renewal of a record that is no longer the chain's head.
    Conflict,
    Failed,
    Unavailable,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AttachJob {
    Attach,
    Recover,
    Renew,
}

/// What the caller does next.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AttachNext {
    /// Send `out[..n]`, a REQUEST payload, and hand its reply payload to
    /// `on_reply`.
    Send(usize),
    Done(AttachCode),
}

/// The node's enrolled device key.
pub trait DeviceKey {
    /// Sign `input` as the key's credential suite signs; the signature's
    /// length.
    fn sign(&mut self, input: &[u8], signature: &mut [u8]) -> Option<usize>;
}

/// How long a request is good for: well inside Kagi's window.
const REQUEST_WINDOW_MS: u64 = 60_000;
const MAX_SIGNATURE: usize = 128;

const A_IDLE: u8 = 0;
const A_CHALLENGE: u8 = 1;
const A_REQUEST: u8 = 2;

/// One attach's facts: what the proof binds.
pub struct AttachFacts<'a> {
    pub resource: [u8; 16],
    pub epoch: u32,
    pub fence: u64,
    pub lifetime_secs: u32,
    /// `crypt_block`'s `"FXRK" ‖ public` announcement.
    pub recipient: &'a [u8],
    /// A recovery's ticket; zero otherwise.
    pub ticket: [u8; 16],
    /// A renewal's record renewed: the chain's head.
    pub renewed: &'a [u8],
}

pub struct KagiAttach {
    issuer: [u8; sk::MAX_ID],
    issuer_len: u8,
    device: [u8; sk::MAX_ID],
    device_len: u8,
    device_suite: u16,
    device_public: [u8; 65],
    device_public_len: u8,

    job: AttachJob,
    stage: u8,
    resource: [u8; 16],
    epoch: u32,
    fence: u64,
    lifetime: u32,
    recipient: [u8; sk::RECIPIENT_LEN],
    ticket: [u8; 16],
    renewed: [u8; sk::MAX_RECORD],
    renewed_len: u16,
    /// Kagi's refusal code when it refused, else 0.
    pub refusal: u8,
    /// The release or renewal Kagi answered: the chain's head.
    record: [u8; sk::MAX_RECORD],
    record_len: u16,
    /// An attach's or recovery's bundle, for `crypt_block`.
    bundle: [u8; sk::BUNDLE_LEN],
    bundle_len: u16,
    /// The attachment's expiry, Unix ms.
    pub expiry_ms: u64,
}

impl KagiAttach {
    pub const fn empty() -> Self {
        KagiAttach {
            issuer: [0; sk::MAX_ID],
            issuer_len: 0,
            device: [0; sk::MAX_ID],
            device_len: 0,
            device_suite: 0,
            device_public: [0; 65],
            device_public_len: 0,
            job: AttachJob::Attach,
            stage: A_IDLE,
            resource: [0; 16],
            epoch: 0,
            fence: 0,
            lifetime: 0,
            recipient: [0; sk::RECIPIENT_LEN],
            ticket: [0; 16],
            renewed: [0; sk::MAX_RECORD],
            renewed_len: 0,
            refusal: 0,
            record: [0; sk::MAX_RECORD],
            record_len: 0,
            bundle: [0; sk::BUNDLE_LEN],
            bundle_len: 0,
            expiry_ms: 0,
        }
    }

    /// The issuer the proofs are addressed to, and this node's device: its
    /// Kagi device id, its key's credential suite and public half. False
    /// when any is unusable.
    pub fn configure(&mut self, issuer: &[u8], device: &[u8], suite: u16, public: &[u8]) -> bool {
        if issuer.is_empty()
            || issuer.len() > sk::MAX_ID
            || device.is_empty()
            || device.len() > sk::MAX_ID
            || public.is_empty()
            || public.len() > self.device_public.len()
        {
            return false;
        }
        self.issuer[..issuer.len()].copy_from_slice(issuer);
        self.issuer_len = issuer.len() as u8;
        self.device[..device.len()].copy_from_slice(device);
        self.device_len = device.len() as u8;
        self.device_suite = suite;
        self.device_public[..public.len()].copy_from_slice(public);
        self.device_public_len = public.len() as u8;
        true
    }

    pub fn device(&self) -> &[u8] {
        &self.device[..self.device_len as usize]
    }

    /// The release or renewal last answered.
    pub fn record(&self) -> &[u8] {
        &self.record[..self.record_len as usize]
    }

    /// The bundle an attach or recovery was answered with.
    pub fn bundle(&self) -> &[u8] {
        &self.bundle[..self.bundle_len as usize]
    }

    /// Start `job` for `f`: the challenge request goes first.
    pub fn begin(&mut self, job: AttachJob, f: &AttachFacts<'_>, out: &mut [u8]) -> AttachNext {
        self.job = job;
        self.stage = A_IDLE;
        self.refusal = 0;
        self.record_len = 0;
        self.bundle_len = 0;
        self.expiry_ms = 0;
        if self.device_len == 0 || f.recipient.len() != sk::RECIPIENT_LEN {
            return AttachNext::Done(AttachCode::Failed);
        }
        if sk::recipient_public(f.recipient).is_none() {
            return AttachNext::Done(AttachCode::Failed);
        }
        if job == AttachJob::Renew && (f.renewed.is_empty() || f.renewed.len() > sk::MAX_RECORD) {
            return AttachNext::Done(AttachCode::Failed);
        }
        self.resource = f.resource;
        self.epoch = f.epoch;
        self.fence = f.fence;
        self.lifetime = f.lifetime_secs;
        self.recipient.copy_from_slice(f.recipient);
        self.ticket = f.ticket;
        self.renewed[..f.renewed.len()].copy_from_slice(f.renewed);
        self.renewed_len = f.renewed.len() as u16;
        if job == AttachJob::Renew {
            // The renewal names the attachment it extends.
            let Ok(rec) = sk::Record::parse(f.renewed) else {
                return AttachNext::Done(AttachCode::Failed);
            };
            match sk::attachment_id(&rec) {
                Some(id) => self.ticket = id,
                None => return AttachNext::Done(AttachCode::Failed),
            }
        }
        let mut body = [0u8; 1 + 16 + 1 + sk::MAX_ID];
        let mut w = sk::Writer::new(&mut body);
        let ok = w.u8(sk::resource::VOLUME).is_some()
            && w.bytes(&self.resource).is_some()
            && w.f8(&self.device[..self.device_len as usize]).is_some();
        if !ok {
            return AttachNext::Done(AttachCode::Failed);
        }
        let n = w.len();
        self.stage = A_CHALLENGE;
        request(sk::op::CHALLENGE, &body[..n], out)
    }

    /// The reply payload to this job's request; `out` takes the next
    /// request. `now_ms` is wall-clock Unix ms; `anti_replay` is 16 fresh
    /// random bytes.
    pub fn on_reply<K: DeviceKey>(
        &mut self,
        payload: &[u8],
        key: &mut K,
        now_ms: u64,
        anti_replay: [u8; 16],
        out: &mut [u8],
    ) -> AttachNext {
        let Some(r) = svc::read_reply(payload) else {
            return AttachNext::Done(AttachCode::Unavailable);
        };
        if self.stage == A_IDLE {
            return AttachNext::Done(AttachCode::Failed);
        }
        if r.status != svc::STATUS_OK {
            return AttachNext::Done(self.refused(r.status, r.refusal));
        }
        if self.stage == A_CHALLENGE {
            let Some(challenge) = r.extra.get(..32) else {
                return AttachNext::Done(AttachCode::Failed);
            };
            let mut c = [0u8; 32];
            c.copy_from_slice(challenge);
            return self.ask(key, c, now_ms, anti_replay, out);
        }
        AttachNext::Done(self.answered(&r))
    }

    fn refused(&mut self, status: u8, refusal: u8) -> AttachCode {
        if status != svc::STATUS_REFUSED {
            return AttachCode::Failed;
        }
        self.refusal = refusal;
        let is = |x: sk::Refusal| refusal == x.code();
        if is(sk::Refusal::LedgerUnavailable) || is(sk::Refusal::Busy) {
            AttachCode::Unavailable
        } else if self.job == AttachJob::Recover && is(sk::Refusal::CustodianRefused) {
            AttachCode::Custodians
        } else if self.job == AttachJob::Renew && is(sk::Refusal::FenceChanged) {
            AttachCode::FenceChanged
        } else if self.job == AttachJob::Renew && is(sk::Refusal::AttachmentExpired) {
            AttachCode::Expired
        } else if self.job == AttachJob::Renew && is(sk::Refusal::Conflict) {
            AttachCode::Conflict
        } else {
            AttachCode::Refused
        }
    }

    /// The attach, recovery or renewal itself, proven over the challenge.
    fn ask<K: DeviceKey>(
        &mut self,
        key: &mut K,
        challenge: [u8; 32],
        now_ms: u64,
        anti_replay: [u8; 16],
        out: &mut [u8],
    ) -> AttachNext {
        if now_ms == 0 {
            // No trusted clock: a request with no honest expiry is not one.
            return AttachNext::Done(AttachCode::Unavailable);
        }
        let (kind, op) = match self.job {
            AttachJob::Attach => (sk::kind::ATTACHMENT, sk::op::ATTACH),
            AttachJob::Recover => (sk::kind::RECOVERY, sk::op::RECOVER),
            AttachJob::Renew => (sk::kind::RENEWAL, sk::op::RENEW_ATTACHMENT),
        };
        let mut req = sk::AttachRequest {
            kind,
            resource_kind: sk::resource::VOLUME,
            resource: self.resource,
            epoch: self.epoch,
            fence: self.fence,
            lifetime_secs: self.lifetime,
            expiry_ms: now_ms.saturating_add(REQUEST_WINDOW_MS),
            anti_replay,
            challenge,
            ticket: self.ticket,
            recipient: &self.recipient,
            device: &self.device[..self.device_len as usize],
            device_suite: self.device_suite,
            device_public: &self.device_public[..self.device_public_len as usize],
            proof: &[],
            evidence: &[],
        };
        let mut input = [0u8; sk::MAX_PROOF_INPUT];
        let Some(n) =
            sk::write_proof_input(&req, &self.issuer[..self.issuer_len as usize], &mut input)
        else {
            return AttachNext::Done(AttachCode::Failed);
        };
        let mut proof = [0u8; MAX_SIGNATURE];
        let Some(pn) = key.sign(&input[..n], &mut proof) else {
            return AttachNext::Done(AttachCode::Failed);
        };
        req.proof = &proof[..pn];
        let mut body = [0u8; svc::MAX_REQUEST];
        let Some(mut bn) = svc::write_attach_body(&req, &mut body) else {
            return AttachNext::Done(AttachCode::Failed);
        };
        if self.job == AttachJob::Renew {
            let renewed = &self.renewed[..self.renewed_len as usize];
            let mut w = sk::Writer::new(&mut body[bn..]);
            if w.f16(renewed).is_none() {
                return AttachNext::Done(AttachCode::Failed);
            }
            bn += w.len();
        }
        self.stage = A_REQUEST;
        // The head (resource kind and id) is the body's own first bytes.
        request(op, &body[..bn], out)
    }

    fn answered(&mut self, r: &svc::Reply<'_>) -> AttachCode {
        let Ok(rec) = sk::Record::parse(r.record) else {
            return AttachCode::Failed;
        };
        let b = rec.binding;
        let kind = match self.job {
            AttachJob::Attach => sk::kind::ATTACHMENT,
            AttachJob::Recover => sk::kind::RECOVERY,
            AttachJob::Renew => sk::kind::RENEWAL,
        };
        if b.kind != kind || b.resource != self.resource || r.record.len() > sk::MAX_RECORD {
            return AttachCode::Failed;
        }
        if self.job != AttachJob::Renew {
            if r.bundle.len() != sk::BUNDLE_LEN {
                return AttachCode::Failed;
            }
            self.bundle.copy_from_slice(r.bundle);
            self.bundle_len = sk::BUNDLE_LEN as u16;
            // The handle's life runs from the release, a recovery's as an
            // attachment's.
            self.expiry_ms = b
                .issued_ms
                .saturating_add(u64::from(b.lifetime_secs).saturating_mul(1000));
        } else {
            self.expiry_ms = b.expiry_ms;
        }
        self.record[..r.record.len()].copy_from_slice(r.record);
        self.record_len = r.record.len() as u16;
        AttachCode::Ok
    }
}

/// A REQUEST payload: the envelope `svc::write_request` lays, without its
/// three-byte header — `POST /storage-key` carries the payload alone.
fn request(op: u8, body: &[u8], out: &mut [u8]) -> AttachNext {
    let mut frame = [0u8; 3 + 1 + svc::MAX_REQUEST];
    match svc::write_request(op, body, &mut frame) {
        Some(n) if n >= 3 && n - 3 <= out.len() => {
            out[..n - 3].copy_from_slice(&frame[3..n]);
            AttachNext::Send(n - 3)
        }
        _ => AttachNext::Done(AttachCode::Failed),
    }
}
