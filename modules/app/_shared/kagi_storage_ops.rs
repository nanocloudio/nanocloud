// The control plane's Kagi storage-key effects, without I/O: one pipeline
// effect in, one or two storage-key requests out, the typed outcome from
// their replies. It is Kagi's own wire, mounted by the includer as `sk`
// (storage_key), `svc` (storage_key_service) and `key_vault`, so nothing
// here restates a Kagi format.
//
// | Effect | Kagi | Body after the head | Result |
// |---|---|---|---|
// | 20 KAGI_GRANT | 4 grant | device, policy, actor | grant = the device |
// | 21 KAGI_REPLACE | 7 replace device | old, new, actor | grant = new |
// | 24 KAGI_CREATE | 2 create | tenant, policy, aead, custodians, actor | the creation held |
// | 25 KAGI_RECORD_SET | 3 record set | epoch, the three envelopes, actor | set = its id |
// | 31 VAULT_SPLIT | — | the provisioning vault splits a fresh key under the held creation | — |
// | 32 VAULT_DESTROY | — | the vault's handle destroyed | — |
// | 60 KAGI_ROTATE_GRANT | 8 rotate grant | device, policy, actor | — |
// | 61 KAGI_REVOKE | 6 revoke | actor | — |
// | 65 KAGI_ROTATE_KEY | 9 rotate key | custodians, actor | r1 = the new epoch |
// | 71 KAGI_RETIRE_EPOCH | 10 retire epoch | epoch, actor | — |
// | 78 KAGI_RECOVERY_AUTHORISE | 11 authorise recovery | epoch, device, ttl, actor | r1 = the ticket |
// | 80 KAGI_REGRANT | 7, or 4 over a revoked grant | as those | grant = the device |
// | 81 KAGI_ERASE | 14 erase | epoch, pins, actor | — |
//
// The resource is the first 16 bytes of sha256 of the volume name. The
// volume key is generated and split inside this node's vault and never
// leaves it; what crosses to Kagi is the three custody envelopes, each
// sealed to one custodian. The creation authorisation and the envelopes are
// held here only from the create (or rotation) to the record set.
//
// Each request is one exchange with the storage-key service: its exchange id
// is the correlation, so the engine names none; the includer matches a reply
// to the request it sent.
//
// Kagi refuses a second create, grant, replacement, revocation or rotation
// as its own decision. A pipeline asks again when an answer was late, so a
// refusal that this engine can show is the answer to its own earlier,
// identical decision is answered `ok`: a revoke over a revoked grant, a
// grant or replacement the last grant seen already names, a rotation whose
// creation is held, a retirement already made here. Any other refusal is
// the pipeline's to see.

/// How an effect ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Code {
    Ok,
    /// Kagi decided against it.
    Refused,
    /// The arguments, or a reply, are not something this engine can act on.
    Failed,
    /// Kagi is busy or its ledger is; worth asking again.
    Unavailable,
}

/// What the caller does next.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Next {
    /// Send `out[..n]`, a storage-key REQUEST envelope, as one exchange, and
    /// hand its REPLY to `on_reply`.
    Send(usize),
    Done(Code),
}

pub const OP_KAGI_GRANT: i64 = 20;
pub const OP_KAGI_REPLACE: i64 = 21;
pub const OP_KAGI_CREATE: i64 = 24;
pub const OP_KAGI_RECORD_SET: i64 = 25;
pub const OP_VAULT_SPLIT: i64 = 31;
pub const OP_VAULT_DESTROY: i64 = 32;
pub const OP_KAGI_ROTATE_GRANT: i64 = 60;
pub const OP_KAGI_REVOKE: i64 = 61;
pub const OP_KAGI_ROTATE_KEY: i64 = 65;
pub const OP_KAGI_RETIRE_EPOCH: i64 = 71;
pub const OP_KAGI_RECOVERY_AUTHORISE: i64 = 78;
pub const OP_KAGI_REGRANT: i64 = 80;
pub const OP_KAGI_ERASE: i64 = 81;

/// The effects this engine answers.
pub fn kagi_effect(op: i64) -> bool {
    matches!(
        op,
        OP_KAGI_GRANT
            | OP_KAGI_REPLACE
            | OP_KAGI_CREATE
            | OP_KAGI_RECORD_SET
            | OP_VAULT_SPLIT
            | OP_VAULT_DESTROY
            | OP_KAGI_ROTATE_GRANT
            | OP_KAGI_REVOKE
            | OP_KAGI_ROTATE_KEY
            | OP_KAGI_RETIRE_EPOCH
            | OP_KAGI_RECOVERY_AUTHORISE
            | OP_KAGI_REGRANT
            | OP_KAGI_ERASE
    )
}

/// One effect's arguments: `a1..a5` and `n1`, `n2` of the request record.
pub struct Effect<'a> {
    pub op: i64,
    pub a: [&'a [u8]; 5],
    pub n1: i64,
    pub n2: i64,
}

/// The provisioning vault: the volume key's custody while it is split.
pub trait Vault {
    /// A fresh 32-byte volume key that may be split: a handle, or a
    /// negative errno.
    fn generate(&mut self) -> i32;
    /// `SHARE_SPLIT` of `handle` under `arg` (whose output pointer the
    /// implementation sets), the three envelopes into `out`. 0, or a
    /// negative errno.
    fn split(
        &mut self,
        handle: i32,
        arg: &mut [u8; key_vault::share::split::LEN],
        out: &mut [u8; ENVELOPES],
    ) -> i32;
    fn destroy(&mut self, handle: i32);
}

/// The three custody envelopes, in share-index order.
pub const ENVELOPES: usize = 3 * sk::ENVELOPE_LEN;
/// The three custodians' recovery public keys, in share-index order.
pub const CUSTODIANS: usize = 3 * sk::PUBLIC_LEN;
/// Creations held between a create or rotation and its record set.
const HELD: usize = 4;
/// Grants seen, by resource.
const SEEN: usize = 8;
/// Retirements made, remembered.
const RETIRED: usize = 8;
/// Longest actor this engine names.
pub const MAX_ACTOR: usize = 64;
/// Longest result: a 16-byte id in hex, or a device.
const RESULT_CAP: usize = sk::MAX_ID;

const S_IDLE: u8 = 0;
const S_ASKED: u8 = 1;
/// A regrant's replacement was refused over a revoked grant: grant afresh.
const S_REGRANT_FRESH: u8 = 2;

#[derive(Clone, Copy)]
struct Held {
    used: bool,
    age: u32,
    resource: [u8; 16],
    epoch: u32,
    creation: [u8; sk::MAX_RECORD],
    creation_len: u16,
    /// The vault's handle to the split key, or -1.
    handle: i32,
    envelopes: [u8; ENVELOPES],
    split: bool,
    /// The set Kagi recorded, once it has.
    set: [u8; 16],
    recorded: bool,
}

const NO_HELD: Held = Held {
    used: false,
    age: 0,
    resource: [0; 16],
    epoch: 0,
    creation: [0; sk::MAX_RECORD],
    creation_len: 0,
    handle: -1,
    envelopes: [0; ENVELOPES],
    split: false,
    set: [0; 16],
    recorded: false,
};

/// What the last grant or ticket record for a resource said.
#[derive(Clone, Copy)]
struct Seen {
    used: bool,
    age: u32,
    resource: [u8; 16],
    device: [u8; sk::MAX_ID],
    device_len: u8,
    revoked: bool,
    has_policy: bool,
    custody: u8,
    min_tier: u8,
    min_assurance: u8,
    lifetime: u32,
}

const NO_SEEN: Seen = Seen {
    used: false,
    age: 0,
    resource: [0; 16],
    device: [0; sk::MAX_ID],
    device_len: 0,
    revoked: false,
    has_policy: false,
    custody: 0,
    min_tier: 0,
    min_assurance: 0,
    lifetime: 0,
};

pub struct KagiStorage {
    custodians: [u8; CUSTODIANS],
    aead: u16,
    actor: [u8; MAX_ACTOR],
    actor_len: u8,
    held: [Held; HELD],
    seen: [Seen; SEEN],
    retired: [([u8; 16], u32); RETIRED],
    retired_at: u8,
    age: u32,

    // The effect in flight.
    op: i64,
    stage: u8,
    resource: [u8; 16],
    epoch: u32,
    device: [u8; sk::MAX_ID],
    device_len: u8,
    job_actor: [u8; MAX_ACTOR],
    job_actor_len: u8,
    custody: u8,
    min_tier: u8,
    lifetime: u32,
    /// Kagi's refusal code when it refused, else 0.
    pub refusal: u8,
    /// The result field (11 set, 12 grant, 14 r1) and its value.
    pub result_field: u8,
    result: [u8; RESULT_CAP],
    result_len: u8,
}

impl KagiStorage {
    pub const fn empty() -> Self {
        KagiStorage {
            custodians: [0; CUSTODIANS],
            aead: 0,
            actor: [0; MAX_ACTOR],
            actor_len: 0,
            held: [NO_HELD; HELD],
            seen: [NO_SEEN; SEEN],
            retired: [([0; 16], 0); RETIRED],
            retired_at: 0,
            age: 0,
            op: 0,
            stage: S_IDLE,
            resource: [0; 16],
            epoch: 0,
            device: [0; sk::MAX_ID],
            device_len: 0,
            job_actor: [0; MAX_ACTOR],
            job_actor_len: 0,
            custody: 0,
            min_tier: 0,
            lifetime: 0,
            refusal: 0,
            result_field: 0,
            result: [0; RESULT_CAP],
            result_len: 0,
        }
    }

    /// The custodians' recovery keys, the custody envelopes' AEAD and who
    /// this control plane says asked when an effect names no one. False when
    /// any is unusable: three distinct uncompressed P-256 keys, a share AEAD,
    /// a non-empty actor.
    pub fn configure(&mut self, custodians: &[u8], aead: u16, actor: &[u8]) -> bool {
        let keys_ok = custodians.len() == CUSTODIANS && {
            let k = |i: usize| &custodians[i * sk::PUBLIC_LEN..(i + 1) * sk::PUBLIC_LEN];
            (0..3).all(|i| k(i)[0] == 0x04) && k(0) != k(1) && k(1) != k(2) && k(0) != k(2)
        };
        let aead_ok = matches!(
            aead,
            key_vault::share::aead::CHACHA20_POLY1305 | key_vault::share::aead::AES_256_GCM
        );
        if !keys_ok || !aead_ok || actor.is_empty() || actor.len() > MAX_ACTOR {
            return false;
        }
        self.custodians.copy_from_slice(custodians);
        self.aead = aead;
        self.actor[..actor.len()].copy_from_slice(actor);
        self.actor_len = actor.len() as u8;
        true
    }

    pub fn result(&self) -> &[u8] {
        &self.result[..self.result_len as usize]
    }

    /// Start `e`; `out` takes the request envelope.
    pub fn begin<V: Vault>(
        &mut self,
        e: &Effect<'_>,
        sha: sk::Sha256Fn,
        vault: &mut V,
        out: &mut [u8],
    ) -> Next {
        self.op = e.op;
        self.stage = S_IDLE;
        self.refusal = 0;
        self.result_field = 0;
        self.result_len = 0;
        self.device_len = 0;
        self.job_actor_len = 0;
        if self.actor_len == 0 {
            return Next::Done(Code::Failed);
        }
        let vol = match e.op {
            OP_KAGI_REPLACE => e.a[2],
            _ => e.a[0],
        };
        if vol.is_empty() {
            return Next::Done(Code::Failed);
        }
        let mut d = [0u8; 32];
        sha(vol, &mut d);
        self.resource.copy_from_slice(&d[..16]);
        let actor_arg: &[u8] = match e.op {
            OP_KAGI_ROTATE_GRANT | OP_KAGI_RECOVERY_AUTHORISE => e.a[3],
            OP_KAGI_REVOKE | OP_KAGI_ERASE => e.a[1],
            OP_KAGI_ROTATE_KEY | OP_KAGI_RETIRE_EPOCH | OP_KAGI_REGRANT => e.a[2],
            _ => &[],
        };
        let actor_arg = if actor_arg.is_empty() {
            &self.actor[..self.actor_len as usize]
        } else {
            actor_arg
        };
        if actor_arg.len() > MAX_ACTOR {
            return Next::Done(Code::Failed);
        }
        let mut actor = [0u8; MAX_ACTOR];
        actor[..actor_arg.len()].copy_from_slice(actor_arg);
        self.job_actor = actor;
        self.job_actor_len = actor_arg.len() as u8;

        let mut body = [0u8; svc::MAX_REQUEST];
        let mut w = sk::Writer::new(&mut body);
        let head = w.u8(sk::resource::VOLUME).is_some() && w.bytes(&self.resource).is_some();
        if !head {
            return Next::Done(Code::Failed);
        }
        let kagi = match e.op {
            OP_KAGI_CREATE => {
                let Some((custody, tier, life)) = policy_of(e.a[2], e.n1, e.n2) else {
                    return Next::Done(Code::Failed);
                };
                let ok = w.f8(e.a[1]).is_some()
                    && write_policy(&mut w, custody, tier, 0, life)
                    && w.u16(self.aead).is_some()
                    && w.bytes(&self.custodians).is_some();
                if !ok || e.a[1].is_empty() {
                    return Next::Done(Code::Failed);
                }
                self.epoch = 1;
                sk::op::CREATE
            }
            OP_VAULT_SPLIT => return Next::Done(self.split(epoch_or_one(e.a[1]), vault)),
            OP_VAULT_DESTROY => return Next::Done(self.destroy(epoch_or_one(e.a[1]), vault)),
            OP_KAGI_RECORD_SET => {
                let Some(epoch) = epoch_or_one(e.a[1]) else {
                    return Next::Done(Code::Failed);
                };
                self.epoch = epoch;
                if let Some(h) = self.find_held(epoch) {
                    if self.held[h].recorded {
                        let set = self.held[h].set;
                        self.set_hex(11, &set);
                        return Next::Done(Code::Ok);
                    }
                }
                // Without the envelopes held, the request says so with three
                // empty ones; Kagi refuses it.
                let empty = [0u8; 0];
                let envs = self.find_held(epoch).filter(|&h| self.held[h].split);
                let ok = w.u32(epoch).is_some()
                    && (0..3).all(|i| match envs {
                        Some(h) => w
                            .f16(
                                &self.held[h].envelopes
                                    [i * sk::ENVELOPE_LEN..(i + 1) * sk::ENVELOPE_LEN],
                            )
                            .is_some(),
                        None => w.f16(&empty).is_some(),
                    });
                if !ok {
                    return Next::Done(Code::Failed);
                }
                sk::op::RECORD_SET
            }
            OP_KAGI_GRANT | OP_KAGI_ROTATE_GRANT => {
                let Some((custody, tier, life)) = policy_of(e.a[2], e.n1, e.n2) else {
                    return Next::Done(Code::Failed);
                };
                if !self.set_device(e.a[1]) {
                    return Next::Done(Code::Failed);
                }
                (self.custody, self.min_tier, self.lifetime) = (custody, tier, life);
                if !(w.f8(e.a[1]).is_some() && write_policy(&mut w, custody, tier, 0, life)) {
                    return Next::Done(Code::Failed);
                }
                if e.op == OP_KAGI_GRANT {
                    sk::op::GRANT
                } else {
                    sk::op::ROTATE_GRANT
                }
            }
            OP_KAGI_REPLACE => {
                if !self.set_device(e.a[1])
                    || e.a[0].is_empty()
                    || !(w.f8(e.a[0]).is_some() && w.f8(e.a[1]).is_some())
                {
                    return Next::Done(Code::Failed);
                }
                sk::op::REPLACE_DEVICE
            }
            OP_KAGI_REVOKE => sk::op::REVOKE,
            OP_KAGI_ROTATE_KEY => {
                let Some(from) = decimal_u32(e.a[1]) else {
                    return Next::Done(Code::Failed);
                };
                self.epoch = from.saturating_add(1);
                if w.bytes(&self.custodians).is_none() {
                    return Next::Done(Code::Failed);
                }
                sk::op::ROTATE_KEY
            }
            OP_KAGI_RETIRE_EPOCH => {
                let Some(epoch) = decimal_u32(e.a[1]) else {
                    return Next::Done(Code::Failed);
                };
                self.epoch = epoch;
                if w.u32(epoch).is_none() {
                    return Next::Done(Code::Failed);
                }
                sk::op::RETIRE_EPOCH
            }
            OP_KAGI_RECOVERY_AUTHORISE => {
                let (Some(epoch), Ok(ttl)) = (decimal_u32(e.a[1]), u32::try_from(e.n1)) else {
                    return Next::Done(Code::Failed);
                };
                let ok = self.set_device(e.a[2])
                    && w.u32(epoch).is_some()
                    && w.f8(e.a[2]).is_some()
                    && w.u32(ttl).is_some();
                if !ok {
                    return Next::Done(Code::Failed);
                }
                self.epoch = epoch;
                sk::op::RECOVERY_AUTHORISE
            }
            OP_KAGI_REGRANT => {
                // a1 vol, a2 the recovered device, a3 actor, a4 the device the
                // grant names now.
                if !self.set_device(e.a[1]) {
                    return Next::Done(Code::Failed);
                }
                let old = e.a[3];
                if old == e.a[1] {
                    let device = self.device;
                    self.set_result(12, &device[..self.device_len as usize]);
                    return Next::Done(Code::Ok);
                }
                if old.is_empty() {
                    return self.fresh_grant(out);
                }
                if !(w.f8(old).is_some() && w.f8(e.a[1]).is_some()) {
                    return Next::Done(Code::Failed);
                }
                sk::op::REPLACE_DEVICE
            }
            OP_KAGI_ERASE => {
                let Some(epoch) = epoch_or_one(e.a[2]) else {
                    return Next::Done(Code::Failed);
                };
                let Ok(pins) = u32::try_from(e.n1) else {
                    return Next::Done(Code::Failed);
                };
                self.epoch = epoch;
                if !(w.u32(epoch).is_some() && w.u32(pins).is_some()) {
                    return Next::Done(Code::Failed);
                }
                sk::op::ERASE
            }
            _ => return Next::Done(Code::Failed),
        };
        if w.f8(&self.job_actor[..self.job_actor_len as usize])
            .is_none()
        {
            return Next::Done(Code::Failed);
        }
        let n = w.len();
        self.ask(kagi, &body[..n], out)
    }

    fn ask(&mut self, kagi: u8, body: &[u8], out: &mut [u8]) -> Next {
        match svc::write_request(kagi, body, out) {
            Some(n) => {
                if self.stage == S_IDLE {
                    self.stage = S_ASKED;
                }
                Next::Send(n)
            }
            None => Next::Done(Code::Failed),
        }
    }

    /// A regrant over a revoked grant: grant the recovered device afresh
    /// under the policy the last grant or ticket seen for the resource
    /// carried.
    fn fresh_grant(&mut self, out: &mut [u8]) -> Next {
        let Some(at) = self.find_seen() else {
            return Next::Done(Code::Refused);
        };
        let s = self.seen[at];
        if !s.has_policy {
            return Next::Done(Code::Refused);
        }
        let mut body = [0u8; 2 * sk::MAX_ID + 64];
        let mut w = sk::Writer::new(&mut body);
        let ok = w.u8(sk::resource::VOLUME).is_some()
            && w.bytes(&self.resource).is_some()
            && w.f8(&self.device[..self.device_len as usize]).is_some()
            && write_policy(&mut w, s.custody, s.min_tier, s.min_assurance, s.lifetime)
            && w.f8(&self.job_actor[..self.job_actor_len as usize])
                .is_some();
        if !ok {
            return Next::Done(Code::Failed);
        }
        let n = w.len();
        self.stage = S_REGRANT_FRESH;
        self.ask(sk::op::GRANT, &body[..n], out)
    }

    /// The reply to this job's request; `out` takes a follow-up request.
    pub fn on_reply<V: Vault>(
        &mut self,
        r: &svc::Reply<'_>,
        vault: &mut V,
        out: &mut [u8],
    ) -> Next {
        if self.stage == S_IDLE {
            return Next::Done(Code::Failed);
        }
        if r.status == svc::STATUS_OK {
            return Next::Done(self.on_ok(r, vault));
        }
        if r.status != svc::STATUS_REFUSED {
            return Next::Done(Code::Failed);
        }
        let refusal = r.refusal;
        self.refusal = refusal;
        let is = |x: sk::Refusal| refusal == x.code();
        if is(sk::Refusal::LedgerUnavailable) || is(sk::Refusal::Busy) {
            return Next::Done(Code::Unavailable);
        }
        let device = self.device;
        let device = &device[..self.device_len as usize];
        let seen = self.find_seen().map(|i| self.seen[i]);
        let names = |s: &Seen| &s.device[..s.device_len as usize] == device;
        match self.op {
            OP_KAGI_REVOKE if is(sk::Refusal::GrantRevoked) => {
                self.note_revoked();
                Next::Done(Code::Ok)
            }
            OP_KAGI_GRANT
                if is(sk::Refusal::Conflict)
                    && seen.is_some_and(|s| {
                        names(&s)
                            && !s.revoked
                            && s.has_policy
                            && (s.custody, s.min_tier, s.lifetime)
                                == (self.custody, self.min_tier, self.lifetime)
                    }) =>
            {
                self.set_result(12, device);
                Next::Done(Code::Ok)
            }
            OP_KAGI_REPLACE | OP_KAGI_REGRANT
                if self.stage == S_ASKED
                    && is(sk::Refusal::Conflict)
                    && seen.is_some_and(|s| names(&s) && !s.revoked) =>
            {
                self.set_result(12, device);
                Next::Done(Code::Ok)
            }
            OP_KAGI_REGRANT if self.stage == S_ASKED && is(sk::Refusal::GrantRevoked) => {
                self.note_revoked();
                self.fresh_grant(out)
            }
            OP_KAGI_CREATE if is(sk::Refusal::Conflict) && self.find_held(1).is_some() => {
                Next::Done(Code::Ok)
            }
            OP_KAGI_ROTATE_KEY
                if is(sk::Refusal::RotationInProgress) && self.find_held(self.epoch).is_some() =>
            {
                let epoch = self.epoch;
                self.set_decimal(14, epoch);
                Next::Done(Code::Ok)
            }
            OP_KAGI_RETIRE_EPOCH
                if is(sk::Refusal::WrongEpoch)
                    && self
                        .retired
                        .iter()
                        .any(|&(res, e)| res == self.resource && e == self.epoch && e != 0) =>
            {
                Next::Done(Code::Ok)
            }
            _ => Next::Done(Code::Refused),
        }
    }

    fn on_ok<V: Vault>(&mut self, r: &svc::Reply<'_>, vault: &mut V) -> Code {
        let Ok(rec) = sk::Record::parse(r.record) else {
            return Code::Failed;
        };
        let b = rec.binding;
        if b.resource != self.resource || b.resource_kind != sk::resource::VOLUME {
            return Code::Failed;
        }
        match self.op {
            OP_KAGI_CREATE | OP_KAGI_ROTATE_KEY => {
                // The creation names the custodians asked for and the epoch
                // expected; the split runs under it.
                let keys_ok = rec.items().len() == 3
                    && (0..3).all(|i| {
                        rec.item(i)
                            == Some(&self.custodians[i * sk::PUBLIC_LEN..(i + 1) * sk::PUBLIC_LEN])
                    });
                if b.kind != sk::kind::CREATION || !keys_ok {
                    return Code::Failed;
                }
                if self.op == OP_KAGI_CREATE && b.epoch != 1 {
                    return Code::Failed;
                }
                self.epoch = b.epoch;
                if !self.hold(b.epoch, r.record, vault) {
                    return Code::Failed;
                }
                if self.op == OP_KAGI_ROTATE_KEY {
                    self.set_decimal(14, b.epoch);
                }
                Code::Ok
            }
            OP_KAGI_RECORD_SET => {
                if b.kind != sk::kind::RECOVERY_SET || b.epoch != self.epoch {
                    return Code::Failed;
                }
                if let Some(h) = self.find_held(self.epoch) {
                    let held = &mut self.held[h];
                    held.recorded = true;
                    held.set = b.set_id;
                    held.split = false;
                    held.envelopes = [0; ENVELOPES];
                    held.creation_len = 0;
                }
                self.set_hex(11, &b.set_id);
                Code::Ok
            }
            OP_KAGI_RECOVERY_AUTHORISE => {
                if b.kind != sk::kind::RECOVERY_TICKET {
                    return Code::Failed;
                }
                // The ticket carries the grant's policy: a regrant over a
                // revoked grant is decided under it.
                let at = self.seen_slot();
                let s = &mut self.seen[at];
                s.has_policy = true;
                (s.custody, s.min_tier, s.min_assurance, s.lifetime) =
                    (b.custody, b.min_tier, b.min_assurance, b.lifetime_secs);
                self.set_hex(14, &b.grant_id);
                Code::Ok
            }
            OP_KAGI_ERASE => {
                if b.kind != sk::kind::ERASURE {
                    return Code::Failed;
                }
                Code::Ok
            }
            _ => {
                // Every other verb answers the grant it committed.
                if b.kind != sk::kind::GRANT {
                    return Code::Failed;
                }
                self.see_grant(&rec);
                if self.op == OP_KAGI_RETIRE_EPOCH {
                    let at = self.retired_at as usize % RETIRED;
                    self.retired[at] = (self.resource, self.epoch);
                    self.retired_at = self.retired_at.wrapping_add(1);
                }
                if matches!(self.op, OP_KAGI_GRANT | OP_KAGI_REPLACE | OP_KAGI_REGRANT) {
                    let device = rec.names.device;
                    if device.len() > RESULT_CAP {
                        return Code::Failed;
                    }
                    self.set_result(12, device);
                }
                Code::Ok
            }
        }
    }

    /// `VAULT_SPLIT`: a fresh key in the vault, split under the held
    /// creation into three custody envelopes held for the record set.
    fn split<V: Vault>(&mut self, epoch: Option<u32>, vault: &mut V) -> Code {
        let Some(epoch) = epoch else {
            return Code::Failed;
        };
        let Some(h) = self.find_held(epoch) else {
            return Code::Failed;
        };
        let held = &mut self.held[h];
        if held.recorded || held.split {
            return Code::Ok;
        }
        let Ok(rec) = sk::Record::parse(&held.creation[..held.creation_len as usize]) else {
            return Code::Failed;
        };
        let mut arg = [0u8; key_vault::share::split::LEN];
        if sk::write_split_arg(&rec, 0, 0, &mut arg).is_err() {
            return Code::Failed;
        }
        if held.handle < 0 {
            let k = vault.generate();
            if k < 0 {
                return Code::Unavailable;
            }
            held.handle = k;
        }
        if vault.split(held.handle, &mut arg, &mut held.envelopes) != 0 {
            vault.destroy(held.handle);
            held.handle = -1;
            return Code::Failed;
        }
        held.split = true;
        Code::Ok
    }

    /// `VAULT_DESTROY`: the vault's key handle goes. Once the set is
    /// recorded, nothing for the epoch is held any longer.
    fn destroy<V: Vault>(&mut self, epoch: Option<u32>, vault: &mut V) -> Code {
        let Some(epoch) = epoch else {
            return Code::Failed;
        };
        if let Some(h) = self.find_held(epoch) {
            let held = &mut self.held[h];
            if held.handle >= 0 {
                vault.destroy(held.handle);
                held.handle = -1;
            }
            held.split = false;
            held.envelopes = [0; ENVELOPES];
            if held.recorded {
                *held = NO_HELD;
            }
        }
        Code::Ok
    }

    fn find_held(&self, epoch: u32) -> Option<usize> {
        self.held
            .iter()
            .position(|h| h.used && h.resource == self.resource && h.epoch == epoch)
    }

    /// Hold a creation for the split. The same creation asked again keeps
    /// the key already split under it; a new one for the resource and epoch
    /// replaces it; otherwise the oldest held is dropped, its vault handle
    /// destroyed with it.
    fn hold<V: Vault>(&mut self, epoch: u32, creation: &[u8], vault: &mut V) -> bool {
        if creation.len() > sk::MAX_RECORD {
            return false;
        }
        self.age = self.age.wrapping_add(1);
        let at = match self.find_held(epoch) {
            Some(h) => h,
            None => match self.held.iter().position(|h| !h.used) {
                Some(h) => h,
                None => {
                    let mut oldest = 0;
                    for i in 1..HELD {
                        if self.held[i].age < self.held[oldest].age {
                            oldest = i;
                        }
                    }
                    oldest
                }
            },
        };
        let prev = &mut self.held[at];
        let same = prev.used
            && prev.resource == self.resource
            && prev.epoch == epoch
            && &prev.creation[..prev.creation_len as usize] == creation;
        if !same {
            if prev.handle >= 0 {
                vault.destroy(prev.handle);
            }
            *prev = NO_HELD;
            prev.used = true;
            prev.resource = self.resource;
            prev.epoch = epoch;
            prev.creation[..creation.len()].copy_from_slice(creation);
            prev.creation_len = creation.len() as u16;
        }
        prev.age = self.age;
        true
    }

    fn seen_slot(&mut self) -> usize {
        self.age = self.age.wrapping_add(1);
        if let Some(i) = self.find_seen() {
            self.seen[i].age = self.age;
            return i;
        }
        let at = match self.seen.iter().position(|s| !s.used) {
            Some(i) => i,
            None => {
                let mut oldest = 0;
                for i in 1..SEEN {
                    if self.seen[i].age < self.seen[oldest].age {
                        oldest = i;
                    }
                }
                oldest
            }
        };
        self.seen[at] = NO_SEEN;
        self.seen[at].used = true;
        self.seen[at].age = self.age;
        self.seen[at].resource = self.resource;
        at
    }

    fn find_seen(&self) -> Option<usize> {
        self.seen
            .iter()
            .position(|s| s.used && s.resource == self.resource)
    }

    fn see_grant(&mut self, rec: &sk::Record<'_>) {
        let device = rec.names.device;
        if device.len() > sk::MAX_ID {
            return;
        }
        let b = rec.binding;
        let at = self.seen_slot();
        let s = &mut self.seen[at];
        s.device[..device.len()].copy_from_slice(device);
        s.device_len = device.len() as u8;
        s.revoked = b.revoked();
        s.has_policy = true;
        (s.custody, s.min_tier, s.min_assurance, s.lifetime) =
            (b.custody, b.min_tier, b.min_assurance, b.lifetime_secs);
    }

    fn note_revoked(&mut self) {
        if let Some(i) = self.find_seen() {
            self.seen[i].revoked = true;
        }
    }

    fn set_device(&mut self, device: &[u8]) -> bool {
        if device.is_empty() || device.len() > sk::MAX_ID {
            return false;
        }
        self.device[..device.len()].copy_from_slice(device);
        self.device_len = device.len() as u8;
        true
    }

    fn set_result(&mut self, field: u8, v: &[u8]) {
        let n = v.len().min(RESULT_CAP);
        self.result[..n].copy_from_slice(&v[..n]);
        self.result_len = n as u8;
        self.result_field = field;
    }

    fn set_hex(&mut self, field: u8, id: &[u8; 16]) {
        let mut hex = [0u8; 32];
        if sk::hex_into(id, &mut hex) == Some(32) {
            self.set_result(field, &hex);
        }
    }

    fn set_decimal(&mut self, field: u8, v: u32) {
        let mut buf = [0u8; 10];
        let mut n = 0;
        let mut x = v;
        loop {
            buf[n] = b'0' + (x % 10) as u8;
            n += 1;
            x /= 10;
            if x == 0 {
                break;
            }
        }
        buf[..n].reverse();
        self.set_result(field, &buf[..n]);
    }
}

/// `possession` or `hardware`, a tier and a lifetime in seconds, as Kagi's
/// policy fields.
fn policy_of(custody: &[u8], tier: i64, lifetime: i64) -> Option<(u8, u8, u32)> {
    let custody = match custody {
        b"possession" => sk::custody::POSSESSION_BOUND,
        b"hardware" => sk::custody::HARDWARE_BOUND,
        _ => return None,
    };
    let tier = u8::try_from(tier).ok()?;
    let lifetime = u32::try_from(lifetime).ok().filter(|&l| l > 0)?;
    Some((custody, tier, lifetime))
}

fn write_policy(w: &mut sk::Writer<'_>, custody: u8, tier: u8, assurance: u8, life: u32) -> bool {
    w.u8(custody).is_some()
        && w.u8(tier).is_some()
        && w.u8(assurance).is_some()
        && w.u32(life).is_some()
}

fn decimal_u32(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 10 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    u32::try_from(v).ok().filter(|&v| v != 0)
}

/// An epoch argument; empty is the first.
fn epoch_or_one(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        Some(1)
    } else {
        decimal_u32(b)
    }
}
