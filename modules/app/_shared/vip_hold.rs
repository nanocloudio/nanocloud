// The node's load-balancer address ownership: node_net's own hold and an
// address controller's, decided in one place.
//
// node_net is the single writer of the host's held-address table (net.route
// HOLD). It answers for a Service's load-balancer address in one of two ways:
//
// - **its own hold** (legacy): a claim `/net-hold/<addr>` = `node=<n>`,
//   created under ABSENT by an eligible node, given up when the node may no
//   longer answer, taken over from a node that is not ready;
// - **the controller's**: an address controller on this node (any module
//   speaking Fluxor's `addr_ctl` contract to node_net) drives the
//   address through Fluxor's `addr_ctl` contract (ADDR_ADD disarmed, ADDR_ARM
//   and ADDR_FENCE under the install token node_net minted, ADDR_DEL), the
//   same frames it sends Fluxor `ip` on bare metal, and node_net realises
//   them in the HOLD table and answers on `addr_evt`.
//
// The rules, which keep every address at one owner or none only across a
// fence:
//
// - **a controller takes an address by marking it.** Its ADD writes
//   `/net-vip/<addr>` = `node=<n>`; while a mark stands no node's own hold
//   claims the address, so the controller set — Clustor's fenced authority —
//   decides who answers for it, not the race between nodes.
// - **adoption in place.** An ADD on the node that holds the claim adopts
//   it: the HOLD row stays armed, the claim stays, nothing is removed and
//   nothing is installed twice. ARM is then a no-op on the wire.
// - **another node arms only after the claim is released.** An ADD where
//   another node holds the claim installs the row fenced; its ARM waits
//   (`arm_pending`) until no claim stands, then claims under ABSENT and arms
//   once the claim reads as its own. The holder releases only by its own
//   controller's FENCE (the fenced handover), or by being taken over as not
//   ready.
// - **a fence is the handover.** FENCE is the old owner's confirmed
//   emission-disable — all the proof a controller needs before it commits a
//   new owner (a graceful move needs no more than it, and a move's source
//   is never told to delete) — so the fenced row stops
//   emitting at once and the claim is given up on the next pass. The row
//   stays installed, fenced, and the mark stays, until the controller
//   re-arms it or deletes it; a re-ARM claims again like any other.
// - **a delete that was not fenced falls back to the hold.** The controller
//   restarted (its first act is a clean-slate ADDR_DEL) or let the address
//   go without cutting it: the row stays as it is, the mark is removed, and
//   the node's own hold takes the address back with the claim it already
//   has. A delete after a FENCE removes the fenced row; the claim went
//   with the fence, and the mark goes on a later pass like any mark of
//   this node's without an entry (a destination that armed holds the
//   claim and re-marks; with none — a release — the hold resumes).
// - **controller loss falls back to the hold.** An entry outlives silence
//   (nothing is torn down for want of frames), and a mark whose node is not
//   ready is removed, so the own-hold rules resume for the address.
//
// Pure, no_std, zero-alloc; the I/O is node_net's.

/// Addresses the controller may hold here at once (node_net's table).
pub const VH_MAX: usize = 32;
/// Events queued for `addr_evt`, the largest of which is an ADDED frame.
pub const VH_EVQ: usize = 8;
pub const VH_EVT_MAX: usize = 3 + 36;

pub const VH_ADDR_ADD: u8 = abi::registry::msg::identity::ADDR_ADD;
pub const VH_ADDR_DEL: u8 = abi::registry::msg::identity::ADDR_DEL;
pub const VH_ADDR_ARM: u8 = abi::registry::msg::identity::ADDR_ARM;
pub const VH_ADDR_FENCE: u8 = abi::registry::msg::identity::ADDR_FENCE;
pub const VH_MSG_ADDED: u8 = abi::registry::msg::identity::MSG_ADDR_ADDED;
pub const VH_MSG_ARMED: u8 = abi::registry::msg::identity::MSG_ADDR_ARMED;
pub const VH_MSG_FENCED: u8 = abi::registry::msg::identity::MSG_ADDR_FENCED;
pub const VH_MSG_REFUSED: u8 = abi::registry::msg::identity::MSG_ADDR_REFUSED;
pub const VH_REFUSE_NOT_FOUND: u8 = 1;
pub const VH_REFUSE_TOKEN: u8 = 2;
pub const VH_REFUSE_TABLE_FULL: u8 = 5;
/// node_net hands the row to the host provider; it cannot prove the wire.
pub const VH_CUTOFF_RING_HANDOFF: u8 = 0;

/// The claim `/net-hold/<addr>` as last read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VhClaim {
    Free,
    Ours,
    /// Another node's; `ready` is that node's readiness.
    Other {
        ready: bool,
    },
}

/// The mark `/net-vip/<addr>` as last read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VhMark {
    None,
    Ours,
    Other { ready: bool },
}

/// What one address's facts decide: whether this node's row is armed (None:
/// no row), and the one store write owed for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct VhDecision {
    pub row: Option<bool>,
    pub write: VhWrite,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VhWrite {
    None,
    /// Create the claim under ABSENT.
    Claim,
    /// Delete the claim (ours, or a not-ready node's, on the revision read).
    Unclaim,
    /// Create (or take over) the mark.
    Mark,
    /// Delete the mark (ours, or a not-ready node's).
    Unmark,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct VhEntry {
    pub used: u8,
    /// The HOLD row emits.
    pub armed: u8,
    /// Cut by ADDR_FENCE since the last ARM: the claim is given up.
    pub fenced: u8,
    /// ARM asked while the claim was another node's (or not yet ours).
    pub arm_pending: u8,
    /// Installed over this node's own claim (adoption).
    pub adopted: u8,
    pub prefix_len: u8,
    pub _pad: [u8; 2],
    pub addr: u32,
    pub generation: u32,
    pub token: [u8; 16],
}

impl VhEntry {
    pub const EMPTY: VhEntry = VhEntry {
        used: 0,
        armed: 0,
        fenced: 0,
        arm_pending: 0,
        adopted: 0,
        prefix_len: 0,
        _pad: [0; 2],
        addr: 0,
        generation: 0,
        token: [0; 16],
    };
}

/// Handovers owed after an entry is gone: the claim to release (a fenced
/// delete) or the mark to remove (an unfenced one), by address.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct VhOwed {
    pub addr: u32,
    pub write: u8,
    pub _pad: [u8; 3],
}

#[repr(C)]
pub struct VipHold {
    pub entries: [VhEntry; VH_MAX],
    pub owed: [VhOwed; VH_MAX],
    pub owed_n: u8,
    pub ev_head: u8,
    pub ev_n: u8,
    pub _pad: u8,
    pub next_gen: u32,
    pub cutoffs: u64,
    pub ev_len: [u8; VH_EVQ],
    pub evq: [[u8; VH_EVT_MAX]; VH_EVQ],
    /// Frames refused (bad length, unknown opcode) and events dropped on a
    /// full queue.
    pub malformed: u32,
    pub ev_dropped: u32,
}

/// An IPv4 address in a 16-byte slot (bytes 0..4, network order), or None.
pub fn vh_addr(slot: &[u8]) -> Option<u32> {
    if slot.len() < 16 || slot[4..16].iter().any(|&b| b != 0) {
        return None;
    }
    Some(u32::from_be_bytes([slot[0], slot[1], slot[2], slot[3]]))
}

fn slot_of(a: u32) -> [u8; 16] {
    let mut s = [0u8; 16];
    s[..4].copy_from_slice(&a.to_be_bytes());
    s
}

impl Default for VipHold {
    fn default() -> Self {
        Self::new()
    }
}

impl VipHold {
    pub const fn new() -> Self {
        VipHold {
            entries: [VhEntry::EMPTY; VH_MAX],
            owed: [VhOwed {
                addr: 0,
                write: 0,
                _pad: [0; 3],
            }; VH_MAX],
            owed_n: 0,
            ev_head: 0,
            ev_n: 0,
            _pad: 0,
            next_gen: 0,
            cutoffs: 0,
            ev_len: [0; VH_EVQ],
            evq: [[0; VH_EVT_MAX]; VH_EVQ],
            malformed: 0,
            ev_dropped: 0,
        }
    }

    pub fn entry(&self, addr: u32) -> Option<&VhEntry> {
        self.entries.iter().find(|e| e.used != 0 && e.addr == addr)
    }

    fn index(&self, addr: u32) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.used != 0 && e.addr == addr)
    }

    /// Addresses the controller holds here.
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|e| e.used != 0).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Room for the events one frame can produce.
    pub fn ev_room(&self) -> bool {
        (self.ev_n as usize) < VH_EVQ
    }

    fn push_ev(&mut self, op: u8, payload: &[u8]) {
        if self.ev_n as usize >= VH_EVQ || payload.len() + 3 > VH_EVT_MAX {
            self.ev_dropped = self.ev_dropped.wrapping_add(1);
            return;
        }
        let at = (self.ev_head as usize + self.ev_n as usize) % VH_EVQ;
        let f = &mut self.evq[at];
        f[0] = op;
        f[1..3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
        f[3..3 + payload.len()].copy_from_slice(payload);
        self.ev_len[at] = (3 + payload.len()) as u8;
        self.ev_n += 1;
    }

    /// The next event for `addr_evt`, if any.
    pub fn event(&self) -> Option<&[u8]> {
        if self.ev_n == 0 {
            return None;
        }
        let at = self.ev_head as usize;
        Some(&self.evq[at][..self.ev_len[at] as usize])
    }

    pub fn event_written(&mut self) {
        if self.ev_n > 0 {
            self.ev_head = ((self.ev_head as usize + 1) % VH_EVQ) as u8;
            self.ev_n -= 1;
        }
    }

    fn refuse(&mut self, slot: &[u8], op: u8, reason: u8) {
        let mut p = [0u8; 18];
        p[..16].copy_from_slice(&slot[..16]);
        p[16] = op;
        p[17] = reason;
        self.push_ev(VH_MSG_REFUSED, &p);
    }

    fn owe(&mut self, addr: u32, w: VhWrite) {
        let code = match w {
            VhWrite::Unclaim => 1,
            VhWrite::Unmark => 2,
            _ => return,
        };
        if let Some(o) = self.owed[..self.owed_n as usize]
            .iter_mut()
            .find(|o| o.addr == addr)
        {
            o.write = code;
            return;
        }
        if (self.owed_n as usize) < VH_MAX {
            self.owed[self.owed_n as usize] = VhOwed {
                addr,
                write: code,
                _pad: [0; 3],
            };
            self.owed_n += 1;
        }
    }

    fn owed_for(&self, addr: u32) -> Option<VhWrite> {
        self.owed[..self.owed_n as usize]
            .iter()
            .find(|o| o.addr == addr)
            .map(|o| {
                if o.write == 1 {
                    VhWrite::Unclaim
                } else {
                    VhWrite::Unmark
                }
            })
    }

    /// The handover owed for `addr` was written (or is moot).
    pub fn owed_done(&mut self, addr: u32) {
        let n = self.owed_n as usize;
        if let Some(i) = self.owed[..n].iter().position(|o| o.addr == addr) {
            self.owed[i] = self.owed[n - 1];
            self.owed_n -= 1;
        }
    }

    /// Addresses with a handover still owed (for the pass to visit even
    /// when no Service names them any more).
    pub fn owed_addrs(&self) -> impl Iterator<Item = u32> + '_ {
        self.owed[..self.owed_n as usize].iter().map(|o| o.addr)
    }

    /// One `addr_ctl` frame (`[op][len u16][payload]`). `claim` is the
    /// address's claim as last read; `token` the fresh install token an ADD
    /// would mint (16 bytes from the kernel CSPRNG).
    pub fn on_frame(&mut self, frame: &[u8], claim: VhClaim, token: [u8; 16]) {
        if frame.len() < 3 + 16 {
            self.malformed = self.malformed.wrapping_add(1);
            return;
        }
        let op = frame[0];
        let len = u16::from_le_bytes([frame[1], frame[2]]) as usize;
        let Some(p) = frame.get(3..3 + len) else {
            self.malformed = self.malformed.wrapping_add(1);
            return;
        };
        let Some(addr) = vh_addr(p) else {
            // Not an IPv4 address in its slot: nothing node_net holds.
            self.refuse(p, op, VH_REFUSE_NOT_FOUND);
            return;
        };
        match op {
            VH_ADDR_ADD if len >= 19 => self.add(addr, p[16], claim, token),
            VH_ADDR_ARM if len >= 32 => self.arm(addr, &p[16..32], claim),
            VH_ADDR_FENCE if len >= 32 => self.fence(addr, &p[16..32]),
            VH_ADDR_DEL => self.del(addr),
            _ => {
                self.malformed = self.malformed.wrapping_add(1);
                self.refuse(p, op, VH_REFUSE_NOT_FOUND);
            }
        }
    }

    fn add(&mut self, addr: u32, prefix_len: u8, claim: VhClaim, token: [u8; 16]) {
        let i = match self.index(addr) {
            Some(i) => i,
            None => match self.entries.iter().position(|e| e.used == 0) {
                Some(i) => i,
                None => {
                    self.refuse(&slot_of(addr), VH_ADDR_ADD, VH_REFUSE_TABLE_FULL);
                    return;
                }
            },
        };
        // A new install supersedes any handover owed for the address.
        self.owed_done(addr);
        self.next_gen = self.next_gen.wrapping_add(1).max(1);
        let adopted = claim == VhClaim::Ours;
        self.entries[i] = VhEntry {
            used: 1,
            // Adoption keeps the row emitting: the node already answered
            // for the address, and the controller's commit made it the
            // owner; installing it fenced would cut a live owner.
            armed: adopted as u8,
            fenced: 0,
            arm_pending: 0,
            adopted: adopted as u8,
            prefix_len,
            _pad: [0; 2],
            addr,
            generation: self.next_gen,
            token,
        };
        let mut p = [0u8; 36];
        p[..16].copy_from_slice(&slot_of(addr));
        p[16..32].copy_from_slice(&token);
        p[32..36].copy_from_slice(&self.next_gen.to_le_bytes());
        self.push_ev(VH_MSG_ADDED, &p);
    }

    fn token_ok(&mut self, i: usize, tok: &[u8], op: u8) -> bool {
        if self.entries[i].token[..] == tok[..16] {
            return true;
        }
        self.refuse(&slot_of(self.entries[i].addr), op, VH_REFUSE_TOKEN);
        false
    }

    fn armed_ev(&mut self, i: usize) {
        let mut p = [0u8; 20];
        p[..16].copy_from_slice(&slot_of(self.entries[i].addr));
        p[16..20].copy_from_slice(&self.entries[i].generation.to_le_bytes());
        self.push_ev(VH_MSG_ARMED, &p);
    }

    fn arm(&mut self, addr: u32, tok: &[u8], claim: VhClaim) {
        let Some(i) = self.index(addr) else {
            self.refuse(&slot_of(addr), VH_ADDR_ARM, VH_REFUSE_NOT_FOUND);
            return;
        };
        if !self.token_ok(i, tok, VH_ADDR_ARM) {
            return;
        }
        self.entries[i].fenced = 0;
        if claim == VhClaim::Ours {
            self.entries[i].armed = 1;
            self.entries[i].arm_pending = 0;
            self.armed_ev(i);
        } else {
            // Another node still answers for it, or the claim is not yet
            // ours: armed once it is (`decide` claims it when free).
            self.entries[i].arm_pending = 1;
        }
    }

    fn fence(&mut self, addr: u32, tok: &[u8]) {
        let Some(i) = self.index(addr) else {
            self.refuse(&slot_of(addr), VH_ADDR_FENCE, VH_REFUSE_NOT_FOUND);
            return;
        };
        if !self.token_ok(i, tok, VH_ADDR_FENCE) {
            return;
        }
        let e = &mut self.entries[i];
        e.armed = 0;
        e.arm_pending = 0;
        e.fenced = 1;
        self.cutoffs = self.cutoffs.wrapping_add(1);
        let mut p = [0u8; 30];
        p[..16].copy_from_slice(&slot_of(addr));
        p[16..20].copy_from_slice(&e.generation.to_le_bytes());
        p[20..28].copy_from_slice(&self.cutoffs.to_le_bytes());
        p[28] = VH_CUTOFF_RING_HANDOFF;
        p[29] = 0;
        self.push_ev(VH_MSG_FENCED, &p);
    }

    fn del(&mut self, addr: u32) {
        let Some(i) = self.index(addr) else {
            // A clean-slate delete of an address the controller never
            // added here: nothing of its to remove, and the node's own
            // hold, if any, is untouched.
            self.refuse(&slot_of(addr), VH_ADDR_DEL, VH_REFUSE_NOT_FOUND);
            return;
        };
        let fenced = self.entries[i].fenced != 0;
        self.entries[i] = VhEntry::EMPTY;
        if fenced {
            // After the handover: the claim, if this pass has not given it
            // up yet, goes; the mark stays for the destination's controller.
            self.owe(addr, VhWrite::Unclaim);
        } else {
            // Not cut: the node's own hold takes the address back, with
            // whatever claim stands.
            self.owe(addr, VhWrite::Unmark);
        }
    }

    /// Decide `addr` from its facts: `eligible` is whether this node may
    /// answer for it under its own hold (ready, a segment, a policy it can
    /// serve). Call once per address per pass, then `settle` once the write
    /// it names has been made.
    pub fn decide(
        &mut self,
        addr: u32,
        eligible: bool,
        claim: VhClaim,
        mark: VhMark,
    ) -> VhDecision {
        if let Some(i) = self.index(addr) {
            let e = self.entries[i];
            // The controller drives this address here: it must be marked so
            // no node's own hold claims it. A live mark (ours, or another
            // node's controller mid-handover) is left as it stands.
            if matches!(mark, VhMark::None | VhMark::Other { ready: false }) {
                return VhDecision {
                    row: Some(e.armed != 0 && claim != VhClaim::Other { ready: true }),
                    write: VhWrite::Mark,
                };
            }
            if e.arm_pending != 0 {
                match claim {
                    VhClaim::Ours => {
                        self.entries[i].armed = 1;
                        self.entries[i].arm_pending = 0;
                        self.armed_ev(i);
                        return VhDecision {
                            row: Some(true),
                            write: VhWrite::None,
                        };
                    }
                    VhClaim::Free => {
                        return VhDecision {
                            row: Some(false),
                            write: VhWrite::Claim,
                        }
                    }
                    VhClaim::Other { ready: false } => {
                        // Its holder is out of service: taken over.
                        return VhDecision {
                            row: Some(false),
                            write: VhWrite::Unclaim,
                        };
                    }
                    VhClaim::Other { ready: true } => {
                        return VhDecision {
                            row: Some(false),
                            write: VhWrite::None,
                        }
                    }
                }
            }
            // A fenced entry no longer emits: the fence is the handover, so
            // its claim goes now, not at a delete that a move never sends.
            if e.fenced != 0 && claim == VhClaim::Ours {
                return VhDecision {
                    row: Some(false),
                    write: VhWrite::Unclaim,
                };
            }
            // An armed entry keeps its claim; a fenced one holds a row that
            // does not emit.
            let write = if e.armed != 0 && claim == VhClaim::Free {
                VhWrite::Claim
            } else {
                VhWrite::None
            };
            return VhDecision {
                row: Some(e.armed != 0 && claim != VhClaim::Other { ready: true }),
                write,
            };
        }
        // A handover owed after the controller let the address go.
        if let Some(w) = self.owed_for(addr) {
            return match w {
                VhWrite::Unclaim if claim == VhClaim::Ours => VhDecision {
                    row: Some(false),
                    write: VhWrite::Unclaim,
                },
                VhWrite::Unmark if mark == VhMark::Ours => VhDecision {
                    row: self.own_row(eligible, claim),
                    write: VhWrite::Unmark,
                },
                _ => {
                    self.owed_done(addr);
                    self.decide(addr, eligible, claim, mark)
                }
            };
        }
        match mark {
            // Another node's controller manages it: no new own-hold claim
            // here. A claim this node already holds keeps answering — the
            // controller's destination arms only once it is released (by
            // this node's own controller fencing it, or by this node no
            // longer being eligible), so yielding here would leave the
            // address with no owner.
            VhMark::Other { ready: true } => VhDecision {
                row: if claim == VhClaim::Ours {
                    self.own_row(eligible, claim)
                } else {
                    None
                },
                write: if claim == VhClaim::Ours && !eligible {
                    VhWrite::Unclaim
                } else {
                    VhWrite::None
                },
            },
            // A mark whose node is gone: controller loss, the hold resumes.
            VhMark::Other { ready: false } => VhDecision {
                row: self.own_row(eligible, claim),
                write: VhWrite::Unmark,
            },
            // Our own mark without an entry (node_net restarted under a
            // live controller, or a lost write): the hold resumes; the
            // controller re-marks when it adds again.
            VhMark::Ours => VhDecision {
                row: self.own_row(eligible, claim),
                write: VhWrite::Unmark,
            },
            VhMark::None => VhDecision {
                row: self.own_row(eligible, claim),
                write: match (claim, eligible) {
                    (VhClaim::Free, true) => VhWrite::Claim,
                    (VhClaim::Ours, false) => VhWrite::Unclaim,
                    (VhClaim::Other { ready: false }, true) => VhWrite::Unclaim,
                    _ => VhWrite::None,
                },
            },
        }
    }

    /// The row node_net's own hold puts in the table: armed with the claim,
    /// fenced without it.
    fn own_row(&self, eligible: bool, claim: VhClaim) -> Option<bool> {
        Some(eligible && claim == VhClaim::Ours)
    }

    /// The write `decide` named for `addr` was made (`Unclaim` or `Unmark`
    /// that settled a handover clears it).
    pub fn settle(&mut self, addr: u32, write: VhWrite) {
        if self.index(addr).is_none()
            && matches!(write, VhWrite::Unclaim | VhWrite::Unmark)
            && self.owed_for(addr) == Some(write)
        {
            self.owed_done(addr);
        }
    }
}
