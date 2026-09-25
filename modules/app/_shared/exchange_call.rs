// One call at a time to an exchange provider, without I/O: the requester
// half every connector here shares when a chain's record becomes a request
// to another module (kagi's typed operations, an HTTP endpoint, wave's HTTP
// client) and the answer becomes the record the chain goes on with. Mounted
// by the connectors and by `tests/harness`; the records are the SDK's
// exchange contract (`abi::contracts::exchange`), which the includer mounts.
//
// A call is one exchange. Its id is the call's counter (bytes 0..8) and the
// connector's own request port (bytes 8..12), so two connectors fanned into
// one provider never choose the same id and each takes only its own
// answers. The request is held whole until the call ends: a request the
// channel has no room for is owed and written again on the next step, and a
// call the provider loses to `LINK DOWN` is issued again, under the same id,
// once the provider says `LINK UP`. The first call does not wait for an UP —
// LINK reports a change, never a precondition.
//
// The answer is collected whole, up to `CALL_BODY` bytes, which is also the
// response credit every call grants: an answer past it is refused rather
// than cut (the provider is told with an ABORT, the connector sees a 502).

use abi::contracts::exchange as exch;

/// The largest answer body a call collects, and the response credit it
/// grants.
const CALL_BODY: usize = exch::PAYLOAD_MAX;

/// What one response-direction record meant to the call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CallEvent {
    /// Nothing that ends the call: another exchange's record, a CREDIT, an
    /// answer still arriving, a record that does not parse.
    Nothing,
    /// The answer is whole: [`Call::answer`] reads it.
    Answered,
    /// The call ended without an answer: the provider's ABORT, or an answer
    /// past `CALL_BODY` (502). The status says which.
    Failed(u16),
    /// The provider's backend link dropped: an open call is owed again.
    LinkDown,
    /// The provider is (re)connected: owed calls go out.
    LinkUp,
}

struct Call {
    /// A call is open: issued and not yet answered or failed.
    open: bool,
    /// Its request is still to be written: never placed, or lost to a
    /// `LINK DOWN`.
    owed: bool,
    /// The provider said `LINK DOWN` and has not said `UP` since.
    down: bool,
    /// The request has reached the provider at least once.
    placed: bool,
    /// An ABORT is owed to the provider, for a call given up on: an answer
    /// it refused, or one it stopped waiting for. Its reason.
    abort_owed: bool,
    abort_reason: u8,
    head: bool,
    n: u64,
    id: exch::ExchangeId,
    req: [u8; exch::RECORD_MAX],
    req_len: u16,
    status: u16,
    body: [u8; CALL_BODY],
    body_len: u16,
}

impl Call {
    const fn new() -> Self {
        Call {
            open: false,
            owed: false,
            down: false,
            placed: false,
            abort_owed: false,
            abort_reason: 0,
            head: false,
            n: 0,
            id: exch::ExchangeId::NONE,
            req: [0; exch::RECORD_MAX],
            req_len: 0,
            status: 0,
            body: [0; CALL_BODY],
            body_len: 0,
        }
    }

    /// Reset in place: a module's state is zeroed memory, never `new()`.
    fn reset(&mut self) {
        self.open = false;
        self.owed = false;
        self.down = false;
        self.placed = false;
        self.abort_owed = false;
        self.abort_reason = 0;
        self.head = false;
        self.n = 0;
        self.id = exch::ExchangeId::NONE;
        self.req_len = 0;
        self.status = 0;
        self.body_len = 0;
    }

    fn idle(&self) -> bool {
        !self.open
    }

    /// The id of call `n` from the connector whose request port is `port`.
    fn id_for(n: u64, port: i32) -> exch::ExchangeId {
        let mut id = exch::ExchangeId::from_u64(n);
        let p = port.to_le_bytes();
        id.0[8] = p[0];
        id.0[9] = p[1];
        id.0[10] = p[2];
        id.0[11] = p[3];
        id
    }

    /// Open a call: `write` lays its request record (a request HEAD under the
    /// id it is given, granting `CALL_BODY` of response credit) and answers
    /// its length. False, and nothing opened, when no call can open (one is
    /// already) or the request does not fit one record.
    fn begin<F>(&mut self, port: i32, write: F) -> bool
    where
        F: FnOnce(&exch::ExchangeId, &mut [u8]) -> Option<usize>,
    {
        if self.open {
            return false;
        }
        let n = self.n.wrapping_add(1);
        let id = Self::id_for(n, port);
        let Some(len) = write(&id, &mut self.req) else {
            return false;
        };
        if len == 0 || len > exch::RECORD_MAX {
            return false;
        }
        self.n = n;
        self.id = id;
        self.req_len = len as u16;
        self.open = true;
        self.owed = true;
        self.placed = false;
        self.abort_owed = false;
        self.head = false;
        self.status = 0;
        self.body_len = 0;
        true
    }

    /// The request record owed to the provider, if one is and the provider
    /// is not down.
    fn outgoing(&self) -> Option<&[u8]> {
        if !self.open || !self.owed || self.down {
            return None;
        }
        self.req.get(..self.req_len as usize)
    }

    /// The owed request was placed.
    fn sent(&mut self) {
        self.owed = false;
        self.placed = true;
    }

    /// Stop waiting for the open call (the connector's own deadline passed):
    /// it ends here, and a provider that has the request is told with an
    /// ABORT, so it frees what it holds and answers nothing more.
    fn cancel(&mut self) {
        if !self.open {
            return;
        }
        self.open = false;
        self.owed = false;
        if self.placed {
            self.abort_owed = true;
            self.abort_reason = exch::abort::STALLED;
        }
    }

    /// An ABORT owed to the provider for a call given up on, laid into
    /// `out`; the length. Taken once.
    fn abort_record(&mut self, out: &mut [u8]) -> Option<usize> {
        if !self.abort_owed {
            return None;
        }
        let n = exch::write_abort(&self.id, self.abort_reason, out)?;
        self.abort_owed = false;
        Some(n)
    }

    fn collect(&mut self, data: &[u8]) -> bool {
        let at = self.body_len as usize;
        let Some(end) = at.checked_add(data.len()) else {
            return false;
        };
        let Some(dst) = self.body.get_mut(at..end) else {
            return false;
        };
        dst.copy_from_slice(data);
        self.body_len = end as u16;
        true
    }

    /// The answer was larger than a call takes: refused, and the provider
    /// told.
    fn refuse_answer(&mut self) -> CallEvent {
        self.open = false;
        self.abort_owed = true;
        self.abort_reason = exch::abort::TOO_LARGE;
        CallEvent::Failed(exch::status::BAD_GATEWAY)
    }

    /// Take one record from the provider.
    fn take(&mut self, record: &[u8]) -> CallEvent {
        let Some(rec) = exch::parse_response(record) else {
            return CallEvent::Nothing;
        };
        match rec {
            exch::Record::Link { state } => {
                if state == exch::link::DOWN {
                    self.down = true;
                    // Unknowable now: asked again, whole, after the UP.
                    if self.open {
                        self.owed = true;
                        self.head = false;
                        self.status = 0;
                        self.body_len = 0;
                    }
                    CallEvent::LinkDown
                } else if state == exch::link::UP {
                    self.down = false;
                    CallEvent::LinkUp
                } else {
                    CallEvent::Nothing
                }
            }
            exch::Record::Head(h) => {
                if !self.open || h.id != self.id || self.head {
                    return CallEvent::Nothing;
                }
                // An answer to the request as first sent settles it, even
                // while a re-issue is owed.
                self.owed = false;
                self.head = true;
                self.status = h.status;
                if !self.collect(h.body) {
                    return self.refuse_answer();
                }
                if h.flags & exch::flag::MORE == 0 {
                    self.open = false;
                    return CallEvent::Answered;
                }
                CallEvent::Nothing
            }
            exch::Record::Body { id, flags, data } => {
                if !self.open || id != self.id || !self.head {
                    return CallEvent::Nothing;
                }
                if !self.collect(data) {
                    return self.refuse_answer();
                }
                if flags & exch::flag::MORE == 0 {
                    self.open = false;
                    return CallEvent::Answered;
                }
                CallEvent::Nothing
            }
            exch::Record::Abort { id, .. } => {
                if !self.open || id != self.id {
                    return CallEvent::Nothing;
                }
                self.open = false;
                CallEvent::Failed(exch::status::BAD_GATEWAY)
            }
            exch::Record::Credit { .. } | exch::Record::Datagram { .. } => CallEvent::Nothing,
        }
    }

    /// The whole answer: its status and body.
    fn answer(&self) -> (u16, &[u8]) {
        (
            self.status,
            self.body.get(..self.body_len as usize).unwrap_or(&[]),
        )
    }
}

/// Lay a request HEAD that carries `body` whole, granting `CALL_BODY` of
/// response credit. The caller's `write` for [`Call::begin`].
fn call_head(
    id: &exch::ExchangeId,
    method: u8,
    target: &[u8],
    headers: &[u8],
    body: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    exch::write_request_head(
        &exch::RequestHead {
            id: *id,
            flags: 0,
            method,
            target,
            headers,
            peer: &[],
            resp_credit: CALL_BODY as u32,
            body,
        },
        out,
    )
}
