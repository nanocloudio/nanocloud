"""A test driver's end of fluxor's `remote_channel` (net carrier).

A graph under test takes its driver's records on a `remote_channel` that
listens on linux_net with no peer authentication and byte-stream channels:

  - name: requests
    type: remote_channel
    transport: net
    role: listen
    port: <port>
    require_peer: 0
    channels: "OctetStream:bytes:16384"        (one entry per channel)

wired `linux_net.net_out -> requests.transport_in`,
`requests.transport_out -> linux_net.net_in`, `requests.ch<i>_rx -> <a
module's input>` and `<the module's output> -> requests.ch<i>`.

`connect(port)` dials it, exchanges the channel table, and returns channel 0
as a socket-like object: `sendall(data)` carries bytes on the channel,
`recv(n)` returns what arrived on it. `connect(port, channels=n)` returns the
session's n channels as a list. The wire (fluxor `remote_channel/wire.rs`):
`[kind:1][channel:1][len:2 LE][body]`; HELLO carries the table, CREDIT the
room a receiver frees, BEGIN `[record_len:4 LE][bytes]` and MORE a record.
Each side starts with credit for one whole record and its 4-byte charge, per
channel.
"""
import socket
import struct
import time

HELLO, REFUSE, CREDIT, BEGIN, MORE = 1, 2, 3, 4, 5
CONTROL = 0xFF
CHARGE = 4
FRAME_BODY_MAX = 0xFFFF


class Session:
    def __init__(self, sock, content_type, max_record, channels):
        self.s = sock
        self.max = max_record
        self.inbuf = bytearray()
        self.chans = [Channel(self, i) for i in range(channels)]
        ct = content_type.encode()
        entry = bytes([len(ct)]) + ct + struct.pack("<I", max_record)
        table = b"FXRC" + bytes([channels]) + entry * channels
        self.frame(HELLO, CONTROL, table)
        while True:
            kind, _, body = self.read_frame()
            if kind == HELLO:
                if body != table:
                    raise ConnectionError("the graph's channel table differs: %r" % body)
                return
            if kind == REFUSE:
                raise ConnectionError("the graph refused the session: reason %d" % body[0])

    def frame(self, kind, channel, body):
        self.s.sendall(struct.pack("<BBH", kind, channel, len(body)) + body)

    def fill(self, n):
        while len(self.inbuf) < n:
            chunk = self.s.recv(65536)
            if not chunk:
                raise ConnectionError("the graph closed the session")
            self.inbuf += chunk

    def read_frame(self):
        self.fill(4)
        kind, channel, n = struct.unpack("<BBH", self.inbuf[:4])
        self.fill(4 + n)
        body = bytes(self.inbuf[4:4 + n])
        del self.inbuf[:4 + n]
        return kind, channel, body

    def pump(self):
        """Take one frame from the graph."""
        kind, ch, body = self.read_frame()
        if kind == REFUSE:
            raise ConnectionError("the graph refused the session: reason %d" % body[0])
        if ch >= len(self.chans):
            raise ConnectionError("a frame for channel %d, which the table has not" % ch)
        c = self.chans[ch]
        if kind == CREDIT:
            c.credit += struct.unpack("<I", body)[0]
        elif kind == BEGIN:
            c.rec_len = struct.unpack("<I", body[:4])[0]
            c.rec_left = c.rec_len - (len(body) - 4)
            c.rx += body[4:]
            c.record_done()
        elif kind == MORE:
            c.rec_left -= len(body)
            c.rx += body
            c.record_done()
        else:
            raise ConnectionError("unexpected frame kind %d" % kind)


class Channel:
    def __init__(self, session, index):
        self.session = session
        self.index = index
        self.credit = session.max + CHARGE
        self.rx = bytearray()
        self.rec_len = 0
        self.rec_left = 0

    def record_done(self):
        if self.rec_left == 0:
            # The record left the graph's view: its room is ours to return.
            self.session.frame(CREDIT, self.index, struct.pack("<I", self.rec_len + CHARGE))

    def sendall(self, data):
        data = bytes(data)
        se = self.session
        while data:
            piece = data[: se.max]
            while self.credit < len(piece) + CHARGE:
                se.pump()
            self.credit -= len(piece) + CHARGE
            first = piece[: FRAME_BODY_MAX - 4]
            se.frame(BEGIN, self.index, struct.pack("<I", len(piece)) + first)
            rest = piece[len(first):]
            while rest:
                se.frame(MORE, self.index, rest[:FRAME_BODY_MAX])
                rest = rest[FRAME_BODY_MAX:]
            data = data[len(piece):]

    def recv(self, n):
        while not self.rx:
            self.session.pump()
        out = bytes(self.rx[:n])
        del self.rx[:n]
        return out

    def settimeout(self, t):
        self.session.s.settimeout(t)

    def close(self):
        self.session.s.close()


def connect(port, host="127.0.0.1", content_type="OctetStream", max_record=16384,
            timeout=30, attempts=150, channels=None):
    """Dial a listening remote_channel and open its session; retries while
    the graph comes up. Channel 0, or with `channels` that many as a list."""
    last = None
    for _ in range(attempts):
        try:
            s = socket.create_connection((host, port), timeout=timeout)
        except OSError as e:
            last = e
            time.sleep(0.2)
            continue
        try:
            se = Session(s, content_type, max_record, channels or 1)
            return se.chans if channels else se.chans[0]
        except (ConnectionError, OSError) as e:
            # A listener still binding, or a session it closed: dial again.
            last = e
            s.close()
            time.sleep(0.2)
    raise ConnectionError("no session with the graph's remote_channel on %s:%d: %s" % (host, port, last))
