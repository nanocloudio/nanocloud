// Streaming JSON path extraction for nanocloud fmods — included via
// `include!("../_shared/jsonstream.rs")`. No allocator, fixed memory.
//
// `json.rs` reads a document that is WHOLE in a buffer. A stored object is not
// bounded by any buffer a connector can hold (a ConfigMap is up to 1 MiB), so a
// projection over it reads the object in chunks and keeps only what it was
// asked for: the value at each requested dotted path, captured as it streams
// past. Memory is a path stack (`JS_DEPTH` segments) and one capture buffer per
// requested path; the document itself is never held.
//
// Semantics match `j_path`/`j_sub`: keys compared as written, array elements by
// decimal index, a string's content without its quotes (escapes kept), any
// other value as written. A value longer than its capture buffer is TRUNCATED
// and reported so — a projection built from a cut value would be a lie, so the
// caller fails closed on it.

/// Paths one scan can extract.
const JS_WANT: usize = 12;
/// Nesting whose path SEGMENTS are kept: a path names at most this many. The
/// structure itself is followed far deeper (`JS_MAX_DEPTH`), so a capture above
/// a deep subtree still closes where it should.
const JS_DEPTH: usize = 12;
/// Nesting followed at all (a bit per level, array or object). A document
/// nested deeper is refused: the scan stops and what was not seen whole is
/// absent.
const JS_MAX_DEPTH: usize = 128;
/// A key longer than this is compared by its first `JS_KEY` bytes only — and
/// then never matches (a truncated key is not the key asked for).
const JS_KEY: usize = 64;
/// One captured value.
const JS_CAP: usize = 1024;

#[derive(Clone, Copy, PartialEq)]
enum JsSt {
    /// Expecting a value (after `:` or `,` in an array, or at the top).
    Value,
    /// In an object, expecting a key or `}`.
    KeyOrEnd,
    /// In an object after `,`: a key must follow.
    Key,
    /// Reading a key's characters.
    InKey,
    /// After a key: `:` must follow.
    Colon,
    /// Reading a string value.
    InStr,
    /// Reading a number/true/false/null.
    Scalar,
    /// After a value: `,` or the container's close.
    After,
    /// The document ended (or was malformed): nothing more is read.
    Done,
}

#[derive(Clone, Copy)]
struct JsFrame {
    /// `{` or `[`.
    kind: u8,
    /// The segment this frame's CURRENT child is at: its key, or its index.
    key: [u8; JS_KEY],
    key_len: u8,
    key_cut: bool,
    index: u32,
}

const JS_FRAME_EMPTY: JsFrame = JsFrame {
    kind: 0,
    key: [0; JS_KEY],
    key_len: 0,
    key_cut: false,
    index: 0,
};

/// One requested path's capture.
#[derive(Clone, Copy)]
struct JsCap {
    buf: [u8; JS_CAP],
    len: u16,
    /// 0 not seen, 1 capturing, 2 captured, 3 truncated.
    state: u8,
    /// The stack depth the captured value sits at.
    depth: u8,
}

const JS_CAP_EMPTY: JsCap = JsCap {
    buf: [0; JS_CAP],
    len: 0,
    state: 0,
    depth: 0,
};

struct JsonStream<'w> {
    want: [&'w [u8]; JS_WANT],
    nwant: usize,
    stack: [JsFrame; JS_DEPTH],
    /// Frames open; frames past `JS_DEPTH` are counted, not stored.
    depth: usize,
    /// Bit `d` set: the container at depth `d` is an array.
    arrays: u128,
    st: JsSt,
    esc: bool,
    caps: [JsCap; JS_WANT],
}

impl<'w> JsonStream<'w> {
    /// A scan for `paths` (dotted; at most `JS_WANT`, the rest ignored).
    fn new(paths: &[&'w [u8]]) -> Self {
        let mut want: [&'w [u8]; JS_WANT] = [&[]; JS_WANT];
        let mut nwant = 0usize;
        for p in paths.iter().take(JS_WANT) {
            want[nwant] = p;
            nwant += 1;
        }
        JsonStream {
            want,
            nwant,
            stack: [JS_FRAME_EMPTY; JS_DEPTH],
            depth: 0,
            arrays: 0,
            st: JsSt::Value,
            esc: false,
            caps: [JS_CAP_EMPTY; JS_WANT],
        }
    }

    /// The captured value of path `i`: `Some` once seen whole, `None` when
    /// absent (or truncated — see [`JsonStream::truncated`]).
    fn get(&self, i: usize) -> Option<&[u8]> {
        let c = self.caps.get(i)?;
        if c.state != 2 {
            return None;
        }
        let v = c.buf.get(..c.len as usize)?;
        // A string's content without its quotes, as `j_path` answers.
        match v {
            [b'"', inner @ .., b'"'] => Some(inner),
            _ => Some(v),
        }
    }

    /// Was path `i`'s value longer than a capture holds?
    fn truncated(&self, i: usize) -> bool {
        self.caps.get(i).is_some_and(|c| c.state == 3)
    }

    /// Does the current position (the stack's segments) name `path`?
    fn at(&self, path: &[u8]) -> bool {
        if self.depth > JS_DEPTH {
            return false;
        }
        let mut segs = path.split(|&c| c == b'.').filter(|s| !s.is_empty());
        for f in self.stack.iter().take(self.depth) {
            let Some(seg) = segs.next() else {
                return false;
            };
            let ok = if f.kind == b'{' {
                !f.key_cut && f.key.get(..f.key_len as usize) == Some(seg)
            } else {
                parse_index(seg) == Some(f.index)
            };
            if !ok {
                return false;
            }
        }
        segs.next().is_none()
    }

    /// A value begins here: start capturing it for every path that names it.
    fn value_starts(&mut self) {
        for i in 0..self.nwant {
            if self.caps[i].state == 0 && self.at(self.want[i]) {
                self.caps[i].state = 1;
                self.caps[i].depth = self.depth as u8;
            }
        }
    }

    /// `c` belongs to every capture in progress.
    fn capture(&mut self, c: u8) {
        for cap in self.caps.iter_mut().take(self.nwant) {
            if cap.state == 1 {
                match cap.buf.get_mut(cap.len as usize) {
                    Some(d) => {
                        *d = c;
                        cap.len += 1;
                    }
                    None => cap.state = 3,
                }
            }
        }
    }

    /// The value at the current depth ended: close its captures.
    fn value_ends(&mut self) {
        for cap in self.caps.iter_mut().take(self.nwant) {
            if cap.state == 1 && cap.depth as usize == self.depth {
                cap.state = 2;
            }
        }
    }

    fn push(&mut self, kind: u8) {
        if self.depth >= JS_MAX_DEPTH {
            self.st = JsSt::Done;
            return;
        }
        if self.depth < JS_DEPTH {
            self.stack[self.depth] = JsFrame {
                kind,
                ..JS_FRAME_EMPTY
            };
        }
        let bit = 1u128 << self.depth;
        if kind == b'[' {
            self.arrays |= bit;
        } else {
            self.arrays &= !bit;
        }
        self.depth += 1;
    }

    /// Feed the next chunk of the document.
    fn feed(&mut self, chunk: &[u8]) {
        for &c in chunk {
            self.byte(c);
        }
    }

    fn byte(&mut self, c: u8) {
        let ws = matches!(c, b' ' | b'\t' | b'\n' | b'\r');
        match self.st {
            JsSt::Done => {}
            JsSt::Value => {
                if ws {
                    self.capture(c);
                    return;
                }
                // `[]`: the close of an empty array, not an element.
                if c == b']' && self.top_kind() == Some(b'[') {
                    self.capture(c);
                    self.close(b'[');
                    return;
                }
                self.value_starts();
                self.capture(c);
                match c {
                    b'{' => {
                        self.push(b'{');
                        if self.st != JsSt::Done {
                            self.st = JsSt::KeyOrEnd;
                        }
                    }
                    b'[' => {
                        self.push(b'[');
                        if self.st != JsSt::Done {
                            self.st = JsSt::Value;
                        }
                    }
                    b'"' => {
                        self.st = JsSt::InStr;
                        self.esc = false;
                    }
                    _ => self.st = JsSt::Scalar,
                }
            }
            JsSt::KeyOrEnd | JsSt::Key => {
                if ws {
                    self.capture(c);
                    return;
                }
                self.capture(c);
                match c {
                    b'"' => {
                        if let Some(f) = self.top() {
                            f.key_len = 0;
                            f.key_cut = false;
                        }
                        self.st = JsSt::InKey;
                        self.esc = false;
                    }
                    b'}' if self.st == JsSt::KeyOrEnd => self.close(b'{'),
                    _ => self.st = JsSt::Done,
                }
            }
            JsSt::InKey => {
                self.capture(c);
                if self.esc {
                    self.esc = false;
                } else if c == b'\\' {
                    self.esc = true;
                } else if c == b'"' {
                    self.st = JsSt::Colon;
                    return;
                }
                if let Some(f) = self.top() {
                    match f.key.get_mut(f.key_len as usize) {
                        Some(d) => {
                            *d = c;
                            f.key_len += 1;
                        }
                        None => f.key_cut = true,
                    }
                }
            }
            JsSt::Colon => {
                self.capture(c);
                if ws {
                    return;
                }
                self.st = if c == b':' { JsSt::Value } else { JsSt::Done };
            }
            JsSt::InStr => {
                self.capture(c);
                if self.esc {
                    self.esc = false;
                } else if c == b'\\' {
                    self.esc = true;
                } else if c == b'"' {
                    self.value_ends();
                    self.st = JsSt::After;
                }
            }
            JsSt::Scalar => {
                if ws || matches!(c, b',' | b'}' | b']') {
                    self.value_ends();
                    self.st = JsSt::After;
                    self.byte(c);
                } else {
                    self.capture(c);
                }
            }
            JsSt::After => {
                self.capture(c);
                if ws {
                    return;
                }
                match (c, self.top_kind()) {
                    (b',', Some(b'{')) => self.st = JsSt::Key,
                    (b',', Some(b'[')) => {
                        if let Some(f) = self.top() {
                            f.index = f.index.wrapping_add(1);
                        }
                        self.st = JsSt::Value;
                    }
                    (b'}', Some(b'{')) => self.close(b'{'),
                    (b']', Some(b'[')) => self.close(b'['),
                    _ => self.st = JsSt::Done,
                }
            }
        }
    }

    /// The container of `kind` closes: the value it was ends.
    fn close(&mut self, kind: u8) {
        if self.top_kind() != Some(kind) {
            self.st = JsSt::Done;
            return;
        }
        self.depth -= 1;
        self.value_ends();
        self.st = if self.depth == 0 {
            JsSt::Done
        } else {
            JsSt::After
        };
    }

    fn top(&mut self) -> Option<&mut JsFrame> {
        if self.depth == 0 || self.depth > JS_DEPTH {
            return None;
        }
        self.stack.get_mut(self.depth - 1)
    }

    fn top_kind(&self) -> Option<u8> {
        if self.depth == 0 {
            return None;
        }
        Some(if self.arrays & (1u128 << (self.depth - 1)) != 0 {
            b'['
        } else {
            b'{'
        })
    }
}

/// A decimal array index; `None` for anything else.
fn parse_index(seg: &[u8]) -> Option<u32> {
    if seg.is_empty() || seg.len() > 9 {
        return None;
    }
    let mut v = 0u32;
    for &c in seg {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u32;
    }
    Some(v)
}
