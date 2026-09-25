// An argv in a flat record: whitespace-separated tokens, each with `%XX`
// standing for a byte it cannot carry raw. Written by the projection
// modifiers (`pathmod.rs`'s `!argv`, `!jargv`) and read by whatever runs it
// (`workload_effect`'s spawn) — one encoding, defined here once, `include!`d
// by both.

/// An argv rendered for a flat record: tokens separated by one space, each
/// byte a token cannot carry raw — space, tab, newline, `;`, `,` and `%` —
/// written as `%XX`, so a token with spaces in it (`sh -c "a; b"`) keeps its
/// bounds through the record and the reader splits on whitespace and
/// unescapes each token. Writes nothing past `out`; [`ArgvOut::len`] is 0
/// when the argv did not fit, so a truncated command is never rendered.
struct ArgvOut<'a> {
    out: &'a mut [u8],
    o: usize,
    /// Bytes written to the current token.
    tok: usize,
    full: bool,
}

impl<'a> ArgvOut<'a> {
    fn new(out: &'a mut [u8]) -> Self {
        ArgvOut {
            out,
            o: 0,
            tok: 0,
            full: false,
        }
    }

    fn put(&mut self, b: u8) {
        match self.out.get_mut(self.o) {
            Some(slot) => {
                *slot = b;
                self.o += 1;
            }
            None => self.full = true,
        }
    }

    fn byte(&mut self, b: u8) {
        if self.tok == 0 && self.o > 0 {
            self.put(b' ');
        }
        self.tok += 1;
        if matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b';' | b',' | b'%') {
            let hexd = b"0123456789ABCDEF";
            self.put(b'%');
            self.put(hexd[(b >> 4) as usize]);
            self.put(hexd[(b & 0xf) as usize]);
        } else {
            self.put(b);
        }
    }

    /// End the current token. An empty token cannot be carried by a
    /// whitespace-split argv; it is dropped.
    fn next_token(&mut self) {
        self.tok = 0;
    }

    fn len(&self) -> usize {
        if self.full {
            0
        } else {
            self.o
        }
    }
}

/// A hex digit's value.
fn argv_hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decode one token of a record's argv into `out`: `%XX` is the byte it
/// names, anything else itself. `None` when `out` cannot hold it.
fn argv_decode(tok: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut o = 0usize;
    let mut i = 0usize;
    while i < tok.len() {
        let (byte, step) = match (tok[i], tok.get(i + 1), tok.get(i + 2)) {
            (b'%', Some(&h), Some(&l)) => match (argv_hex(h), argv_hex(l)) {
                (Some(h), Some(l)) => ((h << 4) | l, 3),
                _ => (b'%', 1),
            },
            (c, _, _) => (c, 1),
        };
        *out.get_mut(o)? = byte;
        o += 1;
        i += step;
    }
    Some(o)
}

/// A record's argv split into its (still encoded) tokens.
fn argv_tokens(cmd: &[u8]) -> impl Iterator<Item = &[u8]> {
    cmd.split(|&c| c == b' ' || c == b'\t' || c == b'\n')
        .filter(|t| !t.is_empty())
}
