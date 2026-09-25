#!/usr/bin/env python3
"""Assemble a Chronicle decode program (`rd` bytecode) from a listing.

    rdasm.py <program.rd>      -> the program as hex on stdout

A decode program turns a raw protocol unit into a record (`pipeline` param
`decode`). Chronicle has no text syntax for these; this is one, kept to the
opcodes the VM defines (chronicle modules/common/deser_core.rs, vm_core.rs):

    int <width> [le|be]   read a binary integer at the cursor
    taken                 pop n, push the next n input bytes
    rest                  push the rest of the input
    set <field>           pop into the record at <field>
    store <slot> / load <slot>       locals 0..7
    str "<text>"          push a literal (\\n \\r \\t \\\\ \\" escapes)
    before|after|hdr "<arg>"         value readers (pop a value, push a part)
    part '<c>' <index>    the index-th <c>-separated part
    field <n>             pop a record frame, push its field n (absent -> null)
    partb '<c>' '<o>' '<e>' <index>  as part, but not splitting between <o> and <e>
    pctdec                URL-decode a query value (%XX, + as space)
    jsonat                pop a path, pop a document, push the value there (absent -> null)
    jsondef               pop raw, pop a path, pop a document, push it with the default
    cat <n>  hex  coalesce  gate  drop <n>
    finish
`#` starts a comment; `;` separates statements on a line.
"""
import re
import struct
import sys

OP = {
    "int": 0x75, "taken": 0x74, "rest": 0x78, "set": 0x40, "store": 0x54, "load": 0x55,
    "str": 0x12, "before": 0x7C, "after": 0x7D, "part": 0x7E, "hdr": 0x7F,
    "coalesce": 0x81, "cat": 0x82, "hex": 0x83, "drop": 0x84, "gate": 0x85, "finish": 0x41,
    "field": 0x86, "jsonat": 0x87, "jsondef": 0x88, "pctdec": 0x89, "partb": 0x8B,
}


def lit(tok):
    m = re.fullmatch(r'"((?:[^"\\]|\\.)*)"', tok)
    if not m:
        raise SystemExit(f"expected a quoted literal, got {tok!r}")
    return bytes(m.group(1), "utf-8").decode("unicode_escape").encode("latin-1")


def assemble(text):
    out = bytearray()
    for raw in text.splitlines():
        line = raw.split("#", 1)[0]
        for stmt in line.split(";"):
            toks = re.findall(r'"(?:[^"\\]|\\.)*"|\'.\'|\S+', stmt)
            if not toks:
                continue
            name, args = toks[0], toks[1:]
            if name not in OP:
                raise SystemExit(f"unknown op {name!r} in: {raw.strip()}")
            out.append(OP[name])
            if name == "int":
                out += bytes([int(args[0]), 1 if (args[1:] or ["le"])[0] == "le" else 0])
            elif name == "set":
                out += struct.pack("<I", int(args[0]))
            elif name in ("store", "load", "cat", "field"):
                out.append(int(args[0]))
            elif name == "drop":
                out += struct.pack("<H", int(args[0]))
            elif name in ("str", "before", "after", "hdr"):
                b = lit(args[0])
                out += struct.pack("<H", len(b)) + b
            elif name == "part":
                out += bytes([ord(args[0][1]), int(args[1])])
            elif name == "partb":
                out += bytes([ord(args[0][1]), ord(args[1][1]), ord(args[2][1]), int(args[3])])
    return bytes(out)


print(assemble(open(sys.argv[1]).read()).hex())
