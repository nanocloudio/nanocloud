# Compile a .uproc entry to its decision param, at TEST TIME.
# Sourced, not run.
#
# Compiling here rather than carrying the hex inline in each E2E is what keeps
# the two from drifting: an edit to the .uproc would leave a baked param stale,
# and a stale decision does not error — it decides something else. Two key
# parts moving between fields is enough to leave a sweep probing
# `/replicasets.apps/default` instead of `/replicasets.apps/default/web`, so
# every owner looks absent and a live-owned pod is collected — caught only by
# an assertion specific enough to notice. Compiling here means the param is
# always the source's.
# The first `<field>: {hex: "<hex>"}` of `<entry>`'s compiled graph — a program
# param is a `blob`, written as its inline hex entry — or nothing when it
# does not compile. The whole document is read before it is searched: a reader
# that stopped at the first match would leave the compiler writing to a closed
# pipe, which under `pipefail` fails the caller however the match went.
_nc_graph_field() { # _nc_graph_field <uproc-path> <entry> <field>
  local hex out
  hex="$(python3 -c "import sys;print(open(sys.argv[1],'rb').read().hex())" "$1")"
  out="$(fluxor exec chronicle -- graph "$hex" "$2" linux 2>/dev/null)" || true
  [[ "$out" =~ $3:\ \{hex:\ \"([0-9a-f]+)\"\} ]] && printf '%s' "${BASH_REMATCH[1]}"
  return 0
}

nc_decision() { # nc_decision <uproc-path> <entry>
  _nc_graph_field "$1" "$2" decision
}

# The same, for a pipeline that lowers to an `ir_stages` container rather than a
# decision — a codec-only stage, such as the one that rebuilds a record for an
# encoder. Baking these by hand is how a record grows a field the encoder never
# sees: the stage program copies a FIXED list, and the symptom is an envelope
# byte that silently stays zero.
nc_stages() { # nc_stages <uproc-path> <entry>
  _nc_graph_field "$1" "$2" ir_stages
}

# The stage kinds that go with `nc_stages`' container, when the pipeline holds
# a map stage (empty otherwise: a container alone is all compute). A node that
# carries map stages is wrong without them — the container would run each
# map's header as bytecode — so they are baked beside `ir_stages`, from the
# same compile.
nc_stage_kinds() { # nc_stage_kinds <uproc-path> <entry>
  _nc_graph_field "$1" "$2" stage_kinds
}
