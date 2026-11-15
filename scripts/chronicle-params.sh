# Compile a .plan entry to its decision param, at TEST TIME.
# Sourced, not run.
#
# Compiling here rather than carrying the hex inline in each E2E is what keeps
# the two from drifting: an edit to the .plan would leave a baked param stale,
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
#
# The applet's stdout can come back CUT SHORT with exit status 0 on a loaded
# host: Fluxor's `cli_out` sink retires once the exit code is latched and its
# 8 KiB bridge reads empty, without asking whether the channel feeding it
# still holds bytes — a graph larger than the bridge loses its tail (seen as
# exactly 7168 of 12367 bytes, 4 runs in 2500 under contention). A cut param
# never matches the closing `"}`, so the compile is asked again, and said so
# on stderr, a bounded number of times: a source that does not compile fails
# every attempt and still answers nothing. Remove the retry when Fluxor's sink
# waits for its input channel to drain.
NC_COMPILE_TRIES="${NC_COMPILE_TRIES:-6}"
_nc_graph_field() { # _nc_graph_field <plan-path> <entry> <field>
  local hex out try=1
  hex="$(python3 -c "import sys;print(open(sys.argv[1],'rb').read().hex())" "$1")"
  while :; do
    out="$(fluxor exec chronicle -- graph "$hex" "$2" linux 2>/dev/null)" || true
    if [[ "$out" =~ $3:\ \{hex:\ \"([0-9a-f]+)\"\} ]]; then
      printf '%s' "${BASH_REMATCH[1]}"
      return 0
    fi
    # A refusal (`error: ...`, no graph at all) and a whole graph that simply
    # carries no such field are answers; only a graph begun and not finished
    # is a cut.
    if [[ "$out" != "target: "* ]] || [[ "$out" == *$'\nwiring:\n'*"to: cli_out.bytes_in" ]]; then
      return 0
    fi
    [ "$try" -ge "$NC_COMPILE_TRIES" ] && return 0
    echo "   note: chronicle answered ${#out} byte(s) for $(basename "$1") $2 with no $3 param — asking again ($try/$NC_COMPILE_TRIES; a cut applet stdout)" >&2
    try=$((try + 1))
  done
}

nc_decision() { # nc_decision <plan-path> <entry>
  _nc_graph_field "$1" "$2" decision
}

# The same, for a pipeline that lowers to an `ir_stages` container rather than a
# decision — a codec-only stage, such as the one that rebuilds a record for an
# encoder. Baking these by hand is how a record grows a field the encoder never
# sees: the stage program copies a FIXED list, and the symptom is an envelope
# byte that silently stays zero.
nc_stages() { # nc_stages <plan-path> <entry>
  _nc_graph_field "$1" "$2" ir_stages
}

# The stage kinds that go with `nc_stages`' container, when the pipeline holds
# a map stage (empty otherwise: a container alone is all compute). A node that
# carries map stages is wrong without them — the container would run each
# map's header as bytecode — so they are baked beside `ir_stages`, from the
# same compile.
nc_stage_kinds() { # nc_stage_kinds <plan-path> <entry>
  _nc_graph_field "$1" "$2" stage_kinds
}
