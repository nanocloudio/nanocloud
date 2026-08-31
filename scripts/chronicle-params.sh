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
nc_decision() { # nc_decision <uproc-path> <entry>
  local hex
  hex="$(python3 -c "import sys;print(open(sys.argv[1],'rb').read().hex())" "$1")"
  fluxor exec chronicle -- graph "$hex" "$2" linux 2>/dev/null \
    | grep -oP '(?<=decision: ")[0-9a-f]+' | head -1 | tr -d '\n'
}

# The same, for a pipeline that lowers to an `ir_stages` container rather than a
# decision — a codec-only stage, such as the one that rebuilds a record for an
# encoder. Baking these by hand is how a record grows a field the encoder never
# sees: the stage program copies a FIXED list, and the symptom is an envelope
# byte that silently stays zero.
nc_stages() { # nc_stages <uproc-path> <entry>
  local hex
  hex="$(python3 -c "import sys;print(open(sys.argv[1],'rb').read().hex())" "$1")"
  fluxor exec chronicle -- graph "$hex" "$2" linux 2>/dev/null \
    | grep -oP '(?<=ir_stages: ")[0-9a-f]+' | head -1 | tr -d '\n'
}
