#!/usr/bin/env bash
# The HPA's stabilisation window as an aggregation `ir_def` (hex on stdout):
#   hpa-agg.sh <window_ms> <step_ms>
#
# Over hpa.uproc's recommendations (hp.Rec {1 key, 2 t, 3 desired}): one lane
# per <ns>/<target>, event time `t`, a sliding window, operators
# max(desired) and max(t), a Continuous trigger (every recommendation emits the
# windows it touched), and the emit hpa.uproc's `apply` reads (hp.Stab {1 key,
# 2 desired, 3 tmax, 4 wend}). Compiled from source on every run, like the
# decision params (chronicle-params.sh), so it never drifts from them.
set -euo pipefail
WIN="${1:?window ms}" STEP="${2:?step ms}"
SCHEMA='Rec{key:str@1,t:int@2,desired:int@3};Ctx{key:str@1,state:State@2,window:Win@3};State{mx:int@1,tmax:int@2};Win{start:int@1,end:int@2};Stab{key:str@1,desired:int@2,tmax:int@3,wend:int@4};'
ir() { fluxor exec chronicle -- compile-source "$SCHEMA" "$1" "$2" "$3" 2>/dev/null | head -1; }
KEY="$(ir r Rec 'r.key')"
TIME="$(ir r Rec 'r.t')"
DES="$(ir r Rec 'r.desired')"
EMIT="$(ir ctx Ctx 'Stab{key: ctx.key, desired: ctx.state.mx, tmax: ctx.state.tmax, wend: ctx.window.end}')"
for v in "$KEY" "$TIME" "$DES" "$EMIT"; do
  case "$v" in ''|error*) echo "hpa-agg: a program did not compile" >&2; exit 1 ;; esac
done
# agg <window> <lateness> <lanes> <step> <horizon> <key> <time> <emit> <kind>:<sel>…
# kind 3 = max. Lateness 0: recommendations arrive in time order.
DEF="$(fluxor exec chronicle -- agg "$WIN" 0 64 "$STEP" 0 "$KEY" "$TIME" "$EMIT" "3:$DES" "3:$TIME" 2>/dev/null | head -1)"
case "$DEF" in ''|error*) echo "hpa-agg: agg did not assemble" >&2; exit 1 ;; esac
# Trailing trigger bytes: 01 = Continuous.
printf '%s01\n' "$DEF"
