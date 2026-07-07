#!/usr/bin/env bash
# devimint smoke test for the ACTIVE PROBE — the Phase 5.0 exit gate
# (docs/phase5-plan.md §5.0.8). A probe is a two-leg exact-net round trip on the ordinary
# move machinery: mint on the candidate (S -> C), then redeem back (C -> S), proving
# REDEEMABILITY. This gate drives it LIVE on the two-fed harness and asserts: the round
# trip nets its own delta then drains back (combined wallet loss = fees only, never a
# sweep); every leg + the umbrella row are auditable in `history` with the active_probe
# reason; a sustained window of qualifying successes flips the probe verb's OWN verdict to
# `passed` while `status` (default policy) stays conservative; and a candidate-scoped
# failure (an unjoined fed) is a NoAttempt that exits non-zero and never demotes.
#
# NOT part of the rb-lite gate (needs a LIVE two-fed devimint; run by hand). Same harness
# as smoke_history/smoke_evacuate: docs/devimint-two-fed-harness.patch supplies
# $FED_B_INVITE and connects/pegs the shared LDK gateway. REBUILD wallet-cli into this
# repo's target-nix BEFORE running (the fedimint devshell redirects cargo's target dir —
# docs/devimint-runbook.md):
#   CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix \
#     nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
set -euo pipefail

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found at $WALLET_CLI — build it first:" >&2
  echo "  CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi

WCLI_DIR=$(mktemp -d)
GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
FED_B_INV="${FED_B_INVITE:-${FM_INVITE_CODE_B:?two-fed harness did not export FED_B_INVITE}}"
wcli() { "$WALLET_CLI" --data-dir "$WCLI_DIR" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# Per-fed spendable (msat). NOTE: no `exit` in the awk (SIGPIPE-safe with multiple feds).
bal_for() { wcli balance | awk -v f="$1" '$1 == f":" {print $2}'; }

# TSV columns (§11): 1 seq 2 updated_at 3 kind 4 status 5 amount 6 recv_fee 7 send_fee
#                    8 actor 9 reason 10 key.
col() { echo "$1" | awk -F'\t' -v c="$2" '{print $c}'; }

LEG_FEE_CAP=10000   # PROBE_LEG_FEE_CAP_MSAT default
PROBE_AMOUNT=20000  # PROBE_AMOUNT_MSAT default

command -v gateway-ldk >/dev/null && gateway-ldk connect-fed "$FED_B_INV" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------------------
echo "== JOIN both federations; FUND fed A (the spending source) =="
FED_A=$(wcli join "$FM_INVITE_CODE")
FED_B=$(wcli join "$FED_B_INV")
echo "A=$FED_A (source)  B=$FED_B (candidate)"

DI_ERR=$(mktemp)
INV_A=$(wcli direct-inflow --to "$FED_A" --amount 500000 --gateway "$GW" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^intent_key: //p' "$DI_ERR")
[[ -n "$INV_A" && -n "$KEY_FUND" ]] || { cat "$DI_ERR" >&2; fail "direct-inflow gave no invoice/key"; }
SEND1=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND1" >/dev/null 2>&1 || true
[[ "$(wcli await-move "$KEY_FUND")" == "done" ]] || fail "funding direct-inflow did not settle"

# ---------------------------------------------------------------------------------------
echo "== PROBE B from A: a live round trip — mint on B, redeem back to A =="
A0=$(bal_for "$FED_A"); B0=$(bal_for "$FED_B")
[[ "$A0" =~ ^[0-9]+$ ]] || { wcli balance >&2; fail "could not parse fed A balance"; }
[[ "$B0" =~ ^[0-9]+$ ]] || { wcli balance >&2; fail "could not parse fed B balance"; }
(( B0 == 0 )) || fail "candidate B should start EMPTY (use a fresh --data-dir); holds $B0"
echo "pre-probe: A=$A0 msat  B=$B0 msat"

P_OUT=$(mktemp); P_ERR=$(mktemp)
if ! wcli probe "$FED_B" --from "$FED_A" --gateway "$GW" >"$P_OUT" 2>"$P_ERR"; then
  echo "  --- probe stdout ---" >&2; cat "$P_OUT" >&2
  echo "  --- probe stderr ---" >&2; cat "$P_ERR" >&2
  fail "a healthy probe B from A must succeed (exit 0)"
fi
grep -q '^attempt: ok$' "$P_OUT" || { cat "$P_OUT" >&2; fail "probe did not report 'attempt: ok'"; }
IN_KEY=$(sed -n 's/^in_key: //p' "$P_ERR")
OUT_KEY=$(sed -n 's/^out_key: //p' "$P_ERR")
[[ -n "$IN_KEY" && -n "$OUT_KEY" ]] || { cat "$P_ERR" >&2; fail "probe printed no in_key/out_key"; }

A1=$(bal_for "$FED_A"); B1=$(bal_for "$FED_B")
echo "post-probe: A=$A1 msat  B=$B1 msat"
# NEVER-OVER + NO-SWEEP: B ends at a small residue (< the out leg fee cap), and the COMBINED
# S+C wallet total falls by fees only (bounded by 2 legs' fee caps). B started empty, so the
# residue IS pre-existing-fund-free by construction.
(( B1 < LEG_FEE_CAP )) || fail "candidate residue $B1 msat is not below the out leg fee cap $LEG_FEE_CAP"
COMBINED_BEFORE=$(( A0 + B0 )); COMBINED_AFTER=$(( A1 + B1 ))
LOSS=$(( COMBINED_BEFORE - COMBINED_AFTER ))
(( LOSS >= 0 )) || fail "combined wallet total ROSE ($COMBINED_BEFORE -> $COMBINED_AFTER) — impossible"
(( LOSS <= 2 * LEG_FEE_CAP )) || fail "combined loss $LOSS msat exceeds 2x leg fee cap (fees only expected)"
echo "round trip OK: B residue $B1 msat (< $LEG_FEE_CAP), combined loss $LOSS msat (fees only, <= $((2*LEG_FEE_CAP)))"

# ---------------------------------------------------------------------------------------
echo "== HISTORY: the umbrella probe row + both legs, all reason=active_probe =="
wcli history --limit 50 | sed 's/^/  | /'
PROBE_ROWS=$(wcli history --limit 50 | awk -F'\t' '$3 == "probe" && $4 == "succeeded" && $8 == "user" && $9 == "active_probe"' | wc -l)
(( PROBE_ROWS >= 1 )) || fail "no succeeded user probe umbrella row with reason active_probe"
for k in "$IN_KEY" "$OUT_KEY"; do
  ROW=$(wcli history --limit 50 | awk -F'\t' -v key="$k" '$10 == key' | head -1)
  [[ -n "$ROW" ]] || fail "no history row for probe leg $k"
  [[ "$(col "$ROW" 3)" == "move" ]] || fail "probe leg $k kind: $(col "$ROW" 3) (expected move)"
  [[ "$(col "$ROW" 9)" == "active_probe" ]] || fail "probe leg $k reason: $(col "$ROW" 9)"
done
echo "history OK: umbrella probe row + IN/OUT legs, all active_probe"

# ---------------------------------------------------------------------------------------
echo "== SUSTAINED WINDOW: 2 more probes -> the verb's verdict flips to 'passed' =="
# The verb evaluates verdict_after under ITS OWN (shrunk-span) policy; status uses DEFAULT.
sleep 1
wcli probe "$FED_B" --from "$FED_A" --gateway "$GW" --min-span-secs 1 >/dev/null 2>&1 || fail "2nd probe failed"
sleep 1
P3_OUT=$(mktemp)
wcli probe "$FED_B" --from "$FED_A" --gateway "$GW" --min-span-secs 1 >"$P3_OUT" 2>/dev/null || fail "3rd probe failed"
grep -q '^verdict: passed$' "$P3_OUT" || { cat "$P3_OUT" >&2; fail "3 qualifying successes over a 1s span did not read 'passed'"; }
echo "verdict OK: the probe verb reads 'passed' after 3 successes spanning its shrunk window"

# status uses the DEFAULT policy (24h span): the same 3-successes-in-seconds history is
# NOT yet a sustained pass. B's row must read insufficient (conservative production gate).
ST_OUT=$(mktemp)
wcli status --spending "$FED_A" --standby "$FED_B" \
  --spending-target 0 --standby-target 0 --per-fed-cap 400000 --max-fee 1000000 \
  --gateway "$GW" --occurrence 0 >"$ST_OUT" 2>/dev/null || fail "status exited non-zero"
if ! grep -E "^${FED_B} .*active_probe=insufficient" "$ST_OUT" >/dev/null; then
  echo "  --- status ---" >&2; cat "$ST_OUT" >&2
  fail "status (default policy) should report B active_probe=insufficient, not a pass"
fi
echo "status OK: default-policy verdict stays 'insufficient' under the test-shrunk window"

# ---------------------------------------------------------------------------------------
echo "== NO-ATTEMPT FAILURE: probing an UNJOINED fed exits non-zero, never demotes =="
UNJOINED="$(printf '%064d' 0)"  # a 32-byte zero fed id we never joined
NA_OUT=$(mktemp)
if wcli probe "$UNJOINED" --from "$FED_A" --gateway "$GW" >"$NA_OUT" 2>/dev/null; then
  cat "$NA_OUT" >&2; fail "probing an unjoined fed must exit non-zero"
fi
grep -q '^attempt: none ' "$NA_OUT" || { cat "$NA_OUT" >&2; fail "an unjoined-fed probe must print 'attempt: none <diagnostic>'"; }
# B's verdict history is untouched by the unrelated failure (still 'passed' under the shrunk verb policy).
V_OUT=$(mktemp)
wcli probe "$FED_B" --from "$FED_A" --gateway "$GW" --min-span-secs 1 >"$V_OUT" 2>/dev/null || fail "post-failure re-probe failed"
grep -q '^verdict: passed$' "$V_OUT" || { cat "$V_OUT" >&2; fail "B's verdict must be untouched by an unrelated unjoined-fed failure"; }
echo "no-attempt OK: unjoined-fed probe exits non-zero with 'attempt: none'; B's verdict untouched"

echo "OK: wallet-cli active-probe smoke passed (round trip nets its delta then drains — combined"
echo "    loss is fees only, never a sweep; umbrella + both legs auditable as active_probe; a"
echo "    sustained window flips the verb's verdict to passed while status stays conservative; an"
echo "    unjoined-fed probe is a non-zero NoAttempt that never demotes)"
