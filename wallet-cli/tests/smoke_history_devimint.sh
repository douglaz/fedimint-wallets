#!/usr/bin/env bash
# devimint smoke test for the OPERATION LEDGER — the Phase 4.C exit gate
# (docs/phase4-implementation-spec.md §13, docs/operation-history-spec.md §6): a full
# two-fed session — join A+B → direct-inflow (fund) → raw receive (--key round trip) →
# cross-fed move → a FORCED fee-cap failure → an agent tick with induced OverCap refusals —
# must be fully reconstructible from `wallet-cli history`/`show`: kinds, actors, reasons,
# fees, errors, both legs' op ids, and `created_at_ms` non-decreasing by seq.
#
# NOT part of the rb-lite gate (needs a LIVE two-fed devimint; run by hand). Same harness as
# smoke_move_devimint.sh: the docs/devimint-two-fed-harness.patch supplies $FED_B_INVITE and
# connects/pegs the shared LDK gateway. REBUILD wallet-cli into this repo's target-nix BEFORE
# running (the fedimint devshell redirects cargo's target dir — see docs/devimint-runbook.md):
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
wcli() { "$WALLET_CLI" --standalone --data-dir "$WCLI_DIR" --gateway "$GW" "$@"; }
join_fed() {
  local started key state
  started=$(wcli join "$1") || return
  key=${started#* }
  state=$(wcli await-move "$key") || return
  [[ "$state" == "done" ]] || { echo "join $key did not settle: $state" >&2; return 1; }
  cut -d: -f2 <<<"$key"
}
fail() { echo "FAIL: $*" >&2; exit 1; }

# TSV columns (§11): 1 seq, 2 updated_at, 3 kind, 4 status, 5 amount, 6 recv_fee,
# 7 send_fee_quoted, 8 actor, 9 reason, 10 key.
hist_row() { # hist_row <key> — the single TSV row whose key column matches exactly
  wcli history --limit 100 | awk -F'\t' -v k="$1" '$10 == k' | head -1
}
col() { echo "$1" | awk -F'\t' -v c="$2" '{print $c}'; }

command -v gateway-ldk >/dev/null && gateway-ldk connect-fed "$FED_B_INV" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------------------
echo "== JOIN both federations (two join rows) =="
FED_A=$(join_fed "$FM_INVITE_CODE")
FED_B=$(join_fed "$FED_B_INV")
echo "A=$FED_A B=$FED_B"

# ---------------------------------------------------------------------------------------
echo "== FUND fed A: direct-inflow 500000 msat (user actor) =="
DI_ERR=$(mktemp)
INV_A=$(wcli direct-inflow --to "$FED_A" --amount 500000 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^key: //p' "$DI_ERR")
[[ -n "$INV_A" && -n "$KEY_FUND" ]] || { cat "$DI_ERR" >&2; fail "direct-inflow gave no invoice/key"; }
SEND1=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND1" >/dev/null 2>&1 || true
[[ "$(wcli await-move "$KEY_FUND")" == "done" ]] || fail "funding direct-inflow did not settle"

# ---------------------------------------------------------------------------------------
echo "== RAW receive 100000 msat on A (operation-key terminalization) =="
RC_ERR=$(mktemp)
INV_RAW=$(wcli receive --amount 100000 --to "$FED_A" 2>"$RC_ERR")
KEY_RAW=$(sed -n 's/^key: //p' "$RC_ERR")
[[ -n "$INV_RAW" && -n "$KEY_RAW" ]] || { cat "$RC_ERR" >&2; fail "receive gave no invoice/key"; }
SEND2=$(fedimint-cli module lnv2 send "$INV_RAW" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND2" >/dev/null 2>&1 || true
STATE_RAW=$(wcli await-receive "$KEY_RAW")
[[ "$STATE_RAW" == "claimed" ]] || fail "raw receive expected 'claimed', got '$STATE_RAW'"

# ---------------------------------------------------------------------------------------
echo "== MOVE A->B 50000 msat (user actor; both op ids must resolve) =="
MV_ERR=$(mktemp)
MOVE_PHASE1=$(wcli move --from "$FED_A" --to "$FED_B" --amount 50000 2>"$MV_ERR")
KEY_MOVE=$(sed -n 's/^key: //p' "$MV_ERR")
[[ "$MOVE_PHASE1" == "started $KEY_MOVE" && -n "$KEY_MOVE" ]] || { cat "$MV_ERR" >&2; fail "move did not start (phase1=$MOVE_PHASE1)"; }
MOVE_STATE=$(wcli await-move "$KEY_MOVE")
[[ "$MOVE_STATE" == "done" ]] || fail "move did not settle (state=$MOVE_STATE)"

# ---------------------------------------------------------------------------------------
echo "== FORCED FAILURE: move with --fee-cap 1 must refuse BEFORE paying =="
BAL_BEFORE=$(wcli balance | awk '$1 == "total" {print $(NF-1)}')
FMV_ERR=$(mktemp)
FMV_PHASE1=$(wcli move --from "$FED_A" --to "$FED_B" --amount 50000 --fee-cap 1 --occurrence 1 2>"$FMV_ERR")
KEY_FAILED=$(sed -n 's/^key: //p' "$FMV_ERR")
[[ -n "$KEY_FAILED" ]] || { cat "$FMV_ERR" >&2; fail "failed move printed no operation key"; }
if wcli await-move "$KEY_FAILED" >>"$FMV_ERR" 2>&1; then
  fail "a 1-msat fee-cap move unexpectedly terminalized successfully ($FMV_PHASE1)"
fi
BAL_AFTER=$(wcli balance | awk '$1 == "total" {print $(NF-1)}')
[[ "$BAL_BEFORE" == "$BAL_AFTER" ]] || fail "the refused move changed balances ($BAL_BEFORE -> $BAL_AFTER)"

# ---------------------------------------------------------------------------------------
echo "== AGENT tick: tiny per-fed cap induces OverCap refusal rows =="
if ! wcli tick \
      --spending "$FED_A" --standby "$FED_B" \
      --spending-target 0 --standby-target 0 \
      --per-fed-cap 10000 --max-fee 1000000 --gateway "$GW" --occurrence 0 >/dev/null 2>&1; then
  fail "advisory-only tick exited non-zero"
fi

# ---------------------------------------------------------------------------------------
echo "== HISTORY assertions =="
wcli history --limit 100 | sed 's/^/  | /'

# Two Succeeded user joins.
JOINS=$(wcli history --limit 100 | awk -F'\t' '$3 == "join" && $4 == "succeeded" && $8 == "user"' | wc -l)
(( JOINS == 2 )) || fail "expected 2 succeeded join rows, got $JOINS"

# The funding direct-inflow: succeeded, exact amount, user actor, user_initiated reason,
# and a recorded receive fee (the §2.3 quote — exact on a Succeeded row).
ROW=$(hist_row "$KEY_FUND"); [[ -n "$ROW" ]] || fail "no history row for $KEY_FUND"
[[ "$(col "$ROW" 3)" == "direct-inflow" ]] || fail "fund row kind: $(col "$ROW" 3)"
[[ "$(col "$ROW" 4)" == "succeeded" ]] || fail "fund row status: $(col "$ROW" 4)"
[[ "$(col "$ROW" 5)" == "500000" ]] || fail "fund row amount: $(col "$ROW" 5)"
[[ "$(col "$ROW" 8)" == "user" ]] || fail "fund row actor: $(col "$ROW" 8)"
[[ "$(col "$ROW" 9)" == "user_initiated" ]] || fail "fund row reason: $(col "$ROW" 9)"
[[ "$(col "$ROW" 6)" != "-" ]] || fail "fund row carries no receive fee"

# The raw receive: succeeded with the GROSS invoiced amount.
ROW=$(hist_row "$KEY_RAW"); [[ -n "$ROW" ]] || fail "no history row for $KEY_RAW"
[[ "$(col "$ROW" 3)" == "receive" && "$(col "$ROW" 4)" == "succeeded" ]] || fail "raw receive row: $ROW"
[[ "$(col "$ROW" 5)" == "100000" ]] || fail "raw receive amount: $(col "$ROW" 5)"

# The settled move: succeeded, user; `show` resolves BOTH legs' op ids.
ROW=$(hist_row "$KEY_MOVE"); [[ -n "$ROW" ]] || fail "no history row for $KEY_MOVE"
[[ "$(col "$ROW" 3)" == "move" && "$(col "$ROW" 4)" == "succeeded" && "$(col "$ROW" 8)" == "user" ]] \
  || fail "move row: $ROW"
SHOW_MOVE=$(wcli show "$KEY_MOVE")
OPS=$(echo "$SHOW_MOVE" | grep -cE "(send_op|recv_op): [0-9a-f]{16,}") || true
(( OPS == 2 )) || { echo "$SHOW_MOVE" >&2; fail "show $KEY_MOVE did not resolve both op ids"; }

# The forced failure: Failed, error names the fee cap, and the refusal is EXPLAINED — the
# §2.3 receive-side quote persisted before the cap check shows up as the row's fee.
ROW=$(hist_row "$KEY_FAILED"); [[ -n "$ROW" ]] || fail "no history row for $KEY_FAILED"
[[ "$(col "$ROW" 4)" == "failed" ]] || fail "forced-failure row status: $(col "$ROW" 4)"
[[ "$(col "$ROW" 6)" != "-" ]] || fail "forced-failure row carries no receive-fee quote (§2.3)"
wcli show "$KEY_FAILED" | grep -qi "fee over cap" || fail "show $KEY_FAILED lacks the fee-over-cap error"

# The tick row (agent, standing_instruction) and at least one OverCap refusal row.
TICKS=$(wcli history --limit 100 | awk -F'\t' '$3 == "tick" && $4 == "succeeded" && $8 ~ /^agent/ && $9 == "standing_instruction"' | wc -l)
(( TICKS >= 1 )) || fail "no succeeded agent tick row with standing_instruction"
REFUSALS=$(wcli history --limit 100 | awk -F'\t' '$3 == "refusal" && $9 == "over_cap"' | wc -l)
(( REFUSALS >= 1 )) || fail "no over_cap refusal row from the tick"

# created_at_ms non-decreasing by seq (seq is the ordering authority — §13).
ORDERED=$(wcli history --json --limit 100 | jq -s 'sort_by(.seq) | [.[].created_at_ms] | . == sort' )
[[ "$ORDERED" == "true" ]] || fail "created_at_ms is not non-decreasing by seq"

# Actor filter honors the split.
AGENT_ONLY=$(wcli history --limit 100 --actor agent | awk -F'\t' '$8 == "user"' | wc -l)
(( AGENT_ONLY == 0 )) || fail "--actor agent leaked user rows"

echo "OK: wallet-cli history smoke passed (join/fund/raw-receive/move/forced-failure/tick all"
echo "    reconstructible: kinds, actors, reasons, fees incl. the explained fee-cap refusal,"
echo "    both move op ids, seq-ordered timestamps, actor filters)"
