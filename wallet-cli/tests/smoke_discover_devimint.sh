#!/usr/bin/env bash
# devimint smoke test for DISCOVERY + the probe-gated funding path — the Phase 5.1 exit gate
# (docs/phase5-plan.md §5.1.7). It drives the WHOLE §5.1 invariant end-to-end on the two-fed
# harness: a federation the agent DISCOVERS and auto-joins is probe-GATED — the allocator must
# refuse to fund it until it has PASSED an active probe, and must fund it once it has. Chain:
#
#   1. `discover --source manual --invite <B> --auto-join`  -> B is AGENT-joined (AutoJoined),
#      structurally vetted, but NOT user-owned: a `Discover` row, an `AutoJoin` row, and an
#      `actor: Agent` `join` row all land in `history`.
#   2. `tick --spending A --standby B`  -> BAILS non-zero ("failed the fundability gate"): B is
#      probe-gated, so the allocator will not fund it and a pin does NOT bypass the gate. B stays
#      EMPTY. (This is the core "probes gate, discovery never promotes" invariant.)
#   3. three `probe B --from A`  -> B's active-probe verdict flips to `passed`.
#   4. the SAME `tick`  -> NOW decides + performs the fund-standby Move A->B. B is funded.
#
# A is a USER join (`wallet-cli join` -> UserApproved -> ungated spending fed); B is only ever
# reached through DISCOVERY (never `wallet-cli join`), so it stays agent-owned/probe-gated until
# it passes. That difference is the whole test.
#
# NOT part of the rb-lite gate (needs a LIVE two-fed devimint; run by hand). Same harness as
# smoke_tick/smoke_probe: docs/devimint-two-fed-harness.patch supplies $FED_B_INVITE and
# connects/pegs the shared LDK gateway. REBUILD wallet-cli into this repo's target-nix BEFORE
# running (the fedimint devshell redirects cargo's target dir — docs/devimint-runbook.md):
#   CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix \
#     nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
#
# Run (inside `devimint --num-feds 2 dev-fed --exec`, FM_ENABLE_MODULE_LNV2=1, FED_B_INVITE set) —
# see smoke_tick_devimint.sh's header for the full two-federation bring-up.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run inside \`dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"
FED_B_INVITE="${FED_B_INVITE:-${FM_INVITE_CODE_B:-}}"
if [[ -z "$FED_B_INVITE" ]]; then
  echo "FAIL: FED_B_INVITE (or FM_INVITE_CODE_B) not set — this is a TWO-federation smoke." >&2
  exit 1
fi
if [[ "$FED_B_INVITE" == "$FM_INVITE_CODE" ]]; then
  echo "FAIL: FED_B_INVITE equals FM_INVITE_CODE — spending and candidate must be DIFFERENT feds." >&2
  exit 1
fi

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build it: CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
FUND_MSAT=800000        # fund fed A. Must exceed spending-target + max_fee reserve + standby + probe costs.
SPENDING_TARGET=100000  # keep >=100 sat on A; its surplus funds the candidate once B passes.
STANDBY_TARGET=100000   # fund the (probed) candidate B toward 100 sat -> the fund-standby move size.
MAX_FEE=100000          # 100 sat: generous vs the ~10-sat devimint move fee, yet the allocator reserves
                        # the FULL cap from A's above-target surplus, so it must stay well under it.
RECV_SLACK=2000         # 2 sat: bounds the lnv2 receive-fee under-estimate on B (never over-credit).
A_FEE_HEADROOM=80000    # 80 sat: generous bound on total fees A pays (3 probe round-trips + the move).
LEG_FEE_CAP=10000       # probe PROBE_LEG_FEE_CAP_MSAT default (bounds B's post-probe residue).

DATA_DIR="$(mktemp -d)"
DI_ERR="$(mktemp)"; DISC_OUT="$(mktemp)"; DISC_ERR="$(mktemp)"
TICK_OUT="$(mktemp)"; TICK_ERR="$(mktemp)"; P_OUT="$(mktemp)"; P_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$DI_ERR" "$DISC_OUT" "$DISC_ERR" "$TICK_OUT" "$TICK_ERR" "$P_OUT" "$P_ERR"' EXIT

wcli() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW" "$@"; }
join_fed() {
  local started key state
  started=$(wcli join "$1") || return
  key=${started#* }
  state=$(wcli await-move "$key") || return
  [[ "$state" == "done" ]] || { echo "join $key did not settle: $state" >&2; return 1; }
  cut -d: -f2 <<<"$key"
}
fail() { echo "FAIL: $*" >&2; exit 1; }
bal_for() { wcli balance | awk -v id="$1" '$1 == id":" && $3 == "msat" { print $2 }'; }
performed_count() { sed -n 's/.*performed=\([0-9]*\).*/\1/p' "$1"; }
# candidate registry TSV: 1 id 2 state 3 source 4 discovered_at 5 structural 6 checked_at 7 updated 8 invite
cand_field() { wcli candidates "$@"; }

# ---------------------------------------------------------------------------------------
echo "== JOIN fed A (spending, USER-owned -> UserApproved); DISCOVER fed B (agent-owned) =="
FED_A=$(join_fed "$FM_INVITE_CODE")
echo "fed A (spending, user): $FED_A"
# A user join must land A as UserApproved (ungated) — that is what keeps it fundable as spending.
wcli candidates --state userapproved | awk -F'\t' -v a="$FED_A" '$1 == a {found=1} END{exit !found}' \
  || fail "fed A should be a UserApproved candidate after a user join"

if command -v gateway-ldk >/dev/null; then
  gateway-ldk connect-fed "$FED_B_INVITE" >/dev/null 2>&1 \
    && echo "connected fed B to the LDK gateway" \
    || echo "note: gateway-ldk connect-fed non-zero (likely already connected) — continuing"
fi

# DISCOVER B from a MANUAL source (the offline/live-gate source) and AUTO-JOIN it. --scorer-allow-regtest
# relaxes the network floor (devimint feds are regtest); --gateway pins the shared route for the join's
# route checks. B becomes AutoJoined (agent-owned, probe-GATED) — NEVER user-joined.
if ! wcli discover --source manual --invite "$FED_B_INVITE" --auto-join \
      --gateway "$GW" --scorer-allow-regtest >"$DISC_OUT" 2>"$DISC_ERR"; then
  echo "  --- discover stdout ---" >&2; cat "$DISC_OUT" >&2
  echo "  --- discover stderr ---" >&2; cat "$DISC_ERR" >&2
  fail "discover --auto-join exited non-zero"
fi
echo "-- discover summary --"; cat "$DISC_OUT"

# Exactly one AutoJoined candidate (B); capture its id from the registry.
mapfile -t AUTOJOINED < <(wcli candidates --state autojoined | awk -F'\t' '{print $1}')
(( ${#AUTOJOINED[@]} == 1 )) || { wcli candidates >&2; fail "expected exactly 1 AutoJoined candidate, got ${#AUTOJOINED[@]}"; }
FED_B="${AUTOJOINED[0]}"
echo "fed B (candidate, agent-joined): $FED_B"
[[ "$FED_A" != "$FED_B" ]] || fail "A and B resolved to the same federation id"

# The discovery chain is auditable: a Discover row, an AutoJoin row, and an Agent join row for B.
# history cols (§11): 3 kind, 8 actor (`user` | `agent:<occurrence>`). A's join is actor `user`;
# B's auto-join is the only `agent:*` join, so that pair uniquely identifies the agent join.
wcli history --limit 50 | awk -F'\t' '$3 == "discover"' | grep -q . || fail "no discover row in history"
wcli history --limit 50 | awk -F'\t' '$3 == "autojoin"' | grep -q . || fail "no autojoin row in history"
wcli history --limit 50 | awk -F'\t' '$3 == "join" && $8 ~ /^agent:/' | grep -q . \
  || { wcli history --limit 50 >&2; fail "no agent join row for the auto-joined candidate B"; }
echo "discovery OK: B is AutoJoined; discover + autojoin + agent-join rows present"

# ---------------------------------------------------------------------------------------
echo "== FUND fed A: direct-inflow ${FUND_MSAT} msat =="
INV_A=$(wcli direct-inflow --to "$FED_A" --amount "$FUND_MSAT" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^key: //p' "$DI_ERR")
[[ -n "$INV_A" && -n "$KEY_FUND" ]] || { cat "$DI_ERR" >&2; fail "funding direct-inflow gave no invoice/key"; }
SEND_FUND=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_FUND" >/dev/null 2>&1 || true
[[ "$(wcli await-move "$KEY_FUND")" == "done" ]] || fail "funding direct-inflow did not settle"

A0=$(bal_for "$FED_A"); B0=$(bal_for "$FED_B")
[[ "$A0" =~ ^[0-9]+$ && "$B0" =~ ^[0-9]+$ ]] || { wcli balance >&2; fail "could not parse balances"; }
echo "post-fund: A=${A0} msat  B=${B0} msat (B, agent-joined, EMPTY)"
(( B0 == 0 )) || fail "candidate B should start EMPTY, holds ${B0} msat"
(( A0 >= SPENDING_TARGET + STANDBY_TARGET + A_FEE_HEADROOM )) \
  || fail "fed A ${A0} msat under spending+standby+fees budget"

# ---------------------------------------------------------------------------------------
echo "== GATED TICK: an unproven AutoJoined standby must NOT be funded (tick BAILS) =="
# Both ticks use the SAME shrunk gate window (--probe-min-span-secs 1) so the ONLY variable
# between the gated and ungated tick is whether B has PASSED its probes — not a policy diff.
# (The gated tick bails anyway: B is NeverProbed then. Default gate span is 24h, unreachable live.)
tick() { wcli tick --spending "$FED_A" --standby "$FED_B" \
  --spending-target "$SPENDING_TARGET" --standby-target "$STANDBY_TARGET" \
  --max-fee "$MAX_FEE" --gateway "$GW" --probe-min-span-secs 1 --occurrence "$1"; }
if tick 0 >"$TICK_OUT" 2>"$TICK_ERR"; then
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  echo "  --- tick stderr ---" >&2; cat "$TICK_ERR" >&2
  fail "the tick must BAIL while B is probe-gated (a pin does NOT bypass the probe gate)"
fi
grep -q "failed the fundability gate" "$TICK_ERR" \
  || { cat "$TICK_ERR" >&2; fail "gated tick did not explain the probe-gate refusal"; }
grep -q "$FED_B" "$TICK_ERR" || { cat "$TICK_ERR" >&2; fail "gated-tick refusal did not name B"; }
(( $(bal_for "$FED_B") == 0 )) || fail "B was funded despite being probe-gated"
echo "gate OK: tick bailed (fundability gate), B still EMPTY"

# ---------------------------------------------------------------------------------------
echo "== PROBE B from A three times -> the active-probe verdict flips to 'passed' =="
run_probe() { # run_probe <label> <outfile>
  if ! wcli probe "$FED_B" --from "$FED_A"  --min-span-secs 1 >"$1" 2>"$P_ERR"; then
    echo "  --- $1 out ---" >&2; cat "$1" >&2; echo "  --- err ---" >&2; cat "$P_ERR" >&2
    fail "a probe of B from A failed"
  fi
}
run_probe "$P_OUT"; sleep 1
run_probe "$P_OUT"; sleep 1
run_probe "$P_OUT"
grep -q '^verdict: passed$' "$P_OUT" || { cat "$P_OUT" >&2; fail "3 probes did not flip the verdict to 'passed'"; }
echo "probe OK: B's active-probe verdict is 'passed'"

# ---------------------------------------------------------------------------------------
echo "== UNGATED TICK: with B PASSED, the allocator now funds the standby A->B =="
BPRE=$(bal_for "$FED_B")  # B's small post-probe residue (< the out-leg fee cap)
(( BPRE < LEG_FEE_CAP )) || fail "unexpected pre-fund residue on B: ${BPRE} msat"
if ! tick 1 >"$TICK_OUT" 2>"$TICK_ERR"; then
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  echo "  --- tick stderr ---" >&2; cat "$TICK_ERR" >&2
  fail "the tick must now fund B (probe passed) — but it bailed"
fi
echo "-- tick decisions --"; cat "$TICK_OUT"
grep -Eq "move [0-9]+ msat ${FED_A} -> ${FED_B} .*reason StandbyBelowTarget" "$TICK_OUT" \
  || { cat "$TICK_OUT" >&2; fail "the ungated tick did not decide a fund-standby Move A -> B"; }
[[ "$(performed_count "$TICK_OUT")" == "1" ]] || { cat "$TICK_OUT" >&2; fail "expected exactly 1 performed move"; }

A1=$(bal_for "$FED_A"); B1=$(bal_for "$FED_B")
echo "post-fund: A=${A1} msat  B=${B1} msat"
# B funded to ~STANDBY_TARGET (the allocator sizes the move to the shortfall below target, so B
# lands at ~target regardless of the residue), NEVER over.
(( B1 <= STANDBY_TARGET )) || fail "B over-credited: ${B1} > ${STANDBY_TARGET} (never-over violated)"
(( B1 >= STANDBY_TARGET - RECV_SLACK )) || fail "B under target: ${B1} < $((STANDBY_TARGET - RECV_SLACK))"
(( B1 > BPRE )) || fail "B did not rise on the ungated funding tick"

# The funding move is auditable in history (kind move, A -> B).
wcli history --limit 80 | awk -F'\t' '$3 == "move"' | grep -q . || fail "no move row for the funding tick"
echo "fund OK: allocator funded B to ${B1} msat (~${STANDBY_TARGET}, never over)"

echo "OK: wallet-cli discovery smoke passed (discover -> agent auto-join B; a probe-GATED"
echo "    standby is refused funding by tick — the pin does not bypass the gate — and B stays"
echo "    empty; three probes flip B to 'passed'; the SAME tick then funds B to ~${STANDBY_TARGET} msat."
echo "    Full chain — discover, autojoin, agent-join, probes, gated-then-ungated funding — is in history)"
