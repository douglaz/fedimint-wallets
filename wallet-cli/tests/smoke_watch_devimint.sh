#!/usr/bin/env bash
# devimint smoke test for the SELF-RUNNING WATCH LOOP — the Phase 5.2 / Phase 5 EXIT GATE
# (docs/phase5-plan.md §5.2.7). Where smoke_discover (§5.1) drives discover/probe/tick as SEPARATE
# operator verbs, this drives the WHOLE §5.2 invariant AUTONOMOUSLY through a single verb — repeated
# `wallet-cli watch --once` cycles — proving the loop needs no hand-holding between steps:
#
#   1. cycle 1: `watch --once` DISCOVERS fed B from a manual source and auto-joins it (agent-owned,
#      probe-GATED). B is NOT user-joined, so it stays gated until it PASSES an active probe.
#   2. gated cycles: B is not yet probe-eligible, so the tick auto-designates NO standby and does
#      not fund B (a gated fed is simply not eligible — no bail, the cycle continues). Meanwhile the
#      loop's SCHEDULED probes drive B toward a pass, one money-moving probe per due cycle.
#   3. pass cycle: three scheduled probes flip B's verdict to `passed`.
#   4. fund cycle: the VERY NEXT tick now auto-designates the (eligible) B as standby and performs
#      the fund-standby Move A->B — with no operator `tick`/`probe`/`discover` verb ever run.
#   5. evacuate cycle: fed B is forced to look like it is winding down (the DEBUG-only
#      WALLET_CLI_FORCE_SHUTDOWN seam); the next `watch --once` tick senses it and evacuates B->A,
#      draining the dying fed back into the safe user-owned fed.
#
# EVERY money move here is Agent-attributed (`agent:<occurrence>` in history) — discover, auto-join,
# scheduled probes, the fund move, and the evacuate — because the loop, not the operator, drove them.
# That is the whole gate: the wallet runs its own risk engine unattended.
#
# DEBUG binary REQUIRED: the WALLET_CLI_FORCE_SHUTDOWN evacuation seam is compiled out of --release,
# so the money path can never be forced to evacuate in production. Build the debug binary into this
# repo's target-nix BEFORE running (the fedimint devshell redirects cargo's target dir):
#   CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix \
#     nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
#
# NOT part of the rb-lite gate (needs a LIVE two-fed devimint; run by hand). Same harness as
# smoke_discover/smoke_tick/smoke_probe: docs/devimint-two-fed-harness.patch supplies $FED_B_INVITE
# and connects/pegs the shared LDK gateway. Run inside `devimint --num-feds 2 dev-fed --exec`
# (FM_ENABLE_MODULE_LNV2=1, FED_B_INVITE set) — see smoke_tick_devimint.sh's header for the full
# two-federation bring-up.
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
  echo "Build the DEBUG binary (the force-shutdown seam is compiled out of --release):" >&2
  echo "  CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
case "$WALLET_CLI" in
  *release*) echo "FAIL: $WALLET_CLI looks like a RELEASE build; the force-shutdown seam needs DEBUG." >&2; exit 1 ;;
esac
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
FUND_MSAT=800000        # fund fed A. Must exceed spending-target + max_fee reserve + standby + probe costs.
SPENDING_TARGET=100000  # keep >=100 sat on A; its surplus funds the candidate once B passes.
STANDBY_TARGET=100000   # fund the (probed) candidate B toward 100 sat -> the fund-standby move size.
MAX_FEE=100000          # 100 sat: generous vs the ~10-sat devimint move fee; the allocator reserves the
                        # FULL cap from A's above-target surplus, so it must stay well under it.
PER_FED_CAP=100000000   # 100k sat: far above every balance here, so neither funding nor evacuate is cap-bound.
RECV_SLACK=2000         # 2 sat: bounds the lnv2 receive-fee under-estimate on B (never over-credit).
LEG_FEE_CAP=10000       # probe PROBE_LEG_FEE_CAP_MSAT default (bounds B's post-probe residue).
MAX_CYCLES=15           # safety bound on the number of autonomous `watch --once` cycles.

DATA_DIR="$(mktemp -d)"
DI_ERR="$(mktemp)"; W_OUT="$(mktemp)"; W_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$DI_ERR" "$W_OUT" "$W_ERR"' EXIT

wcli() { "$WALLET_CLI" --data-dir "$DATA_DIR" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
bal_for() { wcli balance | awk -v id="$1" '$1 == id":" && $3 == "msat" { print $2 }'; }

# B's federation id is only known AFTER the loop discovers it (cycle 1), so the standby is pinned
# from cycle 2 on. Pinning is required, NOT a loop limitation: devimint feds run on REGTEST, so the
# scorer's require_mainnet floor makes them ineligible for AUTO-designation (see smoke_tick) — the
# operator pins the designation and the money path re-checks health + the probe GATE. Discovery,
# probing, funding, and evacuation still happen autonomously inside `watch --once`.
STANDBY_ARG=()

# One autonomous cycle. $1 = a diagnostic label; extra env (e.g. WALLET_CLI_FORCE_SHUTDOWN) is
# inherited from the caller. The whole §5.2 pipeline is in these flags — the loop discovers B,
# schedules probes, and funds/evacuates from a designated spending fed A, all in one verb. A gated
# standby makes the tick fail INSIDE the cycle (recorded, cycle continues) — not a non-zero exit.
watch_once() {
  local label="$1"
  if ! wcli watch --once \
        --spending "$FED_A" "${STANDBY_ARG[@]}" \
        --spending-target "$SPENDING_TARGET" --standby-target "$STANDBY_TARGET" \
        --max-fee "$MAX_FEE" --per-fed-cap "$PER_FED_CAP" --gateway "$GW" \
        --source manual --invite "$FED_B_INVITE" --auto-join --scorer-allow-regtest \
        --probe-min-span-secs 1 --probe-min-successes 3 \
        --min-interval-secs 1 --discover-every-secs 3600 --probe-retry-backoff-secs 1 \
        --max-probe-attempts-per-week 50 --max-probe-spend-per-week-msat 5000000 \
        >"$W_OUT" 2>"$W_ERR"; then
    echo "  --- watch --once ($label) stdout ---" >&2; cat "$W_OUT" >&2
    echo "  --- watch --once ($label) stderr ---" >&2; cat "$W_ERR" >&2
    fail "watch --once ($label) exited non-zero — a fatal loop error, not a recorded step failure"
  fi
  # print_watch_report writes the per-cycle occurrence/reconcile/tick/probe/discover summary to
  # STDERR — surface it so the loop's autonomous progress (esp. the probe verdict) is visible.
  grep -E '^(watch occurrence|reconcile|tick|probe|discover)' "$W_ERR" | sed "s/^/  [$label] /" || true
}

# ---------------------------------------------------------------------------------------
echo "== JOIN fed A (spending, USER-owned -> UserApproved) =="
FED_A=$(wcli join "$FM_INVITE_CODE")
echo "fed A (spending, user): $FED_A"
wcli candidates --state userapproved | awk -F'\t' -v a="$FED_A" '$1 == a {found=1} END{exit !found}' \
  || fail "fed A should be a UserApproved candidate after a user join"

if command -v gateway-ldk >/dev/null; then
  gateway-ldk connect-fed "$FED_B_INVITE" >/dev/null 2>&1 \
    && echo "connected fed B to the LDK gateway" \
    || echo "note: gateway-ldk connect-fed non-zero (likely already connected) — continuing"
fi

# ---------------------------------------------------------------------------------------
echo "== FUND fed A: direct-inflow ${FUND_MSAT} msat =="
INV_A=$(wcli direct-inflow --to "$FED_A" --amount "$FUND_MSAT" --gateway "$GW" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^intent_key: //p' "$DI_ERR")
[[ -n "$INV_A" && -n "$KEY_FUND" ]] || { cat "$DI_ERR" >&2; fail "funding direct-inflow gave no invoice/key"; }
SEND_FUND=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_FUND" >/dev/null 2>&1 || true
[[ "$(wcli await-move "$KEY_FUND")" == "done" ]] || fail "funding direct-inflow did not settle"
A0=$(bal_for "$FED_A")
[[ "$A0" =~ ^[0-9]+$ ]] || { wcli balance >&2; fail "could not parse A balance"; }
(( A0 >= SPENDING_TARGET + STANDBY_TARGET )) || fail "fed A ${A0} msat under spending+standby budget"
echo "post-fund: A=${A0} msat"

# ---------------------------------------------------------------------------------------
echo "== AUTONOMOUS LOOP: watch --once discovers B, probes it, and funds it — no operator verbs =="
FED_B=""
FUNDED=0
GATED_OBSERVED=0
for (( c=1; c<=MAX_CYCLES; c++ )); do
  watch_once "cycle $c"
  # After the first cycle B is auto-joined; capture its id from the registry.
  if [[ -z "$FED_B" ]]; then
    mapfile -t AUTOJOINED < <(wcli candidates --state autojoined | awk -F'\t' '{print $1}')
    if (( ${#AUTOJOINED[@]} == 1 )); then
      FED_B="${AUTOJOINED[0]}"
      [[ "$FED_A" != "$FED_B" ]] || fail "A and B resolved to the same federation id"
      STANDBY_ARG=(--standby "$FED_B")   # pin B as the standby now that its id is known
      echo "cycle $c: DISCOVERED + auto-joined fed B (agent-owned, probe-gated): $FED_B"
    fi
    continue
  fi
  BB=$(bal_for "$FED_B"); BB="${BB:-0}"
  echo "cycle $c: B balance = ${BB} msat"
  # While gated (not yet passed), B must not be FUNDED — it holds at most a small probe residue
  # (< the out-leg fee cap), never the standby target. Observing that residue-level balance is the
  # proof the gate held (a funded B would be ~STANDBY_TARGET).
  if (( BB < LEG_FEE_CAP )); then
    GATED_OBSERVED=1
  fi
  if (( BB >= STANDBY_TARGET - RECV_SLACK )); then
    FUNDED=1
    echo "cycle $c: B AUTONOMOUSLY FUNDED to ${BB} msat"
    break
  fi
  sleep 1
done

[[ -n "$FED_B" ]] || fail "the loop never discovered/auto-joined fed B"
if (( FUNDED != 1 )); then
  echo "  --- post-mortem: candidates ---" >&2; wcli candidates >&2
  echo "  --- post-mortem: B history ---" >&2; wcli history --limit 40 --fed "$FED_B" >&2
  echo "  --- post-mortem: balances ---" >&2; wcli balance >&2
  fail "the loop did not autonomously fund B within ${MAX_CYCLES} cycles (see per-cycle probe verdicts above)"
fi
(( GATED_OBSERVED == 1 )) || fail "B was never observed at residue level while gated — the probe gate did not hold"

B1=$(bal_for "$FED_B"); A1=$(bal_for "$FED_A")
(( B1 <= STANDBY_TARGET )) || fail "B over-credited: ${B1} > ${STANDBY_TARGET} (never-over violated)"
echo "autonomous funding OK: A=${A1} msat  B=${B1} msat (B funded to ~${STANDBY_TARGET}, never over)"

# The autonomous chain is auditable and Agent-attributed: discover + autojoin + agent-join +
# scheduled probes + the fund move — history actor col (§11, col 8) is `agent:<occurrence>`.
wcli history --limit 100 | awk -F'\t' '$3 == "discover"' | grep -q . || fail "no discover row in history"
wcli history --limit 100 | awk -F'\t' '$3 == "autojoin"' | grep -q . || fail "no autojoin row in history"
wcli history --limit 100 | awk -F'\t' '$3 == "join" && $8 ~ /^agent:/' | grep -q . \
  || { wcli history --limit 100 >&2; fail "no agent join row for the auto-joined fed B"; }
FUND_MOVE=$(wcli history --limit 100 | awk -F'\t' '$3 == "move" && $8 ~ /^agent:/' | grep -c .)
(( FUND_MOVE >= 1 )) || { wcli history --limit 100 >&2; fail "no agent-attributed move row for the funding tick"; }
echo "audit OK: discover, autojoin, agent-join, and the agent-driven fund move are all in history"

# ---------------------------------------------------------------------------------------
echo "== FORCE-SHUTDOWN fed B: the next watch cycle senses it and EVACUATES B -> A =="
BPRE=$(bal_for "$FED_B")
(( BPRE > LEG_FEE_CAP )) || fail "B should hold its funded balance before evacuate, has ${BPRE} msat"
# The DEBUG-only seam OR's B into shutdown_scheduled; the tick inside watch_once then decides an
# Evacuate B->A (reason ShutdownNotice) and drains B into the safe user-owned fed A. print_watch_report
# writes only a per-cycle SUMMARY to STDERR (no per-decision lines), so the evacuate is verified by its
# EFFECT: B drains, and an agent-attributed shutdown_notice move lands in history. Keep B force-flagged
# across the settle cycles so the tick will not re-fund the now-draining fed, and reconcile drives any
# still-in-flight evacuate leg to completion (the retryable-pending guard skips a duplicate evacuate).
WALLET_CLI_FORCE_SHUTDOWN="$FED_B" watch_once "evacuate"
echo "-- evacuate cycle report (stderr) --"; cat "$W_ERR"
for (( d=1; d<=MAX_CYCLES; d++ )); do
  BNOW=$(bal_for "$FED_B"); BNOW="${BNOW:-0}"
  (( BNOW <= LEG_FEE_CAP )) && break
  sleep 1
  WALLET_CLI_FORCE_SHUTDOWN="$FED_B" watch_once "evacuate-settle $d"
done
BEND=$(bal_for "$FED_B"); AEND=$(bal_for "$FED_A")
(( BEND < BPRE )) || fail "B was not drained by the evacuate (${BEND} msat, was ${BPRE})"
(( BEND <= LEG_FEE_CAP )) || fail "B still holds ${BEND} msat after evacuate (expected it drained to ~0)"
echo "evacuate OK: B drained ${BPRE} -> ${BEND} msat into A (now ${AEND} msat)"

# The evacuate is auditable and Agent-attributed: history kind `evacuation` (col 3), actor
# `agent:<occurrence>` (col 8), reason `shutdown_notice` (col 9).
wcli history --limit 120 \
  | awk -F'\t' '$3 == "evacuation" && $8 ~ /^agent:/ && $9 == "shutdown_notice"' | grep -q . \
  || { wcli history --limit 120 >&2; fail "no agent-attributed evacuation (shutdown_notice) row in history"; }

echo "OK: wallet-cli WATCH-LOOP smoke passed — the unattended loop autonomously DISCOVERED and"
echo "    auto-joined B, kept it probe-GATED and empty, drove SCHEDULED probes to a pass, FUNDED B"
echo "    to ~${STANDBY_TARGET} msat (never over), then SENSED B's forced shutdown and EVACUATED it"
echo "    back into A — every step Agent-attributed in history, driven only by \`watch --once\`."
