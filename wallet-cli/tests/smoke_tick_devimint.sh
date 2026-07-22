#!/usr/bin/env bash
# devimint smoke test for the `wallet-cli` ORCHESTRATOR TICK — the Phase 2 step 2.3 exit gate
# (docs/phase2-plan.md). ONE tick = probe every open fed → score() → build the
# AllocatorSnapshot from the standing-instruction policy → decide() → apply() through the
# Phase-1 executor. This smoke proves the WHOLE engine wires end-to-end: with the spending fed
# funded ABOVE its target and the standby EMPTY (below its target), the pure allocator's
# `decide()` must emit a fund-standby `Move A→B`, and `apply()` must PERFORM it (B rises, A
# falls) — the same cross-federation internal-swap the `move` smoke drives, but chosen by the
# allocator rather than named on the command line.
#
# NOT part of the rb-lite gate (compile + clippy + fmt + the PURE build_snapshot golden tests).
# Like the other smokes it needs a LIVE devimint setup, so the maintainer runs it manually.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# TWO FEDERATIONS — READ THIS FIRST (identical setup to smoke_move_devimint.sh)
# ────────────────────────────────────────────────────────────────────────────────────────────
# `devimint dev-fed` brings up exactly ONE federation (fed-0 → $FM_INVITE_CODE). A genuine
# cross-fed tick needs a SECOND federation B that the SAME LDK gateway serves. Supply B's invite
# via $FED_B_INVITE (or $FM_INVITE_CODE_B). See smoke_move_devimint.sh's "TWO FEDERATIONS" note
# for the two ways to obtain it and the docs/devimint-two-fed-harness.patch that automates it.
#
# The LDK gateway MUST serve BOTH feds for the internal `is_direct_swap` to fire (fed A is
# auto-connected by dev-fed; connect fed B with `gateway-ldk connect-fed "$FED_B_INVITE"`). The
# harness patch also pegs in the gateway's B-side liquidity so it can fund the incoming contract.
#
# WHY EXPLICIT --spending/--standby (not auto-designation): devimint feds run on REGTEST, so the
# scorer's `require_mainnet` floor makes them INELIGIBLE — auto-designation would pick nothing.
# The standing-instruction policy lets the operator PIN the designation, which bypasses the
# scorer gate for designation (the money path only re-checks health + probe reachability). So
# this smoke pins A=spending, B=standby; the auto-designation path is covered by the pure
# build_snapshot golden tests instead.
# ────────────────────────────────────────────────────────────────────────────────────────────
#
#   # 1. Build wallet-cli (from this repo):
#   cd ~/p/fedimint-wallets
#   nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
#
#   # 2. Build fedimint/devimint once (from ~/p/fedimint), per docs/devimint-runbook.md §1:
#   cd ~/p/fedimint
#   nix develop -c cargo build --workspace --bins
#
#   # 3. Bring up dev-fed (fed A) + your second federation (fed B) sharing the LDK gateway, then
#   #    run this inside the exec with FED_B_INVITE set to fed B's invite:
#   nix develop -c bash -c '
#     set -euo pipefail
#     source scripts/_common.sh
#     add_target_dir_to_path
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export RUST_LOG=warn
#     export FM_ENABLE_MODULE_LNV2=1           # ensure lnv2 + the LDK gateway are up
#     export FED_B_INVITE="fed1...."           # <-- fed B (see the TWO FEDERATIONS note above)
#     devimint --link-test-dir "${CARGO_BUILD_TARGET_DIR:-target}/devimint" \
#       --num-feds 2 dev-fed \
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_tick_devimint.sh
#   '
#
# Inside `dev-fed --exec` devimint sets FM_INVITE_CODE (fed A/fed-0's invite) and FM_PORT_GW_LDK
# (the LDK lnv2 gateway's API port), and puts the funded internal client `fedimint-cli` (~1M sats,
# joined to fed A) + the gateway aliases (`gateway-ldk`) on PATH. Our `wallet-cli` joins BOTH feds
# with its OWN fresh seed. We fund our fed-A balance via direct-inflow (the funded client pays),
# then run ONE `wallet-cli tick` that decides + performs the fund-standby move A→B.
#
# The EXACT-net gate: as in the move/direct-inflow smokes, devimint zeroes only the gateway's
# LIGHTNING routing fee (FM_DEFAULT_ROUTING_FEES=0,0), NOT its TRANSACTION fee — so B nets ~N (a
# hair under, never over: the gross-up floors the gateway ppm), and A falls by N + both legs' fees.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run this inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run this inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"

# Fed B's invite: primary $FED_B_INVITE, alias $FM_INVITE_CODE_B. Fed A is $FM_INVITE_CODE (fed-0).
FED_B_INVITE="${FED_B_INVITE:-${FM_INVITE_CODE_B:-}}"
if [[ -z "$FED_B_INVITE" ]]; then
  echo "FAIL: FED_B_INVITE (or FM_INVITE_CODE_B) not set — this is a TWO-federation smoke." >&2
  echo "      dev-fed only brings up fed A (\$FM_INVITE_CODE). Stand up a second federation B" >&2
  echo "      served by the same LDK gateway and set FED_B_INVITE to its invite code." >&2
  echo "      See the 'TWO FEDERATIONS' note at the top of this script." >&2
  exit 1
fi
if [[ "$FED_B_INVITE" == "$FM_INVITE_CODE" ]]; then
  echo "FAIL: FED_B_INVITE equals FM_INVITE_CODE — spending and standby must be DIFFERENT feds." >&2
  exit 1
fi

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build it first: nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
FUND_MSAT=500000       # fund fed A with 500 sat via direct-inflow (must exceed target + move + fees)
SPENDING_TARGET=100000 # keep >=100 sat on the spending fed; A's SURPLUS above this funds the standby
STANDBY_TARGET=100000  # fund the empty standby toward 100 sat -> the expected fund-standby move size
MAX_FEE=100000         # 100 sat: generous vs the ~10-sat devimint move fee, yet WELL under A's surplus
                       # above target. The allocator reserves the FULL fee cap from that surplus when
                       # sizing a fund-standby move (available = spendable - target - max_fee), so a
                       # cap larger than the surplus saturates `available` to 0 and the move is refused.
RECV_SLACK=1000        # 1 sat — bounds lnv2 receive-fee-quote under-estimate on B (per the other smokes)
A_FEE_HEADROOM=50000   # 50 sat — generous upper bound on the TOTAL move fee A pays on top of the move

DATA_DIR="$(mktemp -d)"
DI_ERR="$(mktemp)"
TICK_OUT="$(mktemp)"
TICK_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$DI_ERR" "$TICK_OUT" "$TICK_ERR"' EXIT

wcli() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW" "$@"; }
join_fed() {
  local started key state
  started=$(wcli join "$1") || return
  key=${started#* }
  state=$(wcli await-move "$key") || return
  [[ "$state" == "done" ]] || { echo "join $key did not settle: $state" >&2; return 1; }
  cut -d: -f2 <<<"$key"
}
balance_msat_for_fed() {
  local fed_id="$1"
  # NOTE: no `exit` in awk — it must consume ALL of `wcli balance`'s output, else awk closes the
  # pipe early and wallet-cli gets SIGPIPE mid-print (panics) once there are multiple feds.
  wcli balance | awk -v id="$fed_id" '$1 == id ":" && $3 == "msat" { print $2 }'
}
performed_count() { sed -n 's/.*performed=\([0-9]*\).*/\1/p' "$1"; }

echo "== join BOTH federations =="
FED_A=$(join_fed "$FM_INVITE_CODE")
FED_B=$(join_fed "$FED_B_INVITE")
echo "fed A (spending): $FED_A"
echo "fed B (standby):  $FED_B"
if [[ "$FED_A" == "$FED_B" ]]; then
  echo "FAIL: both invites resolved to the SAME federation id ($FED_A) — need two distinct feds." >&2
  exit 1
fi

# Ensure the shared LDK gateway serves fed B (fed A is auto-connected by dev-fed). Best-effort:
# 'already connected' is fine; if the alias is missing, the tick's move below surfaces a clear error.
echo "== ensure the shared LDK gateway serves fed B (connect-fed, best-effort) =="
if command -v gateway-ldk >/dev/null; then
  gateway-ldk connect-fed "$FED_B_INVITE" >/dev/null 2>&1 \
    && echo "connected fed B to the LDK gateway" \
    || echo "note: gateway-ldk connect-fed returned non-zero (likely already connected) — continuing"
else
  echo "note: 'gateway-ldk' alias not on PATH; assuming the LDK gateway already serves fed B"
fi

# ---------------------------------------------------------------------------------------
echo "== FUND fed A (spending): direct-inflow ${FUND_MSAT} msat (funded client pays; gateway swaps in) =="
INV_A=$(wcli direct-inflow --to "$FED_A" --amount "$FUND_MSAT" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^key: //p' "$DI_ERR")
if [[ -z "$INV_A" || -z "$KEY_FUND" ]]; then
  echo "FAIL: funding direct-inflow did not yield an invoice + operation key:" >&2
  echo "  invoice=$INV_A" >&2; echo "  --- direct-inflow stderr ---" >&2; cat "$DI_ERR" >&2
  exit 1
fi
# await-send FIRST so the internal swap completes before we finalize our receive (reliable pattern).
SEND_FUND=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_FUND" >/dev/null 2>&1
FUND_STATE=$(wcli await-move "$KEY_FUND")
if [[ "$FUND_STATE" != "done" ]]; then
  echo "FAIL: funding await-move expected 'done', got '$FUND_STATE'" >&2
  exit 1
fi

A0=$(balance_msat_for_fed "$FED_A")
B0=$(balance_msat_for_fed "$FED_B")
[[ "$A0" =~ ^[0-9]+$ ]] || { echo "FAIL: could not parse fed A balance" >&2; wcli balance >&2; exit 1; }
[[ "$B0" =~ ^[0-9]+$ ]] || { echo "FAIL: could not parse fed B balance" >&2; wcli balance >&2; exit 1; }
echo "pre-tick balances: A=${A0} msat (funded)  B=${B0} msat (standby, should be empty)"
if (( B0 != 0 )); then
  echo "FAIL: standby fed B should start EMPTY, but holds ${B0} msat — the fund-standby move" >&2
  echo "      amount would not equal STANDBY_TARGET. Use a fresh --data-dir." >&2
  exit 1
fi
if (( A0 < SPENDING_TARGET + STANDBY_TARGET + A_FEE_HEADROOM )); then
  echo "FAIL: fed A only has ${A0} msat, not enough to keep ${SPENDING_TARGET} + fund ${STANDBY_TARGET} + fees" >&2
  exit 1
fi

# ---------------------------------------------------------------------------------------
echo "== TICK: probe -> score -> decide -> apply (must fund the empty standby B from A's surplus) =="
# A is above its spending target, B is below its standby target: decide() must emit exactly one
# fund-standby Move A->B of ~STANDBY_TARGET, and apply() must perform it. Pin the designation
# (regtest feds are scorer-ineligible; see the WHY note at the top) and the shared gateway.
if ! wcli tick \
      --spending "$FED_A" --standby "$FED_B" \
      --spending-target "$SPENDING_TARGET" --standby-target "$STANDBY_TARGET" \
      --max-fee "$MAX_FEE" --gateway "$GW" --occurrence 0 >"$TICK_OUT" 2>"$TICK_ERR"; then
  echo "FAIL: wallet-cli tick exited non-zero" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  echo "  --- tick stderr ---" >&2; cat "$TICK_ERR" >&2
  exit 1
fi
echo "-- tick decisions + summary --"; cat "$TICK_OUT"

# decide() produced a fund-standby Move A->B (the core assertion: the allocator, not the operator,
# chose this move). The decision line is `move <n> msat <A> -> <B> (... reason StandbyBelowTarget)`.
if ! grep -Eq "move [0-9]+ msat ${FED_A} -> ${FED_B} .*reason StandbyBelowTarget" "$TICK_OUT"; then
  echo "FAIL: tick did not decide a fund-standby Move ${FED_A} -> ${FED_B}" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  exit 1
fi
# apply() performed exactly one intent (the move).
TICK_PERFORMED=$(performed_count "$TICK_OUT")
if [[ "$TICK_PERFORMED" != "1" ]]; then
  echo "FAIL: expected the tick to perform exactly 1 move, got performed=${TICK_PERFORMED:-?}" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  exit 1
fi

A1=$(balance_msat_for_fed "$FED_A")
B1=$(balance_msat_for_fed "$FED_B")
echo "post-tick balances: A=${A1} msat  B=${B1} msat"

# B rose by ~STANDBY_TARGET: within [target-slack, target], NEVER over (never over-credit).
B_DELTA=$(( B1 - B0 ))
echo "B delta: ${B_DELTA} msat (target ${STANDBY_TARGET}, slack ${RECV_SLACK} below, never over)"
if (( B_DELTA > STANDBY_TARGET || B_DELTA < STANDBY_TARGET - RECV_SLACK )); then
  echo "FAIL: fed B rose by ${B_DELTA} msat, not within [$((STANDBY_TARGET - RECV_SLACK)), ${STANDBY_TARGET}]" >&2
  exit 1
fi

# A fell by STANDBY_TARGET + fees: at least STANDBY_TARGET (A funds the grossed-up invoice + send
# fee), at most STANDBY_TARGET + a generous fee headroom.
A_DELTA=$(( A0 - A1 ))
echo "A delta: -${A_DELTA} msat (>= ${STANDBY_TARGET}, <= $((STANDBY_TARGET + A_FEE_HEADROOM)))"
if (( A_DELTA < STANDBY_TARGET || A_DELTA > STANDBY_TARGET + A_FEE_HEADROOM )); then
  echo "FAIL: fed A fell by ${A_DELTA} msat, not within [${STANDBY_TARGET}, $((STANDBY_TARGET + A_FEE_HEADROOM))]" >&2
  exit 1
fi

# ---------------------------------------------------------------------------------------
echo "== STALE OCCURRENCE: re-running the SAME terminal tick must fail loudly =="
# Same occurrence (0) -> decide reuses the same move key (`move:A:B:0`, which is amount-INDEPENDENT),
# so the fund-standby move maps back to the now-`Done` intent and the tick must fail with a distinct
# stale-occurrence signal instead of quietly skipping forever (recurring schedulers gate on this).
#
# To force decide() to RE-EMIT that same-key move we must keep B below the standby target on this
# second run. The first tick may legitimately have credited B to EXACTLY the target (the executor
# floors the gross-up, so B nets a hair under — but an exact-net is allowed by the "never over"
# money invariant, and if it happened B would sit AT target and decide() would emit nothing). Raising
# the standby target above B's current balance makes B unambiguously below-target regardless, so the
# same-key move is re-decided and the terminal-replay path is exercised deterministically.
STALE_STANDBY_TARGET=$(( B1 + STANDBY_TARGET ))  # strictly above B1 -> B is below-target for sure
if wcli tick \
      --spending "$FED_A" --standby "$FED_B" \
      --spending-target "$SPENDING_TARGET" --standby-target "$STALE_STANDBY_TARGET" \
      --max-fee "$MAX_FEE" --gateway "$GW" --occurrence 0 >"$TICK_OUT" 2>"$TICK_ERR"; then
  echo "FAIL: stale same-occurrence tick unexpectedly exited zero" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  exit 1
fi
if ! grep -q "already-terminal" "$TICK_ERR"; then
  echo "FAIL: stale same-occurrence tick did not explain the terminal replay" >&2
  echo "  --- tick stderr ---" >&2; cat "$TICK_ERR" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  exit 1
fi
if ! grep -q "fresh --occurrence" "$TICK_ERR"; then
  echo "FAIL: stale same-occurrence tick did not tell the operator to advance occurrence" >&2
  echo "  --- tick stderr ---" >&2; cat "$TICK_ERR" >&2
  exit 1
fi
A2=$(balance_msat_for_fed "$FED_A")
B2=$(balance_msat_for_fed "$FED_B")
if (( A2 != A1 || B2 != B1 )); then
  echo "FAIL: balances changed on a stale same-occurrence tick: A ${A1}->${A2}, B ${B1}->${B2}" >&2
  exit 1
fi

echo "OK: wallet-cli tick smoke passed (decide chose fund-standby A->B; apply performed it; B netted ~${STANDBY_TARGET} msat, never over; A fell by ${A_DELTA} msat; stale occurrence fails without moving funds)"
