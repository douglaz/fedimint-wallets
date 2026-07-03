#!/usr/bin/env bash
# devimint smoke test for the `wallet-cli` EVACUATE TICK — the Phase 3.A exit gate
# (docs/phase3-plan.md §3.A.4). ONE tick = probe every open fed → score() → build the
# AllocatorSnapshot from the standing-instruction policy → decide() → apply(). Here fed A is
# forced to look like it is winding DOWN, so the pure allocator's `decide()` must emit an
# `Evacuate A→B` (drain the dying fed into `safest_other`, bounded by B's cap room), and
# `apply()` must PERFORM it (B rises, A drains to ~0) — the last money-path primitive. It is the
# SAME validated two-leg internal-swap the `move`/`tick` smokes drive, but chosen by the shutdown
# sensing path rather than a top-up.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# THE FORCE-SHUTDOWN SEAM IS DEBUG-BUILD ONLY — READ THIS FIRST
# ────────────────────────────────────────────────────────────────────────────────────────────
# A real federation winding down is slow and hard to stage, so — exactly like the crash smoke's
# `WALLET_CLI_CRASH_AT` killpoints — the probe reads a test-only `WALLET_CLI_FORCE_SHUTDOWN=<fed
# hex>` env hook (comma-separated list ok) and OR's it into `shutdown_scheduled`. That seam is
# gated to `#[cfg(debug_assertions)]` (the crate's test-hook pattern); a `--release` wallet binary
# compiles it out entirely, so the money path can NEVER be forced to evacuate in production. This
# smoke therefore MUST run the DEBUG `wallet-cli` binary (the default `cargo build` output). The
# real no-auth sense path (`get_meta_expiration_timestamp` + public `/status.scheduled_shutdown`)
# is what a production build uses; this seam only lets the smoke trip it deterministically.
#
# NOT part of the rb-lite gate (compile + clippy + fmt + the PURE golden tests: the
# `derive_shutdown_scheduled` cases, the Evacuate→plan mapping, and `decisions_to_apply` keeping
# Evacuate). Like the other smokes it needs a LIVE devimint setup, so the maintainer runs it by
# hand.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# TWO FEDERATIONS — identical setup to smoke_tick_devimint.sh
# ────────────────────────────────────────────────────────────────────────────────────────────
# `devimint dev-fed` brings up exactly ONE federation (fed-0 → $FM_INVITE_CODE). A genuine
# cross-fed evacuate needs a SECOND federation B that the SAME LDK gateway serves. Supply B's
# invite via $FED_B_INVITE (or $FM_INVITE_CODE_B). See smoke_move_devimint.sh's "TWO FEDERATIONS"
# note for the two ways to obtain it and the docs/devimint-two-fed-harness.patch that automates it.
#
# The LDK gateway MUST serve BOTH feds for the internal `is_direct_swap` to fire (fed A is
# auto-connected by dev-fed; connect fed B with `gateway-ldk connect-fed "$FED_B_INVITE"`). The
# harness patch also pegs in the gateway's B-side liquidity so it can fund the incoming contract.
#
# WHY EXPLICIT --spending/--standby (not auto-designation): devimint feds run on REGTEST, so the
# scorer's `require_mainnet` floor makes them INELIGIBLE — auto-designation would pick nothing.
# The standing-instruction policy lets the operator PIN the designation; the money path then only
# re-checks health + probe reachability. So this smoke pins A=spending, B=standby. NOTE the
# evacuating SOURCE A is unhealthy/shutting-down BY DEFINITION — that is expected and must NOT
# abort the evacuate; only the DESTINATION B needs to be route-eligible (`safest_other` +
# `fed_in_executable_move` already encode this). Fed A's quorum stays LIVE (only its shutdown
# flag is forced), so it can still sign the send leg — the evacuate completes.
# ────────────────────────────────────────────────────────────────────────────────────────────
#
#   # 1. Build wallet-cli (DEBUG — the force-shutdown seam is compiled out of --release):
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
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_evacuate_devimint.sh
#   '
#
# Inside `dev-fed --exec` devimint sets FM_INVITE_CODE (fed A/fed-0's invite) and FM_PORT_GW_LDK
# (the LDK lnv2 gateway's API port), and puts the funded internal client `fedimint-cli` (~1M sats,
# joined to fed A) + the gateway aliases (`gateway-ldk`) on PATH. Our `wallet-cli` joins BOTH feds
# with its OWN fresh seed. We fund our fed-A balance via direct-inflow (the funded client pays),
# then run ONE `wallet-cli tick` with A force-flagged shutting-down; decide() emits Evacuate A→B
# and apply() performs it.
#
# The EXACT-net gate: as in the move/tick smokes, devimint zeroes only the gateway's LIGHTNING
# routing fee (FM_DEFAULT_ROUTING_FEES=0,0), NOT its TRANSACTION fee — so B nets ~N (a hair under,
# never over: the gross-up floors the gateway ppm), and A falls by N + both legs' fees.
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
  echo "FAIL: FED_B_INVITE equals FM_INVITE_CODE — the dying and safe feds must be DIFFERENT." >&2
  exit 1
fi

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build the DEBUG binary first (the force-shutdown seam is compiled out of --release):" >&2
  echo "  nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
FUND_MSAT=500000       # fund fed A with 500 sat via direct-inflow (the dying fed's balance)
CAP_HEADROOM=50000     # 50 sat left UNCAPPED in A so it can pay the move's fees. The allocator's
                       # evacuate amount = min(A.spendable, cap_room(B)); we bound it just BELOW
                       # A's balance so A retains enough to cover the grossed-up invoice + send fee
                       # (a full-balance evacuate would be short by exactly the fees). A single tick
                       # therefore drains A to ~CAP_HEADROOM-minus-fees, i.e. ~0.
MAX_FEE=1000000        # 1000 sat: a generous per-move fee cap so the cap-check never bites on devimint
RECV_SLACK=1000        # 1 sat — bounds lnv2 receive-fee-quote under-estimate on B (per the other smokes)
A_FEE_HEADROOM=50000   # 50 sat — generous upper bound on the TOTAL move fee A pays on top of the move

DATA_DIR="$(mktemp -d)"
DI_ERR="$(mktemp)"
STATUS_OUT="$(mktemp)"
STATUS_ERR="$(mktemp)"
TICK_OUT="$(mktemp)"
TICK_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$DI_ERR" "$STATUS_OUT" "$STATUS_ERR" "$TICK_OUT" "$TICK_ERR"' EXIT

wcli() { "$WALLET_CLI" --data-dir "$DATA_DIR" "$@"; }
balance_msat_for_fed() {
  local fed_id="$1"
  # NOTE: no `exit` in awk — it must consume ALL of `wcli balance`'s output, else awk closes the
  # pipe early and wallet-cli gets SIGPIPE mid-print (panics) once there are multiple feds.
  wcli balance | awk -v id="$fed_id" '$1 == id ":" && $3 == "msat" { print $2 }'
}
performed_count() { sed -n 's/.*performed=\([0-9]*\).*/\1/p' "$1"; }

echo "== join BOTH federations =="
FED_A=$(wcli join "$FM_INVITE_CODE")
FED_B=$(wcli join "$FED_B_INVITE")
echo "fed A (dying/spending): $FED_A"
echo "fed B (safe/standby):   $FED_B"
if [[ "$FED_A" == "$FED_B" ]]; then
  echo "FAIL: both invites resolved to the SAME federation id ($FED_A) — need two distinct feds." >&2
  exit 1
fi

# Ensure the shared LDK gateway serves fed B (fed A is auto-connected by dev-fed). Best-effort:
# 'already connected' is fine; if the alias is missing, the evacuate below surfaces a clear error.
echo "== ensure the shared LDK gateway serves fed B (connect-fed, best-effort) =="
if command -v gateway-ldk >/dev/null; then
  gateway-ldk connect-fed "$FED_B_INVITE" >/dev/null 2>&1 \
    && echo "connected fed B to the LDK gateway" \
    || echo "note: gateway-ldk connect-fed returned non-zero (likely already connected) — continuing"
else
  echo "note: 'gateway-ldk' alias not on PATH; assuming the LDK gateway already serves fed B"
fi

# ---------------------------------------------------------------------------------------
echo "== FUND fed A: direct-inflow ${FUND_MSAT} msat (funded client pays; gateway swaps in) =="
INV_A=$(wcli direct-inflow --to "$FED_A" --amount "$FUND_MSAT" --gateway "$GW" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^intent_key: //p' "$DI_ERR")
if [[ -z "$INV_A" || -z "$KEY_FUND" ]]; then
  echo "FAIL: funding direct-inflow did not yield an invoice + intent key:" >&2
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
echo "pre-evacuate balances: A=${A0} msat (dying, funded)  B=${B0} msat (safe, should be empty)"
if (( B0 != 0 )); then
  echo "FAIL: safe fed B should start EMPTY, but holds ${B0} msat — the evacuate amount would" >&2
  echo "      not equal cap_room. Use a fresh --data-dir." >&2
  exit 1
fi
if (( A0 <= CAP_HEADROOM )); then
  echo "FAIL: fed A only has ${A0} msat, not enough above the ${CAP_HEADROOM} msat fee headroom" >&2
  exit 1
fi

# Bound the evacuate BELOW A's balance so A keeps enough for fees: per-fed cap = A0 - headroom, so
# cap_room(B) = per_fed_cap - B0 = A0 - CAP_HEADROOM, and the allocator's
# amount = min(A0, cap_room(B)) = A0 - CAP_HEADROOM. That is the exact NET B must end up with.
PER_FED_CAP=$(( A0 - CAP_HEADROOM ))
EXPECTED_EVAC=$PER_FED_CAP   # == cap_room(B) since B starts empty

# ---------------------------------------------------------------------------------------
echo "== SENSE check: a HEALTHY fed (no force) must NOT decide an evacuate =="
# Prove the real no-auth read path returns shutdown_scheduled=false on a live devimint fed: a
# dry-run status with NO force must not emit an evacuate for A. (This is the SENSE half of the gate;
# the force seam below is only how we trip the ACT half deterministically.)
if ! wcli status \
      --spending "$FED_A" --standby "$FED_B" \
      --spending-target 0 --standby-target 0 \
      --per-fed-cap "$PER_FED_CAP" --max-fee "$MAX_FEE" --gateway "$GW" --occurrence 0 >"$STATUS_OUT" 2>"$STATUS_ERR"; then
  echo "FAIL: wallet-cli status (sense dry-run) exited non-zero" >&2
  echo "  --- status stdout ---" >&2; cat "$STATUS_OUT" >&2
  echo "  --- status stderr ---" >&2; cat "$STATUS_ERR" >&2
  exit 1
fi
if grep -Eq "evacuate [0-9]+ msat ${FED_A} ->" "$STATUS_OUT"; then
  echo "FAIL: a HEALTHY fed A produced an evacuate decision without WALLET_CLI_FORCE_SHUTDOWN —" >&2
  echo "      the real shutdown-sense read is mis-detecting a shutdown." >&2
  echo "  --- status stdout ---" >&2; cat "$STATUS_OUT" >&2
  exit 1
fi
echo "sense OK: healthy fed A decides no evacuate"

# ---------------------------------------------------------------------------------------
echo "== TICK: force A shutting-down -> decide Evacuate A->B -> apply drains A into B =="
# WALLET_CLI_FORCE_SHUTDOWN scoped to THIS command (env-prefix on the wcli function, like the crash
# smoke's WALLET_CLI_CRASH_AT). The probe OR's it into shutdown_scheduled, so decide() emits an
# Evacuate A->B of ~EXPECTED_EVAC (bounded by B's cap room) and apply() performs it.
if ! WALLET_CLI_FORCE_SHUTDOWN="$FED_A" wcli tick \
      --spending "$FED_A" --standby "$FED_B" \
      --spending-target 0 --standby-target 0 \
      --per-fed-cap "$PER_FED_CAP" --max-fee "$MAX_FEE" --gateway "$GW" --occurrence 0 >"$TICK_OUT" 2>"$TICK_ERR"; then
  echo "FAIL: wallet-cli evacuate tick exited non-zero" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  echo "  --- tick stderr ---" >&2; cat "$TICK_ERR" >&2
  exit 1
fi
echo "-- tick decisions + summary --"; cat "$TICK_OUT"

# decide() produced an Evacuate A->B with reason ShutdownNotice (the core assertion: the shutdown
# sensing path, not a top-up, chose this move). Line: `evacuate <n> msat <A> -> <B> (... reason ShutdownNotice)`.
if ! grep -Eq "evacuate [0-9]+ msat ${FED_A} -> ${FED_B} .*reason ShutdownNotice" "$TICK_OUT"; then
  echo "FAIL: tick did not decide an Evacuate ${FED_A} -> ${FED_B} (reason ShutdownNotice)" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  exit 1
fi
# The evacuate amount is bounded EXACTLY by B's cap room (A0 - CAP_HEADROOM).
if ! grep -Eq "evacuate ${EXPECTED_EVAC} msat ${FED_A} -> ${FED_B} " "$TICK_OUT"; then
  echo "FAIL: expected the evacuate amount to be ${EXPECTED_EVAC} msat (cap_room bound); got:" >&2
  grep -E "evacuate " "$TICK_OUT" >&2
  exit 1
fi
# apply() performed exactly one intent (the evacuate).
TICK_PERFORMED=$(performed_count "$TICK_OUT")
if [[ "$TICK_PERFORMED" != "1" ]]; then
  echo "FAIL: expected the tick to perform exactly 1 evacuate, got performed=${TICK_PERFORMED:-?}" >&2
  echo "  --- tick stdout ---" >&2; cat "$TICK_OUT" >&2
  exit 1
fi

A1=$(balance_msat_for_fed "$FED_A")
B1=$(balance_msat_for_fed "$FED_B")
echo "post-evacuate balances: A=${A1} msat  B=${B1} msat"

# B rose by ~EXPECTED_EVAC: within [amount-slack, amount], NEVER over (never over-credit).
B_DELTA=$(( B1 - B0 ))
echo "B delta: ${B_DELTA} msat (evacuate amount ${EXPECTED_EVAC}, slack ${RECV_SLACK} below, never over)"
if (( B_DELTA > EXPECTED_EVAC || B_DELTA < EXPECTED_EVAC - RECV_SLACK )); then
  echo "FAIL: fed B rose by ${B_DELTA} msat, not within [$((EXPECTED_EVAC - RECV_SLACK)), ${EXPECTED_EVAC}]" >&2
  exit 1
fi

# A fell by EXPECTED_EVAC + fees: at least the moved amount (A funds the grossed-up invoice + send
# fee), at most amount + a generous fee headroom.
A_DELTA=$(( A0 - A1 ))
echo "A delta: -${A_DELTA} msat (>= ${EXPECTED_EVAC}, <= $((EXPECTED_EVAC + A_FEE_HEADROOM)))"
if (( A_DELTA < EXPECTED_EVAC || A_DELTA > EXPECTED_EVAC + A_FEE_HEADROOM )); then
  echo "FAIL: fed A fell by ${A_DELTA} msat, not within [${EXPECTED_EVAC}, $((EXPECTED_EVAC + A_FEE_HEADROOM))]" >&2
  exit 1
fi

# A drained to ~0: the dying fed retains only the uncapped fee headroom minus the actual fees, i.e.
# at most CAP_HEADROOM. This is the whole point — the wallet FLED the dying federation.
echo "A residual: ${A1} msat (<= ${CAP_HEADROOM} — A drained to ~0)"
if (( A1 > CAP_HEADROOM )); then
  echo "FAIL: fed A did not drain to ~0; ${A1} msat remains (expected <= ${CAP_HEADROOM})" >&2
  exit 1
fi

echo "OK: wallet-cli evacuate smoke passed (sense: healthy fed no-evacuate; forced shutdown made"
echo "    decide choose Evacuate A->B reason ShutdownNotice; apply drained A into B — B netted"
echo "    ~${EXPECTED_EVAC} msat, never over; A fell by ${A_DELTA} msat to ${A1} msat, ~0)"
