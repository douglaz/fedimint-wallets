#!/usr/bin/env bash
# devimint CRASH GATE for the `wallet-cli` cross-federation MOVE — the Phase-1 exit gate
# (spec §0/§5/§9/§10). It proves the two-leg A->B Move SURVIVES A CRASH: kill `wallet-cli`
# mid-move at DETERMINISTIC killpoints (a test-only `WALLET_CLI_CRASH_AT` env hook that
# `std::process::abort()`s exactly there — an uncatchable, unclean kill, like `kill -9`/OOM),
# then `wallet-cli reconcile` must RESUME and complete the move with NO double-pay and NO second
# payable invoice: B rises by ~N exactly ONCE (never 2N, never over N), A falls by ~N + fees
# exactly ONCE (never twice).
#
# NOT part of the rb-lite gate (compile + clippy + fmt + unit tests). Like the other smokes it
# needs a LIVE two-federation devimint setup, so the maintainer runs it manually.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# WHY DETERMINISTIC KILLPOINTS, NOT SLEEPS (spec §10)
# ────────────────────────────────────────────────────────────────────────────────────────────
# A timing-based kill ("sleep 0.3s then SIGKILL") is racy — it lands in a different spot every
# run. Instead `wallet-cli move` reads `WALLET_CLI_CRASH_AT=<named step>` and aborts at exactly
# that point in the Move `perform` loop (wallet-fedimint/src/executor.rs), so each crash-window
# case is reproducible. The four killpoints, in loop order:
#
#   before-move-record   — receive op is committed in the CLIENT db, but our MoveRecord (recv_op
#                          + invoice) is NOT yet persisted. Resume must RECOVER the recv op by
#                          `move_id` via backfill (§5) → no SECOND invoice minted.
#   after-receive-commit — MoveRecord (recv_op + invoice) persisted, receive committed, but the
#                          irreversible `Pay` has not run. Resume must go straight to `Pay`,
#                          reattaching the FIXED invoice → no re-mint.
#   before-send          — invoice exists, NO send started. Resume must `pay` EXACTLY once.
#   after-send-commit    — send op committed in the CLIENT db, but our MoveRecord does NOT yet
#                          carry `send_op`. Resume must NOT double-pay: backfill recovers the
#                          send op by `move_id`; if that misses, a re-drive re-`pay`s and the
#                          client dedups to `AlreadyInFlight`/`AlreadyPaid` (§5).
#
# NOTE on `before-move-record` (§5 bounded hazard): this killpoint fires just AFTER `mc.receive`
# RETURNS, so the receive op is already committed and backfill recovers it — no orphan for THIS
# case. The one TRUE orphan is a crash in the pre-receive-commit sub-window (after the gateway
# mints B's invoice but before the receive op commits, INSIDE the fedimint client call) — that
# invoice expires unpaid and cannot be hit by a deterministic killpoint. The gate here is that no
# DOUBLE credit/debit ever occurs; a bounded lone expired invoice is acceptable (spec §5).
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# TWO FEDERATIONS — READ THIS FIRST (same setup as smoke_move_devimint.sh)
# ────────────────────────────────────────────────────────────────────────────────────────────
# `devimint dev-fed` brings up exactly ONE federation (fed-0 → $FM_INVITE_CODE); `--num-feds N`
# only RESERVES ports. A genuine cross-fed move needs a SECOND federation B served by the SAME LDK
# gateway. Apply docs/devimint-two-fed-harness.patch to devimint (it stands up fed B under
# `--num-feds 2`, connects + funds the LDK gateway on B, and exports $FED_B_INVITE), or stand up
# B yourself and `gateway-ldk connect-fed "$FED_B_INVITE"`. See smoke_move_devimint.sh's
# "TWO FEDERATIONS" note for the full recipe.
#
#   # 1. Build wallet-cli (from this repo):
#   cd ~/p/fedimint-wallets
#   nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
#
#   # 2. Build fedimint/devimint once (from ~/p/fedimint) WITH docs/devimint-two-fed-harness.patch
#   #    applied, per docs/devimint-runbook.md §1:
#   cd ~/p/fedimint
#   git apply /home/master/p/fedimint-wallets/docs/devimint-two-fed-harness.patch
#   nix develop -c cargo build --workspace --bins
#
#   # 3. Bring up fed A + fed B sharing the LDK gateway, then run this inside the exec:
#   nix develop -c bash -c '
#     set -euo pipefail
#     source scripts/_common.sh
#     add_target_dir_to_path
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export RUST_LOG=warn
#     export FM_ENABLE_MODULE_LNV2=1           # ensure lnv2 + the LDK gateway are up
#     devimint --link-test-dir "${CARGO_BUILD_TARGET_DIR:-target}/devimint" \
#       --num-feds 2 dev-fed \
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_crash_move_devimint.sh
#   '
#
# Inside `dev-fed --exec` devimint sets FM_INVITE_CODE (fed A) + FM_PORT_GW_LDK (the LDK lnv2
# gateway) and — with the harness patch — FED_B_INVITE (fed B). Our `wallet-cli` joins BOTH feds
# with its OWN fresh seed, funds fed A via direct-inflow (the funded internal client pays), then
# runs the crash gate: for EACH killpoint, crash a fresh Move mid-flight, reconcile, assert the
# move completed EXACTLY once.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run this inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run this inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"

# Fed B's invite: primary $FED_B_INVITE, alias $FM_INVITE_CODE_B. Fed A is $FM_INVITE_CODE (fed-0).
FED_B_INVITE="${FED_B_INVITE:-${FM_INVITE_CODE_B:-}}"
if [[ -z "$FED_B_INVITE" ]]; then
  echo "FAIL: FED_B_INVITE (or FM_INVITE_CODE_B) not set — this is a TWO-federation crash gate." >&2
  echo "      Apply docs/devimint-two-fed-harness.patch (stands up fed B under --num-feds 2)," >&2
  echo "      or stand up fed B yourself and register it with the shared LDK gateway. See the" >&2
  echo "      'TWO FEDERATIONS' note above." >&2
  exit 1
fi
if [[ "$FED_B_INVITE" == "$FM_INVITE_CODE" ]]; then
  echo "FAIL: FED_B_INVITE equals FM_INVITE_CODE — --from and --to must be DIFFERENT feds." >&2
  exit 1
fi

# NOTE: the `WALLET_CLI_CRASH_AT` killpoint hook is gated to DEBUG builds (the crate's test-hook
# pattern), so this gate MUST run the DEBUG wallet-cli — `cargo build -p wallet-cli` (no --release).
# A --release binary compiles the hook out; the crash step below would then see a clean exit and
# fail loudly at the `rc >= 128` assertion rather than silently skipping the crash.
WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build it first: nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
MOVE_MSAT=100000     # each killpoint moves 100 sat A->B; B must net EXACTLY this once (never over)
RECV_SLACK=1000      # 1 sat — bounds lnv2 receive-fee-quote under-estimate on B (per the move smoke)
A_FEE_HEADROOM=50000 # 50 sat — generous upper bound on the TOTAL move fee A pays on top of MOVE_MSAT
RECONCILE_TRIES=15   # reconcile passes to let a crashed move's legs settle before we give up

# The four deterministic killpoints, in the loop order documented above. Each gets its OWN
# --occurrence so it is a DISTINCT move (distinct idempotency key) that we crash + reconcile
# independently. FUND must cover all four moves + fees.
KILLPOINTS=(before-move-record after-receive-commit before-send after-send-commit)
FUND_MSAT=$(( MOVE_MSAT * ${#KILLPOINTS[@]} + A_FEE_HEADROOM * ${#KILLPOINTS[@]} + 200000 ))

DATA_DIR="$(mktemp -d)"
DI_ERR="$(mktemp)"
MOVE_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$DI_ERR" "$MOVE_ERR"' EXIT

wcli() { "$WALLET_CLI" --data-dir "$DATA_DIR" "$@"; }
balance_msat_for_fed() {
  local fed_id="$1"
  # NOTE: no `exit` in awk — it must consume ALL of `wcli balance`'s output, else awk closes the
  # pipe early and wallet-cli gets SIGPIPE mid-print (panics) once there are multiple feds.
  wcli balance | awk -v id="$fed_id" '$1 == id ":" && $3 == "msat" { print $2 }'
}
fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== join BOTH federations =="
FED_A=$(wcli join "$FM_INVITE_CODE")
FED_B=$(wcli join "$FED_B_INVITE")
echo "fed A (source): $FED_A"
echo "fed B (dest):   $FED_B"
[[ "$FED_A" != "$FED_B" ]] || fail "both invites resolved to the SAME federation id ($FED_A) — need two distinct feds."

# Ensure the shared LDK gateway serves fed B (fed A is auto-connected by dev-fed). Best-effort:
# 'already connected' is fine; if the alias is missing, the move below surfaces a clear error.
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
[[ "$FUND_STATE" == "done" ]] || fail "funding await-move expected 'done', got '$FUND_STATE'"

A_START=$(balance_msat_for_fed "$FED_A")
[[ "$A_START" =~ ^[0-9]+$ ]] || { echo "FAIL: could not parse fed A balance" >&2; wcli balance >&2; exit 1; }
echo "funded: fed A = ${A_START} msat"
(( A_START >= FUND_MSAT - RECV_SLACK )) || fail "fed A only funded to ${A_START} msat, expected ~${FUND_MSAT}"

# ---------------------------------------------------------------------------------------
# The crash gate proper: one fresh, independently-crashed move per killpoint.
occ=0
for point in "${KILLPOINTS[@]}"; do
  echo ""
  echo "==================================================================================="
  echo "== KILLPOINT '${point}'  (occurrence ${occ}): crash mid-move, then reconcile =="
  echo "==================================================================================="

  A0=$(balance_msat_for_fed "$FED_A")
  B0=$(balance_msat_for_fed "$FED_B")
  [[ "$A0" =~ ^[0-9]+$ && "$B0" =~ ^[0-9]+$ ]] || { echo "FAIL: could not parse pre-crash balances" >&2; wcli balance >&2; exit 1; }
  echo "pre-crash balances: A=${A0} msat  B=${B0} msat"
  (( A0 >= MOVE_MSAT + A_FEE_HEADROOM )) || fail "fed A only has ${A0} msat, not enough to move ${MOVE_MSAT} + fees"

  # 1. Crash the move at this killpoint. The hook `std::process::abort()`s (uncatchable), so wcli
  #    dies on a SIGNAL — rc >= 128 (typically 134 = 128 + SIGABRT) — and NEVER prints 'done'.
  #    Guard `set -e` so the expected non-zero exit does not abort the script.
  set +e
  CRASH_OUT=$(WALLET_CLI_CRASH_AT="$point" wcli move \
    --from "$FED_A" --to "$FED_B" --amount "$MOVE_MSAT" --gateway "$GW" --occurrence "$occ" 2>"$MOVE_ERR")
  rc=$?
  set -e
  echo "crashed move exited rc=${rc}, stdout='${CRASH_OUT}'"
  # REQUIRE a signal death (rc >= 128), not merely a non-zero exit: the injected hook ALWAYS raises
  # SIGABRT, so an ordinary error exit (e.g. rc 1 from `perform` returning Err, or a --release
  # binary with the hook compiled out) is NOT our crash. Failing here keeps a real move error from
  # masquerading as the killpoint abort and letting the gate proceed to reconcile a move that never
  # actually crashed at this point.
  (( rc >= 128 )) || fail "killpoint '${point}': expected a CRASH via SIGABRT (rc >= 128), got rc=${rc} (stdout='${CRASH_OUT}') — not the injected abort (did you build a DEBUG wallet-cli?)"
  [[ "$CRASH_OUT" != "done" ]] || fail "killpoint '${point}': expected a CRASH, got a clean 'done'"

  # 2. Reconcile RESUMES the crashed move: reconcile re-drives the Pending/Executing intent,
  #    backfilling its MoveRecord from the op-log first (so a lost recv_op/send_op reattaches
  #    instead of re-minting/re-paying). A Move is driven synchronously to Done; a transient
  #    fault may leave it Pending, so retry a few passes until B is credited.
  credited=0
  for ((t = 1; t <= RECONCILE_TRIES; t++)); do
    RECON=$(wcli reconcile 2>/dev/null || true)
    Bnow=$(balance_msat_for_fed "$FED_B")
    [[ "$Bnow" =~ ^[0-9]+$ ]] || continue
    if (( Bnow - B0 >= MOVE_MSAT - RECV_SLACK )); then
      echo "reconcile pass ${t}: ${RECON}  (B credited: +$(( Bnow - B0 )) msat)"
      credited=1
      break
    fi
    echo "reconcile pass ${t}: ${RECON}  (B not yet credited: +$(( Bnow - B0 )) msat)"
    sleep 1
  done
  (( credited == 1 )) || fail "killpoint '${point}': move never completed after ${RECONCILE_TRIES} reconcile passes"

  # 3. Crash-safety assertions: the move completed EXACTLY once.
  A1=$(balance_msat_for_fed "$FED_A")
  B1=$(balance_msat_for_fed "$FED_B")
  echo "post-reconcile balances: A=${A1} msat  B=${B1} msat"

  # B rose by ~MOVE_MSAT: within [MOVE-slack, MOVE]. NEVER over MOVE (no over-credit); NEVER 2*MOVE
  # (a second invoice being minted AND paid would land here).
  B_DELTA=$(( B1 - B0 ))
  echo "B delta: +${B_DELTA} msat (target ${MOVE_MSAT}; must be in [$((MOVE_MSAT - RECV_SLACK)), ${MOVE_MSAT}])"
  if (( B_DELTA > MOVE_MSAT || B_DELTA < MOVE_MSAT - RECV_SLACK )); then
    fail "killpoint '${point}': fed B rose by ${B_DELTA} msat — a double-credit or over-credit (expected ~${MOVE_MSAT} ONCE)"
  fi

  # A fell by MOVE_MSAT + fees exactly ONCE: at least MOVE_MSAT, at most MOVE_MSAT + headroom. A
  # DOUBLE-pay (~2*MOVE) blows past the headroom and fails here.
  A_DELTA=$(( A0 - A1 ))
  echo "A delta: -${A_DELTA} msat (must be in [${MOVE_MSAT}, $((MOVE_MSAT + A_FEE_HEADROOM))])"
  if (( A_DELTA < MOVE_MSAT || A_DELTA > MOVE_MSAT + A_FEE_HEADROOM )); then
    fail "killpoint '${point}': fed A fell by ${A_DELTA} msat — a double-pay or over-spend (expected ${MOVE_MSAT} + fees ONCE)"
  fi

  # 4. Idempotency after resume: re-running the SAME move (same key) must NOT move funds again —
  #    apply sees the Done intent and SKIPS the drive (no second invoice, no second pay). Reconcile
  #    again must likewise be a no-op.
  MOVE_STATE2=$(wcli move --from "$FED_A" --to "$FED_B" --amount "$MOVE_MSAT" --gateway "$GW" --occurrence "$occ" 2>/dev/null)
  [[ "$MOVE_STATE2" == "done" ]] || fail "killpoint '${point}': idempotent re-run expected 'done', got '$MOVE_STATE2'"
  wcli reconcile >/dev/null 2>&1 || true
  A2=$(balance_msat_for_fed "$FED_A")
  B2=$(balance_msat_for_fed "$FED_B")
  if (( A2 != A1 || B2 != B1 )); then
    fail "killpoint '${point}': balances changed on idempotent re-run/reconcile: A ${A1}->${A2}, B ${B1}->${B2} (a second invoice/pay leaked)"
  fi
  echo "OK killpoint '${point}': B +${B_DELTA} msat once, A -${A_DELTA} msat once; re-run + reconcile no-ops."

  occ=$(( occ + 1 ))
done

echo ""
echo "OK: wallet-cli CRASH GATE passed — for each of {${KILLPOINTS[*]}} the crashed A->B move"
echo "    resumed under reconcile and completed EXACTLY once (no double-pay, no second payable"
echo "    invoice); every idempotent re-run + reconcile was a balance no-op."
