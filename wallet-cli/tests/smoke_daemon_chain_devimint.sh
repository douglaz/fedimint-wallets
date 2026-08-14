#!/usr/bin/env bash
# THE 5.2c AUTONOMOUS CHAIN UNDER THE DAEMON (phase 6a step 7, spec §6a.9): the risk engine's
# full loop — probe-gate → scheduled probes → autonomous FUND → forced shutdown → autonomous
# EVACUATE — driven end-to-end by walletd's OWN scheduler, with the operator touching nothing
# but policy. Supersedes the deleted smoke_watch (5.2c) in the scheduler's new home.
#
# Shape (two phases, one restart):
#   seed (standalone): join A, fund A, manual-discover + auto-join candidate B
#     (the daemon's discovery is Observer-only by design — no manual-invite endpoint)
#   phase 1 (walletd, real gateway): the scheduler probes gated B on its own cadence until
#     the verdict PASSES, then the tick funds B toward the standby target — NEVER OVER.
#   phase 2 (walletd restarted with WALLET_CLI_FORCE_SHUTDOWN=<B> — the probe.rs test seam
#     reads the process env, and a daemon's env changes by restart): the scheduler senses
#     B's shutdown and EVACUATES it back to A autonomously.
#
# Needs the TWO-FED harness. First follow docs/devimint-runbook.md §1's exact-pin
# patch/release build, which exports FEDIMINT_WORKTREE:
#   set -euo pipefail
#   : "${WALLETS_REPO:?run runbook §1 first}"
#   declare -F refuse_cargo_config_for_dir >/dev/null || { echo "missing refuse_cargo_config_for_dir; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }
#   declare -F refuse_ambient_rust_build_overrides >/dev/null || { echo "missing refuse_ambient_rust_build_overrides; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }
#   declare -F run_exact_nix_develop >/dev/null || { echo "missing run_exact_nix_develop; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }
#   declare -F run_exact_cargo >/dev/null || { echo "missing run_exact_cargo; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }
#   declare -F reset_exact_target_dir >/dev/null || { echo "missing reset_exact_target_dir; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }
#   cd "$WALLETS_REPO"
#   refuse_cargo_config_for_dir "$WALLETS_REPO"
#   refuse_ambient_rust_build_overrides
#   [[ ! -e .shrc.local && ! -L .shrc.local ]] || { echo "refusing wallets .shrc.local as a reproducibility precaution" >&2; exit 1; }
#   reset_exact_target_dir "$WALLETS_REPO/target-nix"
#   run_exact_cargo \
#     build --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-daemon -p wallet-cli
#   # Run §2 through its "OUTER PREFLIGHT COMPLETE" marker immediately first.
#   declare -F verify_exact_two_fed_launch_state >/dev/null || { echo "missing verify_exact_two_fed_launch_state; replay docs/devimint-runbook.md §1 and §2 through OUTER PREFLIGHT COMPLETE in this same shell" >&2; exit 1; }
#   verify_exact_two_fed_launch_state
#   cd "$FEDIMINT_WORKTREE"
#   run_exact_nix_develop -c bash -c '
#     set -euo pipefail
#     export CARGO_PROFILE=release
#     source scripts/_common.sh
#     add_target_dir_to_path
#     for variable in $(compgen -v FM_ || true); do
#       unset "$variable"
#     done
#     for variable in $(compgen -v WALLET_CLI_ || true) $(compgen -v WALLETD_ || true); do
#       unset "$variable"
#     done
#     export WALLET_CLI_BIN="$WALLETS_REPO/target-nix/debug/wallet-cli"
#     export WALLETD_BIN="$WALLETS_REPO/target-nix/debug/walletd"
#     export FM_DISCOVER_API_VERSION_TIMEOUT=10
#     PINNED_FEDIMINT_BIN_DIR="$FEDIMINT_WORKTREE/target-nix/release"
#     export FM_FEDIMINTD_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/fedimintd"
#     export FM_FEDIMINT_CLI_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/fedimint-cli"
#     export FM_GATEWAYD_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/gatewayd"
#     export FM_GATEWAY_CLI_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/gateway-cli"
#     export FM_RECURRINGD_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/fedimint-recurringd"
#     DEVIMINT_BIN="$PINNED_FEDIMINT_BIN_DIR/devimint"
#     launch_binaries=(
#       "$WALLET_CLI_BIN"
#       "$FM_FEDIMINTD_BASE_EXECUTABLE"
#       "$FM_FEDIMINT_CLI_BASE_EXECUTABLE"
#       "$FM_GATEWAYD_BASE_EXECUTABLE"
#       "$FM_GATEWAY_CLI_BASE_EXECUTABLE"
#       "$FM_RECURRINGD_BASE_EXECUTABLE"
#       "$DEVIMINT_BIN"
#     )
#     if [[ -n "${WALLETD_BIN:-}" ]]; then
#       launch_binaries+=("$WALLETD_BIN")
#     fi
#     for binary in "${launch_binaries[@]}"; do
#       if [[ "$binary" != /* || ! -f "$binary" || ! -x "$binary" ]]; then
#         echo "refusing launch binary that is not an absolute regular executable: $binary" >&2
#         exit 1
#       fi
#     done
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export FM_ENABLE_MODULE_LNV1=1
#     export FM_ENABLE_MODULE_MINT=1
#     export FM_ENABLE_MODULE_WALLET=1
#     export FM_ENABLE_MODULE_LNV2=1
#     export FM_NUM_FEDS=2
#     "$DEVIMINT_BIN" --link-test-dir "$FEDIMINT_WORKTREE/target-nix/devimint" --num-feds 2 dev-fed \
#       --exec bash "$WALLETS_REPO/wallet-cli/tests/smoke_daemon_chain_devimint.sh"
#   '
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run with FM_ENABLE_MODULE_LNV2=1}"
: "${FED_B_INVITE:?FED_B_INVITE not set — apply docs/devimint-two-fed-harness.patch + FM_NUM_FEDS=2}"

# DEBUG binaries by DESIGN: the WALLET_CLI_FORCE_SHUTDOWN seam is #[cfg(debug_assertions)]
# (probe.rs — a release money path can NEVER be forced to evacuate), so phase 2 requires a
# debug walletd. The responsiveness wallet binaries are also debug; this chain asserts behavior,
# not release-profile performance.
WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
WALLETD="${WALLETD_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/walletd}"
for f in "$WALLET_CLI" "$WALLETD"; do
  if [[ ! -x "$f" ]]; then
    echo "FAIL: missing binary $f" >&2
    echo 'Follow docs/devimint-runbook.md §1 to export FEDIMINT_WORKTREE, then build:' >&2
    echo '  cd "$WALLETS_REPO"' >&2
    echo '  refuse_cargo_config_for_dir "$WALLETS_REPO"' >&2
    echo '  refuse_ambient_rust_build_overrides' >&2
    echo '  [[ ! -e .shrc.local && ! -L .shrc.local ]] || { echo "refusing wallets .shrc.local as a reproducibility precaution" >&2; exit 1; }' >&2
    echo '  declare -F run_exact_nix_develop >/dev/null || { echo "missing run_exact_nix_develop; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }' >&2
    echo '  declare -F run_exact_cargo >/dev/null || { echo "missing run_exact_cargo; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }' >&2
    echo '  declare -F reset_exact_target_dir >/dev/null || { echo "missing reset_exact_target_dir; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }' >&2
    echo '  reset_exact_target_dir "$WALLETS_REPO/target-nix"' >&2
    echo '  run_exact_cargo build --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-daemon -p wallet-cli' >&2
    exit 1
  fi
done
for c in fedimint-cli jq curl; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not on PATH" >&2; exit 1; }
done

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
PORT=19738
FUND_MSAT=800000          # A's working balance
SPENDING_TARGET=300000    # A keeps this much (msat)
STANDBY_TARGET=100000     # the autonomous fund sizes B to ~this (msat) — never over
MAX_FEE=100000            # absolute cap for user money verbs/probe legs, not allocator funding (max_fee_bps_of_move: 300 bps)

SANDBOX="$(mktemp -d)"
export XDG_CONFIG_HOME="$SANDBOX/config"
export XDG_DATA_HOME="$SANDBOX/data"
DATA_DIR="$XDG_DATA_HOME/walletd"
DEBUG_DIR="${SMOKE_DEBUG_DIR:-/tmp/chain-gate-debug}"
WALLETD_LOG="$SANDBOX/walletd-phase1.log"
WALLETD_PID=""
STATUS=1
cleanup() {
  if [[ -n "$WALLETD_PID" ]] && kill -0 "$WALLETD_PID" 2>/dev/null; then
    kill -TERM "$WALLETD_PID" 2>/dev/null || true
    wait "$WALLETD_PID" 2>/dev/null || true
  fi
  if [[ "$STATUS" != "0" ]]; then
    mkdir -p "$DEBUG_DIR"
    cp -f "$SANDBOX"/walletd-phase*.log "$DEBUG_DIR/" 2>/dev/null || true
    echo "diagnostics preserved at $DEBUG_DIR" >&2
    echo "--- walletd log tails ---" >&2
    tail -25 "$SANDBOX"/walletd-phase*.log >&2 2>/dev/null || true
  fi
  rm -rf "$SANDBOX"
}
trap cleanup EXIT

# ---- seed (standalone, daemon down) -----------------------------------------------------------
echo "== seed: join A, fund A, auto-join candidate B, policy =="
mkdir -p "$XDG_CONFIG_HOME/walletd"
cat > "$XDG_CONFIG_HOME/walletd/walletd.toml" <<EOF
port = $PORT
gateway = "$GW"
EOF

wsa() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW" "$@"; }
SEED_ERR="$SANDBOX/seed.stderr"

JOIN_OUT=$(wsa join "$FM_INVITE_CODE")
JOIN_KEY=${JOIN_OUT#* }
[[ "$(wsa await-move "$JOIN_KEY")" == "done" ]] || { echo "FAIL: join A did not settle" >&2; exit 1; }
FED_A=$(cut -d: -f2 <<<"$JOIN_KEY")

INV=$(wsa receive --amount "$FUND_MSAT" 2>"$SEED_ERR")
KEY=$(sed -n 's/^key: //p' "$SEED_ERR")
SEND=$(fedimint-cli module lnv2 send "$INV" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND" >/dev/null 2>&1 || true
[[ "$(wsa await-receive "$KEY")" == "claimed" ]] || { echo "FAIL: funding A did not claim" >&2; exit 1; }

wsa discover --source manual --invite "$FED_B_INVITE" --auto-join --scorer-allow-regtest >/dev/null
FED_B=$(wsa candidates | awk '$2 == "autojoined" { print $1; exit }')
[[ -n "$FED_B" ]] || { echo "FAIL: B was not auto-joined" >&2; wsa candidates >&2; exit 1; }
echo "A=$FED_A (funded ~${FUND_MSAT}); B=$FED_B (auto-joined, gated, empty)"

# 5.2c lesson: regtest feds are never scorer-designated — PIN both roles. Fast cadence; a
# 1s probe span so three scheduled probes qualify the verdict quickly; discovery pushed away.
wsa policy set \
  --spending-fed "$FED_A" --standby-fed "$FED_B" \
  --spending-target "$SPENDING_TARGET" --standby-target "$STANDBY_TARGET" \
  --max-fee "$MAX_FEE" \
  --base-interval-secs 5 --min-interval-secs 1 \
  --probe-min-span-secs 1 --probe-retry-backoff-secs 1 \
  --discover-every-secs 1000000000 >/dev/null
echo "policy: pins A/B, targets ${SPENDING_TARGET}/${STANDBY_TARGET}, 5s cadence, 1s probe span"

"$WALLETD" init >/dev/null
TOKEN=$(cat "$DATA_DIR/token")
BASE="http://127.0.0.1:$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")

wait_healthy() {
  for i in $(seq 1 60); do
    curl -sf "${AUTH[@]}" "$BASE/v1/health" >/dev/null 2>&1 && return 0
    kill -0 "$WALLETD_PID" 2>/dev/null || { echo "FAIL: walletd died at startup" >&2; return 1; }
    sleep 0.2
  done
  echo "FAIL: walletd never became healthy" >&2; return 1
}
wcli_balance_line() { # per-fed msat read via the CLI in client mode ("<hex>: N msat" rows).
  # TOLERANT: client-mode `balance` exits non-zero on a partial view (a fed's balance read
  # can fault transiently mid-move), and under set -e/pipefail an unguarded read here kills
  # the whole gate silently (run 2 died exactly this way). A missed poll just retries.
  { "$WALLET_CLI" balance 2>/dev/null || true; } \
    | awk -v id="$1" '$1 == id ":" && $3 == "msat" { print $2; exit }'
}
stop_walletd() {
  kill -TERM "$WALLETD_PID"
  local rc=0; wait "$WALLETD_PID" || rc=$?
  WALLETD_PID=""
  [[ "$rc" == "0" ]] || { echo "FAIL: walletd exited $rc on SIGTERM" >&2; return 1; }
}

# ---- phase 1: autonomous probe -> gate opens -> autonomous fund -------------------------------
echo "== phase 1: walletd (real gateway) — scheduler probes gated B, then funds it =="
"$WALLETD" > "$WALLETD_LOG" 2>&1 &
WALLETD_PID=$!
wait_healthy

FUNDED=""
for i in $(seq 1 120); do   # up to ~6 min: ~3 probes on the 5s cadence, verdict, fund tick.
  # Trigger on ≥80% of target — a probe leg transiently parks ~probe_amount (20k msat) on B,
  # so a bare non-zero read catches PROBE money, not the fund move (run 1 tripped on 19974).
  BAL_B=$(wcli_balance_line "$FED_B")
  if [[ "$BAL_B" =~ ^[0-9]+$ ]] && (( BAL_B >= STANDBY_TARGET * 8 / 10 )); then FUNDED="$BAL_B"; break; fi
  sleep 3
done
if [[ -z "$FUNDED" ]]; then
  echo "FAIL: the scheduler never funded B to ~target within the window (B at $(wcli_balance_line "$FED_B") msat)" >&2
  "$WALLET_CLI" history | head -20 >&2 || true
  exit 1
fi
echo "B autonomously funded: ${FUNDED} msat (standby target ${STANDBY_TARGET})"
(( FUNDED <= STANDBY_TARGET )) || { echo "FAIL: B funded OVER target (${FUNDED} > ${STANDBY_TARGET})" >&2; exit 1; }

# The audit trail: agent-attributed probe rows then the fund move.
HIST=$("$WALLET_CLI" history)
grep -qE "probe.*agent:" <<<"$HIST" || { echo "FAIL: no agent probe rows in history" >&2; echo "$HIST" >&2; exit 1; }
grep -qE "move.*agent:" <<<"$HIST" || { echo "FAIL: no agent move row in history" >&2; echo "$HIST" >&2; exit 1; }
echo "history shows agent probes + the agent fund move"

stop_walletd
echo "phase 1 done (clean SIGTERM)"

# ---- phase 2: forced shutdown signal -> autonomous evacuation ---------------------------------
echo "== phase 2: restart walletd with WALLET_CLI_FORCE_SHUTDOWN=B — scheduler evacuates =="
WALLETD_LOG="$SANDBOX/walletd-phase2.log"
WALLET_CLI_FORCE_SHUTDOWN="$FED_B" "$WALLETD" > "$WALLETD_LOG" 2>&1 &
WALLETD_PID=$!
wait_healthy

EVACUATED=""
for i in $(seq 1 120); do
  BAL_B=$(wcli_balance_line "$FED_B")
  if [[ "$BAL_B" =~ ^[0-9]+$ ]] && (( BAL_B < FUNDED / 10 )); then EVACUATED="$BAL_B"; break; fi
  sleep 3
done
if [[ -z "$EVACUATED" ]]; then
  echo "FAIL: the scheduler never evacuated B within the window (still $(wcli_balance_line "$FED_B") msat)" >&2
  "$WALLET_CLI" history | head -20 >&2 || true
  exit 1
fi
echo "B autonomously evacuated: ${EVACUATED} msat residue (was ${FUNDED})"

HIST=$("$WALLET_CLI" history)
grep -qE "evacuation.*agent:.*shutdown_notice" <<<"$HIST" || {
  echo "FAIL: no agent evacuation row with reason shutdown_notice" >&2; echo "$HIST" >&2; exit 1; }
echo "history shows the agent evacuation (reason shutdown_notice)"

BAL_A=$(wcli_balance_line "$FED_A")
echo "A ends with ${BAL_A} msat"

stop_walletd
STATUS=0
echo "CHAIN_GATE_EXIT=0"
echo "PASS: gated B probed on the scheduler's cadence, gate opened, B funded ${FUNDED} msat (never over ${STANDBY_TARGET}), forced shutdown evacuated it to ${EVACUATED} msat — all agent-driven under walletd"
