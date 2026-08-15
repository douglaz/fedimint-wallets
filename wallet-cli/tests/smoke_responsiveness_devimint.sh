#!/usr/bin/env bash
# THE RESPONSIVENESS GATE (phase 6a step 7, spec §6a.9) — the product bar made executable:
# "anything that can make the wallet feel unresponsive is a red alert; a payment never can
# block another one." With the watch scheduler ACTIVE and a scheduled probe HELD IN FLIGHT
# by a misbehaving-gateway double (accepts the connection, never responds — the hold-invoice
# stand-in), POST /v1/pay must reach its FIRST EXTERNAL fedimint call in <250 ms:
#   M1  single pay while the probe hangs
#   M2  a second pay while pay 1 is ALSO mid-IO (two pays never serialize)
#   M3  the same bound at FULL DESIGNED CAPACITY (the admission cap's worth of hanging
#       drivers churning the mailbox), then the cap+1 submit is REFUSED, and SIGTERM with a
#       registry full of hung drivers still exits 0 promptly (abort-then-drain, §6a.8).
# Measured externally: the double timestamps every incoming request; the gate computes
# T(curl POST) -> T(first new double line). curl (not the CLI) submits the measured pays so
# process startup never pollutes the 250 ms budget.
#
# Needs the TWO-FED harness (the probe needs a candidate B): first follow
# docs/devimint-runbook.md §1's exact-pin patch/release build, which exports FEDIMINT_WORKTREE:
#
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
#
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
#       --exec bash "$WALLETS_REPO/wallet-cli/tests/smoke_responsiveness_devimint.sh"
#   '
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"
: "${FED_B_INVITE:?FED_B_INVITE not set — apply docs/devimint-two-fed-harness.patch and export FM_NUM_FEDS=2}"

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
WALLETD="${WALLETD_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/walletd}"
DOUBLE_PY="${WALLETS_REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}/wallet-cli/tests/hang_gateway.py"
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
[[ -f "$DOUBLE_PY" ]] || { echo "FAIL: missing $DOUBLE_PY" >&2; exit 1; }
for c in fedimint-cli jq python3; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not on PATH" >&2; exit 1; }
done
command -v curl >/dev/null || { echo "FAIL: lock-pinned curl not on PATH (use run_exact_nix_develop)" >&2; exit 1; }

GW_REAL="http://127.0.0.1:${FM_PORT_GW_LDK}/"
PORT=19737           # walletd, off the default
DOUBLE_PORT=18790    # the misbehaving gateway
FUND_MSAT=800000     # A's working balance (covers 32 pay reservations + the probe's)
PAY_MSAT=10000       # each measured pay
PAY_FEE_CAP=2000     # explicit, so 32 reservations fit A's balance (policy default is 200k)
CAP=32               # wallet-fedimint EXTERNAL_DRIVER_CAP (service/mod.rs)
BOUND_MS=250

SANDBOX="$(mktemp -d)"
export XDG_CONFIG_HOME="$SANDBOX/config"
export XDG_DATA_HOME="$SANDBOX/data"
DATA_DIR="$XDG_DATA_HOME/walletd"
DEBUG_DIR="${SMOKE_DEBUG_DIR:-/tmp/resp-gate-debug}"
DOUBLE_LOG="$SANDBOX/double.log"
WALLETD_LOG="$SANDBOX/walletd.log"
WALLETD_PID=""
DOUBLE_PID=""
STATUS=1
cleanup() {
  if [[ -n "$WALLETD_PID" ]] && kill -0 "$WALLETD_PID" 2>/dev/null; then
    kill -TERM "$WALLETD_PID" 2>/dev/null || true
    wait "$WALLETD_PID" 2>/dev/null || true
  fi
  if [[ -n "$DOUBLE_PID" ]]; then
    kill "$DOUBLE_PID" 2>/dev/null || true
  fi
  if [[ "$STATUS" != "0" ]]; then
    mkdir -p "$DEBUG_DIR"
    cp -f "$WALLETD_LOG" "$DEBUG_DIR/walletd.log" 2>/dev/null || true
    cp -f "$DOUBLE_LOG" "$DEBUG_DIR/double.log" 2>/dev/null || true
    echo "diagnostics preserved at $DEBUG_DIR" >&2
    echo "--- walletd log tail ---" >&2; tail -40 "$WALLETD_LOG" >&2 2>/dev/null || true
  fi
  rm -rf "$SANDBOX"
}
trap cleanup EXIT

# ---- phase 0: seed the wallet STANDALONE (daemon down) against the REAL gateway ---------------
echo "== seed: join A, fund A, auto-join candidate B (standalone, real gateway) =="
mkdir -p "$XDG_CONFIG_HOME/walletd"
cat > "$XDG_CONFIG_HOME/walletd/walletd.toml" <<EOF
port = $PORT
gateway = "http://127.0.0.1:$DOUBLE_PORT/"
EOF

wsa() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW_REAL" "$@"; }
SEED_ERR="$SANDBOX/seed.stderr"

JOIN_OUT=$(wsa join "$FM_INVITE_CODE")
JOIN_KEY=${JOIN_OUT#* }
[[ "$(wsa await-move "$JOIN_KEY")" == "done" ]] || { echo "FAIL: join A did not settle" >&2; exit 1; }
FED_A=$(cut -d: -f2 <<<"$JOIN_KEY")
echo "joined A: $FED_A"

INV_FUND=$(wsa receive --amount "$FUND_MSAT" 2>"$SEED_ERR")
KEY_FUND=$(sed -n 's/^key: //p' "$SEED_ERR")
SEND_FUND=$(fedimint-cli module lnv2 send "$INV_FUND" --gateway "$GW_REAL" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_FUND" >/dev/null 2>&1 || true
[[ "$(wsa await-receive "$KEY_FUND")" == "claimed" ]] || { echo "FAIL: funding A did not claim" >&2; exit 1; }
echo "A funded (~${FUND_MSAT} msat)"

wsa discover --source manual --invite "$FED_B_INVITE" --auto-join --scorer-allow-regtest >/dev/null
FED_B=$(wsa candidates | awk '$2 == "autojoined" { print $1; exit }')
[[ -n "$FED_B" ]] || { echo "FAIL: B was not auto-joined" >&2; wsa candidates >&2; exit 1; }
echo "auto-joined candidate B: $FED_B (probe-gated, empty)"

# Policy for the gate: pin spending A (the measured pays omit fed), fast watch cadence so the
# scheduler probes B within seconds, discovery pushed out of the way.
wsa policy set \
  --spending-fed "$FED_A" \
  --base-interval-secs 5 --min-interval-secs 1 \
  --probe-retry-backoff-secs 1 \
  --discover-every-secs 1000000000 >/dev/null
echo "policy seeded (spending pin A, 5s cadence)"

# ---- phase 1: mint the measured invoices upfront (devimint's client, real gateway) ------------
TOTAL_PAYS=$((CAP))            # 32 admitted pays; the 33rd asserts the cap refusal
echo "== minting $((TOTAL_PAYS + 1)) invoices on devimint's client =="
INVOICES=()
for i in $(seq 0 "$TOTAL_PAYS"); do
  INV=$(fedimint-cli module lnv2 receive "$PAY_MSAT" --gateway "$GW_REAL" 2>/dev/null | jq -r '.[0]')
  [[ -n "$INV" && "$INV" != "null" ]] || { echo "FAIL: invoice mint $i failed" >&2; exit 1; }
  INVOICES+=("$INV")
done
echo "minted ${#INVOICES[@]} invoices"

# ---- phase 2: the double + walletd (pinned to the double) -------------------------------------
echo "== start the misbehaving gateway + walletd (pin = double) =="
python3 "$DOUBLE_PY" "$DOUBLE_PORT" "$DOUBLE_LOG" >"$SANDBOX/double.stdout" 2>&1 &
DOUBLE_PID=$!
sleep 0.3
kill -0 "$DOUBLE_PID" || { echo "FAIL: hang-gateway did not start" >&2; cat "$SANDBOX/double.stdout" >&2; exit 1; }

"$WALLETD" init >/dev/null
TOKEN=$(cat "$DATA_DIR/token")
RUST_LOG="info,wallet_fedimint=debug,wallet_core=debug,walletd=debug" "$WALLETD" > "$WALLETD_LOG" 2>&1 &
WALLETD_PID=$!
BASE="http://127.0.0.1:$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")
for i in $(seq 1 60); do
  curl -sf "${AUTH[@]}" "$BASE/v1/health" >/dev/null 2>&1 && break
  kill -0 "$WALLETD_PID" 2>/dev/null || { echo "FAIL: walletd died at startup" >&2; exit 1; }
  sleep 0.2
  [[ "$i" == 60 ]] && { echo "FAIL: walletd never became healthy" >&2; exit 1; }
done
echo "walletd up (pid $WALLETD_PID)"

# ---- phase 3: wait for the scheduler's probe of B to HANG on the double ----------------------
echo "== waiting for the scheduled probe of B to hit (and hang on) the double =="
for i in $(seq 1 180); do
  if [[ -s "$DOUBLE_LOG" ]]; then break; fi
  sleep 0.5
  [[ "$i" == 180 ]] && { echo "FAIL: no scheduled probe reached the double in 90s" >&2; exit 1; }
done
PROBE_LINES=$(wc -l < "$DOUBLE_LOG")
echo "probe in flight and hanging (double saw $PROBE_LINES request(s)): $(head -1 "$DOUBLE_LOG" | cut -d' ' -f2-)"
# Let the first scheduler cycle finish its bookkeeping: the gate measures steady-state
# responsiveness UNDER HELD IO, not cold-start jitter racing the first tick's writes.
sleep 3

# ---- the measurement --------------------------------------------------------------------------
# T(curl POST /v1/pay) -> T(first NEW double line). Every earlier driver is HANGING on its one
# open socket (a hung request never retries inside the poll window), so the next new line is
# this pay's first external call. Includes the 202 round-trip — the spec's full budget.
submit_pay() { # $1=invoice -> prints http code
  curl -s -o "$SANDBOX/pay-resp.json" -w '%{http_code}' -X POST "$BASE/v1/pay" \
    "${AUTH[@]}" -H 'Content-Type: application/json' \
    -d "{\"invoice\":\"$1\",\"amount\":null,\"fee_cap\":$PAY_FEE_CAP,\"fed\":null}"
}
measure_pay() { # $1=invoice $2=tag
  local lc0 t0 code t202 line ts delta_ms accept_ms
  lc0=$(wc -l < "$DOUBLE_LOG")
  t0=$(date +%s%N)
  code=$(submit_pay "$1")
  t202=$(date +%s%N)
  [[ "$code" == "202" ]] || { echo "FAIL($2): pay HTTP $code: $(cat "$SANDBOX/pay-resp.json")" >&2; return 1; }
  for i in $(seq 1 500); do
    (( $(wc -l < "$DOUBLE_LOG") > lc0 )) && break
    sleep 0.01
  done
  line=$(sed -n "$((lc0 + 1))p" "$DOUBLE_LOG")
  [[ -n "$line" ]] || { echo "FAIL($2): driver made no external call within 5s of the 202" >&2; return 1; }
  ts=${line%% *}
  delta_ms=$(( (ts - t0) / 1000000 ))
  accept_ms=$(( (t202 - t0) / 1000000 ))
  echo "$2: POST -> 202 = ${accept_ms} ms; POST -> first external call = ${delta_ms} ms"
  if (( delta_ms < 0 || delta_ms >= BOUND_MS )); then
    echo "OVER-BOUND($2): ${delta_ms} ms >= ${BOUND_MS} ms" >&2
    MEASURE_FAILURES=$((MEASURE_FAILURES + 1))
  fi
}
MEASURE_FAILURES=0

echo "== M1: single pay while the probe hangs =="
measure_pay "${INVOICES[0]}" "M1"

echo "== M2: second pay while pay 1 is mid-IO (never serialize) =="
measure_pay "${INVOICES[1]}" "M2"

echo "== M3: fill to the admission cap, measure the LAST admitted pay under full load =="
for i in $(seq 2 $((CAP - 2))); do
  CODE=$(submit_pay "${INVOICES[$i]}")
  [[ "$CODE" == "202" ]] || { echo "FAIL: fill pay $i got HTTP $CODE: $(cat "$SANDBOX/pay-resp.json")" >&2; exit 1; }
done
measure_pay "${INVOICES[$((CAP - 1))]}" "M3(cap)"

INFLIGHT=$(curl -sf "${AUTH[@]}" "$BASE/v1/health" | jq -r '.inflight_drivers')
echo "inflight drivers at full load: $INFLIGHT"
(( INFLIGHT >= CAP )) || { echo "FAIL: expected >= $CAP in-flight drivers, saw $INFLIGHT" >&2; exit 1; }

echo "== cap+1 is refused (log-and-reject above the admission cap) =="
CODE=$(submit_pay "${INVOICES[$CAP]}")
[[ "$CODE" == "409" ]] || { echo "FAIL: cap+1 pay got HTTP $CODE (expected 409): $(cat "$SANDBOX/pay-resp.json")" >&2; exit 1; }
grep -q "driver admission cap" "$SANDBOX/pay-resp.json" || { echo "FAIL: cap refusal body: $(cat "$SANDBOX/pay-resp.json")" >&2; exit 1; }

if (( MEASURE_FAILURES > 0 )); then
  echo "FAIL: $MEASURE_FAILURES measurement(s) exceeded the ${BOUND_MS} ms bound (see OVER-BOUND lines)" >&2
  exit 1
fi

echo "== SIGTERM with a registry full of HUNG drivers still exits 0 promptly =="
T_STOP=$(date +%s%N)
kill -TERM "$WALLETD_PID"
WALLETD_EXIT=0
wait "$WALLETD_PID" || WALLETD_EXIT=$?
STOP_MS=$(( ($(date +%s%N) - T_STOP) / 1000000 ))
WALLETD_PID=""
[[ "$WALLETD_EXIT" == "0" ]] || { echo "FAIL: walletd exited $WALLETD_EXIT on SIGTERM" >&2; exit 1; }
(( STOP_MS < 15000 )) || { echo "FAIL: shutdown took ${STOP_MS} ms with hung drivers" >&2; exit 1; }
echo "walletd stopped cleanly in ${STOP_MS} ms with $INFLIGHT drivers mid-IO"

STATUS=0
echo "RESPONSIVENESS_GATE_EXIT=0"
echo "PASS: probe held by the double; M1/M2/M3 first-external-call < ${BOUND_MS} ms (M3 at the full ${CAP}-driver cap); cap+1 refused; SIGTERM clean under hung IO"
