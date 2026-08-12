#!/usr/bin/env bash
# devimint smoke test for the WALLETD DAEMON PATH (phase 6a step 7, spec §6a.9): the
# smoke_money receive+pay gates driven through a RUNNING `walletd` via `wallet-cli` in its
# default CLIENT mode — no --standalone anywhere. Proves the vertical slice end to end:
# CLI → HTTP/bearer → axum handlers → actor decide → async driver → ledger → long-poll await.
#
# NOT part of the rb-lite gate — needs a LIVE devimint federation:
#
#   # 1. Follow docs/devimint-runbook.md §1's exact-pin two-fed patch/release build. It exports
#   #    FEDIMINT_WORKTREE; do not substitute an arbitrary fedimint checkout.
#   # 2. Build walletd + wallet-cli (from this repo):
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
#   run_exact_cargo build --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-daemon -p wallet-cli
#
#   # 3. Bring up a dev federation from the exact-pinned release binary (lnv2 enabled):
#   #    Run §2 through its "OUTER PREFLIGHT COMPLETE" marker immediately first.
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
#     "$DEVIMINT_BIN" --link-test-dir "$FEDIMINT_WORKTREE/target-nix/devimint" dev-fed \
#       --exec bash "$WALLETS_REPO/wallet-cli/tests/smoke_daemon_devimint.sh"
#   '
#
# The daemon gets the devimint LDK gateway via the walletd.toml `gateway` host-config pin
# (runbook §4: devimint never registers it into the lnv2 set). The CLI finds the daemon via
# the client pointer `walletd init` writes under $XDG_CONFIG_HOME — the whole gate runs in a
# throwaway XDG sandbox so it never touches the operator's real walletd.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run this inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run this inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
WALLETD="${WALLETD_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/walletd}"
for bin in "$WALLET_CLI" "$WALLETD"; do
  if [[ ! -x "$bin" ]]; then
    echo "FAIL: binary not found/executable at $bin" >&2
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
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }
command -v jq >/dev/null || { echo "FAIL: jq not on PATH (it is in the fedimint nix devshell)" >&2; exit 1; }
command -v curl >/dev/null || { echo "FAIL: curl not on PATH" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
RECEIVE_MSAT=500000   # our wallet receives 500 sat...
PAY_MSAT=100000       # ...then pays 100 sat back, leaving headroom for fees
PORT=19736            # off the default 9736 so a real walletd on this box is never hit

SANDBOX="$(mktemp -d)"
export XDG_CONFIG_HOME="$SANDBOX/config"
export XDG_DATA_HOME="$SANDBOX/data"
RECV_ERR="$(mktemp)"
WALLETD_LOG="$SANDBOX/walletd.log"
WALLETD_PID=""
# Devimint tears its tmp tree down with the run, taking $SANDBOX with it — on FAILURE copy the
# diagnostics somewhere that survives (override with SMOKE_DEBUG_DIR).
DEBUG_DIR="${SMOKE_DEBUG_DIR:-/tmp/daemon-gate-debug}"
STATUS=1
cleanup() {
  if [[ -n "$WALLETD_PID" ]] && kill -0 "$WALLETD_PID" 2>/dev/null; then
    kill -TERM "$WALLETD_PID" 2>/dev/null || true
    wait "$WALLETD_PID" 2>/dev/null || true
  fi
  if [[ "$STATUS" != "0" ]]; then
    mkdir -p "$DEBUG_DIR"
    cp -f "$WALLETD_LOG" "$DEBUG_DIR/walletd.log" 2>/dev/null || true
    cp -f "$RECV_ERR" "$DEBUG_DIR/last-verb.stderr" 2>/dev/null || true
    cp -rf "$XDG_CONFIG_HOME/walletd" "$DEBUG_DIR/config" 2>/dev/null || true
    echo "diagnostics preserved at $DEBUG_DIR" >&2
    echo "--- walletd log tail ---" >&2
    tail -40 "$WALLETD_LOG" >&2 2>/dev/null || true
  fi
  rm -rf "$SANDBOX" "$RECV_ERR"
}
trap cleanup EXIT

echo "== walletd init (sandboxed XDG at $SANDBOX) =="
mkdir -p "$XDG_CONFIG_HOME/walletd"
cat > "$XDG_CONFIG_HOME/walletd/walletd.toml" <<EOF
port = $PORT
gateway = "$GW"
EOF
"$WALLETD" init

TOKEN_PATH="$XDG_DATA_HOME/walletd/token"
[[ -f "$TOKEN_PATH" ]] || { echo "FAIL: init did not write the token at $TOKEN_PATH" >&2; exit 1; }
TOKEN_MODE=$(stat -c '%a' "$TOKEN_PATH")
[[ "$TOKEN_MODE" == "600" ]] || { echo "FAIL: token mode $TOKEN_MODE, expected 600" >&2; exit 1; }
TOKEN=$(cat "$TOKEN_PATH")
[[ -f "$XDG_CONFIG_HOME/walletd/client.toml" ]] || { echo "FAIL: init did not write the client pointer" >&2; exit 1; }

echo "== start walletd =="
"$WALLETD" > "$WALLETD_LOG" 2>&1 &
WALLETD_PID=$!
HEALTH_URL="http://127.0.0.1:$PORT/v1/health"
for i in $(seq 1 50); do
  if curl -sf -H "Authorization: Bearer $TOKEN" "$HEALTH_URL" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$WALLETD_PID" 2>/dev/null; then
    echo "FAIL: walletd exited during startup" >&2; cat "$WALLETD_LOG" >&2; exit 1
  fi
  sleep 0.2
  if [[ "$i" == 50 ]]; then echo "FAIL: walletd never became healthy" >&2; cat "$WALLETD_LOG" >&2; exit 1; fi
done
echo "walletd up (pid $WALLETD_PID)"

# 401 gate: a wrong token must be rejected before any handler runs.
STATUS=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer wrong" "$HEALTH_URL")
[[ "$STATUS" == "401" ]] || { echo "FAIL: wrong bearer got $STATUS, expected 401" >&2; exit 1; }

# CLIENT MODE from here on: no --standalone, no --data-dir, no --gateway — the pointer file
# and the daemon's host-config pin carry everything.
wcli() { "$WALLET_CLI" "$@"; }
balance_msat_for_fed() {
  local fed_id="$1"
  wcli balance | awk -v id="$fed_id" '$1 == id ":" && $3 == "msat" { print $2; exit }'
}

echo "== join (through the daemon: 202 + await) =="
JOIN_OUT=$(wcli join "$FM_INVITE_CODE")
echo "join: $JOIN_OUT"
JOIN_KEY=${JOIN_OUT#* }
JOIN_STATE=$(wcli await-move "$JOIN_KEY")
[[ "$JOIN_STATE" == "done" ]] || { echo "FAIL: join $JOIN_KEY did not settle: $JOIN_STATE" >&2; exit 1; }
FED_ID=$(cut -d: -f2 <<<"$JOIN_KEY")
echo "joined federation: $FED_ID"

echo "== policy round-trip through the daemon =="
wcli policy get | jq -e '.per_fed_cap' >/dev/null || { echo "FAIL: policy get did not return a Policy" >&2; exit 1; }

# ---------------------------------------------------------------------------------------
echo "== RECEIVE: our wallet mints an invoice via walletd, devimint's funded client pays it =="
RECEIVE_EXIT=0
INV_A=$(wcli receive --amount "$RECEIVE_MSAT" 2>"$RECV_ERR") || RECEIVE_EXIT=$?
if [[ "$RECEIVE_EXIT" != "0" ]]; then
  echo "FAIL: receive exited $RECEIVE_EXIT" >&2
  echo "--- receive stderr ---" >&2; cat "$RECV_ERR" >&2
  exit 1
fi
KEY_A=$(sed -n 's/^key: //p' "$RECV_ERR")
if [[ -z "$INV_A" || -z "$KEY_A" ]]; then
  echo "FAIL: receive did not yield an invoice (stdout) and operation key (stderr):" >&2
  echo "  invoice=$INV_A" >&2; echo "  --- receive stderr ---" >&2; cat "$RECV_ERR" >&2
  exit 1
fi
echo "invoice: $INV_A"
echo "receive operation: $KEY_A"

# Await the payer's SEND first (lnv2's internal swap funds our incoming contract as part of
# the sender's send SM completing) — same ordering the standalone smoke validated.
echo "-- devimint (default-0) pays the invoice; await the send so the swap completes --"
SEND_A=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_A" >/dev/null 2>&1 || true

echo "-- await our receive claim (daemon long-poll) --"
RECV_STATE=$(wcli await-receive "$KEY_A")
echo "await-receive: $RECV_STATE"
[[ "$RECV_STATE" == "claimed" ]] || { echo "FAIL: expected 'claimed', got '$RECV_STATE'" >&2; exit 1; }

BAL_AFTER_RECV=$(balance_msat_for_fed "$FED_ID")
[[ "$BAL_AFTER_RECV" =~ ^[0-9]+$ ]] || { echo "FAIL: could not parse balance for $FED_ID" >&2; wcli balance >&2; exit 1; }
echo "balance after receive: ${BAL_AFTER_RECV} msat (expected ~${RECEIVE_MSAT})"
RECEIVE_FEE_CAP=$(( 50000 + RECEIVE_MSAT / 200 ))   # 50 sat + 0.5% = the lnv2 receive-fee limit
if (( BAL_AFTER_RECV < RECEIVE_MSAT - RECEIVE_FEE_CAP || BAL_AFTER_RECV > RECEIVE_MSAT )); then
  echo "FAIL: balance ${BAL_AFTER_RECV} msat is not ~${RECEIVE_MSAT} msat after receive" >&2
  exit 1
fi

# ---------------------------------------------------------------------------------------
echo "== PAY: devimint's client mints an invoice, our wallet pays it via walletd =="
RECV_B_JSON=$(fedimint-cli module lnv2 receive "$PAY_MSAT" --gateway "$GW" 2>/dev/null)
INV_B=$(jq -r '.[0]' <<<"$RECV_B_JSON")
OP_B_DEV=$(jq -r '.[1]' <<<"$RECV_B_JSON")
if [[ -z "$INV_B" || "$INV_B" == "null" ]]; then
  echo "FAIL: devimint did not mint a payable invoice: $RECV_B_JSON" >&2
  exit 1
fi
echo "invoice to pay: $INV_B"

echo "-- our wallet pays (202 + operation key) --"
PAY_OUT=$(wcli pay "$INV_B" --fed "$FED_ID")
echo "pay: $PAY_OUT"
OP_B=$(awk '{print $2}' <<<"$PAY_OUT")
case "$PAY_OUT" in
  started\ *) : ;;
  *) echo "FAIL: expected 'started <key>', got '$PAY_OUT'" >&2; exit 1 ;;
esac

echo "-- await our send settlement --"
SEND_STATE=$(wcli await-send "$OP_B")
echo "await-send: $SEND_STATE"
[[ "$SEND_STATE" == "success" ]] || { echo "FAIL: expected 'success', got '$SEND_STATE'" >&2; exit 1; }

DEV_RECV_STATE=$(fedimint-cli module lnv2 await-receive "$OP_B_DEV" 2>/dev/null)
echo "devimint await-receive: $DEV_RECV_STATE"
if [[ "$DEV_RECV_STATE" != '"Claimed"' && "$DEV_RECV_STATE" != "Claimed" ]]; then
  echo "FAIL: devimint side did not claim the payment: $DEV_RECV_STATE" >&2
  exit 1
fi
BAL_AFTER_PAY=$(balance_msat_for_fed "$FED_ID")
[[ "$BAL_AFTER_PAY" =~ ^[0-9]+$ ]] || { echo "FAIL: could not parse balance for $FED_ID" >&2; exit 1; }
echo "balance after pay: ${BAL_AFTER_PAY} msat"
(( BAL_AFTER_PAY < BAL_AFTER_RECV )) || { echo "FAIL: balance did not drop after paying" >&2; exit 1; }

# Dedup (spec §4/§6a.6 idempotency): re-submitting the SAME invoice must return the SAME
# operation key and move no additional money. Client mode's phase-1 word is always `started`
# (the 202 carries only the key — render.rs), so key identity + balance are the assertions.
echo "-- re-pay the same invoice: expect the same key, no second debit --"
REPAY_OUT=$(wcli pay "$INV_B" --fed "$FED_ID")
echo "re-pay: $REPAY_OUT"
REPAY_KEY=$(awk '{print $2}' <<<"$REPAY_OUT")
[[ "$REPAY_KEY" == "$OP_B" ]] || { echo "FAIL: re-pay minted a DIFFERENT key ($REPAY_KEY != $OP_B)" >&2; exit 1; }
BAL_AFTER_REPAY=$(balance_msat_for_fed "$FED_ID")
[[ "$BAL_AFTER_REPAY" == "$BAL_AFTER_PAY" ]] || {
  echo "FAIL: re-pay moved money (${BAL_AFTER_PAY} -> ${BAL_AFTER_REPAY} msat)" >&2; exit 1; }

# ---------------------------------------------------------------------------------------
echo "== history through the daemon reconstructs the session =="
wcli history | tee /dev/stderr | grep -q "pay" || { echo "FAIL: history has no pay row" >&2; exit 1; }
wcli show "$OP_B" >/dev/null || { echo "FAIL: show $OP_B failed" >&2; exit 1; }

echo "== SIGTERM: stop intake -> abort drivers -> drain -> exit 0 =="
kill -TERM "$WALLETD_PID"
WALLETD_EXIT=0
wait "$WALLETD_PID" || WALLETD_EXIT=$?
WALLETD_PID=""
if [[ "$WALLETD_EXIT" != "0" ]]; then
  echo "FAIL: walletd exited $WALLETD_EXIT on SIGTERM" >&2
  tail -40 "$WALLETD_LOG" >&2
  exit 1
fi
echo "walletd stopped cleanly"

STATUS=0
echo "DAEMON_GATE_EXIT=0"
echo "PASS: join, receive(claimed), pay(success), dedup-by-key, history, 401, clean SIGTERM — all through a running walletd"
