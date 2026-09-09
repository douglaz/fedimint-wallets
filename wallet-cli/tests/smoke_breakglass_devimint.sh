#!/usr/bin/env bash
# THE BREAK-GLASS GATE (ADR-0030): automated routing resolves ONLY from a federation's lnv2
# vetted list, and the operator's standalone `--gateway <url>` is a single-invocation override
# for the ONE operation the invocation creates or awaits (by operation key). Fed B's vetted list
# stays EMPTY for the whole run; the LDK gateway SERVES B (connect-fed) but is never vetted there.
#   a. register the LDK gateway on every guardian of fed A only;
#   b. standalone: join A and B, fund A UNPINNED (A's vetted list routes the inflow);
#   c. walletd with a fund policy A->B: the scheduler cannot route into B (a `not_probed`
#      refusal, B stays empty) and a client-mode `move` into B sits Pending on a "no lnv2
#      gateway" error — walletd never arms a break-glass, so nothing it drives reaches B.
#      Record that move's key (KEY1) and a second one (KEY2, step g); stop walletd;
#   d. standalone `await-move KEY1 --gateway GW` completes exactly that move; B's balance > 0;
#   e. `tick --gateway` / `probe --gateway` are USAGE errors naming --gateway; `balance --gateway`
#      exits 0 (route-less verbs ignore it);
#   f. `move`, `receive --to B`, `pay --fed B`, each with `--gateway GW`, route through the
#      override against B's empty list;
#   g. KEY2, created WITHOUT the flag, is still Pending at the end — every `--gateway` invocation
#      above re-drove it too (the recovery pass re-drives every pending intent) and none of them
#      could route it.
#
# WHY (c) IS A USER MOVE AND NOT AN ALLOCATOR INTENT: with B's vetted list empty, B's probe reads
# `gateway_available = false`, so the allocator's receive gate (`receive_blocker`,
# wallet-core/src/allocator.rs) refuses B as a credit destination outright — no automated
# Move/Evacuate into B is ever created for an operator to re-drive. The daemon-admitted user move
# stuck on the empty list is the intent an operator actually meets in the ADR-0030 incident.
#
# Needs the TWO-FED harness and DEBUG binaries (same launch as smoke_daemon_chain_devimint.sh).
# First follow docs/devimint-runbook.md §1's exact-pin patch/release build, which exports
# FEDIMINT_WORKTREE:
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
#       "$WALLETD_BIN"
#       "$FM_FEDIMINTD_BASE_EXECUTABLE"
#       "$FM_FEDIMINT_CLI_BASE_EXECUTABLE"
#       "$FM_GATEWAYD_BASE_EXECUTABLE"
#       "$FM_GATEWAY_CLI_BASE_EXECUTABLE"
#       "$FM_RECURRINGD_BASE_EXECUTABLE"
#       "$DEVIMINT_BIN"
#     )
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
#       --exec bash "$WALLETS_REPO/wallet-cli/tests/smoke_breakglass_devimint.sh"
#   '
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run with FM_ENABLE_MODULE_LNV2=1}"
: "${FED_B_INVITE:?FED_B_INVITE not set — apply docs/devimint-two-fed-harness.patch + FM_NUM_FEDS=2}"

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
WALLETD="${WALLETD_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/walletd}"
for f in "$WALLET_CLI" "$WALLETD"; do
  if [[ ! -x "$f" ]]; then
    echo "FAIL: missing binary $f" >&2
    echo 'Follow docs/devimint-runbook.md §1 to export FEDIMINT_WORKTREE, then build:' >&2
    echo '  run_exact_cargo build --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-daemon -p wallet-cli' >&2
    exit 1
  fi
done
for c in fedimint-cli jq; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not on PATH (run inside dev-fed --exec)" >&2; exit 1; }
done

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
source "$(dirname "${BASH_SOURCE[0]}")/devimint_lib.sh"
echo "== a. vet the LDK gateway on fed A ONLY (fed B's lnv2 vetted list stays EMPTY) =="
register_lnv2_gateway "$GW" "$FM_INVITE_CODE"

PORT=19742
FUND_MSAT=1000000   # A's working balance: three 100-sat moves + one 50-sat pay + caps
MOVE_MSAT=100000
FEE_CAP=10000       # explicit on every move/pay: admission reserves amount + cap per PENDING intent
PAY_MSAT=50000
RECV_MSAT=20000
RECV_SLACK=1000     # 1 sat — bounds the lnv2 receive-quote under-estimate on B (per the move smoke)

SANDBOX="$(mktemp -d)"
export XDG_CONFIG_HOME="$SANDBOX/config"
export XDG_DATA_HOME="$SANDBOX/data"
DATA_DIR="$XDG_DATA_HOME/walletd"
DEBUG_DIR="${SMOKE_DEBUG_DIR:-/tmp/breakglass-gate-debug}"
WALLETD_LOG="$SANDBOX/walletd.log"
WALLETD_PID=""
STATUS=1
cleanup() {
  if [[ -n "$WALLETD_PID" ]] && kill -0 "$WALLETD_PID" 2>/dev/null; then
    kill -TERM "$WALLETD_PID" 2>/dev/null || true
    wait "$WALLETD_PID" 2>/dev/null || true
  fi
  if [[ "$STATUS" != "0" ]]; then
    mkdir -p "$DEBUG_DIR"
    cp -f "$WALLETD_LOG" "$SANDBOX"/*.stderr "$DEBUG_DIR/" 2>/dev/null || true
    echo "diagnostics preserved at $DEBUG_DIR" >&2
    echo "--- walletd log tail ---" >&2; tail -30 "$WALLETD_LOG" >&2 2>/dev/null || true
  fi
  rm -rf "$SANDBOX"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$XDG_CONFIG_HOME/walletd"
cat > "$XDG_CONFIG_HOME/walletd/walletd.toml" <<EOF
port = $PORT
EOF

wsa() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" "$@"; }   # daemon DOWN only
wcli() { "$WALLET_CLI" "$@"; }                                         # client mode (pointer file)
join_fed() {
  local started key state
  started=$(wsa join "$1") || return
  key=${started#* }
  state=$(wsa await-move "$key") || return
  [[ "$state" == "done" ]] || { echo "join $key did not settle: $state" >&2; return 1; }
  cut -d: -f2 <<<"$key"
}
# NOTE: no `exit` in awk — it must consume ALL of `balance`'s output (SIGPIPE-safe with two feds).
bal_for() { wsa balance | awk -v id="$1" '$1 == id ":" && $3 == "msat" { print $2 }'; }
op_status() { wsa show "$1" | sed -n 's/^status: //p'; }
assert_pending() { # assert_pending <key> <label>
  local st; st=$(op_status "$1")
  case "$st" in
    succeeded|failed) fail "$2 ($1) is terminal ($st) — the break-glass leaked onto an unnamed key" ;;
    "") fail "$2 ($1) has no ledger row" ;;
  esac
  echo "$2 still non-terminal: status=$st"
}
wait_healthy() {
  for _ in $(seq 1 60); do
    wcli health >/dev/null 2>&1 && return 0
    kill -0 "$WALLETD_PID" 2>/dev/null || { echo "FAIL: walletd died at startup" >&2; return 1; }
    sleep 0.2
  done
  echo "FAIL: walletd never became healthy" >&2; return 1
}
stop_walletd() {
  kill -TERM "$WALLETD_PID"
  local rc=0; wait "$WALLETD_PID" || rc=$?
  WALLETD_PID=""
  [[ "$rc" == "0" ]] || fail "walletd exited $rc on SIGTERM"
}

# ---- b. standalone: join both, fund A unpinned ----------------------------------------------
echo "== b. standalone: join A + B, fund A through A's vetted list (no --gateway) =="
FED_A=$(join_fed "$FM_INVITE_CODE")
FED_B=$(join_fed "$FED_B_INVITE")
[[ "$FED_A" != "$FED_B" ]] || fail "both invites resolved to the same federation id ($FED_A)"
echo "A=$FED_A (vetted: LDK gateway)  B=$FED_B (vetted: EMPTY)"
# The gateway must SERVE B for the break-glass route to exist at all — serving is not vetting.
# The patched two-fed harness already connects the LDK gateway to B; this mirrors
# smoke_evacuate's best-effort re-connect (connect-fed returns non-zero when already connected),
# and says which case it hit so a later "no route" failure can be read back to here.
echo "== ensure the shared LDK gateway serves fed B (connect-fed, best-effort) =="
if command -v gateway-ldk >/dev/null; then
  gateway-ldk connect-fed "$FED_B_INVITE" >/dev/null 2>&1 \
    && echo "connect-fed B: ok" \
    || echo "note: gateway-ldk connect-fed returned non-zero (likely already connected) — continuing"
else
  echo "note: 'gateway-ldk' alias not on PATH; assuming the harness already connected the LDK gateway to fed B"
fi

DI_ERR="$SANDBOX/direct-inflow.stderr"
INV_A=$(wsa direct-inflow --to "$FED_A" --amount "$FUND_MSAT" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^key: //p' "$DI_ERR")
[[ -n "$INV_A" && -n "$KEY_FUND" ]] || { cat "$DI_ERR" >&2; fail "direct-inflow gave no invoice/key"; }
SEND_FUND=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_FUND" >/dev/null 2>&1 || true
[[ "$(wsa await-move "$KEY_FUND")" == "done" ]] || fail "funding A did not settle"
A0=$(bal_for "$FED_A"); B0=$(bal_for "$FED_B")
[[ "$A0" =~ ^[0-9]+$ && "$B0" =~ ^[0-9]+$ ]] || { wsa balance >&2; fail "could not parse balances"; }
(( B0 == 0 )) || fail "B must start EMPTY, holds ${B0} msat"
echo "funded: A=${A0} msat  B=${B0} msat"

# The fund policy smoke_daemon_chain uses: pin A spending / B standby with a standby target so the
# scheduler WANTS to fund B on its first tick, fast cadence, discovery pushed away.
wsa policy set \
  --spending-fed "$FED_A" --standby-fed "$FED_B" \
  --spending-target 200000 --standby-target 300000 --max-fee "$FEE_CAP" \
  --base-interval-secs 5 --min-interval-secs 1 \
  --probe-min-span-secs 1 --probe-retry-backoff-secs 1 \
  --discover-every-secs 1000000000 >/dev/null
"$WALLETD" init >/dev/null

# ---- c. walletd: the scheduler cannot route into B; user moves into B stick Pending ----------
echo "== c. walletd: scheduler refuses B (no vetted route); client-mode moves into B stick Pending =="
RUST_LOG="info,wallet_fedimint=debug" "$WALLETD" > "$WALLETD_LOG" 2>&1 &
WALLETD_PID=$!
wait_healthy

submit_move() { # submit_move <occurrence> -> prints key; the daemon admits it, its driver cannot route it
  local out err="$SANDBOX/move-$1.stderr"
  out=$(wcli move --from "$FED_A" --to "$FED_B" --amount "$MOVE_MSAT" --fee-cap "$FEE_CAP" --occurrence "$1" 2>"$err") \
    || fail "client-mode move (occurrence $1) into B did not admit: $(cat "$err")"
  [[ "$out" == started\ * ]] || fail "expected 'started <key>' from move (occurrence $1), got '$out'"
  echo "${out#* }"
}
KEY1=$(submit_move 0)
KEY2=$(submit_move 1)
echo "pending moves into B: KEY1=$KEY1  KEY2=$KEY2"

# The stuck intent's diagnosis: the executor's `resolve_gateway` leaves it Retryable/Pending with a
# "no lnv2 gateway ..." error (wallet-fedimint/src/executor.rs). Read it from `show` or the daemon log.
STUCK=""
for _ in $(seq 1 90); do
  # `grep -c` consumes the whole stream (a `-q` early exit would SIGPIPE wallet-cli mid-print).
  if { wcli show "$KEY1" 2>/dev/null || true; tail -300 "$WALLETD_LOG"; } | grep -c "no lnv2 gateway" >/dev/null; then
    STUCK=1; break
  fi
  sleep 1
done
[[ -n "$STUCK" ]] || { wcli show "$KEY1" >&2 || true; fail "KEY1 never surfaced a 'no lnv2 gateway' error within 90s"; }
echo "KEY1 is stuck on: $({ wcli show "$KEY1" 2>/dev/null || true; tail -300 "$WALLETD_LOG"; } | grep -m1 "no lnv2 gateway")"

# The scheduler ran (an agent tick row exists) and REFUSED B as a destination (`not_probed`:
# `receive_blocker` in wallet-core/src/allocator.rs — no vetted gateway answers on B). B stays empty.
TICKED=""
for _ in $(seq 1 30); do
  if wcli history --limit 200 | awk -F'\t' '$3 == "tick" && $8 ~ /^agent:/' | grep -c . >/dev/null; then TICKED=1; break; fi
  sleep 2
done
[[ -n "$TICKED" ]] || { wcli history --limit 50 >&2 || true; fail "the scheduler never ticked within 60s"; }
wcli history --limit 200 | awk -F'\t' '$3 == "refusal" && $9 == "not_probed"' | grep -c . >/dev/null \
  || { wcli history --limit 50 >&2 || true; fail "no not_probed refusal row: the scheduler did not refuse the unroutable standby B"; }
BAL_B_DAEMON=$({ wcli balance 2>/dev/null || true; } | awk -v id="$FED_B" '$1 == id ":" && $3 == "msat" { print $2 }')
[[ "$BAL_B_DAEMON" == "0" ]] || fail "walletd moved money into B without a vetted route (B=${BAL_B_DAEMON:-?} msat)"
echo "scheduler ticked, refused B (not_probed), B still 0 msat; KEY1/KEY2 Pending under walletd"
stop_walletd

# ---- d. the break-glass re-drives ONLY the named key ----------------------------------------
echo "== d. standalone await-move KEY1 --gateway: completes exactly KEY1 =="
STATE=$(wsa await-move "$KEY1" --gateway "$GW") || fail "await-move KEY1 --gateway exited non-zero"
[[ "$STATE" == "done" ]] || fail "await-move KEY1 --gateway expected 'done', got '$STATE'"
B1=$(bal_for "$FED_B")
(( B1 > 0 )) || fail "B is still empty after the break-glass move"
(( B1 <= MOVE_MSAT && B1 >= MOVE_MSAT - RECV_SLACK )) \
  || fail "B holds ${B1} msat, not ONE move's worth [$((MOVE_MSAT - RECV_SLACK)), ${MOVE_MSAT}] — did KEY2 leak through?"
assert_pending "$KEY2" "KEY2 (no flag)"
echo "KEY1 done through the break-glass; B=${B1} msat"

# ---- e. dispositions: automated verbs refuse, route-less verbs ignore ------------------------
echo "== e. tick/probe --gateway are usage errors; balance --gateway is ignored =="
E_ERR="$SANDBOX/tick.stderr"
if wsa tick --spending "$FED_A" --standby "$FED_B" --spending-target 0 --standby-target 0 \
     --max-fee "$FEE_CAP" --occurrence 7 --gateway "$GW" >/dev/null 2>"$E_ERR"; then
  fail "tick --gateway exited 0 (must be a usage error)"
fi
grep -q -- '--gateway' "$E_ERR" || fail "tick refusal does not name --gateway: $(cat "$E_ERR")"
E_ERR="$SANDBOX/probe.stderr"
if wsa probe "$FED_B" --from "$FED_A" --gateway "$GW" >/dev/null 2>"$E_ERR"; then
  fail "probe --gateway exited 0 (must be a usage error)"
fi
grep -q -- '--gateway' "$E_ERR" || fail "probe refusal does not name --gateway: $(cat "$E_ERR")"
wsa balance --gateway "$GW" >/dev/null || fail "balance --gateway must be ignored (exit 0)"
B1b=$(bal_for "$FED_B")
[[ "$B1b" == "$B1" ]] || fail "the refused tick/probe moved money: B ${B1} -> ${B1b}"
echo "tick/probe refused --gateway before touching money; balance ignored it"

# ---- f. money verbs route through the override against B's empty list ----------------------
echo "== f. move / receive --to B / pay --fed B, each --gateway =="
MV_ERR="$SANDBOX/move-2.stderr"
MOVE3=$(wsa move --from "$FED_A" --to "$FED_B" --amount "$MOVE_MSAT" --fee-cap "$FEE_CAP" \
          --occurrence 2 --gateway "$GW" 2>"$MV_ERR")
KEY3=$(sed -n 's/^key: //p' "$MV_ERR")
[[ "$MOVE3" == "started $KEY3" && -n "$KEY3" ]] || { cat "$MV_ERR" >&2; fail "move --gateway did not start: '$MOVE3'"; }
# The flag is non-durable (ADR-0030): the await that drives it must carry it again.
[[ "$(wsa await-move "$KEY3" --gateway "$GW")" == "done" ]] || fail "move --gateway (KEY3) did not settle"
B2=$(bal_for "$FED_B")
(( B2 - B1 <= MOVE_MSAT && B2 - B1 >= MOVE_MSAT - RECV_SLACK )) \
  || fail "B rose by $((B2 - B1)) msat on the break-glass move, not ~${MOVE_MSAT}"
echo "move KEY3 done; B=${B2} msat"

RC_ERR="$SANDBOX/receive.stderr"
INV_B=$(wsa receive --to "$FED_B" --amount "$RECV_MSAT" --gateway "$GW" 2>"$RC_ERR")
KEY_RECV=$(sed -n 's/^key: //p' "$RC_ERR")
[[ -n "$INV_B" && -n "$KEY_RECV" ]] || { cat "$RC_ERR" >&2; fail "receive --to B --gateway minted no invoice"; }
echo "receive --to B --gateway minted an invoice (key $KEY_RECV)"

# pay FROM B (empty vetted list) through the break-glass; fedimint-cli (joined to A) is the payee.
RECV_JSON=$(fedimint-cli module lnv2 receive "$PAY_MSAT" --gateway "$GW" 2>/dev/null)
INV_PAY=$(jq -r '.[0]' <<<"$RECV_JSON"); OP_DEV=$(jq -r '.[1]' <<<"$RECV_JSON")
[[ -n "$INV_PAY" && "$INV_PAY" != "null" ]] || fail "devimint did not mint a payable invoice: $RECV_JSON"
PAY_OUT=$(wsa pay "$INV_PAY" --fed "$FED_B" --fee-cap "$FEE_CAP" --gateway "$GW") || fail "pay --gateway did not admit"
[[ "$PAY_OUT" == started\ * ]] || fail "expected 'started <key>' from pay, got '$PAY_OUT'"
[[ "$(wsa await-send "${PAY_OUT#* }" --gateway "$GW")" == "success" ]] || fail "pay --gateway did not settle"
DEV_RECV=$(fedimint-cli module lnv2 await-receive "$OP_DEV" 2>/dev/null | tr -d '"[:space:]')
[[ "$DEV_RECV" == "Claimed" ]] || fail "devimint did not claim the break-glass payment: $DEV_RECV"
B3=$(bal_for "$FED_B")
(( B3 < B2 )) || fail "B did not drop after paying from it (${B2} -> ${B3})"
echo "pay --fed B --gateway settled (devimint claimed); B=${B3} msat"

# ---- g. the unnamed key never saw an override ----------------------------------------------
echo "== g. KEY2 (created without the flag) is still Pending =="
assert_pending "$KEY2" "KEY2 (no flag)"

STATUS=0
echo "BREAKGLASS_GATE_EXIT=0"
echo "PASS: B's vetted list empty throughout — walletd refused B (not_probed) and left user moves Pending on 'no lnv2 gateway'; await-move/move/receive/pay --gateway each routed ONLY their named operation; tick/probe refused the flag; balance ignored it; KEY2 stayed Pending"
