#!/usr/bin/env bash
# devimint smoke for br-n8o: a STRUCTURALLY REFUSED evacuation is superseded by a linked
# replacement when the operator raises the cap — driven by walletd's own scheduler, moving real
# money across two live federations.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# WHY THIS EXISTS — and what the in-process tests do NOT cover
# ────────────────────────────────────────────────────────────────────────────────────────────
# `service::tests` proves the exchange machinery (atomic parent→child, one linked child, restart
# without redrive) but every one of those tests HAND-BUILDS its `EvacuationRefusalEvidence`:
# `ReplacementHandoffExecutor` picks `low.delivered_net = Msat(10)` and `total_fee = cap.at + 1`.
# `size_evacuation` never runs there.
#
# `executor::tests::production_built_refusal_evidence_can_qualify_a_cap_raise` closes the next
# layer — the REAL producer's evidence really can satisfy the REAL gate — but it is still a
# scripted quote closure, and nothing in either layer moves a satoshi.
#
# This gate is the remaining half: a real gateway, a real federation pair, a real refusal, a real
# policy edit, and a real balance delta. It is `br-recanary-y2j-ujs` item 5's own admission
# evidence, and a production canary cannot substitute for it — you cannot safely induce a
# structural refusal on a funded wallet.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# HOW THE STRUCTURAL REFUSAL IS INDUCED — deterministically, not by luck
# ────────────────────────────────────────────────────────────────────────────────────────────
# A structural refusal needs `fixed_component_exceeds_cap_base`: the fee's FIXED component must
# exceed the cap's base while the cap cannot grow fast enough to catch it. So:
#
#   * the evacuation cap is set BASE-ONLY (`--evac-fee-bps 0`), making it flat in the amount;
#   * the gateway's TRANSACTION fee base is set EXPLICITLY, per federation, via
#     `gateway-ldk cfg set-fees --tx-base` — devimint's own `set_federation_transaction_fee`.
#
# Setting the gateway fee rather than inheriting `PaymentFee::TRANSACTION_FEE_DEFAULT` is
# deliberate. A fixture pinned to an upstream constant silently stops exercising the structural
# branch the day upstream changes that constant, and would still pass — the failure mode this
# whole bead exists to prevent. We assert the induced refusal IS structural rather than assuming.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# TWO FEDERATIONS + THE DEBUG BINARIES
# ────────────────────────────────────────────────────────────────────────────────────────────
# `devimint dev-fed` brings up ONE federation. Supply B's invite via $FED_B_INVITE (see
# docs/devimint-two-fed-harness.patch and smoke_move_devimint.sh's "TWO FEDERATIONS" note).
# The shared LDK gateway must serve BOTH feds.
#
# The force-shutdown seam (`WALLET_CLI_FORCE_SHUTDOWN`) is `#[cfg(debug_assertions)]`, so this
# smoke needs the DEBUG walletd + wallet-cli, exactly like smoke_daemon_chain_devimint.sh.
# Rebuild them from THIS branch before running: a stale binary has silently passed a wallet gate
# in this repo twice.
#
# Not in CI, deliberately (see .github/workflows/ci.yml): live federations are too slow. Run by
# hand inside `devimint dev-fed --exec` with FM_ENABLE_MODULE_LNV2=1.

# ────────────────────────────────────────────────────────────────────────────────────────────
# LAUNCH — daemon smoke, so it uses its own block (the runbook's run_two_fed_cli_smoke helper is
# CLI-only by allowlist). Replay docs/devimint-runbook.md §1 and §2 through "OUTER PREFLIGHT
# COMPLETE" in the SAME shell first, then:
#
#   cd "$WALLETS_REPO"
#   run_exact_cargo build --locked --target-dir "$WALLETS_REPO/target-nix" \
#     -p wallet-cli -p wallet-daemon
#   verify_exact_two_fed_launch_state
#   cd "$FEDIMINT_WORKTREE"
#   run_exact_nix_develop -c bash -c '
#     set -euo pipefail
#     export CARGO_PROFILE=release
#     source scripts/_common.sh
#     add_target_dir_to_path
#     for variable in $(compgen -v FM_ || true); do unset "$variable"; done
#     for variable in $(compgen -v WALLET_CLI_ || true) $(compgen -v WALLETD_ || true); do
#       unset "$variable"
#     done
#     export WALLET_CLI_BIN="$WALLETS_REPO/target-nix/debug/wallet-cli"
#     export WALLETD_BIN="$WALLETS_REPO/target-nix/debug/walletd"
#     export FM_DISCOVER_API_VERSION_TIMEOUT=10
#     PINNED="$FEDIMINT_WORKTREE/target-nix/release"
#     export FM_FEDIMINTD_BASE_EXECUTABLE="$PINNED/fedimintd"
#     export FM_FEDIMINT_CLI_BASE_EXECUTABLE="$PINNED/fedimint-cli"
#     export FM_GATEWAYD_BASE_EXECUTABLE="$PINNED/gatewayd"
#     export FM_GATEWAY_CLI_BASE_EXECUTABLE="$PINNED/gateway-cli"
#     export FM_RECURRINGD_BASE_EXECUTABLE="$PINNED/fedimint-recurringd"
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export RUST_LOG=warn
#     export FM_ENABLE_MODULE_LNV1=1 FM_ENABLE_MODULE_MINT=1
#     export FM_ENABLE_MODULE_WALLET=1 FM_ENABLE_MODULE_LNV2=1
#     export FM_NUM_FEDS=2
#     "$PINNED/devimint" --link-test-dir "$FEDIMINT_WORKTREE/target-nix/devimint" \
#       --num-feds 2 dev-fed \
#       --exec bash "$WALLETS_REPO/wallet-cli/tests/smoke_evacuate_supersede_devimint.sh"
#   '
#
# Set KEEP_SANDBOX=1 to retain the wallet data dir and walletd logs after a failure.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# LAST GREEN — 2026-09-02, two live regtest federations
# ────────────────────────────────────────────────────────────────────────────────────────────
#   evidence: low=4894 high=1865230 fees=105698/121074 caps=1000/1000
#   balances unchanged under refusal: A=1986590 B=0
#   child links reciprocal; parent marker cleared; exactly one child
#   moved: A 1986590->286 (debit 1986304)  B 0->1865230 (credit 1865230)  fee 121074
#   fee 121074 fits cap B (400000) and NOT cap A (1000)
#
# The evidence line is the point: `low`/`high` are what the REAL `size_evacuation` measured
# against a REAL gateway, not a fixture's invented pair.

set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"

FED_B_INVITE="${FED_B_INVITE:-${FM_INVITE_CODE_B:-}}"
if [[ -z "$FED_B_INVITE" ]]; then
  echo "FAIL: FED_B_INVITE (or FM_INVITE_CODE_B) not set — this is a TWO-federation smoke." >&2
  exit 1
fi
if [[ "$FED_B_INVITE" == "$FM_INVITE_CODE" ]]; then
  echo "FAIL: FED_B_INVITE equals FM_INVITE_CODE — evacuation needs two DISTINCT feds." >&2
  exit 1
fi

REPO="${WALLETS_REPO:-/home/master/p/fedimint-wallets}"
WALLET_CLI="${WALLET_CLI_BIN:-$REPO/target-nix/debug/wallet-cli}"
WALLETD="${WALLETD_BIN:-$REPO/target-nix/debug/walletd}"
for f in "$WALLET_CLI" "$WALLETD"; do
  [[ -x "$f" ]] || { echo "FAIL: missing/not executable: $f (build the DEBUG binaries)" >&2; exit 1; }
done
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH" >&2; exit 1; }
command -v gateway-ldk  >/dev/null || { echo "FAIL: gateway-ldk not on PATH (needed to set gateway fees)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
PORT="${WALLETD_PORT:-9789}"

FUND_MSAT=2000000        # 2000 sat into A — the balance the evacuation will drain
GW_TX_BASE_MSAT=50000    # 50 sat of gateway FIXED cost per leg. Large and explicit so the fixed
                         # component dominates and the structural predicate is unambiguous.
GW_TX_PPM=3000           # upstream's default proportional part; the fixed term is what matters
CAP_A_BASE=1000          # 1 sat base-only cap: far under the ~100 sat two-leg fixed cost -> refuse
CAP_B_BASE=400000        # 400 sat base-only cap: comfortably over it -> the replacement fits
RECV_SLACK=2000          # bounds lnv2 receive-quote under-estimate (per the other smokes)

SANDBOX="$(mktemp -d)"
export XDG_DATA_HOME="$SANDBOX/data" XDG_CONFIG_HOME="$SANDBOX/config"
DATA_DIR="$XDG_DATA_HOME/walletd"
LOG1="$SANDBOX/walletd-phase1.log"
LOG2="$SANDBOX/walletd-phase2.log"
WALLETD_PID=""
mkdir -p "$XDG_DATA_HOME" "$XDG_CONFIG_HOME"

cleanup() {
  local rc=$?
  if [[ -n "$WALLETD_PID" ]] && kill -0 "$WALLETD_PID" 2>/dev/null; then
    kill -TERM "$WALLETD_PID" 2>/dev/null || true
    wait "$WALLETD_PID" 2>/dev/null || true
  fi
  if (( rc != 0 )); then
    echo "--- seed stderr ---" >&2
    cat "$SANDBOX"/seed.stderr >&2 2>/dev/null || true
    echo "--- walletd log tails ---" >&2
    tail -40 "$SANDBOX"/walletd-phase*.log >&2 2>/dev/null || true
  fi
  if [[ -n "${KEEP_SANDBOX:-}" ]]; then
    echo "sandbox kept at $SANDBOX" >&2
  else
    rm -rf "$SANDBOX"
  fi
}
trap cleanup EXIT

wsa() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW" "$@"; }
wcli_balance_line() {
  { "$WALLET_CLI" balance 2>/dev/null || true; } \
    | awk -v id="$1" '$1 == id ":" && $3 == "msat" { print $2; exit }'
}
wait_healthy() {
  for _ in $(seq 1 90); do
    "$WALLET_CLI" health >/dev/null 2>&1 && return 0
    kill -0 "$WALLETD_PID" 2>/dev/null || { echo "FAIL: walletd died at startup" >&2; return 1; }
    sleep 0.2
  done
  echo "FAIL: walletd never became healthy" >&2; return 1
}
stop_walletd() {
  kill -TERM "$WALLETD_PID"
  local rc=0; wait "$WALLETD_PID" || rc=$?
  WALLETD_PID=""
  [[ "$rc" == "0" ]] || { echo "FAIL: walletd exited $rc on SIGTERM" >&2; return 1; }
}

mkdir -p "$XDG_CONFIG_HOME/walletd"
cat > "$XDG_CONFIG_HOME/walletd/walletd.toml" <<EOF
port = $PORT
gateway = "$GW"
EOF

# JSON assertion helpers as files: `python3 - <<'"'"'PY'"'"' <<<"$JSON"` silently feeds the JSON to
# python AS ITS SCRIPT (the later redirect wins), so keep script and data on separate channels.
cat > "$SANDBOX/check_evidence.py" <<'PYX'
import json, sys
cap_a = int(sys.argv[1])
d = json.load(sys.stdin)
ev = d.get("evacuation_refusal") or {}
assert ev, "the marked row must carry typed evidence, not just a diagnostic string"
low, high = ev["low"], ev["high"]
assert ev["cap_components"]["base_msat"] == cap_a, ev["cap_components"]
assert ev["cap_components"]["bps"] == 0, ev["cap_components"]
assert low["delivered_net"] < high["delivered_net"], (low, high)
for s in (low, high):
    assert s["total_fee"] > s["fee_cap"], s
print("  evidence: low={} high={} fees={}/{} caps={}/{}".format(
    low["delivered_net"], high["delivered_net"],
    low["total_fee"], high["total_fee"], low["fee_cap"], high["fee_cap"]))
PYX

cat > "$SANDBOX/check_child_links.py" <<'PYX'
import json, sys
parent = sys.argv[1]
d = json.load(sys.stdin)
got = d.get("supersedes")
assert got == parent, "child must link back to {}, got {}".format(parent, got)
PYX

cat > "$SANDBOX/check_marker_cleared.py" <<'PYX'
import json, sys
d = json.load(sys.stdin)
assert not d.get("evacuation_refusal_active"), "the retired parent marker must be CLEARED"
PYX

# ---------------------------------------------------------------------------------------
echo "== 1. join both federations, fund A =="
JOIN_A=$(wsa join "$FM_INVITE_CODE"); KEY_A=${JOIN_A#* }
[[ "$(wsa await-move "$KEY_A")" == "done" ]] || { echo "FAIL: join A did not settle" >&2; exit 1; }
FED_A=$(cut -d: -f2 <<<"$KEY_A")
JOIN_B=$(wsa join "$FED_B_INVITE"); KEY_B=${JOIN_B#* }
[[ "$(wsa await-move "$KEY_B")" == "done" ]] || { echo "FAIL: join B did not settle" >&2; exit 1; }
FED_B=$(cut -d: -f2 <<<"$KEY_B")
[[ "$FED_A" != "$FED_B" ]] || { echo "FAIL: both invites resolved to the same fed" >&2; exit 1; }
echo "A (dying) = $FED_A   B (safe) = $FED_B"

gateway-ldk connect-fed "$FED_B_INVITE" >/dev/null 2>&1 || true

SEED_ERR="$SANDBOX/seed.stderr"
INV=$(wsa receive --to "$FED_A" --amount "$FUND_MSAT" 2>"$SEED_ERR") || {
  echo "FAIL: wallet-cli receive exited non-zero" >&2; cat "$SEED_ERR" >&2; exit 1; }
KEY_FUND=$(sed -n 's/^key: //p' "$SEED_ERR")
[[ -n "$INV" && -n "$KEY_FUND" ]] || {
  echo "FAIL: receive produced no invoice/key (invoice='$INV' key='$KEY_FUND')" >&2
  cat "$SEED_ERR" >&2; exit 1; }
SEND=$(fedimint-cli module lnv2 send "$INV" --gateway "$GW" 2>"$SANDBOX/send.stderr" | tr -d '"[:space:]') || {
  echo "FAIL: fedimint-cli lnv2 send exited non-zero" >&2; cat "$SANDBOX/send.stderr" >&2; exit 1; }
[[ -n "$SEND" ]] || {
  echo "FAIL: lnv2 send produced no operation id" >&2; cat "$SANDBOX/send.stderr" >&2; exit 1; }
# await-send BEFORE await-receive: the swap funds the receiver via the sender's state machine.
fedimint-cli module lnv2 await-send "$SEND" >/dev/null 2>&1 || true
[[ "$(wsa await-receive "$KEY_FUND")" == "claimed" ]] || { echo "FAIL: funding A did not claim" >&2; exit 1; }
A0=$(wsa balance | awk -v id="$FED_A" '$1 == id ":" && $3 == "msat" { print $2 }')
B0=$(wsa balance | awk -v id="$FED_B" '$1 == id ":" && $3 == "msat" { print $2 }')
echo "funded: A=${A0} msat  B=${B0} msat"

# ---------------------------------------------------------------------------------------
echo "== 2. set the gateway's FIXED cost explicitly on both feds =="
# Explicit, not inherited: see the header. Both legs, because the evacuation pays send on A and
# receive on B and the structural predicate is about their SUM.
for fed in "$FED_A" "$FED_B"; do
  gateway-ldk cfg set-fees --federation-id "$fed" --tx-base "$GW_TX_BASE_MSAT" --tx-ppm "$GW_TX_PPM" \
    || { echo "FAIL: could not set gateway tx fees for $fed" >&2; exit 1; }
done
echo "gateway tx fee per leg: base=${GW_TX_BASE_MSAT} msat ppm=${GW_TX_PPM}"

# ---------------------------------------------------------------------------------------
echo "== 3. policy: pin roles, base-only evacuation cap BELOW the fixed cost =="
wsa policy set \
  --spending-fed "$FED_A" --standby-fed "$FED_B" \
  --spending-target 0 --standby-target 0 \
  --evac-fee-base-msat "$CAP_A_BASE" --evac-fee-bps 0 \
  --base-interval-secs 5 --min-interval-secs 1 \
  --probe-min-span-secs 1 --probe-retry-backoff-secs 1 \
  --discover-every-secs 1000000000 >/dev/null
echo "evacuation cap A: base=${CAP_A_BASE} msat bps=0 (flat, under the ~$((2*GW_TX_BASE_MSAT)) msat two-leg fixed cost)"

"$WALLETD" init >/dev/null

# ---------------------------------------------------------------------------------------
echo "== 4. walletd senses A shutting down; the evacuation is STRUCTURALLY refused =="
WALLET_CLI_FORCE_SHUTDOWN="$FED_A" "$WALLETD" > "$LOG1" 2>&1 &
WALLETD_PID=$!
wait_healthy

MARKED_KEY=""
for _ in $(seq 1 90); do
  # The marker rides the evacuation's own operation. Find a non-terminal evacuation row and ask
  # `show` whether its typed structural marker is ACTIVE — never infer it from a failed row.
  while read -r key; do
    [[ -n "$key" ]] || continue
    if "$WALLET_CLI" show "$key" --json 2>/dev/null \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("evacuation_refusal_active") else 1)'; then
      MARKED_KEY="$key"; break
    fi
  done < <("$WALLET_CLI" history 2>/dev/null | awk -F'\t' '$3 == "evacuation" { print $10 }' | head -20)
  [[ -n "$MARKED_KEY" ]] && break
  sleep 2
done
if [[ -z "$MARKED_KEY" ]]; then
  echo "FAIL: no evacuation carried an ACTIVE structural-refusal marker within the window." >&2
  echo "      (An ordinary retryable refusal is NOT this gate — the cap must be structurally" >&2
  echo "       unsatisfiable. Check the gateway fee actually took effect.)" >&2
  "$WALLET_CLI" history | head -20 >&2 || true
  exit 1
fi
echo "structurally refused evacuation: $MARKED_KEY"

"$WALLET_CLI" show "$MARKED_KEY" --json | python3 "$SANDBOX/check_evidence.py" "$CAP_A_BASE" || exit 1

A1=$(wcli_balance_line "$FED_A"); B1=$(wcli_balance_line "$FED_B")
if [[ "$A1" != "$A0" || "$B1" != "$B0" ]]; then
  echo "FAIL: a REFUSED evacuation moved money: A ${A0}->${A1}  B ${B0}->${B1}" >&2
  exit 1
fi
echo "balances unchanged under refusal: A=${A1} B=${B1}"

# ---------------------------------------------------------------------------------------
echo "== 5. raise the cap component-wise; the SCHEDULER must supersede on its own =="
# No manual tick. The whole point of br-n8o is that the daemon's own planner retires the marked
# parent and creates the linked child once the cap qualifies.
"$WALLET_CLI" policy set --evac-fee-base-msat "$CAP_B_BASE" --evac-fee-bps 0 >/dev/null \
  || { echo "FAIL: could not raise the evacuation cap" >&2; exit 1; }
echo "evacuation cap B: base=${CAP_B_BASE} msat bps=0"

CHILD_KEY=""
for _ in $(seq 1 90); do
  CHILD_KEY=$("$WALLET_CLI" show "$MARKED_KEY" --json 2>/dev/null \
    | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: raise SystemExit(0)
print(d.get("superseded_by") or "")' || true)
  [[ -n "$CHILD_KEY" ]] && break
  sleep 2
done
[[ -n "$CHILD_KEY" ]] || {
  echo "FAIL: the scheduler never superseded the marked parent after the qualifying cap raise." >&2
  echo "      This is the br-n8o behaviour under test: the parent is stuck with no child." >&2
  "$WALLET_CLI" show "$MARKED_KEY" --json >&2 || true
  exit 1
}
echo "child: $CHILD_KEY"

# Reciprocal links, and the parent's active marker cleared.
"$WALLET_CLI" show "$CHILD_KEY" --json | python3 "$SANDBOX/check_child_links.py" "$MARKED_KEY" || exit 1
"$WALLET_CLI" show "$MARKED_KEY" --json | python3 "$SANDBOX/check_marker_cleared.py" || exit 1
echo "links reciprocal; parent marker cleared"

# Exactly ONE child: no sibling was created for the same parent.
SIBLINGS=$("$WALLET_CLI" history 2>/dev/null | awk -F'\t' '{print $10}' | while read -r k; do
  [[ -n "$k" ]] || continue
  "$WALLET_CLI" show "$k" --json 2>/dev/null \
    | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: raise SystemExit(0)
print(d.get("supersedes") or "")' || true
done | grep -c "^${MARKED_KEY}$" || true)
[[ "$SIBLINGS" == "1" ]] || { echo "FAIL: expected exactly 1 child for the parent, found ${SIBLINGS}" >&2; exit 1; }
echo "exactly one child"

# ---------------------------------------------------------------------------------------
echo "== 6. the replacement MOVES REAL MONEY, within cap B and not cap A =="
MOVED=""
for _ in $(seq 1 120); do
  B2=$(wcli_balance_line "$FED_B")
  if [[ "$B2" =~ ^[0-9]+$ ]] && (( B2 > B0 + RECV_SLACK )); then MOVED="$B2"; break; fi
  sleep 3
done
[[ -n "$MOVED" ]] || { echo "FAIL: the replacement never credited B (B still $(wcli_balance_line "$FED_B"))" >&2; exit 1; }
A2=$(wcli_balance_line "$FED_A")
CREDIT=$(( MOVED - B0 ))
DEBIT=$(( A0 - A2 ))
FEE=$(( DEBIT - CREDIT ))
echo "moved: A ${A0}->${A2} (debit ${DEBIT})  B ${B0}->${MOVED} (credit ${CREDIT})  fee ${FEE}"
(( CREDIT > 0 ))       || { echo "FAIL: destination credit must be positive" >&2; exit 1; }
(( DEBIT >= CREDIT ))  || { echo "FAIL: source debit ${DEBIT} must cover the credit ${CREDIT}" >&2; exit 1; }
(( FEE <= CAP_B_BASE )) || { echo "FAIL: actual fee ${FEE} exceeds the raised cap ${CAP_B_BASE}" >&2; exit 1; }
# The discriminating half: the fee must NOT have fitted the original cap, or the refusal that
# started this gate was never real and the whole fixture proved nothing.
(( FEE > CAP_A_BASE ))  || { echo "FAIL: fee ${FEE} fits the ORIGINAL cap ${CAP_A_BASE} — the structural refusal was not genuine" >&2; exit 1; }
echo "fee ${FEE} fits cap B (${CAP_B_BASE}) and NOT cap A (${CAP_A_BASE}) — the refusal was real"

# ---------------------------------------------------------------------------------------
echo "== 7. restart + reconcile: no second child, no second execution =="
stop_walletd
"$WALLETD" > "$LOG2" 2>&1 &
WALLETD_PID=$!
wait_healthy
sleep 8   # let at least one full scheduler cycle reconcile the restarted state

A3=$(wcli_balance_line "$FED_A"); B3=$(wcli_balance_line "$FED_B")
[[ "$A3" == "$A2" && "$B3" == "$MOVED" ]] || {
  echo "FAIL: balances moved across restart: A ${A2}->${A3}  B ${MOVED}->${B3}" >&2; exit 1; }
SIBLINGS2=$("$WALLET_CLI" history 2>/dev/null | awk -F'\t' '{print $10}' | while read -r k; do
  [[ -n "$k" ]] || continue
  "$WALLET_CLI" show "$k" --json 2>/dev/null \
    | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: raise SystemExit(0)
print(d.get("supersedes") or "")' || true
done | grep -c "^${MARKED_KEY}$" || true)
[[ "$SIBLINGS2" == "1" ]] || { echo "FAIL: restart produced ${SIBLINGS2} children for the parent" >&2; exit 1; }
stop_walletd

echo
echo "PASS: structural refusal -> qualifying cap raise -> exactly one linked child -> real"
echo "      movement within the new cap and outside the old -> stable across restart."
