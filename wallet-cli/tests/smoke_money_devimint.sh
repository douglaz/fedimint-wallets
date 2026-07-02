#!/usr/bin/env bash
# devimint smoke test for the `wallet-cli` MONEY path — receive + pay over lnv2
# (Phase 1 step 4a, ADR-0023). Exercises the real lnv2 `is_direct_swap` path validated in
# docs/fedimint-mechanics.md §5 (single federation, one shared gateway).
#
# NOT part of the rb-lite gate (compile + clippy + fmt + unit tests). Like
# smoke_devimint.sh it needs a LIVE devimint federation, so the maintainer runs it manually:
#
#   # 1. Build wallet-cli (from this repo):
#   cd ~/p/fedimint-wallets
#   nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
#
#   # 2. Build fedimint/devimint once (from ~/p/fedimint), per docs/devimint-runbook.md §1:
#   cd ~/p/fedimint
#   nix develop -c cargo build --workspace --bins
#
#   # 3. Bring up a dev federation and run this script inside it (lnv2 enabled):
#   nix develop -c bash -c '
#     set -euo pipefail
#     source scripts/_common.sh
#     add_target_dir_to_path
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export RUST_LOG=warn
#     export FM_ENABLE_MODULE_LNV2=1           # ensure lnv2 + the LDK gateway are up
#     devimint --link-test-dir "${CARGO_BUILD_TARGET_DIR:-target}/devimint" \
#       --num-feds 1 dev-fed \
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_money_devimint.sh
#   '
#
# Inside `dev-fed --exec` devimint sets: FM_INVITE_CODE (fed-0's invite), FM_PORT_GW_LDK
# (the LDK lnv2 gateway's API port), and puts the funded internal client wrapper
# `fedimint-cli` (client `default-0`, ~1M sats, joined to fed-0) on PATH. Our `wallet-cli`
# joins the SAME federation with its OWN fresh seed, so it is a distinct client; money moves
# between the two through the shared LDK gateway's internal swap.
#
# IMPORTANT: the lnv2 gateway is NOT auto-registered into the federation's vetted list
# (runbook §4), so EVERY lnv2 call — on both `wallet-cli` and `fedimint-cli` — passes
# `--gateway "$GW"` explicitly.
#
# Flow:
#   RECEIVE (devimint default-0 -> our wallet):
#     wallet-cli receive --amount N --gateway $GW   -> invoice INV_A (+ op id OP_A on stderr)
#     fedimint-cli module lnv2 send INV_A --gateway $GW    (default-0 pays; gateway swaps in)
#     wallet-cli await-receive OP_A --fed FED       -> claimed
#     wallet-cli balance                            -> ~N
#   PAY (our wallet -> devimint default-0):
#     fedimint-cli module lnv2 receive M --gateway $GW     -> [INV_B, OP_B_DEV]
#     wallet-cli pay INV_B --fed FED --gateway $GW  -> started OP_B
#     wallet-cli await-send OP_B --fed FED          -> success <preimage>
#     fedimint-cli module lnv2 await-receive OP_B_DEV      -> "Claimed" (money landed)
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run this inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run this inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build it first: nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }
command -v jq >/dev/null || { echo "FAIL: jq not on PATH (it is in the fedimint nix devshell)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
RECEIVE_MSAT=500000   # our wallet receives 500 sat...
PAY_MSAT=100000       # ...then pays 100 sat back, leaving headroom for fees

DATA_DIR="$(mktemp -d)"
RECV_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$RECV_ERR"' EXIT

wcli() { "$WALLET_CLI" --data-dir "$DATA_DIR" "$@"; }
balance_msat_for_fed() {
  local fed_id="$1"
  wcli balance | awk -v id="$fed_id" '$1 == id ":" && $3 == "msat" { print $2; exit }'
}

echo "== join =="
FED_ID=$(wcli join "$FM_INVITE_CODE")
echo "joined federation: $FED_ID"

# ---------------------------------------------------------------------------------------
echo "== RECEIVE: our wallet mints an invoice, devimint's funded client pays it =="
# Invoice -> stdout; "operation_id: <hex>" -> stderr (captured to $RECV_ERR).
INV_A=$(wcli receive --amount "$RECEIVE_MSAT" --gateway "$GW" 2>"$RECV_ERR")
OP_A=$(grep -oiE 'operation_id: [0-9a-f]{64}' "$RECV_ERR" | grep -oiE '[0-9a-f]{64}' | head -n1)
if [[ -z "$INV_A" || -z "$OP_A" ]]; then
  echo "FAIL: receive did not yield an invoice (stdout) and an op id (stderr):" >&2
  echo "  invoice=$INV_A" >&2; echo "  --- receive stderr ---" >&2; cat "$RECV_ERR" >&2
  exit 1
fi
echo "invoice: $INV_A"
echo "receive op: $OP_A"

# devimint's funded client pays the invoice; the LDK gateway direct-swaps it into our wallet.
# IMPORTANT (devimint investigation): await the payer's SEND to Success FIRST. lnv2's internal
# swap funds OUR incoming contract as part of the sender's send state machine completing, so by
# awaiting the send we know the contract is funded before we await our receive — the claim is then
# prompt + reliable (6/6). Racing `await-receive` against a not-yet-funded contract is slow/flaky:
# the receive SM long-polls `await_incoming_contract` and retries on transport timeouts.
echo "-- devimint (default-0) pays the invoice; await the send so the swap completes --"
SEND_A=$(fedimint-cli module lnv2 send "$INV_A" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND_A" >/dev/null 2>&1 || true

echo "-- await our receive claim --"
RECV_STATE=$(wcli await-receive "$OP_A" --fed "$FED_ID")
echo "await-receive: $RECV_STATE"
if [[ "$RECV_STATE" != "claimed" ]]; then
  echo "FAIL: expected receive to be 'claimed', got '$RECV_STATE'" >&2
  exit 1
fi

BAL_AFTER_RECV=$(balance_msat_for_fed "$FED_ID")
if [[ ! "$BAL_AFTER_RECV" =~ ^[0-9]+$ ]]; then
  echo "FAIL: could not parse balance for $FED_ID" >&2
  wcli balance >&2
  exit 1
fi
echo "balance after receive: ${BAL_AFTER_RECV} msat (expected ~${RECEIVE_MSAT})"
# lnv2 mints the invoice for the full RECEIVE_MSAT (the SENDER pays that), but commits an
# incoming contract of `amount - receive_fee` — i.e. the gateway's lnv2 receive fee is taken
# out of OUR claimable balance, not the sender's (fedimint-lnv2-client
# `create_contract_and_fetch_invoice`: contract_amount = receive_fee.subtract_from(amount);
# the invoice still carries the full amount). That fee is capped at RECEIVE_FEE_LIMIT
# (50 sat + 0.5%); devimint's default LDK gateway charges ~nothing, so nearly all of
# RECEIVE_MSAT lands. Floor tolerates the full fee cap so the tolerance matches the mechanics;
# never expect MORE than the invoice amount.
RECEIVE_FEE_CAP=$(( 50000 + RECEIVE_MSAT / 200 ))   # 50 sat + 0.5% = the lnv2 receive-fee limit
if (( BAL_AFTER_RECV < RECEIVE_MSAT - RECEIVE_FEE_CAP || BAL_AFTER_RECV > RECEIVE_MSAT )); then
  echo "FAIL: balance ${BAL_AFTER_RECV} msat is not ~${RECEIVE_MSAT} msat after receive" >&2
  exit 1
fi

# ---------------------------------------------------------------------------------------
echo "== PAY: devimint's client mints an invoice, our wallet pays it =="
RECV_B_JSON=$(fedimint-cli module lnv2 receive "$PAY_MSAT" --gateway "$GW" 2>/dev/null)
INV_B=$(jq -r '.[0]' <<<"$RECV_B_JSON")
OP_B_DEV=$(jq -r '.[1]' <<<"$RECV_B_JSON")
if [[ -z "$INV_B" || "$INV_B" == "null" ]]; then
  echo "FAIL: devimint did not mint a payable invoice: $RECV_B_JSON" >&2
  exit 1
fi
echo "invoice to pay: $INV_B"

echo "-- our wallet pays --"
PAY_OUT=$(wcli pay "$INV_B" --fed "$FED_ID" --gateway "$GW")
echo "pay: $PAY_OUT"
OP_B=$(awk '{print $2}' <<<"$PAY_OUT")
case "$PAY_OUT" in
  started\ *) : ;;  # a fresh payment, as expected on the first pay of this invoice
  *) echo "FAIL: expected 'started <op>', got '$PAY_OUT'" >&2; exit 1 ;;
esac

echo "-- await our send settlement --"
SEND_STATE=$(wcli await-send "$OP_B" --fed "$FED_ID")
echo "await-send: $SEND_STATE"
case "$SEND_STATE" in
  success\ *) : ;;  # carries the preimage
  *) echo "FAIL: expected 'success <preimage>', got '$SEND_STATE'" >&2; exit 1 ;;
esac

# Confirm the money actually landed on the devimint side too.
DEV_RECV_STATE=$(fedimint-cli module lnv2 await-receive "$OP_B_DEV" 2>/dev/null)
echo "devimint await-receive: $DEV_RECV_STATE"
if [[ "$DEV_RECV_STATE" != '"Claimed"' && "$DEV_RECV_STATE" != "Claimed" ]]; then
  echo "FAIL: devimint side did not claim the payment: $DEV_RECV_STATE" >&2
  exit 1
fi

# Dedup check (spec §4): re-paying the SAME invoice must NOT be a fresh payment and must NOT
# move money again — the lnv2 client recognizes it (already-paid / already-in-flight).
echo "-- re-pay the same invoice: expect dedup, not a second debit --"
REPAY_OUT=$(wcli pay "$INV_B" --fed "$FED_ID" --gateway "$GW")
echo "re-pay: $REPAY_OUT"
case "$REPAY_OUT" in
  already-paid\ *|already-in-flight\ *) : ;;
  *) echo "FAIL: expected re-pay to dedup (already-paid/already-in-flight), got '$REPAY_OUT'" >&2; exit 1 ;;
esac

BAL_AFTER_PAY=$(balance_msat_for_fed "$FED_ID")
if [[ ! "$BAL_AFTER_PAY" =~ ^[0-9]+$ ]]; then
  echo "FAIL: could not parse balance for $FED_ID" >&2
  wcli balance >&2
  exit 1
fi
echo "balance after pay: ${BAL_AFTER_PAY} msat"
if (( BAL_AFTER_PAY >= BAL_AFTER_RECV )); then
  echo "FAIL: balance did not drop after paying (${BAL_AFTER_RECV} -> ${BAL_AFTER_PAY} msat)" >&2
  exit 1
fi

echo "OK: wallet-cli money smoke passed (receive -> claimed -> pay -> success, dedup on re-pay)"
