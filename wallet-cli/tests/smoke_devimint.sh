#!/usr/bin/env bash
# devimint smoke test for `wallet-cli join`/`balance` (Phase 1 step 3, ADR-0023).
#
# NOT part of the rb-lite gate (compile + clippy + fmt) — this needs a LIVE devimint
# federation. The maintainer runs it manually, e.g.:
#
#   # 1. Build wallet-cli (from this repo):
#   cd ~/p/fedimint-wallets
#   nix develop /home/master/p/fedimint -c cargo build -p wallet-cli
#
#   # 2. Build fedimint/devimint once (from ~/p/fedimint), per docs/devimint-runbook.md:
#   cd ~/p/fedimint
#   nix develop -c cargo build --workspace --bins
#
#   # 3. Bring up a dev federation and run this script inside it:
#   nix develop -c bash -c '
#     set -euo pipefail
#     source scripts/_common.sh
#     add_target_dir_to_path
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export RUST_LOG=warn
#     export FM_ENABLE_MODULE_LNV2=1
#     devimint --link-test-dir "${CARGO_BUILD_TARGET_DIR:-target}/devimint" \
#       --num-feds 1 dev-fed \
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_devimint.sh
#   '
#
# Inside `dev-fed --exec`, devimint sets FM_INVITE_CODE (fed-0's invite code). This
# script drives the ALREADY-BUILT `wallet-cli` binary against that live federation the
# same way devimint drives `fedimint-cli` (ADR-0023) — not the Rust API in-process.
#
# Asserts: `join` succeeds and returns a federation id; a freshly joined federation (no
# receive/pay wired yet — step 4) has a balance of exactly 0.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run this inside \`devimint dev-fed --exec\`}"

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build it first: nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi

DATA_DIR="$(mktemp -d)"
trap 'rm -rf "$DATA_DIR"' EXIT

echo "== join =="
JOIN_OUT=$("$WALLET_CLI" --standalone --data-dir "$DATA_DIR" join "$FM_INVITE_CODE")
JOIN_KEY=${JOIN_OUT#* }
FED_ID=$(cut -d: -f2 <<<"$JOIN_KEY")
[[ "$("$WALLET_CLI" --standalone --data-dir "$DATA_DIR" await-move "$JOIN_KEY")" == "done" ]]
echo "joined federation: $FED_ID"

echo "== balance =="
BALANCE_OUT=$("$WALLET_CLI" --standalone --data-dir "$DATA_DIR" balance)
echo "$BALANCE_OUT"

if ! grep -qF "${FED_ID}: 0 msat" <<<"$BALANCE_OUT"; then
  echo "FAIL: expected ${FED_ID} balance to be 0 msat, got:" >&2
  echo "$BALANCE_OUT" >&2
  exit 1
fi
if ! grep -qF "total (1/1 federations): 0 msat" <<<"$BALANCE_OUT"; then
  echo "FAIL: expected total balance to be 0 msat, got:" >&2
  echo "$BALANCE_OUT" >&2
  exit 1
fi

echo "== list-feds =="
"$WALLET_CLI" --standalone --data-dir "$DATA_DIR" list-feds

echo "OK: wallet-cli join + balance smoke test passed"
