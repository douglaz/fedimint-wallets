#!/usr/bin/env bash
# devimint smoke test for `wallet-cli join`/`balance` (Phase 1 step 3, ADR-0023).
#
# NOT part of the rb-lite gate (compile + clippy + fmt) — this needs a LIVE devimint
# federation. The maintainer runs it manually, e.g.:
#
#   # 1. Follow docs/devimint-runbook.md §1's exact-pin two-fed patch/release build. It exports
#   #    FEDIMINT_WORKTREE; do not substitute an arbitrary fedimint checkout.
#   # 2. Build wallet-cli (from this repo):
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
#   run_exact_cargo build --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-cli
#
#   # 3. Bring up a dev federation from the exact-pinned release binary:
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
#     for binary in "${launch_binaries[@]}"; do
#       if [[ "$binary" != /* || ! -f "$binary" || ! -x "$binary" ]]; then
#         echo "refusing launch binary that is not an absolute regular executable: $binary" >&2
#         exit 1
#       fi
#     done
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export RUST_LOG=warn
#     export FM_ENABLE_MODULE_LNV1=1
#     export FM_ENABLE_MODULE_MINT=1
#     export FM_ENABLE_MODULE_WALLET=1
#     export FM_ENABLE_MODULE_LNV2=1
#     "$DEVIMINT_BIN" --link-test-dir "$FEDIMINT_WORKTREE/target-nix/devimint" \
#       --num-feds 1 dev-fed \
#       --exec bash "$WALLETS_REPO/wallet-cli/tests/smoke_devimint.sh"
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
  echo 'Follow docs/devimint-runbook.md §1 to export FEDIMINT_WORKTREE, then build:' >&2
  echo '  cd "$WALLETS_REPO"' >&2
  echo '  refuse_cargo_config_for_dir "$WALLETS_REPO"' >&2
  echo '  refuse_ambient_rust_build_overrides' >&2
  echo '  [[ ! -e .shrc.local && ! -L .shrc.local ]] || { echo "refusing wallets .shrc.local as a reproducibility precaution" >&2; exit 1; }' >&2
  echo '  declare -F run_exact_nix_develop >/dev/null || { echo "missing run_exact_nix_develop; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }' >&2
  echo '  declare -F run_exact_cargo >/dev/null || { echo "missing run_exact_cargo; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }' >&2
  echo '  declare -F reset_exact_target_dir >/dev/null || { echo "missing reset_exact_target_dir; replay docs/devimint-runbook.md §1 in this same shell" >&2; exit 1; }' >&2
  echo '  reset_exact_target_dir "$WALLETS_REPO/target-nix"' >&2
  echo '  run_exact_cargo build --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-cli' >&2
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
