#!/usr/bin/env bash
# devimint smoke test for the `wallet-cli` MOVE path — the cross-federation transfer, the
# wallet's core capability (Phase 1 step 4b-live-2, spec §7). A `Move A→B` is TWO ordinary
# fedimint operations through a SHARED gateway's internal swap: B mints an invoice, A pays it,
# the gateway direct-swaps A's outgoing contract into B's incoming contract, both legs settle
# (docs/fedimint-mechanics.md §5). The wallet joins BOTH federations and drives the whole thing
# with a single `wallet-cli move`.
#
# NOT part of the rb-lite gate (compile + clippy + fmt + unit tests). Like the other smokes it
# needs a LIVE devimint setup, so the maintainer runs it manually.
#
# ────────────────────────────────────────────────────────────────────────────────────────────
# TWO FEDERATIONS — READ THIS FIRST (the setup this smoke needs and how to get it)
# ────────────────────────────────────────────────────────────────────────────────────────────
# `devimint dev-fed` brings up exactly ONE federation (fed-0 → $FM_INVITE_CODE). Verified against
# devimint 0.12-alpha: `dev-fed` constructs a single `Federation` (name "default", fed_index 0);
# `--num-feds N` only RESERVES port ranges — it does NOT auto-start or auto-join a second
# federation. So a genuine cross-fed move needs a SECOND federation, B, that the SAME LDK gateway
# serves. You supply B's invite via $FED_B_INVITE (or $FM_INVITE_CODE_B). Two ways to get it:
#
#   (a) If your devimint variant DOES stand up fed-1 (e.g. a patched `--num-feds 2` that starts
#       both), its invite is written to that federation's data dir as an `invite-code` file
#       (devimint/src/federation.rs: each fed writes `invite-code` into its FM_DATA_DIR, then
#       copies it into the client dir). After the LDK gateway connects to it,
#       `gateway-ldk get-invite-codes` also lists every connected federation's invite.
#
#   (b) Otherwise stand up a second federation yourself and register it with the shared gateway:
#         gateway-ldk connect-fed "$FED_B_INVITE"     # gateway-ldk = the FM_PORT_GW_LDK alias
#       (`connect-fed <invite_code>` is the gateway-cli General command; the devimint alias
#       `gateway-ldk` wraps `gateway-cli --rpcpassword=theresnosecondbest -a .../$FM_PORT_GW_LDK`.)
#
# EITHER WAY the LDK gateway MUST serve BOTH feds for the internal `is_direct_swap` to fire — fed
# A is auto-connected by dev-fed; you (or this script, best-effort below) must connect fed B.
# The literal two-fed variant was flagged "not yet validated" in docs/fedimint-mechanics.md; this
# script IS that validation once B is provided.
# ────────────────────────────────────────────────────────────────────────────────────────────
#
#   # 1. Build wallet-cli (from this repo):
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
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_move_devimint.sh
#   '
#
# Inside `dev-fed --exec` devimint sets FM_INVITE_CODE (fed A/fed-0's invite) and FM_PORT_GW_LDK
# (the LDK lnv2 gateway's API port), and puts the funded internal client `fedimint-cli` (~1M sats,
# joined to fed A) + the gateway aliases (`gateway-ldk`) on PATH. Our `wallet-cli` joins BOTH
# feds with its OWN fresh seed. We fund our fed-A balance via direct-inflow (the funded client
# pays), then MOVE that balance A→B through the shared gateway.
#
# The EXACT-net gate: as in the direct-inflow smoke, devimint zeroes only the gateway's LIGHTNING
# routing fee (FM_DEFAULT_ROUTING_FEES=0,0), NOT its TRANSACTION fee — so B nets ~N (a hair under,
# never over: the gross-up floors the gateway ppm to invert its real `subtract_from`), and A falls
# by N + both legs' fees. We assert B is within a 1-sat slack BELOW N (never above), and A dropped
# by at least N and at most N + a generous fee headroom.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run this inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run this inside \`devimint dev-fed --exec\` with FM_ENABLE_MODULE_LNV2=1}"

# Fed B's invite: primary $FED_B_INVITE, alias $FM_INVITE_CODE_B (the "second FM_INVITE_CODE var"
# form). Fed A is $FM_INVITE_CODE (fed-0). See the TWO FEDERATIONS note at the top for how to get it.
FED_B_INVITE="${FED_B_INVITE:-${FM_INVITE_CODE_B:-}}"
if [[ -z "$FED_B_INVITE" ]]; then
  echo "FAIL: FED_B_INVITE (or FM_INVITE_CODE_B) not set — this is a TWO-federation smoke." >&2
  echo "      dev-fed only brings up fed A (\$FM_INVITE_CODE). Stand up a second federation B" >&2
  echo "      served by the same LDK gateway and set FED_B_INVITE to its invite code." >&2
  echo "      See the 'TWO FEDERATIONS' note at the top of this script." >&2
  exit 1
fi
if [[ "$FED_B_INVITE" == "$FM_INVITE_CODE" ]]; then
  echo "FAIL: FED_B_INVITE equals FM_INVITE_CODE — --from and --to must be DIFFERENT feds." >&2
  exit 1
fi

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
if [[ ! -x "$WALLET_CLI" ]]; then
  echo "FAIL: wallet-cli binary not found/executable at $WALLET_CLI" >&2
  echo "Build it first: nix develop /home/master/p/fedimint -c cargo build -p wallet-cli" >&2
  exit 1
fi
command -v fedimint-cli >/dev/null || { echo "FAIL: fedimint-cli not on PATH (run inside dev-fed --exec)" >&2; exit 1; }

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
FUND_MSAT=500000    # fund fed A with 500 sat via direct-inflow (must exceed MOVE + fees)
MOVE_MSAT=100000    # move 100 sat A->B; B must net EXACTLY this (within slack, never over)
RECV_SLACK=1000     # 1 sat — bounds lnv2 receive-fee-quote under-estimate on B (per direct-inflow smoke)
A_FEE_HEADROOM=50000 # 50 sat — generous upper bound on the TOTAL move fee A pays on top of MOVE_MSAT

DATA_DIR="$(mktemp -d)"
DI_ERR="$(mktemp)"
MOVE_ERR="$(mktemp)"
trap 'rm -rf "$DATA_DIR" "$DI_ERR" "$MOVE_ERR"' EXIT

wcli() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW" "$@"; }
join_fed() {
  local started key state
  started=$(wcli join "$1") || return
  key=${started#* }
  state=$(wcli await-move "$key") || return
  [[ "$state" == "done" ]] || { echo "join $key did not settle: $state" >&2; return 1; }
  cut -d: -f2 <<<"$key"
}
balance_msat_for_fed() {
  local fed_id="$1"
  # NOTE: no `exit` in awk — it must consume ALL of `wcli balance`'s output, else awk closes the
  # pipe early and wallet-cli gets SIGPIPE mid-print (panics) once there are multiple feds.
  wcli balance | awk -v id="$fed_id" '$1 == id ":" && $3 == "msat" { print $2 }'
}

echo "== join BOTH federations =="
FED_A=$(join_fed "$FM_INVITE_CODE")
FED_B=$(join_fed "$FED_B_INVITE")
echo "fed A (source): $FED_A"
echo "fed B (dest):   $FED_B"
if [[ "$FED_A" == "$FED_B" ]]; then
  echo "FAIL: both invites resolved to the SAME federation id ($FED_A) — need two distinct feds." >&2
  exit 1
fi

# Ensure the shared LDK gateway serves fed B (fed A is auto-connected by dev-fed). Best-effort:
# 'already connected' is fine; if the alias is missing, the move below will surface a clear error.
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
INV_A=$(wcli direct-inflow --to "$FED_A" --amount "$FUND_MSAT" 2>"$DI_ERR")
KEY_FUND=$(sed -n 's/^key: //p' "$DI_ERR")
if [[ -z "$INV_A" || -z "$KEY_FUND" ]]; then
  echo "FAIL: funding direct-inflow did not yield an invoice + operation key:" >&2
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
echo "pre-move balances: A=${A0} msat  B=${B0} msat"
if (( A0 < MOVE_MSAT + A_FEE_HEADROOM )); then
  echo "FAIL: fed A only has ${A0} msat, not enough to move ${MOVE_MSAT} + fees" >&2
  exit 1
fi

# ---------------------------------------------------------------------------------------
echo "== MOVE ${MOVE_MSAT} msat  fed A -> fed B  (the cross-federation transfer) =="
MOVE_PHASE1=$(wcli move --from "$FED_A" --to "$FED_B" --amount "$MOVE_MSAT" 2>"$MOVE_ERR")
MOVE_KEY=$(sed -n 's/^key: //p' "$MOVE_ERR")
if [[ "$MOVE_PHASE1" != "started $MOVE_KEY" ]]; then
  cat "$MOVE_ERR" >&2
  echo "FAIL: move did not start: $MOVE_PHASE1" >&2
  exit 1
fi
MOVE_STATE=$(wcli await-move "$MOVE_KEY")
echo "move: $MOVE_STATE  (key: $MOVE_KEY)"
if [[ "$MOVE_STATE" != "done" ]]; then
  echo "FAIL: expected move to be 'done', got '$MOVE_STATE'" >&2
  echo "  --- move stderr ---" >&2; cat "$MOVE_ERR" >&2
  exit 1
fi

A1=$(balance_msat_for_fed "$FED_A")
B1=$(balance_msat_for_fed "$FED_B")
echo "post-move balances: A=${A1} msat  B=${B1} msat"

# B rose by ~MOVE_MSAT: within [MOVE-slack, MOVE], NEVER over (the wallet must never over-credit).
B_DELTA=$(( B1 - B0 ))
echo "B delta: ${B_DELTA} msat (target ${MOVE_MSAT}, slack ${RECV_SLACK} below, never over)"
if (( B_DELTA > MOVE_MSAT || B_DELTA < MOVE_MSAT - RECV_SLACK )); then
  echo "FAIL: fed B rose by ${B_DELTA} msat, not within [$((MOVE_MSAT - RECV_SLACK)), ${MOVE_MSAT}]" >&2
  exit 1
fi

# A fell by MOVE_MSAT + fees: at least MOVE_MSAT (A funds the grossed-up invoice + send fee),
# at most MOVE_MSAT + a generous fee headroom.
A_DELTA=$(( A0 - A1 ))
echo "A delta: -${A_DELTA} msat (>= ${MOVE_MSAT}, <= $((MOVE_MSAT + A_FEE_HEADROOM)))"
if (( A_DELTA < MOVE_MSAT || A_DELTA > MOVE_MSAT + A_FEE_HEADROOM )); then
  echo "FAIL: fed A fell by ${A_DELTA} msat, not within [${MOVE_MSAT}, $((MOVE_MSAT + A_FEE_HEADROOM))]" >&2
  exit 1
fi

# ---------------------------------------------------------------------------------------
echo "== IDEMPOTENCY: re-running the SAME move must NOT move funds again =="
# Same (from, to, amount, default fee_cap, occurrence) -> same key -> apply SKIPS the drive
# (the intent is Done); no second invoice, no second pay.
MOVE_PHASE2=$(wcli move --from "$FED_A" --to "$FED_B" --amount "$MOVE_MSAT" 2>/dev/null)
MOVE_KEY2=${MOVE_PHASE2#* }
MOVE_STATE2=$(wcli await-move "$MOVE_KEY2")
echo "re-run move: $MOVE_PHASE2 -> $MOVE_STATE2"
if [[ "$MOVE_STATE2" != "done" || "$MOVE_KEY2" != "$MOVE_KEY" ]]; then
  echo "FAIL: idempotent re-run expected the same key and 'done', got '$MOVE_PHASE2' -> '$MOVE_STATE2'" >&2
  exit 1
fi
A2=$(balance_msat_for_fed "$FED_A")
B2=$(balance_msat_for_fed "$FED_B")
if (( A2 != A1 || B2 != B1 )); then
  echo "FAIL: balances changed on an idempotent re-run: A ${A1}->${A2}, B ${B1}->${B2}" >&2
  exit 1
fi

# reconcile must be a no-op: the move intent is Done (not pending/awaiting). The summary
# format changed in phase 6a step 3 (redriven=..., was awaiting=...) — assert the field
# that carries this check's intent: a Done move re-drives nothing.
echo "-- reconcile: a Done move must not be re-driven --"
RECONCILE_OUT=$(wcli reconcile)
echo "reconcile: $RECONCILE_OUT"
case "$RECONCILE_OUT" in
  *"redriven=0"*) : ;;
  *) echo "FAIL: expected reconcile to report 'redriven=0', got '$RECONCILE_OUT'" >&2; exit 1 ;;
esac
A3=$(balance_msat_for_fed "$FED_A")
B3=$(balance_msat_for_fed "$FED_B")
if (( A3 != A1 || B3 != B1 )); then
  echo "FAIL: balances changed after reconcile: A ${A1}->${A3}, B ${B1}->${B3}" >&2
  exit 1
fi

echo "OK: wallet-cli move smoke passed (A->B done; B netted ~${MOVE_MSAT} msat, never over; A fell by ${A_DELTA} msat; idempotent re-run + reconcile no-ops)"
