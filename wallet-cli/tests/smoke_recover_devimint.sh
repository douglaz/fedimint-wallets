#!/usr/bin/env bash
# THE SEED-RECOVERY GATE (br-m9m, `docs/archive/wallet-recovery-spec.md`): proves that the SEED ALONE —
# plus the federation's invite — rebuilds a lost wallet's ecash EXACTLY, in BOTH loss shapes the
# no-wipe V1 admits, through BOTH front ends that can drive a recovery.
#
# Phase A — whole-store loss, recovered UNDER WALLETD (the `docs/real-sats-pilot-runbook.md`
# "disk dies" path, verbatim: `walletd init` → `restore-mnemonic` → start the daemon →
# `wallet-cli recover` → `await-move`). The daemon keeps the detached D5 recovery driver alive
# for the whole replay, so this covers `POST /v1/recover` + the long-poll await an operator
# actually uses for real sats:
#   1. store 1: join the devimint federation, fund it over lnv2, record the exact spendable msat
#   2. capture the backup unit: `walletd mnemonic` (the 12 words) — BEFORE any loss
#   3. LOSE store 1 for real (`rm -rf`): no client.db, no journal.db, nothing copied forward
#   4. store 2 (a genuinely EMPTY data dir): `walletd restore-mnemonic` imports the words, walletd
#      comes up on it, and we PROVE the daemon's own view is empty — zero federations, total
#      0 msat — so a later non-zero balance can only have come from recovery, never a leftover
#   5. `wallet-cli recover <invite>` + `await-move` → done (recovery replays module history)
#   6. assert the restored balance == the pre-loss balance with ZERO slack (integer `-eq`).
#      Recovery restores the ecash the wallet held; it adds no fee, so anything but exact
#      equality is a real defect. (Fee slack applies ONLY to the step-1 lnv2 funding.)
#
# Phase B — journal-only loss, recovered STANDALONE (the second admitted shape, and the second
# front end). `journal.db` is deleted while `client.db` — which holds the seed AND phase A's now
# ORPHANED client partition — survives:
#   7. the store looks unregistered again (D3 permits recovery), so recovery allocates a FRESH
#      partition one past the orphan (`next_db_prefix`) and leaves the orphan inert. The prefix
#      ARITHMETIC is unit-covered; what this phase adds is the live proof that a recovery over a
#      real store still holding that orphan completes at all (the shape a typed-DB decode once bit)
#   8. same EXACT-equality assertion against the same pre-loss number, then the recovered fed is a
#      `userapproved` candidate (D4.6 — else the allocator would never spend from it) and a real
#      lnv2 payment settles out of it.
# Standalone admits recovery and then ABORTS its detached driver at command shutdown (§6a.7), so
# here the replay actually runs inside `await-move`'s reconcile re-drive (abandon-and-resume) —
# the first thing to know when debugging a phase-B timeout.
#
# Both phases recover into a store with NO registry row for the federation, on purpose: D3 REFUSES
# a recover over a still-registered federation (a surviving journal holds re-drivable Pay/Move
# intents that a fresh empty oplog would double-pay), so recovery only ever runs in exactly these
# two shapes.
#
# NOT part of the rb-lite gate (compile + clippy + fmt + unit tests): it needs a LIVE devimint
# federation, so it is run deliberately:
#
#   # 1. Build the wallet binaries FRESH (a stale binary tests old code — the #1 false pass):
#   cd /home/master/p/fedimint-wallets
#   CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix \
#     nix develop /home/master/p/fedimint -c cargo build -p wallet-daemon -p wallet-cli
#
#   # 2. Bring up a dev federation and run this script inside it (lnv2 enabled):
#   cd ~/p/fedimint
#   export CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint/target-nix
#   nix develop -c bash -c '
#     set -euo pipefail
#     source scripts/_common.sh
#     add_target_dir_to_path
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"   # alias wrappers dev-fed needs
#     export FM_ENABLE_MODULE_LNV2=1
#     devimint --link-test-dir "$CARGO_BUILD_TARGET_DIR/devimint" --num-feds 1 dev-fed \
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_recover_devimint.sh'
#
# Inside `dev-fed --exec` devimint sets FM_INVITE_CODE (fed-0's invite) and FM_PORT_GW_LDK (the
# LDK lnv2 gateway's API port), and puts the funded internal client `fedimint-cli` on PATH. That
# gateway is NOT in the federation's vetted list (runbook §4), so every lnv2 route on both sides
# pins it explicitly — `--gateway "$GW"` standalone, `gateway = "$GW"` in walletd.toml.
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run with FM_ENABLE_MODULE_LNV2=1}"

# EXPLICIT paths, never a PATH copy: a stale binary would test old code and pass falsely.
WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/wallet-cli}"
WALLETD="${WALLETD_BIN:-/home/master/p/fedimint-wallets/target-nix/debug/walletd}"
for f in "$WALLET_CLI" "$WALLETD"; do
  [[ -x "$f" ]] || {
    echo "FAIL: missing binary $f" >&2
    echo "Build it: CARGO_BUILD_TARGET_DIR=/home/master/p/fedimint-wallets/target-nix \\" >&2
    echo "  nix develop /home/master/p/fedimint -c cargo build -p wallet-daemon -p wallet-cli" >&2
    exit 1
  }
done
for c in fedimint-cli jq awk; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not on PATH (run inside dev-fed --exec)" >&2; exit 1; }
done

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
PORT=19741               # walletd's bind port for phase A (sandboxed, one process at a time)
FUND_MSAT=500000         # what store 1 receives before it is lost
SPEND_MSAT=50000         # the post-recovery spendability probe (well under the restored balance)
RECOVER_TIMEOUT=900      # seconds to await a recovery operation's terminal state

SANDBOX="$(mktemp -d)"
# Sandbox the XDG homes so nothing here can ever touch a real ~/.local/share/walletd.
export XDG_CONFIG_HOME="$SANDBOX/config"
export XDG_DATA_HOME="$SANDBOX/data"
# ...and drop WALLETD_TOKEN_PATH, which the XDG sandbox does NOT contain: it beats both the config
# file and `<data_dir>/token` (`wallet-daemon/src/config.rs:90`), so an inherited one would point
# `walletd init`'s token rotation at the operator's REAL bearer token and overwrite it.
unset WALLETD_TOKEN_PATH
DATA_DIR_1="$XDG_DATA_HOME/walletd"             # the funded store — deliberately destroyed
DATA_DIR_2="$XDG_DATA_HOME/walletd-restored"    # the FRESH store — never seeded from store 1
CFG_1="$XDG_CONFIG_HOME/walletd/walletd.toml"
CFG_2="$XDG_CONFIG_HOME/walletd-restored/walletd.toml"
BASE="http://127.0.0.1:$PORT"
DEBUG_DIR="${SMOKE_DEBUG_DIR:-/tmp/recover-gate-debug}"
WALLETD_LOG="$SANDBOX/walletd-recover.log"
WALLETD_PID=""
STATUS=1

# SIGTERM, a BOUNDED wait, then SIGKILL — returns the daemon's exit status (137 if it had to be
# killed). Shutdown aborts the recovery drivers and awaits them (`service/mod.rs:734`), so a driver
# that never reaches an await point would wedge it; an unbounded `wait` here would then hang this
# gate AND the enclosing `dev-fed --exec` forever, with no verdict and no diagnostic. Bash reaps the
# child asynchronously, so `kill -0` goes false as soon as it exits and `wait` still reports the
# recorded status.
stop_walletd() {
  local rc=0 i
  [[ -n "$WALLETD_PID" ]] || return 0
  kill -TERM "$WALLETD_PID" 2>/dev/null || true
  for ((i = 0; i < 150; i++)); do
    kill -0 "$WALLETD_PID" 2>/dev/null || break
    sleep 0.2
  done
  if kill -0 "$WALLETD_PID" 2>/dev/null; then
    echo "walletd ignored SIGTERM for 30s (wedged shutdown) — killing" >&2
    kill -KILL "$WALLETD_PID" 2>/dev/null || true
  fi
  wait "$WALLETD_PID" 2>/dev/null || rc=$?
  WALLETD_PID=""
  return "$rc"
}

# shellcheck disable=SC2329 # invoked indirectly by `trap cleanup EXIT`
cleanup() {
  stop_walletd || true
  if [[ "$STATUS" != "0" ]]; then
    mkdir -p "$DEBUG_DIR"
    # Clear what a PREVIOUS run left first: a failure before walletd starts matches none of the
    # copy patterns below, and a stale walletd log sitting here would be read as this run's.
    rm -f "$DEBUG_DIR"/*.stderr "$DEBUG_DIR"/walletd-*.log
    # The daemon log carries the recovery driver's own errors — the one artifact a failed phase A
    # cannot be diagnosed without. Never the stores, and never the mnemonic (it is held in a shell
    # variable and never written to disk by this script).
    cp -f "$SANDBOX"/*.stderr "$SANDBOX"/walletd-*.log "$DEBUG_DIR/" 2>/dev/null || true
    echo "diagnostics preserved at $DEBUG_DIR" >&2
    echo "--- walletd log tail ---" >&2
    tail -30 "$WALLETD_LOG" >&2 2>/dev/null || true
  fi
  rm -rf "$SANDBOX"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$(dirname "$CFG_1")" "$(dirname "$CFG_2")"
write_host_config() { # $1 = walletd.toml path, $2 = data dir
  cat > "$1" <<EOF
data_dir = "$2"
port = $PORT
gateway = "$GW"
EOF
}
write_host_config "$CFG_1" "$DATA_DIR_1"
write_host_config "$CFG_2" "$DATA_DIR_2"

wsa1() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR_1" --gateway "$GW" "$@"; }
wsa2() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR_2" --gateway "$GW" "$@"; }
# Client mode against the running walletd. Fully explicit (--url + --token-path), so it can never
# read a stale pointer; client mode REJECTS --gateway (walletd's pin is host config, §6a.6).
wcli2() { "$WALLET_CLI" --url "$BASE" --token-path "$DATA_DIR_2/token" "$@"; }
# `balance` prints "<fed hex>: <msat> msat" rows plus a "total (N/M federations): <msat> msat"
# line, identically in both modes. Read the report ONCE (so a failed read is a loud error, not a
# silently empty parse), then pull integers out of it by EXACT field match — never a substring
# grep, which would match 1000000 for 100000.
fed_msat() { awk -v id="$2" '$1 == id ":" && $3 == "msat" { print $2; exit }' <<<"$1"; }
total_msat() { awk '$1 == "total" { print $(NF - 1); exit }' <<<"$1"; }
# The candidate registry is TSV; column 2 is the state tag (`userapproved` for a recovered fed).
candidate_state() { awk -v id="$2" '$1 == id { print $2; exit }' <<<"$1"; }

# ---- 1. fund store 1 --------------------------------------------------------------------------
echo "== store 1: join + fund =="
JOIN_OUT=$(wsa1 join "$FM_INVITE_CODE") || fail "join did not admit"
# Store 1 must be joining FRESH: `already-joined`/`already-in-flight` (`render.rs:89`) would mean
# the sandbox was not empty, and EXPECTED would no longer be the whole pre-loss balance.
case "$JOIN_OUT" in
  started\ *) : ;;
  *) fail "expected 'started <key>' from a fresh join, got '$JOIN_OUT'" ;;
esac
JOIN_KEY=${JOIN_OUT#* }
JOIN_STATE=$(wsa1 await-move "$JOIN_KEY") || fail "join $JOIN_KEY await failed"
[[ "$JOIN_STATE" == "done" ]] || fail "join $JOIN_KEY did not settle: $JOIN_STATE"
FED=$(cut -d: -f2 <<<"$JOIN_KEY")
[[ -n "$FED" ]] || fail "could not read the federation id out of $JOIN_KEY"
echo "joined federation: $FED"

# await-send FIRST: lnv2's internal swap funds OUR incoming contract as part of the SENDER's
# state machine completing, so awaiting the send makes the claim prompt and reliable.
FUND_ERR="$SANDBOX/fund.stderr"
INV=$(wsa1 receive --amount "$FUND_MSAT" 2>"$FUND_ERR") \
  || fail "funding receive failed: $(cat "$FUND_ERR")"
KEY=$(sed -n 's/^key: //p' "$FUND_ERR")
[[ -n "$INV" && -n "$KEY" ]] || fail "receive did not yield an invoice + operation key"
FUND_SEND_ERR="$SANDBOX/fund-send.stderr"
SEND=$(fedimint-cli module lnv2 send "$INV" --gateway "$GW" 2>"$FUND_SEND_ERR" \
  | tr -d '"[:space:]') || fail "devimint funding send failed: $(cat "$FUND_SEND_ERR")"
[[ -n "$SEND" && "$SEND" != "null" ]] || fail "devimint funding send returned no operation id"
fedimint-cli module lnv2 await-send "$SEND" >/dev/null 2>&1 || true
FUND_STATE=$(wsa1 await-receive "$KEY") || fail "funding receive await failed"
[[ "$FUND_STATE" == "claimed" ]] || fail "funding did not claim: $FUND_STATE"

# ---- 2. capture the truth BEFORE the loss -----------------------------------------------------
echo "== capture the pre-loss truth (balance + seed) =="
REPORT_1=$(wsa1 balance) || fail "store-1 balance read failed"
EXPECTED=$(fed_msat "$REPORT_1" "$FED")
[[ "$EXPECTED" =~ ^[0-9]+$ ]] || fail "could not parse store-1 balance for $FED: $REPORT_1"
# A zero here would make the step-6 equality trivially true — the gate must prove a FUNDED wallet
# comes back. The lnv2 receive fee is deducted from the invoiced amount, so expect ~FUND_MSAT.
[[ "$EXPECTED" -gt 0 ]] || fail "store 1 is not funded (${EXPECTED} msat) — nothing to recover"
RECEIVE_FEE_CAP=$(( 50000 + FUND_MSAT / 200 ))   # 50 sat + 0.5% = the lnv2 receive-fee limit
[[ "$EXPECTED" -ge $(( FUND_MSAT - RECEIVE_FEE_CAP )) && "$EXPECTED" -le "$FUND_MSAT" ]] \
  || fail "store-1 funding landed ${EXPECTED} msat, not ~${FUND_MSAT} msat"
echo "store 1 holds ${EXPECTED} msat (funded ${FUND_MSAT})"

# The seed IS the backup unit. Words to stdout, warnings to stderr — capture only the words, and
# never echo them (this is a throwaway regtest wallet, but the habit is the point).
SEED=$("$WALLETD" --config "$CFG_1" mnemonic 2>"$SANDBOX/mnemonic.stderr") \
  || fail "walletd mnemonic failed: $(cat "$SANDBOX/mnemonic.stderr")"
WORDS=$(wc -w <<<"$SEED")
[[ "$WORDS" -eq 12 ]] || fail "walletd mnemonic returned $WORDS words, expected 12"
echo "captured the 12-word seed from store 1"

# ---- 3. genuine store loss --------------------------------------------------------------------
echo "== lose store 1 (rm -rf: no client.db, no journal.db, nothing copied forward) =="
rm -rf "$DATA_DIR_1"
[[ ! -e "$DATA_DIR_1" ]] || fail "store 1 still exists after the simulated loss"

echo "== store 2: a pristine data dir takes the seed import (runbook order) =="
[[ ! -e "$DATA_DIR_2" ]] || fail "store 2 must be pristine before restore-mnemonic ($DATA_DIR_2)"
# `init` → `restore-mnemonic` → start, exactly as the runbook's disk-dies recipe: `init` mints no
# seed, but a daemon START would, and the import then refuses to overwrite it.
"$WALLETD" --config "$CFG_2" init >/dev/null 2>"$SANDBOX/init.stderr" \
  || fail "walletd init on the fresh store failed: $(cat "$SANDBOX/init.stderr")"
printf '%s\n' "$SEED" | "$WALLETD" --config "$CFG_2" restore-mnemonic 2>"$SANDBOX/restore.stderr" \
  || fail "restore-mnemonic failed: $(cat "$SANDBOX/restore.stderr")"
# The import must be the SAME seed store 1 held — otherwise a later balance would prove nothing.
ROUNDTRIP=$("$WALLETD" --config "$CFG_2" mnemonic 2>/dev/null) || fail "store-2 mnemonic read failed"
[[ "$ROUNDTRIP" == "$SEED" ]] || fail "store 2 holds a different seed than the one exported"
echo "store 2 now holds the exported seed (verified word-for-word)"

# ---- 4. bring walletd up on store 2 and prove it is empty (the proof-of-work guard) ------------
echo "== walletd on store 2: prove the daemon's own view is empty before recovery =="
"$WALLETD" --config "$CFG_2" > "$WALLETD_LOG" 2>&1 &
WALLETD_PID=$!
healthy=""
for _ in $(seq 1 150); do
  if wcli2 health >/dev/null 2>&1; then healthy=1; break; fi
  kill -0 "$WALLETD_PID" 2>/dev/null || fail "walletd died at startup (log: $WALLETD_LOG)"
  sleep 0.2
done
[[ -n "$healthy" ]] || fail "walletd never became healthy on $BASE"

FEDS_2=$(wcli2 list-feds) || fail "store-2 list-feds failed"
[[ -z "$FEDS_2" ]] || fail "store 2 already has federations: $FEDS_2"
REPORT_2=$(wcli2 balance) || fail "store-2 balance read failed"
PRE_FED=$(fed_msat "$REPORT_2" "$FED")
[[ -z "$PRE_FED" ]] || fail "store 2 already reports a balance row for $FED: $PRE_FED msat"
PRE_TOTAL=$(total_msat "$REPORT_2")
[[ "$PRE_TOTAL" =~ ^[0-9]+$ ]] || fail "could not parse the store-2 total: $REPORT_2"
[[ "$PRE_TOTAL" -eq 0 ]] || fail "store 2 is not empty (total ${PRE_TOTAL} msat)"
echo "store 2: 0 federations, total 0 msat — any balance after this comes from recovery alone"

# ---- 5. phase A: recover under the daemon (the runbook's disk-dies path) -----------------------
echo "== phase A: recover $FED from the seed, under walletd =="
REC_ERR="$SANDBOX/recover-daemon.stderr"
REC_OUT=$(wcli2 recover "$FM_INVITE_CODE" 2>"$REC_ERR") \
  || fail "recover did not admit: $(cat "$REC_ERR")"
case "$REC_OUT" in
  started\ *) : ;;   # a fresh recovery, as expected on an empty store
  *) fail "expected 'started <key>' from recover, got '$REC_OUT'" ;;
esac
REC_KEY=${REC_OUT#* }
echo "recovery operation: $REC_KEY"
# The daemon keeps the detached D5 driver alive for the whole replay; this is the long-poll await
# (`GET /v1/operations/{key}?wait=true`) the runbook tells an operator to run.
if ! REC_STATE=$(wcli2 await-move "$REC_KEY" --timeout "$RECOVER_TIMEOUT" 2>"$SANDBOX/await-daemon.stderr"); then
  fail "recovery did not reach a successful terminal state (got '${REC_STATE:-<none>}'): $(cat "$SANDBOX/await-daemon.stderr")"
fi
[[ "$REC_STATE" == "done" ]] || fail "recovery terminal state is '$REC_STATE', expected 'done'"
echo "recovery settled under the daemon: done"

# ---- 6. the assertion this gate exists for: EXACT restoration ---------------------------------
REPORT_3=$(wcli2 balance) || fail "post-recovery balance read failed"
RESTORED=$(fed_msat "$REPORT_3" "$FED")
[[ "$RESTORED" =~ ^[0-9]+$ ]] || fail "could not parse the recovered balance for $FED: $REPORT_3"
# ZERO slack, by design: recovery replays the ecash the wallet held and charges no fee, so a
# difference in either direction is a real defect. Never loosen this to force a green.
[[ "$RESTORED" -eq "$EXPECTED" ]] \
  || fail "recovery restored ${RESTORED} msat, expected EXACTLY ${EXPECTED} msat"
echo "phase A restored EXACTLY ${RESTORED} msat from the seed (pre-loss ${EXPECTED} msat)"

# D4.6: `complete_recovery` records UserApproved in the SAME transaction as the registry row.
# Without it the allocator treats a recovered fed as agent-discovered and never spends from it —
# the funds would come back but stay unmanaged.
CAND_A=$(wcli2 candidates) || fail "store-2 candidates read failed"
OWNERSHIP_A=$(candidate_state "$CAND_A" "$FED")
[[ "$OWNERSHIP_A" == "userapproved" ]] \
  || fail "the daemon-recovered fed is '${OWNERSHIP_A:-<absent>}', expected 'userapproved'"
echo "phase A: the recovered fed is a userapproved candidate"

WALLETD_RC=0; stop_walletd || WALLETD_RC=$?
[[ "$WALLETD_RC" == "0" ]] || fail "walletd exited $WALLETD_RC on shutdown (137 = it was killed)"
echo "walletd stopped cleanly"

# ---- 7. phase B: journal-only loss, recovered standalone over the orphaned partition -----------
echo "== phase B: delete ONLY journal.db (client.db — seed + phase-A partition — survives) =="
rm -rf "$DATA_DIR_2/journal.db"
[[ ! -e "$DATA_DIR_2/journal.db" ]] || fail "journal.db survived the phase-B loss"
[[ -e "$DATA_DIR_2/client.db" ]] || fail "phase B must keep client.db (it holds the seed)"
# With no registry row the federation is unregistered again, so D3 permits recovery. `client.db`
# still contains phase A's orphaned partition; this phase proves recovery completes in that live
# shape, while fresh-prefix selection and preservation remain unit-covered.
FEDS_B=$(wsa2 list-feds) || fail "phase-B list-feds failed"
[[ -z "$FEDS_B" ]] || fail "phase B still sees federations after the journal loss: $FEDS_B"
REPORT_B0=$(wsa2 balance) || fail "phase-B pre-recovery balance read failed"
B0_TOTAL=$(total_msat "$REPORT_B0")
[[ "$B0_TOTAL" =~ ^[0-9]+$ ]] || fail "could not parse the phase-B pre-recovery total: $REPORT_B0"
[[ "$B0_TOTAL" -eq 0 ]] || fail "phase B is not journal-empty (total ${B0_TOTAL} msat)"

REC_B_ERR="$SANDBOX/recover-standalone.stderr"
REC_B_OUT=$(wsa2 recover "$FM_INVITE_CODE" 2>"$REC_B_ERR") \
  || fail "phase-B recover did not admit: $(cat "$REC_B_ERR")"
case "$REC_B_OUT" in
  started\ *) : ;;
  *) fail "expected 'started <key>' from the phase-B recover, got '$REC_B_OUT'" ;;
esac
REC_B_KEY=${REC_B_OUT#* }
echo "phase-B recovery operation: $REC_B_KEY"
# Standalone aborted its driver at command shutdown, so THIS call does the replay: await-move
# reconciles first (abandon-and-resume, §6a.8), which re-drives the pending recovery intent.
if ! REC_B_STATE=$(wsa2 await-move "$REC_B_KEY" --timeout "$RECOVER_TIMEOUT" 2>"$SANDBOX/await-standalone.stderr"); then
  fail "phase-B recovery did not reach a successful terminal state (got '${REC_B_STATE:-<none>}'): $(cat "$SANDBOX/await-standalone.stderr")"
fi
[[ "$REC_B_STATE" == "done" ]] || fail "phase-B recovery terminal state is '$REC_B_STATE', expected 'done'"

REPORT_B1=$(wsa2 balance) || fail "phase-B post-recovery balance read failed"
RESTORED_B=$(fed_msat "$REPORT_B1" "$FED")
[[ "$RESTORED_B" =~ ^[0-9]+$ ]] || fail "could not parse the phase-B balance for $FED: $REPORT_B1"
# Nothing was spent between the two phases, so the SAME pre-loss number must come back — again
# with zero slack, and again from a store that reported 0 msat moments earlier.
[[ "$RESTORED_B" -eq "$EXPECTED" ]] \
  || fail "phase-B recovery restored ${RESTORED_B} msat, expected EXACTLY ${EXPECTED} msat"
echo "phase B restored EXACTLY ${RESTORED_B} msat over the orphaned partition"

# ---- 8. the recovered federation is user-owned and spendable ----------------------------------
echo "== the recovered federation is user-owned and spendable =="
CAND_B=$(wsa2 candidates) || fail "phase-B candidates read failed"
OWNERSHIP_B=$(candidate_state "$CAND_B" "$FED")
[[ "$OWNERSHIP_B" == "userapproved" ]] \
  || fail "the phase-B recovered fed is '${OWNERSHIP_B:-<absent>}', expected 'userapproved'"

SPEND_RECV_ERR="$SANDBOX/spend-receive.stderr"
RECV_JSON=$(fedimint-cli module lnv2 receive "$SPEND_MSAT" --gateway "$GW" \
  2>"$SPEND_RECV_ERR") || fail "devimint invoice creation failed: $(cat "$SPEND_RECV_ERR")"
INV_B=$(jq -er '.[0] | select(type == "string" and length > 0)' <<<"$RECV_JSON") \
  || fail "devimint did not return a payable invoice: $RECV_JSON"
OP_B_DEV=$(jq -er '.[1] | select(type == "string" and length > 0)' <<<"$RECV_JSON") \
  || fail "devimint did not return an invoice operation id: $RECV_JSON"
PAY_OUT=$(wsa2 pay "$INV_B" --fed "$FED") || fail "paying from the recovered wallet did not admit"
case "$PAY_OUT" in
  started\ *) : ;;
  *) fail "expected 'started <key>' from pay, got '$PAY_OUT'" ;;
esac
SEND_STATE=$(wsa2 await-send "${PAY_OUT#* }") || fail "the recovered wallet's send did not settle"
[[ "$SEND_STATE" == "success" ]] || fail "expected send 'success', got '$SEND_STATE'"
SPEND_AWAIT_ERR="$SANDBOX/spend-await-receive.stderr"
DEV_RECV=$(fedimint-cli module lnv2 await-receive "$OP_B_DEV" 2>"$SPEND_AWAIT_ERR" \
  | tr -d '"[:space:]') || fail "devimint invoice await failed: $(cat "$SPEND_AWAIT_ERR")"
[[ "$DEV_RECV" == "Claimed" ]] || fail "devimint did not claim the recovered wallet's payment: $DEV_RECV"
echo "recovered wallet is userapproved and spent ${SPEND_MSAT} msat over lnv2 (devimint claimed it)"

STATUS=0
echo "RECOVER_GATE_EXIT=0"
echo "PASS: seed-only recovery restored EXACTLY ${EXPECTED} msat for federation ${FED} — under walletd after a whole-store loss, and standalone after a journal-only loss over the orphaned partition (zero slack both times); the recovered fed is user-owned and spendable"
exit 0
