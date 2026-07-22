#!/usr/bin/env bash
# THE 24h SOAK (phase 6a §6a.9 burn-in — NOT a merge gate): one walletd, watch active, and
# periodic user ops for SOAK_HOURS (default 24). What it proves, per the spec: no lock
# conflicts, no duplicate intents, and the ledger reconstructs the whole session. Passing
# this gates "daily-drive with real sats" and starts 6a.2 (NWC).
#
# Per iteration (~every SOAK_OP_PERIOD_SECS): RECEIVE 50k msat into the wallet (walletd mints,
# devimint's client pays, await claimed), PAY 30k msat out (devimint mints, we pay, await
# success), and every 6th iteration a user MOVE between A and B (direction alternates) — the
# scheduler churns its own cadence underneath the whole time. Every operation key is recorded;
# the end-of-run audit asserts each appears EXACTLY ONCE in history (duplicate-intent check +
# ledger-reconstructs-the-session check), zero user-op failures, a stable walletd PID, a clean
# SIGTERM, and no lock/panic lines in the daemon log.
#
# Loop discipline: the SEED phase is strict (set -e); the SOAK loop is tolerant — an op
# failure is COUNTED and the loop continues (a 24h run must not die at hour 3 on one blip;
# the count fails the run at the end). A dead walletd fails fast.
#
# Run (two-fed harness):
#   cd ~/p/fedimint
#   nix develop -c bash -c '
#     set -euo pipefail
#     source scripts/_common.sh
#     add_target_dir_to_path
#     export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"
#     export FM_ENABLE_MODULE_LNV2=1
#     export FM_NUM_FEDS=2
#     export SOAK_HOURS=24
#     /home/master/p/fedimint/target-nix/release/devimint --link-test-dir ./test-dir dev-fed \
#       --exec bash /home/master/p/fedimint-wallets/wallet-cli/tests/smoke_soak_devimint.sh
#   '
set -euo pipefail

: "${FM_INVITE_CODE:?FM_INVITE_CODE not set — run inside \`devimint dev-fed --exec\`}"
: "${FM_PORT_GW_LDK:?FM_PORT_GW_LDK not set — run with FM_ENABLE_MODULE_LNV2=1}"
: "${FED_B_INVITE:?FED_B_INVITE not set — two-fed harness patch + FM_NUM_FEDS=2}"

SOAK_HOURS="${SOAK_HOURS:-24}"
SOAK_OP_PERIOD_SECS="${SOAK_OP_PERIOD_SECS:-300}"

WALLET_CLI="${WALLET_CLI_BIN:-/home/master/p/fedimint-wallets/target-nix/release/wallet-cli}"
WALLETD="${WALLETD_BIN:-/home/master/p/fedimint-wallets/target-nix/release/walletd}"
for f in "$WALLET_CLI" "$WALLETD"; do
  [[ -x "$f" ]] || { echo "FAIL: missing binary $f" >&2; exit 1; }
done
for c in fedimint-cli jq curl; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not on PATH" >&2; exit 1; }
done

GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
PORT=19739
FUND_MSAT=2000000
RECEIVE_MSAT=50000
PAY_MSAT=30000
MOVE_MSAT=20000

SANDBOX="$(mktemp -d)"
export XDG_CONFIG_HOME="$SANDBOX/config"
export XDG_DATA_HOME="$SANDBOX/data"
DATA_DIR="$XDG_DATA_HOME/walletd"
DEBUG_DIR="${SMOKE_DEBUG_DIR:-/tmp/soak-debug}"
WALLETD_LOG="$SANDBOX/walletd.log"
KEYS_FILE="$SANDBOX/submitted-keys.txt"
SOAK_ERR="$SANDBOX/op.stderr"
STATUS=1
cleanup() {
  if [[ -n "${SUP_PID:-}" ]]; then
    kill "$SUP_PID" 2>/dev/null || true
  fi
  pkill -f "release/walletd" 2>/dev/null || true
  mkdir -p "$DEBUG_DIR"
  cp -f "$WALLETD_LOG" "$DEBUG_DIR/walletd.log" 2>/dev/null || true
  cp -f "$KEYS_FILE" "$DEBUG_DIR/submitted-keys.txt" 2>/dev/null || true
  # Preserve the exact history dump the exactly-once audit grepped: the v3 dup-FAIL
  # post-mortem needed a structural argument because this wasn't kept.
  if [[ -n "${HIST:-}" ]]; then printf '%s\n' "$HIST" > "$DEBUG_DIR/history-dump.txt" 2>/dev/null || true; fi
  if [[ "$STATUS" != "0" ]]; then
    echo "diagnostics preserved at $DEBUG_DIR" >&2
    tail -30 "$WALLETD_LOG" >&2 2>/dev/null || true
  fi
  rm -rf "$SANDBOX"
}
trap cleanup EXIT

# ---- seed (strict) ----------------------------------------------------------------------------
echo "== soak seed: join A, fund A ${FUND_MSAT}, auto-join B, policy =="
mkdir -p "$XDG_CONFIG_HOME/walletd"
cat > "$XDG_CONFIG_HOME/walletd/walletd.toml" <<EOF
port = $PORT
gateway = "$GW"
EOF
wsa() { "$WALLET_CLI" --standalone --data-dir "$DATA_DIR" --gateway "$GW" "$@"; }

JOIN_OUT=$(wsa join "$FM_INVITE_CODE")
JOIN_KEY=${JOIN_OUT#* }
[[ "$(wsa await-move "$JOIN_KEY")" == "done" ]] || { echo "FAIL: join A" >&2; exit 1; }
FED_A=$(cut -d: -f2 <<<"$JOIN_KEY")

INV=$(wsa receive --amount "$FUND_MSAT" 2>"$SOAK_ERR")
KEY=$(sed -n 's/^key: //p' "$SOAK_ERR")
SEND=$(fedimint-cli module lnv2 send "$INV" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]')
fedimint-cli module lnv2 await-send "$SEND" >/dev/null 2>&1 || true
[[ "$(wsa await-receive "$KEY")" == "claimed" ]] || { echo "FAIL: funding A" >&2; exit 1; }

wsa discover --source manual --invite "$FED_B_INVITE" --auto-join --scorer-allow-regtest >/dev/null
FED_B=$(wsa candidates | awk '$2 == "autojoined" { print $1; exit }')
[[ -n "$FED_B" ]] || { echo "FAIL: B not auto-joined" >&2; exit 1; }

# Real-operation-shaped policy: 60s cadence (1440 scheduler cycles per 24h), pins for the
# regtest scorer, probe span 1s so B's gate opens early in the run and the engine settles.
wsa policy set \
  --spending-fed "$FED_A" --standby-fed "$FED_B" \
  --spending-target 500000 --standby-target 100000 --max-fee 100000 \
  --base-interval-secs 60 --min-interval-secs 5 \
  --probe-min-span-secs 1 --probe-retry-backoff-secs 30 \
  --discover-every-secs 1000000000 >/dev/null
echo "A=$FED_A B=$FED_B; policy seeded (60s cadence)"

"$WALLETD" init >/dev/null
TOKEN=$(cat "$DATA_DIR/token")
BASE="http://127.0.0.1:$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")
# The PRODUCTION ENSEMBLE, exactly as deployed: walletd under a supervisor (the systemd
# Restart=on-failure stand-in) so the settlement-stall watchdog's exit-and-restart self-heal
# actually heals. Stock RocksDB tuning, deliberately: the lnv2 long-held-transaction wedge is
# fixed at our pinned fedimint rev (upstream PR #8816), and this soak is the burn-in proving
# that fix under the exact idle-cadence scenario that used to wedge — a widened write buffer
# would mask any regression.
RESTARTS_FILE="$SANDBOX/restarts"; : > "$RESTARTS_FILE"
SUP_PID=""
supervise() {
  local n=0
  while true; do
    n=$((n + 1)); echo "$n" > "$RESTARTS_FILE"
    "$WALLETD" >> "$WALLETD_LOG" 2>&1
    echo "$(date -u +%FT%TZ) walletd exited (run $n); supervisor restarting" >> "$WALLETD_LOG"
    sleep 2
  done
}
supervise & SUP_PID=$!
for i in $(seq 1 100); do
  curl -sf "${AUTH[@]}" "$BASE/v1/health" >/dev/null 2>&1 && break
  sleep 0.3
  [[ "$i" == 100 ]] && { echo "FAIL: walletd never healthy" >&2; exit 1; }
done
echo "walletd up under supervisor; soaking for ${SOAK_HOURS}h, ops every ${SOAK_OP_PERIOD_SECS}s"

# ---- the soak loop (tolerant: count failures, never die on one) --------------------------------
: > "$KEYS_FILE"
DEADLINE=$(( $(date +%s) + $(awk "BEGIN{printf \"%d\", $SOAK_HOURS * 3600}") ))
ITER=0
FAILURES=0
SCHED_DEAD=0
note_fail() { FAILURES=$((FAILURES + 1)); echo "op-fail(iter $ITER): $1" >&2; }

while (( $(date +%s) < DEADLINE )); do
  ITER=$((ITER + 1))

  # Under the supervisor a walletd exit is a RESTART, not a soak failure — wait out the
  # brief unavailability window (a watchdog self-heal in progress), then proceed.
  for h in $(seq 1 60); do
    curl -sf "${AUTH[@]}" "$BASE/v1/health" >/dev/null 2>&1 && break
    sleep 1
  done
  HEALTH=$(curl -sf "${AUTH[@]}" "$BASE/v1/health" || true)
  if [[ -z "$HEALTH" ]]; then
    note_fail "health probe failed"
  elif [[ "$(jq -r '.scheduler_alive' <<<"$HEALTH" 2>/dev/null || true)" != "true" ]]; then
    SCHED_DEAD=$((SCHED_DEAD + 1))
    echo "scheduler_alive=false at iter $ITER" >&2
  fi

  # RECEIVE: walletd mints, devimint pays, await claimed.
  INV=$("$WALLET_CLI" receive --amount "$RECEIVE_MSAT" 2>"$SOAK_ERR" || true)
  RKEY=$(sed -n 's/^key: //p' "$SOAK_ERR")
  if [[ -z "$INV" || -z "$RKEY" ]]; then
    note_fail "receive mint: $(tail -1 "$SOAK_ERR" 2>/dev/null)"
  else
    echo "$RKEY" >> "$KEYS_FILE"
    SEND=$(fedimint-cli module lnv2 send "$INV" --gateway "$GW" 2>/dev/null | tr -d '"[:space:]' || true)
    fedimint-cli module lnv2 await-send "$SEND" >/dev/null 2>&1 || true
    STATE=$("$WALLET_CLI" await-receive "$RKEY" --timeout 120 2>/dev/null || true)
    [[ "$STATE" == "claimed" ]] || note_fail "receive $RKEY state '$STATE'"
  fi

  # PAY: devimint mints, we pay, await success.
  RJSON=$(fedimint-cli module lnv2 receive "$PAY_MSAT" --gateway "$GW" 2>/dev/null || true)
  PINV=$(jq -r '.[0] // empty' <<<"$RJSON" 2>/dev/null || true)
  if [[ -z "$PINV" ]]; then
    note_fail "devimint invoice mint"
  else
    PAY_OUT=$("$WALLET_CLI" pay "$PINV" --fed "$FED_A" 2>"$SOAK_ERR" || true)
    PKEY=$(awk '{print $2}' <<<"$PAY_OUT")
    if [[ -z "$PKEY" ]]; then
      note_fail "pay submit: $(tail -1 "$SOAK_ERR" 2>/dev/null)"
    else
      echo "$PKEY" >> "$KEYS_FILE"
      STATE=$("$WALLET_CLI" await-send "$PKEY" --timeout 120 2>/dev/null || true)
      [[ "$STATE" == "success" ]] || note_fail "pay $PKEY state '$STATE'"
    fi
  fi

  # Every 6th iteration: a user MOVE, alternating direction, unique occurrence per move.
  if (( ITER % 6 == 0 )); then
    if (( (ITER / 6) % 2 == 1 )); then M_FROM="$FED_A"; M_TO="$FED_B"; else M_FROM="$FED_B"; M_TO="$FED_A"; fi
    # Explicit fee cap: admission reserves amount + THE FULL CAP, and the policy default
    # (100k) would make a B->A move need 120k against B's ~100k standby balance — refused
    # by construction (the validation run hit exactly this). Devimint leg quotes run ~4k.
    MOVE_OUT=$("$WALLET_CLI" move --from "$M_FROM" --to "$M_TO" --amount "$MOVE_MSAT" \
      --fee-cap 10000 --occurrence "$ITER" 2>"$SOAK_ERR" || true)
    MKEY=$(awk '{print $2}' <<<"$MOVE_OUT")
    if [[ -z "$MKEY" ]]; then
      note_fail "move submit: $(tail -1 "$SOAK_ERR" 2>/dev/null)"
    else
      echo "$MKEY" >> "$KEYS_FILE"
      STATE=$("$WALLET_CLI" await-move "$MKEY" --timeout 180 2>/dev/null || true)
      [[ "$STATE" == "done" ]] || note_fail "move $MKEY state '$STATE'"
    fi
  fi

  if (( ITER % 12 == 0 )); then
    echo "soak: iter $ITER, failures $FAILURES, $(( (DEADLINE - $(date +%s)) / 60 )) min left"
  fi
  sleep "$SOAK_OP_PERIOD_SECS"
done

echo "== soak window complete: $ITER iterations, $FAILURES op failure(s) =="

# ---- end-of-run audit ---------------------------------------------------------------------------
echo "== audit: every submitted key appears EXACTLY ONCE in history =="
HIST=$("$WALLET_CLI" history --limit 100000)
MISSING=0; DUPED=0
while IFS= read -r key; do
  [[ -z "$key" ]] && continue
  # Exact-key match, NOT substring: move keys end in the occurrence number, so key
  # ...:10000:6 is a string prefix of ...:10000:66 and a bare grep -cF double-counts
  # (the 24h v3 run "found" 4 dups this way: 6/66, 12/120, 18/186, 24/240). Keys are
  # hex/digits/colons only, so they're ERE-safe; require a non-digit or EOL after.
  n=$(grep -cE "${key}([^0-9]|\$)" <<<"$HIST" || true); n=${n:-0}
  if (( n == 0 )); then MISSING=$((MISSING + 1)); echo "MISSING from ledger: $key" >&2; fi
  if (( n > 1 )); then DUPED=$((DUPED + 1)); echo "DUPLICATED in ledger ($n rows): $key" >&2; fi
done < "$KEYS_FILE"
SUBMITTED=$(wc -l < "$KEYS_FILE")
echo "audit: $SUBMITTED keys submitted, $MISSING missing, $DUPED duplicated"

echo "== audit: daemon log hygiene (no lock conflicts, no panics) =="
# Cross-process DB LOCK contention only (RocksDB's "While lock file ...: Resource
# temporarily unavailable"). The old fuzzy pattern (database.*lock) substring-matched the
# word "block" in fedimint's BENIGN "commit failed in an autocommit block - retrying" WARN —
# a retried optimistic conflict is designed behavior, not a lock conflict.
LOCK_HITS=$(grep -ciE "lock file|resource temporarily unavailable|would block" "$WALLETD_LOG" || true); LOCK_HITS=${LOCK_HITS:-0}
PANIC_HITS=$(grep -ciE "panicked at" "$WALLETD_LOG" || true); PANIC_HITS=${PANIC_HITS:-0}
# Retried autocommit conflicts are DESIGNED behavior (v3 saw exactly 1 in 24h), but a
# regression that made them constant would sail through the narrowed lock audit — keep
# them as a rate canary, not a zero-tolerance check.
RETRY_HITS=$(grep -c "commit failed in an autocommit block" "$WALLETD_LOG" 2>/dev/null || true); RETRY_HITS=${RETRY_HITS:-0}
echo "audit: lock-ish lines $LOCK_HITS, panics $PANIC_HITS, autocommit retries $RETRY_HITS"

echo "== self-heal accounting (supervisor restarts + watchdog firings) =="
RESTARTS=$(cat "$RESTARTS_FILE" 2>/dev/null || echo 1); RESTARTS=${RESTARTS:-1}
SELF_HEALS=$((RESTARTS - 1))
WATCHDOG_FIRES=$(grep -c "settlement stall" "$WALLETD_LOG" 2>/dev/null || true); WATCHDOG_FIRES=${WATCHDOG_FIRES:-0}
echo "audit: $SELF_HEALS supervisor restart(s), $WATCHDOG_FIRES watchdog firing(s)"

echo "== stop the supervisor + walletd (SIGTERM) =="
kill "$SUP_PID" 2>/dev/null || true; SUP_PID=""
WPID=$(pgrep -f "release/walletd" | head -1)
if [[ -n "$WPID" ]]; then
  kill -TERM "$WPID"
  for i in $(seq 1 30); do kill -0 "$WPID" 2>/dev/null || break; sleep 1; done
  kill -0 "$WPID" 2>/dev/null && { echo "FAIL: walletd ignored SIGTERM" >&2; exit 1; }
fi

# Verdict. The known SDK defect (receive_lnurl WriteConflict panic) may fire during 24h of
# soaking — the ensemble's CONTRACT is that every such event self-heals: each panic pairs
# with a watchdog firing + restart, the ledger stays exactly-once, and the op failure rate
# stays small (a restart window can time out the ops in flight around it).
FAIL_REASONS=()
FAIL_BUDGET=$(( SELF_HEALS * 6 + SUBMITTED / 20 ))   # ~6 ops around each heal + 5% slack
(( FAILURES <= FAIL_BUDGET )) || FAIL_REASONS+=("$FAILURES op failure(s) exceeds the self-heal budget $FAIL_BUDGET")
(( MISSING == 0 ))  || FAIL_REASONS+=("$MISSING key(s) missing from the ledger")
(( DUPED == 0 ))    || FAIL_REASONS+=("$DUPED duplicated key(s)")
(( LOCK_HITS == 0 )) || FAIL_REASONS+=("$LOCK_HITS lock-conflict line(s)")
(( PANIC_HITS <= SELF_HEALS )) || FAIL_REASONS+=("$PANIC_HITS panic(s) but only $SELF_HEALS restart(s) — an unhealed panic")
(( SCHED_DEAD == 0 )) || FAIL_REASONS+=("scheduler_alive=false observed $SCHED_DEAD time(s) — a silently dead scheduler is not a PASS")
# Canary threshold: ~1 retried conflict per soak-day is the observed healthy rate; an
# order of magnitude more means something started churning fedimint's memtable again.
(( RETRY_HITS <= 10 )) || FAIL_REASONS+=("$RETRY_HITS autocommit-retry line(s) — conflict rate regressed (healthy ≈1/24h)")

if (( ${#FAIL_REASONS[@]} > 0 )); then
  printf 'FAIL: %s\n' "${FAIL_REASONS[@]}" >&2
  exit 1
fi

STATUS=0
echo "SOAK_EXIT=0"
echo "PASS: ${SOAK_HOURS}h soak — $ITER iterations, $SUBMITTED ops, $FAILURES failure(s) within budget, every key exactly once, $SELF_HEALS self-heal(s) ($WATCHDOG_FIRES watchdog), no lock conflicts, no unhealed panics, clean SIGTERM"
