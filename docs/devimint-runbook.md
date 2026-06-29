# devimint runbook — build, run, and drive the money path

Operational notes from standing up fedimint + devimint and validating the cross-fed move
(2026-06-29). This is the HOW-TO that backs the model in
[fedimint-mechanics.md](./fedimint-mechanics.md) and the Phase 1 harness (TODOS T4).
Source tree: `~/p/fedimint` (branch `docs/custodial-receive-spec`, workspace 0.12-alpha).

## 1. Build (once)
```bash
cd ~/p/fedimint
nix develop -c cargo build --workspace --bins      # binaries -> target-nix/debug/
```
- The nix devshell provides the external daemons: **bitcoind 31.0, lnd 0.19.3, esplora,
  lncli**, and the toolchain (cargo 1.93). esplora is NOT on the system PATH — you MUST be
  in `nix develop`.
- `CARGO_BUILD_TARGET_DIR=~/p/fedimint/target-nix`; `add_target_dir_to_path` (from
  `scripts/_common.sh`) puts `target-nix/debug` on PATH via `$CARGO_BUILD_TARGET_BIN_DIR`.
- The **cachix cache is unavailable** ("not a trusted user"), so deps compile from source
  (cold build is long; a warm rebuild of just the workspace was ~4m17s).
- Built binaries: `devimint, fedimintd, gatewayd, gateway-cli, fedimint-cli` (0.12-alpha).
  Don't mix with the prebuilt `~/bin` binaries (those are 0.11.1).

## 2. Run a dev federation + drive it
```bash
cd ~/p/fedimint
nix develop -c bash -c '
  set -euo pipefail
  source scripts/_common.sh
  add_target_dir_to_path
  export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"   # the alias wrappers
  export RUST_LOG=warn
  export FM_ENABLE_MODULE_LNV2=1                             # ensure lnv2 + LDK gateway
  devimint --link-test-dir "${CARGO_BUILD_TARGET_DIR:-target}/devimint" \
    --num-feds 1 dev-fed --exec bash /path/to/your-script.sh
'
```
- `dev-fed` spins up bitcoind + esplora + LND node + LDK node + LND/LDK gateways + a
  4-guardian federation (DKG), opens a channel, pegs in a client (~1M sats), then runs
  `--exec <cmd>` with the env set and **tears down after** (one-shot). For a long-running
  fed, drop `--exec` (it holds until shutdown) and use `devimint rpc env` / `rpc wait` from
  another shell.
- `--num-feds N` (CommonArgs, before the subcommand) = number of federations. `-n`/`--fed-size`
  = guardians per fed (default 4).
- Bring-up takes ~1-3 min.

### Env available inside `--exec`
- `FM_INVITE_CODE` — fed-0's invite. `FM_DATA_DIR`, `FM_CLIENT_DIR` (=`$FM_DATA_DIR/clients/default-0`).
- `FM_PORT_GW_LDK`, `FM_PORT_GW_LND` — gateway API ports. `FM_BTC_CLIENT` (bitcoin-cli wrapper).
- Alias wrappers on PATH: `fm-cli`, `gateway-ldk`, `gateway-lnd`, `bitcoin-cli`, `lncli`.
- The funded internal client is `clients/default-0` (joined to fed-0).

## 3. fedimint-cli cheatsheet (the WORKING forms)
Capture **stdout only** (`2>/dev/null`) — deprecated commands print a `WARN` to stderr that
corrupts JSON parsing. Amounts are **msat** (e.g. `200000` = 200 sat).
```bash
fedimint-cli info | jq .total_amount_msat                       # balance (one number/fed)
# lnv1
fedimint-cli ln-invoice --amount 200000 2>/dev/null             # -> {invoice, operation_id}
fedimint-cli module ln pay <invoice> --force-internal 2>/dev/null  # -> {"Success":{"preimage":..}}
fedimint-cli module ln list-gateways                            # lnv1 gateways
# lnv2 (MUST pass --gateway explicitly; see gotcha below)
GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
fedimint-cli module lnv2 receive 200000 --gateway "$GW"         # -> [invoice, op_id]
fedimint-cli module lnv2 send <invoice> --gateway "$GW"         # -> "<op_id>"
fedimint-cli module lnv2 await-send <op_id>                     # -> {"Success":"<preimage>"}
fedimint-cli module lnv2 await-receive <op_id>                  # -> "Claimed"
```

## 4. Gotchas (each cost a bring-up to learn)
- **lnv2 gateway list is empty by default.** The LDK gateway connects to the fed
  (`fed_count: 1`) but devimint does NOT auto-register it into the federation's vetted lnv2
  `gateways list`. So `module lnv2 receive/send` with auto-select fail with "No gateways are
  available". **Fix: pass `--gateway "http://127.0.0.1:$FM_PORT_GW_LDK/"` explicitly** — the
  client uses it directly (this is what devimint's own tests do: `lnv2_send(&c, &gw.address(), inv)`).
- **Deprecated top-level `ln-invoice`/`ln-pay` warn on stderr** ("Use `module ln ...`"); the
  JSON is on stdout. Use `2>/dev/null`. Note `module ln invoice` has DIFFERENT (positional)
  syntax than `ln-invoice --amount`.
- **`supports_lnv2()` is true by DEFAULT** (unset env → enabled); set `FM_ENABLE_MODULE_LNV2=1`
  to be explicit. (`devimint/src/util.rs:982`.)
- **`--num-feds 2` makes two federations but only auto-joins `default-0` to fed-0.** Fed-1's
  invite is not auto-exported and no client is joined to it; for a real two-fed test you must
  derive fed-1's invite and `fedimint-cli join` a second client + peg it in. (Single-fed
  self receive→send through one gateway already exercises `is_direct_swap`.)
- Don't `2>&1` into a var you `jq`; route logs elsewhere.

## 5. What was validated (see fedimint-mechanics.md "Live validation")
receive non-idempotency; lnv1 internal pay + dedup; **lnv2 `is_direct_swap`** (await-send
`Success`+preimage, await-receive `Claimed`, fee-only balance change) and **lnv2 dedup**
(re-send → `"This invoice has already been paid"`, no second debit). Validation scripts are
in the session scratchpad (`tv3.sh`, `lnv2swap.sh`).

## 6. For the Phase 1 harness (T4)
- Bootstrap the fed **once per test session/CI job** (the bring-up is the cost), then run
  many tests against it; per test use fresh client DBs + amounts. devimint dev-fed is the
  one-fsync-domain fixture.
- Drive via `fedimint-cli` (above) or the client lib directly. For two-fed cross moves,
  script the fed-1 join + a shared gateway connected to both.
- Crash-resume test: kill the client/process mid-operation, reopen the client, assert the
  operation completes (the executor self-resumes) and balances are exactly-once.
