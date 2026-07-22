//! `wallet-cli` — the first-class, permanent headless frontend over the wallet engine
//! (ADR-0023). Thin: all logic lives in `wallet-fedimint`/`wallet-core`; this crate only
//! parses arguments, drives the engine, and formats output. No interactive prompts (the
//! engine assumes no UI).

mod client;
mod exit;
mod render;

use crate::client::WalletdClient;
use crate::exit::CliExit;
use crate::render::AwaitVerb;
use clap::{Args, Parser, Subcommand, ValueEnum};
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::Client;
use fedimint_core::db::Database;
use fedimint_core::invite_code::InviteCode;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use wallet_api::{AwaitTarget, OperationStatusDto, Policy};
use wallet_core::{
    Action, ActiveProbeVerdict, Actor, AllocatorDecision, DiscoveryPolicy, DiscoverySource,
    ExecutionSummary, FederationId, IdempotencyKey, IntentStatus, Journal, Msat, Occurrence,
    OperationKind, OperationRecord, OperationStatus, ProbePolicy, ReasonCode, RefusalDiagnostics,
};
use wallet_fedimint::{
    direct_inflow_nonce_key, join_intent_key, move_key, parse_invoice, raw_pay_key,
    raw_receive_key, AutoJoinReport, AwaitOutcome, CandidateSource, CandidateState, DiscoverReport,
    DiscoverSourceReport, FederationInfo, FedimintJournal, GatewayUrl, Invoice, ManualSource,
    MultiClient, ObserverSource, OpRequest, OperationId, OperationRef, ProbeOutcome, Runtime,
    ScoredFed, ServiceError, Snapshot, SnapshotScope, TickPolicy, WalletClient, WalletService,
};

#[derive(Parser)]
#[command(name = "wallet-cli", about = "Headless multi-federation ecash wallet")]
struct Cli {
    /// Talk to the wallet store DIRECTLY (spec §6a.7): take the exclusive `db.lock` and spin up
    /// the same in-process actor + drivers `walletd` runs, run the one command, shut down. The
    /// DEFAULT is client mode — every operational verb is an HTTP call to a running `walletd`.
    /// `--standalone` is a deliberate flag: a silent fallback would block a supervisor-restarting
    /// daemon behind a lock race the user did not choose.
    #[arg(long, global = true)]
    standalone: bool,

    /// Client mode: override the daemon URL from `~/.config/walletd/client.toml` (devimint gates).
    #[arg(long, global = true)]
    url: Option<String>,

    /// Client mode: override the bearer-token file path from the client pointer (devimint gates).
    #[arg(long, global = true)]
    token_path: Option<PathBuf>,

    /// `--standalone` only: directory holding the wallet's RocksDB and mnemonic. Defaults to the
    /// `data_dir` in walletd.toml, then `$XDG_DATA_HOME/walletd` or `~/.local/share/walletd`.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// `--standalone` only: max wall-clock SECONDS for a single executor `perform` before it is
    /// abandoned and left Pending for the next reconcile (§15.9 — one stalled gateway must not
    /// freeze a whole tick). `0` disables the deadline. Default 600 (10 min).
    #[arg(long)]
    perform_timeout: Option<u64>,

    /// `--standalone` only: pin the shared lnv2 gateway URL for EVERY route this invocation
    /// resolves (money verbs, probes, ticks). Required against devimint, whose LDK gateway is
    /// not registered into any federation's lnv2 set (runbook §4); omitted, routes resolve from
    /// each federation's registered gateway list. Client mode rejects it — walletd's pin is host
    /// config (`walletd.toml`), and the wire has no gateway field (§6a.6).
    #[arg(long, global = true)]
    gateway: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Join a federation by its invite code (idempotent: re-joining an already-joined
    /// federation just opens it).
    Join { invite: String },
    /// Discover candidate federations from configured sources, structurally vet them, and
    /// optionally auto-join within the discovery caps.
    Discover {
        /// Source to use. Repeat for multiple sources. Defaults to manual when --invite is
        /// present, otherwise observer.
        #[arg(long = "source", value_enum)]
        source: Vec<DiscoverSourceArg>,
        /// Observer API base URL.
        #[arg(long, default_value = "https://observer.fedimint.org/api")]
        observer_url: String,
        /// Manual invite code(s) to discover.
        #[arg(long = "invite")]
        invite: Vec<String>,
        /// Auto-join structurally-passing discovered candidates within the configured caps.
        #[arg(long)]
        auto_join: bool,
        /// Allow regtest/signet in the structural scorer. Intended for devimint.
        #[arg(long)]
        scorer_allow_regtest: bool,
        /// Maximum successful Agent auto-joins in the trailing 7 days.
        #[arg(long)]
        max_auto_joins_per_week: Option<u32>,
        /// Lifetime cap on successful Agent-created partitions.
        #[arg(long)]
        lifetime_cap: Option<u32>,
        /// Emit a JSON report instead of TSV-style summary lines.
        #[arg(long)]
        json: bool,
    },
    /// List the durable candidate registry newest-first.
    Candidates {
        /// Filter by candidate state.
        #[arg(long, value_enum)]
        state: Option<CandidateStateArg>,
        /// Emit a JSON array instead of TSV rows.
        #[arg(long)]
        json: bool,
    },
    /// Mark an AutoJoined candidate as user-approved.
    Approve { fed: String },
    /// Print each joined federation's balance (msat) and the total.
    Balance,
    /// List joined federations.
    ListFeds,
    /// Receive Lightning into a federation: print the BOLT11 invoice to stdout (the payable
    /// result) and the operation `key:` to stderr. `await-receive <key>` its settlement. The
    /// gateway is auto-selected by the engine (the wire has no gateway field, §6a.6).
    Receive {
        /// Amount to receive, in millisatoshis.
        #[arg(long)]
        amount: u64,
        /// Maximum receive-side cost; defaults from the DB `Policy` per-move cap (both modes).
        #[arg(long)]
        fee_cap: Option<u64>,
        /// Stable client nonce used to attach a retry to the same receive intent. A fresh nonce
        /// is generated when omitted (each receive is distinct).
        #[arg(long)]
        nonce: Option<String>,
        /// Federation to receive into (hex id). Defaults to the policy spending pin / sole fed.
        #[arg(long)]
        to: Option<String>,
    },
    /// Pay a BOLT11 invoice. Async (§6a.6): prints a phase-1 line (`started`/`already-in-flight`/
    /// `already-paid <key>`) to stdout and the operation `key:` to stderr; `await-send <key>` its
    /// settlement.
    Pay {
        /// The BOLT11 invoice to pay.
        invoice: String,
        /// Amount in millisatoshis, as a cross-check: if present it must match the invoice
        /// amount. Amountless invoices are refused — the lnv2 send API cannot supply an amount.
        #[arg(long)]
        amount: Option<u64>,
        /// Maximum send cost; defaults from the DB `Policy` per-move cap.
        #[arg(long)]
        fee_cap: Option<u64>,
        /// Federation to pay from (hex id). Defaults to the policy spending pin / sole fed.
        #[arg(long)]
        fed: Option<String>,
    },
    /// Block (re-polling until terminal or `--timeout`) on a receive operation, then print its
    /// terminal state (`claimed` / `failed: …`). Keyed by the operation key from `receive`.
    AwaitReceive {
        /// The operation key printed by `receive` (`key: …`).
        key: String,
        /// Seconds to keep polling before giving up (transport timeout). Default 600.
        #[arg(long, default_value_t = 600)]
        timeout: u64,
    },
    /// Block (re-polling until terminal or `--timeout`) on a send operation, then print its
    /// terminal state (`success` / `failed: …`). Keyed by the operation key from `pay`.
    AwaitSend {
        /// The operation key printed by `pay` (`key: …`).
        key: String,
        /// Seconds to keep polling before giving up (transport timeout). Default 600.
        #[arg(long, default_value_t = 600)]
        timeout: u64,
    },
    /// Route an inflow to a chosen federation (spec §6/§7): size + cap-check the receive invoice
    /// so the wallet nets EXACTLY `amount`, print the BOLT11 to stdout and the operation `key:` to
    /// stderr, then `await-move <key>` once the external payer has paid.
    DirectInflow {
        /// Net amount the destination must end up with, in millisatoshis.
        #[arg(long)]
        amount: u64,
        /// Federation to receive into (hex id). Defaults to the policy spending pin / sole fed.
        #[arg(long)]
        to: Option<String>,
        /// Receive-side fee cap, in millisatoshis. Defaults from the DB `Policy`.
        #[arg(long)]
        fee_cap: Option<u64>,
        /// Stable client nonce for idempotency: the same nonce returns the same invoice (no second
        /// mint); change it for another same-amount inflow. Defaults to `0`.
        #[arg(long, default_value = "0")]
        nonce: String,
    },
    /// Block (re-polling until terminal or `--timeout`) on a move / direct-inflow operation, then
    /// print its terminal state (`done` / `failed: …`). Keyed by the operation key.
    AwaitMove {
        /// The operation key printed by `move` / `direct-inflow` (`key: …`).
        key: String,
        /// Seconds to keep polling before giving up (transport timeout). Default 600.
        #[arg(long, default_value_t = 600)]
        timeout: u64,
    },
    /// Move ecash between two joined federations through a shared gateway's internal swap (spec §7).
    /// Async (§6a.6): prints a phase-1 line (`started`/`already-in-flight <key>`) to stdout and the
    /// operation `key:` to stderr; `await-move <key>` its settlement.
    Move {
        /// Source federation to move ecash FROM (hex id).
        #[arg(long)]
        from: String,
        /// Destination federation to move ecash TO (hex id).
        #[arg(long)]
        to: String,
        /// Net amount the destination must end up with, in millisatoshis.
        #[arg(long)]
        amount: u64,
        /// Total move fee cap (BOTH legs), in millisatoshis. Defaults from the DB `Policy`.
        #[arg(long)]
        fee_cap: Option<u64>,
        /// Idempotency occurrence. Reusing the same occurrence reattaches to the same move (no
        /// re-mint/re-pay); bump it to start another same-params move after the first settles.
        #[arg(long, default_value_t = 0)]
        occurrence: u64,
    },
    /// Actively probe a candidate federation (phase 5 §5.0.7): run ONE sats-spending
    /// two-leg round trip (mint `--amount` on <fed> from the source, redeem the delta
    /// back) through the ordinary move machinery, record the attempt in the durable
    /// verdict history, and print `attempt:` + `verdict:` to stdout (keys/diagnostics go
    /// to stderr). Exits non-zero on a failed attempt — a probe IS a money op. A crashed
    /// probe resumes on the next invocation of `probe` for the same federation.
    Probe {
        /// The candidate federation to probe (hex id).
        fed: String,
        /// The spending federation to probe FROM (hex id). When omitted: with exactly TWO
        /// joined federations of which <fed> is one, the other is used; otherwise refused.
        #[arg(long)]
        from: Option<String>,
        /// Probe amount in millisatoshis (default 20000 = 20 sats).
        #[arg(long)]
        amount: Option<u64>,
        /// PER-LEG fee cap in millisatoshis (default 10000 = 10 sats).
        #[arg(long)]
        fee_cap: Option<u64>,
        /// Successes required for a `passed` verdict (default 3).
        #[arg(long)]
        min_successes: Option<u32>,
        /// Seconds the qualifying successes must span (default 24h). SHRINK-ONLY: a value
        /// above the default is rejected (durable retention cannot back a larger window).
        #[arg(long)]
        min_span_secs: Option<u64>,
        /// Seconds before the newest success goes stale (default 7d). SHRINK-ONLY.
        #[arg(long)]
        ttl_secs: Option<u64>,
    },
    /// Re-drive pending intents on demand (spec §6a.6 — the "it's wedged" button): the actor
    /// reads the current `Policy` for caps, so this takes no arguments. Prints the redriven /
    /// awaiters-rehydrated / executing-normalized counts.
    Reconcile,
    /// Run ONE orchestrator tick (Phase 2 step 2.2): probe every open federation, score them,
    /// build the allocator snapshot from the standing-instruction policy, decide, and APPLY the
    /// decisions through the executor — the wallet actually rebalances/tops-up. Prints the
    /// decisions and execution counts to stdout. Recurring schedulers must advance
    /// `--occurrence` after a settled move; a terminal same-occurrence replay exits non-zero
    /// instead of silently skipping the same edge forever.
    Tick {
        #[command(flatten)]
        policy: PolicyFlags,
    },
    /// DRY-RUN a tick (Phase 2 step 2.2): probe, score, and decide, but do NOT apply. Client mode
    /// uses walletd's stored policy; the per-invocation flags below require `--standalone`.
    Status {
        #[command(flatten)]
        policy: PolicyFlags,
    },
    /// Daemon health (spec §6a.6, NEW): actor queue depth, in-flight driver count, scheduler
    /// liveness. In `--standalone` it reflects the one-shot in-process service.
    Health,
    /// Read or edit the standing-instruction `Policy` (spec §6a.6): the user-decided targets,
    /// caps, fees, and budgets stored in the wallet DB and read by the actor at decide time.
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    /// Print the operation ledger newest-first (§11): one TAB-separated row per operation
    /// (`seq  updated_at  kind  status  amount_msat  recv_fee_msat  send_fee_quoted_msat  actor
    /// reason  key`; unknown fields = `-`). Offline — journal scan only. Filters apply before
    /// `--limit`.
    History {
        /// Maximum rows to print (after filters), newest-first.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Filter to operations involving this federation (hex id).
        #[arg(long)]
        fed: Option<String>,
        /// Filter by initiator.
        #[arg(long, value_enum)]
        actor: Option<ActorFilter>,
        /// Filter by status.
        #[arg(long, value_enum)]
        status: Option<StatusFilter>,
        /// Emit one JSON `OperationRecord` per line (JSONL) instead of the TSV table.
        #[arg(long)]
        json: bool,
    },
    /// Show one operation. Client mode resolves correlation keys through walletd; numeric sequence
    /// lookup and the richer offline record require `--standalone`.
    Show {
        /// A correlation key (e.g. `pay:…`) OR a numeric seq.
        reference: String,
        /// Emit the raw `OperationRecord` as JSON instead of the multi-line view.
        #[arg(long)]
        json: bool,
    },
}

/// `wallet-cli policy get|set` (spec §6a.6): `get` prints the stored `Policy`; `set` fetches it,
/// edits the fields named by flags, and PUTs the whole struct back.
#[derive(Subcommand)]
enum PolicyCommand {
    /// Print the stored `Policy` as pretty JSON.
    Get,
    /// Edit the named fields on the fetched `Policy` and PUT the whole struct (the rest is
    /// preserved). Only flags you pass change; omitted fields keep their stored value.
    Set(Box<PolicySetFlags>),
}

/// Per-field overrides for `policy set`. Every field is optional: only the ones passed change the
/// fetched `Policy`. `spending-fed`/`standby-fed` take a hex id; `--clear-*` unpins them.
#[derive(Args, Debug, Default)]
struct PolicySetFlags {
    #[arg(long)]
    per_fed_cap: Option<u64>,
    #[arg(long)]
    spending_target: Option<u64>,
    #[arg(long)]
    standby_target: Option<u64>,
    /// Absolute evacuation and manual-operation fee cap, in millisatoshis.
    #[arg(long)]
    max_fee: Option<u64>,
    /// Proportional funding-move fee cap, basis points (0-10000).
    #[arg(long)]
    max_fee_bps_of_move: Option<u16>,
    #[arg(long)]
    spending_fed: Option<String>,
    #[arg(long)]
    standby_fed: Option<String>,
    /// Unpin the spending federation (mutually exclusive with --spending-fed).
    #[arg(long, conflicts_with = "spending_fed")]
    clear_spending_fed: bool,
    /// Unpin the standby federation (mutually exclusive with --standby-fed).
    #[arg(long, conflicts_with = "standby_fed")]
    clear_standby_fed: bool,
    #[arg(long)]
    probe_min_span_secs: Option<u64>,
    #[arg(long)]
    probe_min_successes: Option<u32>,
    #[arg(long)]
    probe_ttl_secs: Option<u64>,
    #[arg(long)]
    probe_amount: Option<u64>,
    #[arg(long)]
    max_probe_attempts_per_week: Option<u32>,
    #[arg(long)]
    max_probe_spend_per_week: Option<u64>,
    #[arg(long)]
    base_interval_secs: Option<u64>,
    #[arg(long)]
    min_interval_secs: Option<u64>,
    #[arg(long)]
    evacuation_lead_secs: Option<u64>,
    #[arg(long)]
    discover_every_secs: Option<u64>,
    #[arg(long)]
    probe_retry_backoff_secs: Option<u64>,
    #[arg(long)]
    probe_refresh_lead_secs: Option<u64>,
    #[arg(long)]
    max_auto_joins_per_week: Option<u32>,
    #[arg(long)]
    auto_join_lifetime_cap: Option<u32>,
    #[arg(long)]
    max_candidates_per_pass: Option<u32>,
    #[arg(long)]
    per_preview_timeout_secs: Option<u64>,
    #[arg(long)]
    discover_pass_deadline_secs: Option<u64>,
    #[arg(long)]
    auto_join: Option<bool>,
    #[arg(long)]
    require_mainnet: Option<bool>,
}

/// `--actor` filter for `history` (spec §11). `pub(crate)` so client mode can filter wire rows.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum ActorFilter {
    User,
    Agent,
}

/// `--status` filter for `history` (spec §11).
#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum StatusFilter {
    Started,
    Awaiting,
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum DiscoverSourceArg {
    Observer,
    Manual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum CandidateStateArg {
    Discovered,
    Autojoined,
    Userapproved,
    Rejected,
}

/// Per-invocation standing-instruction overrides shared by standalone `tick` and `status`.
/// Omitted fields retain the DB-stored [`Policy`]; these flags never persist changes.
#[derive(Args, Default)]
struct PolicyFlags {
    /// Per-fed balance cap (ADR-0018), in millisatoshis.
    #[arg(long)]
    per_fed_cap: Option<u64>,
    /// Target spending-fed balance, in millisatoshis (top up below it).
    #[arg(long)]
    spending_target: Option<u64>,
    /// Target warm-standby balance, in millisatoshis (fund below it).
    #[arg(long)]
    standby_target: Option<u64>,
    /// Absolute evacuation fee cap, in millisatoshis.
    #[arg(long)]
    max_fee: Option<u64>,
    /// Proportional funding-move fee cap, in basis points of the amount moved (1-10000).
    #[arg(long)]
    max_fee_bps_of_move: Option<u16>,
    /// Pin the spending federation (hex id). Default: auto-designate the best-ranked eligible fed.
    #[arg(long)]
    spending: Option<String>,
    /// Pin the standby federation (hex id). Default: auto-designate the next eligible fed.
    #[arg(long)]
    standby: Option<String>,
    /// Allocation epoch stamped into each decision's idempotency key. Keep it for retrying
    /// Pending/Executing work; bump it after a settled tick to let the decision recur.
    #[arg(long, default_value_t = 0)]
    occurrence: u64,
    /// §5.1.3 FUNDING-GATE probe policy: seconds the qualifying probe successes must span
    /// before a discovered (auto-joined) fed becomes fundable. Default 24h (the conservative
    /// sustained-trust window). Loosen it to accept a shorter window — the operator owns that
    /// risk tradeoff (and it is how a live test funds a just-probed fed without a 24h wait).
    #[arg(long)]
    probe_min_span_secs: Option<u64>,
    /// §5.1.3 funding-gate probe policy: qualifying successes required (default 3).
    #[arg(long)]
    probe_min_successes: Option<u32>,
    /// §5.1.3 funding-gate probe policy: seconds before the newest success goes stale
    /// (default 7d). A verdict outside this ttl window is not a pass.
    #[arg(long)]
    probe_ttl_secs: Option<u64>,
}

impl PolicyFlags {
    fn has_overrides(&self) -> bool {
        self.per_fed_cap.is_some()
            || self.spending_target.is_some()
            || self.standby_target.is_some()
            || self.max_fee.is_some()
            || self.max_fee_bps_of_move.is_some()
            || self.spending.is_some()
            || self.standby.is_some()
            || self.occurrence != 0
            || self.probe_min_span_secs.is_some()
            || self.probe_min_successes.is_some()
            || self.probe_ttl_secs.is_some()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Restore the default SIGPIPE disposition. Rust sets SIGPIPE to SIG_IGN at startup, which
    // turns a consumer closing our stdout early (e.g. `wallet-cli balance | head`, or an awk that
    // exits mid-stream) into an EPIPE that makes the next `println!` PANIC. SIG_DFL makes the
    // process terminate quietly on a broken pipe instead — the Unix CLI convention.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    // Diagnostics go to STDERR (never stdout), so command output stays scriptable: e.g.
    // `id=$(wallet-cli join <invite>)` must capture only the federation id, not log lines.
    // Honor `RUST_LOG` (the smoke runbook sets `RUST_LOG=warn`), defaulting to `warn`.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    // clap's default parse path exits with status 2 for malformed invocations, but exit 2 is
    // reserved for a decide-time REFUSED outcome. Keep help/version at 0 and route every actual
    // usage error through the pinned usage/other code 1.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let code = if error.exit_code() == 0 { 0 } else { 1 };
            error.print()?;
            std::process::exit(code);
        }
    };

    // Client mode is the DEFAULT (spec §6a.7); `--standalone` is a deliberate opt-in. The two
    // modes both funnel their outcome into the [`CliExit`] taxonomy → distinct process exit codes.
    let outcome = if cli.standalone {
        run_standalone(cli).await
    } else {
        run_client(cli).await
    };
    match outcome {
        Ok(()) => Ok(()),
        Err(exit) => {
            eprintln!("{}", exit.message());
            std::process::exit(exit.code());
        }
    }
}

/// Client mode (THE DEFAULT, spec §6a.7): every operational verb becomes an HTTP call to a running
/// `walletd`. The three standalone-only agent/diagnostic verbs (discover/probe/tick — no daemon
/// endpoint, §6a.6) refuse with the two-options message rather than a silent fallback.
async fn run_client(cli: Cli) -> Result<(), CliExit> {
    // `--data-dir` selects the STANDALONE wallet store; in client mode the daemon owns the store
    // (config comes from `client.toml`). Silently ignoring a wallet-SELECTION flag could target a
    // different wallet than the user named — for a money tool that is a spend-from-the-wrong-wallet
    // footgun — so fail loud (§6a.7: never a silent fallback), pointing at the flag it belongs to.
    if cli.data_dir.is_some() {
        return Err(CliExit::Usage(anyhow::anyhow!(
            "--data-dir selects the standalone wallet store and has no effect in client mode; \
             rerun with --standalone to use it, or drop it to target the configured walletd"
        )));
    }
    // Same fail-loud rule for the other standalone-only globals: silently discarding a MONEY
    // deadline (--perform-timeout) or a route pin (--gateway) would give the caller different
    // money behavior than the flag they typed — the daemon keeps its own deadline and pin.
    if cli.perform_timeout.is_some() {
        return Err(CliExit::Usage(anyhow::anyhow!(
            "--perform-timeout bounds the standalone in-process executor and has no effect in \
             client mode (walletd keeps its own deadline); rerun with --standalone to use it"
        )));
    }
    if cli.gateway.is_some() {
        return Err(CliExit::Usage(anyhow::anyhow!(
            "--gateway pins the standalone route and has no effect in client mode (walletd's pin \
             is host config in walletd.toml; the wire has no gateway field); rerun with --standalone"
        )));
    }
    match &cli.command {
        Command::Discover { .. } | Command::Probe { .. } | Command::Tick { .. } => {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "standalone-only verb: rerun with --standalone (this agent verb has no daemon endpoint)"
            )));
        }
        Command::History { fed: Some(_), .. } => {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "history --fed requires --standalone (the daemon history view omits the source federation)"
            )));
        }
        Command::Show { reference, .. } if reference.parse::<u64>().is_ok() => {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "show by numeric sequence requires --standalone (the daemon operation endpoint accepts keys only)"
            )));
        }
        Command::Status { policy } if policy.has_overrides() => {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "status overrides require --standalone (the daemon status endpoint uses the stored policy)"
            )));
        }
        _ => {}
    }
    let client = WalletdClient::resolve(cli.url.as_deref(), cli.token_path.as_deref())?;
    match cli.command {
        Command::Balance => client.balance().await,
        Command::ListFeds => client.list_feds().await,
        Command::History {
            limit,
            fed: _,
            actor,
            status,
            json,
        } => client.history(limit, actor, status, json).await,
        Command::Show { reference, json } => client.show(&reference, json).await,
        Command::Candidates { state, json } => client.candidates(state, json).await,
        Command::Join { invite } => client.join(invite).await,
        Command::Approve { fed } => client.approve(parse_fed_id(&fed)?).await,
        Command::Receive {
            amount,
            fee_cap,
            nonce,
            to,
        } => {
            client
                .receive(
                    amount,
                    fee_cap,
                    nonce.unwrap_or_else(cli_nonce),
                    parse_fed_opt(to.as_deref())?,
                )
                .await
        }
        Command::Pay {
            invoice,
            amount,
            fee_cap,
            fed,
        } => {
            client
                .pay(invoice, amount, fee_cap, parse_fed_opt(fed.as_deref())?)
                .await
        }
        Command::DirectInflow {
            amount,
            to,
            fee_cap,
            nonce,
        } => {
            client
                .direct_inflow(amount, fee_cap, nonce, parse_fed_opt(to.as_deref())?)
                .await
        }
        Command::Move {
            from,
            to,
            amount,
            fee_cap,
            occurrence,
        } => {
            client
                .move_op(
                    parse_fed_id(&from)?,
                    parse_fed_id(&to)?,
                    amount,
                    fee_cap,
                    occurrence,
                )
                .await
        }
        Command::AwaitReceive { key, timeout } => {
            client
                .await_op(AwaitVerb::Receive, &key, Duration::from_secs(timeout))
                .await
        }
        Command::AwaitSend { key, timeout } => {
            client
                .await_op(AwaitVerb::Send, &key, Duration::from_secs(timeout))
                .await
        }
        Command::AwaitMove { key, timeout } => {
            client
                .await_op(AwaitVerb::Move, &key, Duration::from_secs(timeout))
                .await
        }
        Command::Reconcile => client.reconcile().await,
        Command::Health => client.health().await,
        Command::Status { .. } => client.status().await,
        Command::Policy { command } => match command {
            PolicyCommand::Get => {
                let policy = client.get_policy().await?;
                print_policy(&policy)
            }
            PolicyCommand::Set(flags) => {
                let mut policy = client.get_policy().await?;
                apply_policy_set(&mut policy, &flags)?;
                let updated = client.put_policy(&policy).await?;
                print_policy(&updated)
            }
        },
        // Handled above (standalone-only refusal).
        Command::Discover { .. } | Command::Probe { .. } | Command::Tick { .. } => unreachable!(),
    }
}

/// `--standalone` (spec §6a.7): take the exclusive `db.lock` and spin up the same in-process actor
/// and drivers `walletd` runs, run the one command through the same `WalletClient` command path the
/// daemon handlers use, then shut down. A silent fallback to this mode was rejected: it would block
/// a supervisor-restarting daemon behind a lock race the user did not choose.
async fn run_standalone(cli: Cli) -> Result<(), CliExit> {
    let perform_timeout_secs = cli.perform_timeout.unwrap_or(600);
    let perform_timeout =
        (perform_timeout_secs > 0).then(|| Duration::from_secs(perform_timeout_secs));
    let gateway = cli.gateway.clone().map(GatewayUrl);
    let data_dir = resolve_standalone_data_dir(cli.data_dir)?;

    // 0700 like the daemon (`wallet-daemon::config::ensure_private_data_dir`): the mnemonic and
    // wallet store live here, and the process umask (commonly 022) would otherwise leave a fresh
    // default directory world-readable before RocksDB writes the seed.
    ensure_private_data_dir(&data_dir)?;
    let db_path = data_dir.join("client.db");
    // Fast-fail db.lock pre-check (spec §6a.7): fedimint's `open` BLOCKS on a held lock, so a
    // running `walletd` would HANG the CLI here instead of giving the deliberate lock-held error.
    check_db_lock(&db_path)?;
    // The pre-check releases its probe lock before `open` re-acquires — a daemon (re)starting in
    // exactly that window would make `open` block indefinitely. Bound it: past the deadline this
    // IS the lock-held case, reported as such rather than hanging a supervisor-adjacent race.
    let open = fedimint_rocksdb::RocksDb::build(db_path).open();
    let db: Database = tokio::time::timeout(Duration::from_secs(10), open)
        .await
        .map_err(|_| {
            CliExit::Usage(anyhow::anyhow!(
                "opening the wallet store timed out waiting for its lock — another process took \
                 it after the pre-check (walletd restarting?); stop it, or use client mode \
                 (drop --standalone)"
            ))
        })?
        .map_err(|e| CliExit::Usage(anyhow::anyhow!("opening the wallet store: {e:#}")))?
        .into();

    // The journal's OWN RocksDB, matching walletd's split (client.db FIRST — the exclusivity
    // anchor above — then journal.db, always in that order so two processes can never deadlock).
    // A co-located journal's write churn flushes fedimint's tiny no-history memtable out from
    // under any transaction fedimint holds open and fails its commit (the 24h-soak wedge;
    // fixed at our pinned rev, upstream PR #8816, but the isolation stands).
    let journal_open = fedimint_rocksdb::RocksDb::build(data_dir.join("journal.db")).open();
    let journal_db: Database = tokio::time::timeout(Duration::from_secs(10), journal_open)
        .await
        .map_err(|_| {
            CliExit::Usage(anyhow::anyhow!(
                "opening the journal store timed out waiting for its lock (walletd restarting?); \
                 stop it, or use client mode (drop --standalone)"
            ))
        })?
        .map_err(|e| CliExit::Usage(anyhow::anyhow!("opening the journal store: {e:#}")))?
        .into();

    let journal = Arc::new(FedimintJournal::new(journal_db.clone()));

    // §11: `history`/`show`/`candidates`/`approve` are OFFLINE journal reads and MUST work with only
    // the journal open — dispatch them BEFORE any client/network setup (see the phase-4/5 rationale).
    let command = match cli.command {
        Command::History {
            limit,
            fed,
            actor,
            status,
            json,
        } => {
            return run_history(&journal, limit, fed, actor, status, json)
                .await
                .map_err(CliExit::from)
        }
        Command::Show { reference, json } => {
            return run_show(&journal, reference, json)
                .await
                .map_err(CliExit::from)
        }
        Command::Candidates { state, json } => {
            return run_candidates(&journal, state, json)
                .await
                .map_err(CliExit::from)
        }
        Command::Approve { fed } => return run_approve(&journal, fed).await,
        other => other,
    };

    let mnemonic = load_or_generate_mnemonic(&db)
        .await
        .map_err(CliExit::from)?;
    let multi_client = Arc::new(MultiClient::new(db, journal_db, mnemonic).await);

    let joined = journal
        .list_federations()
        .await
        .map_err(|e| CliExit::Usage(anyhow::anyhow!("reading federation registry: {e:?}")))?;
    let joined_ids: Vec<_> = joined.iter().map(|(id, _)| *id).collect();
    let infos: Vec<_> = joined.iter().map(|(_, info)| info.clone()).collect();
    multi_client
        .open_all(&infos)
        .await
        .map_err(|e| CliExit::Usage(anyhow::anyhow!("opening joined federations: {e:#}")))?;
    let open_ids = multi_client.federations();

    // Money/actor verbs run through the SAME `WalletClient` command path the daemon handlers use
    // (actor + drivers + scheduler). The agent/diagnostic verbs (discover/probe/tick) and the live
    // reads (balance/list-feds/status) stay Runtime-direct one-shots — validated, no actor needed.
    if is_actor_verb(&command) {
        return run_standalone_actor(
            command,
            journal,
            multi_client,
            joined_ids,
            gateway,
            perform_timeout,
        )
        .await;
    }
    run_standalone_direct(
        command,
        journal,
        multi_client,
        joined,
        joined_ids,
        open_ids,
        gateway,
        perform_timeout,
    )
    .await
    .map_err(CliExit::from)
}

/// The host-config fields accepted by `walletd`. Standalone only consumes `data_dir`, but parsing
/// the real shape (including `deny_unknown_fields`) keeps a malformed host config from silently
/// selecting a different wallet store than the daemon.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WalletdHostConfig {
    data_dir: Option<String>,
    #[serde(rename = "address")]
    _address: Option<String>,
    #[serde(rename = "port")]
    _port: Option<u16>,
    #[serde(rename = "token_path")]
    _token_path: Option<String>,
    #[serde(rename = "log_level")]
    _log_level: Option<String>,
    // The daemon's route pin is NOT inherited by standalone (route pinning stays the explicit
    // `--gateway` flag — money routing must never change based on a file the user didn't name);
    // parsed only so a pinned host config doesn't fail `deny_unknown_fields` here.
    #[serde(rename = "gateway")]
    _gateway: Option<String>,
}

/// Resolve the store selected by `walletd`: an explicit CLI override wins; otherwise read
/// `~/.config/walletd/walletd.toml` (honoring XDG), then fall back to the owner-ratified default
/// only when that file or its `data_dir` field is absent. This prevents `--standalone` from
/// silently opening a fresh default wallet when the daemon was configured with a custom store.
fn resolve_standalone_data_dir(override_path: Option<PathBuf>) -> Result<PathBuf, CliExit> {
    if let Some(path) = override_path {
        return Ok(path);
    }
    let config_path = walletd_config_home()?.join("walletd.toml");
    let text = match std::fs::read_to_string(&config_path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return default_walletd_data_dir()
        }
        Err(error) => {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "reading host config {}: {error}",
                config_path.display()
            )))
        }
    };
    let config: WalletdHostConfig = toml::from_str(&text).map_err(|error| {
        CliExit::Usage(anyhow::anyhow!(
            "parsing host config {}: {error}",
            config_path.display()
        ))
    })?;
    match config.data_dir {
        Some(path) => resolve_walletd_path(&path),
        None => default_walletd_data_dir(),
    }
}

fn walletd_config_home() -> Result<PathBuf, CliExit> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return Ok(xdg.join("walletd"));
    }
    Ok(walletd_home_dir()?.join(".config").join("walletd"))
}

fn resolve_walletd_path(raw: &str) -> Result<PathBuf, CliExit> {
    let expanded = if raw == "~" {
        walletd_home_dir()?
    } else if let Some(rest) = raw.strip_prefix("~/") {
        walletd_home_dir()?.join(rest)
    } else {
        PathBuf::from(raw)
    };
    if !expanded.is_absolute() {
        return Err(CliExit::Usage(anyhow::anyhow!(
            "path {raw:?} resolves to a non-absolute path {}; use an absolute path or a ~-prefixed one",
            expanded.display()
        )));
    }
    Ok(expanded)
}

fn walletd_home_dir() -> Result<PathBuf, CliExit> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            CliExit::Usage(anyhow::anyhow!(
                "HOME is not set; pass --data-dir for standalone mode"
            ))
        })
}

/// The owner-ratified walletd store location (§6a.6), kept byte-for-byte equivalent to
/// `wallet-daemon::config` for an absent/default host config.
fn default_walletd_data_dir() -> Result<PathBuf, CliExit> {
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return Ok(xdg.join("walletd"));
    }
    Ok(walletd_home_dir()?
        .join(".local")
        .join("share")
        .join("walletd"))
}

/// Create the wallet data dir with owner-only permissions, byte-for-byte the daemon's
/// `wallet-daemon::config::ensure_private_data_dir` behavior — the store holds the mnemonic.
fn ensure_private_data_dir(path: &std::path::Path) -> Result<(), CliExit> {
    std::fs::create_dir_all(path).map_err(|e| {
        CliExit::Usage(anyhow::anyhow!("creating data dir {}: {e}", path.display()))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(|e| {
            CliExit::Usage(anyhow::anyhow!(
                "setting private permissions on {}: {e}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

/// The Runtime-direct standalone verbs (spec §6a.7): the diagnostic reads (balance/list-feds/status)
/// and the standalone-only agent one-shots (discover/probe/tick), UNCHANGED in semantics — they
/// call `Runtime` directly (no actor), exactly as before phase 6a.
#[allow(clippy::too_many_arguments)]
async fn run_standalone_direct(
    command: Command,
    journal: Arc<FedimintJournal>,
    multi_client: Arc<MultiClient>,
    joined: Vec<(FederationId, FederationInfo)>,
    joined_ids: Vec<FederationId>,
    open_ids: Vec<FederationId>,
    gateway: Option<GatewayUrl>,
    perform_timeout: Option<Duration>,
) -> anyhow::Result<()> {
    match command {
        Command::Discover {
            source,
            observer_url,
            invite,
            auto_join,
            scorer_allow_regtest,
            max_auto_joins_per_week,
            lifetime_cap,
            json,
        } => {
            let sources = build_discover_sources(source, observer_url, invite)?;
            let mut policy = DiscoveryPolicy {
                auto_join,
                require_mainnet: !scorer_allow_regtest,
                ..DiscoveryPolicy::default()
            };
            if let Some(max) = max_auto_joins_per_week {
                policy.max_auto_joins_per_week = max;
            }
            if let Some(cap) = lifetime_cap {
                policy.auto_join_lifetime_cap = cap;
            }
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway,
                operator_hard_cap(false),
                perform_timeout,
            );
            let report = runtime.discover(sources, policy).await?;
            if json {
                println!("{}", serde_json::to_string(&discover_report_json(&report))?);
            } else {
                for line in discover_summary_lines(&report) {
                    println!("{line}");
                }
            }
        }
        Command::Balance => {
            // §15.8: a joined fed that failed to open must NOT silently drop out of the total.
            // Print an `unavailable` row for each, label the total `(N/M federations)`, and exit
            // non-zero when any fed is missing so a script never mistakes a partial view for whole.
            let unopened = unopened_feds(&joined_ids, &open_ids);
            let mut total_msat = 0u64;
            for id in &joined_ids {
                if open_ids.contains(id) {
                    let balance = multi_client.balance(id).await?;
                    total_msat += balance.0;
                    println!("{}: {} msat", id.to_hex(), balance.0);
                } else {
                    println!("{}: unavailable (failed to open)", id.to_hex());
                }
            }
            println!(
                "total ({}/{} federations): {total_msat} msat",
                open_ids.len(),
                joined_ids.len()
            );
            if !unopened.is_empty() {
                anyhow::bail!(
                    "{} joined federation(s) failed to open ({}); the total above omits them — \
                     check stderr for the open error",
                    unopened.len(),
                    hex_list(&unopened)
                );
            }
        }
        Command::ListFeds => {
            for (id, info) in joined {
                println!(
                    "{} invite={} joined_at={}",
                    id.to_hex(),
                    info.invite,
                    info.joined_at
                );
            }
        }
        Command::Probe {
            fed,
            from,
            amount,
            fee_cap,
            min_successes,
            min_span_secs,
            ttl_secs,
        } => {
            // Parse-only — deliberately NOT `select_fed`: a not-joined candidate must
            // still reach the runtime's preflight so the refusal lands in `history`
            // (§5.0.5 — a candidate-scoped failed probe is never invisible).
            let candidate = parse_fed_id(&fed)?;
            // Source resolution is a PRE-IDENTITY usage error, not a probe attempt: the
            // umbrella row is `Probe { fed, from, .. }`, so with no resolvable `from` there
            // is no probe to record. It bails to stderr with a non-zero exit (visible to the
            // operator) — distinct from a candidate-scoped fault, which carries a full
            // identity and IS recorded by the runtime.
            let source = probe_source(&journal, &joined_ids, candidate, from.as_deref()).await?;
            // Like --from, the MONEY params of an in-flight probe are fixed: a resume runs
            // its legs with the session's stored amount/fee_cap. Reject a conflicting
            // --amount/--fee-cap rather than silently ignore it (the operator would think a
            // different-sized money probe ran). Omitting them resumes as-is.
            reject_conflicting_probe_money_flags(&journal, candidate, amount, fee_cap).await?;
            let policy =
                build_probe_policy(amount, fee_cap, min_successes, min_span_secs, ttl_secs)?;
            // Probes ride the ordinary move machinery: an explicit --gateway pins the
            // shared route (required against devimint, whose LDK gateway is not registered
            // into the lnv2 set), else the route resolves from each fed's registered
            // gateways. The ADR-0018 hard cap is enforced verbatim — probe legs never
            // bypass it (§5.0.5).
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway,
                operator_hard_cap(false),
                perform_timeout,
            );
            let report = runtime
                .active_probe(candidate, source, &policy, Actor::User)
                .await?;
            // Keys/diagnostics -> stderr; the scriptable attempt/verdict lines -> stdout.
            eprintln!("in_key: {}", report.in_key.0);
            if let Some(out_key) = &report.out_key {
                eprintln!("out_key: {}", out_key.0);
            }
            eprintln!(
                "verdict_before: {}",
                active_probe_label(report.verdict_before)
            );
            // §5.0.7 scriptable contract: attempt + verdict on stdout for EVERY terminal
            // outcome (active_probe returns Ok even for umbrella-only no-attempt refusals;
            // only genuinely transient defers reach the `?` above and bail to stderr).
            match &report.outcome {
                ProbeOutcome::Attempt(attempt) if attempt.ok => {
                    println!("attempt: ok");
                    println!("verdict: {}", active_probe_label(report.verdict_after));
                }
                ProbeOutcome::Attempt(attempt) => {
                    println!(
                        "attempt: failed {}",
                        attempt.error.as_deref().unwrap_or("(no diagnostic)")
                    );
                    println!("verdict: {}", active_probe_label(report.verdict_after));
                    anyhow::bail!("probe attempt failed (a probe is a money operation)");
                }
                ProbeOutcome::NoAttempt(diagnostic) => {
                    // No demoting attempt was recorded (verdict unchanged), but the
                    // invocation failed — surface it on stdout AND exit non-zero.
                    println!("attempt: none {diagnostic}");
                    println!("verdict: {}", active_probe_label(report.verdict_after));
                    anyhow::bail!("probe did not complete (umbrella-only; no attempt recorded)");
                }
            }
        }
        Command::Tick { policy } => {
            // §15.8: a tick must NOT drive money decisions from a partial world-view. Refuse (no
            // action, non-zero exit) BEFORE probing if any joined fed failed to open.
            refuse_on_partial_open(&joined_ids, &open_ids)?;
            let tick_policy =
                build_standalone_tick_policy(&journal, &policy, &joined_ids, &open_ids).await?;
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway.clone(),
                Some(tick_policy.per_fed_cap),
                perform_timeout,
            );
            let report = runtime.tick(&tick_policy).await?;
            // Decisions + the apply summary -> stdout (the scriptable result).
            print_decisions(&report.decisions);
            println!(
                "performed={} skipped={} failed={} terminal_failed_skipped={} retryable={}",
                report.summary.performed,
                report.summary.skipped,
                report.summary.failed,
                report.summary.terminal_failed_skipped,
                report.summary.retryable
            );
            // A tick IS a money operation: if any decision failed to apply, exit NON-ZERO — the
            // same stance `move`/`await-move`/`direct-inflow` take — so a scheduled caller gating
            // on the exit code never mistakes a failed rebalance for success. The per-intent reason
            // is logged to stderr by the executor; stdout already carries the scriptable result.
            if let Some(msg) = tick_apply_failure(&report.summary) {
                anyhow::bail!("{msg}");
            }
        }
        Command::Status { policy } => {
            // §15.8: status is the DIAGNOSTIC, so it still prints the scored view even under a
            // partial open — but it reports the unopened feds as rows and exits non-zero.
            let unopened = unopened_feds(&joined_ids, &open_ids);
            let tick_policy =
                build_standalone_tick_policy(&journal, &policy, &joined_ids, &open_ids).await?;
            // Dry-run only, but the route gate must match the tick that would apply.
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway.clone(),
                Some(tick_policy.per_fed_cap),
                perform_timeout,
            );
            let report = runtime.status(&tick_policy).await?;
            // The dry-run view -> stdout: designation, the per-fed scored rows, then the
            // decisions that WOULD run (nothing is applied).
            println!("spending_fed: {}", opt_fed_hex(report.spending_fed));
            println!("standby_fed: {}", opt_fed_hex(report.standby_fed));
            for scored in &report.scored {
                println!(
                    "{}",
                    describe_scored(scored, report.spending_fed, report.standby_fed)
                );
            }
            for id in &unopened {
                println!("{}: unavailable (failed to open)", id.to_hex());
            }
            print_decisions(&report.decisions);
            if !unopened.is_empty() {
                anyhow::bail!(
                    "{} joined federation(s) failed to open ({}); the scored view above covers \
                     only the open set — repair the fed partition(s) and retry",
                    unopened.len(),
                    hex_list(&unopened)
                );
            }
        }
        // Offline verbs (history/show/candidates/approve) are dispatched before this function; the
        // money/actor verbs (pay/move/receive/direct-inflow/join/await-*/reconcile/policy/health)
        // run through `run_standalone_actor`. The `watch` verb was DELETED (§6a.7 — the daemon's
        // scheduler IS the watch). So only the Runtime-direct verbs reach here.
        _ => unreachable!("non-Runtime-direct standalone verbs are dispatched elsewhere"),
    }

    Ok(())
}

/// Fast-fail `db.lock` pre-check (spec §6a.7). fedimint's `db_locked` layer BLOCKS on a held lock
/// (`new_exclusive` after a failed `new_try_exclusive`), so opening the store would HANG when
/// `walletd` owns it. We non-blockingly probe the SAME `<data_dir>/client.db.lock` (same crate,
/// guaranteed interop), then RELEASE it so the real open can re-acquire it: a held lock is the
/// deliberate "another process owns the store" error, exit 1.
fn check_db_lock(db_path: &std::path::Path) -> Result<(), CliExit> {
    let lock_path = db_path.with_extension("db.lock");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&lock_path)
        .map_err(|e| {
            CliExit::Usage(anyhow::anyhow!(
                "opening lock file {}: {e}",
                lock_path.display()
            ))
        })?;
    match fs_lock::FileLock::new_try_exclusive(file) {
        // Free: drop the probe lock so fedimint's own open re-acquires it.
        Ok(lock) => {
            drop(lock);
            Ok(())
        }
        // Contention (`WouldBlock`, no io error): walletd (or another process) holds the store.
        Err((_, None)) => Err(CliExit::Usage(anyhow::anyhow!(
            "another process owns the wallet store (walletd?); stop it, or use client mode (drop --standalone)"
        ))),
        // A genuine lock syscall fault (e.g. a filesystem without advisory-lock support, or a
        // permission error) is NOT contention — surface it honestly instead of blaming walletd.
        Err((_, Some(error))) => Err(CliExit::Usage(anyhow::anyhow!(
            "probing the wallet store lock {} failed: {error}",
            lock_path.display()
        ))),
    }
}

/// The verbs that go through the in-process `WalletService` (actor + drivers) in `--standalone`,
/// mirroring the daemon handlers' `WalletClient` command path exactly.
fn is_actor_verb(command: &Command) -> bool {
    matches!(
        command,
        Command::Join { .. }
            | Command::Receive { .. }
            | Command::Pay { .. }
            | Command::AwaitReceive { .. }
            | Command::AwaitSend { .. }
            | Command::DirectInflow { .. }
            | Command::AwaitMove { .. }
            | Command::Move { .. }
            | Command::Reconcile
            | Command::Health
            | Command::Policy { .. }
    )
}

/// The invoice-mint hard deadline for receive/direct-inflow (spec §6a.6, a const not policy).
const INVOICE_MINT_DEADLINE: Duration = Duration::from_secs(30);

/// Spin up the same actor + drivers the daemon runs (spec §6a.7: "minus HTTP"), MINUS the watch
/// scheduler — a one-shot standalone command must not fire the background rebalancer (that is the
/// daemon's job; the `watch` verb was deleted for exactly this reason). Drive the ONE money/actor
/// verb through the same `WalletClient` command path the daemon handlers use, then shut down (abort
/// drivers first, then drain the actor — the daemon's SIGTERM order).
async fn run_standalone_actor(
    command: Command,
    journal: Arc<FedimintJournal>,
    multi_client: Arc<MultiClient>,
    joined_ids: Vec<FederationId>,
    gateway: Option<GatewayUrl>,
    perform_timeout: Option<Duration>,
) -> Result<(), CliExit> {
    // No hard cap: the actor reads the DB `Policy` per decide (§6a.6), exactly like the daemon's
    // service runtime. The gateway pin comes from the standalone-only `--gateway` — REQUIRED
    // against devimint, whose LDK gateway is never registered into the lnv2 set (runbook §4);
    // without a pin the driver's registered-gateway scan finds nothing and the operation sits
    // Pending until the mint deadline.
    let runtime = Runtime::new(
        multi_client.clone(),
        journal.clone(),
        gateway,
        None,
        perform_timeout,
    );
    let service = WalletService::start_without_scheduler(runtime)
        .await
        .map_err(service_err_to_exit)?;
    let client = service.client();
    let result = actor_command(
        command,
        &client,
        &journal,
        &multi_client,
        &joined_ids,
        &service,
    )
    .await;
    let shutdown = service.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(e)) => Err(CliExit::Usage(anyhow::anyhow!(
            "wallet service shutdown failed: {e}"
        ))),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(e)) => {
            // The command error is the actionable one; the shutdown fault is a stderr footnote.
            eprintln!("warning: wallet service shutdown failed: {e}");
            Err(err)
        }
    }
}

/// Translate ONE money/actor verb into the `WalletClient` command path, mirroring the daemon
/// handlers (parse → build `Action` → `decide_op`/`resolve_await`/`reconcile`/policy), then render
/// the frozen contract. This is the "one code path" the spec demands — no legacy `Runtime::pay`
/// fork; the actor owns admission, reservations, holds, and (async) driving.
async fn actor_command(
    command: Command,
    client: &WalletClient,
    journal: &FedimintJournal,
    multi_client: &MultiClient,
    joined_ids: &[FederationId],
    service: &WalletService,
) -> Result<(), CliExit> {
    match command {
        Command::Join { invite } => {
            let parsed = InviteCode::from_str(&invite)
                .map_err(|e| CliExit::Refused(format!("invalid invite code: {e}")))?;
            let federation = {
                use fedimint_core::BitcoinHash as _;
                FederationId(parsed.federation_id().0.to_byte_array())
            };
            let invite = parsed.to_string();
            let key = join_intent_key(federation, &invite);
            let membership_preexisting = journal
                .get_federation(&federation)
                .await
                .map_err(storage_cli)?
                .is_some();
            let action = Action::Join {
                federation,
                invite,
                membership_preexisting,
            };
            let decided = client
                .decide_op(op_request(
                    action,
                    key.clone(),
                    Occurrence(0),
                    BTreeMap::new(),
                ))
                .await
                .map_err(service_err_to_exit)?;
            render::print_phase1(
                render::phase1_word(
                    decided.status == IntentStatus::Done,
                    decided.deduplicated,
                    "already-joined",
                ),
                &key.0,
            );
            Ok(())
        }
        Command::Pay {
            invoice,
            amount,
            fee_cap,
            fed,
        } => {
            let policy = client.get_policy().await.map_err(service_err_to_exit)?;
            let details = parse_invoice(&Invoice(invoice.clone()))
                .map_err(|e| CliExit::Refused(format!("invalid BOLT11 invoice: {e}")))?;
            let amount = match (details.amount, amount.map(Msat)) {
                (Some(inv), Some(stated)) if inv != stated => {
                    return Err(CliExit::Refused(
                        "stated --amount does not match the invoice amount".to_owned(),
                    ))
                }
                (Some(inv), _) => inv,
                // The pinned lnv2 send API takes no amount parameter (`MultiClient::pay` →
                // `LightningClientModule::send(bolt11, gateway, meta)`), so an amountless
                // invoice is UNPAYABLE by the engine — refuse at admission rather than admit
                // an operation whose driver can only fail after the 202-equivalent.
                (None, _) => {
                    return Err(CliExit::Refused(
                        "amountless BOLT11 invoices are not payable (the lnv2 send API cannot \
                         supply an amount); request an amount-carrying invoice"
                            .to_owned(),
                    ))
                }
            };
            let fee_cap = fee_cap.map(Msat).unwrap_or(policy.max_fee);
            let from = resolve_fed_standalone(
                parse_fed_opt(fed.as_deref())?,
                policy.spending_fed,
                joined_ids,
            )?;
            let key = raw_pay_key(details.payment_hash);
            let action = Action::Pay {
                from,
                invoice: Invoice(invoice),
                amount,
                fee_cap,
                payment_hash: details.payment_hash,
                gateway: None,
            };
            let balances = sample_balances_standalone(multi_client, &[from]).await?;
            let decided = client
                .decide_op(op_request(action, key.clone(), Occurrence(0), balances))
                .await
                .map_err(service_err_to_exit)?;
            render::print_phase1(
                render::phase1_word(
                    decided.status == IntentStatus::Done,
                    decided.deduplicated,
                    "already-paid",
                ),
                &key.0,
            );
            Ok(())
        }
        Command::Move {
            from,
            to,
            amount,
            fee_cap,
            occurrence,
        } => {
            let from = parse_fed_id(&from)?;
            let to = parse_fed_id(&to)?;
            if from == to {
                return Err(CliExit::Refused(
                    "move --from and --to must be different federations (from == to is a no-op)"
                        .to_owned(),
                ));
            }
            ensure_joined_standalone(from, joined_ids)?;
            ensure_joined_standalone(to, joined_ids)?;
            let policy = client.get_policy().await.map_err(service_err_to_exit)?;
            let fee_cap = fee_cap.map(Msat).unwrap_or(policy.max_fee);
            let key = move_key(&from, &to, Msat(amount), fee_cap, Occurrence(occurrence));
            let action = Action::Move {
                from,
                to,
                amount: Msat(amount),
                fee_cap,
            };
            let balances = sample_balances_standalone(multi_client, &[from, to]).await?;
            let decided = client
                .decide_op(op_request(
                    action,
                    key.clone(),
                    Occurrence(occurrence),
                    balances,
                ))
                .await
                .map_err(service_err_to_exit)?;
            render::print_phase1(
                render::phase1_word(
                    decided.status == IntentStatus::Done,
                    decided.deduplicated,
                    "already-done",
                ),
                &key.0,
            );
            Ok(())
        }
        Command::Receive {
            amount,
            fee_cap,
            nonce,
            to,
        } => {
            let nonce = nonce.unwrap_or_else(cli_nonce);
            validate_nonce_cli(&nonce)?;
            let policy = client.get_policy().await.map_err(service_err_to_exit)?;
            let to = resolve_fed_standalone(
                parse_fed_opt(to.as_deref())?,
                policy.spending_fed,
                joined_ids,
            )?;
            let fee_cap = fee_cap.map(Msat).unwrap_or(policy.max_fee);
            let key = raw_receive_key(to, Msat(amount), &nonce);
            let action = Action::Receive {
                to,
                amount: Msat(amount),
                fee_cap,
                nonce,
                gateway: None,
            };
            let balances = sample_balances_standalone(multi_client, &[to]).await?;
            block_for_invoice_standalone(client, action, key, balances).await
        }
        Command::DirectInflow {
            amount,
            to,
            fee_cap,
            nonce,
        } => {
            validate_nonce_cli(&nonce)?;
            let policy = client.get_policy().await.map_err(service_err_to_exit)?;
            let to = resolve_fed_standalone(
                parse_fed_opt(to.as_deref())?,
                policy.spending_fed,
                joined_ids,
            )?;
            let fee_cap = fee_cap.map(Msat).unwrap_or(policy.max_fee);
            let key = direct_inflow_nonce_key(to, Msat(amount), &nonce);
            let action = Action::DirectInflow {
                to,
                amount: Msat(amount),
                fee_cap,
            };
            let balances = sample_balances_standalone(multi_client, &[to]).await?;
            block_for_invoice_standalone(client, action, key, balances).await
        }
        Command::AwaitReceive { key, timeout } => {
            await_standalone(
                client,
                journal,
                multi_client,
                AwaitVerb::Receive,
                key,
                timeout,
            )
            .await
        }
        Command::AwaitSend { key, timeout } => {
            await_standalone(client, journal, multi_client, AwaitVerb::Send, key, timeout).await
        }
        Command::AwaitMove { key, timeout } => {
            await_standalone(client, journal, multi_client, AwaitVerb::Move, key, timeout).await
        }
        Command::Reconcile => {
            // Mirror the daemon's `/v1/reconcile` handler exactly: actor-side intent re-drive
            // first (idempotent; the actor registers the re-drive drivers itself), THEN the
            // off-actor O(ledger) ledger repair (§10.3 / TL-4). The repair runs here — AFTER
            // reconcile returns, OUTSIDE the actor's critical section — and its CAS hardening
            // makes it a no-op against any row the actor already terminalized. Best-effort: a
            // repair I/O fault is logged, never fails the button (the re-drive already committed).
            // Standalone is often the ONLY recovery path (walletd is down — that's why the user
            // chose it); omitting the repair here would leave crash-orphaned ledger rows —
            // nonterminal after their intent stopped being pending/awaiting, which the actor
            // re-drive cannot discover — permanently stale in `history`/`show` with no daemon to
            // fix them later.
            let report = client.reconcile().await.map_err(service_err_to_exit)?;
            if let Err(error) = journal.repair_ledger(multi_client).await {
                // Diagnostics go to stderr (the CLI convention), matching the daemon's warn-log.
                eprintln!(
                    "warning: reconcile: off-actor ledger repair faulted; continuing: {error:?}"
                );
            }
            println!(
                "redriven={} awaiters_rehydrated={} executing_normalized={}",
                report.redriven, report.awaiters_rehydrated, report.executing_normalized
            );
            Ok(())
        }
        Command::Health => {
            let inflight = match client.snapshot(SnapshotScope::Registry).await {
                Ok(Snapshot::Registry { drivers }) => drivers,
                _ => 0,
            };
            println!(
                "actor_queue_depth={} inflight_drivers={} scheduler_alive={}",
                client.queue_depth(),
                inflight,
                service
                    .scheduler_liveness()
                    .load(std::sync::atomic::Ordering::Relaxed)
            );
            Ok(())
        }
        Command::Policy { command } => match command {
            PolicyCommand::Get => {
                let policy = client.get_policy().await.map_err(service_err_to_exit)?;
                print_policy(&policy)
            }
            PolicyCommand::Set(flags) => {
                let mut policy = client.get_policy().await.map_err(service_err_to_exit)?;
                apply_policy_set(&mut policy, &flags)?;
                let updated = client
                    .put_policy(policy)
                    .await
                    .map_err(service_err_to_exit)?;
                print_policy(&updated)
            }
        },
        _ => unreachable!("actor_command received a non-actor verb"),
    }
}

/// Build an `OpRequest` for a user-initiated verb (the daemon handlers' `submit_operation` shape).
fn op_request(
    action: Action,
    key: IdempotencyKey,
    occurrence: Occurrence,
    balances: BTreeMap<FederationId, Msat>,
) -> OpRequest {
    OpRequest {
        decision: AllocatorDecision {
            action,
            reason: ReasonCode::UserInitiated,
            occurrence,
            idempotency_key: key,
        },
        actor: Actor::User,
        now_ms: cli_now_ms(),
        balances,
        probe_session_nonce: None,
    }
}

/// Mirror handlers::block_for_invoice: admit the receive/direct-inflow, then BLOCK for its minted
/// BOLT11 under the hard mint deadline. The invoice is the payable result on stdout; a terminal
/// without an invoice is a journaled failure (exit 3); the deadline is a transport timeout (exit 4).
async fn block_for_invoice_standalone(
    client: &WalletClient,
    action: Action,
    key: IdempotencyKey,
    balances: BTreeMap<FederationId, Msat>,
) -> Result<(), CliExit> {
    client
        .decide_op(op_request(action, key.clone(), Occurrence(0), balances))
        .await
        .map_err(service_err_to_exit)?;
    let deadline = Instant::now() + INVOICE_MINT_DEADLINE;
    match client
        .resolve_await(key.clone(), AwaitTarget::InvoiceArtifact, deadline)
        .await
    {
        Ok(AwaitOutcome::Invoice(invoice)) => {
            render::print_value_with_key(&invoice.0, &key.0);
            Ok(())
        }
        Ok(AwaitOutcome::Terminal(_)) => Err(CliExit::Failed(format!(
            "operation {} terminalized without a payable invoice",
            key.0
        ))),
        Err(ServiceError::Timeout) => Err(CliExit::Transport(format!(
            "invoice mint deadline elapsed for {}; settlement continues asynchronously",
            key.0
        ))),
        Err(e) => Err(service_err_to_exit(e)),
    }
}

/// Mirror the daemon's `GET /v1/operations/{key}?wait=true`: park until the operation is terminal
/// (or `--timeout`), then render its terminal state from the ledger row. In a one-shot standalone
/// process the pending intent has no live driver, so re-drive it first (abandon-and-resume, §6a.8).
async fn await_standalone(
    client: &WalletClient,
    journal: &FedimintJournal,
    multi_client: &MultiClient,
    verb: AwaitVerb,
    key: String,
    timeout: u64,
) -> Result<(), CliExit> {
    let key = IdempotencyKey(key);
    client.reconcile().await.map_err(service_err_to_exit)?;
    // Off-actor ledger repair, mirroring the daemon (its scheduler runs this every cycle; a
    // one-shot standalone process has no scheduler). Without it, a crash that left the intent
    // terminal but its ledger row non-terminal would render "not terminal yet" FOREVER below —
    // resolve_await reads intent terminality, the render reads the row. Best-effort like the
    // daemon's: a repair fault must not fail the await (the re-drive above already committed).
    if let Err(error) = journal.repair_ledger(multi_client).await {
        eprintln!("warning: ledger repair failed: {error:?}");
    }
    let deadline = Instant::now() + Duration::from_secs(timeout);
    match client
        .resolve_await(key.clone(), AwaitTarget::Terminal, deadline)
        .await
    {
        Ok(_) => {}
        Err(ServiceError::Timeout) => {
            return Err(CliExit::Transport(format!(
                "await timed out after {timeout}s waiting for operation {} to terminalize",
                key.0
            )))
        }
        Err(ServiceError::NotFound(m)) => return Err(CliExit::Usage(anyhow::anyhow!(m))),
        Err(e) => return Err(service_err_to_exit(e)),
    }
    let record = journal
        .operation(&OperationRef::Key(key.clone()))
        .await
        .map_err(|e| CliExit::Usage(anyhow::anyhow!("reading operation ledger: {e:?}")))?
        .ok_or_else(|| CliExit::Usage(anyhow::anyhow!("no operation found for key {}", key.0)))?;
    render::await_terminal(
        verb,
        kind_and_amount(&record.kind).0,
        record_status_dto(record.status),
        record.error.as_deref(),
        &key.0,
    )
}

/// Mirror handlers::resolve_fed: the explicit `--fed`/`--to`/`--from`, else the policy pin, else the
/// sole joined federation. An unopened fed is left to admission (sample_balances omits it, so a
/// spend refuses cleanly with insufficient-after-reservations rather than admitting an unfunded one).
fn resolve_fed_standalone(
    explicit: Option<FederationId>,
    pin: Option<FederationId>,
    joined: &[FederationId],
) -> Result<FederationId, CliExit> {
    let chosen = match explicit.or(pin) {
        Some(id) => id,
        None => match joined {
            [only] => *only,
            [] => {
                return Err(CliExit::Refused(
                    "no federations joined; run `join <invite>` first".to_owned(),
                ))
            }
            _ => {
                return Err(CliExit::Refused(
                    "multiple federations joined; name one with --fed/--to/--from".to_owned(),
                ))
            }
        },
    };
    ensure_joined_standalone(chosen, joined)?;
    Ok(chosen)
}

fn ensure_joined_standalone(id: FederationId, joined: &[FederationId]) -> Result<(), CliExit> {
    if joined.contains(&id) {
        Ok(())
    } else {
        Err(CliExit::Refused(format!(
            "federation {} is not joined",
            id.to_hex()
        )))
    }
}

/// Mirror handlers::sample_balances: omit unopened feds (admission treats a missing fed as zero),
/// but fail CLOSED on a balance read that faults — never size an admission against a silently-zeroed
/// balance.
async fn sample_balances_standalone(
    mc: &MultiClient,
    feds: &[FederationId],
) -> Result<BTreeMap<FederationId, Msat>, CliExit> {
    let mut balances = BTreeMap::new();
    let open = mc.federations();
    for fed in feds {
        if !open.contains(fed) {
            continue;
        }
        match mc.balance(fed).await {
            Ok(msat) => {
                balances.insert(*fed, msat);
            }
            Err(e) => {
                return Err(CliExit::Usage(anyhow::anyhow!(
                    "reading balance for federation {} failed: {e}",
                    fed.to_hex()
                )))
            }
        }
    }
    Ok(balances)
}

/// Mirror handlers::validate_nonce: non-empty, RFC 3986 unreserved bytes only (the nonce becomes the
/// `{key}` path segment of `/v1/operations/{key}`, so it must be a round-trippable single segment).
fn validate_nonce_cli(nonce: &str) -> Result<(), CliExit> {
    if nonce.is_empty() {
        return Err(CliExit::Refused("nonce must not be empty".to_owned()));
    }
    if !nonce
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~'))
    {
        return Err(CliExit::Refused(
            "nonce must contain only unreserved URL characters (A-Z a-z 0-9 - . _ ~)".to_owned(),
        ));
    }
    Ok(())
}

fn record_status_dto(status: OperationStatus) -> OperationStatusDto {
    match status {
        OperationStatus::Started => OperationStatusDto::Started,
        OperationStatus::Awaiting => OperationStatusDto::Awaiting,
        OperationStatus::Succeeded => OperationStatusDto::Succeeded,
        OperationStatus::Failed => OperationStatusDto::Failed,
    }
}

fn parse_fed_opt(hex: Option<&str>) -> Result<Option<FederationId>, CliExit> {
    match hex {
        Some(h) => Ok(Some(parse_fed_id(h)?)),
        None => Ok(None),
    }
}

/// Map the layered `ServiceError` (spec §6a.6 taxonomy) onto the CLI exit-code taxonomy: Refused =
/// decide-time, nothing journaled (exit 2); timeout and transient service/storage availability =
/// transport (exit 4); not-found is usage/other (exit 1). This mirrors the daemon's 500/503
/// `Failed` responses, which the thin client maps to exit 4. A driver FAILED terminal is surfaced
/// by the await/block-for-invoice paths, not here.
fn service_err_to_exit(error: ServiceError) -> CliExit {
    match error {
        ServiceError::Refused { reason, message } => {
            CliExit::Refused(format!("{message} ({reason:?})"))
        }
        ServiceError::Timeout => CliExit::Transport("operation wait deadline elapsed".to_owned()),
        ServiceError::NotFound(m) => CliExit::Usage(anyhow::anyhow!(m)),
        ServiceError::Storage(m) => CliExit::Transport(format!("storage error: {m}")),
        ServiceError::ShuttingDown | ServiceError::ActorStopped => {
            CliExit::Transport(error.to_string())
        }
    }
}

fn storage_cli(error: wallet_core::ExecError) -> CliExit {
    CliExit::Usage(anyhow::anyhow!("wallet store error: {error:?}"))
}

/// `policy get` output: the stored `Policy` as pretty JSON (shared by client + standalone).
fn print_policy(policy: &Policy) -> Result<(), CliExit> {
    let json = serde_json::to_string_pretty(policy)
        .map_err(|e| CliExit::Usage(anyhow::anyhow!("serializing policy: {e}")))?;
    println!("{json}");
    Ok(())
}

/// Apply `policy set` flag overrides onto a fetched `Policy` (only the named fields change). The
/// resulting struct is PUT whole; validation (contradiction checks) happens server/actor-side.
fn apply_policy_set(policy: &mut Policy, flags: &PolicySetFlags) -> Result<(), CliExit> {
    if let Some(v) = flags.per_fed_cap {
        policy.per_fed_cap = Msat(v);
    }
    if let Some(v) = flags.spending_target {
        policy.spending_target = Msat(v);
    }
    if let Some(v) = flags.standby_target {
        policy.standby_target = Msat(v);
    }
    if let Some(v) = flags.max_fee {
        policy.max_fee = Msat(v);
    }
    if let Some(v) = flags.max_fee_bps_of_move {
        // Out-of-range values (> 10000) are rejected by `Policy::validate()` on the PUT, so the
        // refusal surfaces through the same path as every other policy contradiction.
        policy.max_fee_bps_of_move = v;
    }
    if flags.clear_spending_fed {
        policy.spending_fed = None;
    }
    if let Some(hex) = &flags.spending_fed {
        policy.spending_fed = Some(parse_fed_id(hex)?);
    }
    if flags.clear_standby_fed {
        policy.standby_fed = None;
    }
    if let Some(hex) = &flags.standby_fed {
        policy.standby_fed = Some(parse_fed_id(hex)?);
    }
    if let Some(v) = flags.probe_min_span_secs {
        policy.probe_min_span_secs = v;
    }
    if let Some(v) = flags.probe_min_successes {
        policy.probe_min_successes = v;
    }
    if let Some(v) = flags.probe_ttl_secs {
        policy.probe_ttl_secs = v;
    }
    if let Some(v) = flags.probe_amount {
        policy.probe_amount = Msat(v);
    }
    if let Some(v) = flags.max_probe_attempts_per_week {
        policy.max_probe_attempts_per_week = v;
    }
    if let Some(v) = flags.max_probe_spend_per_week {
        policy.max_probe_spend_per_week = Msat(v);
    }
    if let Some(v) = flags.base_interval_secs {
        policy.base_interval_secs = v;
    }
    if let Some(v) = flags.min_interval_secs {
        policy.min_interval_secs = v;
    }
    if let Some(v) = flags.evacuation_lead_secs {
        policy.evacuation_lead_secs = v;
    }
    if let Some(v) = flags.discover_every_secs {
        policy.discover_every_secs = v;
    }
    if let Some(v) = flags.probe_retry_backoff_secs {
        policy.probe_retry_backoff_secs = v;
    }
    if let Some(v) = flags.probe_refresh_lead_secs {
        policy.probe_refresh_lead_secs = v;
    }
    if let Some(v) = flags.max_auto_joins_per_week {
        policy.max_auto_joins_per_week = v;
    }
    if let Some(v) = flags.auto_join_lifetime_cap {
        policy.auto_join_lifetime_cap = v;
    }
    if let Some(v) = flags.max_candidates_per_pass {
        policy.max_candidates_per_pass = v;
    }
    if let Some(v) = flags.per_preview_timeout_secs {
        policy.per_preview_timeout_secs = v;
    }
    if let Some(v) = flags.discover_pass_deadline_secs {
        policy.discover_pass_deadline_secs = v;
    }
    if let Some(v) = flags.auto_join {
        policy.auto_join = v;
    }
    if let Some(v) = flags.require_mainnet {
        policy.require_mainnet = v;
    }
    Ok(())
}

/// `history` (§11): scan the ledger newest-first, apply the filters, then take `--limit` (the
/// spec's only pagination is the reverse seq scan; a personal wallet's ledger is tiny, §7
/// non-goals). Offline — journal scan only.
async fn run_history(
    journal: &FedimintJournal,
    limit: usize,
    fed: Option<String>,
    actor: Option<ActorFilter>,
    status: Option<StatusFilter>,
    json: bool,
) -> anyhow::Result<()> {
    let fed_filter = fed.as_deref().map(parse_fed_id).transpose()?;
    let rows = journal
        .history(usize::MAX, None)
        .await
        .map_err(|e| anyhow::anyhow!("reading the operation ledger: {e:?}"))?;
    for record in rows
        .into_iter()
        .filter(|r| fed_filter.is_none_or(|f| record_involves_fed(r, f)))
        .filter(|r| actor.is_none_or(|a| a.matches(r.actor)))
        .filter(|r| status.is_none_or(|s| s.matches(r.status)))
        .take(limit)
    {
        if json {
            println!("{}", serde_json::to_string(&record)?);
        } else {
            println!("{}", history_tsv(&record));
        }
    }
    Ok(())
}

/// `show <key|seq>` (§11): resolve one record by correlation key OR numeric seq and print it plus
/// its live linked intent status. Offline — journal scan only.
async fn run_show(journal: &FedimintJournal, reference: String, json: bool) -> anyhow::Result<()> {
    // A numeric reference is a seq; anything else is a correlation key (keys are always
    // `<verb>:…`-prefixed, never bare digits).
    let sel = match reference.parse::<u64>() {
        Ok(seq) => OperationRef::Seq(seq),
        Err(_) => OperationRef::Key(IdempotencyKey(reference.clone())),
    };
    let Some(record) = journal
        .operation(&sel)
        .await
        .map_err(|e| anyhow::anyhow!("reading the operation ledger: {e:?}"))?
    else {
        anyhow::bail!("no operation found for {reference:?}");
    };
    if json {
        println!("{}", serde_json::to_string(&record)?);
    } else {
        print_show_record(&record);
        // The linked intent status, read live (intent-keyed rows only; `-` otherwise).
        let intent_status = journal
            .get(&record.correlation_key)
            .await
            .ok()
            .flatten()
            .map(|i| i.status);
        println!(
            "linked_intent_status: {}",
            intent_status.map_or("-", intent_status_tag)
        );
    }
    Ok(())
}

async fn run_candidates(
    journal: &FedimintJournal,
    state: Option<CandidateStateArg>,
    json: bool,
) -> anyhow::Result<()> {
    let rows = candidate_rows(journal, state).await?;
    if json {
        let values = rows
            .iter()
            .map(|(id, record)| {
                serde_json::json!({
                    "id": id.to_hex(),
                    "invite": record.invite.to_string(),
                    "source": discovery_source_tag(record.source),
                    "discovered_at_ms": record.discovered_at_ms,
                    "structural": structural_tag(&record.structural),
                    "structural_checked_at_ms": record.structural_checked_at_ms,
                    "state": candidate_state_tag(record.state),
                    "updated_at_ms": record.updated_at_ms,
                })
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string(&values)?);
    } else {
        for (id, record) in rows {
            println!("{}", candidate_tsv(id, &record));
        }
    }
    Ok(())
}

async fn run_approve(journal: &FedimintJournal, fed: String) -> Result<(), CliExit> {
    let id = parse_fed_id(&fed).map_err(CliExit::from)?;
    match journal.get_candidate(&id).await.map_err(storage_cli)? {
        None => {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "candidate {} was not found",
                id.to_hex()
            )))
        }
        Some(candidate) if candidate.state != CandidateState::AutoJoined => {
            return Err(CliExit::Refused(format!(
                "candidate {} is {:?}, not AutoJoined",
                id.to_hex(),
                candidate.state
            )))
        }
        Some(_) => {}
    }
    let key = approve_candidate(journal, id, cli_now_ms(), &cli_nonce())
        .await
        .map_err(|error| match error {
            // A concurrent approval can win after the state check, just as in the daemon handler.
            wallet_core::ExecError::Permanent(message) => CliExit::Refused(message),
            error => storage_cli(error),
        })?;
    println!("{}", id.to_hex());
    eprintln!("key: {}", key.0);
    Ok(())
}

async fn candidate_rows(
    journal: &FedimintJournal,
    state: Option<CandidateStateArg>,
) -> anyhow::Result<Vec<(FederationId, wallet_fedimint::CandidateRecord)>> {
    let mut rows = journal
        .list_candidates()
        .await
        .map_err(|e| anyhow::anyhow!("reading candidate registry: {e:?}"))?;
    rows.retain(|(_, record)| state.is_none_or(|filter| filter.matches(record.state)));
    rows.sort_by_key(|(_, record)| std::cmp::Reverse((record.updated_at_ms, record.id)));
    Ok(rows)
}

async fn approve_candidate(
    journal: &FedimintJournal,
    id: FederationId,
    now_ms: u64,
    nonce: &str,
) -> Result<IdempotencyKey, wallet_core::ExecError> {
    let key = IdempotencyKey(format!("approve:{}:{nonce}", id.to_hex()));
    journal
        .approve_auto_joined_candidate(id, &key, now_ms)
        .await?;
    Ok(key)
}

fn build_discover_sources(
    selected: Vec<DiscoverSourceArg>,
    observer_url: String,
    invites: Vec<String>,
) -> anyhow::Result<Vec<Box<dyn CandidateSource>>> {
    let selected = if selected.is_empty() {
        if invites.is_empty() {
            vec![DiscoverSourceArg::Observer]
        } else {
            vec![DiscoverSourceArg::Manual]
        }
    } else {
        selected
    };
    let mut sources: Vec<Box<dyn CandidateSource>> = Vec::new();
    if selected.contains(&DiscoverSourceArg::Manual) {
        anyhow::ensure!(
            !invites.is_empty(),
            "--source manual requires at least one --invite"
        );
        let invites = invites
            .iter()
            .map(|invite| InviteCode::from_str(invite))
            .collect::<Result<Vec<_>, _>>()?;
        sources.push(Box::new(ManualSource::from_invites(invites)));
    } else if !invites.is_empty() {
        anyhow::bail!("--invite requires --source manual, or omit --source to select manual");
    }
    if selected.contains(&DiscoverSourceArg::Observer) {
        sources.push(Box::new(ObserverSource::new(observer_url)));
    }
    anyhow::ensure!(!sources.is_empty(), "no discovery sources configured");
    Ok(sources)
}

fn discover_summary_lines(report: &DiscoverReport) -> Vec<String> {
    let mut lines = report
        .sources
        .iter()
        .map(source_summary_line)
        .collect::<Vec<_>>();
    lines.push(auto_join_summary_line(&report.auto_join));
    lines.push(discover_progress_summary_line(report));
    lines
}

/// Build the `discover --json` payload from the lowercase-tag vocabulary the TSV summary and
/// `candidates --json` already use (`source: "observer"`, `status: "ok"`/`"failed:reason"`), so a
/// machine consumer sees ONE encoding of these concepts across every command — not the derive's
/// PascalCase/adjacently-tagged enum form.
fn discover_report_json(report: &DiscoverReport) -> serde_json::Value {
    serde_json::json!({
        "sources": report
            .sources
            .iter()
            .map(|source| serde_json::json!({
                "source": discovery_source_tag(source.source),
                "status": source_status_tag(&source.status),
                "found": source.found,
                "structurally_passed": source.structurally_passed,
                "rejected": source.rejected,
            }))
            .collect::<Vec<_>>(),
        "auto_join": {
            "considered": report.auto_join.considered,
            "joined": report.auto_join.joined,
            "blocked_concurrent": report.auto_join.blocked_concurrent,
            "blocked_weekly": report.auto_join.blocked_weekly,
            "blocked_lifetime": report.auto_join.blocked_lifetime,
        },
        "progress": {
            "backlog": report.progress.backlog,
            "deferred": report.progress.deferred,
            "attempted": report.progress.attempted,
            "wrapped": report.progress.wrapped,
            "next_cursor": report
                .progress
                .next_cursor
                .map(|cursor| cursor.to_hex()),
        },
    })
}

fn source_summary_line(report: &DiscoverSourceReport) -> String {
    format!(
        "source={} status={} found={} passed={} rejected={}",
        discovery_source_tag(report.source),
        source_status_tag(&report.status),
        report.found,
        report.structurally_passed,
        report.rejected
    )
}

fn auto_join_summary_line(report: &AutoJoinReport) -> String {
    format!(
        "autojoin considered={} joined={} blocked_concurrent={} blocked_weekly={} blocked_lifetime={}",
        report.considered,
        report.joined,
        report.blocked_concurrent,
        report.blocked_weekly,
        report.blocked_lifetime
    )
}

fn discover_progress_summary_line(report: &DiscoverReport) -> String {
    let next_cursor = report
        .progress
        .next_cursor
        .map(|cursor| cursor.to_hex())
        .unwrap_or_else(|| "none".to_owned());
    format!(
        "progress backlog={} deferred={} attempted={} wrapped={} next_cursor={}",
        report.progress.backlog,
        report.progress.deferred,
        report.progress.attempted,
        report.progress.wrapped,
        next_cursor
    )
}

/// Load the persisted standing instruction for a standalone `tick` / override-`status`, then
/// layer the invocation-only flags without writing the row (§6a.6's DRY ruling).
async fn build_standalone_tick_policy(
    journal: &FedimintJournal,
    flags: &PolicyFlags,
    joined_ids: &[FederationId],
    open_ids: &[FederationId],
) -> anyhow::Result<TickPolicy> {
    let stored = journal
        .get_policy()
        .await
        .map_err(|error| anyhow::anyhow!("reading stored policy: {error:?}"))?
        .unwrap_or_default();
    build_tick_policy(&stored, flags, joined_ids, open_ids)
}

/// Build a [`TickPolicy`] from the supplied standing instruction plus this invocation's flags:
/// each supplied flag overrides the stored field, and each designation flag is validated as a
/// joined+open federation.
fn build_tick_policy(
    stored: &Policy,
    flags: &PolicyFlags,
    joined_ids: &[FederationId],
    open_ids: &[FederationId],
) -> anyhow::Result<TickPolicy> {
    // §5.3: make the snapshot's clock honest for any future time-aware `decide()` logic.
    // Unix SECONDS from the wall clock; a pre-epoch clock degrades to 0 rather than
    // failing the tick. (The probe sources its own `now` for shutdown derivation — this
    // is independent of that.)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut policy = TickPolicy::from(stored);
    policy.now = now;
    if let Some(v) = flags.per_fed_cap {
        policy.per_fed_cap = Msat(v);
    }
    if let Some(v) = flags.spending_target {
        policy.target_spending_balance = Msat(v);
    }
    if let Some(v) = flags.standby_target {
        policy.standby_target = Msat(v);
    }
    if let Some(v) = flags.max_fee {
        policy.max_fee = Msat(v);
    }
    if let Some(v) = flags.max_fee_bps_of_move {
        policy.max_fee_bps_of_move = v;
    }
    policy.occurrence = Occurrence(flags.occurrence);
    if let Some(gate) = gate_policy_override(flags) {
        policy.probe_gate_policy = gate;
    }
    if let Some(hex) = flags.spending.as_deref() {
        policy.spending_fed = Some(select_fed(joined_ids, open_ids, Some(hex))?);
    }
    if let Some(hex) = flags.standby.as_deref() {
        policy.standby_fed = Some(select_fed(joined_ids, open_ids, Some(hex))?);
    }
    // Reject pinning both roles to the SAME fed: the allocator treats a self-fund as a no-op,
    // so this would silently produce no rebalance. Mirror `move`'s `from == to` guard and fail
    // with a clear diagnostic. (Pinning only one role is fine — `build_snapshot` auto-designates
    // a DISTINCT counterpart for the other.)
    if let (Some(spending), Some(standby)) = (policy.spending_fed, policy.standby_fed) {
        anyhow::ensure!(
            spending != standby,
            "--spending and --standby must be different federations (a shared fed is a no-op rebalance)"
        );
    }
    Ok(policy)
}

/// Print each allocator decision on its own `decision: …` line, or `decisions: none` when the
/// tick/dry-run produced no decisions.
fn print_decisions(decisions: &[AllocatorDecision]) {
    if decisions.is_empty() {
        println!("decisions: none");
        return;
    }
    for decision in decisions {
        println!("decision: {}", describe_decision(decision));
    }
}

/// The non-zero-exit message for a `tick` whose apply did not settle every executable decision,
/// or `None` when it did. A tick is a money operation, so — like `move`/`await-move`/
/// `direct-inflow` — it must exit NON-ZERO on any failed decision, including an existing terminal
/// `Failed` intent that `apply` skips rather than resurrects. The executor logs each failing
/// intent's key + reason to stderr.
fn tick_apply_failure(summary: &ExecutionSummary) -> Option<String> {
    (summary.failed > 0 || summary.terminal_failed_skipped > 0).then(|| {
        format!(
            "tick: {} decision(s) did not apply (performed={} skipped={} failed={} \
             terminal_failed_skipped={} retryable={}); check stderr for per-intent reasons. \
             Retryable failures ({} of failed) can be re-driven with reconcile or the same \
             --occurrence; terminal Failed intents require correcting the input/route and \
             starting a fresh --occurrence",
            summary.failed + summary.terminal_failed_skipped,
            summary.performed,
            summary.skipped,
            summary.failed,
            summary.terminal_failed_skipped,
            summary.retryable,
            summary.retryable
        )
    })
}

/// A one-line human description of an allocator decision (its action + reason). The advisory
/// `RefuseInflow` action is surfaced here even though `apply` never executes it.
fn describe_decision(decision: &AllocatorDecision) -> String {
    match &decision.action {
        Action::Move {
            from,
            to,
            amount,
            fee_cap,
        } => format!(
            "move {} msat {} -> {} (fee_cap {} msat, reason {:?})",
            amount.0,
            from.to_hex(),
            to.to_hex(),
            fee_cap.0,
            decision.reason
        ),
        Action::DirectInflow {
            to,
            amount,
            fee_cap,
        } => format!(
            "direct-inflow {} msat -> {} (fee_cap {} msat, reason {:?})",
            amount.0,
            to.to_hex(),
            fee_cap.0,
            decision.reason
        ),
        Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
        } => format!(
            "evacuate {} msat {} -> {} (fee_cap {} msat, reason {:?})",
            amount.0,
            from.to_hex(),
            to.to_hex(),
            fee_cap.0,
            decision.reason
        ),
        Action::RefuseInflow { fed, reason, .. } => {
            format!("refuse-inflow {} (reason {reason:?})", fed.to_hex())
        }
        Action::Pay { from, amount, .. } => {
            format!("pay {} msat from {}", amount.0, from.to_hex())
        }
        Action::Receive { to, amount, .. } => {
            format!("receive {} msat into {}", amount.0, to.to_hex())
        }
        Action::Join { federation, .. } => format!("join {}", federation.to_hex()),
    }
}

/// A one-line human description of a federation's scored view for `status`: its designated role
/// (if any), fundability verdict, rank, spendable balance, and probe/health flags.
fn describe_scored(
    scored: &ScoredFed,
    spending: Option<FederationId>,
    standby: Option<FederationId>,
) -> String {
    let role = if Some(scored.id) == spending {
        " [spending]"
    } else if Some(scored.id) == standby {
        " [standby]"
    } else {
        ""
    };
    // `eligible` is the POST-GATE fundability the tick applies (§5.1.3 active-probe gate + pin
    // override), NOT the raw scorer verdict — so an AutoJoined fed the tick would refuse never
    // shows `eligible=true`. `active_probe`/`reasons` below explain any gap from the scorer view.
    format!(
        "{}{role} eligible={} rank={} spendable={} msat probed_ok={} healthy={} active_probe={} \
         reasons={:?}",
        scored.id.to_hex(),
        scored.gated_eligible,
        scored.verdict.rank_score,
        scored.status.balance.spendable.0,
        scored.status.probed_ok,
        scored.status.healthy,
        scored.active_probe.map_or("-", active_probe_label),
        scored.verdict.reasons,
    )
}

/// The stable lowercase label for an active-probe verdict (§5.0.6's
/// `active_probe=passed|never|expired|…` vocabulary).
fn active_probe_label(verdict: ActiveProbeVerdict) -> &'static str {
    match verdict {
        ActiveProbeVerdict::Passed => "passed",
        ActiveProbeVerdict::NeverProbed => "never",
        ActiveProbeVerdict::Insufficient => "insufficient",
        ActiveProbeVerdict::Expired => "expired",
        ActiveProbeVerdict::Failed => "failed",
        ActiveProbeVerdict::FailedSinceLastPass => "failed-since-pass",
    }
}

/// §5.0.7 source resolution when `--from` is omitted: exactly TWO joined federations of
/// which the candidate is one ⇒ the other (the common probe topology); anything else is
/// refused — deterministic, and deliberately NOT coupled to the tick's designation logic
/// (a probe must not silently ride whatever auto-designation picked this run).
/// Resolve the probe's spending federation `S` (§5.0.7), RESUME-AWARE: an in-flight probe
/// carries its own fixed source, so a resume must reuse it rather than re-infer — otherwise a
/// two-fed inference (or a different `--from`) could point the resumed legs at the wrong
/// source. Precedence: an in-flight session's `from` (a resume — the session's source is
/// authoritative and any conflicting `--from` is refused) → an explicit `--from` → the
/// two-fed auto-rule.
async fn probe_source(
    journal: &FedimintJournal,
    joined: &[FederationId],
    candidate: FederationId,
    from: Option<&str>,
) -> anyhow::Result<FederationId> {
    let explicit = from.map(parse_fed_id).transpose()?;

    if let Some(session) = journal
        .probe_record(&candidate)
        .await
        .map_err(ledger_err)?
        .and_then(|rec| rec.in_flight)
    {
        if let Some(explicit) = explicit {
            anyhow::ensure!(
                explicit == session.from,
                "federation {} has an in-flight probe from {}; --from {} conflicts — omit \
                 --from to resume it, or let it finish first",
                candidate.to_hex(),
                session.from.to_hex(),
                explicit.to_hex()
            );
        }
        return Ok(session.from);
    }

    if let Some(explicit) = explicit {
        return Ok(explicit);
    }
    if let [a, b] = joined {
        if *a == candidate {
            return Ok(*b);
        }
        if *b == candidate {
            return Ok(*a);
        }
    }
    anyhow::bail!(
        "cannot infer the probe source: pass --from <spending-fed-hex> (auto-resolution \
         applies only when exactly two federations are joined and <fed> is one of them)"
    )
}

/// Refuse a resume whose `--amount`/`--fee-cap` conflict with the in-flight session's
/// stored money params (§5.0.7): a resume runs the legs with the SESSION's values, so a
/// differing flag would mislead the operator about what money probe ran. Omitting the flags
/// (or matching them) is fine; no in-flight session makes this a no-op.
async fn reject_conflicting_probe_money_flags(
    journal: &FedimintJournal,
    candidate: FederationId,
    amount: Option<u64>,
    fee_cap: Option<u64>,
) -> anyhow::Result<()> {
    if let Some(session) = journal
        .probe_record(&candidate)
        .await
        .map_err(ledger_err)?
        .and_then(|rec| rec.in_flight)
    {
        if let Some(a) = amount {
            anyhow::ensure!(
                a == session.amount_msat,
                "federation {} has an in-flight probe of {} msat; --amount {} conflicts — \
                 omit it to resume, or let the probe finish first",
                candidate.to_hex(),
                session.amount_msat,
                a
            );
        }
        if let Some(f) = fee_cap {
            anyhow::ensure!(
                f == session.leg_fee_cap_msat,
                "federation {} has an in-flight probe with a {} msat leg fee cap; --fee-cap \
                 {} conflicts — omit it to resume, or let the probe finish first",
                candidate.to_hex(),
                session.leg_fee_cap_msat,
                f
            );
        }
    }
    Ok(())
}

/// Build the probe [`ProbePolicy`] from the §5.0.7 flags. The verdict-window flags exist
/// so a smoke can SHRINK the window and are clamped SHRINK-ONLY: `--ttl-secs` /
/// `--min-span-secs` above their defaults are rejected — §5.0.4's durable retention keeps
/// only sub-default-`ttl` attempts (plus the newest success/attempt), so a larger window
/// could not be computed from the history it advertises.
fn build_probe_policy(
    amount: Option<u64>,
    fee_cap: Option<u64>,
    min_successes: Option<u32>,
    min_span_secs: Option<u64>,
    ttl_secs: Option<u64>,
) -> anyhow::Result<ProbePolicy> {
    let defaults = ProbePolicy::default();
    let min_span_ms = min_span_secs.map(|s| s.saturating_mul(1000));
    let ttl_ms = ttl_secs.map(|s| s.saturating_mul(1000));
    if let Some(ttl) = ttl_ms {
        anyhow::ensure!(
            ttl <= defaults.ttl_ms,
            "--ttl-secs {} exceeds the default {}s: durable probe retention keeps only \
             attempts younger than the default ttl (plus the newest success/attempt), so a \
             larger verdict window cannot be computed from stored history (shrink-only)",
            ttl / 1000,
            defaults.ttl_ms / 1000
        );
    }
    if let Some(span) = min_span_ms {
        anyhow::ensure!(
            span <= defaults.min_span_ms,
            "--min-span-secs {} exceeds the default {}s: durable probe retention is sized \
             to the default verdict window, so a larger span cannot be computed from stored \
             history (shrink-only)",
            span / 1000,
            defaults.min_span_ms / 1000
        );
    }
    let resolved_span = min_span_ms.unwrap_or(defaults.min_span_ms);
    let resolved_ttl = ttl_ms.unwrap_or(defaults.ttl_ms);
    // `Passed` needs qualifying successes spanning `min_span` whose NEWEST is within `ttl`;
    // if `ttl < span` that is unsatisfiable and the probe would report `insufficient`
    // forever. Reject the contradiction at parse time (defaults satisfy ttl > span).
    anyhow::ensure!(
        resolved_ttl >= resolved_span,
        "--ttl-secs {} is shorter than --min-span-secs {}: no verdict can ever pass \
         (a sustained span cannot fit inside a shorter ttl window)",
        resolved_ttl / 1000,
        resolved_span / 1000
    );
    Ok(ProbePolicy {
        amount_msat: amount.unwrap_or(defaults.amount_msat),
        leg_fee_cap_msat: fee_cap.unwrap_or(defaults.leg_fee_cap_msat),
        min_successes: min_successes.unwrap_or(defaults.min_successes),
        min_span_ms: resolved_span,
        ttl_ms: resolved_ttl,
    })
}

/// A federation id as hex, or the literal `none` for an undesignated slot.
fn opt_fed_hex(id: Option<FederationId>) -> String {
    id.map_or_else(|| "none".to_string(), |fed| fed.to_hex())
}

/// Select the federation to act on: the explicit `--to`/`--fed` hex if given (and joined),
/// else the sole joined federation. Errors clearly when the choice is empty or ambiguous.
fn select_fed(
    joined_feds: &[FederationId],
    open_feds: &[FederationId],
    explicit: Option<&str>,
) -> anyhow::Result<FederationId> {
    match explicit {
        Some(hex) => {
            let id = parse_fed_id(hex)?;
            anyhow::ensure!(
                joined_feds.contains(&id),
                "federation {} not joined",
                id.to_hex()
            );
            require_open(&id, open_feds)?;
            Ok(id)
        }
        None => match joined_feds {
            [only] => {
                require_open(only, open_feds)?;
                Ok(*only)
            }
            [] => anyhow::bail!("no federations joined; run `join <invite>` first"),
            _ => {
                anyhow::bail!("multiple federations joined; select one with --to/--fed <fed-hex>")
            }
        },
    }
}

fn require_open(id: &FederationId, open_feds: &[FederationId]) -> anyhow::Result<()> {
    anyhow::ensure!(
        open_feds.contains(id),
        "federation {} is joined but failed to open",
        id.to_hex()
    );
    Ok(())
}

/// Joined federations that failed to open this run (§15.8): the joined registry minus the
/// successfully-opened set, in registry order. PURE — `open_all` is best-effort, so a joined fed
/// that vanished from the open set would otherwise silently drop out of balance totals and every
/// money decision. `balance`/`tick`/`status` use this to fail loudly instead.
fn unopened_feds(joined: &[FederationId], open: &[FederationId]) -> Vec<FederationId> {
    joined
        .iter()
        .copied()
        .filter(|id| !open.contains(id))
        .collect()
}

/// §15.8: refuse a money-driving verb (a `tick`) when any joined fed failed to open — a partial
/// world-view must not drive money decisions, the same doctrine as `missing_pinned_feds`. The
/// non-zero exit lets a scheduler gating on the exit code catch it; the message goes to stderr.
fn refuse_on_partial_open(joined: &[FederationId], open: &[FederationId]) -> anyhow::Result<()> {
    let unopened = unopened_feds(joined, open);
    anyhow::ensure!(
        unopened.is_empty(),
        "tick refused: {} joined federation(s) failed to open ({}); a partial world-view must \
         not drive money decisions — repair the fed partition(s) and retry (check stderr for the \
         open error)",
        unopened.len(),
        hex_list(&unopened)
    );
    Ok(())
}

/// The hard per-fed balance cap for an OPERATOR verb (§15.2): the ADR-0018 v1 default unless
/// `--allow-over-cap` was passed, in which case the cap is DISABLED (`None`) — an explicit
/// override, never silence.
fn operator_hard_cap(allow_over_cap: bool) -> Option<Msat> {
    if allow_over_cap {
        None
    } else {
        Some(TickPolicy::default().per_fed_cap)
    }
}

/// Comma-join federation ids as hex for a diagnostic message.
fn hex_list(ids: &[FederationId]) -> String {
    ids.iter()
        .map(|id| id.to_hex())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Parse a 64-char hex federation id into `wallet_core::FederationId`, reusing fedimint's
/// own validated parser (the CLI already depends on fedimint-core).
fn parse_fed_id(hex: &str) -> anyhow::Result<FederationId> {
    use fedimint_core::BitcoinHash as _;
    let id = fedimint_core::config::FederationId::from_str(hex)
        .map_err(|e| anyhow::anyhow!("invalid federation id {hex:?}: {e}"))?;
    Ok(FederationId(id.0.to_byte_array()))
}

/// Lowercase hex of arbitrary bytes (op ids, preimages) — matches `FederationId::to_hex`'s
/// format so ids round-trip through the CLI without pulling in a `hex` dependency.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a `String` is infallible.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Load the wallet's mnemonic from `db`, or generate + persist a new one. Mirrors
/// `fedimint-cli`'s own `load_or_generate_mnemonic`, verified against
/// `~/p/fedimint/fedimint-cli/src/lib.rs`.
///
/// §15.11: use `load_decodable_client_secret_opt`, which cleanly separates the three cases —
/// ABSENT (`Ok(None)`) → first run, generate + persist; PRESENT + decodable (`Ok(Some)`) → reuse;
/// PRESENT but corrupt (`Err`) → ABORT naming the decode failure. The old `if let Ok(..)` form
/// collapsed the last two, so a corrupt row fell through to the generate path and surfaced only as
/// a misleading "already exists, cannot overwrite" abort from the SDK's overwrite guard (no silent
/// regeneration is possible either way — the SDK refuses to overwrite an existing secret).
async fn load_or_generate_mnemonic(db: &Database) -> anyhow::Result<Mnemonic> {
    match Client::load_decodable_client_secret_opt::<Vec<u8>>(db).await {
        Ok(Some(entropy)) => Ok(Mnemonic::from_entropy(&entropy)?),
        Ok(None) => {
            let mnemonic = Bip39RootSecretStrategy::<12>::random(&mut rand::thread_rng());
            Client::store_encodable_client_secret(db, mnemonic.to_entropy()).await?;
            Ok(mnemonic)
        }
        Err(e) => {
            Err(e.context("wallet client secret is present in the database but failed to decode"))
        }
    }
}

// --- operation-ledger recording + display helpers (spec §9-§11) -----------------------------

/// Map a ledger-write [`wallet_core::ExecError`] into `anyhow` for a `?` that must fail the
/// command — the pre-side-effect `record_started` writes (§10.1): if we cannot even open the
/// history row, do NOT proceed to the money op (nothing has happened yet).
fn ledger_err(e: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("ledger write failed: {e:?}")
}

/// Wall-clock unix millis for a caller-provided ledger timestamp (§9.4). Display material.
fn cli_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A fresh 128-bit nonce as 32 lowercase-hex chars for a per-attempt ledger key (§10.1 — a
/// 32-bit nonce risks birthday collisions over a wallet lifetime). The CLI owns randomness.
fn cli_nonce() -> String {
    use rand::RngCore as _;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    to_hex(&bytes)
}

impl ActorFilter {
    fn matches(self, actor: Actor) -> bool {
        matches!(
            (self, actor),
            (ActorFilter::User, Actor::User) | (ActorFilter::Agent, Actor::Agent { .. })
        )
    }

    /// Match a wire `OperationView.actor` tag (`"user"` / `"agent:<n>"`), for client-mode filtering.
    pub(crate) fn matches_actor_tag(self, tag: &str) -> bool {
        match self {
            ActorFilter::User => tag == "user",
            ActorFilter::Agent => tag.starts_with("agent"),
        }
    }
}

impl StatusFilter {
    fn matches(self, status: OperationStatus) -> bool {
        matches!(
            (self, status),
            (StatusFilter::Started, OperationStatus::Started)
                | (StatusFilter::Awaiting, OperationStatus::Awaiting)
                | (StatusFilter::Succeeded, OperationStatus::Succeeded)
                | (StatusFilter::Failed, OperationStatus::Failed)
        )
    }

    /// Match a wire `OperationStatusDto`, for client-mode filtering.
    pub(crate) fn matches_status_dto(self, status: OperationStatusDto) -> bool {
        matches!(
            (self, status),
            (StatusFilter::Started, OperationStatusDto::Started)
                | (StatusFilter::Awaiting, OperationStatusDto::Awaiting)
                | (StatusFilter::Succeeded, OperationStatusDto::Succeeded)
                | (StatusFilter::Failed, OperationStatusDto::Failed)
        )
    }
}

impl CandidateStateArg {
    fn matches(self, state: CandidateState) -> bool {
        matches!(
            (self, state),
            (CandidateStateArg::Discovered, CandidateState::Discovered)
                | (CandidateStateArg::Autojoined, CandidateState::AutoJoined)
                | (
                    CandidateStateArg::Userapproved,
                    CandidateState::UserApproved
                )
                | (CandidateStateArg::Rejected, CandidateState::Rejected)
        )
    }

    /// The lowercase tag matching a wire `CandidateView.state`, for client-mode filtering.
    pub(crate) fn tag(self) -> &'static str {
        match self {
            CandidateStateArg::Discovered => "discovered",
            CandidateStateArg::Autojoined => "autojoined",
            CandidateStateArg::Userapproved => "userapproved",
            CandidateStateArg::Rejected => "rejected",
        }
    }
}

/// Whether a record involves `fed` (for `history --fed`): a `Move` matches either endpoint.
fn record_involves_fed(record: &OperationRecord, fed: FederationId) -> bool {
    match &record.kind {
        OperationKind::Join { fed: f } | OperationKind::Refusal { fed: f, .. } => *f == fed,
        OperationKind::Receive { fed: f, .. } | OperationKind::Pay { fed: f, .. } => *f == fed,
        OperationKind::DirectInflow { to, .. } => *to == fed,
        OperationKind::Move { from, to, .. } => *from == fed || *to == fed,
        // Either endpoint matches, so `history --fed <source>` stays complete even for a
        // pair-scoped route failure whose move intents never existed (§5.0.5).
        OperationKind::Probe { fed: f, from, .. } => *f == fed || *from == fed,
        OperationKind::Approve { fed: f } => *f == fed,
        OperationKind::Tick { .. }
        | OperationKind::Discover { .. }
        | OperationKind::AutoJoin { .. } => false,
    }
}

/// One TAB-separated `history` row (§11); unknown fields render as `-`.
fn history_tsv(r: &OperationRecord) -> String {
    let (kind, amount) = kind_and_amount(&r.kind);
    [
        r.seq.to_string(),
        rfc3339_from_millis(r.updated_at_ms),
        kind.to_owned(),
        status_tag(r.status).to_owned(),
        opt_msat(amount),
        opt_msat(r.fees.receive_fee),
        opt_msat(r.fees.send_fee_quoted),
        actor_tag(r.actor),
        reason_tag(r.reason).to_owned(),
        r.correlation_key.0.clone(),
    ]
    .join("\t")
}

/// The multi-line `show` view (§11): the full record plus kind-specific op ids/gateway.
fn print_show_record(r: &OperationRecord) {
    let (kind, amount) = kind_and_amount(&r.kind);
    println!("seq: {}", r.seq);
    println!("key: {}", r.correlation_key.0);
    println!("kind: {kind}");
    println!(
        "status: {}{}",
        status_tag(r.status),
        if r.repaired { " (repaired)" } else { "" }
    );
    println!("actor: {}", actor_tag(r.actor));
    println!("reason: {}", reason_tag(r.reason));
    println!("created_at: {}", rfc3339_from_millis(r.created_at_ms));
    println!("updated_at: {}", rfc3339_from_millis(r.updated_at_ms));
    println!("amount_msat: {}", opt_msat(amount));
    println!("fee_cap_msat: {}", opt_msat(r.fees.fee_cap));
    println!("receive_fee_msat: {}", opt_msat(r.fees.receive_fee));
    println!("send_fee_quoted_msat: {}", opt_msat(r.fees.send_fee_quoted));
    print_kind_details(&r.kind);
    println!("error: {}", r.error.as_deref().unwrap_or("-"));
}

/// Print the recorded refusal arithmetic (§9.3), one line per figure the deciding site
/// computed. Shared by the standalone `show` (over an `OperationRecord`) and the client `show`
/// (over an `OperationView.refusal`) so both diagnose a refusal identically.
pub(crate) fn print_refusal_diagnostics(diagnostics: &RefusalDiagnostics) {
    if let Some(v) = diagnostics.source {
        println!("source: {}", v.to_hex());
    }
    if let Some(v) = diagnostics.want {
        println!("want_msat: {}", v.0);
    }
    if let Some(v) = diagnostics.available {
        println!("available_msat: {}", v.0);
    }
    if let Some(v) = diagnostics.source_spendable {
        println!("source_spendable_msat: {}", v.0);
    }
    if let Some(v) = diagnostics.max_fee {
        println!("max_fee_msat: {}", v.0);
    }
    if let Some(v) = diagnostics.cap_room {
        println!("cap_room_msat: {}", v.0);
    }
    if let Some(v) = diagnostics.amount {
        // Distinct label: the row's headline `amount_msat` is `-` for a refusal (no operation
        // amount), so a second `amount_msat` here would be a conflicting duplicate key.
        println!("decision_amount_msat: {}", v.0);
    }
    if let Some(v) = diagnostics.min_move {
        println!("min_move_msat: {}", v.0);
    }
}

fn print_kind_details(kind: &OperationKind) {
    match kind {
        OperationKind::Join { fed } => {
            println!("fed: {}", fed.to_hex())
        }
        OperationKind::Refusal { fed, diagnostics } => {
            println!("fed: {}", fed.to_hex());
            print_refusal_diagnostics(diagnostics);
        }
        OperationKind::Receive { op_id, gateway, .. } => {
            println!("op_id: {}", opt_op(op_id));
            println!("gateway: {}", opt_gw(gateway));
        }
        OperationKind::Pay {
            op_id,
            gateway,
            payment_hash,
            ..
        } => {
            println!("op_id: {}", opt_op(op_id));
            println!("gateway: {}", opt_gw(gateway));
            println!(
                "payment_hash: {}",
                payment_hash.map_or_else(|| "-".to_owned(), |h| to_hex(&h))
            );
        }
        OperationKind::DirectInflow {
            to,
            recv_op,
            gateway,
            ..
        } => {
            println!("to: {}", to.to_hex());
            println!("recv_op: {}", opt_op(recv_op));
            println!("gateway: {}", opt_gw(gateway));
        }
        OperationKind::Move {
            from,
            to,
            send_op,
            recv_op,
            gateway,
            evacuation,
            amount: _,
        } => {
            println!("from: {}", from.to_hex());
            println!("to: {}", to.to_hex());
            println!("evacuation: {evacuation}");
            println!("send_op: {}", opt_op(send_op));
            println!("recv_op: {}", opt_op(recv_op));
            println!("gateway: {}", opt_gw(gateway));
        }
        OperationKind::Probe {
            fed,
            from,
            cost_msat,
            amount_msat: _,
        } => {
            println!("fed: {}", fed.to_hex());
            println!("from: {}", from.to_hex());
            println!("cost_msat: {}", opt_msat(*cost_msat));
        }
        OperationKind::Tick {
            occurrence,
            decisions,
            performed,
            failed,
        } => {
            println!("occurrence: {}", occurrence.0);
            println!("decisions: {decisions}");
            println!("performed: {performed}");
            println!("failed: {failed}");
        }
        OperationKind::Discover {
            source,
            status,
            found,
            structurally_passed,
            rejected,
        } => {
            println!("source: {}", discovery_source_tag(*source));
            println!("source_status: {}", source_status_tag(status));
            println!("found: {found}");
            println!("structurally_passed: {structurally_passed}");
            println!("rejected: {rejected}");
        }
        OperationKind::AutoJoin {
            considered,
            joined,
            blocked_concurrent,
            blocked_weekly,
            blocked_lifetime,
        } => {
            println!("considered: {considered}");
            println!("joined: {joined}");
            println!("blocked_concurrent: {blocked_concurrent}");
            println!("blocked_weekly: {blocked_weekly}");
            println!("blocked_lifetime: {blocked_lifetime}");
        }
        OperationKind::Approve { fed } => {
            println!("fed: {}", fed.to_hex());
        }
    }
}

/// `(kind label, headline amount)` for a record (§11): the label vocab + the kind's amount.
fn kind_and_amount(kind: &OperationKind) -> (&'static str, Option<Msat>) {
    match kind {
        OperationKind::Join { .. } => ("join", None),
        OperationKind::Receive {
            amount_invoiced, ..
        } => ("receive", Some(*amount_invoiced)),
        OperationKind::Pay { invoice_amount, .. } => ("pay", *invoice_amount),
        OperationKind::DirectInflow { amount, .. } => ("direct-inflow", Some(*amount)),
        OperationKind::Move {
            amount, evacuation, ..
        } => (
            if *evacuation { "evacuation" } else { "move" },
            Some(*amount),
        ),
        OperationKind::Refusal { .. } => ("refusal", None),
        OperationKind::Probe { amount_msat, .. } => ("probe", Some(*amount_msat)),
        OperationKind::Tick { .. } => ("tick", None),
        OperationKind::Discover { .. } => ("discover", None),
        OperationKind::AutoJoin { .. } => ("autojoin", None),
        OperationKind::Approve { .. } => ("approve", None),
    }
}

fn discovery_source_tag(source: DiscoverySource) -> &'static str {
    match source {
        DiscoverySource::Observer => "observer",
        DiscoverySource::Nostr => "nostr",
        DiscoverySource::Manual => "manual",
    }
}

fn source_status_tag(status: &wallet_core::SourceStatus) -> String {
    match status {
        wallet_core::SourceStatus::Ok => "ok".to_owned(),
        wallet_core::SourceStatus::Failed(reason) => format!("failed:{reason}"),
    }
}

fn candidate_state_tag(state: CandidateState) -> &'static str {
    match state {
        CandidateState::Discovered => "discovered",
        CandidateState::AutoJoined => "autojoined",
        CandidateState::UserApproved => "userapproved",
        CandidateState::Rejected => "rejected",
    }
}

fn structural_tag(structural: &wallet_fedimint::StructuralOutcome) -> String {
    match structural {
        wallet_fedimint::StructuralOutcome::Passed => "passed".to_owned(),
        wallet_fedimint::StructuralOutcome::Rejected(reason) => format!("rejected:{reason}"),
    }
}

fn candidate_tsv(id: FederationId, record: &wallet_fedimint::CandidateRecord) -> String {
    [
        id.to_hex(),
        candidate_state_tag(record.state).to_owned(),
        discovery_source_tag(record.source).to_owned(),
        record.discovered_at_ms.to_string(),
        structural_tag(&record.structural),
        record.structural_checked_at_ms.to_string(),
        record.updated_at_ms.to_string(),
        record.invite.to_string(),
    ]
    .join("\t")
}

fn status_tag(status: OperationStatus) -> &'static str {
    match status {
        OperationStatus::Started => "started",
        OperationStatus::Awaiting => "awaiting",
        OperationStatus::Succeeded => "succeeded",
        OperationStatus::Failed => "failed",
    }
}

fn actor_tag(actor: Actor) -> String {
    match actor {
        Actor::User => "user".to_owned(),
        Actor::Agent { occurrence } => format!("agent:{}", occurrence.0),
    }
}

/// The snake_case reason tag (§11). Mirrors the allocator's private `reason_tag` — this is the
/// display layer, so the small duplication across the module boundary is intentional.
fn reason_tag(reason: ReasonCode) -> &'static str {
    match reason {
        ReasonCode::SpendingBelowTarget => "spending_below_target",
        ReasonCode::StandbyBelowTarget => "standby_below_target",
        ReasonCode::ShutdownNotice => "shutdown_notice",
        ReasonCode::Unhealthy => "unhealthy",
        ReasonCode::OverCap => "over_cap",
        ReasonCode::NotProbed => "not_probed",
        ReasonCode::LowReputation => "low_reputation",
        ReasonCode::UserInitiated => "user_initiated",
        ReasonCode::StandingInstruction => "standing_instruction",
        ReasonCode::ActiveProbe => "active_probe",
    }
}

/// A stable, lowercase label for an intent status (never the `Debug`-rendered `Some(..)` wrapper).
fn status_label(status: Option<IntentStatus>) -> &'static str {
    match status {
        Some(IntentStatus::Pending) => "pending",
        Some(IntentStatus::Executing) => "executing",
        Some(IntentStatus::Awaiting) => "awaiting",
        Some(IntentStatus::Done) => "done",
        Some(IntentStatus::Failed) => "failed",
        None => "unknown",
    }
}

fn intent_status_tag(status: IntentStatus) -> &'static str {
    status_label(Some(status))
}

fn opt_msat(amount: Option<Msat>) -> String {
    amount.map_or_else(|| "-".to_owned(), |m| m.0.to_string())
}

fn opt_op(op: &Option<OperationId>) -> String {
    op.map_or_else(|| "-".to_owned(), |o| to_hex(&o.0))
}

fn opt_gw(gateway: &Option<GatewayUrl>) -> String {
    gateway
        .as_ref()
        .map_or_else(|| "-".to_owned(), |g| g.0.clone())
}

/// RFC3339 UTC (`YYYY-MM-DDThh:mm:ss.mmmZ`) from unix millis, no date-library dependency
/// (Howard Hinnant's civil-from-days algorithm). Display-only; `seq` is the ordering
/// authority, so a skewed clock degrades this string, never the order (§9.4).
fn rfc3339_from_millis(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let millis = ms % 1000;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    // Civil date from days since 1970-01-01 (Hinnant): shift the epoch to 0000-03-01.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Build the §5.1.3 funding-gate probe-policy override from the operator's `PolicyFlags`, or
/// `None` when no gate flag was passed (leaving `TickPolicy`'s conservative default). Only the
/// sustained-WINDOW knobs are tunable; amount/leg_fee_cap stay at the default STRENGTH so real
/// (default-strength) probes qualify. `secs -> ms` uses saturating multiplication.
fn gate_policy_override(flags: &PolicyFlags) -> Option<wallet_core::ProbePolicy> {
    if flags.probe_min_span_secs.is_none()
        && flags.probe_min_successes.is_none()
        && flags.probe_ttl_secs.is_none()
    {
        return None;
    }
    let mut gate = wallet_core::ProbePolicy::default();
    if let Some(v) = flags.probe_min_span_secs {
        gate.min_span_ms = v.saturating_mul(1000);
    }
    if let Some(v) = flags.probe_min_successes {
        gate.min_successes = v;
    }
    if let Some(v) = flags.probe_ttl_secs {
        gate.ttl_ms = v.saturating_mul(1000);
    }
    Some(gate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wallet_core::FeeBreakdown;
    use wallet_fedimint::DiscoverPassProgress;

    #[test]
    fn gate_policy_override_maps_window_flags_or_none() {
        // No gate flag -> None (TickPolicy keeps the conservative default).
        assert!(gate_policy_override(&PolicyFlags::default()).is_none());
        // A window override -> a policy with secs converted to ms; STRENGTH (amount/fee_cap)
        // stays at default so real default-strength probes still qualify.
        let flags = PolicyFlags {
            probe_min_span_secs: Some(1),
            probe_min_successes: Some(2),
            probe_ttl_secs: Some(30),
            ..PolicyFlags::default()
        };
        let gate = gate_policy_override(&flags).expect("override built");
        let default = wallet_core::ProbePolicy::default();
        assert_eq!(gate.min_span_ms, 1_000);
        assert_eq!(gate.min_successes, 2);
        assert_eq!(gate.ttl_ms, 30_000);
        assert_eq!(gate.amount_msat, default.amount_msat, "strength unchanged");
        assert_eq!(
            gate.leg_fee_cap_msat, default.leg_fee_cap_msat,
            "strength unchanged"
        );
    }

    fn fed(byte: u8) -> FederationId {
        FederationId([byte; 32])
    }

    fn exec_err(e: wallet_core::ExecError) -> anyhow::Error {
        anyhow::anyhow!("{e:?}")
    }

    fn test_invite() -> InviteCode {
        InviteCode::from_str(
            "fed11qgqpu8rhwden5te0vejkg6tdd9h8gepwd4cxcumxv4jzuen0duhsqqfqh6nl7sgk72caxfx8khtfnn8y436q3nhyrkev3qp8ugdhdllnh86qmp42pm",
        )
        .expect("valid invite code")
    }

    fn test_candidate(id: FederationId, state: CandidateState) -> wallet_fedimint::CandidateRecord {
        wallet_fedimint::CandidateRecord {
            id,
            invite: test_invite(),
            source: wallet_core::DiscoverySource::Manual,
            discovered_at_ms: 1_700_000_000_000,
            structural: wallet_fedimint::StructuralOutcome::Passed,
            structural_checked_at_ms: 1_700_000_000_100,
            state,
            updated_at_ms: 1_700_000_000_200,
        }
    }

    #[tokio::test]
    async fn approve_flips_autojoined_candidate_and_writes_ledger() -> anyhow::Result<()> {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let journal = FedimintJournal::new(MemDatabase::new().into_database());
        let id = fed(0x30);
        journal
            .put_candidate(&test_candidate(id, CandidateState::AutoJoined))
            .await
            .map_err(exec_err)?;

        let key = approve_candidate(&journal, id, 1_700_000_001_000, "abc")
            .await
            .map_err(exec_err)?;

        let updated = journal
            .get_candidate(&id)
            .await
            .map_err(exec_err)?
            .expect("candidate remains present");
        assert_eq!(updated.state, CandidateState::UserApproved);
        assert_eq!(updated.updated_at_ms, 1_700_000_001_000);

        let row = journal
            .operation(&OperationRef::Key(key))
            .await
            .map_err(exec_err)?
            .expect("approve row recorded");
        assert_eq!(row.actor, Actor::User);
        assert_eq!(row.reason, ReasonCode::UserInitiated);
        assert_eq!(row.status, OperationStatus::Succeeded);
        assert_eq!(row.kind, OperationKind::Approve { fed: id });
        Ok(())
    }

    #[tokio::test]
    async fn approve_refuses_non_autojoined_candidate() -> anyhow::Result<()> {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let journal = FedimintJournal::new(MemDatabase::new().into_database());
        let id = fed(0x31);
        journal
            .put_candidate(&test_candidate(id, CandidateState::Discovered))
            .await
            .map_err(exec_err)?;

        let err = approve_candidate(&journal, id, 1_700_000_001_000, "abc")
            .await
            .expect_err("non-AutoJoined candidates are refused");

        assert!(
            matches!(
                &err,
                wallet_core::ExecError::Permanent(message) if message.contains("not AutoJoined")
            ),
            "{err:?}"
        );
        let unchanged = journal
            .get_candidate(&id)
            .await
            .map_err(exec_err)?
            .expect("candidate remains present");
        assert_eq!(unchanged.state, CandidateState::Discovered);
        assert!(journal
            .operation(&OperationRef::Key(IdempotencyKey(format!(
                "approve:{}:abc",
                id.to_hex()
            ))))
            .await
            .map_err(exec_err)?
            .is_none());

        let cli_error = run_approve(&journal, id.to_hex())
            .await
            .expect_err("standalone approval preserves the refused exit category");
        assert_eq!(cli_error.code(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn candidates_filter_by_state_newest_first() -> anyhow::Result<()> {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let journal = FedimintJournal::new(MemDatabase::new().into_database());
        let mut older = test_candidate(fed(0x32), CandidateState::Discovered);
        older.updated_at_ms = 10;
        let mut newer = test_candidate(fed(0x33), CandidateState::Discovered);
        newer.updated_at_ms = 20;
        let approved = test_candidate(fed(0x34), CandidateState::UserApproved);
        journal.put_candidate(&older).await.map_err(exec_err)?;
        journal.put_candidate(&approved).await.map_err(exec_err)?;
        journal.put_candidate(&newer).await.map_err(exec_err)?;

        let rows = candidate_rows(&journal, Some(CandidateStateArg::Discovered)).await?;

        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![fed(0x33), fed(0x32)]
        );
        assert!(rows
            .iter()
            .all(|(_, rec)| rec.state == CandidateState::Discovered));
        Ok(())
    }

    #[test]
    fn discover_summary_and_json_shape_are_stable() -> anyhow::Result<()> {
        let report = DiscoverReport {
            sources: vec![DiscoverSourceReport {
                source: DiscoverySource::Observer,
                status: wallet_core::SourceStatus::Failed("timeout".into()),
                found: 0,
                structurally_passed: 0,
                rejected: 0,
            }],
            auto_join: AutoJoinReport {
                considered: 2,
                joined: 1,
                blocked_concurrent: 0,
                blocked_weekly: 1,
                blocked_lifetime: 0,
            },
            progress: DiscoverPassProgress {
                next_cursor: Some(fed(0x35)),
                wrapped: false,
                backlog: true,
                attempted: 1,
                deferred: 2,
            },
        };

        let lines = discover_summary_lines(&report);
        assert_eq!(
            lines,
            vec![
                "source=observer status=failed:timeout found=0 passed=0 rejected=0".to_owned(),
                "autojoin considered=2 joined=1 blocked_concurrent=0 blocked_weekly=1 blocked_lifetime=0"
                    .to_owned(),
                format!(
                    "progress backlog=true deferred=2 attempted=1 wrapped=false next_cursor={}",
                    fed(0x35).to_hex()
                ),
            ]
        );
        // `--json` uses the SAME lowercase-tag vocabulary as the TSV and `candidates --json`, not
        // the derive's PascalCase/adjacently-tagged enum form.
        let json = discover_report_json(&report);
        assert_eq!(json["sources"][0]["source"], "observer");
        assert_eq!(json["sources"][0]["status"], "failed:timeout");
        assert_eq!(json["auto_join"]["blocked_weekly"], 1);
        assert_eq!(json["progress"]["backlog"], true);
        assert_eq!(json["progress"]["deferred"], 2);
        assert_eq!(json["progress"]["attempted"], 1);
        assert_eq!(json["progress"]["wrapped"], false);
        assert_eq!(json["progress"]["next_cursor"], fed(0x35).to_hex());
        Ok(())
    }

    /// §15.11: a PRESENT-but-corrupt client-secret row must abort NAMING the decode failure, never
    /// fall through to the generate path (where the SDK's overwrite guard would surface a
    /// misleading "already exists" error). Store a 64-byte fixed array — its consensus encoding is
    /// 64 raw bytes with no length prefix, so reading it back as a length-prefixed `Vec<u8>` leaves
    /// trailing bytes and the whole-buffer decode fails: exactly the corrupt-row case.
    #[tokio::test]
    async fn corrupt_client_secret_row_aborts_naming_the_decode_failure() {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let db = MemDatabase::new().into_database();
        Client::store_encodable_client_secret(&db, [0u8; 64])
            .await
            .expect("store a raw-array secret that is not a valid Vec<u8> encoding");

        let err = load_or_generate_mnemonic(&db)
            .await
            .expect_err("a corrupt secret row must abort, not regenerate");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to decode") || msg.to_lowercase().contains("decod"),
            "the abort must name the decode failure, got: {msg}"
        );
    }

    /// The absent-row case: a fresh database has no secret, so the helper generates + persists one
    /// and returns it (the normal first-run path, distinct from the corrupt-row abort above).
    #[tokio::test]
    async fn absent_client_secret_row_generates_a_fresh_mnemonic() {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let db = MemDatabase::new().into_database();
        let first = load_or_generate_mnemonic(&db)
            .await
            .expect("absent row generates a fresh mnemonic");
        // Persisted: a second load returns the SAME mnemonic (not a fresh one).
        let second = load_or_generate_mnemonic(&db)
            .await
            .expect("the generated secret is reused on the next load");
        assert_eq!(first.to_entropy(), second.to_entropy());
    }

    #[test]
    fn default_federation_selection_uses_joined_registry_not_open_clients() {
        let a = fed(1);
        let b = fed(2);

        let err = select_fed(&[a, b], &[a], None).expect_err("joined registry is ambiguous");

        assert!(
            err.to_string().contains("multiple federations joined"),
            "{err}"
        );
    }

    #[test]
    fn sole_joined_default_must_also_be_open() {
        let a = fed(1);

        let err = select_fed(&[a], &[], None).expect_err("joined but unopened fed is unusable");

        assert!(
            err.to_string().contains("joined but failed to open"),
            "{err}"
        );
    }

    #[test]
    fn explicit_selection_distinguishes_not_joined_from_not_open() {
        let joined = fed(1);
        let other = fed(2);

        let not_open =
            select_fed(&[joined], &[], Some(&joined.to_hex())).expect_err("fed is not open");
        assert!(
            not_open.to_string().contains("joined but failed to open"),
            "{not_open}"
        );

        let not_joined =
            select_fed(&[joined], &[joined], Some(&other.to_hex())).expect_err("fed is unknown");
        assert!(
            not_joined.to_string().contains("not joined"),
            "{not_joined}"
        );
    }

    #[test]
    fn single_joined_and_open_federation_is_selected_by_default() {
        let a = fed(1);

        assert_eq!(select_fed(&[a], &[a], None).unwrap(), a);
    }

    fn policy_flags_with_designation(
        spending: Option<String>,
        standby: Option<String>,
    ) -> PolicyFlags {
        PolicyFlags {
            spending,
            standby,
            ..PolicyFlags::default()
        }
    }

    #[test]
    fn build_tick_policy_rejects_equal_spending_and_standby() {
        // Pinning both roles to the same fed is a silent no-op rebalance; reject it with a
        // diagnostic, matching `move`'s `from == to` stance.
        let a = fed(1);
        let flags = policy_flags_with_designation(Some(a.to_hex()), Some(a.to_hex()));
        let err = build_tick_policy(&Policy::default(), &flags, &[a], &[a])
            .expect_err("equal pin must be rejected");
        assert!(
            err.to_string().contains("must be different federations"),
            "{err}"
        );
    }

    #[test]
    fn build_tick_policy_accepts_a_single_pinned_role() {
        // Pinning only one role is legitimate — the other is auto-designated distinctly by
        // `build_snapshot`, so `build_tick_policy` must not reject it.
        let a = fed(1);
        let b = fed(2);
        let flags = policy_flags_with_designation(Some(a.to_hex()), None);
        let policy = build_tick_policy(&Policy::default(), &flags, &[a, b], &[a, b])
            .expect("single pin is valid");
        assert_eq!(policy.spending_fed, Some(a));
        assert_eq!(policy.standby_fed, None);
    }

    #[tokio::test]
    async fn standalone_tick_reads_stored_policy_then_layers_flags() -> anyhow::Result<()> {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let a = fed(1);
        let b = fed(2);
        let stored = Policy {
            per_fed_cap: Msat(9_000_000),
            spending_target: Msat(4_000_000),
            standby_target: Msat(2_000_000),
            max_fee: Msat(90_000),
            spending_fed: Some(a),
            standby_fed: Some(b),
            probe_min_span_secs: 123,
            probe_min_successes: 7,
            probe_ttl_secs: 456,
            probe_amount: Msat(33_000),
            ..Policy::default()
        };
        let journal = FedimintJournal::new(MemDatabase::new().into_database());
        journal.put_policy(&stored).await.map_err(exec_err)?;

        let flags = PolicyFlags {
            max_fee: Some(12_000),
            occurrence: 8,
            ..PolicyFlags::default()
        };
        let policy = build_standalone_tick_policy(&journal, &flags, &[a, b], &[a, b])
            .await
            .expect("stored policy with one invocation override is valid");

        assert_eq!(policy.per_fed_cap, stored.per_fed_cap);
        assert_eq!(policy.target_spending_balance, stored.spending_target);
        assert_eq!(policy.standby_target, stored.standby_target);
        assert_eq!(policy.max_fee, Msat(12_000));
        assert_eq!(policy.spending_fed, Some(a));
        assert_eq!(policy.standby_fed, Some(b));
        assert_eq!(policy.occurrence, Occurrence(8));
        assert_eq!(policy.probe_gate_policy.min_span_ms, 123_000);
        assert_eq!(policy.probe_gate_policy.min_successes, 7);
        assert_eq!(policy.probe_gate_policy.ttl_ms, 456_000);
        assert_eq!(policy.probe_gate_policy.amount_msat, 33_000);
        Ok(())
    }

    #[test]
    fn standalone_service_faults_match_client_transport_exit() {
        for error in [
            ServiceError::Storage("read fault".to_owned()),
            ServiceError::ShuttingDown,
            ServiceError::ActorStopped,
        ] {
            assert_eq!(service_err_to_exit(error).code(), 4);
        }
    }

    #[test]
    fn tick_apply_failure_fires_only_when_a_decision_failed() {
        // A clean tick (nothing failed) exits zero — no failure message.
        let clean = ExecutionSummary {
            performed: 2,
            skipped: 1,
            failed: 0,
            terminal_failed_skipped: 0,
            retryable: 0,
        };
        assert!(tick_apply_failure(&clean).is_none());

        // Any failed decision (a retryable `Pending` OR a permanent `Failed`, both counted as
        // `failed` by `apply`) must produce a non-zero-exit message, matching the money-op
        // exit-code convention `move`/`await-move`/`direct-inflow` already follow. Here the single
        // failure is retryable, so the message surfaces the `retryable` sub-count (§15.11).
        let failed = ExecutionSummary {
            performed: 1,
            skipped: 0,
            failed: 1,
            terminal_failed_skipped: 0,
            retryable: 1,
        };
        let msg = tick_apply_failure(&failed).expect("a failed decision must fail the tick");
        assert!(msg.contains("did not apply"), "{msg}");
        assert!(msg.contains("failed=1"), "{msg}");
        assert!(msg.contains("retryable=1"), "{msg}");
        assert!(msg.contains("Retryable failures"), "{msg}");

        let terminal_skip = ExecutionSummary {
            performed: 0,
            skipped: 1,
            failed: 0,
            terminal_failed_skipped: 1,
            retryable: 0,
        };
        let msg =
            tick_apply_failure(&terminal_skip).expect("a terminal Failed skip must fail the tick");
        assert!(msg.contains("terminal_failed_skipped=1"), "{msg}");
        assert!(msg.contains("fresh --occurrence"), "{msg}");
    }

    #[test]
    fn unopened_feds_is_the_joined_minus_open_set() {
        // §15.8. The joined registry minus the successfully-opened set, in registry order.
        let a = fed(1);
        let b = fed(2);
        let c = fed(3);
        // All open -> nothing unopened.
        assert!(unopened_feds(&[a, b, c], &[a, b, c]).is_empty());
        // One joined fed failed to open -> reported (registry order preserved).
        assert_eq!(unopened_feds(&[a, b, c], &[a, c]), vec![b]);
        // Every fed failed to open (empty open set).
        assert_eq!(unopened_feds(&[a, b], &[]), vec![a, b]);
        // No feds joined -> nothing unopened.
        assert!(unopened_feds(&[], &[]).is_empty());
    }

    #[test]
    fn refuse_on_partial_open_bails_only_when_a_fed_failed_to_open() {
        let a = fed(1);
        let b = fed(2);
        // A fully-open wallet drives the tick.
        refuse_on_partial_open(&[a, b], &[a, b]).expect("all open -> tick proceeds");
        // A partial open refuses loudly (non-zero exit) and names the missing fed.
        let err = refuse_on_partial_open(&[a, b], &[a]).expect_err("partial open must refuse");
        let msg = err.to_string();
        assert!(msg.contains("tick refused"), "{msg}");
        assert!(msg.contains(&b.to_hex()), "{msg}");
    }

    #[test]
    fn operator_hard_cap_defaults_on_and_allow_over_cap_disables_it() {
        // §15.2. Off by default -> the ADR-0018 v1 cap is enforced.
        assert_eq!(
            operator_hard_cap(false),
            Some(TickPolicy::default().per_fed_cap)
        );
        // `--allow-over-cap` -> None (an explicit override, cap disabled).
        assert_eq!(operator_hard_cap(true), None);
    }

    #[test]
    fn status_label_is_a_bare_lowercase_word_not_the_option_debug_wrapper() {
        // Regression: `eprintln!("status: {:?}", Some(Awaiting))` leaked `Some(Awaiting)`.
        assert_eq!(status_label(Some(IntentStatus::Awaiting)), "awaiting");
        assert_eq!(status_label(Some(IntentStatus::Done)), "done");
        assert_eq!(status_label(Some(IntentStatus::Failed)), "failed");
        assert_eq!(status_label(None), "unknown");
    }

    // --- §11 history/show formatting ---

    fn ledger_record(
        kind: OperationKind,
        actor: Actor,
        status: OperationStatus,
    ) -> OperationRecord {
        OperationRecord {
            seq: 7,
            correlation_key: IdempotencyKey("pay:0101:n".to_string()),
            kind,
            actor,
            reason: ReasonCode::UserInitiated,
            status,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
            fees: FeeBreakdown {
                fee_cap: None,
                receive_fee: None,
                send_fee_quoted: Some(Msat(88)),
            },
            error: None,
            repaired: false,
        }
    }

    #[test]
    fn rfc3339_from_millis_golden() {
        assert_eq!(rfc3339_from_millis(0), "1970-01-01T00:00:00.000Z");
        // 1_700_000_000 unix seconds = 2023-11-14T22:13:20Z, plus 456 ms.
        assert_eq!(
            rfc3339_from_millis(1_700_000_000_456),
            "2023-11-14T22:13:20.456Z"
        );
    }

    #[test]
    fn history_tsv_golden_columns() {
        // A `pay` row `Awaiting`, invoice amount known, only the send fee quoted, no receive fee.
        let record = ledger_record(
            OperationKind::Pay {
                fed: fed(1),
                invoice_amount: Some(Msat(50_000)),
                payment_hash: None,
                op_id: None,
                gateway: None,
            },
            Actor::User,
            OperationStatus::Awaiting,
        );
        assert_eq!(
            history_tsv(&record),
            "7\t2023-11-14T22:13:20.000Z\tpay\tawaiting\t50000\t-\t88\tuser\tuser_initiated\tpay:0101:n"
        );
    }

    #[test]
    fn history_tsv_renders_unknown_fields_as_dash_and_agent_actor() {
        // A `join` row by the agent: no amount / fees, actor = `agent:<occurrence>`.
        let mut record = ledger_record(
            OperationKind::Join { fed: fed(2) },
            Actor::Agent {
                occurrence: Occurrence(9),
            },
            OperationStatus::Succeeded,
        );
        record.reason = ReasonCode::StandingInstruction;
        record.fees = FeeBreakdown::default();
        let line = history_tsv(&record);
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols[2], "join");
        assert_eq!(cols[3], "succeeded");
        assert_eq!(cols[4], "-", "no amount");
        assert_eq!(cols[5], "-", "no receive fee");
        assert_eq!(cols[6], "-", "no send fee");
        assert_eq!(cols[7], "agent:9");
        assert_eq!(cols[8], "standing_instruction");
    }

    #[test]
    fn kind_and_amount_labels() {
        let evac = OperationKind::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(40_000),
            send_op: None,
            recv_op: None,
            gateway: None,
            evacuation: true,
        };
        assert_eq!(kind_and_amount(&evac), ("evacuation", Some(Msat(40_000))));
        assert_eq!(
            kind_and_amount(&OperationKind::Refusal {
                fed: fed(1),
                diagnostics: Default::default()
            }),
            ("refusal", None)
        );
        assert_eq!(
            kind_and_amount(&OperationKind::Discover {
                source: wallet_core::DiscoverySource::Manual,
                status: wallet_core::SourceStatus::Ok,
                found: 2,
                structurally_passed: 1,
                rejected: 1,
            }),
            ("discover", None)
        );
        assert_eq!(
            kind_and_amount(&OperationKind::AutoJoin {
                considered: 2,
                joined: 1,
                blocked_concurrent: 0,
                blocked_weekly: 0,
                blocked_lifetime: 0,
            }),
            ("autojoin", None)
        );
        assert_eq!(
            kind_and_amount(&OperationKind::Approve { fed: fed(1) }),
            ("approve", None)
        );
    }

    #[test]
    fn status_and_actor_filters_match() {
        assert!(StatusFilter::Awaiting.matches(OperationStatus::Awaiting));
        assert!(!StatusFilter::Awaiting.matches(OperationStatus::Failed));
        assert!(ActorFilter::User.matches(Actor::User));
        assert!(ActorFilter::Agent.matches(Actor::Agent {
            occurrence: Occurrence(1)
        }));
        assert!(!ActorFilter::User.matches(Actor::Agent {
            occurrence: Occurrence(1)
        }));
    }

    #[test]
    fn record_involves_fed_matches_either_move_endpoint() {
        let mv = OperationKind::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(1),
            send_op: None,
            recv_op: None,
            gateway: None,
            evacuation: false,
        };
        let record = ledger_record(mv, Actor::User, OperationStatus::Awaiting);
        assert!(record_involves_fed(&record, fed(1)));
        assert!(record_involves_fed(&record, fed(2)));
        assert!(!record_involves_fed(&record, fed(3)));
    }

    #[test]
    fn record_involves_fed_matches_approve_but_not_source_neutral_discovery_rows() {
        let approve = ledger_record(
            OperationKind::Approve { fed: fed(2) },
            Actor::User,
            OperationStatus::Succeeded,
        );
        assert!(record_involves_fed(&approve, fed(2)));
        assert!(!record_involves_fed(&approve, fed(1)));

        let discover = ledger_record(
            OperationKind::Discover {
                source: wallet_core::DiscoverySource::Manual,
                status: wallet_core::SourceStatus::Ok,
                found: 1,
                structurally_passed: 1,
                rejected: 0,
            },
            Actor::Agent {
                occurrence: Occurrence(1),
            },
            OperationStatus::Succeeded,
        );
        assert!(!record_involves_fed(&discover, fed(1)));

        let autojoin = ledger_record(
            OperationKind::AutoJoin {
                considered: 1,
                joined: 0,
                blocked_concurrent: 1,
                blocked_weekly: 0,
                blocked_lifetime: 0,
            },
            Actor::Agent {
                occurrence: Occurrence(1),
            },
            OperationStatus::Succeeded,
        );
        assert!(!record_involves_fed(&autojoin, fed(1)));
    }

    fn probe_session_from(source: FederationId) -> wallet_fedimint::ProbeSession {
        wallet_fedimint::ProbeSession {
            nonce: "0123456789abcdef0123456789abcdef".to_string(),
            from: source,
            amount_msat: 20_000,
            leg_fee_cap_msat: 10_000,
            c_spendable_before_in_msat: 0,
            out_net_msat: None,
            started_at_ms: 1,
        }
    }

    /// §5.0.7: an in-flight probe's source is authoritative — a resume must reuse it and never
    /// re-infer, so a two-fed auto-rule (or a conflicting `--from`) cannot repoint the resumed
    /// legs at the wrong source.
    #[tokio::test]
    async fn probe_source_prefers_the_in_flight_session_then_explicit_then_auto() {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;

        let (a, b, c) = (fed(1), fed(2), fed(3));
        let journal = FedimintJournal::new(MemDatabase::new().into_database());

        // No session: explicit --from wins.
        assert_eq!(
            probe_source(&journal, &[a, b, c], c, Some(&a.to_hex()))
                .await
                .expect("explicit source"),
            a
        );
        // No session, no --from, exactly two joined: infer the other one.
        assert_eq!(
            probe_source(&journal, &[a, c], c, None)
                .await
                .expect("two-fed inference"),
            a
        );
        // No session, no --from, three joined: refuse.
        assert!(probe_source(&journal, &[a, b, c], c, None).await.is_err());

        // An in-flight probe of `c` from `b`: a resume uses the session's source...
        journal
            .begin_probe_session(&c, &probe_session_from(b))
            .await
            .expect("begin session");
        assert_eq!(
            probe_source(&journal, &[a, c], c, None)
                .await
                .expect("resume ignores the two-fed rule"),
            b,
            "the two-fed auto-rule must not override an in-flight session's source"
        );
        // ...and a conflicting --from is refused rather than silently repointing the legs.
        assert!(
            probe_source(&journal, &[a, c], c, Some(&a.to_hex()))
                .await
                .is_err(),
            "a --from conflicting with the in-flight session must refuse"
        );
        // A matching --from is accepted.
        assert_eq!(
            probe_source(&journal, &[a, c], c, Some(&b.to_hex()))
                .await
                .expect("matching --from"),
            b
        );
    }

    #[test]
    fn build_probe_policy_rejects_ttl_shorter_than_span() {
        // Contradiction: a sustained span cannot fit inside a shorter ttl -> never passes.
        let err = build_probe_policy(None, None, None, Some(3600), Some(60))
            .expect_err("ttl < span must be rejected");
        assert!(err.to_string().contains("shorter than"), "{err}");
        // ttl == span is allowed (the boundary).
        assert!(build_probe_policy(None, None, None, Some(3600), Some(3600)).is_ok());
    }

    #[tokio::test]
    async fn conflicting_resume_money_flags_are_rejected() {
        use fedimint_core::db::mem_impl::MemDatabase;
        use fedimint_core::db::IRawDatabaseExt as _;
        let c = fed(3);
        let journal = FedimintJournal::new(MemDatabase::new().into_database());
        // No in-flight session: any flags are fine.
        reject_conflicting_probe_money_flags(&journal, c, Some(50_000), Some(9_000))
            .await
            .expect("no session -> no conflict");
        // Begin a 20k / 10k session, then a conflicting --amount / --fee-cap is refused,
        // while matching or omitted flags resume cleanly.
        journal
            .begin_probe_session(&c, &probe_session_from(fed(1)))
            .await
            .expect("begin session");
        assert!(
            reject_conflicting_probe_money_flags(&journal, c, Some(50_000), None)
                .await
                .is_err()
        );
        assert!(
            reject_conflicting_probe_money_flags(&journal, c, None, Some(9_000))
                .await
                .is_err()
        );
        reject_conflicting_probe_money_flags(&journal, c, Some(20_000), Some(10_000))
            .await
            .expect("matching flags resume");
        reject_conflicting_probe_money_flags(&journal, c, None, None)
            .await
            .expect("omitted flags resume");
    }
}
