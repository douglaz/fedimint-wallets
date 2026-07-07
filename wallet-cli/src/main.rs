//! `wallet-cli` — the first-class, permanent headless frontend over the wallet engine
//! (ADR-0023). Thin: all logic lives in `wallet-fedimint`/`wallet-core`; this crate only
//! parses arguments, drives the engine, and formats output. No interactive prompts (the
//! engine assumes no UI).

use clap::{Args, Parser, Subcommand, ValueEnum};
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::Client;
use fedimint_core::db::Database;
use fedimint_core::invite_code::InviteCode;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use wallet_core::{
    Action, ActiveProbeVerdict, Actor, AllocatorDecision, ExecutionSummary, FederationId,
    FeeBreakdown, IdempotencyKey, IntentStatus, Journal, Msat, Occurrence, OperationKind,
    OperationRecord, OperationStatus, ProbePolicy, RawOpUpdate, ReasonCode,
};
use wallet_fedimint::{
    parse_invoice, FedimintJournal, FinalizeOutcome, GatewayUrl, Invoice, LedgerRepairOracle,
    MoveOutcome, MultiClient, OperationId, OperationRef, ProbeOutcome, ReceiveState, Runtime,
    ScoredFed, SendOutcome, SendState, TickPolicy,
};

#[derive(Parser)]
#[command(name = "wallet-cli", about = "Headless multi-federation ecash wallet")]
struct Cli {
    /// Directory holding the wallet's RocksDB and mnemonic.
    #[arg(long, default_value = "./.wallet-cli-data")]
    data_dir: PathBuf,

    /// Max wall-clock SECONDS for a single executor `perform` before it is abandoned and left
    /// Pending for the next reconcile (§15.9 — one stalled gateway must not freeze a whole tick).
    /// `0` disables the deadline. Default 600 (10 min).
    #[arg(long, default_value_t = 600)]
    perform_timeout: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Join a federation by its invite code (idempotent: re-joining an already-joined
    /// federation just opens it).
    Join { invite: String },
    /// Print each joined federation's balance (msat) and the total.
    Balance,
    /// List joined federations.
    ListFeds,
    /// Receive Lightning into a federation: print the BOLT11 invoice to stdout and its
    /// operation id (hex) to stderr. The invoice is the payable result; persist the op id
    /// to `await-receive` its settlement.
    Receive {
        /// Amount to receive, in millisatoshis.
        #[arg(long)]
        amount: u64,
        /// Federation to receive into (hex id). Defaults to the sole joined federation.
        #[arg(long)]
        to: Option<String>,
        /// lnv2 gateway URL to mint the invoice. Defaults to lnv2 auto-selecting a live
        /// registered gateway; pass one explicitly against devimint (its LDK gateway is not
        /// auto-registered — see docs/devimint-runbook.md §4).
        #[arg(long)]
        gateway: Option<String>,
    },
    /// Pay a BOLT11 invoice from a federation. Prints the outcome (started / already-in-flight
    /// / already-paid) and the operation id to stdout.
    Pay {
        /// The BOLT11 invoice to pay.
        invoice: String,
        /// Federation to pay from (hex id). Defaults to the sole joined federation.
        #[arg(long)]
        fed: Option<String>,
        /// lnv2 gateway URL. Defaults to lnv2 auto-select (the invoice's issuing gateway,
        /// for the direct-swap path); pass one explicitly against devimint.
        #[arg(long)]
        gateway: Option<String>,
    },
    /// Block until a receive operation reaches a final state, then print it
    /// (claimed / expired / failed).
    AwaitReceive {
        /// The receive operation id (hex), as printed by `receive`.
        op: String,
        /// The federation the receive was created on (hex id).
        #[arg(long)]
        fed: String,
        /// The correlation key printed by `receive` (`key: …`). When given, the ledger row is
        /// advanced to its terminal state here; without it, reconcile repair advances it later.
        #[arg(long)]
        key: Option<String>,
    },
    /// Block until a send operation reaches a final state, then print it
    /// (success <preimage> / refunded / failed).
    AwaitSend {
        /// The send operation id (hex), as printed by `pay`.
        op: String,
        /// The federation the payment was sent from (hex id).
        #[arg(long)]
        fed: String,
        /// The correlation key printed by `pay` (`key: …`). When given, the ledger row is
        /// advanced to its terminal state here; without it, reconcile repair advances it later.
        #[arg(long)]
        key: Option<String>,
    },
    /// Route an inflow to a chosen federation via the executor (spec §6/§7): size + cap-check
    /// the receive invoice so the wallet nets EXACTLY `amount`, print the BOLT11 to stdout and
    /// the intent key to stderr, then `await-move <key>` once the external payer has paid.
    DirectInflow {
        /// Net amount the destination must end up with, in millisatoshis.
        #[arg(long)]
        amount: u64,
        /// Federation to receive into (hex id). Defaults to the sole joined federation.
        #[arg(long)]
        to: Option<String>,
        /// Receive-side fee cap, in millisatoshis. Defaults to a deliberately generous guard
        /// (amount + 1000 sat); pass this to enforce a tight maximum receive fee.
        #[arg(long)]
        fee_cap: Option<u64>,
        /// lnv2 gateway URL to route the inflow. Defaults to the first registered lnv2
        /// gateway; pass one explicitly against devimint (its LDK gateway is not
        /// auto-registered — see docs/devimint-runbook.md §4).
        #[arg(long)]
        gateway: Option<String>,
        /// Allow the destination to exceed the hard per-fed balance cap (ADR-0018). Off by
        /// default: an inflow that would push the destination over the cap is refused pre-mint.
        /// Operator override only — an explicit escape hatch, never silent.
        #[arg(long)]
        allow_over_cap: bool,
        /// Idempotency occurrence. Reusing the same occurrence returns the same invoice; bump it
        /// to create another same-amount inflow after the first one settles or fails.
        #[arg(long, default_value_t = 0)]
        occurrence: u64,
    },
    /// Finalize an awaiting DirectInflow: block on its receive op, then print the final intent
    /// status (done / failed).
    AwaitMove {
        /// The intent key (as printed to stderr by `direct-inflow`).
        key: String,
        /// The federation the inflow receives into (hex id). Optional guard; the destination is
        /// read from the intent's move record.
        #[arg(long)]
        fed: Option<String>,
    },
    /// Move ecash between two joined federations through a shared gateway's internal swap
    /// (spec §7 — the wallet's core cross-federation capability): federation `--from` pays an
    /// invoice minted on `--to`, so `--to` nets EXACTLY `--amount`. Synchronous: blocks until the
    /// move settles, then prints done/failed to stdout and the move key to stderr.
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
        /// Total move fee cap (BOTH legs), in millisatoshis. Defaults to a deliberately generous
        /// guard (amount + 1000 sat); pass this to bound the total move cost tightly.
        #[arg(long)]
        fee_cap: Option<u64>,
        /// Shared lnv2 gateway URL routing the swap — it must serve BOTH federations. Defaults to
        /// the first gateway registered on `--to`; pass one explicitly against devimint (its LDK
        /// gateway is not auto-registered — see docs/devimint-runbook.md §4).
        #[arg(long)]
        gateway: Option<String>,
        /// Allow the destination to exceed the hard per-fed balance cap (ADR-0018). Off by
        /// default: a move that would push the destination over the cap is refused pre-mint.
        /// Operator override only — an explicit escape hatch, never silent.
        #[arg(long)]
        allow_over_cap: bool,
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
    /// Re-drive pending intents and rebuild move records from the op-log (spec §9 resume loop):
    /// print performed/failed/skipped/retryable/awaiting counts; awaiting intent keys go to stderr.
    Reconcile {
        /// Per-fed balance cap to enforce while resuming pending pre-mint intents. Use the same
        /// value that authorized the original tick when reconciling work from `tick --per-fed-cap`.
        #[arg(long)]
        per_fed_cap: Option<u64>,
        /// Resume pending intents that were originally authorized with an over-cap operator
        /// override (`direct-inflow --allow-over-cap` / `move --allow-over-cap`).
        #[arg(long)]
        allow_over_cap: bool,
    },
    /// Run ONE orchestrator tick (Phase 2 step 2.2): probe every open federation, score them,
    /// build the allocator snapshot from the standing-instruction policy, decide, and APPLY the
    /// decisions through the executor — the wallet actually rebalances/tops-up. Prints the
    /// decisions and execution counts to stdout. Recurring schedulers must advance
    /// `--occurrence` after a settled move; a terminal same-occurrence replay exits non-zero
    /// instead of silently skipping the same edge forever.
    Tick {
        #[command(flatten)]
        policy: PolicyFlags,
        /// Shared lnv2 gateway URL routing any rebalance `Move` this tick performs — it must
        /// serve BOTH endpoints of the move. Defaults to each fed's first registered gateway;
        /// pass one explicitly against devimint (its LDK gateway is not auto-registered — see
        /// docs/devimint-runbook.md §4).
        #[arg(long)]
        gateway: Option<String>,
    },
    /// DRY-RUN a tick (Phase 2 step 2.2): probe, score, and decide, but do NOT apply. Prints the
    /// per-federation scored view (eligibility, rank, balance) and the decisions that WOULD run.
    /// Use the same `--occurrence` value you would pass to `tick`; recurring schedulers must
    /// advance it after a settled move.
    Status {
        #[command(flatten)]
        policy: PolicyFlags,
        /// Shared lnv2 gateway URL to validate route availability for the dry run. Pass the same
        /// value as `tick --gateway` against devimint, where the LDK gateway is not
        /// auto-registered.
        #[arg(long)]
        gateway: Option<String>,
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
    /// Show one operation's full record + its live linked intent status (§11). Offline. Resolve
    /// by correlation key or by numeric seq.
    Show {
        /// A correlation key (e.g. `pay:…`) OR a numeric seq.
        reference: String,
        /// Emit the raw `OperationRecord` as JSON instead of the multi-line view.
        #[arg(long)]
        json: bool,
    },
}

/// `--actor` filter for `history` (spec §11).
#[derive(Clone, Copy, Debug, ValueEnum)]
enum ActorFilter {
    User,
    Agent,
}

/// `--status` filter for `history` (spec §11).
#[derive(Clone, Copy, Debug, ValueEnum)]
enum StatusFilter {
    Started,
    Awaiting,
    Succeeded,
    Failed,
}

/// The standing-instruction (ADR-0009) flags shared by `tick` and `status`. Every numeric flag
/// falls back to [`TickPolicy::default`]'s v1 default; the designation flags fall back to
/// auto-designation from the scored-eligible feds.
#[derive(Args)]
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
    /// Per-move fee cap, in millisatoshis.
    #[arg(long)]
    max_fee: Option<u64>,
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
    let cli = Cli::parse();
    // §15.9: the per-`perform` deadline threaded into every engine verb that drives money. `0`
    // disables it.
    let perform_timeout =
        (cli.perform_timeout > 0).then(|| Duration::from_secs(cli.perform_timeout));

    tokio::fs::create_dir_all(&cli.data_dir).await?;
    let db_path = cli.data_dir.join("client.db");
    let db: Database = fedimint_rocksdb::RocksDb::build(db_path)
        .open()
        .await?
        .into();

    let journal = Arc::new(FedimintJournal::new(db.clone()));

    // §11: `history`/`show` are OFFLINE journal scans and MUST work with only the journal open.
    // Dispatch them BEFORE any wallet-client setup — `load_or_generate_mnemonic` would persist a
    // fresh seed and `MultiClient::open_all` reaches the network to resume each federation's state
    // machines, both of which defeat "read-only, never touches the network" (and a corrupt/absent
    // client secret must not block a diagnostic ledger read).
    let command = match cli.command {
        Command::History {
            limit,
            fed,
            actor,
            status,
            json,
        } => return run_history(&journal, limit, fed, actor, status, json).await,
        Command::Show { reference, json } => return run_show(&journal, reference, json).await,
        other => other,
    };

    let mnemonic = load_or_generate_mnemonic(&db).await?;
    let multi_client = Arc::new(MultiClient::new(db, mnemonic).await);

    let joined = journal
        .list_federations()
        .await
        .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?;
    let joined_ids: Vec<_> = joined.iter().map(|(id, _)| *id).collect();
    let infos: Vec<_> = joined.iter().map(|(_, info)| info.clone()).collect();
    multi_client.open_all(&infos).await?;
    let open_ids = multi_client.federations();

    match command {
        Command::Join { invite } => {
            let invite = InviteCode::from_str(&invite)?;
            let fed_id = {
                use fedimint_core::BitcoinHash as _;
                FederationId(invite.federation_id().0.to_byte_array())
            };
            // §10.2: check the membership registry FIRST — an already-joined fed is (re)opened
            // only, with NO ledger row (the idempotent fast path; nothing happened).
            let already = journal
                .get_federation(&fed_id)
                .await
                .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?
                .is_some();
            if already {
                let outcome = multi_client.join(invite).await?;
                println!("{}", outcome.id.to_hex());
            } else {
                // A fresh join: write the `Started` attempt row BEFORE the join, then terminalize
                // truthfully — `newly_joined` distinguishes a real membership from the
                // concurrent-registration window (the pre-written row cannot be un-written).
                let key = IdempotencyKey(format!("join:{}:{}", fed_id.to_hex(), cli_nonce()));
                journal
                    .record_started(
                        &key,
                        OperationKind::Join { fed: fed_id },
                        Actor::User,
                        ReasonCode::UserInitiated,
                        cli_now_ms(),
                        None,
                    )
                    .await
                    .map_err(ledger_err)?;
                let outcome = match multi_client.join(invite).await {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        let _ = journal
                            .record_terminal(
                                &key,
                                OperationStatus::Failed,
                                cli_now_ms(),
                                Some(&e.to_string()),
                                None,
                            )
                            .await;
                        return Err(e);
                    }
                };
                let note = (!outcome.newly_joined)
                    .then_some("already joined (concurrent/prior); no-op re-open");
                note_ledger(
                    journal
                        .record_terminal(&key, OperationStatus::Succeeded, cli_now_ms(), note, None)
                        .await,
                );
                println!("{}", outcome.id.to_hex());
                eprintln!("key: {}", key.0);
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
        Command::Receive {
            amount,
            to,
            gateway,
        } => {
            let id = select_fed(&joined_ids, &open_ids, to.as_deref())?;
            let amount = Msat(amount);
            // §10.1: open the recorded window BEFORE gateway resolution — a nonce-only per-attempt
            // key, so the row exists even if the (below) resolution/SDK call fails synchronously.
            let key = IdempotencyKey(format!("recv:{}:{}", id.to_hex(), cli_nonce()));
            journal
                .record_started(
                    &key,
                    OperationKind::Receive {
                        fed: id,
                        amount_invoiced: amount,
                        op_id: None,
                        gateway: None,
                    },
                    Actor::User,
                    ReasonCode::UserInitiated,
                    cli_now_ms(),
                    None,
                )
                .await
                .map_err(ledger_err)?;
            // `pick_receive_gateway` bails on no-registered-gateway — inside the window, so it
            // lands a `Failed` row (§10.1).
            let sdk_gateway = match pick_receive_gateway(&multi_client, &id, gateway).await {
                Ok(g) => g,
                Err(e) => return fail_raw_row(&journal, &key, e).await,
            };
            // Two-stage fee capture (§9.3): a pre-call best-effort ESTIMATE against a concrete
            // gateway (the `gateway` field stays None — the auto-selected choice is unknown).
            if let Some(fee) =
                estimate_receive_fee(&multi_client, &id, amount, sdk_gateway.clone()).await
            {
                note_ledger(journal.record_update(&key, receive_fee_upd(fee)).await);
            }
            let meta = serde_json::json!({ "role": "receive", "correlation_key": key.0 });
            let (invoice, op) = match multi_client.receive(&id, amount, sdk_gateway, meta).await {
                Ok(result) => result,
                Err(e) => return fail_raw_row(&journal, &key, e).await,
            };
            // The op id advances the row `Started → Awaiting` (the federation accepted the op).
            note_ledger(journal.record_update(&key, op_id_upd(op)).await);
            // Invoice -> stdout (the payable result); op id + key -> stderr (diagnostic handles).
            println!("{}", invoice.0);
            eprintln!("operation_id: {}", to_hex(&op.0));
            eprintln!("key: {}", key.0);
        }
        Command::Pay {
            invoice,
            fed,
            gateway,
        } => {
            let id = select_fed(&joined_ids, &open_ids, fed.as_deref())?;
            // §10.1: the window opens BEFORE parsing — a malformed BOLT11 has no payment hash,
            // yet its failed attempt must still be a durable row.
            let key = IdempotencyKey(format!("pay:{}:{}", id.to_hex(), cli_nonce()));
            journal
                .record_started(
                    &key,
                    OperationKind::Pay {
                        fed: id,
                        invoice_amount: None,
                        payment_hash: None,
                        op_id: None,
                        gateway: None,
                    },
                    Actor::User,
                    ReasonCode::UserInitiated,
                    cli_now_ms(),
                    None,
                )
                .await
                .map_err(ledger_err)?;
            let invoice = Invoice(invoice);
            // Parse (amount + payment hash) — a parse failure is the synchronous-error path.
            let details = match parse_invoice(&invoice) {
                Ok(details) => details,
                Err(e) => return fail_raw_row(&journal, &key, e).await,
            };
            // Post-parse `record_update` (amount + hash, durable BEFORE the SDK call) plus a
            // best-effort send-fee estimate (§9.3 / §10.1).
            let send_fee = estimate_send_fee(
                &multi_client,
                &id,
                &invoice,
                gateway.clone().map(GatewayUrl),
            )
            .await;
            journal
                .record_update(
                    &key,
                    pay_parse_upd(details.amount, details.payment_hash, send_fee),
                )
                .await
                .map_err(ledger_err)?;
            let meta = serde_json::json!({ "role": "send", "correlation_key": key.0 });
            let outcome = match multi_client
                .pay(&id, invoice, gateway.map(GatewayUrl), meta)
                .await
            {
                Ok(outcome) => outcome,
                Err(e) => return fail_raw_row(&journal, &key, e.into()).await,
            };
            match outcome {
                SendOutcome::Started(op) => {
                    note_ledger(journal.record_update(&key, op_id_upd(op)).await);
                    println!("started {}", to_hex(&op.0));
                }
                SendOutcome::AlreadyInFlight(op) => {
                    note_ledger(journal.record_update(&key, op_id_upd(op)).await);
                    println!("already-in-flight {}", to_hex(&op.0));
                }
                SendOutcome::AlreadyPaid(op) => {
                    // Terminal at creation. Read the ORIGINAL op-log meta for definitive fees
                    // FIRST, THEN terminalize — freezing the row before the lookup would keep
                    // blank/estimated fees (§10.1).
                    let upd = settlement_upd(&multi_client, &id, op).await;
                    note_ledger(
                        journal
                            .record_terminal(
                                &key,
                                OperationStatus::Succeeded,
                                cli_now_ms(),
                                None,
                                Some(upd),
                            )
                            .await,
                    );
                    println!("already-paid {}", to_hex(&op.0));
                }
            }
            eprintln!("key: {}", key.0);
        }
        Command::AwaitReceive { op, fed, key } => {
            let id = select_fed(&joined_ids, &open_ids, Some(&fed))?;
            let op = OperationId(parse_hex32(&op)?);
            let state = multi_client.await_receive(&id, op).await?;
            if let Some(key) = &key {
                let (status, error) = match &state {
                    ReceiveState::Claimed => (OperationStatus::Succeeded, None),
                    ReceiveState::Expired => {
                        (OperationStatus::Failed, Some("receive expired".to_string()))
                    }
                    ReceiveState::Failed(msg) => (OperationStatus::Failed, Some(msg.clone())),
                };
                terminalize_awaited(
                    &journal,
                    &multi_client,
                    &id,
                    op,
                    key,
                    AwaitRole::Receive,
                    (status, error),
                )
                .await;
            }
            match state {
                ReceiveState::Claimed => println!("claimed"),
                ReceiveState::Expired => println!("expired"),
                ReceiveState::Failed(msg) => println!("failed: {msg}"),
            }
        }
        Command::AwaitSend { op, fed, key } => {
            let id = select_fed(&joined_ids, &open_ids, Some(&fed))?;
            let op = OperationId(parse_hex32(&op)?);
            let state = multi_client.await_send(&id, op).await?;
            if let Some(key) = &key {
                let (status, error) = match &state {
                    SendState::Success(_) => (OperationStatus::Succeeded, None),
                    SendState::Refunded => {
                        (OperationStatus::Failed, Some("send refunded".to_string()))
                    }
                    SendState::Failed(msg) => (OperationStatus::Failed, Some(msg.clone())),
                };
                terminalize_awaited(
                    &journal,
                    &multi_client,
                    &id,
                    op,
                    key,
                    AwaitRole::Send,
                    (status, error),
                )
                .await;
            }
            match state {
                SendState::Success(preimage) => println!("success {}", to_hex(&preimage.0)),
                SendState::Refunded => println!("refunded"),
                SendState::Failed(msg) => println!("failed: {msg}"),
            }
        }
        Command::DirectInflow {
            amount,
            to,
            fee_cap,
            gateway,
            allow_over_cap,
            occurrence,
        } => {
            let id = select_fed(&joined_ids, &open_ids, to.as_deref())?;
            let gateway = pick_receive_gateway(&multi_client, &id, gateway).await?;
            let amount = Msat(amount);
            let fee_cap = Msat(fee_cap.unwrap_or_else(|| default_direct_inflow_fee_cap(amount.0)));
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway,
                operator_hard_cap(allow_over_cap),
                perform_timeout,
            );
            let outcome = runtime
                .direct_inflow(id, amount, fee_cap, Occurrence(occurrence))
                .await?;
            // Surface the invoice to stdout ONLY when it is a real, payable result: an
            // `Awaiting` inflow (payable now) or an already-settled `Done` idempotent re-run
            // (same invoice, proving no second mint). A terminal `Failed` intent keeps a DEAD
            // invoice that must never be presented as the scriptable result, and a still-`Pending`
            // / absent one has nothing to pay — both `bail!` with guidance and a non-zero exit.
            match (
                direct_inflow_surfaces_invoice(outcome.status),
                outcome.invoice,
            ) {
                (true, Some(invoice)) => {
                    // Invoice -> stdout (the payable result); key + status -> stderr (handles).
                    println!("{}", invoice.0);
                    eprintln!("intent_key: {}", outcome.key.0);
                    eprintln!("status: {}", status_label(outcome.status));
                    if outcome.status == Some(IntentStatus::Done) {
                        eprintln!(
                            "note: intent already settled; bump --occurrence for a new inflow"
                        );
                    }
                }
                (_, _) => anyhow::bail!(
                    "{}",
                    missing_direct_inflow_invoice_message(&outcome.key, outcome.status)
                ),
            }
        }
        Command::AwaitMove { key, fed } => {
            let expected_fed = match fed.as_deref() {
                Some(hex) => Some(select_fed(&joined_ids, &open_ids, Some(hex))?),
                None => None,
            };
            // `await-move` never mints (it finalizes an existing inflow), so the cap is moot —
            // pass `None`; the perform deadline is still threaded for consistency.
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                None,
                None,
                perform_timeout,
            );
            let key = IdempotencyKey(key);
            match runtime.await_move(&key, expected_fed).await? {
                FinalizeOutcome::Done => println!("done"),
                FinalizeOutcome::Failed(msg) => {
                    // Report the terminal status on stdout (the scriptable result), then fail the
                    // process so a caller gating on the exit code (`if wallet-cli await-move …`)
                    // never mistakes a failed finalization for a settled receive — matching
                    // direct-inflow's deliberate non-zero-on-non-payable stance.
                    println!("failed: {msg}");
                    anyhow::bail!("await-move: inflow {} did not settle", key.0);
                }
            }
        }
        Command::Move {
            from,
            to,
            amount,
            fee_cap,
            gateway,
            allow_over_cap,
            occurrence,
        } => {
            let from_id = select_fed(&joined_ids, &open_ids, Some(&from))?;
            let to_id = select_fed(&joined_ids, &open_ids, Some(&to))?;
            anyhow::ensure!(
                from_id != to_id,
                "move --from and --to must be different federations (from == to is a no-op)"
            );
            // Resolve the shared gateway relative to the RECEIVE leg (`to`), which is where the
            // executor pins it for a fresh move; it must also serve `from` for the internal swap.
            let gateway = pick_receive_gateway(&multi_client, &to_id, gateway).await?;
            let amount = Msat(amount);
            let fee_cap = Msat(fee_cap.unwrap_or_else(|| default_move_fee_cap(amount.0)));
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway,
                operator_hard_cap(allow_over_cap),
                perform_timeout,
            );
            let outcome = runtime
                .do_move(
                    from_id,
                    to_id,
                    amount,
                    fee_cap,
                    Occurrence(occurrence),
                    ReasonCode::UserInitiated,
                    Actor::User,
                )
                .await?;
            // done/failed -> stdout (the scriptable result); the move key -> stderr (the handle).
            match outcome.status {
                Some(IntentStatus::Done) => {
                    println!("done");
                    eprintln!("move_key: {}", outcome.key.0);
                }
                status => {
                    // Non-`Done` is not a settled move: report it and fail the process so a caller
                    // gating on the exit code never mistakes it for success (matching await-move /
                    // direct-inflow's deliberate non-zero-on-non-settled stance).
                    println!("failed: {}", move_failure_reason(&outcome));
                    eprintln!("move_key: {}", outcome.key.0);
                    anyhow::bail!(
                        "move {} did not settle (status {})",
                        outcome.key.0,
                        status_label(status)
                    );
                }
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
            let policy =
                build_probe_policy(amount, fee_cap, min_successes, min_span_secs, ttl_secs)?;
            // Probes ride the ordinary move machinery: no pinned gateway (the route
            // resolves from the registered set) and the ADR-0018 hard cap enforced
            // verbatim — probe legs never bypass it (§5.0.5).
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                None,
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
        Command::Reconcile {
            per_fed_cap,
            allow_over_cap,
        } => {
            // Reconcile re-drives already-journaled intents. The intent itself does not persist
            // cap authorization, so expose the same resume policy explicitly: default ADR-0018
            // cap unless the operator supplies the original tick cap or the over-cap override.
            let hard_cap = reconcile_hard_cap(per_fed_cap, allow_over_cap)?;
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                None,
                hard_cap,
                perform_timeout,
            );
            let summary = runtime.reconcile().await?;
            // Counts -> stdout (the scriptable result); awaiting keys -> stderr (handles). §15.11:
            // `retryable` is the subset of `failed` left Pending for a later pass, so a scheduler
            // looping reconcile can tell a transient retry from a terminal `failed − retryable`.
            println!(
                "performed={} failed={} skipped={} retryable={} awaiting={}",
                summary.performed,
                summary.failed,
                summary.skipped,
                summary.retryable,
                summary.awaiting
            );
            for key in &summary.awaiting_keys {
                eprintln!("awaiting: {}", key.0);
            }
        }
        Command::Tick { policy, gateway } => {
            // §15.8: a tick must NOT drive money decisions from a partial world-view. Refuse (no
            // action, non-zero exit) BEFORE probing if any joined fed failed to open.
            refuse_on_partial_open(&joined_ids, &open_ids)?;
            let tick_policy = build_tick_policy(&policy, &joined_ids, &open_ids)?;
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway.map(GatewayUrl),
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
        Command::Status { policy, gateway } => {
            // §15.8: status is the DIAGNOSTIC, so it still prints the scored view even under a
            // partial open — but it reports the unopened feds as rows and exits non-zero.
            let unopened = unopened_feds(&joined_ids, &open_ids);
            let tick_policy = build_tick_policy(&policy, &joined_ids, &open_ids)?;
            // Dry-run only, but the route gate must match the tick that would apply.
            let runtime = Runtime::new(
                multi_client.clone(),
                journal.clone(),
                gateway.map(GatewayUrl),
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
        // §11: dispatched OFFLINE before any client setup (see the top of `main`).
        Command::History { .. } | Command::Show { .. } => {
            unreachable!("history/show are dispatched offline before the wallet client is opened")
        }
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

/// Build a [`TickPolicy`] from the shared policy flags: each numeric flag overrides the v1
/// default, and each designation flag (when given) is validated as a joined+open federation.
fn build_tick_policy(
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
    let mut policy = TickPolicy {
        now,
        ..TickPolicy::default()
    };
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
    policy.occurrence = Occurrence(flags.occurrence);
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
        Action::RefuseInflow { fed, reason } => {
            format!("refuse-inflow {} (reason {reason:?})", fed.to_hex())
        }
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
    format!(
        "{}{role} eligible={} rank={} spendable={} msat probed_ok={} healthy={} active_probe={} \
         reasons={:?}",
        scored.id.to_hex(),
        scored.verdict.eligible_to_fund,
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
    Ok(ProbePolicy {
        amount_msat: amount.unwrap_or(defaults.amount_msat),
        leg_fee_cap_msat: fee_cap.unwrap_or(defaults.leg_fee_cap_msat),
        min_successes: min_successes.unwrap_or(defaults.min_successes),
        min_span_ms: min_span_ms.unwrap_or(defaults.min_span_ms),
        ttl_ms: ttl_ms.unwrap_or(defaults.ttl_ms),
    })
}

/// A federation id as hex, or the literal `none` for an undesignated slot.
fn opt_fed_hex(id: Option<FederationId>) -> String {
    id.map_or_else(|| "none".to_string(), |fed| fed.to_hex())
}

/// A deliberately loose default receive-side fee cap for a CLI `direct-inflow`: the net amount
/// plus 1000 sat of headroom. This is an intentional no-surprises guard for the happy path, not
/// meaningful fee protection; pass `--fee-cap` to bound the receive cost tightly.
fn default_direct_inflow_fee_cap(amount_msat: u64) -> u64 {
    amount_msat.saturating_add(1_000_000)
}

/// A deliberately loose default fee cap for a CLI `move`: the net amount plus 1000 sat of
/// headroom, covering BOTH legs' federation + gateway fees on the happy path. This is a
/// no-surprises guard, not meaningful fee protection; pass `--fee-cap` to bound the move cost.
fn default_move_fee_cap(amount_msat: u64) -> u64 {
    amount_msat.saturating_add(1_000_000)
}

/// A human-readable reason a `move` did not settle. A `Permanent` failure (fee over cap,
/// refund/failed settlement) records its cause on the `MoveRecord`; a transient fault leaves the
/// move `Pending` with no recorded outcome, so point the operator at the re-drive paths.
fn move_failure_reason(outcome: &MoveOutcome) -> String {
    if let Some(reason) = &outcome.outcome {
        return reason.clone();
    }
    match outcome.status {
        Some(IntentStatus::Pending) | Some(IntentStatus::Executing) => format!(
            "move not settled (status {}); a transient fault left it re-drivable — run \
             reconcile, or re-run move with the same --occurrence and --gateway",
            status_label(outcome.status)
        ),
        other => format!("move not settled (status {})", status_label(other)),
    }
}

/// Whether a finished `direct-inflow` should surface its invoice on stdout as the scriptable
/// result (spec §7). Only an `Awaiting` inflow (payable now) or an already-settled `Done`
/// idempotent re-run (the same invoice, proving no second mint) does. A terminal `Failed` intent
/// keeps a DEAD invoice that must never be presented as payable, and a still-`Pending`/`Executing`
/// or absent one has nothing to pay.
fn direct_inflow_surfaces_invoice(status: Option<IntentStatus>) -> bool {
    matches!(status, Some(IntentStatus::Awaiting | IntentStatus::Done))
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

fn missing_direct_inflow_invoice_message(
    key: &IdempotencyKey,
    status: Option<IntentStatus>,
) -> String {
    match status {
        Some(IntentStatus::Failed) => format!(
            "direct-inflow has no payable invoice (intent {} status Failed); this intent is \
             terminal and any minted invoice is dead — retry/reconcile will not re-drive it. \
             Correct the inputs, then start a fresh inflow with a new --occurrence value",
            key.0
        ),
        Some(IntentStatus::Pending) | Some(IntentStatus::Executing) | None => format!(
            "direct-inflow has no payable invoice (intent {} status {}); the receive may have \
             failed before the invoice was persisted — retry direct-inflow with the same \
             --occurrence or run reconcile",
            key.0,
            status_label(status)
        ),
        Some(IntentStatus::Awaiting) | Some(IntentStatus::Done) => format!(
            "direct-inflow has no payable invoice (intent {} status {}); run reconcile to rebuild \
             the move record from the operation log, then retry the command",
            key.0,
            status_label(status)
        ),
    }
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

/// The hard cap policy used while resuming already-journaled pending work. Reconcile cannot infer
/// the cap that authorized an intent from the legacy intent shape, so callers can restate it:
/// default ADR-0018 cap, an explicit tick cap, or the operator over-cap override.
fn reconcile_hard_cap(
    per_fed_cap: Option<u64>,
    allow_over_cap: bool,
) -> anyhow::Result<Option<Msat>> {
    anyhow::ensure!(
        !(allow_over_cap && per_fed_cap.is_some()),
        "--allow-over-cap and --per-fed-cap are mutually exclusive"
    );
    if allow_over_cap {
        Ok(None)
    } else {
        Ok(Some(
            per_fed_cap
                .map(Msat)
                .unwrap_or_else(|| TickPolicy::default().per_fed_cap),
        ))
    }
}

/// Comma-join federation ids as hex for a diagnostic message.
fn hex_list(ids: &[FederationId]) -> String {
    ids.iter()
        .map(|id| id.to_hex())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve the optional CLI gateway flag. An explicit URL becomes a pinned gateway. Without one,
/// require at least one registered lnv2 gateway and return `None`: raw `receive` passes that to
/// lnv2's auto-selection, while `direct-inflow` pins the executor to the first registered gateway
/// for crash-stable replay. Use `--gateway` when liveness or devimint routing matters.
async fn pick_receive_gateway(
    multi_client: &MultiClient,
    id: &FederationId,
    explicit: Option<String>,
) -> anyhow::Result<Option<GatewayUrl>> {
    if let Some(url) = explicit {
        return Ok(Some(GatewayUrl(url)));
    }
    if multi_client.gateways(id).await?.is_empty() {
        anyhow::bail!(
            "no lnv2 gateways registered for {}; pass one explicitly with --gateway \
             (see docs/devimint-runbook.md §4)",
            id.to_hex()
        );
    }
    Ok(None)
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

/// Parse exactly 32 bytes of hex (an operation id) into `[u8; 32]`.
fn parse_hex32(s: &str) -> anyhow::Result<[u8; 32]> {
    anyhow::ensure!(
        s.len() == 64,
        "expected a 64-char hex operation id, got {} chars",
        s.len()
    );
    let bytes = s.as_bytes();
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = (hex_nibble(bytes[i * 2])? << 4) | hex_nibble(bytes[i * 2 + 1])?;
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> anyhow::Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => anyhow::bail!("invalid hex character: {:?}", c as char),
    }
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

/// Log a best-effort raw-op ledger write failure without failing the command — used AFTER the
/// SDK call, where the money op is already authoritative and reconcile repair (§10.3) heals any
/// resulting history gap. A recording hiccup must never regress the live money operation.
fn note_ledger(result: Result<(), impl std::fmt::Debug>) {
    if let Err(e) = result {
        eprintln!("note: raw-op ledger write failed (reconcile will repair): {e:?}");
    }
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

/// Terminalize a raw op's pre-written ledger row `Failed` with `error`, then surface `error` —
/// the synchronous-error path (§10.1): never leave the `Started` row for a repair to mislabel.
async fn fail_raw_row(
    journal: &FedimintJournal,
    key: &IdempotencyKey,
    error: anyhow::Error,
) -> anyhow::Result<()> {
    if let Err(e) = journal
        .record_terminal(
            key,
            OperationStatus::Failed,
            cli_now_ms(),
            Some(&error.to_string()),
            None,
        )
        .await
    {
        eprintln!("note: recording the failed ledger row failed: {e:?}");
    }
    Err(error)
}

fn op_id_upd(op: OperationId) -> RawOpUpdate {
    RawOpUpdate {
        op_id: Some(op),
        ..Default::default()
    }
}

fn receive_fee_upd(fee: Msat) -> RawOpUpdate {
    RawOpUpdate {
        fees: Some(FeeBreakdown {
            receive_fee: Some(fee),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn pay_parse_upd(
    amount: Option<Msat>,
    payment_hash: [u8; 32],
    send_fee: Option<Msat>,
) -> RawOpUpdate {
    RawOpUpdate {
        invoice_amount: amount,
        payment_hash: Some(payment_hash),
        fees: send_fee.map(|f| FeeBreakdown {
            send_fee_quoted: Some(f),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Pre-call receive-fee estimate (§9.3): the gateway deduction on the invoiced amount plus the
/// fed claim fee on the post-gateway contract, quoted against a concrete gateway. Best-effort —
/// any quote failure degrades to `None` (never blocks the receive; the definitive value
/// backfills at settlement).
async fn estimate_receive_fee(
    mc: &MultiClient,
    id: &FederationId,
    amount: Msat,
    sdk_gateway: Option<GatewayUrl>,
) -> Option<Msat> {
    let gateway = estimate_gateway(mc, id, sdk_gateway).await?;
    let gateway_deduction = mc.receive_gateway_fee(id, &gateway).await.ok()?.on(amount);
    let contract = Msat(amount.0.saturating_sub(gateway_deduction.0));
    let fed_fee = mc.receive_fee_quote(id, contract).await.ok()?;
    Some(Msat(gateway_deduction.0.saturating_add(fed_fee.0)))
}

/// Pre-call send-fee estimate (§9.3): the gateway send fee on the invoice plus the fed send-tx
/// fee on the funded contract, quoted against a concrete gateway. Best-effort (degrades to
/// `None`; the exact value backfills from the op-log contract at settlement).
async fn estimate_send_fee(
    mc: &MultiClient,
    id: &FederationId,
    invoice: &Invoice,
    sdk_gateway: Option<GatewayUrl>,
) -> Option<Msat> {
    let amount = parse_invoice(invoice).ok()?.amount?;
    let gateway = estimate_gateway(mc, id, sdk_gateway).await?;
    let gateway_quote = mc
        .send_gateway_fee(id, &gateway, invoice)
        .await
        .ok()?
        .on(amount);
    let contract = Msat(amount.0.saturating_add(gateway_quote.0));
    let fed_fee = mc.send_fee_quote_for_amount(id, contract).await.ok()?;
    Some(Msat(gateway_quote.0.saturating_add(fed_fee.0)))
}

/// The concrete gateway to quote a raw-op fee ESTIMATE against: the explicit `--gateway` if
/// given, else the fed's first registered gateway; `None` when none is available (the estimate
/// then degrades to a blank fee — the definitive value backfills at settlement, §9.3). The
/// actual auto-selected gateway is unknown pre-call, so the row's `gateway` field stays `None`.
async fn estimate_gateway(
    mc: &MultiClient,
    id: &FederationId,
    sdk_gateway: Option<GatewayUrl>,
) -> Option<GatewayUrl> {
    match sdk_gateway {
        Some(gateway) => Some(gateway),
        None => mc.gateways(id).await.ok()?.into_iter().next(),
    }
}

/// The definitive settlement enrichment for a raw op (§9.3 backfill), read from its op-log meta
/// via the repair oracle. On failure the fees degrade to `None` (the op id is still recorded).
async fn settlement_upd(mc: &MultiClient, id: &FederationId, op: OperationId) -> RawOpUpdate {
    match mc.observe_op(*id, op).await {
        Ok(obs) => RawOpUpdate {
            op_id: Some(op),
            gateway: obs.gateway,
            invoice_amount: obs.invoice_amount,
            payment_hash: obs.payment_hash,
            fees: Some(obs.fees),
            // Definitive iff the op is TERMINAL: settlement fees then replace any pre-call
            // estimate outright (even with `None`) so a terminal row never freezes an
            // estimate as an observed cost.
            fees_definitive: obs.terminal.is_some(),
        },
        Err(e) => {
            eprintln!(
                "note: could not read settlement fees for {}: {e:?}",
                to_hex(&op.0)
            );
            op_id_upd(op)
        }
    }
}

/// Whether an `await-*` `--key` names a `send` (`pay:`) or a `receive` (`recv:`) row.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AwaitRole {
    Send,
    Receive,
}

/// Verify a `--key`-named ledger row is the one this `await-*` op belongs to before a terminal
/// write (§10.1). A terminal ledger row is IMMUTABLE, so a `--key` naming an UNRELATED row (wrong
/// verb, wrong federation, or a different in-flight op) would permanently corrupt that row's
/// history — refuse instead. Returns `Ok(true)` for a BLANK row (op id not yet recorded — the
/// crash-before-`record_update` window): the caller must then prove via the op-log's
/// `correlation_key` meta that the awaited op really is this row's before terminalizing —
/// a blank row of the same kind/federation could belong to a DIFFERENT attempt.
fn awaited_row_matches(
    row: &OperationRecord,
    role: AwaitRole,
    id: &FederationId,
    op: OperationId,
) -> Result<bool, String> {
    let (row_fed, row_op) = match (&row.kind, role) {
        (OperationKind::Pay { fed, op_id, .. }, AwaitRole::Send) => (fed, op_id),
        (OperationKind::Receive { fed, op_id, .. }, AwaitRole::Receive) => (fed, op_id),
        _ => return Err("its kind is not the awaited pay/receive operation".to_owned()),
    };
    if row_fed != id {
        return Err("it belongs to a different federation".to_owned());
    }
    match row_op {
        Some(existing) if *existing != op => {
            Err("it already tracks a different operation".to_owned())
        }
        Some(_) => Ok(false),
        None => Ok(true),
    }
}

/// Advance the `--key` ledger row to its terminal state after an `await-*` (§10.1), carrying
/// the definitive settlement enrichment. Auxiliary — a recording fault is logged, not fatal.
async fn terminalize_awaited(
    journal: &FedimintJournal,
    mc: &MultiClient,
    id: &FederationId,
    op: OperationId,
    key: &str,
    role: AwaitRole,
    outcome: (OperationStatus, Option<String>),
) {
    let (status, error) = outcome;
    let key = IdempotencyKey(key.to_owned());
    // Guard against a mistyped/mismatched `--key`: a terminal row cannot be un-written, so verify
    // the row is this op's before touching it (§10.1). A missing row is a no-op anyway.
    match journal.operation(&OperationRef::Key(key.clone())).await {
        Ok(Some(row)) => {
            match awaited_row_matches(&row, role, id, op) {
                Err(why) => {
                    eprintln!(
                        "note: --key {} does not match this operation ({why}); not recording",
                        key.0
                    );
                    return;
                }
                // A BLANK row (op id never recorded) is only accepted when the op-log proves
                // the awaited op was created under THIS correlation key — the pre-call meta
                // embeds it, so a genuine crash-before-`record_update` attempt matches. A
                // deduped retry (the op carries another attempt's key) or a wrong blank row
                // is refused and left to reconcile's hash-dedup repair, which records the
                // attribution ambiguity honestly instead of silently mis-attaching history.
                Ok(true) => match mc.find_op_by_correlation_key(*id, &key).await {
                    Ok(Some(found)) if found == op => {}
                    Ok(_) => {
                        eprintln!(
                            "note: --key {} has no recorded op id and the op-log does not tie \
                             this operation to it; not recording (reconcile repairs it)",
                            key.0
                        );
                        return;
                    }
                    Err(e) => {
                        eprintln!(
                            "note: could not verify --key {} against the op-log: {e:?}; \
                             not recording",
                            key.0
                        );
                        return;
                    }
                },
                Ok(false) => {}
            }
        }
        Ok(None) => {
            eprintln!("note: no ledger row for --key {}; not recording", key.0);
            return;
        }
        Err(e) => {
            eprintln!(
                "note: could not read the ledger row for --key {}: {e:?}",
                key.0
            );
            return;
        }
    }
    let upd = settlement_upd(mc, id, op).await;
    if let Err(e) = journal
        .record_terminal(&key, status, cli_now_ms(), error.as_deref(), Some(upd))
        .await
    {
        eprintln!("note: recording the terminal ledger row failed: {e:?}");
    }
}

impl ActorFilter {
    fn matches(self, actor: Actor) -> bool {
        matches!(
            (self, actor),
            (ActorFilter::User, Actor::User) | (ActorFilter::Agent, Actor::Agent { .. })
        )
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
}

/// Whether a record involves `fed` (for `history --fed`): a `Move` matches either endpoint.
fn record_involves_fed(record: &OperationRecord, fed: FederationId) -> bool {
    match &record.kind {
        OperationKind::Join { fed: f } | OperationKind::Refusal { fed: f } => *f == fed,
        OperationKind::Receive { fed: f, .. } | OperationKind::Pay { fed: f, .. } => *f == fed,
        OperationKind::DirectInflow { to, .. } => *to == fed,
        OperationKind::Move { from, to, .. } => *from == fed || *to == fed,
        // Either endpoint matches, so `history --fed <source>` stays complete even for a
        // pair-scoped route failure whose move intents never existed (§5.0.5).
        OperationKind::Probe { fed: f, from, .. } => *f == fed || *from == fed,
        OperationKind::Tick { .. } => false,
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

fn print_kind_details(kind: &OperationKind) {
    match kind {
        OperationKind::Join { fed } | OperationKind::Refusal { fed } => {
            println!("fed: {}", fed.to_hex())
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
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn fed(byte: u8) -> FederationId {
        FederationId([byte; 32])
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

    #[test]
    fn failed_direct_inflow_missing_invoice_message_does_not_suggest_reconcile_retry() {
        let key = IdempotencyKey("direct-inflow:test:0".into());
        let msg = missing_direct_inflow_invoice_message(&key, Some(IntentStatus::Failed));

        assert!(msg.contains("terminal"), "{msg}");
        assert!(msg.contains("new --occurrence"), "{msg}");
        assert!(!msg.contains("run reconcile"), "{msg}");
        assert!(!msg.contains("retry direct-inflow"), "{msg}");
    }

    #[test]
    fn pending_direct_inflow_missing_invoice_message_is_retryable() {
        let key = IdempotencyKey("direct-inflow:test:0".into());
        let msg = missing_direct_inflow_invoice_message(&key, Some(IntentStatus::Pending));

        assert!(msg.contains("same --occurrence"), "{msg}");
        assert!(msg.contains("run reconcile"), "{msg}");
    }

    #[test]
    fn only_awaiting_or_done_direct_inflow_surfaces_the_invoice() {
        // Payable now, or an idempotent post-settlement re-run (same invoice, no second mint).
        assert!(direct_inflow_surfaces_invoice(Some(IntentStatus::Awaiting)));
        assert!(direct_inflow_surfaces_invoice(Some(IntentStatus::Done)));
        // A terminal Failed intent keeps a DEAD invoice: it must NEVER be surfaced as the
        // scriptable stdout result (a scripted `INV=$(direct-inflow …)` must not get a dead
        // BOLT11 with exit 0). Pending/Executing/absent have nothing payable to surface.
        assert!(!direct_inflow_surfaces_invoice(Some(IntentStatus::Failed)));
        assert!(!direct_inflow_surfaces_invoice(Some(IntentStatus::Pending)));
        assert!(!direct_inflow_surfaces_invoice(Some(
            IntentStatus::Executing
        )));
        assert!(!direct_inflow_surfaces_invoice(None));
    }

    #[test]
    fn move_failure_reason_prefers_the_recorded_outcome() {
        // A `Permanent` move failure records its cause on the MoveRecord; the CLI surfaces it
        // verbatim rather than a generic status line.
        let recorded = MoveOutcome {
            key: IdempotencyKey("move:aa:bb:0".into()),
            status: Some(IntentStatus::Failed),
            outcome: Some("fee over cap".into()),
        };
        assert_eq!(move_failure_reason(&recorded), "fee over cap");
    }

    #[test]
    fn move_failure_reason_points_a_pending_move_at_the_re_drive_paths() {
        // A transient fault leaves the move `Pending` with no recorded outcome: the message must
        // tell the operator it is re-drivable (reconcile / same-occurrence re-run), not terminal.
        let pending = MoveOutcome {
            key: IdempotencyKey("move:aa:bb:0".into()),
            status: Some(IntentStatus::Pending),
            outcome: None,
        };
        let msg = move_failure_reason(&pending);
        assert!(msg.contains("re-drivable"), "{msg}");
        assert!(msg.contains("reconcile"), "{msg}");
        assert!(msg.contains("--occurrence"), "{msg}");
    }

    fn policy_flags_with_designation(
        spending: Option<String>,
        standby: Option<String>,
    ) -> PolicyFlags {
        PolicyFlags {
            per_fed_cap: None,
            spending_target: None,
            standby_target: None,
            max_fee: None,
            spending,
            standby,
            occurrence: 0,
        }
    }

    #[test]
    fn build_tick_policy_rejects_equal_spending_and_standby() {
        // Pinning both roles to the same fed is a silent no-op rebalance; reject it with a
        // diagnostic, matching `move`'s `from == to` stance.
        let a = fed(1);
        let flags = policy_flags_with_designation(Some(a.to_hex()), Some(a.to_hex()));
        let err = build_tick_policy(&flags, &[a], &[a]).expect_err("equal pin must be rejected");
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
        let policy = build_tick_policy(&flags, &[a, b], &[a, b]).expect("single pin is valid");
        assert_eq!(policy.spending_fed, Some(a));
        assert_eq!(policy.standby_fed, None);
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
    fn reconcile_hard_cap_can_restate_the_original_resume_policy() {
        // Default reconcile still enforces ADR-0018 on pre-mint resumes.
        assert_eq!(
            reconcile_hard_cap(None, false).expect("default cap"),
            Some(TickPolicy::default().per_fed_cap)
        );
        // Work created by `tick --per-fed-cap` can be resumed under the same cap.
        assert_eq!(
            reconcile_hard_cap(Some(42), false).expect("custom cap"),
            Some(Msat(42))
        );
        // Work created by an operator `--allow-over-cap` verb can be resumed without a cap.
        assert_eq!(
            reconcile_hard_cap(None, true).expect("allow over cap"),
            None
        );
        // The two policies are intentionally exclusive: one sets a cap, the other disables it.
        let err = reconcile_hard_cap(Some(42), true).expect_err("conflicting cap flags");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
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
            kind_and_amount(&OperationKind::Refusal { fed: fed(1) }),
            ("refusal", None)
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

    fn op(byte: u8) -> OperationId {
        OperationId([byte; 32])
    }

    fn pay_row(fed_id: FederationId, op_id: Option<OperationId>) -> OperationRecord {
        ledger_record(
            OperationKind::Pay {
                fed: fed_id,
                invoice_amount: None,
                payment_hash: None,
                op_id,
                gateway: None,
            },
            Actor::User,
            OperationStatus::Awaiting,
        )
    }

    #[test]
    fn awaited_row_matches_accepts_the_awaited_pay_row() {
        // No op id yet (crash before the op-id update): accepted as BLANK — the caller must
        // then prove the correlation via the op-log meta before terminalizing (§10.1).
        assert_eq!(
            awaited_row_matches(&pay_row(fed(1), None), AwaitRole::Send, &fed(1), op(7)),
            Ok(true)
        );
        // Op id present and equal → accepted outright (not blank).
        assert_eq!(
            awaited_row_matches(
                &pay_row(fed(1), Some(op(7))),
                AwaitRole::Send,
                &fed(1),
                op(7)
            ),
            Ok(false)
        );
    }

    #[test]
    fn awaited_row_matches_rejects_a_mismatched_key() {
        // Wrong verb: a pay row cannot be terminalized by an `await-receive --key`.
        assert!(
            awaited_row_matches(&pay_row(fed(1), None), AwaitRole::Receive, &fed(1), op(7))
                .is_err()
        );
        // Wrong federation: a valid key from another fed must not corrupt this row.
        assert!(
            awaited_row_matches(&pay_row(fed(2), None), AwaitRole::Send, &fed(1), op(7)).is_err()
        );
        // A different in-flight op already recorded on the row.
        assert!(awaited_row_matches(
            &pay_row(fed(1), Some(op(9))),
            AwaitRole::Send,
            &fed(1),
            op(7)
        )
        .is_err());
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
}
