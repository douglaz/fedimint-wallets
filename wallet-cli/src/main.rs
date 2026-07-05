//! `wallet-cli` — the first-class, permanent headless frontend over the wallet engine
//! (ADR-0023). Thin: all logic lives in `wallet-fedimint`/`wallet-core`; this crate only
//! parses arguments, drives the engine, and formats output. No interactive prompts (the
//! engine assumes no UI).

use clap::{Args, Parser, Subcommand};
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
    Action, AllocatorDecision, ExecutionSummary, FederationId, IdempotencyKey, IntentStatus, Msat,
    Occurrence,
};
use wallet_fedimint::{
    FedimintJournal, FinalizeOutcome, GatewayUrl, Invoice, MoveOutcome, MultiClient, OperationId,
    ReceiveState, Runtime, ScoredFed, SendOutcome, SendState, TickPolicy,
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
    },
    /// Block until a send operation reaches a final state, then print it
    /// (success <preimage> / refunded / failed).
    AwaitSend {
        /// The send operation id (hex), as printed by `pay`.
        op: String,
        /// The federation the payment was sent from (hex id).
        #[arg(long)]
        fed: String,
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

    match cli.command {
        Command::Join { invite } => {
            let invite = InviteCode::from_str(&invite)?;
            let id = multi_client.join(invite).await?;
            println!("{}", id.to_hex());
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
            let gateway = pick_receive_gateway(&multi_client, &id, gateway).await?;
            // The move-coordination meta (a `move_id`) lands in step 4b; for now just tag the role.
            let meta = serde_json::json!({ "role": "receive" });
            let (invoice, op) = multi_client
                .receive(&id, Msat(amount), gateway, meta)
                .await?;
            // Invoice -> stdout (the payable result); op id -> stderr (diagnostic handle).
            println!("{}", invoice.0);
            eprintln!("operation_id: {}", to_hex(&op.0));
        }
        Command::Pay {
            invoice,
            fed,
            gateway,
        } => {
            let id = select_fed(&joined_ids, &open_ids, fed.as_deref())?;
            let meta = serde_json::json!({ "role": "send" });
            let outcome = multi_client
                .pay(&id, Invoice(invoice), gateway.map(GatewayUrl), meta)
                .await?;
            match outcome {
                SendOutcome::Started(op) => println!("started {}", to_hex(&op.0)),
                SendOutcome::AlreadyInFlight(op) => println!("already-in-flight {}", to_hex(&op.0)),
                SendOutcome::AlreadyPaid(op) => println!("already-paid {}", to_hex(&op.0)),
            }
        }
        Command::AwaitReceive { op, fed } => {
            let id = select_fed(&joined_ids, &open_ids, Some(&fed))?;
            let op = OperationId(parse_hex32(&op)?);
            match multi_client.await_receive(&id, op).await? {
                ReceiveState::Claimed => println!("claimed"),
                ReceiveState::Expired => println!("expired"),
                ReceiveState::Failed(msg) => println!("failed: {msg}"),
            }
        }
        Command::AwaitSend { op, fed } => {
            let id = select_fed(&joined_ids, &open_ids, Some(&fed))?;
            let op = OperationId(parse_hex32(&op)?);
            match multi_client.await_send(&id, op).await? {
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
                .do_move(from_id, to_id, amount, fee_cap, Occurrence(occurrence))
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
        "{}{role} eligible={} rank={} spendable={} msat probed_ok={} healthy={} reasons={:?}",
        scored.id.to_hex(),
        scored.verdict.eligible_to_fund,
        scored.verdict.rank_score,
        scored.status.balance.spendable.0,
        scored.status.probed_ok,
        scored.status.healthy,
        scored.verdict.reasons,
    )
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
}
