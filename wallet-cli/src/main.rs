//! `wallet-cli` — the first-class, permanent headless frontend over the wallet engine
//! (ADR-0023). Thin: all logic lives in `wallet-fedimint`/`wallet-core`; this crate only
//! parses arguments, drives the engine, and formats output. No interactive prompts (the
//! engine assumes no UI).

use clap::{Parser, Subcommand};
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::Client;
use fedimint_core::db::Database;
use fedimint_core::invite_code::InviteCode;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use wallet_core::{FederationId, IdempotencyKey, IntentStatus, Msat, Occurrence};
use wallet_fedimint::{
    FedimintJournal, FinalizeOutcome, GatewayUrl, Invoice, MultiClient, OperationId, ReceiveState,
    Runtime, SendOutcome, SendState,
};

#[derive(Parser)]
#[command(name = "wallet-cli", about = "Headless multi-federation ecash wallet")]
struct Cli {
    /// Directory holding the wallet's RocksDB and mnemonic.
    #[arg(long, default_value = "./.wallet-cli-data")]
    data_dir: PathBuf,

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
    /// Re-drive pending intents and rebuild move records from the op-log (spec §9 resume loop):
    /// print performed/failed/skipped/awaiting counts; awaiting intent keys go to stderr.
    Reconcile,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
            let mut total_msat = 0u64;
            for id in &open_ids {
                let balance = multi_client.balance(id).await?;
                total_msat += balance.0;
                println!("{}: {} msat", id.to_hex(), balance.0);
            }
            println!("total: {total_msat} msat");
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
            occurrence,
        } => {
            let id = select_fed(&joined_ids, &open_ids, to.as_deref())?;
            let gateway = pick_receive_gateway(&multi_client, &id, gateway).await?;
            let amount = Msat(amount);
            let fee_cap = Msat(fee_cap.unwrap_or_else(|| default_direct_inflow_fee_cap(amount.0)));
            let runtime = Runtime::new(multi_client.clone(), journal.clone(), gateway);
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
            let runtime = Runtime::new(multi_client.clone(), journal.clone(), None);
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
        Command::Reconcile => {
            let runtime = Runtime::new(multi_client.clone(), journal.clone(), None);
            let summary = runtime.reconcile().await?;
            // Counts -> stdout (the scriptable result); awaiting keys -> stderr (handles).
            println!(
                "performed={} failed={} skipped={} awaiting={}",
                summary.performed, summary.failed, summary.skipped, summary.awaiting
            );
            for key in &summary.awaiting_keys {
                eprintln!("awaiting: {}", key.0);
            }
        }
    }

    Ok(())
}

/// A deliberately loose default receive-side fee cap for a CLI `direct-inflow`: the net amount
/// plus 1000 sat of headroom. This is an intentional no-surprises guard for the happy path, not
/// meaningful fee protection; pass `--fee-cap` to bound the receive cost tightly.
fn default_direct_inflow_fee_cap(amount_msat: u64) -> u64 {
    amount_msat.saturating_add(1_000_000)
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
async fn load_or_generate_mnemonic(db: &Database) -> anyhow::Result<Mnemonic> {
    if let Ok(entropy) = Client::load_decodable_client_secret::<Vec<u8>>(db).await {
        return Ok(Mnemonic::from_entropy(&entropy)?);
    }
    let mnemonic = Bip39RootSecretStrategy::<12>::random(&mut rand::thread_rng());
    Client::store_encodable_client_secret(db, mnemonic.to_entropy()).await?;
    Ok(mnemonic)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fed(byte: u8) -> FederationId {
        FederationId([byte; 32])
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
    fn status_label_is_a_bare_lowercase_word_not_the_option_debug_wrapper() {
        // Regression: `eprintln!("status: {:?}", Some(Awaiting))` leaked `Some(Awaiting)`.
        assert_eq!(status_label(Some(IntentStatus::Awaiting)), "awaiting");
        assert_eq!(status_label(Some(IntentStatus::Done)), "done");
        assert_eq!(status_label(Some(IntentStatus::Failed)), "failed");
        assert_eq!(status_label(None), "unknown");
    }
}
