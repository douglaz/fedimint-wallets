//! `walletd` — the 24/7 wallet daemon (spec §6a): one process owns the wallet DB under an
//! exclusive lock, runs the watch scheduler, and fronts the in-process `WalletService` with a
//! local, bearer-authed axum HTTP surface. The CLI (step 6) becomes a thin client over it.
//!
//! Two subcommands: `walletd` (serve until SIGTERM) and `walletd init` (scaffold host config,
//! rotate the 0600 bearer token, write the `~/.config/walletd/` client pointer, seed the
//! default `Policy` row insert-if-absent). All money/decision logic lives in
//! wallet-fedimint/wallet-core; this crate only translates HTTP ⇄ `WalletClient`.

mod config;
mod error;
mod handlers;
mod server;
#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::Client;
use fedimint_core::db::Database;
use std::path::PathBuf;
use std::sync::Arc;
use wallet_api::Policy;
use wallet_fedimint::{FedimintJournal, MultiClient, Runtime, WalletService};

#[derive(Parser)]
#[command(
    name = "walletd",
    about = "The 24/7 fedimint wallet daemon + local API"
)]
struct Cli {
    /// Host config path (`walletd.toml`). Defaults to `~/.config/walletd/walletd.toml`.
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Scaffold host config, rotate the 0600 bearer token, write the client pointer, and seed
    /// the default Policy row (insert-if-absent). Idempotent: re-running rotates the token and
    /// rewrites host config while preserving the DB + policy.
    Init,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = match cli.config {
        Some(path) => path,
        None => config::default_config_path()?,
    };
    match cli.command {
        Some(Command::Init) => run_init(&config_path).await,
        None => run_serve(&config_path).await,
    }
}

/// `walletd init`: filesystem scaffolding + a durable default-policy seed. Deliberately does
/// NOT start the scheduler (no network on a setup command); the policy seeds via step-4's
/// insert-if-absent [`FedimintJournal::seed_policy`], exactly the durable effect a full start
/// would produce. Opening the RocksDB takes the exclusive `db.lock` BEFORE any credential is
/// rotated, so init while a daemon owns the DB BLOCKS on that lock — fedimint's db-locked logs
/// "Waiting for the database lock" and waits, exactly as a second `walletd` serve would — and
/// only proceeds once the daemon stops and releases it. The running daemon's token is therefore
/// never rotated out from under it (token rotation is a while-stopped operation, spec P3).
async fn run_init(config_path: &std::path::Path) -> Result<()> {
    init_tracing("info");
    let config = config::scaffold_config(config_path)?;
    config::ensure_private_data_dir(&config.data_dir)?;
    // Acquire the exclusive store lock before rotating credentials. If another walletd owns
    // the DB, init must leave the token and client pointer untouched so clients remain able to
    // authenticate to that running process.
    let db = open_db(&config).await?;
    let journal = FedimintJournal::new(db);
    // Insert-if-absent: a re-init for token rotation NEVER resets an existing policy.
    journal
        .seed_policy(&Policy::default())
        .await
        .map_err(|error| anyhow::anyhow!("seeding the default policy: {error:?}"))?;

    let token_path = config.token_path.clone();
    let _token = config::rotate_token(&config)?;
    let pointer = config::write_client_pointer(&config)?;

    println!("initialized walletd");
    println!("  host config:    {}", config_path.display());
    println!("  data dir:       {}", config.data_dir.display());
    println!("  token (0600):   {}", token_path.display());
    println!("  client pointer: {}", pointer.display());
    println!("  api url:        {}", config.url());
    Ok(())
}

/// `walletd`: bring the wallet up (mirror wallet-cli's construction, daemon-shaped) and serve
/// until SIGTERM. Set up once, serve forever, then abort-then-drain — not one-command-and-exit.
async fn run_serve(config_path: &std::path::Path) -> Result<()> {
    let config = config::load(config_path)?;
    init_tracing(&config.log_level);
    config::ensure_private_data_dir(&config.data_dir)?;
    let token = config::read_token(&config)?;
    let db = open_db(&config).await?;
    let journal = Arc::new(FedimintJournal::new(db.clone()));

    let mnemonic = load_or_generate_mnemonic(&db).await?;
    let multi_client = Arc::new(MultiClient::new(db, mnemonic).await);
    let joined = journal
        .list_federations()
        .await
        .map_err(|error| anyhow::anyhow!("reading the federation registry: {error:?}"))?;
    let infos: Vec<_> = joined.into_iter().map(|(_, info)| info).collect();
    // A federation that fails to open logs + continues (open_all is per-fed tolerant); the
    // ledger totals already omit unopened feds.
    multi_client.open_all(&infos).await?;

    // Bind before starting WalletService: the scheduler begins immediately on start, so a
    // port conflict must fail before any background workflow can admit work.
    let listener = server::bind(&config.bind()).await?;

    // hard_cap is None ON PURPOSE — the actor reads per_fed_cap from the stored Policy per
    // decide (step 4); no constructor cap.
    let service_runtime = Runtime::new(multi_client.clone(), journal.clone(), None, None, None);
    let service = WalletService::start(service_runtime)
        .await
        .map_err(|error| anyhow::anyhow!("starting the wallet service: {error}"))?;

    // A separate detached runtime over the SAME mc + journal for the dry-run `/v1/status`
    // probe path (decide-only; it journals no money ops).
    let read_runtime = Arc::new(Runtime::new(
        multi_client.clone(),
        journal.clone(),
        None,
        None,
        None,
    ));

    let state = server::AppState {
        client: service.client(),
        journal: journal.clone(),
        mc: Some(multi_client),
        runtime: Some(read_runtime),
        scheduler_alive: service.scheduler_liveness(),
        token: Arc::from(token.as_str()),
        invoice_deadline: handlers::INVOICE_MINT_DEADLINE,
        await_deadline: handlers::AWAIT_LONGPOLL_DEADLINE,
    };

    server::run(service, state, listener).await
}

async fn open_db(config: &config::WalletdConfig) -> Result<Database> {
    let db: Database = fedimint_rocksdb::RocksDb::build(config.db_path())
        .open()
        .await
        .with_context(|| {
            format!(
                "opening the wallet store {} (is another walletd running?)",
                config.db_path().display()
            )
        })?
        .into();
    Ok(db)
}

/// Load the persisted client secret, or generate + persist a fresh mnemonic on first run
/// (mirrors wallet-cli).
async fn load_or_generate_mnemonic(db: &Database) -> Result<Mnemonic> {
    match Client::load_decodable_client_secret_opt::<Vec<u8>>(db).await {
        Ok(Some(entropy)) => Ok(Mnemonic::from_entropy(&entropy)?),
        Ok(None) => {
            let mnemonic = Bip39RootSecretStrategy::<12>::random(&mut rand::thread_rng());
            Client::store_encodable_client_secret(db, mnemonic.to_entropy()).await?;
            Ok(mnemonic)
        }
        Err(error) => {
            Err(error
                .context("wallet client secret is present in the database but failed to decode"))
        }
    }
}

/// Structured `tracing` logs to stderr, honoring `RUST_LOG` over the configured level.
fn init_tracing(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}
