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
use wallet_fedimint::{FedimintJournal, MultiClient};

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

    let journal = FedimintJournal::new(db.clone());
    let mnemonic = load_or_generate_mnemonic(&db).await?;
    let multi_client = MultiClient::new(db, mnemonic).await;

    let joined = journal
        .list_federations()
        .await
        .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?;
    let infos: Vec<_> = joined.iter().map(|(_, info)| info.clone()).collect();
    multi_client.open_all(&infos).await?;

    match cli.command {
        Command::Join { invite } => {
            let invite = InviteCode::from_str(&invite)?;
            let id = multi_client.join(invite).await?;
            println!("{}", id.to_hex());
        }
        Command::Balance => {
            let mut total_msat = 0u64;
            for id in multi_client.federations() {
                let balance = multi_client.balance(&id).await?;
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
    }

    Ok(())
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
