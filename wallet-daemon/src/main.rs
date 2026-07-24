//! `walletd` — the 24/7 wallet daemon (spec §6a): one process owns the wallet DB under an
//! exclusive lock, runs the watch scheduler, and fronts the in-process `WalletService` with a
//! local, bearer-authed axum HTTP surface. The CLI (step 6) becomes a thin client over it.
//!
//! Subcommands cover host initialization, read-only mnemonic export, and seed restoration;
//! no subcommand serves until SIGTERM. All money/decision logic lives in
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
use std::time::Duration;
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
    /// Print the wallet's 12-word recovery mnemonic to stdout so the operator can write it
    /// down (the seed otherwise lives ONLY inside client.db — losing that store without a
    /// backup loses the funds). Read-only: never generates a seed, and fails loudly if the
    /// store has none. Blocks on the exclusive store lock, so it only reveals the secret
    /// while the daemon is stopped — same while-stopped semantics as `init`'s token rotation.
    Mnemonic,
    /// Import a 12-word seed into a store that has NONE, so a lost wallet can be rebuilt with
    /// `recover` (`docs/wallet-recovery-spec.md`). The mirror of `mnemonic`: reads the words from
    /// STDIN (never argv — argv leaks into shell history and `ps`), validates the BIP-39 checksum,
    /// and REFUSES if a seed already exists (no `--force`; overwriting silently strands whatever it
    /// funded). Ordering: `init` → `restore-mnemonic` → then start (`init` mints no seed; only the
    /// daemon start path does, so starting first mints a random seed and this import then refuses).
    RestoreMnemonic,
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
        Some(Command::Mnemonic) => run_mnemonic(&config_path).await,
        Some(Command::RestoreMnemonic) => run_restore_mnemonic(&config_path).await,
        None => run_serve(&config_path).await,
    }
}

/// `walletd mnemonic`: reveal the persisted seed for a written backup. Deliberately NOT a
/// load-or-generate: revealing must never mint a seed (a mistyped data dir would silently
/// hand the operator words that control an empty wallet). The words go to stdout alone —
/// warnings go to stderr — so `walletd mnemonic > seed.txt` captures exactly the secret.
async fn run_mnemonic(config_path: &std::path::Path) -> Result<()> {
    let config = config::load(config_path)?;
    let db = open_db(&config).await?;
    let entropy = Client::load_decodable_client_secret_opt::<Vec<u8>>(&db)
        .await
        .map_err(|error| {
            error.context("wallet client secret is present in the database but failed to decode")
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no wallet seed in {} — this store has never run a wallet (check --config \
                 and the data_dir it points at)",
                config.db_path().display()
            )
        })?;
    let mnemonic = Mnemonic::from_entropy(&entropy)?;
    eprintln!("write these 12 words down and store them offline; anyone holding them");
    eprintln!("controls the funds. This is the ONLY copy outside client.db.");
    println!("{mnemonic}");
    Ok(())
}

/// `walletd restore-mnemonic`: import a 12-word seed into a store that has NONE (the mirror of
/// `walletd mnemonic`). Reads the words from stdin, validates the BIP-39 checksum, and persists
/// them — refusing if a seed already exists. Opening the RocksDB takes the exclusive store lock, so
/// this is a while-stopped operation like `mnemonic`/`init`.
async fn run_restore_mnemonic(config_path: &std::path::Path) -> Result<()> {
    let config = config::load(config_path)?;
    // The next write installs full spend authority. Apply the same 0700 data-directory invariant
    // as `init` and daemon startup before RocksDB creates or opens any seed-bearing files.
    config::ensure_private_data_dir(&config.data_dir)?;
    let db = open_db(&config).await?;
    let phrase = read_mnemonic_phrase(std::io::stdin().lock())?;
    restore_mnemonic_into(&db, &phrase, &config.db_path()).await?;
    eprintln!(
        "restored the wallet seed; start the daemon, then `recover <invite>` each federation \
         whose balance you need rebuilt"
    );
    Ok(())
}

/// Read the mnemonic phrase from `reader` (stdin in production — never argv, which leaks into
/// shell history and `ps`). Collapses surrounding/internal whitespace so a phrase pasted with a
/// trailing newline or odd spacing still parses. Split out so the stdin contract is unit-testable.
fn read_mnemonic_phrase(mut reader: impl std::io::Read) -> Result<String> {
    let mut raw = String::new();
    reader
        .read_to_string(&mut raw)
        .context("reading the mnemonic from stdin")?;
    let phrase = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if phrase.is_empty() {
        anyhow::bail!(
            "no mnemonic on stdin — pipe the 12 words in, e.g. `walletd restore-mnemonic < seed.txt`"
        );
    }
    Ok(phrase)
}

/// Validate and persist `phrase` as the wallet seed into `db`. Refuses if a seed already exists
/// (overwriting silently strands whatever it funded — the one irreversible action here), and
/// rejects an invalid BIP-39 checksum BEFORE any write. Split from [`run_restore_mnemonic`] so it
/// is testable over an in-memory db without stdin/RocksDB. The SDK's secret store ALSO refuses
/// overwrite; the explicit check here yields the actionable message and skips checksum work.
async fn restore_mnemonic_into(
    db: &Database,
    phrase: &str,
    db_path: &std::path::Path,
) -> Result<()> {
    if Client::load_decodable_client_secret_opt::<Vec<u8>>(db)
        .await
        .context("checking for an existing wallet seed")?
        .is_some()
    {
        anyhow::bail!(
            "a wallet seed already exists in {} — restore refuses to overwrite it (that would \
             strand whatever it funded); to restore a different seed, start from a clean data dir",
            db_path.display()
        );
    }
    let mnemonic = Mnemonic::parse_normalized(phrase).map_err(|error| {
        anyhow::anyhow!("the supplied words are not a valid BIP-39 mnemonic: {error}")
    })?;
    anyhow::ensure!(
        mnemonic.word_count() == 12,
        "the supplied mnemonic must contain exactly 12 words (got {})",
        mnemonic.word_count()
    );
    Client::store_encodable_client_secret(db, mnemonic.to_entropy())
        .await
        .context("persisting the restored wallet seed")?;
    Ok(())
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
    // authenticate to that running process. Same fixed order as serve: client.db (the
    // exclusivity anchor), then journal.db (where the policy row lives after the split).
    let _db = open_db(&config).await?;
    let journal_db = open_journal_db(&config).await?;
    let journal = FedimintJournal::new(journal_db);
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
    // TWO RocksDBs, opened in a FIXED order (client.db first — the exclusivity anchor every
    // other process contends on, then journal.db; a consistent order can never deadlock).
    // The journal MUST NOT share the fedimint clients' store: fedimint tunes RocksDB to a
    // 2MB write buffer with no extra memtable history, so a co-located journal's tick/ledger
    // churn flushes the history out from under any transaction fedimint holds open and its
    // commit dies `TryAgain`→`WriteConflict` (the 24h-soak wedge; that instance is fixed at
    // our pinned rev, upstream PR #8816). Separate DBs = separate memtables.
    let db = open_db(&config).await?;
    let journal_db = open_journal_db(&config).await?;
    let journal = Arc::new(FedimintJournal::new(journal_db.clone()));

    let mnemonic = load_or_generate_mnemonic(&db).await?;
    let multi_client = Arc::new(MultiClient::new(db, journal_db, mnemonic).await);
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
    // decide (step 4); no constructor cap. The gateway pin is host config (walletd.toml):
    // required against devimint, whose LDK gateway is never registered into the lnv2 set.
    let pinned_gateway = config.gateway.clone().map(wallet_fedimint::GatewayUrl);
    // Per-`perform` wall-clock deadline (§15.9). The daemon MUST bound this: an unbounded
    // `perform` lets a stalled settlement long-poll (e.g. a `Move`'s receive-claim await over
    // a flaky transport) park its driver forever — the driver never yields, so reconcile can
    // never re-drive it and the move hangs indefinitely (observed on an iroh-transport fed).
    // On timeout the intent is left `Pending` (never terminalized), and the next reconcile
    // re-drives it with a FRESH await, the way `wallet-cli` already bounds every op. Join is
    // exempt inside the timeout wrapper (DKG config download is legitimately slow).
    let perform_timeout = perform_timeout_from_env();
    let service_runtime = Runtime::new(
        multi_client.clone(),
        journal.clone(),
        pinned_gateway.clone(),
        None,
        perform_timeout,
    );
    let service = WalletService::start(service_runtime)
        .await
        .map_err(|error| anyhow::anyhow!("starting the wallet service: {error}"))?;

    // A separate detached runtime over the SAME mc + journal for the dry-run `/v1/status`
    // probe path (decide-only; it journals no money ops).
    let read_runtime = Arc::new(Runtime::new(
        multi_client.clone(),
        journal.clone(),
        pinned_gateway,
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

/// The per-`perform` deadline for the driving runtime, from `WALLETD_PERFORM_TIMEOUT_SECS`
/// (default 600s, matching `wallet-cli`). `0` disables the deadline — allowed but discouraged,
/// since an unbounded `perform` can park a driver on a stalled settlement long-poll forever.
/// A non-numeric value falls back to the default rather than failing daemon startup.
fn perform_timeout_from_env() -> Option<Duration> {
    parse_perform_timeout(
        std::env::var("WALLETD_PERFORM_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

/// Pure parse of the `WALLETD_PERFORM_TIMEOUT_SECS` value (split from the env read so it is
/// unit-testable without process-global env). `None`/blank/non-numeric → the 600s default; an
/// explicit `0` → no deadline.
fn parse_perform_timeout(raw: Option<&str>) -> Option<Duration> {
    const DEFAULT_SECS: u64 = 600;
    let secs = raw
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_SECS);
    (secs > 0).then(|| Duration::from_secs(secs))
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

/// The app journal's OWN RocksDB (`journal.db`), separate from the fedimint clients' store —
/// see `run_serve` for why co-locating them wedges fedimint's long-held lnv2 transactions.
async fn open_journal_db(config: &config::WalletdConfig) -> Result<Database> {
    let db: Database = fedimint_rocksdb::RocksDb::build(config.journal_db_path())
        .open()
        .await
        .with_context(|| {
            format!(
                "opening the journal store {} (is another walletd running?)",
                config.journal_db_path().display()
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

#[cfg(test)]
mod perform_timeout_tests {
    use super::{parse_perform_timeout, Duration};

    #[test]
    fn defaults_to_600s_when_unset_or_garbage() {
        assert_eq!(parse_perform_timeout(None), Some(Duration::from_secs(600)));
        assert_eq!(
            parse_perform_timeout(Some("")),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            parse_perform_timeout(Some("not-a-number")),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn honors_an_explicit_value_and_disables_on_zero() {
        assert_eq!(
            parse_perform_timeout(Some("180")),
            Some(Duration::from_secs(180))
        );
        assert_eq!(
            parse_perform_timeout(Some("  180  ")),
            Some(Duration::from_secs(180))
        );
        assert_eq!(parse_perform_timeout(Some("0")), None);
    }
}

#[cfg(test)]
mod restore_mnemonic_tests {
    use super::{read_mnemonic_phrase, restore_mnemonic_into};
    use fedimint_bip39::Mnemonic;
    use fedimint_client::Client;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::{Database, IRawDatabaseExt as _};
    use std::path::Path;

    fn mem_db() -> Database {
        MemDatabase::new().into_database()
    }

    /// A valid 12-word BIP-39 phrase (the all-zero entropy mnemonic, checksum correct).
    fn valid_phrase() -> String {
        Mnemonic::from_entropy(&[0u8; 16])
            .expect("valid 12-word entropy")
            .to_string()
    }

    #[test]
    fn reads_the_phrase_from_stdin_and_collapses_whitespace() {
        // A phrase piped with a trailing newline and padded spacing still parses to the canonical
        // single-space form — proving the words come from the reader (stdin), not argv.
        let phrase = read_mnemonic_phrase("  abandon   abandon\nabandon\t\n".as_bytes())
            .expect("non-empty input parses");
        assert_eq!(phrase, "abandon abandon abandon");
    }

    #[test]
    fn empty_stdin_is_a_clean_error() {
        assert!(read_mnemonic_phrase("   \n\t ".as_bytes()).is_err());
    }

    #[tokio::test]
    async fn restore_refuses_when_a_seed_already_exists() {
        let db = mem_db();
        // Seed the store as the daemon's first start would.
        let existing = Mnemonic::from_entropy(&[1u8; 16]).expect("valid entropy");
        Client::store_encodable_client_secret(&db, existing.to_entropy())
            .await
            .expect("first seed stores");

        // Importing over it is refused — a DIFFERENT valid phrase must not overwrite.
        let result = restore_mnemonic_into(&db, &valid_phrase(), Path::new("/tmp/client.db")).await;
        assert!(result.is_err(), "restore must refuse to overwrite a seed");
        assert!(result.unwrap_err().to_string().contains("already exists"));

        // The original seed is untouched.
        let stored = Client::load_decodable_client_secret_opt::<Vec<u8>>(&db)
            .await
            .expect("seed reads")
            .expect("seed present");
        assert_eq!(stored, existing.to_entropy());
    }

    #[tokio::test]
    async fn restore_rejects_a_bad_bip39_checksum_and_writes_nothing() {
        let db = mem_db();
        // 12 valid words but a wrong checksum (the last word must be "about", not "abandon").
        let bad = "abandon abandon abandon abandon abandon abandon abandon abandon abandon \
                   abandon abandon abandon";
        let result = restore_mnemonic_into(&db, bad, Path::new("/tmp/client.db")).await;
        assert!(result.is_err(), "an invalid checksum must be rejected");

        // Nothing was persisted, so the store is still seedless (a later valid import can proceed).
        assert!(Client::load_decodable_client_secret_opt::<Vec<u8>>(&db)
            .await
            .expect("seed reads")
            .is_none());
    }

    #[tokio::test]
    async fn restore_rejects_a_valid_mnemonic_that_is_not_twelve_words() {
        let db = mem_db();
        let twenty_four_words = Mnemonic::from_entropy(&[2u8; 32])
            .expect("32 bytes produce a valid 24-word mnemonic")
            .to_string();

        let result =
            restore_mnemonic_into(&db, &twenty_four_words, Path::new("/tmp/client.db")).await;
        assert!(result
            .expect_err("restore accepts exactly the wallet's 12-word seed format")
            .to_string()
            .contains("12 words"));
        assert!(Client::load_decodable_client_secret_opt::<Vec<u8>>(&db)
            .await
            .expect("seed reads")
            .is_none());
    }

    #[tokio::test]
    async fn restore_persists_a_valid_mnemonic_into_an_empty_store() {
        let db = mem_db();
        let phrase = valid_phrase();
        restore_mnemonic_into(&db, &phrase, Path::new("/tmp/client.db"))
            .await
            .expect("a valid phrase into an empty store succeeds");

        let stored = Client::load_decodable_client_secret_opt::<Vec<u8>>(&db)
            .await
            .expect("seed reads")
            .expect("seed present");
        assert_eq!(
            stored,
            Mnemonic::from_entropy(&[0u8; 16]).unwrap().to_entropy()
        );
    }
}
