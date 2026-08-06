//! Localhost HTTP sidecar skeleton and fail-closed provisioning (§6c.9 step 1).

mod config;
mod password;

use anyhow::{ensure, Context, Result};
use clap::{Args, Parser, Subcommand};
use password::ValidatedPassword;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

/// Fixed by §6c.1, not configurable; loopback is not an authentication boundary.
const BIND_ADDR: Ipv4Addr = Ipv4Addr::LOCALHOST;

#[derive(Parser)]
#[command(
    name = "wallet-web",
    about = "The localhost web frontend for the fedimint wallet daemon"
)]
struct Cli {
    /// Config path; defaults to `~/.config/wallet-web/wallet-web.toml`.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Prompt without echo and write a complete new 0600 config.
    Init(InitArgs),
}

#[derive(Args)]
struct InitArgs {
    /// Loopback port; the address is fixed to 127.0.0.1.
    #[arg(long, default_value_t = config::DEFAULT_PORT)]
    port: u16,

    /// walletd URL: http:// at a loopback IP literal (not localhost).
    #[arg(long, default_value = "http://127.0.0.1:9736")]
    daemon_url: String,

    /// Path to walletd's 0600 bearer token file.
    #[arg(long)]
    token_path: String,

    /// Browser-facing scheme, host, and port; never derived from Host.
    #[arg(long)]
    public_origin: String,

    /// Sliding idle session timeout. May only tighten ADR-0028's 4h ceiling.
    #[arg(long, default_value = config::DEFAULT_IDLE_TIMEOUT)]
    session_idle_timeout: String,

    /// Absolute session cap. May only tighten ADR-0028's 24h ceiling.
    #[arg(long, default_value = config::DEFAULT_ABSOLUTE_TIMEOUT)]
    session_absolute_timeout: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing();
    let config_path = match cli.config {
        Some(path) => path,
        None => config::default_config_path()?,
    };
    match cli.command {
        Some(Command::Init(args)) => run_init(&config_path, args),
        None => run_serve(&config_path).await,
    }
}

fn run_init(config_path: &Path, args: InitArgs) -> Result<()> {
    // Refuse BEFORE the prompt. `config::init` refuses to clobber an existing config anyway, but
    // discovering that after typing and confirming a password with no echo makes an operator
    // rotating a password type it twice for nothing. That check stays where it is — it is the
    // guarantee; this one only moves the failure to where it costs least.
    ensure!(
        !config_path.exists(),
        "sidecar config {} already exists; init writes a complete config and would reset any \
         tightened session timeout to its ceiling. Remove the file to re-provision",
        config_path.display()
    );
    let password = prompt_password()?;
    let raw = config::RawConfig {
        port: args.port,
        daemon_url: args.daemon_url,
        token_path: args.token_path,
        password_hash: Some(password::hash(&password)?),
        session_idle_timeout: args.session_idle_timeout,
        session_absolute_timeout: args.session_absolute_timeout,
        public_origin: args.public_origin,
    };
    config::init(config_path, raw)?;
    eprintln!(
        "wrote {} (0600). Start the sidecar with `wallet-web`.",
        config_path.display()
    );
    Ok(())
}

/// `rpassword` reads the controlling terminal, never stdin, and disables echo.
fn prompt_password() -> Result<ValidatedPassword> {
    let terminal_hint = "`wallet-web init` prompts on the terminal, so it needs an interactive \
                         one; the password is deliberately not read from stdin";
    let first = prompt_password_line("New wallet-web password: ")
        .with_context(|| format!("reading the password with echo disabled ({terminal_hint})"))?;
    let confirmed = prompt_password_line("Confirm password: ")
        .with_context(|| format!("reading the confirmation ({terminal_hint})"))?;
    ensure!(first == confirmed, "the two passwords do not match");
    ValidatedPassword::new(first)
}

/// `rpassword` 7.5.4 clears `ISIG` and then raises SIGINT itself on Ctrl-C, from inside
/// `read_password` while the guard that restores termios is still live (`rpassword-7.5.4/src/
/// unix.rs`: `apply_terminal_configuration`, `send_signal_sigint`). Under the default disposition
/// that kills the process before the guard runs, leaving the operator's terminal with no echo, no
/// line editing, and no working Ctrl-C. Ignoring SIGINT across the prompt drops that raise on the
/// floor; `read_password` then returns `Interrupted`, its guard restores the terminal, and `init`
/// aborts having written nothing.
fn prompt_password_line(prompt: &str) -> std::io::Result<String> {
    let previous = unsafe { libc::signal(libc::SIGINT, libc::SIG_IGN) };
    let result = rpassword::prompt_password(prompt);
    if previous != libc::SIG_ERR {
        unsafe { libc::signal(libc::SIGINT, previous) };
    }
    result
}

/// Validate every fail-closed condition before binding.
async fn run_serve(config_path: &Path) -> Result<()> {
    let config = config::load(config_path)?;
    let listener = tokio::net::TcpListener::bind((BIND_ADDR, config.port))
        .await
        .with_context(|| format!("binding {BIND_ADDR}:{}", config.port))?;
    tracing::info!(
        address = %listener.local_addr().context("reading the listener address")?,
        daemon_url = %config.daemon_url,
        public_origin = %config.public_origin,
        token_path = %config.token_path.display(),
        session_idle_timeout_s = config.session_idle_timeout.as_secs(),
        session_absolute_timeout_s = config.session_absolute_timeout.as_secs(),
        "wallet-web listening",
    );
    // Step 1 has no routes, including no placeholder or health check.
    axum::serve(listener, axum::Router::new())
        .await
        .context("serving wallet-web")
}

/// Structured stderr tracing; `RUST_LOG` is deliberately not a config key.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}
