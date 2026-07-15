//! `walletd.toml` — HOST/deployment config ONLY (spec §6a.6 "who decides it"): data dir, bind
//! address/port, token file path, log level. Nothing the USER decides lives here — targets,
//! caps, fees, and cadences are `Policy`, stored in the DB and mutated via `PUT /v1/policy`.
//!
//! Also owns the `walletd init` filesystem scaffolding: the host config, the 0600 bearer token,
//! and the `~/.config/walletd/` client pointer (URL + token path) that step 6's CLI reads.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Owner-ratified default bind (Lightning's 9735 + 1).
pub const DEFAULT_ADDRESS: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 9736;
const DEFAULT_LOG_LEVEL: &str = "info";

/// The one documented env override (spec §6a.0: "env override for the devimint gates only"):
/// points the daemon and the CLI at a bearer token file outside the default location. Not a
/// general env-config layer — this single knob and nothing else.
pub const TOKEN_PATH_ENV: &str = "WALLETD_TOKEN_PATH";

/// Process-global env vars are shared across the daemon's parallel unit tests, including tests
/// outside this module. Every test that reads or mutates the token override holds this lock.
#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The resolved host config: every path absolute (a daemon needs an absolute home, never a
/// CWD-relative one), every value defaulted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletdConfig {
    pub data_dir: PathBuf,
    pub address: String,
    pub port: u16,
    pub token_path: PathBuf,
    pub log_level: String,
    /// Optional lnv2 gateway URL pinning EVERY route the daemon resolves. Host config — which
    /// gateway is reachable is a deployment fact, not user policy. Required for the devimint
    /// gates (its LDK gateway is never registered into the lnv2 set, runbook §4); production
    /// deployments normally omit it and routes resolve from each federation's registered list.
    pub gateway: Option<String>,
}

/// The on-disk `walletd.toml` shape. Every field optional so an operator writes only what they
/// override; [`WalletdConfig`] fills the rest from the owner-ratified defaults.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    data_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gateway: Option<String>,
}

impl WalletdConfig {
    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("client.db")
    }

    /// The app journal's own store, SEPARATE from the fedimint clients' `client.db` — a
    /// co-located journal's write churn flushes fedimint's tiny (2MB, no-history) memtable
    /// out from under its long-held lnv2 transactions, panicking their commits (the 24h-soak
    /// wedge). Never merge these back into one RocksDB.
    pub fn journal_db_path(&self) -> PathBuf {
        self.data_dir.join("journal.db")
    }

    pub fn bind(&self) -> String {
        format!("{}:{}", authority_host(&self.address), self.port)
    }

    pub fn url(&self) -> String {
        format!("http://{}:{}", authority_host(&self.address), self.port)
    }

    fn from_raw(raw: RawConfig) -> Result<Self> {
        let data_dir = match raw.data_dir {
            Some(dir) => resolve_path(&dir)?,
            None => default_data_dir()?,
        };
        // The env override wins over the file, then the file, then `<data_dir>/token`.
        let token_path = match std::env::var(TOKEN_PATH_ENV).ok().filter(|v| !v.is_empty()) {
            Some(env_path) => resolve_path(&env_path)?,
            None => match raw.token_path {
                Some(path) => resolve_path(&path)?,
                None => data_dir.join("token"),
            },
        };
        Ok(Self {
            data_dir,
            address: raw.address.unwrap_or_else(|| DEFAULT_ADDRESS.to_owned()),
            port: raw.port.unwrap_or(DEFAULT_PORT),
            token_path,
            log_level: raw
                .log_level
                .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_owned()),
            gateway: raw.gateway,
        })
    }

    /// Serialize the resolved config back to `walletd.toml` shape (all values explicit), so a
    /// re-scaffold canonicalizes the file without ever inventing a CWD-relative path.
    fn to_raw(&self) -> RawConfig {
        RawConfig {
            data_dir: Some(self.data_dir.display().to_string()),
            address: Some(self.address.clone()),
            port: Some(self.port),
            token_path: Some(self.token_path.display().to_string()),
            log_level: Some(self.log_level.clone()),
            gateway: self.gateway.clone(),
        }
    }
}

/// Parse an existing `walletd.toml` into a resolved config. Errors if the file is missing —
/// the caller points the operator at `walletd init`.
pub fn load(config_path: &Path) -> Result<WalletdConfig> {
    let text = std::fs::read_to_string(config_path).with_context(|| {
        format!(
            "reading host config {} (run `walletd init` first)",
            config_path.display()
        )
    })?;
    let raw: RawConfig = toml::from_str(&text)
        .with_context(|| format!("parsing host config {}", config_path.display()))?;
    WalletdConfig::from_raw(raw)
}

/// `walletd init`: read-or-default the host config, canonicalize it back to disk (preserving
/// any operator edits — a re-init never resets a set field to a default), and return the
/// resolved config. Idempotent.
pub fn scaffold_config(config_path: &Path) -> Result<WalletdConfig> {
    let raw = match std::fs::read_to_string(config_path) {
        Ok(text) => toml::from_str(&text)
            .with_context(|| format!("parsing existing host config {}", config_path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => RawConfig::default(),
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", config_path.display()))
        }
    };
    let config = WalletdConfig::from_raw(raw)?;
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config directory {}", parent.display()))?;
    }
    let serialized = toml::to_string_pretty(&config.to_raw()).context("serializing host config")?;
    std::fs::write(config_path, serialized)
        .with_context(|| format!("writing host config {}", config_path.display()))?;
    Ok(config)
}

/// Generate a fresh bearer token and write it to `token_path` with 0600 permissions,
/// overwriting any prior token (rotation). The parent directory is created if absent.
pub fn rotate_token(config: &WalletdConfig) -> Result<String> {
    use rand::RngCore as _;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token = hex_lower(&bytes);
    if let Some(parent) = config.token_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating token directory {}", parent.display()))?;
    }
    write_secret_file(&config.token_path, token.as_bytes())
        .with_context(|| format!("writing token {}", config.token_path.display()))?;
    Ok(token)
}

/// Read the bearer token the serving daemon authenticates against.
pub fn read_token(config: &WalletdConfig) -> Result<String> {
    let token = std::fs::read_to_string(&config.token_path).with_context(|| {
        format!(
            "reading bearer token {} (run `walletd init`)",
            config.token_path.display()
        )
    })?;
    let token = token.trim().to_owned();
    if token.is_empty() {
        bail!("bearer token file {} is empty", config.token_path.display());
    }
    Ok(token)
}

/// Create the wallet store directory and make it private to the daemon's OS user. The
/// persisted client secret lives below this directory, so the usual umask-derived `0755`
/// directory mode would expose wallet material to other local users on multi-user hosts.
pub fn ensure_private_data_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("creating data dir {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("setting private permissions on {}", path.display()))?;
    }
    Ok(())
}

/// Write the `~/.config/walletd/client.toml` pointer (daemon URL + token path) so step 6's CLI
/// finds the running daemon. Returns the pointer path.
pub fn write_client_pointer(config: &WalletdConfig) -> Result<PathBuf> {
    let home = config_home()?;
    std::fs::create_dir_all(&home)
        .with_context(|| format!("creating client config directory {}", home.display()))?;
    let pointer = home.join("client.toml");
    let body = ClientPointer {
        url: config.url(),
        token_path: config.token_path.display().to_string(),
    };
    let serialized = toml::to_string_pretty(&body).context("serializing client pointer")?;
    std::fs::write(&pointer, serialized)
        .with_context(|| format!("writing client pointer {}", pointer.display()))?;
    Ok(pointer)
}

#[derive(Serialize, Deserialize)]
struct ClientPointer {
    url: String,
    token_path: String,
}

/// `~/.config/walletd/walletd.toml` — the default host-config location.
pub fn default_config_path() -> Result<PathBuf> {
    Ok(config_home()?.join("walletd.toml"))
}

/// `$XDG_CONFIG_HOME/walletd` or `~/.config/walletd`.
fn config_home() -> Result<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return Ok(xdg.join("walletd"));
    }
    Ok(home_dir()?.join(".config").join("walletd"))
}

/// `$XDG_DATA_HOME/walletd` or `~/.local/share/walletd` (owner-ratified default data dir).
fn default_data_dir() -> Result<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return Ok(xdg.join("walletd"));
    }
    Ok(home_dir()?.join(".local").join("share").join("walletd"))
}

fn home_dir() -> Result<PathBuf> {
    match std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        Some(home) => Ok(PathBuf::from(home)),
        None => bail!("HOME is not set; a daemon needs an absolute home directory"),
    }
}

/// Expand a leading `~` to the home directory and require the result to be absolute — a
/// long-lived daemon must never anchor its store to a CWD-relative path (spec §6a.6).
fn resolve_path(raw: &str) -> Result<PathBuf> {
    let expanded = if raw == "~" {
        home_dir()?
    } else if let Some(rest) = raw.strip_prefix("~/") {
        home_dir()?.join(rest)
    } else {
        PathBuf::from(raw)
    };
    if !expanded.is_absolute() {
        bail!(
            "path {raw:?} resolves to a non-absolute path {}; use an absolute path or a ~-prefixed one",
            expanded.display()
        );
    }
    Ok(expanded)
}

/// Bracket IPv6 literals when they are used as a socket or URL authority. Hostnames, IPv4
/// literals, and already-bracketed IPv6 literals pass through unchanged.
fn authority_host(address: &str) -> String {
    if address.starts_with('[') && address.ends_with(']') {
        address.to_owned()
    } else if address.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{address}]")
    } else {
        address.to_owned()
    }
}

/// Atomically write a 0600 secret file: create a sibling temp with 0600, write + flush to disk,
/// then rename it over the target. Truncating the live token in place would let a crash between
/// truncate and write leave an EMPTY credential — and `read_token` fails closed on an empty file,
/// so a systemd `Restart=on-failure` daemon would crash-loop until a human re-runs init. The
/// rename makes a reader see either the old token or the new one, never a torn/empty one.
#[cfg(unix)]
fn write_secret_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;
    let tmp_path = {
        let mut name = path.file_name().unwrap_or_default().to_os_string();
        name.push(".tmp");
        match path.parent() {
            Some(parent) => parent.join(name),
            None => PathBuf::from(name),
        }
    };
    // Clear a leftover temp from a prior interrupted rotation, then create fresh (0600).
    let _ = std::fs::remove_file(&tmp_path);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)?;
    // Re-assert the mode so the temp is always 0600 regardless of umask.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    // Atomic replace: no window where the target is truncated/empty.
    std::fs::rename(&tmp_path, path)
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A unique scratch directory under the OS temp dir (no external tempfile dep).
    fn scratch() -> PathBuf {
        let unique = format!(
            "walletd-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    #[tokio::test]
    async fn absent_config_resolves_owner_ratified_defaults() -> Result<()> {
        let _env = TEST_ENV_LOCK.lock().await;
        let dir = scratch();
        // Point HOME at the scratch dir so ~-expansion is hermetic.
        std::env::set_var("HOME", &dir);
        std::env::remove_var("XDG_CONFIG_HOME");
        std::env::remove_var("XDG_DATA_HOME");
        std::env::remove_var(TOKEN_PATH_ENV);
        let config = WalletdConfig::from_raw(RawConfig::default())?;
        assert_eq!(config.address, DEFAULT_ADDRESS);
        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(config.log_level, "info");
        assert_eq!(config.data_dir, dir.join(".local/share/walletd"));
        assert_eq!(config.token_path, dir.join(".local/share/walletd/token"));
        Ok(())
    }

    #[tokio::test]
    async fn relative_xdg_homes_are_ignored() -> Result<()> {
        let _env = TEST_ENV_LOCK.lock().await;
        let dir = scratch();
        std::env::set_var("HOME", &dir);
        std::env::set_var("XDG_CONFIG_HOME", "relative-config");
        std::env::set_var("XDG_DATA_HOME", "relative-data");
        std::env::remove_var(TOKEN_PATH_ENV);

        let config = WalletdConfig::from_raw(RawConfig::default())?;
        assert_eq!(config.data_dir, dir.join(".local/share/walletd"));
        assert_eq!(
            default_config_path()?,
            dir.join(".config/walletd/walletd.toml")
        );
        assert!(config.data_dir.is_absolute());
        assert!(default_config_path()?.is_absolute());
        Ok(())
    }

    #[test]
    fn ipv6_bind_and_url_use_bracketed_authorities() {
        let config = WalletdConfig {
            data_dir: PathBuf::from("/tmp/walletd"),
            address: "::1".to_owned(),
            port: DEFAULT_PORT,
            token_path: PathBuf::from("/tmp/walletd/token"),
            log_level: "info".to_owned(),
            gateway: None,
        };

        assert_eq!(config.bind(), "[::1]:9736");
        assert_eq!(config.url(), "http://[::1]:9736");
    }

    #[tokio::test]
    async fn explicit_toml_values_parse_and_override() -> Result<()> {
        let _env = TEST_ENV_LOCK.lock().await;
        let dir = scratch();
        std::env::remove_var(TOKEN_PATH_ENV);
        let config_path = dir.join("walletd.toml");
        let data_dir = dir.join("data");
        let token_path = dir.join("secret.token");
        std::fs::write(
            &config_path,
            format!(
                "data_dir = {:?}\naddress = \"0.0.0.0\"\nport = 12345\ntoken_path = {:?}\nlog_level = \"debug\"\n",
                data_dir.display().to_string(),
                token_path.display().to_string()
            ),
        )?;
        let config = load(&config_path)?;
        assert_eq!(config.data_dir, data_dir);
        assert_eq!(config.address, "0.0.0.0");
        assert_eq!(config.port, 12345);
        assert_eq!(config.token_path, token_path);
        assert_eq!(config.log_level, "debug");
        Ok(())
    }

    #[test]
    fn unknown_config_field_is_rejected() {
        let dir = scratch();
        let config_path = dir.join("walletd.toml");
        std::fs::write(&config_path, "port = 9736\nmystery = true\n").unwrap();
        let error = load(&config_path).expect_err("unknown field accepted");
        assert!(error.to_string().contains("parsing host config"));
    }

    #[tokio::test]
    async fn token_path_env_override_wins() -> Result<()> {
        let _env = TEST_ENV_LOCK.lock().await;
        let dir = scratch();
        let override_path = dir.join("env.token");
        std::env::set_var(TOKEN_PATH_ENV, &override_path);
        let raw = RawConfig {
            data_dir: Some(dir.join("data").display().to_string()),
            token_path: Some(dir.join("file.token").display().to_string()),
            ..RawConfig::default()
        };
        let config = WalletdConfig::from_raw(raw)?;
        std::env::remove_var(TOKEN_PATH_ENV);
        assert_eq!(config.token_path, override_path);
        Ok(())
    }

    #[tokio::test]
    async fn init_scaffolds_0600_token_and_client_pointer_and_is_idempotent() -> Result<()> {
        let _env = TEST_ENV_LOCK.lock().await;
        let dir = scratch();
        std::env::set_var("HOME", &dir);
        std::env::set_var("XDG_CONFIG_HOME", dir.join("cfg"));
        std::env::remove_var(TOKEN_PATH_ENV);
        let config_path = dir.join("cfg/walletd/walletd.toml");

        let config = scaffold_config(&config_path)?;
        assert!(config_path.exists(), "walletd.toml scaffolded");
        let first_token = rotate_token(&config)?;
        let pointer = write_client_pointer(&config)?;

        // The token file is 0600.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&config.token_path)?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "token must be 0600, got {mode:o}");
        }
        assert_eq!(std::fs::read_to_string(&config.token_path)?, first_token);
        assert!(pointer.exists(), "client pointer written");
        let pointer_body = std::fs::read_to_string(&pointer)?;
        assert!(pointer_body.contains(&config.url()));

        // Re-init: the host config still parses to the same resolved config, and the token
        // rotates (a fresh secret) while paths are preserved.
        let reinit = scaffold_config(&config_path)?;
        assert_eq!(reinit, config, "re-init preserves resolved host config");
        let second_token = rotate_token(&reinit)?;
        assert_ne!(first_token, second_token, "re-init rotates the token");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn wallet_data_directory_is_forced_to_0700() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = scratch().join("data");
        std::fs::create_dir_all(&dir)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))?;

        ensure_private_data_dir(&dir)?;

        let mode = std::fs::metadata(&dir)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        Ok(())
    }
}
