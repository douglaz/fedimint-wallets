//! Closed `wallet-web.toml` schema and fail-closed startup validation (§6c.1/2/6).

use crate::password;
use anyhow::{anyhow, bail, ensure, Context, Result};
use axum::http::Uri;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DEFAULT_PORT: u16 = 9737;

/// A live session is full wallet control, so config may only tighten ADR-0028's ceilings.
pub const MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
pub const MAX_ABSOLUTE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// The `init` defaults are the ceilings themselves; `wallet-web init` is their only source, so
/// there is no serde-side default layer that could drift away from these.
pub const DEFAULT_IDLE_TIMEOUT: &str = "4h";
pub const DEFAULT_ABSOLUTE_TIMEOUT: &str = "24h";

/// The on-disk inventory is closed; notably, bind address and log level are not config. Every key
/// but the hash is required: `init` always writes a complete file, so a missing key means a
/// hand-edited config, and inventing a value for it is exactly the silent widening §6c.2 forbids.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    pub port: u16,
    pub daemon_url: String,
    pub token_path: String,
    /// Absent fails closed: there is no default credential or setup page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_hash: Option<String>,
    pub session_idle_timeout: String,
    pub session_absolute_timeout: String,
    pub public_origin: String,
}

/// Resolved config; its only constructor runs every startup refusal.
#[derive(Clone, PartialEq, Eq)]
pub struct WebConfig {
    pub port: u16,
    pub daemon_url: String,
    pub token_path: PathBuf,
    #[allow(dead_code)]
    pub password_hash: String,
    pub session_idle_timeout: Duration,
    pub session_absolute_timeout: Duration,
    pub public_origin: String,
}

/// `Debug` is written by hand on both config types so `password_hash` can never reach a log line,
/// a span field, or an error message through a `{:?}`. `ValidatedPassword` goes out of its way to
/// make the plaintext unprintable; a derived `Debug` on the struct holding the PHC string would
/// undo half of that the first time a later bead threads `WebConfig` into handler state.
macro_rules! redacted_debug {
    ($type:ty, $($field:ident),+) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($type))
                    $(.field(stringify!($field), &self.$field))+
                    .field("password_hash", &"<redacted>")
                    .finish()
            }
        }
    };
}

redacted_debug!(
    RawConfig,
    port,
    daemon_url,
    token_path,
    session_idle_timeout,
    session_absolute_timeout,
    public_origin
);
redacted_debug!(
    WebConfig,
    port,
    daemon_url,
    token_path,
    session_idle_timeout,
    session_absolute_timeout,
    public_origin
);

impl WebConfig {
    fn from_raw(raw: RawConfig) -> Result<Self> {
        let password_hash = raw
            .password_hash
            .filter(|hash| !hash.trim().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "no password hash configured — run `wallet-web init`. The sidecar fails \
                     closed: there is no default credential and no first-load setup page \
                     (ADR-0028)"
                )
            })?;
        password::validate_phc(&password_hash)?;
        Ok(Self {
            port: raw.port,
            daemon_url: validate_daemon_url(&raw.daemon_url)?,
            token_path: validate_token_path(&raw.token_path)?,
            password_hash,
            session_idle_timeout: bounded_timeout(
                "session_idle_timeout",
                &raw.session_idle_timeout,
                MAX_IDLE_TIMEOUT,
            )?,
            session_absolute_timeout: bounded_timeout(
                "session_absolute_timeout",
                &raw.session_absolute_timeout,
                MAX_ABSOLUTE_TIMEOUT,
            )?,
            public_origin: validate_public_origin(&raw.public_origin)?,
        })
    }
}

/// Startup calls this before binding, so every error is a refusal to start.
pub fn load(config_path: &Path) -> Result<WebConfig> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = std::fs::metadata(config_path).with_context(|| {
        format!(
            "reading sidecar config {} (run `wallet-web init` first)",
            config_path.display()
        )
    })?;
    // This file is the sole store of the password hash, and that password fronts
    // seed-recovery-level capability (ADR-0028). `init` writes it 0600; if it is not 0600 now,
    // another local account could already have lifted the hash for offline guessing — or replaced
    // it with one it knows. Re-checking every startup is the point: init ran once, months ago.
    let mode = metadata.permissions().mode() & 0o777;
    ensure!(
        mode & 0o077 == 0,
        "sidecar config {} is mode {mode:o}: it holds the password hash and must not be readable \
         or writable by group or other. `chmod 600` it, or re-run `wallet-web init`",
        config_path.display()
    );
    // The mode above protects the file; this protects the NAME. Directory write permission lets
    // another account rename their own 0600 file over this one, and a 0600 attacker-owned config
    // passes every check above it.
    ensure_config_dir_not_replaceable(config_parent(config_path))?;
    let text = std::fs::read_to_string(config_path)
        .with_context(|| format!("reading sidecar config {}", config_path.display()))?;
    let raw: RawConfig = toml::from_str(&text)
        .with_context(|| format!("parsing sidecar config {}", config_path.display()))?;
    WebConfig::from_raw(raw)
}

/// Validate through the startup path, then atomically write the config 0600.
pub fn init(config_path: &Path, raw: RawConfig) -> Result<WebConfig> {
    let config = WebConfig::from_raw(raw.clone())?;
    // `init` writes a COMPLETE config from its arguments, so silently replacing an existing one
    // would reset a tightened `session_idle_timeout` back to the ADR-0028 ceiling — the widening
    // §6c.2 forbids, performed by the command an operator runs to ROTATE a password.
    ensure!(
        !config_path.exists(),
        "sidecar config {} already exists; init writes a complete config and would reset any \
         tightened session timeout to its ceiling. Remove the file to re-provision",
        config_path.display()
    );
    ensure_private_config_dir(config_parent(config_path))?;
    let serialized = toml::to_string_pretty(&raw).context("serializing sidecar config")?;
    write_secret_file(config_path, serialized.as_bytes())
        .with_context(|| format!("writing sidecar config {}", config_path.display()))?;
    Ok(config)
}

/// The directory the config file lands in. `--config wallet-web.toml` has a parent, but it is the
/// EMPTY path, which no filesystem call accepts; the current directory is what it means.
fn config_parent(config_path: &Path) -> &Path {
    match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Create a missing sidecar-owned config directory as 0700, and refuse an existing one that
/// another local account can write to: directory write permission lets them replace the 0600
/// config, and its password hash, by rename. An existing safe directory keeps the mode the
/// operator gave it — unlike walletd's own data dir, `--config` may point somewhere the sidecar
/// does not own, and forcing 0700 there would lock out its other users.
fn ensure_private_config_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    if path.exists() {
        return ensure_config_dir_not_replaceable(path);
    }

    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
        .create(path)
        .with_context(|| format!("creating config directory {}", path.display()))?;
    // `.mode()` is umask-limited; this re-assert is not.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("setting private permissions on {}", path.display()))?;
    Ok(())
}

/// Refuse a config directory another local account can write to. Checked at every STARTUP, not
/// only at `init`, for exactly the reason the file's own mode is: `init` blessed this directory
/// once, months ago, and it may be group-writable now. Without the startup check an attacker who
/// gains write access installs their own 0600 config carrying their own Argon2id hash — the file
/// mode check passes, `validate_phc` passes, and the sidecar authenticates against a password
/// they chose, which is full wallet control including `/v1/recover` (ADR-0028 "Consequences").
fn ensure_config_dir_not_replaceable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = std::fs::metadata(path)
        .with_context(|| format!("reading config directory {}", path.display()))?
        .permissions()
        .mode();
    // The sticky bit is the `/tmp` case: writable by everyone, but only an entry's owner may
    // replace it, which is exactly the property being checked for.
    ensure!(
        mode & 0o022 == 0 || mode & 0o1000 != 0,
        "config directory {} is mode {:o}: another local account could replace the config \
         there, and with it the password hash the sidecar authenticates against",
        path.display(),
        mode & 0o7777
    );
    Ok(())
}

pub fn default_config_path() -> Result<PathBuf> {
    let home = if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        xdg
    } else {
        match std::env::var_os("HOME").filter(|value| !value.is_empty()) {
            Some(home) => PathBuf::from(home).join(".config"),
            None => bail!("HOME is not set; wallet-web needs an absolute config directory"),
        }
    };
    Ok(home.join("wallet-web").join("wallet-web.toml"))
}

/// Parse as `SocketAddr`: only `http://` loopback IP literals with explicit ports pass. Returns the
/// bare origin, which is what a request URL is built from.
fn validate_daemon_url(raw: &str) -> Result<String> {
    let rest = raw.strip_prefix("http://").ok_or_else(|| {
        anyhow!("daemon_url {raw:?} must use http:// with a loopback IP literal (§6c.1)")
    })?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    // Later beads reach the daemon by appending `/v1/...` to this value, so anything after the
    // authority silently corrupts every request rather than failing here.
    let trailing = &rest[authority.len()..];
    ensure!(
        trailing.is_empty() || trailing == "/",
        "daemon_url {raw:?} must be a bare origin with no path, query, or fragment: request paths \
         are appended to it"
    );
    let socket: SocketAddr = authority.parse().map_err(|_| {
        anyhow!(
            "daemon_url {raw:?} authority {authority:?} is not an IP literal and port; DNS names, \
             including localhost, are refused (§6c.1)"
        )
    })?;
    match socket.ip() {
        IpAddr::V4(address) => ensure!(
            address.is_loopback(),
            "daemon_url {raw:?} host {address} is not in 127.0.0.0/8"
        ),
        IpAddr::V6(address) => ensure!(
            address == Ipv6Addr::LOCALHOST,
            "daemon_url {raw:?} host [{address}] is not the IPv6 loopback ::1"
        ),
    }
    Ok(format!("http://{authority}"))
}

/// The sidecar is long-lived, so a CWD-relative token path resolves against wherever it happened
/// to be started; `walletd` refuses the same shape for the same reason
/// (`wallet-daemon/src/config.rs::resolve_path`).
///
/// `~` IS expanded, matching that same function. This file belongs to `walletd`, and
/// `token_path = "~/.local/share/walletd/token"` is valid in `walletd.toml`; refusing it here
/// would make one spelling of one path work in one config file and fail in the other, which an
/// operator hits by copying the value across.
fn validate_token_path(raw: &str) -> Result<PathBuf> {
    let path = match raw.strip_prefix("~/") {
        Some(rest) => home_dir()?.join(rest),
        None if raw == "~" => home_dir()?,
        None => PathBuf::from(raw),
    };
    ensure!(
        path.is_absolute(),
        "token_path {raw:?} must resolve to an absolute path to walletd's 0600 bearer token; a \
         long-lived sidecar must not anchor it to a working directory"
    );
    Ok(path)
}

fn home_dir() -> Result<PathBuf> {
    match std::env::var_os("HOME").filter(|value| !value.is_empty()) {
        Some(home) => Ok(PathBuf::from(home)),
        None => bail!("HOME is not set; wallet-web cannot expand a ~-prefixed path"),
    }
}

/// Parse and canonicalize the explicit public origin; it is never derived from `Host`.
fn validate_public_origin(raw: &str) -> Result<String> {
    let uri: Uri = raw
        .parse()
        .with_context(|| format!("public_origin {raw:?} is not a valid URI origin"))?;
    let scheme = uri.scheme_str().ok_or_else(|| {
        anyhow!("public_origin {raw:?} must be a full origin, e.g. \"http://127.0.0.1:9737\"")
    })?;
    ensure!(
        matches!(scheme, "http" | "https"),
        "public_origin {raw:?} must use the http or https scheme"
    );
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow!("public_origin {raw:?} has no host"))?;
    ensure!(
        !authority.as_str().contains('@') && uri.path() == "/" && uri.query().is_none(),
        "public_origin {raw:?} must be a bare origin (scheme, host, port) with no path, query, \
         fragment, or userinfo — it is compared against Origin/Referer exactly"
    );
    let host = authority.host();
    ensure!(!host.is_empty(), "public_origin {raw:?} has no host");
    // `http::Authority` reports "no port" and "a port that is not a number" identically, both as
    // `port_u16() == None`. Read the port section off the authority text instead, so a typo like
    // `wallet.example:notaport` is refused rather than silently read as the scheme's default.
    let port_text = authority
        .as_str()
        .strip_prefix(host)
        .and_then(|rest| rest.strip_prefix(':'));
    let explicit_port = match port_text {
        Some(text) => Some(
            text.parse::<u16>()
                .with_context(|| format!("public_origin {raw:?} port {text:?} is not a number"))?,
        ),
        None => None,
    };
    let host = match host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        Some(ip) => format!(
            "[{}]",
            ip.parse::<Ipv6Addr>()
                .with_context(|| format!("public_origin {raw:?} has an invalid IPv6 host"))?
        ),
        None => host.to_ascii_lowercase(),
    };
    // An absent port MEANS the scheme's default, and both spellings serialize to the same origin:
    // a browser sends `Origin: https://wallet.example` for `https://wallet.example` and for
    // `https://wallet.example:443` alike. Requiring an explicit port would refuse the natural
    // thing an operator writes behind a TLS proxy — and would refuse this function's OWN output,
    // since the default port is elided just below. A canonical form has to be idempotent, or the
    // value stored in the config is one the next startup rejects.
    let default_port = if scheme == "https" { 443 } else { 80 };
    let port = match explicit_port.unwrap_or(default_port) {
        port if port == default_port => String::new(),
        port => format!(":{port}"),
    };
    Ok(format!("{scheme}://{host}{port}"))
}

/// Zero is allowed because immediate expiry is the fail-closed direction.
fn bounded_timeout(key: &str, raw: &str, ceiling: Duration) -> Result<Duration> {
    let value = parse_duration(raw).with_context(|| format!("{key} = {raw:?}"))?;
    ensure!(
        value <= ceiling,
        "{key} = {raw:?} exceeds the {}h ceiling ADR-0028 fixes; configuration may only tighten \
         the session window, never widen it (§6c.2)",
        ceiling.as_secs() / 3600
    );
    Ok(value)
}

/// Parse the deliberately small `<count><s|m|h>` config grammar.
fn parse_duration(raw: &str) -> Result<Duration> {
    let text = raw.trim();
    let (digits, seconds_per_unit) = if let Some(rest) = text.strip_suffix('h') {
        (rest, 3600)
    } else if let Some(rest) = text.strip_suffix('m') {
        (rest, 60)
    } else if let Some(rest) = text.strip_suffix('s') {
        (rest, 1)
    } else {
        bail!("expected a duration like \"4h\", \"30m\", or \"90s\"");
    };
    let count: u64 = digits
        .parse()
        .map_err(|_| anyhow!("expected a whole count before the unit, got {digits:?}"))?;
    count
        .checked_mul(seconds_per_unit)
        .map(Duration::from_secs)
        .context("duration overflows")
}

/// Atomic 0600 write with mode re-asserted after open, matching walletd's secret-file shape.
fn write_secret_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    let mut tmp_path = path.as_os_str().to_owned();
    tmp_path.push(".tmp");
    let tmp_path = PathBuf::from(tmp_path);
    let _ = std::fs::remove_file(&tmp_path);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)?;
    // `.mode()` is umask-limited; this re-assert is not.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp_path, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::password::{MIN_M_COST, MIN_P_COST, MIN_T_COST};
    use argon2::Algorithm;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn scratch() -> PathBuf {
        let unique = format!(
            "wallet-web-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("make scratch dir private");
        dir
    }

    /// Startup refuses a config that is readable or writable beyond its owner, so every test
    /// fixture is written the way `init` writes it.
    fn write_config_0600(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(path, body).expect("write");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }

    fn valid_raw() -> RawConfig {
        RawConfig {
            port: DEFAULT_PORT,
            daemon_url: "http://127.0.0.1:9736".to_owned(),
            token_path: "/var/lib/walletd/token".to_owned(),
            password_hash: Some(password::test_phc(
                Algorithm::Argon2id,
                MIN_M_COST,
                MIN_T_COST,
                MIN_P_COST,
            )),
            session_idle_timeout: DEFAULT_IDLE_TIMEOUT.to_owned(),
            session_absolute_timeout: DEFAULT_ABSOLUTE_TIMEOUT.to_owned(),
            public_origin: "http://127.0.0.1:9737".to_owned(),
        }
    }

    fn load_with(mutate: impl FnOnce(&mut RawConfig)) -> Result<WebConfig> {
        let mut raw = valid_raw();
        mutate(&mut raw);
        let path = scratch().join("wallet-web.toml");
        write_config_0600(&path, &toml::to_string_pretty(&raw).expect("serialize"));
        load(&path)
    }

    /// `umask` is global; one test flips it and all test-created directory modes are re-asserted.
    #[test]
    fn init_writes_the_config_0600_under_a_permissive_umask() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        let path = scratch().join("nested").join("wallet-web.toml");
        let previous = unsafe { libc::umask(0o000) };
        let result = init(&path, valid_raw());
        unsafe { libc::umask(previous) };
        result?;

        let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "wallet-web.toml must be 0600, got {mode:o}");
        let directory_mode = std::fs::metadata(path.parent().expect("config parent"))?
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            directory_mode, 0o700,
            "wallet-web config directory must be 0700, got {directory_mode:o}"
        );
        Ok(())
    }

    #[test]
    fn startup_refuses_a_config_with_no_password_hash() {
        let error = load_with(|raw| raw.password_hash = None).expect_err("started with no hash");
        assert!(
            error.to_string().contains("no password hash configured"),
            "unexpected error: {error}"
        );
        // An empty string is the same refusal: it is "configured" only in the file's shape.
        assert!(load_with(|raw| raw.password_hash = Some(String::new())).is_err());
    }

    #[test]
    fn startup_refuses_a_malformed_phc_string() {
        for malformed in [
            "not-a-phc-string",
            "$argon2id$v=19$m=19456,t=2,p=1",
            // The PHC parser accepts this 4-byte decoded salt, but Argon2 cannot verify it.
            "$argon2id$v=19$m=19456,t=2,p=1$YWJjZA$aGFzaGhhc2hoYXNoaGFzaA",
            "$argon2id$v=19$m=nineteen,t=2,p=1$c29tZXNhbHQ$aGFzaGhhc2hoYXNoaGFzaA",
        ] {
            let refused = load_with(|raw| raw.password_hash = Some(malformed.to_owned())).is_err();
            assert!(refused, "started on malformed PHC {malformed:?}");
        }
    }

    #[test]
    fn startup_refuses_argon2id_params_below_the_pinned_minimums() {
        let weak = [
            password::test_phc(Algorithm::Argon2id, MIN_M_COST - 1, MIN_T_COST, MIN_P_COST),
            password::test_phc(Algorithm::Argon2id, MIN_M_COST, MIN_T_COST - 1, MIN_P_COST),
            // p=0 is not expressible through the hasher, so this one is hand-written.
            "$argon2id$v=19$m=19456,t=2,p=0$c29tZXNhbHQ$aGFzaGhhc2hoYXNoaGFzaA".to_owned(),
            // Argon2 v=16 predates the version init pins and is refused even at strong costs.
            password::test_phc(Algorithm::Argon2id, MIN_M_COST, MIN_T_COST, MIN_P_COST)
                .replacen("v=19", "v=16", 1),
        ];
        for phc in weak {
            let refused = load_with(|raw| raw.password_hash = Some(phc.clone())).is_err();
            assert!(refused, "started on weak argon2 parameters {phc}");
        }
    }

    /// The ALGORITHM is fixed as Argon2id: a strong-looking `argon2i` or `argon2d` string passes
    /// every parameter check and must still be refused.
    #[test]
    fn startup_refuses_argon2i_and_argon2d_even_with_strong_params() {
        for algorithm in [Algorithm::Argon2i, Algorithm::Argon2d] {
            let phc = password::test_phc(algorithm, MIN_M_COST * 2, MIN_T_COST * 2, MIN_P_COST);
            let error = load_with(|raw| raw.password_hash = Some(phc))
                .expect_err("started on a non-argon2id hash");
            assert!(
                error.to_string().contains("only argon2id is accepted"),
                "unexpected error for {algorithm:?}: {error}"
            );
        }
    }

    #[test]
    fn startup_refuses_a_non_loopback_daemon_url() {
        for url in [
            // Resolves to loopback today but is a name, not an IP literal.
            "http://localhost:9736",
            "http://192.168.1.5:9736",
            "https://127.0.0.1:9736",
        ] {
            let refused = load_with(|raw| raw.daemon_url = url.to_owned()).is_err();
            assert!(refused, "started with daemon_url {url:?}");
        }
    }

    #[test]
    fn unknown_config_field_is_rejected() {
        let path = scratch().join("wallet-web.toml");
        let mut body = toml::to_string_pretty(&valid_raw()).expect("serialize");
        // The key an implementation might grow next, and the one the posture forbids.
        body.push_str("\nbind_address = \"0.0.0.0\"\n");
        write_config_0600(&path, &body);
        assert!(load(&path).is_err());
    }

    /// The config is the only store of the password hash, so its mode is a startup condition, not
    /// only something `init` gets right once.
    #[test]
    fn startup_refuses_a_config_readable_or_writable_beyond_its_owner() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        let path = scratch().join("wallet-web.toml");
        let body = toml::to_string_pretty(&valid_raw()).expect("serialize");
        for mode in [0o644, 0o640, 0o606, 0o660] {
            write_config_0600(&path, &body);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
            let error = load(&path).expect_err("started on an exposed config");
            assert!(
                error.to_string().contains("must not be readable"),
                "unexpected error for mode {mode:o}: {error}"
            );
        }
        write_config_0600(&path, &body);
        load(&path)?;
        Ok(())
    }

    #[test]
    fn startup_refuses_a_token_path_that_is_not_absolute() {
        for token_path in ["", "relative/token", "./token"] {
            let refused = load_with(|raw| raw.token_path = token_path.to_owned()).is_err();
            assert!(refused, "started with token_path {token_path:?}");
        }
    }

    /// `walletd` expands `~` for this same file (`wallet-daemon/src/config.rs::resolve_path`), so
    /// refusing it here would make one spelling of one path valid in `walletd.toml` and fatal in
    /// `wallet-web.toml` — which an operator hits by copying the value across.
    #[test]
    fn startup_expands_a_tilde_token_path_the_way_walletd_does() -> Result<()> {
        let home = PathBuf::from(std::env::var_os("HOME").expect("HOME is set under cargo test"));
        let config = load_with(|raw| raw.token_path = "~/.local/share/walletd/token".to_owned())?;
        assert_eq!(config.token_path, home.join(".local/share/walletd/token"));
        Ok(())
    }

    /// The file's own mode protects its CONTENTS; the directory's mode protects its NAME. An
    /// attacker who can write the directory renames their own 0600 config, carrying their own
    /// Argon2id hash, over this one — and every check above the directory then passes, so the
    /// sidecar authenticates against a password they chose. `init` blessed this directory once;
    /// startup re-checks it for exactly the reason it re-checks the file.
    #[test]
    fn startup_refuses_a_config_directory_another_local_account_can_write() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = scratch();
        let path = dir.join("wallet-web.toml");
        write_config_0600(&path, &toml::to_string_pretty(&valid_raw())?);
        load(&path).context("a 0700 config directory must load")?;

        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777))?;
        let refused = load(&path);
        // Restore before asserting, so a failure here cannot leave a world-writable scratch dir.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        assert!(
            refused.is_err(),
            "started with the config directory world-writable"
        );
        Ok(())
    }

    /// Later beads build request URLs by appending to `daemon_url`, so it must be a bare origin.
    #[test]
    fn startup_refuses_a_daemon_url_carrying_a_path_query_or_fragment() -> Result<()> {
        for url in [
            "http://127.0.0.1:9736/v1/../evil",
            "http://127.0.0.1:9736?a=b",
            "http://127.0.0.1:9736#frag",
        ] {
            let refused = load_with(|raw| raw.daemon_url = url.to_owned()).is_err();
            assert!(refused, "started with daemon_url {url:?}");
        }
        // A bare trailing slash is the one accepted form, stored without it.
        let config = load_with(|raw| raw.daemon_url = "http://127.0.0.1:9736/".to_owned())?;
        assert_eq!(config.daemon_url, "http://127.0.0.1:9736");
        Ok(())
    }

    /// Re-running `init` to rotate a password must not silently widen a tightened session window.
    #[test]
    fn init_refuses_to_overwrite_an_existing_config() -> Result<()> {
        let path = scratch().join("wallet-web.toml");
        init(
            &path,
            RawConfig {
                session_idle_timeout: "15m".to_owned(),
                ..valid_raw()
            },
        )?;
        let error = init(&path, valid_raw()).expect_err("clobbered an existing config");
        assert!(
            error.to_string().contains("already exists"),
            "unexpected error: {error}"
        );
        assert_eq!(load(&path)?.session_idle_timeout, Duration::from_secs(900));
        Ok(())
    }

    /// `--config wallet-web.toml` has an EMPTY parent, which no filesystem call accepts.
    #[test]
    fn a_config_path_with_no_directory_component_means_the_current_directory() {
        assert_eq!(config_parent(Path::new("wallet-web.toml")), Path::new("."));
        assert_eq!(
            config_parent(Path::new("/etc/wallet-web/wallet-web.toml")),
            Path::new("/etc/wallet-web")
        );
        assert_eq!(
            config_parent(Path::new("dir/wallet-web.toml")),
            Path::new("dir")
        );
    }

    #[test]
    fn startup_refuses_session_timeouts_above_the_adr_ceilings() {
        assert!(load_with(|raw| raw.session_idle_timeout = "5h".to_owned()).is_err());
        assert!(load_with(|raw| raw.session_absolute_timeout = "1500m".to_owned()).is_err());
    }

    #[test]
    fn startup_refuses_an_unparseable_session_timeout() -> Result<()> {
        for value in ["", "4", "four hours", "4 h", "-1h", "4d", "h"] {
            let refused = load_with(|raw| raw.session_idle_timeout = value.to_owned()).is_err();
            assert!(refused, "started with session_idle_timeout {value:?}");
        }
        // Tightening the window is always allowed.
        let config = load_with(|raw| {
            raw.session_idle_timeout = "15m".to_owned();
            raw.session_absolute_timeout = "3600s".to_owned();
        })?;
        assert_eq!(config.session_idle_timeout, Duration::from_secs(900));
        assert_eq!(config.session_absolute_timeout, Duration::from_secs(3600));
        Ok(())
    }

    #[test]
    fn startup_refuses_a_public_origin_that_is_not_a_bare_origin() -> Result<()> {
        for origin in [
            "127.0.0.1:9737",
            "ftp://127.0.0.1:9737",
            "https://wallet.example/wallet",
            "https://user:pass@wallet.example",
            "https://",
            "https://:443",
            "https://wallet.example:notaport",
            "https://wallet.example:443:444",
            "https://wallet example:443",
            "https://[::1:443",
        ] {
            let refused = load_with(|raw| raw.public_origin = origin.to_owned()).is_err();
            assert!(refused, "started with public_origin {origin:?}");
        }
        let config = load_with(|raw| raw.public_origin = "https://Wallet.EXAMPLE:443".to_owned())?;
        assert_eq!(config.public_origin, "https://wallet.example");
        let config = load_with(|raw| raw.public_origin = "http://[0:0::1]:9737/".to_owned())?;
        assert_eq!(config.public_origin, "http://[::1]:9737");
        Ok(())
    }

    /// A canonical form that its own validator rejects is not a canonical form. An absent port
    /// MEANS the scheme's default and a browser serializes both spellings identically, so an
    /// operator behind a TLS proxy must be able to write the bare origin — and the value this
    /// function stores must survive being read back.
    #[test]
    fn public_origin_canonicalization_is_idempotent_and_admits_a_bare_origin() -> Result<()> {
        for (written, canonical) in [
            ("https://wallet.example", "https://wallet.example"),
            ("https://wallet.example:443", "https://wallet.example"),
            ("http://wallet.example", "http://wallet.example"),
            ("http://wallet.example:80", "http://wallet.example"),
            ("http://127.0.0.1:9737", "http://127.0.0.1:9737"),
            ("https://wallet.example:8443", "https://wallet.example:8443"),
        ] {
            let config = load_with(|raw| raw.public_origin = written.to_owned())?;
            assert_eq!(
                config.public_origin, canonical,
                "canonicalizing {written:?}"
            );
            let again = load_with(|raw| raw.public_origin = canonical.to_owned())?;
            assert_eq!(
                again.public_origin, canonical,
                "canonical form {canonical:?} must itself be accepted unchanged"
            );
        }
        Ok(())
    }
}
