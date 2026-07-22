//! The "sense" layer (Phase 2 step 2.1): turn each OPEN federation into the real
//! inputs the pure `wallet_core::scorer::score` / `wallet_core::allocator::decide`
//! consume (`docs/phase2-plan.md`).
//!
//! Split deliberately in two:
//! - a PURE assembler ([`assemble_facts`] / [`assemble_status`]) that maps a raw,
//!   serde-able [`ProbeResult`] into a `FederationFacts` / `FederationStatus`. No
//!   fedimint types, no I/O, no async — golden-tested from recorded fixtures, so it
//!   is in the fast rb-lite gate.
//! - an I/O [`FedimintProbeRunner`] that produces a [`ProbeResult`] from a live
//!   `MultiClient` client handle.
//!
//! **Probes are LIGHT — NO sats spent (decision).** Structural facts come from the
//! authenticated `ClientConfig` and are FREE (ADR-0019). The empirical fields use
//! light reachability + capability proxies (a timed threshold consensus read, a
//! registered gateway that answers `routing_info`, the presence of the wallet
//! module), NEVER a real receive/pay round-trip. That is why the two scorer inputs
//! that would normally
//! require an active probe — `round_trip_ok` and `peg_out_quotable` — are filled here
//! from no-sats proxies, each documented at its assignment.
//!
//! The runner is I/O: it is validated live on devimint by the maintainer, NOT by the
//! rb-lite gate. Every fedimint API it touches is verified against the pinned source
//! (`douglaz/fedimint` @ `b108ec6`).

use crate::multi_client::MultiClient;
use crate::types::GatewayUrl;
use fedimint_api_client::api::{FederationApiExt as _, StatusResponse};
use fedimint_client::db::ChronologicalOperationLogKey;
use fedimint_client::ClientHandleArc;
use fedimint_core::endpoint_constants::STATUS_ENDPOINT;
use fedimint_core::module::ApiRequestErased;
use fedimint_core::NumPeers;
use fedimint_lnv2_client::common::LightningInvoice;
use fedimint_lnv2_client::LightningOperationMeta;
use fedimint_wallet_client::WalletClientModule;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use wallet_core::{FedBalance, FederationFacts, FederationId, FederationStatus, Module, Msat};

/// The raw result of probing ONE federation: a plain, serde-able data struct with no
/// fedimint types, so it can be recorded as a golden fixture and fed to the pure
/// assembler without a live client.
///
/// Fields group as: structural facts (free, from the authed `ClientConfig`), light
/// empirical signals (no sats), lifecycle, and balance (msat).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProbeResult {
    // ---- Structural facts from the authenticated config (ADR-0019, FREE) ----
    /// Number of guardians = the authed config's api-endpoint count.
    pub guardian_count: u32,
    /// Consensus threshold (`2f+1`) derived from the guardian set.
    pub threshold: u32,
    /// Whether the wallet module's bitcoin network is mainnet.
    pub is_mainnet: bool,
    /// Every consensus module kind present in the config (e.g. `mint`, `wallet`,
    /// `ln`, `lnv2`), verbatim strings so the pure assembler owns the mapping.
    pub module_kinds: Vec<String>,
    /// Whether an `lnv2` module is present (its own field because a fed with no
    /// LNv2 cannot send/receive at all — it gates eligibility unconditionally).
    pub has_lnv2: bool,

    // ---- Light empirical signals (the trust gate, ADR-0017 — NO sats spent) ----
    /// A threshold consensus read answered: quorum is live.
    pub quorum_live: bool,
    /// Wall-clock latency (ms) of that threshold read.
    pub latency_ms: u32,
    /// The federation has a usable lnv2 gateway route (the no-sats proxy for
    /// "can route a receive/pay"). If the caller pinned a gateway, this means that
    /// exact gateway validates for the federation; otherwise it means the federation's
    /// first registered gateway validates, matching the executor's default selection.
    pub gateway_available: bool,
    /// The wallet (on-chain peg-out) module is present in the authed config (the
    /// no-sats proxy for "peg-out is quotable").
    pub wallet_module_present: bool,

    // ---- Lifecycle ----
    /// RAW UNTRUSTED expiry: the MERGED-meta `federation_expiry_timestamp`
    /// (`client.get_meta_expiration_timestamp()`, no auth) as unix seconds, if present. This is
    /// the `LegacyMetaSource` MERGE of the config meta with the federation's `meta_override_url`,
    /// so a single override host can OVERWRITE or forge it
    /// (`fedimint-client-module/src/meta.rs`) — it is NOT consensus-backed. Alone it NEVER
    /// schedules a shutdown; a CORROBORATING source must confirm it first (§15.1). `None` = no
    /// expiry meta. Carried raw so [`derive_shutdown_scheduled`] stays pure and golden-testable.
    pub expiry_timestamp_secs: Option<u64>,
    /// RAW CORROBORATING expiry (a): the AT-JOIN consensus config meta's
    /// `federation_expiry_timestamp` (`client.config().global.meta`), as unix seconds. The joined
    /// config is not forgeable by the override host, so its OWN value drives the derivation when
    /// present (a cached snapshot — catches feds that declared an expiry from the start). `None` =
    /// the joined config declared no expiry.
    pub config_expiry_secs: Option<u64>,
    /// RAW CORROBORATING expiry (b): the meta MODULE's consensus value, when the federation runs a
    /// meta module (fresh AND consensus-backed). Currently always `None` — the wallet client does
    /// not register the meta-module (see the runner note and `challenges-round-1`); the derivation
    /// still MODELS it so it can be wired without changing the trust logic. `None` = no signal.
    pub meta_module_expiry_secs: Option<u64>,
    /// RAW CORROBORATING shutdown signal (c): f+1 peers' public `/status` responses reported a
    /// `federation.scheduled_shutdown` session index (per-peer reads need BFT corroboration before
    /// driving a money-moving evacuation — see [`status_scheduled_shutdown`]). Best-effort — a
    /// `/status` transport error reads as `false` (no signal), never a probe failure.
    pub status_scheduled_shutdown: bool,
    /// The federation is winding down (do not fund it — evacuate it). DERIVED from the raw signals
    /// above via [`derive_shutdown_scheduled`] (an UNTRUSTED override expiry never schedules on its
    /// own — only a corroborated value does), OR'd with the debug-only force-shutdown seam. Kept on
    /// the result so the pure assemblers read one boolean.
    pub shutdown_scheduled: bool,

    // ---- Balance (msat) ----
    /// Spendable ecash balance.
    pub spendable_msat: u64,
    /// Value committed to in-flight (pending) outgoing sends.
    pub in_flight_msat: u64,
    /// Value of pending incoming receives not yet claimed.
    pub claimable_msat: u64,
}

/// Map a raw [`ProbeResult`] into the scorer's `FederationFacts`. PURE: no I/O, no
/// async, total over the input. Every `FederationFacts` field is filled here.
pub fn assemble_facts(p: &ProbeResult, id: FederationId) -> FederationFacts {
    FederationFacts {
        id,
        // Structural facts map straight through (they came from the authed config).
        guardian_count: p.guardian_count,
        threshold: p.threshold,
        is_mainnet: p.is_mainnet,
        modules: p.module_kinds.iter().map(|k| module_from_kind(k)).collect(),
        // Own empirical probe: quorum answered a threshold consensus read.
        quorum_live: p.quorum_live,
        // PROXY (no sats): a usable lnv2 gateway route is the stand-in for "a
        // receive/pay round-trip would succeed". We never perform the real
        // round-trip (decision: light probes), so gateway availability is what the
        // probe gate reads for `round_trip_ok`. This fails CLOSED — no usable route
        // reads as not-routable, the safe direction for a trust gate.
        round_trip_ok: p.gateway_available,
        // PROXY (no sats): the wallet module (the on-chain peg-out path) being
        // present in the authed config is the stand-in for "a peg-out is quotable".
        // We do not fetch a live peg-out quote here.
        peg_out_quotable: p.wallet_module_present,
        latency_ms: p.latency_ms,
        shutdown_scheduled: p.shutdown_scheduled,
        has_lnv2: p.has_lnv2,
        // The Fedimint Observer is an UNTRUSTED prior wired only in Phase 3
        // (ADR-0020). The sense layer never fabricates it, so it is always `None`
        // here — a missing prior is never a rejection, it just adds no rank bonus.
        observer: None,
        // The ACTIVE probe verdict (§5.0.6) is journal + designation material, not sense
        // material: the tick/status assembler fills it from `probe_record` against the
        // snapshot's designated spending fed. The sense layer never fabricates it.
        active_probe: None,
    }
}

/// Map a raw [`ProbeResult`] into the allocator's `FederationStatus`. PURE: no I/O,
/// no async, total over the input. Every `FederationStatus` field is filled here.
pub fn assemble_status(p: &ProbeResult, id: FederationId) -> FederationStatus {
    FederationStatus {
        id,
        balance: FedBalance {
            spendable: Msat(p.spendable_msat),
            in_flight: Msat(p.in_flight_msat),
            claimable: Msat(p.claimable_msat),
            // `reserved_fee` is set by the allocator/executor when it PLANS a move,
            // not sensed from the federation — a fresh probe reserves nothing.
            reserved_fee: Msat(0),
        },
        // `probed_ok` = BOTH liveness (quorum answered) AND a usable route (the
        // pinned gateway, if supplied, or the executor-default first registered
        // gateway answers `routing_info`): the two no-sats empirical signals the allocator's
        // receive-gating reads before it directs an inflow into a federation.
        probed_ok: p.quorum_live && p.gateway_available,
        // Reputation comes from the Phase-3 Observer; the sense layer is neutral.
        reputation: 0,
        shutdown_notice: p.shutdown_scheduled,
        // `healthy` tracks quorum liveness; `shutdown_notice` (above) drives the
        // separate evacuation path in the allocator.
        healthy: p.quorum_live,
        // §15.3: the scorer verdict is the fundability gate for evacuation DESTINATIONS,
        // and it only exists during snapshot assembly (`build_snapshot`). A probe-only
        // caller has no verdict, so it reports `false` — fail-closed, the safe direction:
        // an un-scored fed is never picked as an evacuation destination.
        eligible_to_fund: false,
    }
}

/// Map a fedimint module-kind string to the scorer's `Module`. The kind strings are
/// the module `KIND` constants verified against the pinned source
/// (`modules/*-common/src/lib.rs`): `mint`, `wallet`, `ln`, `lnv2`, `meta`.
fn module_from_kind(kind: &str) -> Module {
    match kind {
        "mint" => Module::Mint,
        "ln" => Module::Ln,
        "lnv2" => Module::Lnv2,
        "wallet" => Module::Wallet,
        "meta" => Module::Meta,
        _ => Module::Other,
    }
}

/// How long BEFORE a federation's `federation_expiry_timestamp` we begin treating it as
/// shutting-down, so the wallet evacuates while the gateways are still up — the whole point
/// is to LEAVE before expiry, not after (ADR-0018). 24h.
const SHUTDOWN_EVACUATION_LEAD_SECS: u64 = 86_400;

/// PURE derivation of `shutdown_scheduled` from the no-auth signals the probe reads plus the wall
/// clock (§15.1). No I/O, no async — golden-tested. The MERGED-meta `override_expiry` is UNTRUSTED
/// (a single `meta_override_url` host can forge it — `fedimint-client-module/src/meta.rs`), so it
/// NEVER schedules a shutdown on its own; a shutdown is scheduled only when a CORROBORATING source
/// confirms it, and the derivation uses THAT source's own value, never the merged one:
///   - `status_scheduled` (the f+1-corroborated `/status.scheduled_shutdown`) → schedule: the
///     shutdown FACT is peer-corroborated, so evacuate (ADR-0018 — leave before expiry). The
///     override expiry could refine lead timing, but a corroborated schedule means go now.
///   - `config_expiry` (the at-join consensus config meta) within `lead_secs` of `now` (also
///     covers an already-passed expiry) → schedule, using the CONFIG value.
///   - `meta_module_expiry` (the meta-module consensus value) within the lead window → schedule,
///     using the META-MODULE value.
///
/// An uncorroborated override-only expiry does NOT schedule (the runner warns for observability). A
/// missing/absent signal is never a shutdown.
fn derive_shutdown_scheduled(
    override_expiry: Option<u64>,
    config_expiry: Option<u64>,
    meta_module_expiry: Option<u64>,
    status_scheduled: bool,
    now_secs: u64,
    lead_secs: u64,
) -> bool {
    // (c) A peer-corroborated scheduled shutdown: evacuate now (the fact is corroborated).
    if status_scheduled {
        return true;
    }
    // The merged override expiry is deliberately NOT consulted for the timing decision — it is
    // reserved for lead timing under (c), where a corroborated fact already governs.
    let _ = override_expiry;
    // (a)/(b) A CORROBORATED expiry within `lead_secs` of `now` schedules, using that source's own
    // value. `now + lead_secs >= expiry` also fires on an expiry already passed.
    [config_expiry, meta_module_expiry]
        .into_iter()
        .flatten()
        .any(|expiry| now_secs.saturating_add(lead_secs) >= expiry)
}

/// Test-only force-shutdown seam (Phase 3.A) — MIRRORS `executor::maybe_crash`'s
/// `debug_assertions` + env pattern. Reports a federation as shutting-down when
/// `WALLET_CLI_FORCE_SHUTDOWN` lists its hex id (comma-separated), so the deferred devimint
/// smoke can force an evacuation deterministically without winding a real federation down.
/// The `#[cfg(not(debug_assertions))]` stub below is compiled into a release binary instead,
/// so the money path can NEVER be forced to evacuate in production.
#[cfg(debug_assertions)]
fn forced_shutdown(id: &FederationId) -> bool {
    forced_shutdown_matches(
        std::env::var("WALLET_CLI_FORCE_SHUTDOWN").ok().as_deref(),
        id,
    )
}

/// Release counterpart: the force seam is elided, so no environment can force an evacuation.
#[cfg(not(debug_assertions))]
fn forced_shutdown(_id: &FederationId) -> bool {
    false
}

/// Whether the `WALLET_CLI_FORCE_SHUTDOWN` value (`None` when unset) lists `id`'s hex. Split
/// out from [`forced_shutdown`] so the match logic is unit-tested WITHOUT process-global env
/// (the same split as `executor::crash_point_matches`). The value is a comma-separated list of
/// federation hex ids; surrounding whitespace is ignored and the compare is case-insensitive.
/// In a `--release` non-test build the seam above is elided and this predicate is unused; it
/// stays defined (and tested) rather than gated so `cargo test --release` still compiles it.
#[cfg_attr(not(debug_assertions), allow(dead_code))]
fn forced_shutdown_matches(configured: Option<&str>, id: &FederationId) -> bool {
    let Some(list) = configured else {
        return false;
    };
    let hex = id.to_hex();
    list.split(',')
        .map(str::trim)
        .any(|entry| entry.eq_ignore_ascii_case(hex.as_str()))
}

/// Probes each open federation into a [`ProbeResult`] over a shared [`MultiClient`].
/// I/O — validated live on devimint, NOT in the rb-lite gate.
pub struct FedimintProbeRunner {
    mc: Arc<MultiClient>,
    pinned_gateway: Option<GatewayUrl>,
}

/// Op-log page size for the pending-balance scan. Paging runs to exhaustion, so this
/// only trades round-trips against per-page memory; it is not a coverage cap.
const PROBE_OPLOG_PAGE_SIZE: usize = 100;

impl FedimintProbeRunner {
    pub fn new(mc: Arc<MultiClient>) -> Self {
        Self::with_pinned_gateway(mc, None)
    }

    pub fn with_pinned_gateway(mc: Arc<MultiClient>, pinned_gateway: Option<GatewayUrl>) -> Self {
        Self { mc, pinned_gateway }
    }

    /// Probe one open federation. LIGHT — NO sats spent: structural facts from the
    /// authenticated `ClientConfig` (free), empirical fields from light reachability
    /// + capability proxies, never a real receive/pay round-trip.
    pub async fn probe(&self, id: &FederationId) -> anyhow::Result<ProbeResult> {
        let client = self.mc.client(id)?;

        // ---- Structural facts: FREE, from the authenticated config (ADR-0019) ----
        let config = client.config().await;

        // Guardian count + threshold. The guardian set IS the api-endpoint set;
        // `NumPeers::threshold()` is `total - max_evil` = `2f+1` (verified in
        // fedimint-core/src/peer_id.rs).
        let num_endpoints = config.global.api_endpoints.len();
        let guardian_count = num_endpoints as u32;
        let threshold = NumPeers::from(num_endpoints).threshold() as u32;

        // Every module kind, verbatim; the pure assembler owns the string→`Module`
        // mapping. `has_lnv2` / `wallet_module_present` are derived here against the
        // pinned `KIND` constants so the runner, not the assembler, knows the truth.
        let module_kinds: Vec<String> = config
            .modules
            .values()
            .map(|m| m.kind.as_str().to_string())
            .collect();
        let has_lnv2 = module_kinds
            .iter()
            .any(|k| k == fedimint_lnv2_client::common::KIND.as_str());
        let wallet_module_present = module_kinds
            .iter()
            .any(|k| k == fedimint_wallet_client::common::KIND.as_str());

        // network → is_mainnet, read from the wallet module's authed config. Only
        // meaningful when the wallet module is present; a fed without it can never be
        // mainnet-gated anyway (it fails the scorer's required-modules floor), so
        // `false` is the correct conservative answer there.
        let is_mainnet = if wallet_module_present {
            client
                .get_first_module::<WalletClientModule>()?
                .get_network()
                == bitcoin::Network::Bitcoin
        } else {
            false
        };

        // ---- Light empirical signals: NO sats spent (ADR-0017) ----

        // `quorum_live` + `latency_ms`: a single THRESHOLD consensus read
        // (`session_count` requests current consensus, needing a threshold of
        // guardians to agree — verified in fedimint-api-client). Success ⇒ quorum is
        // live; the wall-clock is the observed latency. No money moves.
        let started = Instant::now();
        let quorum_live = client.api().session_count().await.is_ok();
        let latency_ms = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);

        // `gateway_available`: a pinned lnv2 gateway, if supplied, must answer
        // `routing_info` for this federation; otherwise the executor-default first registered
        // lnv2 gateway must answer. NOTE (runbook §4): devimint does NOT auto-register its
        // LDK gateway, so the live tick passes that gateway URL directly. As the
        // `round_trip_ok` proxy this fails CLOSED: a missing lnv2 module, empty list,
        // stale/unreachable gateway, invalid pinned gateway, or unreachable gateway
        // registry all read as not-routable, producing a scorable `ProbeResult`
        // instead of hiding a dead federation behind `Err`.
        let gateway_available = self.gateway_available(id, has_lnv2).await;

        // ---- Balance (msat) ----
        // `?` here (and on `get_first_module` for `is_mainnet` above) is intentional and
        // NOT the gateway read's degrade-to-`false`: `balance`/`config`/module reads are
        // LOCAL (the client's own db), so a failure means the fed genuinely can't be
        // sensed — Err is the honest result, and the caller skips it like `open_all`
        // does. The gateway read degrades to `false` instead because it is a NETWORK
        // capability signal whose absence is itself a meaningful (fail-closed) fact.
        let spendable_msat = self.mc.balance(id).await?.0;
        // `in_flight` / `claimable` are a best-effort scan of the op-log for pending
        // (no cached final outcome) lnv2 legs. Light: a local db read, no network,
        // no sats. The allocator decides on `spendable` today; these fields exist so
        // the model can later account for pending value without a balance rewrite.
        let (in_flight_msat, claimable_msat) = pending_lnv2_balances(&client).await;

        // `shutdown_scheduled`: the UNTRUSTED merged override expiry never schedules on its own; a
        // source the override host cannot forge must corroborate it first (§15.1). We read every
        // no-auth signal here (each best-effort — a failed/absent read is NOT a shutdown) and the
        // pure `derive_shutdown_scheduled` below combines them, so the derivation is golden-testable
        // off the raw fields.
        //
        // UNTRUSTED: the MERGED-meta `federation_expiry_timestamp` (`get_meta_expiration_timestamp`,
        // LegacyMetaSource = config meta OVERWRITABLE by `meta_override_url`). Read for
        // observability + lead timing, never trusted alone.
        let expiry_timestamp_secs = client
            .get_meta_expiration_timestamp()
            .await
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        // CORROBORATOR (a): the AT-JOIN consensus config meta — the joined config the override host
        // cannot forge. Read directly from the (already-fetched) config, NOT the merged view.
        let config_expiry_secs = config_meta_expiry_secs(&config.global.meta);
        // CORROBORATOR (b): the meta-MODULE consensus value. The wallet client registers only
        // ln/mint/wallet/lnv2 (`multi_client::client_builder`), NOT the meta module, so this
        // consensus signal is not read here — see `challenges-round-1`. `None` for now; the
        // derivation MODELS it so wiring the meta-module client later is a one-place change.
        let meta_module_expiry_secs = None;
        // CORROBORATOR (c): the public `/status.federation.scheduled_shutdown`, f+1-corroborated.
        // Best-effort — a transport error is warn-logged and treated as no signal.
        let status_scheduled_shutdown = status_scheduled_shutdown(&client, id).await;
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // Derive from the raw signals (override-only never schedules), then OR the debug-only
        // force-shutdown seam (a strict no-op / compiled out in release — see `forced_shutdown`).
        let shutdown_scheduled = derive_shutdown_scheduled(
            expiry_timestamp_secs,
            config_expiry_secs,
            meta_module_expiry_secs,
            status_scheduled_shutdown,
            now_secs,
            SHUTDOWN_EVACUATION_LEAD_SECS,
        ) || forced_shutdown(id);

        // §15.1 item 2: the merged meta carries an expiry that NO corroborating source backs
        // (absent from the at-join config meta, the meta module, and the f+1 `/status` signal), so
        // it came from `meta_override_url` ALONE. We do NOT schedule on it (a single override host
        // could forge it), but we WARN — an operator needs to see a federation announcing an
        // uncorroborated expiry, whether that is a legitimate expiry declared through the wrong
        // channel or a forged early expiry aimed at triggering a premature evacuation.
        let override_uncorroborated = expiry_timestamp_secs.is_some()
            && config_expiry_secs.is_none()
            && meta_module_expiry_secs.is_none()
            && !status_scheduled_shutdown;
        if override_uncorroborated {
            tracing::warn!(
                federation = %id.to_hex(),
                override_expiry_secs = ?expiry_timestamp_secs,
                "probe: federation announced an expiry only via meta_override_url, uncorroborated \
                 by config meta / meta module / f+1 /status — NOT scheduling a shutdown (§15.1); \
                 investigate whether this is a legitimate expiry or a forged override"
            );
        }

        Ok(ProbeResult {
            guardian_count,
            threshold,
            is_mainnet,
            module_kinds,
            has_lnv2,
            quorum_live,
            latency_ms,
            gateway_available,
            wallet_module_present,
            expiry_timestamp_secs,
            config_expiry_secs,
            meta_module_expiry_secs,
            status_scheduled_shutdown,
            shutdown_scheduled,
            spendable_msat,
            in_flight_msat,
            claimable_msat,
        })
    }

    async fn gateway_available(&self, id: &FederationId, has_lnv2: bool) -> bool {
        if !has_lnv2 {
            return false;
        }

        if let Some(gateway) = &self.pinned_gateway {
            return match self.mc.validate_gateway(id, gateway).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(
                        federation = %id.to_hex(),
                        gateway = %gateway.0,
                        error = ?e,
                        "probe: pinned gateway failed routing-info validation"
                    );
                    false
                }
            };
        }

        match self.mc.gateways(id).await {
            Ok(gateways) => {
                // §15.6: ANY registered gateway that validates makes the fed routable — a stale
                // first-registered gateway must not hide a healthy fed (the SDK's own
                // `select_gateway` scans until responsive). Scan until one answers.
                for gateway in &gateways {
                    match self.mc.validate_gateway(id, gateway).await {
                        Ok(()) => return true,
                        Err(e) => tracing::warn!(
                            federation = %id.to_hex(),
                            gateway = %gateway.0,
                            error = ?e,
                            "probe: registered gateway failed routing-info validation; scanning next"
                        ),
                    }
                }
                false
            }
            Err(e) => {
                tracing::warn!(
                    federation = %id.to_hex(),
                    error = ?e,
                    "probe: treating gateway registry read failure as unavailable"
                );
                false
            }
        }
    }
}

/// The `federation_expiry_timestamp` in a federation's AT-JOIN consensus config meta
/// (`config.global.meta`), as unix seconds, or `None` when absent/unparseable (§15.1 (a)). The
/// config meta stores string values (some JSON-escaped), so accept a plain integer string OR a
/// one-layer JSON number/quoted-number.
fn config_meta_expiry_secs(meta: &std::collections::BTreeMap<String, String>) -> Option<u64> {
    parse_meta_expiry_secs(meta.get("federation_expiry_timestamp")?)
}

/// Parse a meta value into a unix-seconds `u64`: a plain decimal string, or one layer of JSON
/// (a number, or a quoted decimal string). `None` on anything else.
fn parse_meta_expiry_secs(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(secs);
    }
    match serde_json::from_str::<serde_json::Value>(trimmed).ok()? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
}

/// SECONDARY shutdown signal: the public `/status` endpoint exposes
/// `federation.scheduled_shutdown` as an optional session index. Query it through ordinary
/// single-peer no-auth requests instead of `client.api().status()`: the SDK helper is no-auth
/// but still admin-shaped and requires `self_peer`, which normal wallet clients do not have.
/// Best-effort — an absent federation block, absent scheduled shutdown, or transport/helper
/// error is NEVER a probe failure, just "no signal".
///
/// `/status` is a PER-PEER (non-consensus) read, and this signal now drives a money-moving
/// `Evacuate`: one desynced/compromised guardian must not be able to force the wallet to
/// drain the federation and pay LN fees. So the signal requires **f+1 corroborating peers**
/// (`f = (n-1)/3`, the BFT fault bound): f+1 reports guarantee at least one HONEST peer says
/// a shutdown is scheduled. Early-exits once corroborated. The consensus-backed expiry meta
/// (the PRIMARY signal) needs no such gate.
async fn status_scheduled_shutdown(client: &ClientHandleArc, id: &FederationId) -> bool {
    let api = client.api();
    let peers: Vec<_> = api.all_peers().iter().copied().collect();
    let needed = shutdown_report_quorum(peers.len());
    let mut reporting = 0usize;
    for peer in peers {
        match api
            .request_single_peer_federation::<StatusResponse>(
                STATUS_ENDPOINT.to_owned(),
                ApiRequestErased::default(),
                peer,
            )
            .await
        {
            Ok(status) => {
                if status
                    .federation
                    .and_then(|f| f.scheduled_shutdown)
                    .is_some()
                {
                    reporting += 1;
                    if reporting >= needed {
                        return true;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    federation = %id.to_hex(),
                    peer = %peer,
                    error = ?e,
                    "probe: /status peer read failed; treating peer as no shutdown signal"
                );
            }
        }
    }
    if reporting > 0 {
        tracing::warn!(
            federation = %id.to_hex(),
            reporting,
            needed,
            "probe: scheduled_shutdown reported by fewer than f+1 peers; not corroborated, ignoring"
        );
    }
    false
}

/// The f+1 corroboration bound for the per-peer `/status.scheduled_shutdown` signal:
/// `f = (n-1)/3` (the BFT fault bound already used by `NumPeers`), so f+1 reporting peers
/// include at least one honest one. PURE; `n == 0` degrades to 1 (an empty peer set can
/// then never corroborate, since zero reports < 1).
fn shutdown_report_quorum(n: usize) -> usize {
    n.saturating_sub(1) / 3 + 1
}

/// Sum the pending (unsettled) lnv2 OUTGOING value from `client`'s op-log, returning
/// `(in_flight_msat, claimable_msat)`. Best-effort and light: a local db read only (no
/// network, no sats). An op with a cached final outcome has settled and is skipped; an
/// unsettled Send contributes its invoice amount to `in_flight` (funding a send commits
/// those funds out of `spendable` as soon as the send op exists). Malformed / non-lnv2
/// ops are skipped.
///
/// `claimable` is deliberately always `0`: a receive op exists from the moment its
/// invoice is CREATED — before any payment arrives — and the op-log caches only the
/// TERMINAL outcome, so a light read cannot tell an unpaid open invoice from a paid-
/// but-unclaimed contract. Counting every open receive would report money that has not
/// arrived as claimable — the unsafe over-report direction for a balance field — so we
/// report `0` until the paid-but-unclaimed state is available (that needs the receive
/// state machine's update stream; Phase 3).
///
/// Because outcomes are cached only once an op's update stream reaches a terminal
/// state, a freshly-reopened client can transiently OVER-report `in_flight` (a send that
/// already settled but whose outcome has not been re-cached still looks unsettled) until
/// it re-subscribes; this is why the value is advisory (not consumed by `decide` yet)
/// and the whole runner is validated live.
async fn pending_lnv2_balances(client: &ClientHandleArc) -> (u64, u64) {
    let log = client.operation_log();
    let mut last_seen: Option<ChronologicalOperationLogKey> = None;
    let mut in_flight = 0u64;
    // Always 0 — a light op-log read cannot confirm a receive was paid (see fn doc).
    let claimable = 0u64;
    loop {
        let page = log
            .paginate_operations_rev(PROBE_OPLOG_PAGE_SIZE, last_seen)
            .await;
        let page_len = page.len();
        if let Some((key, _)) = page.last() {
            last_seen = Some(*key);
        }
        for (_key, entry) in page {
            // Only lnv2 ops carry a receive/send lightning leg; skip the rest.
            if entry.operation_module_kind() != fedimint_lnv2_client::common::KIND.as_str() {
                continue;
            }
            // A cached final outcome means the op already settled (claimed / paid /
            // refunded / expired); only still-pending ops are in-flight/claimable.
            // Decoding to `Value` cannot fail, so this never panics.
            if entry.outcome::<serde_json::Value>().is_some() {
                continue;
            }
            let Ok(meta) = entry.try_meta::<LightningOperationMeta>() else {
                continue;
            };
            match meta {
                LightningOperationMeta::Send(send) => {
                    in_flight = in_flight.saturating_add(invoice_msat(&send.invoice));
                }
                // An open receive is an invoice created BEFORE any payment arrives; a
                // light op-log read can't prove it was paid (see fn doc), so it never
                // contributes to `claimable` — counting it would fabricate arrived funds.
                LightningOperationMeta::Receive(_) => {}
                // A gateway-minted LNURL receive is not part of our move protocol.
                LightningOperationMeta::LnurlReceive(_) => {}
            }
        }
        // A short (or empty) page is the last (`paginate_operations_rev` returns up
        // to `limit` newest-first), so fewer than `limit` means the log is exhausted.
        if page_len < PROBE_OPLOG_PAGE_SIZE {
            break;
        }
    }
    (in_flight, claimable)
}

/// The msat amount of an lnv2 leg's BOLT11 invoice, or `0` for an amountless invoice.
fn invoice_msat(invoice: &LightningInvoice) -> u64 {
    let LightningInvoice::Bolt11(bolt11) = invoice;
    bolt11.amount_milli_satoshis().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wallet_core::scorer::ReasonCode as ScorerReason;
    use wallet_core::types::ReasonCode as AllocReason;
    use wallet_core::{decide, score, Action, AllocatorSnapshot, Occurrence, ScorerPolicy};

    // ---- Recorded fixtures -------------------------------------------------------
    //
    // A healthy fed's probe, serialized as JSON exactly as the runner would emit it.
    // Decoding this string is the "recorded fixture" path: it proves `ProbeResult` is
    // serde-able and drives the pure assembler without a live client.
    const HEALTHY_PROBE_JSON: &str = r#"{
        "guardian_count": 4,
        "threshold": 3,
        "is_mainnet": true,
        "module_kinds": ["mint", "wallet", "ln", "lnv2"],
        "has_lnv2": true,
        "quorum_live": true,
        "latency_ms": 42,
        "gateway_available": true,
        "wallet_module_present": true,
        "expiry_timestamp_secs": null,
        "config_expiry_secs": null,
        "meta_module_expiry_secs": null,
        "status_scheduled_shutdown": false,
        "shutdown_scheduled": false,
        "spendable_msat": 1000000,
        "in_flight_msat": 5000,
        "claimable_msat": 7000
    }"#;

    fn healthy_probe() -> ProbeResult {
        serde_json::from_str(HEALTHY_PROBE_JSON).expect("healthy fixture is valid ProbeResult JSON")
    }

    fn fed_id(byte: u8) -> FederationId {
        FederationId([byte; 32])
    }

    #[test]
    fn probe_result_json_round_trips() {
        let probe = healthy_probe();
        let json = serde_json::to_string(&probe).expect("serialize");
        let back: ProbeResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(probe, back);
    }

    #[test]
    fn healthy_fed_is_eligible_via_score() {
        let facts = assemble_facts(&healthy_probe(), fed_id(1));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(
            verdict.eligible_to_fund,
            "a 4-guardian mainnet fed with mint+wallet+lnv2, live quorum and a gateway \
             must be fundable; reasons: {:?}",
            verdict.reasons
        );
        // Rank is only meaningful when eligible, and must be non-zero here.
        assert!(verdict.rank_score > 0);
    }

    #[test]
    fn structural_facts_map_straight_through() {
        let probe = healthy_probe();
        let facts = assemble_facts(&probe, fed_id(1));
        assert_eq!(facts.id, fed_id(1));
        assert_eq!(facts.guardian_count, 4);
        assert_eq!(facts.threshold, 3);
        assert!(facts.is_mainnet);
        assert!(facts.has_lnv2);
        assert_eq!(
            facts.modules,
            vec![Module::Mint, Module::Wallet, Module::Ln, Module::Lnv2]
        );
        // The documented no-sats proxies.
        assert_eq!(facts.round_trip_ok, probe.gateway_available);
        assert_eq!(facts.peg_out_quotable, probe.wallet_module_present);
        // The Observer prior is never sensed here (Phase 3).
        assert!(facts.observer.is_none());
    }

    #[test]
    fn no_lnv2_fed_is_ineligible() {
        let mut probe = healthy_probe();
        probe.has_lnv2 = false;
        probe.module_kinds = vec!["mint".into(), "wallet".into(), "ln".into()];
        let facts = assemble_facts(&probe, fed_id(2));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(
            verdict.reasons.contains(&ScorerReason::NoLnv2),
            "reasons: {:?}",
            verdict.reasons
        );
    }

    #[test]
    fn shutdown_scheduled_fed_is_unfundable_and_flags_status() {
        let mut probe = healthy_probe();
        probe.shutdown_scheduled = true;
        let facts = assemble_facts(&probe, fed_id(3));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(verdict.reasons.contains(&ScorerReason::ShutdownScheduled));

        let status = assemble_status(&probe, fed_id(3));
        assert!(status.shutdown_notice);
        // A shutdown notice does not, by itself, mean quorum is dead.
        assert!(status.healthy);
    }

    #[test]
    fn derive_shutdown_corroboration_table() {
        // §15.1 golden. Columns: (override_expiry, config_expiry, meta_module_expiry, status).
        let lead = SHUTDOWN_EVACUATION_LEAD_SECS;
        let now = 1_000_000;
        let near = now + lead / 2; // inside the lead window
        let far = now + 10 * lead; // outside the lead window
        let derive = |override_e, config_e, meta_e, status| {
            derive_shutdown_scheduled(override_e, config_e, meta_e, status, now, lead)
        };

        // OVERRIDE-ONLY never schedules — whatever its value (near / already-passed / far), no
        // corroborator means no evacuation. This is the whole point of §15.1.
        assert!(!derive(Some(near), None, None, false), "override-only near");
        assert!(
            !derive(Some(now - 1), None, None, false),
            "override-only passed"
        );
        assert!(!derive(Some(far), None, None, false), "override-only far");

        // Each CORROBORATOR alone (with a near value) schedules; an accompanying override is inert.
        assert!(derive(None, Some(near), None, false), "config alone");
        assert!(
            derive(Some(far), Some(near), None, false),
            "config + override"
        );
        assert!(derive(None, None, Some(near), false), "meta-module alone");
        assert!(
            derive(Some(far), None, Some(near), false),
            "meta-module + override"
        );
        assert!(derive(None, None, None, true), "status alone");
        assert!(derive(Some(far), None, None, true), "status + override");

        // The decision uses the CORROBORATED source's timestamp, NOT the override's: a NEAR
        // override with a FAR config does NOT schedule (config governs)...
        assert!(
            !derive(Some(near), Some(far), None, false),
            "a near override cannot force a far config"
        );
        // ...and a FAR override with a NEAR config DOES schedule (config governs).
        assert!(
            derive(Some(far), Some(near), None, false),
            "a near config schedules despite a far override"
        );

        // A corroborator that is present but FAR-future does not schedule yet; exactly at the lead
        // boundary is inclusive; everything absent is false.
        assert!(
            !derive(None, Some(far), None, false),
            "config far -> not yet"
        );
        assert!(
            derive(None, Some(now + lead), None, false),
            "config at lead boundary"
        );
        assert!(!derive(None, None, None, false), "all absent");
    }

    #[test]
    fn config_meta_expiry_parses_plain_and_json_values() {
        use std::collections::BTreeMap;
        // Plain decimal string.
        assert_eq!(parse_meta_expiry_secs("1700000000"), Some(1_700_000_000));
        // Surrounding whitespace tolerated.
        assert_eq!(parse_meta_expiry_secs(" 42 "), Some(42));
        // One layer of JSON (a bare number or a quoted decimal).
        assert_eq!(parse_meta_expiry_secs("1700000000 "), Some(1_700_000_000));
        assert_eq!(
            parse_meta_expiry_secs("\"1700000000\""),
            Some(1_700_000_000)
        );
        // Non-numeric / negative / absent -> None (never a spurious shutdown).
        assert_eq!(parse_meta_expiry_secs("not-a-number"), None);
        assert_eq!(parse_meta_expiry_secs("-5"), None);

        let mut meta = BTreeMap::new();
        assert_eq!(config_meta_expiry_secs(&meta), None);
        meta.insert(
            "federation_expiry_timestamp".to_string(),
            "1700000000".to_string(),
        );
        assert_eq!(config_meta_expiry_secs(&meta), Some(1_700_000_000));
    }

    #[test]
    fn shutdown_report_quorum_is_the_bft_f_plus_one_bound() {
        // f = (n-1)/3, quorum = f+1: one honest reporter is guaranteed.
        assert_eq!(shutdown_report_quorum(0), 1); // empty set can never corroborate
        assert_eq!(shutdown_report_quorum(1), 1);
        assert_eq!(shutdown_report_quorum(4), 2); // devimint: 4 guardians, f=1
        assert_eq!(shutdown_report_quorum(7), 3);
        assert_eq!(shutdown_report_quorum(10), 4);
    }

    #[test]
    fn forced_shutdown_matches_only_listed_hex_ids() {
        let a = fed_id(0xaa);
        let b = fed_id(0xbb);
        let ha = a.to_hex();
        let hb = b.to_hex();
        // Unset never forces (mirrors an unset WALLET_CLI_CRASH_AT).
        assert!(!forced_shutdown_matches(None, &a));
        // A single-id list forces only that fed.
        assert!(forced_shutdown_matches(Some(ha.as_str()), &a));
        assert!(!forced_shutdown_matches(Some(ha.as_str()), &b));
        // A comma-separated list (with whitespace + mixed case) forces each listed fed.
        let list = format!(" {}, {} ", ha.to_uppercase(), hb);
        assert!(forced_shutdown_matches(Some(list.as_str()), &a));
        assert!(forced_shutdown_matches(Some(list.as_str()), &b));
        // An empty value forces nothing.
        assert!(!forced_shutdown_matches(Some(""), &a));
    }

    #[test]
    fn quorum_dead_fed_is_not_probed_ok_and_unhealthy() {
        let mut probe = healthy_probe();
        probe.quorum_live = false;
        let status = assemble_status(&probe, fed_id(4));
        assert!(!status.probed_ok);
        assert!(!status.healthy);

        // And the scorer's probe gate rejects it.
        let facts = assemble_facts(&probe, fed_id(4));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(verdict.reasons.contains(&ScorerReason::ProbeFailed));
    }

    #[test]
    fn no_gateway_fails_probe_gate_and_status() {
        // The `round_trip_ok` / `probed_ok` gateway proxy fails CLOSED.
        let mut probe = healthy_probe();
        probe.gateway_available = false;
        let facts = assemble_facts(&probe, fed_id(5));
        assert!(!facts.round_trip_ok);
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(verdict.reasons.contains(&ScorerReason::ProbeFailed));

        let status = assemble_status(&probe, fed_id(5));
        assert!(!status.probed_ok);
    }

    #[test]
    fn balances_map_into_fed_balance() {
        let status = assemble_status(&healthy_probe(), fed_id(1));
        assert_eq!(status.balance.spendable, Msat(1_000_000));
        assert_eq!(status.balance.in_flight, Msat(5_000));
        assert_eq!(status.balance.claimable, Msat(7_000));
        // A fresh probe reserves no fee — that is the allocator/executor's job.
        assert_eq!(status.balance.reserved_fee, Msat(0));
    }

    #[test]
    fn assembled_statuses_drive_decide_to_rebalance() {
        // Two distinct healthy feds: spending is under target, standby is funded.
        // decide() must move funds from the standby into the spending fed. This proves
        // the whole sense→decide wire.
        let mut spending_probe = healthy_probe();
        spending_probe.spendable_msat = 10_000;
        let mut spending = assemble_status(&spending_probe, fed_id(0xaa));
        spending.eligible_to_fund = true;

        let mut standby_probe = healthy_probe();
        standby_probe.spendable_msat = 5_000_000;
        let mut standby = assemble_status(&standby_probe, fed_id(0xbb));
        standby.eligible_to_fund = true;

        let snapshot = AllocatorSnapshot {
            federations: vec![spending.clone(), standby.clone()],
            spending_fed: Some(spending.id),
            standby_fed: Some(standby.id),
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(1_000_000),
            max_fee: Msat(10_000),
            min_move: Msat(5_000),
            reservations: wallet_core::Reservations::default(),
            now: 0,
        };

        let decisions = decide(&snapshot, Occurrence(1));
        let moved = decisions.iter().find_map(|d| match &d.action {
            Action::Move {
                from, to, amount, ..
            } if *from == standby.id && *to == spending.id => Some(*amount),
            _ => None,
        });
        assert_eq!(
            moved,
            Some(Msat(990_000)),
            "decide() should top the spending fed up to target from the standby; got {decisions:?}"
        );
        // And the recorded decision is the top-up reason, not an advisory refusal.
        assert!(decisions
            .iter()
            .any(|d| d.reason == AllocReason::SpendingBelowTarget));
    }
}
