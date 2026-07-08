//! The discovery SOURCE seam (phase 5 §5.1.0): the untrusted [`CandidateSource`] trait and
//! its offline [`ManualSource`]. Sources SUGGEST candidate federations; they never assert
//! trust — every structural fact is re-derived from the AUTHENTICATED config downstream
//! (§5.1.2), so a wrong/hostile source can waste a config fetch but cannot promote a fed.
//!
//! This slice ships the trait + the manual/fixture impl (the offline + live-gate source), so
//! the 5.1b pipeline and the devimint gate have a source to drive. The `ObserverSource` (HTTP)
//! and `NostrSource` sit behind the SAME trait and are 5.1b/deferred — no source is
//! load-bearing (ADR-0020); discovery unions whatever the configured sources return.

use crate::multi_client::bridge_federation_id;
use async_trait::async_trait;
use fedimint_core::invite_code::InviteCode;
use wallet_core::{DiscoverySource, FederationId, SourceStatus};

/// One untrusted candidate announcement (§5.1.0): a federation id + invite + network hint.
///
/// `claimed_id` is the SOURCE's RAW claim (the Observer `id` field, the Nostr `d` tag), NOT
/// re-derived from `invite`, so the pipeline's Sybil check
/// (`claimed_id == invite.federation_id() == config.federation_id()`) is meaningful — a source
/// whose claimed id disagrees with its own invite is internally inconsistent and dropped. For
/// [`ManualSource::from_invites`] the caller supplies the invite and `claimed_id` IS the
/// invite's own id, so the check is a no-op there (the user is the source).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CandidateAnnouncement {
    pub claimed_id: FederationId,
    pub invite: InviteCode,
    /// e.g. "bitcoin"/"signet" — a hint, re-checked structurally against the authenticated config.
    pub network_hint: Option<String>,
    /// Provenance (Observer | Nostr | Manual), recorded on the discovery ledger row.
    pub source: DiscoverySource,
}

/// A source's contribution to one discovery pass. Best-effort AND status-bearing: a source
/// that errors/times out returns `{ candidates: [], status: Failed(reason) }` — it never blocks
/// discovery of the others, but a DOWN source stays distinguishable from a healthy source that
/// truly found nothing (§5.1.0).
pub struct SourceResult {
    pub candidates: Vec<CandidateAnnouncement>,
    pub status: SourceStatus,
}

/// The swappable discovery seam (§5.1.0). Every concrete source (Manual now, Observer/Nostr in
/// 5.1b) implements exactly this, so the pure pipeline is unit-testable against a fixture source.
#[async_trait]
pub trait CandidateSource {
    async fn candidates(&self) -> SourceResult;
}

/// A fixed candidate list — a CLI `--invite` list or a test fixture — that ALWAYS reports
/// `status: Ok` (§5.1.0). The offline + live-gate source: it needs no network, so it drives
/// both the unit tests and the devimint gate (pointed at the harness's fed B).
pub struct ManualSource {
    candidates: Vec<CandidateAnnouncement>,
}

impl ManualSource {
    /// Wrap an explicit announcement list (each already carries its `claimed_id`/`source`).
    pub fn new(candidates: Vec<CandidateAnnouncement>) -> Self {
        Self { candidates }
    }

    /// Build a manual source from a bare invite list: each announcement's `claimed_id` is the
    /// invite's OWN federation id (the user is the source, so the Sybil check is a no-op) and
    /// `source` is [`DiscoverySource::Manual`], with no network hint.
    pub fn from_invites(invites: Vec<InviteCode>) -> Self {
        let candidates = invites
            .into_iter()
            .map(|invite| CandidateAnnouncement {
                claimed_id: bridge_federation_id(invite.federation_id()),
                invite,
                network_hint: None,
                source: DiscoverySource::Manual,
            })
            .collect();
        Self { candidates }
    }
}

#[async_trait]
impl CandidateSource for ManualSource {
    async fn candidates(&self) -> SourceResult {
        SourceResult {
            candidates: self.candidates.clone(),
            status: SourceStatus::Ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_invite() -> InviteCode {
        InviteCode::from_str(
            "fed11qgqpu8rhwden5te0vejkg6tdd9h8gepwd4cxcumxv4jzuen0duhsqqfqh6nl7sgk72caxfx8khtfnn8y436q3nhyrkev3qp8ugdhdllnh86qmp42pm",
        )
        .expect("valid invite code")
    }

    #[tokio::test]
    async fn manual_source_from_invites_derives_claimed_id_and_reports_ok() {
        let invite = test_invite();
        let expected_id = bridge_federation_id(invite.federation_id());
        let source = ManualSource::from_invites(vec![invite.clone()]);

        let result = source.candidates().await;

        assert_eq!(result.status, SourceStatus::Ok);
        assert_eq!(
            result.candidates,
            vec![CandidateAnnouncement {
                claimed_id: expected_id,
                invite,
                network_hint: None,
                source: DiscoverySource::Manual,
            }]
        );
    }
}
