//! Pure discovery types + auto-join accounting (phase 5 §5.1). No I/O, no fedimint SDK:
//! the source PROVENANCE + STATUS the ledger records ([`DiscoverySource`]/[`SourceStatus`],
//! referenced by [`crate::OperationKind::Discover`]), the bounded auto-join [`DiscoveryPolicy`],
//! and the pure [`auto_join_budget`] cap decision the 5.1b pipeline consumes.
//!
//! The `InviteCode`-bearing pieces — the `CandidateSource` trait, `CandidateAnnouncement`, the
//! durable `0x09` candidate registry, and the registry/ledger COUNTS these caps evaluate — live
//! in `wallet-fedimint` (they need the fedimint SDK). This module is the dependency-light half,
//! so the cap arithmetic stays golden-tested here with no database.

/// Provenance of a candidate announcement / discovery ledger row (§5.1.0). Recorded so
/// `history` can attribute every discovered fed to WHO surfaced it; a source never asserts
/// trust, so this is audit metadata only. `Observer`/`Nostr` sources are 5.1b/deferred; 5.1a
/// ships `Manual` (the offline + live-gate source).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DiscoverySource {
    Observer,
    Nostr,
    Manual,
}

/// Whether a [`crate::OperationKind::Discover`] pass's source produced a result or FAILED
/// (§5.1.0/§5.1.2). A DOWN source (`Failed`) stays distinguishable on the ledger from a
/// healthy source that truly found nothing (`Ok` with `found: 0`) — the wallet is correct
/// if the Observer is down (ADR-0020), but the ledger still records which happened.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SourceStatus {
    Ok,
    Failed(String),
}

/// Trailing-7d structural recheck backoff (§5.1.1): a stored candidate whose structural
/// verdict is older than this is re-fetched + re-floored on the next discovery pass, so a
/// fed initially `Rejected` for a now-upgradeable property is reconsidered and a `Discovered`
/// fed's facts do not go permanently stale — without a config fetch every pass.
pub const STRUCTURAL_RECHECK_BACKOFF_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// The bounded, disclosed auto-join policy (§5.1.4). All three caps are hard bounds on the
/// agent-created partition set; `require_mainnet` relaxes the structural floor's network
/// requirement for the devimint gate (§5.1.6 `--scorer-allow-regtest`), production keeps it
/// `true`. `Default` is the v1 policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryPolicy {
    /// Auto-joined feds whose probe is not yet `Passed` (the in-flight probing surface).
    pub max_concurrent_unproven: u32,
    /// SUCCESSFUL new-partition agent joins allowed in the trailing 7 days (a rate limit).
    pub max_auto_joins_per_week: u32,
    /// The SETTLED lifetime bound: total agent-created partitions ever (§5.1.4). Joins are
    /// one-way in v1 (no eviction), so this is the simplest bound that keeps a long-running
    /// wallet's partition set finite. Counted from IMMUTABLE agent-`join` history, never the
    /// mutable candidate state (§P1).
    pub auto_join_lifetime_cap: u32,
    /// See [`STRUCTURAL_RECHECK_BACKOFF_MS`]; carried on the policy so the pipeline reads one
    /// knob.
    pub structural_recheck_backoff_ms: u64,
    /// Whether the structural floor requires a mainnet network. Production `true`; the
    /// devimint gate sets `false` so a regtest fed is not rejected on `network` before
    /// auto-join (§5.1.6).
    pub require_mainnet: bool,
}

impl Default for DiscoveryPolicy {
    fn default() -> Self {
        Self {
            max_concurrent_unproven: 3,
            max_auto_joins_per_week: 5,
            auto_join_lifetime_cap: 20,
            structural_recheck_backoff_ms: STRUCTURAL_RECHECK_BACKOFF_MS,
            require_mainnet: true,
        }
    }
}

/// Which cap (if any) blocks the next auto-join (§5.1.4). [`Allowed`](BudgetVerdict::Allowed)
/// means every cap has room; otherwise the SINGLE most fundamental binding constraint, so the
/// 5.1b pipeline increments exactly one `blocked_*` counter per blocked candidate on the
/// source-neutral `AutoJoin` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetVerdict {
    Allowed,
    BlockedConcurrent,
    BlockedWeekly,
    BlockedLifetime,
}

/// The pure auto-join cap decision (§5.1.4): given the three already-computed counts and the
/// policy, return which cap (if any) blocks the next auto-join. The 5.1b pipeline computes the
/// counts from the registry (`concurrent_unproven`) and the immutable ledger join history
/// (`weekly`/`lifetime`) and consumes this verdict; keeping the arithmetic pure here makes the
/// cap matrix golden-tested with no database.
///
/// Precedence is HARDEST-bound-first — `lifetime`, then `weekly`, then `concurrent` — so the
/// reported reason is the most durable one: a wallet at the lifetime cap can never auto-join
/// again (eviction is deferred, §5.1.4), which is a truer diagnostic than "wait for a probe to
/// pass". A candidate blocked by more than one cap is attributed to the most fundamental.
pub fn auto_join_budget(
    concurrent_unproven: u32,
    weekly_auto_joins: u32,
    lifetime_auto_joins: u32,
    policy: &DiscoveryPolicy,
) -> BudgetVerdict {
    if lifetime_auto_joins >= policy.auto_join_lifetime_cap {
        BudgetVerdict::BlockedLifetime
    } else if weekly_auto_joins >= policy.max_auto_joins_per_week {
        BudgetVerdict::BlockedWeekly
    } else if concurrent_unproven >= policy.max_concurrent_unproven {
        BudgetVerdict::BlockedConcurrent
    } else {
        BudgetVerdict::Allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_allows_when_every_cap_has_room() {
        let policy = DiscoveryPolicy::default();
        assert_eq!(auto_join_budget(0, 0, 0, &policy), BudgetVerdict::Allowed);
        // One below each cap is still allowed (the caps are exclusive upper bounds).
        assert_eq!(auto_join_budget(2, 4, 19, &policy), BudgetVerdict::Allowed);
    }

    #[test]
    fn concurrent_cap_blocks_at_the_limit() {
        let policy = DiscoveryPolicy::default();
        assert_eq!(
            auto_join_budget(3, 0, 0, &policy),
            BudgetVerdict::BlockedConcurrent
        );
    }

    #[test]
    fn weekly_cap_blocks_at_the_limit() {
        let policy = DiscoveryPolicy::default();
        assert_eq!(
            auto_join_budget(0, 5, 0, &policy),
            BudgetVerdict::BlockedWeekly
        );
    }

    #[test]
    fn lifetime_cap_blocks_at_the_limit() {
        let policy = DiscoveryPolicy::default();
        assert_eq!(
            auto_join_budget(0, 0, 20, &policy),
            BudgetVerdict::BlockedLifetime
        );
    }

    #[test]
    fn precedence_reports_the_hardest_bound_first() {
        let policy = DiscoveryPolicy::default();
        // All three exhausted -> lifetime is the most fundamental (eviction is deferred).
        assert_eq!(
            auto_join_budget(3, 5, 20, &policy),
            BudgetVerdict::BlockedLifetime
        );
        // Under lifetime but weekly + concurrent both hit -> weekly (a week's wait beats a
        // probe's wait as the binding reason).
        assert_eq!(
            auto_join_budget(3, 5, 10, &policy),
            BudgetVerdict::BlockedWeekly
        );
        // Only concurrent hit.
        assert_eq!(
            auto_join_budget(3, 1, 10, &policy),
            BudgetVerdict::BlockedConcurrent
        );
    }

    #[test]
    fn a_relaxed_policy_lifts_the_caps() {
        let policy = DiscoveryPolicy {
            max_concurrent_unproven: 10,
            max_auto_joins_per_week: 10,
            auto_join_lifetime_cap: 100,
            ..DiscoveryPolicy::default()
        };
        assert_eq!(auto_join_budget(3, 5, 20, &policy), BudgetVerdict::Allowed);
    }
}
