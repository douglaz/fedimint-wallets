//! The discovery SOURCE seam (phase 5 §5.1.0): the untrusted [`CandidateSource`] trait and
//! its offline [`ManualSource`]. Sources SUGGEST candidate federations; they never assert
//! trust — every structural fact is re-derived from the AUTHENTICATED config downstream
//! (§5.1.2), so a wrong/hostile source can waste a config fetch but cannot promote a fed.
//!
//! This module keeps the source adapters small and untrusted: `ManualSource` is the offline
//! fixture/CLI source, `ObserverSource` is the HTTP source, and future sources sit behind the
//! same trait. No source is load-bearing (ADR-0020); discovery unions whatever the configured
//! sources return.

use crate::journal::{CandidateRecord, CandidateState, StructuralOutcome};
use crate::multi_client::{bridge_federation_id, JoinOutcome};
use async_trait::async_trait;
use fedimint_core::invite_code::InviteCode;
use fedimint_core::runtime;
use fedimint_core::BitcoinHash as _;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr as _;
use std::time::{Duration, Instant};
use wallet_core::{
    auto_join_budget, discover_pass_plan, discover_pass_plan_in_rotation, score_structural, Actor,
    BudgetVerdict, DiscoveryPolicy, DiscoverySource, FederationFacts, FederationId, IdempotencyKey,
    Occurrence, OperationKind, ReasonCode, ScorerPolicy, SourceStatus, WatchPolicy,
};

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
pub trait CandidateSource: Send + Sync {
    fn source(&self) -> DiscoverySource;
    async fn candidates(&self) -> SourceResult;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoverReport {
    pub sources: Vec<DiscoverSourceReport>,
    pub auto_join: AutoJoinReport,
    pub progress: DiscoverPassProgress,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiscoverPassProgress {
    pub next_cursor: Option<FederationId>,
    pub wrapped: bool,
    pub backlog: bool,
    pub attempted: u32,
    pub deferred: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundedDiscoverReport {
    pub report: DiscoverReport,
    pub next_rotation: Vec<FederationId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoverSourceReport {
    pub source: DiscoverySource,
    pub status: SourceStatus,
    pub found: u32,
    pub structurally_passed: u32,
    pub rejected: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AutoJoinReport {
    pub considered: u32,
    pub joined: u32,
    pub blocked_concurrent: u32,
    pub blocked_weekly: u32,
    pub blocked_lifetime: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct AutoJoinOutcome {
    report: AutoJoinReport,
    candidate_ids: BTreeSet<FederationId>,
    completed_window: bool,
    stopped_for_budget: bool,
}

pub(crate) enum AutoJoinAttempt {
    Joined(JoinOutcome),
    Failed(anyhow::Error),
    DeadlineElapsed,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DiscoverPassResume<'a> {
    pub cursor: Option<FederationId>,
    pub rotation: &'a [FederationId],
    pub occurrence: Occurrence,
}

#[derive(Clone, Debug)]
pub struct PreviewedCandidate {
    pub id: FederationId,
    pub facts: FederationFacts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AutoJoinCounts {
    pub concurrent_unproven: u32,
    pub weekly_auto_joins: u32,
    pub lifetime_auto_joins: u32,
}

#[async_trait]
pub(crate) trait DiscoveryBackend {
    async fn joined_federations(&self) -> anyhow::Result<BTreeSet<FederationId>>;
    async fn joined_federation_invites(&self) -> anyhow::Result<Vec<(FederationId, InviteCode)>>;
    async fn get_candidate(&self, id: FederationId) -> anyhow::Result<Option<CandidateRecord>>;
    async fn put_candidate(&self, record: CandidateRecord) -> anyhow::Result<()>;
    async fn list_candidates(&self) -> anyhow::Result<Vec<(FederationId, CandidateRecord)>>;
    async fn agent_created_federation(&self, id: FederationId) -> anyhow::Result<bool>;
    async fn preview(&self, invite: &InviteCode) -> anyhow::Result<PreviewedCandidate>;
    async fn auto_join_counts(&self, now_ms: u64) -> anyhow::Result<AutoJoinCounts>;
    async fn join_as_agent(
        &self,
        id: FederationId,
        invite: InviteCode,
        occurrence: Occurrence,
        now_ms: u64,
        join_timeout: Duration,
    ) -> AutoJoinAttempt;
    async fn record_discover(
        &self,
        key: IdempotencyKey,
        occurrence: Occurrence,
        report: &DiscoverSourceReport,
        now_ms: u64,
    ) -> anyhow::Result<()>;
    async fn record_auto_join(
        &self,
        key: IdempotencyKey,
        occurrence: Occurrence,
        report: &AutoJoinReport,
        now_ms: u64,
    ) -> anyhow::Result<()>;
}

struct AuthenticatedAnnouncement {
    report_index: usize,
    announcement: CandidateAnnouncement,
    preview: PreviewedCandidate,
}

struct AuthenticationResult {
    authenticated: Option<AuthenticatedAnnouncement>,
    attempted_preview: bool,
}

struct IndexedAnnouncement {
    report_index: usize,
    announcement: CandidateAnnouncement,
}

struct AutoJoinBounds<'a> {
    timing: DiscoverTiming,
    window: Vec<FederationId>,
    occurrence: Occurrence,
    attempted_ids: &'a mut BTreeSet<FederationId>,
}

#[derive(Clone, Copy)]
struct DiscoverTiming {
    pass_started_at: Instant,
    pass_deadline: Duration,
    preview_timeout: Duration,
}

impl DiscoverTiming {
    fn deadline_elapsed(self) -> bool {
        self.pass_started_at.elapsed() >= self.pass_deadline
    }

    fn remaining_budget(self) -> Option<Duration> {
        let remaining = self
            .pass_deadline
            .checked_sub(self.pass_started_at.elapsed())?;
        if remaining.is_zero() {
            None
        } else {
            Some(remaining)
        }
    }

    fn preview_budget(self) -> Option<Duration> {
        self.remaining_budget()
            .map(|remaining| remaining.min(self.preview_timeout))
    }

    fn source_budget(self, sources_remaining: usize) -> Option<Duration> {
        let remaining = self.remaining_budget()?;
        let sources_remaining = u32::try_from(sources_remaining.max(1)).unwrap_or(u32::MAX);
        let fair_share = remaining / sources_remaining;
        Some(if fair_share.is_zero() {
            remaining.min(Duration::from_millis(1))
        } else {
            fair_share
        })
    }
}

async fn source_candidates_with_deadline(
    source_adapter: &dyn CandidateSource,
    source: DiscoverySource,
    timing: DiscoverTiming,
    sources_remaining: usize,
) -> SourceResult {
    let Some(budget) = timing.source_budget(sources_remaining) else {
        return SourceResult {
            candidates: Vec::new(),
            status: SourceStatus::Failed(
                "discover pass deadline elapsed before source collection".to_owned(),
            ),
        };
    };
    match runtime::timeout(budget, source_adapter.candidates()).await {
        Ok(result) => result,
        Err(_elapsed) => SourceResult {
            candidates: Vec::new(),
            status: SourceStatus::Failed(format!(
                "{source:?} source collection exceeded {}ms source budget",
                budget.as_millis()
            )),
        },
    }
}

#[cfg(test)]
pub(crate) async fn run_discover_pass(
    sources: &[Box<dyn CandidateSource>],
    policy: &DiscoveryPolicy,
    backend: &impl DiscoveryBackend,
    now_ms: u64,
    nonce: &str,
) -> anyhow::Result<DiscoverReport> {
    Ok(run_discover_pass_bounded(
        sources,
        policy,
        backend,
        now_ms,
        nonce,
        &WatchPolicy::default(),
        None,
    )
    .await?
    .report)
}

#[cfg(test)]
pub(crate) async fn run_discover_pass_bounded(
    sources: &[Box<dyn CandidateSource>],
    policy: &DiscoveryPolicy,
    backend: &impl DiscoveryBackend,
    now_ms: u64,
    nonce: &str,
    watch_policy: &WatchPolicy,
    cursor: Option<FederationId>,
) -> anyhow::Result<BoundedDiscoverReport> {
    run_discover_pass_bounded_with_rotation(
        sources,
        policy,
        backend,
        now_ms,
        nonce,
        watch_policy,
        DiscoverPassResume {
            cursor,
            rotation: &[],
            occurrence: Occurrence(0),
        },
    )
    .await
}

pub(crate) async fn run_discover_pass_bounded_with_rotation(
    sources: &[Box<dyn CandidateSource>],
    policy: &DiscoveryPolicy,
    backend: &impl DiscoveryBackend,
    now_ms: u64,
    nonce: &str,
    watch_policy: &WatchPolicy,
    resume: DiscoverPassResume<'_>,
) -> anyhow::Result<BoundedDiscoverReport> {
    let cursor = resume.cursor;
    let occurrence = resume.occurrence;
    let timing = DiscoverTiming {
        pass_started_at: Instant::now(),
        pass_deadline: Duration::from_millis(watch_policy.discover_pass_deadline_ms),
        preview_timeout: Duration::from_millis(watch_policy.per_preview_timeout_ms),
    };
    let mut reports = Vec::with_capacity(sources.len());
    let mut grouped: BTreeMap<FederationId, Vec<IndexedAnnouncement>> = BTreeMap::new();

    for (source_index, source_adapter) in sources.iter().enumerate() {
        let source = source_adapter.source();
        let sources_remaining = sources.len().saturating_sub(source_index);
        let result = source_candidates_with_deadline(
            source_adapter.as_ref(),
            source,
            timing,
            sources_remaining,
        )
        .await;
        let report_index = reports.len();
        reports.push(DiscoverSourceReport {
            source,
            status: result.status.clone(),
            found: count_saturating_u32(result.candidates.len()),
            structurally_passed: 0,
            rejected: 0,
        });
        for candidate in result.candidates {
            grouped
                .entry(candidate.claimed_id)
                .or_default()
                .push(IndexedAnnouncement {
                    report_index,
                    announcement: candidate,
                });
        }
    }

    let joined = backend.joined_federations().await?;
    let joined_invites = backend.joined_federation_invites().await?;
    recover_agent_joined_candidates(&joined, &joined_invites, backend, now_ms).await?;
    let scorer_policy = discovery_scorer_policy(policy);
    let mut floored_this_pass = BTreeSet::new();
    let auto_join_candidate_ids = if policy.auto_join {
        load_auto_join_candidate_ids(backend, &joined).await?
    } else {
        BTreeSet::new()
    };
    let all_candidate_ids = grouped
        .keys()
        .copied()
        .chain(auto_join_candidate_ids.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rotation_plan = build_discover_rotation_plan(
        cursor,
        &all_candidate_ids,
        resume.rotation,
        watch_policy.max_candidates_per_pass,
    );
    let plan = rotation_plan.plan;
    let mut progress = DiscoverPassProgress {
        next_cursor: cursor,
        wrapped: false,
        backlog: false,
        attempted: 0,
        deferred: 0,
    };

    let planned_wrap = plan.wrapped;
    let planned_next_cursor = plan.next_cursor;
    let planned_window = plan.window;
    let mut structural_attempted_ids = BTreeSet::new();
    let mut completed_window = true;
    for claimed_id in planned_window.iter().copied() {
        if timing.deadline_elapsed() {
            completed_window = false;
            break;
        }
        let Some(announcements) = grouped.get(&claimed_id) else {
            continue;
        };
        let existing = backend.get_candidate(claimed_id).await?;
        if timing.deadline_elapsed() {
            completed_window = false;
            break;
        }
        if joined.contains(&claimed_id) {
            if joined_needs_refresh(existing.as_ref(), announcements) {
                let result =
                    authenticate_first_valid(announcements, backend, &mut reports, timing).await?;
                if let Some(auth) = result.authenticated {
                    structural_attempted_ids.insert(claimed_id);
                    handle_joined_candidate(auth, existing, backend, now_ms).await?;
                } else if result.attempted_preview {
                    structural_attempted_ids.insert(claimed_id);
                } else {
                    completed_window = false;
                    break;
                }
            } else {
                structural_attempted_ids.insert(claimed_id);
            }
            continue;
        }

        let needs_fetch = needs_structural_fetch(announcements, existing.as_ref(), now_ms, policy);
        if !needs_fetch {
            structural_attempted_ids.insert(claimed_id);
            continue;
        }

        let auth =
            match authenticate_first_valid(announcements, backend, &mut reports, timing).await? {
                AuthenticationResult {
                    authenticated: Some(auth),
                    ..
                } => auth,
                AuthenticationResult {
                    attempted_preview, ..
                } => {
                    if attempted_preview {
                        structural_attempted_ids.insert(claimed_id);
                        continue;
                    }
                    completed_window = false;
                    break;
                }
            };
        structural_attempted_ids.insert(claimed_id);

        let verdict = score_structural(&auth.preview.facts, &scorer_policy);
        let structural = if verdict.eligible_to_fund {
            increment_report(&mut reports, auth.report_index, |r| {
                r.structurally_passed = r.structurally_passed.saturating_add(1);
            });
            StructuralOutcome::Passed
        } else {
            increment_report(&mut reports, auth.report_index, |r| {
                r.rejected = r.rejected.saturating_add(1);
            });
            StructuralOutcome::Rejected(rejection_reason(&verdict.reasons))
        };
        let state = match (&structural, existing.as_ref().map(|r| r.state)) {
            (StructuralOutcome::Passed, Some(CandidateState::AutoJoined)) => {
                CandidateState::AutoJoined
            }
            (StructuralOutcome::Passed, Some(CandidateState::UserApproved)) => {
                CandidateState::UserApproved
            }
            (StructuralOutcome::Passed, _) => CandidateState::Discovered,
            (StructuralOutcome::Rejected(_), _) => CandidateState::Rejected,
        };
        let record = candidate_record(
            auth.preview.id,
            auth.announcement.invite,
            auth.announcement.source,
            structural,
            state,
            existing.as_ref(),
            now_ms,
        );
        backend.put_candidate(record).await?;
        floored_this_pass.insert(auth.preview.id);
    }

    for (index, report) in reports.iter().enumerate() {
        if let Err(e) = backend
            .record_discover(
                discover_ledger_key(&reports, index, nonce),
                occurrence,
                report,
                now_ms,
            )
            .await
        {
            tracing::warn!(error = ?e, "discover: recording source ledger row failed");
        }
    }

    let mut auto_join_attempted_ids = BTreeSet::new();
    let auto_join = if policy.auto_join {
        Some(
            run_auto_join(
                policy,
                backend,
                &scorer_policy,
                &joined,
                &floored_this_pass,
                now_ms,
                AutoJoinBounds {
                    timing,
                    window: planned_window.clone(),
                    occurrence,
                    attempted_ids: &mut auto_join_attempted_ids,
                },
            )
            .await?,
        )
    } else {
        None
    };
    let cursor_attempted_ids = cursor_progress_attempts(
        &structural_attempted_ids,
        auto_join.as_ref(),
        &auto_join_attempted_ids,
    );
    finalize_progress_attempts(
        &mut progress,
        cursor,
        planned_next_cursor,
        &planned_window,
        &cursor_attempted_ids,
    );
    let auto_join_completed = auto_join
        .as_ref()
        .is_none_or(|outcome| outcome.completed_window || outcome.stopped_for_budget);
    progress.wrapped =
        all_candidate_ids.is_empty() || (completed_window && auto_join_completed && planned_wrap);
    progress.backlog = !progress.wrapped && !all_candidate_ids.is_empty();
    progress.deferred = deferred_count(
        all_candidate_ids.len(),
        progress.attempted,
        progress.wrapped,
    );
    if progress.deferred > 0 {
        tracing::info!(
            deferred = progress.deferred,
            attempted = progress.attempted,
            "discover: deferred candidates to a later pass"
        );
    }
    let auto_join_report = auto_join
        .as_ref()
        .map_or_else(AutoJoinReport::default, |outcome| outcome.report.clone());
    if let Err(e) = backend
        .record_auto_join(
            IdempotencyKey(format!("autojoin:{nonce}")),
            occurrence,
            &auto_join_report,
            now_ms,
        )
        .await
    {
        tracing::warn!(error = ?e, "discover: recording auto-join ledger row failed");
    }

    Ok(BoundedDiscoverReport {
        report: DiscoverReport {
            sources: reports,
            auto_join: auto_join_report,
            progress,
        },
        next_rotation: if progress.backlog {
            rotation_plan.rotation
        } else {
            Vec::new()
        },
    })
}

async fn load_auto_join_candidate_ids(
    backend: &impl DiscoveryBackend,
    joined: &BTreeSet<FederationId>,
) -> anyhow::Result<BTreeSet<FederationId>> {
    Ok(backend
        .list_candidates()
        .await?
        .into_iter()
        .filter_map(|(_, record)| {
            (record.state == CandidateState::Discovered && !joined.contains(&record.id))
                .then_some(record.id)
        })
        .collect())
}

fn needs_structural_fetch(
    announcements: &[IndexedAnnouncement],
    existing: Option<&CandidateRecord>,
    now_ms: u64,
    policy: &DiscoveryPolicy,
) -> bool {
    let Some(existing) = existing else {
        return true;
    };
    if announcements.is_empty() {
        return false;
    }
    let stale = now_ms.saturating_sub(existing.structural_checked_at_ms)
        > policy.structural_recheck_backoff_ms;
    // Re-fetch when the stored invite is NO LONGER announced (a genuine rotation — the known
    // endpoint is gone) OR the row is stale. A differing invite that merely COEXISTS with the
    // still-announced stored invite does NOT force a re-fetch: the stored invite is still a
    // known-good way to reach the fed, and honoring the backoff here is a deliberate
    // DoS-defense — otherwise a noisy/hostile Observer could force an authenticated config
    // fetch every pass by advertising ever-changing alternate invites (the untrusted-source
    // volume/time class deferred to 5.2). A rotated invite is adopted within <= the backoff
    // window, and auto-join re-validates the invite with a fresh fetch regardless, so a truly
    // dead stored invite self-corrects. A prior review flagged the coexistence case as a bug
    // against the spec's literal wording; rejected with this evidence and the spec's 5.1.2
    // step 1 refined to match - the strict-backoff behavior is the more robust design.
    let stored_invite_announced = announcements
        .iter()
        .any(|indexed| indexed.announcement.invite == existing.invite);
    !stored_invite_announced || stale
}

async fn authenticate_first_valid(
    announcements: &[IndexedAnnouncement],
    backend: &impl DiscoveryBackend,
    reports: &mut [DiscoverSourceReport],
    timing: DiscoverTiming,
) -> anyhow::Result<AuthenticationResult> {
    let mut tried = Vec::<InviteCode>::new();
    let mut attempted_preview = false;
    for indexed in announcements {
        let announcement = &indexed.announcement;
        if tried.contains(&announcement.invite) {
            continue;
        }
        tried.push(announcement.invite.clone());
        let Some(preview_timeout) = timing.preview_budget() else {
            tracing::info!(
                federation = %announcement.claimed_id.to_hex(),
                "discover: stopped candidate previews at pass deadline"
            );
            return Ok(AuthenticationResult {
                authenticated: None,
                attempted_preview,
            });
        };
        attempted_preview = true;
        let preview =
            match preview_with_timeout(backend, &announcement.invite, preview_timeout).await {
                Ok(preview) => preview,
                Err(e) => {
                    tracing::warn!(
                        source = ?announcement.source,
                        federation = %announcement.claimed_id.to_hex(),
                        error = ?e,
                        "discover: config preview failed; leaving candidate unchanged"
                    );
                    continue;
                }
            };
        let invite_id = bridge_federation_id(announcement.invite.federation_id());
        if announcement.claimed_id != invite_id || preview.id != invite_id {
            increment_report(reports, indexed.report_index, |r| {
                r.rejected = r.rejected.saturating_add(1);
            });
            tracing::warn!(
                source = ?announcement.source,
                claimed = %announcement.claimed_id.to_hex(),
                invite = %invite_id.to_hex(),
                config = %preview.id.to_hex(),
                "discover: dropping candidate whose claimed id, invite id, and config id do not match"
            );
            continue;
        }
        return Ok(AuthenticationResult {
            authenticated: Some(AuthenticatedAnnouncement {
                report_index: indexed.report_index,
                announcement: announcement.clone(),
                preview,
            }),
            attempted_preview,
        });
    }
    Ok(AuthenticationResult {
        authenticated: None,
        attempted_preview,
    })
}

async fn preview_with_timeout(
    backend: &impl DiscoveryBackend,
    invite: &InviteCode,
    preview_timeout: Duration,
) -> anyhow::Result<PreviewedCandidate> {
    match runtime::timeout(preview_timeout, backend.preview(invite)).await {
        Ok(result) => result,
        Err(_elapsed) => anyhow::bail!(
            "config preview exceeded {}ms timeout",
            preview_timeout.as_millis()
        ),
    }
}

fn deferred_count(total_candidates: usize, attempted: u32, wrapped: bool) -> u32 {
    if total_candidates == 0 || wrapped {
        return 0;
    }
    let attempted = usize::try_from(attempted).unwrap_or(usize::MAX);
    count_saturating_u32(total_candidates.saturating_sub(attempted))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RotationPlan {
    plan: wallet_core::DiscoverPassPlan,
    rotation: Vec<FederationId>,
}

fn build_discover_rotation_plan(
    cursor: Option<FederationId>,
    all_candidate_ids_sorted: &[FederationId],
    previous_rotation: &[FederationId],
    max_candidates_per_pass: usize,
) -> RotationPlan {
    if previous_rotation.is_empty() {
        return RotationPlan {
            plan: discover_pass_plan(cursor, all_candidate_ids_sorted, max_candidates_per_pass),
            rotation: all_candidate_ids_sorted.to_vec(),
        };
    }

    let mut seen = BTreeSet::new();
    let mut rotation = Vec::new();
    for id in previous_rotation {
        if seen.insert(*id)
            && (all_candidate_ids_sorted.binary_search(id).is_ok() || cursor == Some(*id))
        {
            rotation.push(*id);
        }
    }
    for id in all_candidate_ids_sorted {
        if seen.insert(*id) {
            rotation.push(*id);
        }
    }

    let cursor_in_rotation = cursor
        .map(|cursor| rotation.contains(&cursor))
        .unwrap_or(true);
    if rotation.is_empty() || !cursor_in_rotation {
        return RotationPlan {
            plan: discover_pass_plan(cursor, all_candidate_ids_sorted, max_candidates_per_pass),
            rotation: all_candidate_ids_sorted.to_vec(),
        };
    }

    let plan = discover_pass_plan_in_rotation(
        cursor,
        &rotation,
        all_candidate_ids_sorted,
        max_candidates_per_pass,
    );
    RotationPlan { plan, rotation }
}

fn cursor_progress_attempts(
    structural_attempted: &BTreeSet<FederationId>,
    auto_join: Option<&AutoJoinOutcome>,
    auto_join_attempted: &BTreeSet<FederationId>,
) -> BTreeSet<FederationId> {
    let mut attempted = structural_attempted.clone();
    if let Some(outcome) = auto_join {
        for candidate in &outcome.candidate_ids {
            if !auto_join_attempted.contains(candidate) {
                attempted.remove(candidate);
            }
        }
        attempted.extend(auto_join_attempted.iter().copied());
    }
    attempted
}

fn finalize_progress_attempts(
    progress: &mut DiscoverPassProgress,
    cursor: Option<FederationId>,
    empty_next_cursor: Option<FederationId>,
    planned_window: &[FederationId],
    attempted: &BTreeSet<FederationId>,
) {
    if planned_window.is_empty() {
        progress.next_cursor = empty_next_cursor;
        progress.attempted = 0;
        return;
    }
    progress.next_cursor = planned_window
        .iter()
        .copied()
        .take_while(|id| attempted.contains(id))
        .last()
        .or(cursor);
    progress.attempted = count_saturating_u32(attempted.len());
}

async fn recover_agent_joined_candidates(
    joined: &BTreeSet<FederationId>,
    joined_invites: &[(FederationId, InviteCode)],
    backend: &impl DiscoveryBackend,
    now_ms: u64,
) -> anyhow::Result<()> {
    let mut existing_ids = BTreeSet::new();
    for (_, mut record) in backend.list_candidates().await? {
        existing_ids.insert(record.id);
        if !joined.contains(&record.id)
            || !matches!(
                record.state,
                CandidateState::Discovered | CandidateState::Rejected
            )
            || !backend.agent_created_federation(record.id).await?
        {
            continue;
        }
        record.state = CandidateState::AutoJoined;
        record.structural = StructuralOutcome::Passed;
        record.updated_at_ms = now_ms;
        backend.put_candidate(record).await?;
    }
    for (id, invite) in joined_invites {
        if !joined.contains(id) || existing_ids.contains(id) {
            continue;
        }
        backend
            .put_candidate(CandidateRecord {
                id: *id,
                invite: invite.clone(),
                source: DiscoverySource::Manual,
                discovered_at_ms: now_ms,
                structural: StructuralOutcome::Passed,
                structural_checked_at_ms: now_ms,
                state: CandidateState::AutoJoined,
                updated_at_ms: now_ms,
            })
            .await?;
    }
    Ok(())
}

/// Whether a JOINED federation's candidate row needs a config fetch this pass (§5.1.2 step 0).
/// An up-to-date `AutoJoined`/`UserApproved` row whose stored invite matches every announced
/// invite needs nothing — skip the network round-trip. Only a MISSING row (a restore artifact to
/// seed `AutoJoined`) or a ROTATED invite (an announced invite differing from the stored one, to
/// refresh) is worth a fetch; a `Discovered`/`Rejected` row with no Agent join history is
/// superseded by membership and left untouched WITHOUT a fetch. This keeps N joined feds from
/// costing N wasted fetches per pass.
fn joined_needs_refresh(
    existing: Option<&CandidateRecord>,
    announcements: &[IndexedAnnouncement],
) -> bool {
    match existing {
        None => true,
        Some(record)
            if matches!(
                record.state,
                CandidateState::AutoJoined | CandidateState::UserApproved
            ) =>
        {
            announcements
                .iter()
                .any(|indexed| indexed.announcement.invite != record.invite)
        }
        Some(_) => false,
    }
}

async fn handle_joined_candidate(
    auth: AuthenticatedAnnouncement,
    existing: Option<CandidateRecord>,
    backend: &impl DiscoveryBackend,
    now_ms: u64,
) -> anyhow::Result<()> {
    match existing {
        Some(mut record)
            if matches!(
                record.state,
                CandidateState::AutoJoined | CandidateState::UserApproved
            ) =>
        {
            if record.invite != auth.announcement.invite {
                record.invite = auth.announcement.invite;
                record.updated_at_ms = now_ms;
                backend.put_candidate(record).await?;
            }
        }
        Some(_) => {}
        None => {
            backend
                .put_candidate(CandidateRecord {
                    id: auth.preview.id,
                    invite: auth.announcement.invite,
                    source: auth.announcement.source,
                    discovered_at_ms: now_ms,
                    structural: StructuralOutcome::Passed,
                    structural_checked_at_ms: now_ms,
                    state: CandidateState::AutoJoined,
                    updated_at_ms: now_ms,
                })
                .await?;
        }
    }
    Ok(())
}

fn candidate_record(
    id: FederationId,
    invite: InviteCode,
    source: DiscoverySource,
    structural: StructuralOutcome,
    state: CandidateState,
    existing: Option<&CandidateRecord>,
    now_ms: u64,
) -> CandidateRecord {
    CandidateRecord {
        id,
        invite,
        source: existing.map_or(source, |r| r.source),
        discovered_at_ms: existing.map_or(now_ms, |r| r.discovered_at_ms),
        structural,
        structural_checked_at_ms: now_ms,
        state,
        updated_at_ms: now_ms,
    }
}

async fn run_auto_join(
    policy: &DiscoveryPolicy,
    backend: &impl DiscoveryBackend,
    scorer_policy: &ScorerPolicy,
    joined: &BTreeSet<FederationId>,
    floored_this_pass: &BTreeSet<FederationId>,
    now_ms: u64,
    bounds: AutoJoinBounds<'_>,
) -> anyhow::Result<AutoJoinOutcome> {
    // Only NON-joined `Discovered` rows are auto-join candidates. A fed already in the joined
    // registry that still carries a `Discovered` row is user/restored-owned and must not be
    // re-joined as an Agent: that would record a no-op Agent join and push it behind the probe
    // gate + concurrent cap.
    let candidates: BTreeMap<FederationId, CandidateRecord> = backend
        .list_candidates()
        .await?
        .into_iter()
        .filter(|(_, record)| {
            record.state == CandidateState::Discovered && !joined.contains(&record.id)
        })
        .map(|(_, record)| (record.id, record))
        .collect();

    let mut report = AutoJoinReport::default();
    let candidate_ids = candidates.keys().copied().collect::<BTreeSet<_>>();
    if candidates.is_empty() {
        return Ok(AutoJoinOutcome {
            report,
            candidate_ids,
            completed_window: true,
            stopped_for_budget: false,
        });
    }
    let AutoJoinBounds {
        timing,
        window,
        occurrence,
        attempted_ids,
    } = bounds;
    let mut completed_window = true;
    let mut stopped_for_budget = false;
    let mut counts = backend.auto_join_counts(now_ms).await?;
    for id in window {
        if timing.deadline_elapsed() {
            completed_window = false;
            tracing::info!(
                considered = report.considered,
                "discover: auto-join stopped at pass deadline"
            );
            break;
        }
        let Some(mut candidate) = candidates.get(&id).cloned() else {
            continue;
        };
        report.considered = report.considered.saturating_add(1);

        match auto_join_budget(
            counts.concurrent_unproven,
            counts.weekly_auto_joins,
            counts.lifetime_auto_joins,
            policy,
        ) {
            BudgetVerdict::Allowed => {}
            BudgetVerdict::BlockedConcurrent => {
                attempted_ids.insert(candidate.id);
                report.blocked_concurrent = report.blocked_concurrent.saturating_add(1);
                continue;
            }
            BudgetVerdict::BlockedWeekly => {
                attempted_ids.insert(candidate.id);
                report.blocked_weekly = report.blocked_weekly.saturating_add(1);
                stopped_for_budget = true;
                break;
            }
            BudgetVerdict::BlockedLifetime => {
                attempted_ids.insert(candidate.id);
                report.blocked_lifetime = report.blocked_lifetime.saturating_add(1);
                stopped_for_budget = true;
                break;
            }
        }

        if !floored_this_pass.contains(&candidate.id) {
            let Some(preview_timeout) = timing.preview_budget() else {
                completed_window = false;
                tracing::info!(
                    considered = report.considered,
                    "discover: auto-join stopped at pass deadline"
                );
                break;
            };
            attempted_ids.insert(candidate.id);
            match preview_with_timeout(backend, &candidate.invite, preview_timeout).await {
                Ok(preview) => {
                    let invite_id = bridge_federation_id(candidate.invite.federation_id());
                    if preview.id != candidate.id || preview.id != invite_id {
                        candidate.state = CandidateState::Rejected;
                        candidate.structural = StructuralOutcome::Rejected("IdMismatch".into());
                        candidate.structural_checked_at_ms = now_ms;
                        candidate.updated_at_ms = now_ms;
                        backend.put_candidate(candidate).await?;
                        continue;
                    }
                    let verdict = score_structural(&preview.facts, scorer_policy);
                    if !verdict.eligible_to_fund {
                        candidate.state = CandidateState::Rejected;
                        candidate.structural =
                            StructuralOutcome::Rejected(rejection_reason(&verdict.reasons));
                        candidate.structural_checked_at_ms = now_ms;
                        candidate.updated_at_ms = now_ms;
                        backend.put_candidate(candidate).await?;
                        continue;
                    }
                    candidate.structural = StructuralOutcome::Passed;
                    candidate.structural_checked_at_ms = now_ms;
                    candidate.updated_at_ms = now_ms;
                    backend.put_candidate(candidate.clone()).await?;
                }
                Err(e) => {
                    tracing::warn!(
                        federation = %candidate.id.to_hex(),
                        error = ?e,
                        "discover: auto-join revalidation failed; leaving candidate discovered"
                    );
                    continue;
                }
            }
        } else {
            attempted_ids.insert(candidate.id);
        }

        match join_as_agent_with_timeout(backend, &candidate, occurrence, now_ms, timing).await {
            AutoJoinAttempt::Joined(outcome) => {
                if outcome.newly_joined {
                    candidate.state = CandidateState::AutoJoined;
                    candidate.updated_at_ms = now_ms;
                    backend.put_candidate(candidate).await?;
                    report.joined = report.joined.saturating_add(1);
                    counts.concurrent_unproven = counts.concurrent_unproven.saturating_add(1);
                    counts.weekly_auto_joins = counts.weekly_auto_joins.saturating_add(1);
                    counts.lifetime_auto_joins = counts.lifetime_auto_joins.saturating_add(1);
                } else {
                    tracing::warn!(
                        federation = %candidate.id.to_hex(),
                        "discover: auto-join reopened an existing federation; leaving candidate discovered"
                    );
                }
            }
            AutoJoinAttempt::Failed(e) => tracing::warn!(
                federation = %candidate.id.to_hex(),
                error = ?e,
                "discover: auto-join failed; leaving candidate discovered"
            ),
            AutoJoinAttempt::DeadlineElapsed => {
                completed_window = false;
                tracing::info!(
                    considered = report.considered,
                    federation = %candidate.id.to_hex(),
                    "discover: auto-join stopped at pass deadline"
                );
                break;
            }
        }
    }
    Ok(AutoJoinOutcome {
        report,
        candidate_ids,
        completed_window,
        stopped_for_budget,
    })
}

async fn join_as_agent_with_timeout(
    backend: &impl DiscoveryBackend,
    candidate: &CandidateRecord,
    occurrence: Occurrence,
    now_ms: u64,
    timing: DiscoverTiming,
) -> AutoJoinAttempt {
    let Some(join_timeout) = timing.remaining_budget() else {
        return AutoJoinAttempt::DeadlineElapsed;
    };
    backend
        .join_as_agent(
            candidate.id,
            candidate.invite.clone(),
            occurrence,
            now_ms,
            join_timeout,
        )
        .await
}

fn discovery_scorer_policy(policy: &DiscoveryPolicy) -> ScorerPolicy {
    ScorerPolicy {
        require_mainnet: policy.require_mainnet,
        ..ScorerPolicy::default()
    }
}

fn rejection_reason(reasons: &[wallet_core::ScorerReasonCode]) -> String {
    reasons
        .first()
        .map_or_else(|| "Rejected".to_owned(), |reason| format!("{reason:?}"))
}

fn increment_report(
    reports: &mut [DiscoverSourceReport],
    index: usize,
    f: impl FnOnce(&mut DiscoverSourceReport),
) {
    if let Some(report) = reports.get_mut(index) {
        f(report);
    }
}

fn count_saturating_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

fn discovery_source_label(source: DiscoverySource) -> &'static str {
    match source {
        DiscoverySource::Observer => "observer",
        DiscoverySource::Nostr => "nostr",
        DiscoverySource::Manual => "manual",
    }
}

fn discover_ledger_key(
    reports: &[DiscoverSourceReport],
    index: usize,
    nonce: &str,
) -> IdempotencyKey {
    let report = &reports[index];
    let label = discovery_source_label(report.source);
    let duplicate_variant = reports
        .iter()
        .filter(|candidate| candidate.source == report.source)
        .count()
        > 1;
    if duplicate_variant {
        IdempotencyKey(format!("discover:{label}:{index}:{nonce}"))
    } else {
        IdempotencyKey(format!("discover:{label}:{nonce}"))
    }
}

pub(crate) fn discover_kind(report: &DiscoverSourceReport) -> OperationKind {
    OperationKind::Discover {
        source: report.source,
        status: report.status.clone(),
        found: report.found,
        structurally_passed: report.structurally_passed,
        rejected: report.rejected,
    }
}

pub(crate) fn auto_join_kind(report: &AutoJoinReport) -> OperationKind {
    OperationKind::AutoJoin {
        considered: report.considered,
        joined: report.joined,
        blocked_concurrent: report.blocked_concurrent,
        blocked_weekly: report.blocked_weekly,
        blocked_lifetime: report.blocked_lifetime,
    }
}

pub(crate) fn discovery_actor(occurrence: Occurrence) -> Actor {
    Actor::Agent { occurrence }
}

pub(crate) const DISCOVERY_REASON: ReasonCode = ReasonCode::StandingInstruction;

/// A fixed candidate list — a CLI `--invite` list or a test fixture — that ALWAYS reports
/// `status: Ok` (§5.1.0). The offline + live-gate source: it needs no network, so it drives
/// both the unit tests and the devimint gate (pointed at the harness's fed B).
pub struct ManualSource {
    candidates: Vec<CandidateAnnouncement>,
}

impl ManualSource {
    /// Wrap an explicit announcement list, normalizing provenance to `Manual`.
    pub fn new(mut candidates: Vec<CandidateAnnouncement>) -> Self {
        for candidate in &mut candidates {
            candidate.source = DiscoverySource::Manual;
        }
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
    fn source(&self) -> DiscoverySource {
        DiscoverySource::Manual
    }

    async fn candidates(&self) -> SourceResult {
        SourceResult {
            candidates: self.candidates.clone(),
            status: SourceStatus::Ok,
        }
    }
}

/// Fedimint Observer HTTP source (§5.1.5). The Observer is discovery-only and untrusted:
/// rows are converted to candidate announcements, then the pipeline re-fetches authenticated
/// configs before any registry promotion.
pub struct ObserverSource {
    base_url: String,
    http: reqwest::Client,
}

/// Overall per-request timeout for the untrusted Observer HTTP source (§5.1.0/§5.1.5).
/// Discovery is best-effort: a slow, stalled, or hostile endpoint must degrade to
/// `SourceStatus::Failed`, never hang the whole `discover` pass. reqwest applies this from
/// connect through response-body read, so a mid-stream stall trips it too.
const OBSERVER_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const OBSERVER_MAX_BODY_BYTES: usize = 1024 * 1024;

impl ObserverSource {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(OBSERVER_HTTP_TIMEOUT)
            .build()
            .expect("failed to build timeout-configured Observer HTTP client");
        Self::with_client(base_url, http)
    }

    pub fn with_client(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.into(),
            http,
        }
    }

    fn federations_url(&self) -> String {
        format!("{}/federations", self.base_url.trim_end_matches('/'))
    }
}

#[async_trait]
impl CandidateSource for ObserverSource {
    fn source(&self) -> DiscoverySource {
        DiscoverySource::Observer
    }

    async fn candidates(&self) -> SourceResult {
        let response = match self.http.get(self.federations_url()).send().await {
            Ok(response) => response,
            Err(e) => {
                return SourceResult {
                    candidates: Vec::new(),
                    status: SourceStatus::Failed(e.to_string()),
                };
            }
        };
        let body = match response.error_for_status() {
            Ok(response) => match read_observer_body(response).await {
                Ok(body) => body,
                Err(e) => {
                    return SourceResult {
                        candidates: Vec::new(),
                        status: SourceStatus::Failed(e.to_string()),
                    };
                }
            },
            Err(e) => {
                return SourceResult {
                    candidates: Vec::new(),
                    status: SourceStatus::Failed(e.to_string()),
                };
            }
        };

        match parse_observer_federations(&body) {
            Ok(candidates) => SourceResult {
                candidates,
                status: SourceStatus::Ok,
            },
            Err(e) => SourceResult {
                candidates: Vec::new(),
                status: SourceStatus::Failed(e),
            },
        }
    }
}

async fn read_observer_body(mut response: reqwest::Response) -> Result<String, String> {
    if response
        .content_length()
        .is_some_and(|len| len > OBSERVER_MAX_BODY_BYTES as u64)
    {
        return Err(format!(
            "observer response exceeds {OBSERVER_MAX_BODY_BYTES} byte limit"
        ));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if body.len().saturating_add(chunk.len()) > OBSERVER_MAX_BODY_BYTES {
            return Err(format!(
                "observer response exceeds {OBSERVER_MAX_BODY_BYTES} byte limit"
            ));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|e| format!("observer response is not valid UTF-8: {e}"))
}

/// Parse a `GET /federations` response into candidate announcements. Unknown fields are
/// ignored and malformed rows are skipped. An empty body is a healthy empty result; malformed
/// JSON or the wrong top-level shape is a source failure.
pub fn parse_observer_federations(body: &str) -> Result<Vec<CandidateAnnouncement>, String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let value = serde_json::from_str::<serde_json::Value>(trimmed)
        .map_err(|e| format!("observer response is not valid JSON: {e}"))?;
    let rows = observer_rows(&value).ok_or_else(|| {
        "observer response must be an array or contain a federations array".to_owned()
    })?;
    Ok(rows
        .iter()
        .filter_map(parse_observer_row)
        .collect::<Vec<CandidateAnnouncement>>())
}

fn observer_rows(value: &serde_json::Value) -> Option<&[serde_json::Value]> {
    if let Some(rows) = value.as_array() {
        return Some(rows.as_slice());
    }
    value
        .get("federations")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
}

fn parse_observer_row(row: &serde_json::Value) -> Option<CandidateAnnouncement> {
    let id = row.get("id")?.as_str()?;
    let invite = row.get("invite")?.as_str()?;
    let claimed_id = parse_federation_id(id).ok()?;
    let invite = InviteCode::from_str(invite).ok()?;
    let network_hint = row
        .get("network")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    Some(CandidateAnnouncement {
        claimed_id,
        invite,
        network_hint,
        source: DiscoverySource::Observer,
    })
}

fn parse_federation_id(hex: &str) -> anyhow::Result<FederationId> {
    let id = fedimint_core::config::FederationId::from_str(hex)
        .map_err(|e| anyhow::anyhow!("invalid federation id {hex:?}: {e}"))?;
    Ok(FederationId(id.0.to_byte_array()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fedimint_core::util::SafeUrl;
    use fedimint_core::PeerId;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::Mutex;
    use std::time::Duration;
    use wallet_core::{Module, WatchPolicy};

    fn test_invite() -> InviteCode {
        InviteCode::from_str(
            "fed11qgqpu8rhwden5te0vejkg6tdd9h8gepwd4cxcumxv4jzuen0duhsqqfqh6nl7sgk72caxfx8khtfnn8y436q3nhyrkev3qp8ugdhdllnh86qmp42pm",
        )
        .expect("valid invite code")
    }

    fn fed(byte: u8) -> FederationId {
        FederationId([byte; 32])
    }

    fn fedimint_id(id: FederationId) -> fedimint_core::config::FederationId {
        fedimint_core::config::FederationId::from_str(&id.to_hex()).expect("valid fed id")
    }

    fn invite_for(id: FederationId, host: &str) -> InviteCode {
        InviteCode::new(
            SafeUrl::parse(&format!("https://{host}.example")).expect("valid url"),
            PeerId::from(0),
            fedimint_id(id),
            None,
        )
    }

    fn good_facts(id: FederationId) -> FederationFacts {
        FederationFacts {
            id,
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            modules: vec![Module::Mint, Module::Wallet, Module::Lnv2],
            quorum_live: false,
            round_trip_ok: false,
            peg_out_quotable: false,
            latency_ms: 0,
            shutdown_scheduled: false,
            has_lnv2: true,
            observer: None,
            active_probe: None,
        }
    }

    fn candidate(
        id: FederationId,
        invite: InviteCode,
        state: CandidateState,
        checked_at: u64,
    ) -> CandidateRecord {
        CandidateRecord {
            id,
            invite,
            source: DiscoverySource::Manual,
            discovered_at_ms: 1_000,
            structural: if state == CandidateState::Rejected {
                StructuralOutcome::Rejected("MissingModule".into())
            } else {
                StructuralOutcome::Passed
            },
            structural_checked_at_ms: checked_at,
            state,
            updated_at_ms: checked_at,
        }
    }

    #[derive(Clone)]
    enum PreviewReply {
        Ok(PreviewedCandidate),
        Fail(&'static str),
        Pending,
        SleepThenOk(Duration, PreviewedCandidate),
    }

    #[derive(Clone)]
    enum JoinReply {
        Pending,
    }

    struct FakeSource {
        source: DiscoverySource,
        result: SourceResult,
    }

    #[async_trait]
    impl CandidateSource for FakeSource {
        fn source(&self) -> DiscoverySource {
            self.source
        }

        async fn candidates(&self) -> SourceResult {
            SourceResult {
                candidates: self.result.candidates.clone(),
                status: self.result.status.clone(),
            }
        }
    }

    struct SlowSource {
        source: DiscoverySource,
        delay: Duration,
        result: SourceResult,
    }

    #[async_trait]
    impl CandidateSource for SlowSource {
        fn source(&self) -> DiscoverySource {
            self.source
        }

        async fn candidates(&self) -> SourceResult {
            fedimint_core::runtime::sleep(self.delay).await;
            SourceResult {
                candidates: self.result.candidates.clone(),
                status: self.result.status.clone(),
            }
        }
    }

    #[derive(Default)]
    struct FakeBackend {
        joined: Mutex<BTreeSet<FederationId>>,
        joined_invites: Mutex<BTreeMap<FederationId, InviteCode>>,
        candidates: Mutex<BTreeMap<FederationId, CandidateRecord>>,
        get_candidate_delay: Mutex<Option<Duration>>,
        previews: Mutex<BTreeMap<String, PreviewReply>>,
        previewed: Mutex<Vec<String>>,
        join_replies: Mutex<BTreeMap<FederationId, JoinReply>>,
        counts: Mutex<AutoJoinCounts>,
        count_calls: Mutex<u32>,
        agent_created: Mutex<BTreeSet<FederationId>>,
        joins: Mutex<Vec<FederationId>>,
        join_occurrences: Mutex<Vec<(FederationId, Occurrence)>>,
        discover_keys: Mutex<Vec<IdempotencyKey>>,
        discover_rows: Mutex<Vec<DiscoverSourceReport>>,
        auto_rows: Mutex<Vec<AutoJoinReport>>,
    }

    impl FakeBackend {
        fn with_preview(&self, invite: &InviteCode, reply: PreviewReply) {
            self.previews
                .lock()
                .expect("previews lock")
                .insert(invite.to_string(), reply);
        }

        fn with_join(&self, id: FederationId, reply: JoinReply) {
            self.join_replies
                .lock()
                .expect("join replies lock")
                .insert(id, reply);
        }

        fn put_existing(&self, record: CandidateRecord) {
            self.candidates
                .lock()
                .expect("candidates lock")
                .insert(record.id, record);
        }

        fn delay_get_candidate(&self, delay: Duration) {
            *self
                .get_candidate_delay
                .lock()
                .expect("get-candidate delay lock") = Some(delay);
        }

        fn put_joined(&self, id: FederationId, invite: InviteCode) {
            self.joined.lock().expect("joined lock").insert(id);
            self.joined_invites
                .lock()
                .expect("joined invites lock")
                .insert(id, invite);
        }

        fn get_record(&self, id: FederationId) -> Option<CandidateRecord> {
            self.candidates
                .lock()
                .expect("candidates lock")
                .get(&id)
                .cloned()
        }
    }

    #[async_trait]
    impl DiscoveryBackend for FakeBackend {
        async fn joined_federations(&self) -> anyhow::Result<BTreeSet<FederationId>> {
            Ok(self.joined.lock().expect("joined lock").clone())
        }

        async fn joined_federation_invites(
            &self,
        ) -> anyhow::Result<Vec<(FederationId, InviteCode)>> {
            Ok(self
                .joined_invites
                .lock()
                .expect("joined invites lock")
                .iter()
                .map(|(id, invite)| (*id, invite.clone()))
                .collect())
        }

        async fn get_candidate(&self, id: FederationId) -> anyhow::Result<Option<CandidateRecord>> {
            let delay = *self
                .get_candidate_delay
                .lock()
                .expect("get-candidate delay lock");
            if let Some(delay) = delay {
                fedimint_core::runtime::sleep(delay).await;
            }
            Ok(self
                .candidates
                .lock()
                .expect("candidates lock")
                .get(&id)
                .cloned())
        }

        async fn put_candidate(&self, record: CandidateRecord) -> anyhow::Result<()> {
            self.candidates
                .lock()
                .expect("candidates lock")
                .insert(record.id, record);
            Ok(())
        }

        async fn list_candidates(&self) -> anyhow::Result<Vec<(FederationId, CandidateRecord)>> {
            Ok(self
                .candidates
                .lock()
                .expect("candidates lock")
                .iter()
                .map(|(id, record)| (*id, record.clone()))
                .collect())
        }

        async fn agent_created_federation(&self, id: FederationId) -> anyhow::Result<bool> {
            Ok(self
                .agent_created
                .lock()
                .expect("agent-created lock")
                .contains(&id))
        }

        async fn preview(&self, invite: &InviteCode) -> anyhow::Result<PreviewedCandidate> {
            self.previewed
                .lock()
                .expect("previewed lock")
                .push(invite.to_string());
            let reply = {
                self.previews
                    .lock()
                    .expect("previews lock")
                    .get(&invite.to_string())
                    .cloned()
            };
            match reply {
                Some(PreviewReply::Ok(preview)) => Ok(preview),
                Some(PreviewReply::Fail(reason)) => anyhow::bail!("{reason}"),
                Some(PreviewReply::Pending) => std::future::pending().await,
                Some(PreviewReply::SleepThenOk(duration, preview)) => {
                    fedimint_core::runtime::sleep(duration).await;
                    Ok(preview)
                }
                None => anyhow::bail!("no preview fixture"),
            }
        }

        async fn auto_join_counts(&self, _now_ms: u64) -> anyhow::Result<AutoJoinCounts> {
            let mut calls = self.count_calls.lock().expect("count calls lock");
            *calls = calls.saturating_add(1);
            Ok(*self.counts.lock().expect("counts lock"))
        }

        async fn join_as_agent(
            &self,
            id: FederationId,
            _invite: InviteCode,
            occurrence: Occurrence,
            _now_ms: u64,
            join_timeout: Duration,
        ) -> AutoJoinAttempt {
            self.joins.lock().expect("joins lock").push(id);
            self.join_occurrences
                .lock()
                .expect("join occurrences lock")
                .push((id, occurrence));
            let reply = self
                .join_replies
                .lock()
                .expect("join replies lock")
                .get(&id)
                .cloned();
            match reply {
                Some(JoinReply::Pending) => {
                    let _ = runtime::timeout(join_timeout, std::future::pending::<()>()).await;
                    AutoJoinAttempt::DeadlineElapsed
                }
                None => AutoJoinAttempt::Joined(JoinOutcome {
                    id,
                    newly_joined: true,
                }),
            }
        }

        async fn record_discover(
            &self,
            key: IdempotencyKey,
            _occurrence: Occurrence,
            report: &DiscoverSourceReport,
            _now_ms: u64,
        ) -> anyhow::Result<()> {
            self.discover_keys
                .lock()
                .expect("discover keys lock")
                .push(key);
            self.discover_rows
                .lock()
                .expect("discover rows lock")
                .push(report.clone());
            Ok(())
        }

        async fn record_auto_join(
            &self,
            _key: IdempotencyKey,
            _occurrence: Occurrence,
            report: &AutoJoinReport,
            _now_ms: u64,
        ) -> anyhow::Result<()> {
            self.auto_rows
                .lock()
                .expect("auto rows lock")
                .push(report.clone());
            Ok(())
        }
    }

    fn boxed_source(announcement: CandidateAnnouncement) -> Box<dyn CandidateSource> {
        Box::new(FakeSource {
            source: announcement.source,
            result: SourceResult {
                candidates: vec![announcement],
                status: SourceStatus::Ok,
            },
        })
    }

    fn announcement(
        claimed_id: FederationId,
        invite: InviteCode,
        source: DiscoverySource,
    ) -> CandidateAnnouncement {
        CandidateAnnouncement {
            claimed_id,
            invite,
            network_hint: None,
            source,
        }
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

    #[test]
    fn observer_parse_maps_claimed_id_from_observer_field_and_skips_bad_rows() {
        let invite = test_invite();
        let invite_id = bridge_federation_id(invite.federation_id());
        let other_id = FederationId([0x42; 32]);
        let body = serde_json::json!({
            "federations": [
                {
                    "id": other_id.to_hex(),
                    "name": "observer claim differs from invite",
                    "invite": invite.to_string(),
                    "network": "bitcoin",
                    "ignored": {"field": true}
                },
                {
                    "id": invite_id.to_hex(),
                    "invite": "not an invite"
                },
                {
                    "id": "not hex",
                    "invite": invite.to_string()
                }
            ]
        })
        .to_string();

        let parsed = parse_observer_federations(&body).expect("observer fixture parses");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].claimed_id, other_id);
        assert_eq!(parsed[0].invite, invite);
        assert_eq!(parsed[0].network_hint.as_deref(), Some("bitcoin"));
        assert_eq!(parsed[0].source, DiscoverySource::Observer);
        assert_ne!(parsed[0].claimed_id, invite_id);
    }

    #[test]
    fn observer_parse_accepts_top_level_array_and_empty_body() {
        let invite = test_invite();
        let id = bridge_federation_id(invite.federation_id());
        let body = serde_json::json!([
            {
                "id": id.to_hex(),
                "invite": invite.to_string()
            }
        ])
        .to_string();

        assert_eq!(
            parse_observer_federations("")
                .expect("empty body parses")
                .len(),
            0
        );
        assert_eq!(
            parse_observer_federations("   ")
                .expect("blank body parses")
                .len(),
            0
        );

        let parsed = parse_observer_federations(&body).expect("observer array parses");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].claimed_id, id);
        assert_eq!(parsed[0].invite, invite);
    }

    #[test]
    fn observer_parse_rejects_bad_top_level_documents() {
        assert!(parse_observer_federations("{")
            .expect_err("invalid JSON is a source failure")
            .contains("valid JSON"));
        assert!(parse_observer_federations(r#"{"not_federations":[]}"#)
            .expect_err("wrong shape is a source failure")
            .contains("federations array"));
    }

    #[tokio::test]
    async fn pipeline_drops_claimed_id_mismatch_without_writing_candidate() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let invite_id = fed(1);
        let claimed = fed(2);
        let invite = invite_for(invite_id, "good");
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: invite_id,
                facts: good_facts(invite_id),
            }),
        );
        let sources = vec![boxed_source(announcement(
            claimed,
            invite,
            DiscoverySource::Manual,
        ))];

        let report = run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_000,
            "0000000000000001",
        )
        .await?;

        assert!(backend.get_record(claimed).is_none());
        assert_eq!(report.sources[0].rejected, 1);
        Ok(())
    }

    #[tokio::test]
    async fn pipeline_short_circuits_joined_candidates_preserving_provenance() -> anyhow::Result<()>
    {
        let backend = FakeBackend::default();
        let id = fed(3);
        let old_invite = invite_for(id, "old");
        let new_invite = invite_for(id, "new");
        backend.joined.lock().expect("joined lock").insert(id);
        backend.put_existing(candidate(
            id,
            old_invite,
            CandidateState::UserApproved,
            1_000,
        ));
        backend.with_preview(
            &new_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources = vec![boxed_source(announcement(
            id,
            new_invite.clone(),
            DiscoverySource::Observer,
        ))];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_000,
            "0000000000000002",
        )
        .await?;

        let updated = backend.get_record(id).expect("candidate updated");
        assert_eq!(updated.state, CandidateState::UserApproved);
        assert_eq!(updated.source, DiscoverySource::Manual);
        assert_eq!(updated.invite, new_invite);

        let restored = fed(4);
        let restored_invite = invite_for(restored, "restored");
        backend.joined.lock().expect("joined lock").insert(restored);
        backend.with_preview(
            &restored_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: restored,
                facts: good_facts(restored),
            }),
        );
        let sources = vec![boxed_source(announcement(
            restored,
            restored_invite,
            DiscoverySource::Manual,
        ))];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            11_000,
            "0000000000000003",
        )
        .await?;

        let seeded = backend.get_record(restored).expect("restore row seeded");
        assert_eq!(seeded.state, CandidateState::AutoJoined);
        Ok(())
    }

    #[tokio::test]
    async fn missing_joined_candidate_row_is_seeded_from_registry_without_source(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x40);
        let invite = invite_for(id, "registry-only");
        backend.put_joined(id, invite.clone());
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();

        let report = run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            12_000,
            "0000000000000040",
        )
        .await?;

        let seeded = backend.get_record(id).expect("restore row seeded");
        assert_eq!(seeded.state, CandidateState::AutoJoined);
        assert_eq!(seeded.invite, invite);
        assert_eq!(seeded.structural, StructuralOutcome::Passed);
        assert!(
            backend.previewed.lock().expect("previewed lock").is_empty(),
            "registry-only recovery must not require a live source config fetch"
        );
        assert_eq!(report.auto_join.considered, 0);
        Ok(())
    }

    #[tokio::test]
    async fn stale_rejected_candidate_is_re_floored_to_discovered() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(5);
        let invite = invite_for(id, "stale");
        backend.put_existing(candidate(
            id,
            invite.clone(),
            CandidateState::Rejected,
            1_000,
        ));
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources = vec![boxed_source(announcement(
            id,
            invite,
            DiscoverySource::Manual,
        ))];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            1_000 + wallet_core::STRUCTURAL_RECHECK_BACKOFF_MS + 1,
            "0000000000000004",
        )
        .await?;

        let updated = backend.get_record(id).expect("candidate re-floored");
        assert_eq!(updated.state, CandidateState::Discovered);
        assert_eq!(updated.structural, StructuralOutcome::Passed);
        Ok(())
    }

    #[tokio::test]
    async fn extra_distinct_invite_does_not_defeat_backoff_when_stored_invite_is_announced(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x41);
        let stored = invite_for(id, "stored");
        let alternate = invite_for(id, "alternate");
        backend.put_existing(candidate(
            id,
            stored.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        let sources: Vec<Box<dyn CandidateSource>> = vec![
            Box::new(FakeSource {
                source: DiscoverySource::Observer,
                result: SourceResult {
                    candidates: vec![announcement(id, alternate, DiscoverySource::Observer)],
                    status: SourceStatus::Ok,
                },
            }),
            Box::new(FakeSource {
                source: DiscoverySource::Manual,
                result: SourceResult {
                    candidates: vec![announcement(id, stored.clone(), DiscoverySource::Manual)],
                    status: SourceStatus::Ok,
                },
            }),
        ];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_001,
            "0000000000000041",
        )
        .await?;

        assert!(
            backend.previewed.lock().expect("previewed lock").is_empty(),
            "an up-to-date row with its stored invite still announced should not re-fetch every pass"
        );
        assert_eq!(
            backend.get_record(id).expect("candidate unchanged").invite,
            stored
        );
        Ok(())
    }

    #[tokio::test]
    async fn multiple_invites_for_one_id_reconcile_to_first_valid_match() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(6);
        let wrong_id = fed(7);
        let wrong_invite = invite_for(wrong_id, "wrong");
        let good_invite = invite_for(id, "right");
        backend.with_preview(&wrong_invite, PreviewReply::Fail("stale invite"));
        backend.with_preview(
            &good_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![
            Box::new(FakeSource {
                source: DiscoverySource::Observer,
                result: SourceResult {
                    candidates: vec![announcement(id, wrong_invite, DiscoverySource::Observer)],
                    status: SourceStatus::Ok,
                },
            }),
            Box::new(FakeSource {
                source: DiscoverySource::Manual,
                result: SourceResult {
                    candidates: vec![announcement(
                        id,
                        good_invite.clone(),
                        DiscoverySource::Manual,
                    )],
                    status: SourceStatus::Ok,
                },
            }),
        ];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_000,
            "0000000000000005",
        )
        .await?;

        let updated = backend.get_record(id).expect("candidate discovered");
        assert_eq!(updated.state, CandidateState::Discovered);
        assert_eq!(updated.invite, good_invite);
        Ok(())
    }

    #[tokio::test]
    async fn same_variant_sources_keep_separate_tallies_and_ledger_keys() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let rejected_id = fed(0x42);
        let invite_id = fed(0x43);
        let rejected_invite = invite_for(invite_id, "mismatch");
        backend.with_preview(
            &rejected_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: invite_id,
                facts: good_facts(invite_id),
            }),
        );

        let passed_id = fed(0x44);
        let passed_invite = invite_for(passed_id, "passed");
        backend.with_preview(
            &passed_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: passed_id,
                facts: good_facts(passed_id),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![
            Box::new(FakeSource {
                source: DiscoverySource::Manual,
                result: SourceResult {
                    candidates: vec![announcement(
                        rejected_id,
                        rejected_invite,
                        DiscoverySource::Manual,
                    )],
                    status: SourceStatus::Ok,
                },
            }),
            Box::new(FakeSource {
                source: DiscoverySource::Manual,
                result: SourceResult {
                    candidates: vec![announcement(
                        passed_id,
                        passed_invite,
                        DiscoverySource::Manual,
                    )],
                    status: SourceStatus::Ok,
                },
            }),
        ];

        let report = run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_000,
            "0000000000000042",
        )
        .await?;

        assert_eq!(report.sources.len(), 2);
        assert_eq!(report.sources[0].rejected, 1);
        assert_eq!(report.sources[0].structurally_passed, 0);
        assert_eq!(report.sources[1].rejected, 0);
        assert_eq!(report.sources[1].structurally_passed, 1);
        let keys = backend.discover_keys.lock().expect("discover keys lock");
        assert_eq!(keys[0].0, "discover:manual:0:0000000000000042");
        assert_eq!(keys[1].0, "discover:manual:1:0000000000000042");
        Ok(())
    }

    #[tokio::test]
    async fn auto_join_caps_each_block_and_are_recorded() -> anyhow::Result<()> {
        let cases = [
            (
                AutoJoinCounts {
                    concurrent_unproven: 3,
                    weekly_auto_joins: 0,
                    lifetime_auto_joins: 0,
                },
                (1, 0, 0),
            ),
            (
                AutoJoinCounts {
                    concurrent_unproven: 0,
                    weekly_auto_joins: 5,
                    lifetime_auto_joins: 0,
                },
                (0, 1, 0),
            ),
            (
                AutoJoinCounts {
                    concurrent_unproven: 0,
                    weekly_auto_joins: 0,
                    lifetime_auto_joins: 20,
                },
                (0, 0, 1),
            ),
        ];

        for (counts, expected) in cases {
            let backend = FakeBackend::default();
            *backend.counts.lock().expect("counts lock") = counts;
            let id = fed(8);
            let invite = invite_for(id, "cap");
            backend.put_existing(candidate(
                id,
                invite.clone(),
                CandidateState::Discovered,
                10_000,
            ));
            backend.with_preview(
                &invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id,
                    facts: good_facts(id),
                }),
            );
            let policy = DiscoveryPolicy {
                auto_join: true,
                ..DiscoveryPolicy::default()
            };
            let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
                source: DiscoverySource::Manual,
                result: SourceResult {
                    candidates: Vec::new(),
                    status: SourceStatus::Ok,
                },
            })];

            let report =
                run_discover_pass(&sources, &policy, &backend, 20_000, "0000000000000006").await?;

            assert_eq!(report.auto_join.considered, 1);
            assert_eq!(report.auto_join.joined, 0);
            assert_eq!(
                (
                    report.auto_join.blocked_concurrent,
                    report.auto_join.blocked_weekly,
                    report.auto_join.blocked_lifetime,
                ),
                expected
            );
            assert_eq!(
                backend
                    .auto_rows
                    .lock()
                    .expect("auto rows lock")
                    .last()
                    .expect("auto row recorded"),
                &report.auto_join
            );
            assert!(
                backend.previewed.lock().expect("previewed lock").is_empty(),
                "a cap-blocked candidate must not trigger join-time preview"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn auto_join_counts_are_loaded_once_and_incremented_locally() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let first = fed(0x45);
        let second = fed(0x46);
        let first_invite = invite_for(first, "first-autojoin");
        let second_invite = invite_for(second, "second-autojoin");
        backend.put_existing(candidate(
            first,
            first_invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.put_existing(candidate(
            second,
            second_invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.with_preview(
            &first_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: first,
                facts: good_facts(first),
            }),
        );
        backend.with_preview(
            &second_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: second,
                facts: good_facts(second),
            }),
        );
        let policy = DiscoveryPolicy {
            auto_join: true,
            max_concurrent_unproven: 10,
            max_auto_joins_per_week: 1,
            auto_join_lifetime_cap: 10,
            ..DiscoveryPolicy::default()
        };
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();

        let report =
            run_discover_pass(&sources, &policy, &backend, 20_000, "0000000000000045").await?;

        assert_eq!(
            *backend.count_calls.lock().expect("count calls lock"),
            1,
            "auto-join counts should be scanned once per pass"
        );
        assert_eq!(report.auto_join.considered, 2);
        assert_eq!(report.auto_join.joined, 1);
        assert_eq!(report.auto_join.blocked_weekly, 1);
        assert_eq!(
            backend.joins.lock().expect("joins lock").as_slice(),
            [first]
        );
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [first_invite.to_string()],
            "the globally-blocked second candidate should not be revalidated"
        );
        Ok(())
    }

    #[tokio::test]
    async fn watch_auto_join_threads_cycle_occurrence_into_join_row() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x47);
        let invite = invite_for(id, "watch-autojoin-occurrence");
        backend.put_existing(candidate(
            id,
            invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let occurrence = Occurrence(42);

        let outcome = run_discover_pass_bounded_with_rotation(
            &sources,
            &policy,
            &backend,
            20_000,
            "0000000000000047",
            &WatchPolicy::default(),
            DiscoverPassResume {
                cursor: None,
                rotation: &[],
                occurrence,
            },
        )
        .await?;

        assert_eq!(outcome.report.auto_join.joined, 1);
        assert_eq!(
            backend
                .join_occurrences
                .lock()
                .expect("join occurrences lock")
                .as_slice(),
            [(id, occurrence)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn down_source_records_failed_status_without_failing_pass() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: Vec::new(),
                status: SourceStatus::Failed("timeout".into()),
            },
        })];

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_000,
            "0000000000000007",
            &WatchPolicy::default(),
            None,
        )
        .await?;
        let report = &outcome.report;

        assert_eq!(report.sources[0].source, DiscoverySource::Observer);
        assert_eq!(
            report.sources[0].status,
            SourceStatus::Failed("timeout".into())
        );
        assert_eq!(report.sources[0].found, 0);
        assert!(backend
            .candidates
            .lock()
            .expect("candidates lock")
            .is_empty());
        assert!(outcome.report.progress.wrapped);
        assert!(!outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.next_cursor, None);
        Ok(())
    }

    #[tokio::test]
    async fn empty_pass_resets_stale_cursor_before_later_lower_ids() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let empty_sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: Vec::new(),
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            max_candidates_per_pass: 2,
            ..WatchPolicy::default()
        };

        let empty = run_discover_pass_bounded(
            &empty_sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_250,
            "0000000000000008",
            &watch,
            Some(fed(0x15)),
        )
        .await?;

        assert!(empty.report.progress.wrapped);
        assert!(!empty.report.progress.backlog);
        assert_eq!(empty.report.progress.next_cursor, None);

        let ids = [fed(0x10), fed(0x11), fed(0x20), fed(0x21)];
        let invites = ids
            .iter()
            .enumerate()
            .map(|(index, id)| invite_for(*id, &format!("after-empty-{index}")))
            .collect::<Vec<_>>();
        for (id, invite) in ids.iter().zip(invites.iter()) {
            backend.with_preview(
                invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id: *id,
                    facts: good_facts(*id),
                }),
            );
        }
        let later_sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: ids
                    .iter()
                    .zip(invites.iter())
                    .map(|(id, invite)| {
                        announcement(*id, invite.clone(), DiscoverySource::Observer)
                    })
                    .collect(),
                status: SourceStatus::Ok,
            },
        })];

        let later = run_discover_pass_bounded(
            &later_sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_500,
            "0000000000000009",
            &watch,
            empty.report.progress.next_cursor,
        )
        .await?;

        assert_eq!(later.report.progress.attempted, 2);
        assert_eq!(later.report.progress.next_cursor, Some(ids[1]));
        assert!(!later.report.progress.wrapped);
        assert!(later.report.progress.backlog);
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [invites[0].to_string(), invites[1].to_string()]
        );
        assert!(backend.get_record(ids[0]).is_some());
        assert!(backend.get_record(ids[1]).is_some());
        assert!(backend.get_record(ids[2]).is_none());
        assert!(backend.get_record(ids[3]).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn zero_sized_candidate_window_does_not_advance_cursor() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let cursor = fed(0x15);
        let id = fed(0x16);
        let invite = invite_for(id, "paused-window");
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![announcement(id, invite, DiscoverySource::Observer)],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            max_candidates_per_pass: 0,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_375,
            "000000000000000a",
            &watch,
            Some(cursor),
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 0);
        assert_eq!(outcome.report.progress.next_cursor, None);
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 1);
        assert!(backend.previewed.lock().expect("previewed lock").is_empty());
        assert!(backend.get_record(id).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn source_collection_uses_the_whole_pass_deadline() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x08);
        let invite = invite_for(id, "slow-source");
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(SlowSource {
            source: DiscoverySource::Observer,
            delay: Duration::from_millis(50),
            result: SourceResult {
                candidates: vec![announcement(id, invite.clone(), DiscoverySource::Observer)],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 1,
            per_preview_timeout_ms: 100,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_500,
            "0000000000000008",
            &watch,
            None,
        )
        .await?;

        match &outcome.report.sources[0].status {
            SourceStatus::Failed(reason) => {
                assert!(reason.contains("source collection exceeded"), "{reason}")
            }
            status => panic!("expected source timeout, got {status:?}"),
        }
        assert_eq!(outcome.report.sources[0].found, 0);
        assert!(outcome.report.progress.wrapped);
        assert!(!outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.next_cursor, None);
        assert!(backend.get_record(id).is_none());
        assert!(backend.previewed.lock().expect("previewed lock").is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn slow_source_collection_does_not_starve_later_sources() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x09);
        let invite = invite_for(id, "later-source");
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![
            Box::new(SlowSource {
                source: DiscoverySource::Observer,
                delay: Duration::from_millis(100),
                result: SourceResult {
                    candidates: Vec::new(),
                    status: SourceStatus::Ok,
                },
            }),
            Box::new(FakeSource {
                source: DiscoverySource::Manual,
                result: SourceResult {
                    candidates: vec![announcement(id, invite.clone(), DiscoverySource::Manual)],
                    status: SourceStatus::Ok,
                },
            }),
        ];
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 80,
            per_preview_timeout_ms: 20,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_750,
            "0000000000000009",
            &watch,
            None,
        )
        .await?;

        match &outcome.report.sources[0].status {
            SourceStatus::Failed(reason) => {
                assert!(reason.contains("source collection exceeded"), "{reason}")
            }
            status => panic!("expected source timeout, got {status:?}"),
        }
        assert_eq!(outcome.report.sources[0].found, 0);
        assert_eq!(outcome.report.sources[1].status, SourceStatus::Ok);
        assert_eq!(outcome.report.sources[1].found, 1);
        assert_eq!(outcome.report.progress.attempted, 1);
        assert!(outcome.report.progress.wrapped);
        assert!(!outcome.report.progress.backlog);
        assert!(backend.get_record(id).is_some());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [invite.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn joined_candidate_with_unchanged_invite_skips_the_config_fetch() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x0A);
        let invite = invite_for(id, "joined");
        backend.joined.lock().expect("joined lock").insert(id);
        backend.put_existing(candidate(
            id,
            invite.clone(),
            CandidateState::AutoJoined,
            5_000,
        ));
        // No preview fixture: an unchanged invite on a joined fed must not reach the network.
        let sources = vec![boxed_source(announcement(
            id,
            invite.clone(),
            DiscoverySource::Observer,
        ))];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            10_000,
            "000000000000000a",
        )
        .await?;

        assert!(
            backend.previewed.lock().expect("previewed lock").is_empty(),
            "an unchanged invite on a joined fed must not trigger a config preview"
        );
        let unchanged = backend.get_record(id).expect("candidate unchanged");
        assert_eq!(unchanged.state, CandidateState::AutoJoined);
        assert_eq!(unchanged.invite, invite);

        // A ROTATED invite for the same joined fed DOES fetch, to adopt the newest valid one.
        let rotated = invite_for(id, "rotated");
        backend.with_preview(
            &rotated,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources = vec![boxed_source(announcement(
            id,
            rotated.clone(),
            DiscoverySource::Observer,
        ))];

        run_discover_pass(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            11_000,
            "000000000000000b",
        )
        .await?;

        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [rotated.to_string()],
            "a rotated invite must be authenticated before adoption"
        );
        let refreshed = backend.get_record(id).expect("candidate refreshed");
        assert_eq!(refreshed.state, CandidateState::AutoJoined);
        assert_eq!(refreshed.invite, rotated);
        Ok(())
    }

    #[tokio::test]
    async fn joined_fed_with_stale_discovered_row_is_not_auto_joined() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x0C);
        let invite = invite_for(id, "stale-discovered");
        backend.joined.lock().expect("joined lock").insert(id);
        backend.put_existing(candidate(
            id,
            invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Manual,
            result: SourceResult {
                candidates: Vec::new(),
                status: SourceStatus::Ok,
            },
        })];

        let report =
            run_discover_pass(&sources, &policy, &backend, 20_000, "000000000000000c").await?;

        assert!(
            backend.joins.lock().expect("joins lock").is_empty(),
            "a fed already in the joined registry must not be auto-joined as an Agent"
        );
        assert_eq!(report.auto_join.considered, 0);
        assert_eq!(report.auto_join.joined, 0);
        assert_eq!(
            backend.get_record(id).expect("candidate present").state,
            CandidateState::Discovered,
            "the stale Discovered row is left as-is, not flipped to AutoJoined"
        );
        Ok(())
    }

    #[tokio::test]
    async fn joined_discovered_row_with_agent_join_history_recovers_to_auto_joined(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x0D);
        let invite = invite_for(id, "partial-agent-join");
        backend.joined.lock().expect("joined lock").insert(id);
        backend
            .agent_created
            .lock()
            .expect("agent-created lock")
            .insert(id);
        backend.put_existing(candidate(
            id,
            invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();

        let report =
            run_discover_pass(&sources, &policy, &backend, 20_000, "000000000000000d").await?;

        assert!(
            backend.previewed.lock().expect("previewed lock").is_empty(),
            "recovery is ledger-based and must not require a fresh config fetch"
        );
        assert!(
            backend.joins.lock().expect("joins lock").is_empty(),
            "a recovered existing partition must not be joined again"
        );
        assert_eq!(report.auto_join.considered, 0);
        let recovered = backend.get_record(id).expect("candidate recovered");
        assert_eq!(recovered.state, CandidateState::AutoJoined);
        assert_eq!(recovered.structural, StructuralOutcome::Passed);
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cap_defers_overflow_without_dropping_it() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let ids = [fed(0x51), fed(0x52), fed(0x53), fed(0x54)];
        let invites = ids
            .iter()
            .enumerate()
            .map(|(i, id)| invite_for(*id, &format!("cap-{i}")))
            .collect::<Vec<_>>();
        for (id, invite) in ids.iter().zip(invites.iter()) {
            backend.with_preview(
                invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id: *id,
                    facts: good_facts(*id),
                }),
            );
        }
        let source = Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: ids
                    .iter()
                    .zip(invites.iter())
                    .map(|(id, invite)| {
                        announcement(*id, invite.clone(), DiscoverySource::Observer)
                    })
                    .collect(),
                status: SourceStatus::Ok,
            },
        });
        let sources: Vec<Box<dyn CandidateSource>> = vec![source];
        let watch = WatchPolicy {
            max_candidates_per_pass: 2,
            ..WatchPolicy::default()
        };

        let first = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            20_000,
            "0000000000000051",
            &watch,
            None,
        )
        .await?;

        assert_eq!(first.report.progress.attempted, 2);
        assert_eq!(first.report.progress.next_cursor, Some(ids[1]));
        assert!(first.report.progress.backlog);
        assert_eq!(first.report.progress.deferred, 2);
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [invites[0].to_string(), invites[1].to_string()]
        );
        assert!(backend.get_record(ids[0]).is_some());
        assert!(backend.get_record(ids[1]).is_some());
        assert!(backend.get_record(ids[2]).is_none());
        assert!(backend.get_record(ids[3]).is_none());

        let second = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            21_000,
            "0000000000000052",
            &watch,
            first.report.progress.next_cursor,
        )
        .await?;

        assert_eq!(second.report.progress.attempted, 2);
        assert_eq!(second.report.progress.next_cursor, Some(ids[3]));
        assert!(second.report.progress.wrapped);
        assert!(!second.report.progress.backlog);
        assert_eq!(second.report.progress.deferred, 0);
        assert!(backend.get_record(ids[2]).is_some());
        assert!(backend.get_record(ids[3]).is_some());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [
                invites[0].to_string(),
                invites[1].to_string(),
                invites[2].to_string(),
                invites[3].to_string(),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn rotation_snapshot_keeps_fresh_ids_behind_deferred_source_ids() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let original_ids = [fed(0x51), fed(0x60), fed(0x70)];
        let original_invites = original_ids
            .iter()
            .enumerate()
            .map(|(i, id)| invite_for(*id, &format!("original-rotation-{i}")))
            .collect::<Vec<_>>();
        for (id, invite) in original_ids.iter().zip(original_invites.iter()) {
            backend.with_preview(
                invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id: *id,
                    facts: good_facts(*id),
                }),
            );
        }
        let watch = WatchPolicy {
            max_candidates_per_pass: 1,
            ..WatchPolicy::default()
        };
        let first_sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: original_ids
                    .iter()
                    .zip(original_invites.iter())
                    .map(|(id, invite)| {
                        announcement(*id, invite.clone(), DiscoverySource::Observer)
                    })
                    .collect(),
                status: SourceStatus::Ok,
            },
        })];

        let first = run_discover_pass_bounded(
            &first_sources,
            &DiscoveryPolicy::default(),
            &backend,
            25_000,
            "0000000000000057",
            &watch,
            None,
        )
        .await?;

        assert_eq!(first.report.progress.next_cursor, Some(original_ids[0]));
        assert!(first.report.progress.backlog);
        assert_eq!(first.next_rotation, original_ids.to_vec());

        let fresh_ids = [fed(0x52), fed(0x53)];
        let fresh_invites = fresh_ids
            .iter()
            .enumerate()
            .map(|(i, id)| invite_for(*id, &format!("fresh-rotation-{i}")))
            .collect::<Vec<_>>();
        for (id, invite) in fresh_ids.iter().zip(fresh_invites.iter()) {
            backend.with_preview(
                invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id: *id,
                    facts: good_facts(*id),
                }),
            );
        }
        let second_sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: original_ids
                    .iter()
                    .zip(original_invites.iter())
                    .chain(fresh_ids.iter().zip(fresh_invites.iter()))
                    .map(|(id, invite)| {
                        announcement(*id, invite.clone(), DiscoverySource::Observer)
                    })
                    .collect(),
                status: SourceStatus::Ok,
            },
        })];
        let second_watch = WatchPolicy {
            max_candidates_per_pass: 2,
            ..WatchPolicy::default()
        };

        let second = run_discover_pass_bounded_with_rotation(
            &second_sources,
            &DiscoveryPolicy::default(),
            &backend,
            26_000,
            "0000000000000058",
            &second_watch,
            DiscoverPassResume {
                cursor: first.report.progress.next_cursor,
                rotation: &first.next_rotation,
                occurrence: Occurrence(0),
            },
        )
        .await?;

        assert_eq!(second.report.progress.next_cursor, Some(original_ids[2]));
        assert!(backend.get_record(original_ids[1]).is_some());
        assert!(backend.get_record(original_ids[2]).is_some());
        assert!(backend.get_record(fresh_ids[0]).is_none());
        assert!(backend.get_record(fresh_ids[1]).is_none());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [
                original_invites[0].to_string(),
                original_invites[1].to_string(),
                original_invites[2].to_string(),
            ]
        );
        assert_eq!(
            second.next_rotation,
            vec![
                original_ids[0],
                original_ids[1],
                original_ids[2],
                fresh_ids[0],
                fresh_ids[1],
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cap_keeps_backlog_when_cursor_at_max_starts_partial_window(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let ids = [fed(0x55), fed(0x56), fed(0x57), fed(0x58)];
        let invites = ids
            .iter()
            .enumerate()
            .map(|(i, id)| invite_for(*id, &format!("cap-at-max-{i}")))
            .collect::<Vec<_>>();
        for (id, invite) in ids.iter().zip(invites.iter()) {
            backend.with_preview(
                invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id: *id,
                    facts: good_facts(*id),
                }),
            );
        }
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: ids
                    .iter()
                    .zip(invites.iter())
                    .map(|(id, invite)| {
                        announcement(*id, invite.clone(), DiscoverySource::Observer)
                    })
                    .collect(),
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            max_candidates_per_pass: 2,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            23_000,
            "0000000000000055",
            &watch,
            Some(ids[3]),
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 2);
        assert_eq!(outcome.report.progress.next_cursor, Some(ids[1]));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 2);
        assert!(backend.get_record(ids[0]).is_some());
        assert!(backend.get_record(ids[1]).is_some());
        assert!(backend.get_record(ids[2]).is_none());
        assert!(backend.get_record(ids[3]).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cap_keeps_backlog_when_stale_cursor_reaches_end_without_wrap(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let ids = [fed(0x50), fed(0x51), fed(0x53), fed(0x54)];
        let invites = ids
            .iter()
            .enumerate()
            .map(|(i, id)| invite_for(*id, &format!("cap-stale-cursor-{i}")))
            .collect::<Vec<_>>();
        for (id, invite) in ids.iter().zip(invites.iter()) {
            backend.with_preview(
                invite,
                PreviewReply::Ok(PreviewedCandidate {
                    id: *id,
                    facts: good_facts(*id),
                }),
            );
        }
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: ids
                    .iter()
                    .zip(invites.iter())
                    .map(|(id, invite)| {
                        announcement(*id, invite.clone(), DiscoverySource::Observer)
                    })
                    .collect(),
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            max_candidates_per_pass: 2,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            24_000,
            "0000000000000056",
            &watch,
            Some(fed(0x52)),
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 2);
        assert_eq!(outcome.report.progress.next_cursor, Some(ids[3]));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 2);
        assert!(backend.get_record(ids[0]).is_none());
        assert!(backend.get_record(ids[1]).is_none());
        assert!(backend.get_record(ids[2]).is_some());
        assert!(backend.get_record(ids[3]).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cap_defers_auto_join_overflow_without_dropping_it() -> anyhow::Result<()> {
        let backend = FakeBackend::default();

        let source_id = fed(0x91);
        let source_invite = invite_for(source_id, "shared-cap-source");
        let mut rejected_facts = good_facts(source_id);
        rejected_facts.has_lnv2 = false;
        backend.with_preview(
            &source_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: source_id,
                facts: rejected_facts,
            }),
        );

        let later_auto_join = fed(0x92);
        let later_invite = invite_for(later_auto_join, "shared-cap-auto-join");
        backend.put_existing(candidate(
            later_auto_join,
            later_invite.clone(),
            CandidateState::Discovered,
            1_000,
        ));
        backend.with_preview(
            &later_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: later_auto_join,
                facts: good_facts(later_auto_join),
            }),
        );

        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![announcement(
                    source_id,
                    source_invite.clone(),
                    DiscoverySource::Observer,
                )],
                status: SourceStatus::Ok,
            },
        })];
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let watch = WatchPolicy {
            max_candidates_per_pass: 1,
            ..WatchPolicy::default()
        };

        let first = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            50_000,
            "0000000000000091",
            &watch,
            None,
        )
        .await?;

        assert_eq!(first.report.progress.attempted, 1);
        assert_eq!(first.report.progress.next_cursor, Some(source_id));
        assert!(first.report.progress.backlog);
        assert_eq!(first.report.progress.deferred, 1);
        assert_eq!(first.report.auto_join.considered, 0);
        assert!(backend.joins.lock().expect("joins lock").is_empty());
        assert_eq!(
            backend.get_record(source_id).expect("source record").state,
            CandidateState::Rejected
        );
        assert_eq!(
            backend.get_record(later_auto_join).expect("auto row").state,
            CandidateState::Discovered
        );

        let second = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            51_000,
            "0000000000000092",
            &watch,
            first.report.progress.next_cursor,
        )
        .await?;

        assert_eq!(second.report.progress.attempted, 1);
        assert_eq!(second.report.progress.next_cursor, Some(later_auto_join));
        assert!(second.report.progress.wrapped);
        assert!(!second.report.progress.backlog);
        assert_eq!(second.report.progress.deferred, 0);
        assert_eq!(second.report.auto_join.considered, 1);
        assert_eq!(second.report.auto_join.joined, 1);
        assert_eq!(
            backend.joins.lock().expect("joins lock").as_slice(),
            [later_auto_join]
        );
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [source_invite.to_string(), later_invite.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn per_preview_timeout_is_transient_and_advances_cursor() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x61);
        let invite = invite_for(id, "timeout");
        backend.with_preview(&invite, PreviewReply::Pending);
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![announcement(id, invite.clone(), DiscoverySource::Observer)],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            per_preview_timeout_ms: 1,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            30_000,
            "0000000000000061",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 1);
        assert_eq!(outcome.report.progress.next_cursor, Some(id));
        assert!(outcome.report.progress.wrapped);
        assert!(!outcome.report.progress.backlog);
        assert!(backend.get_record(id).is_none());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [invite.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn whole_pass_deadline_stops_early_and_signals_backlog() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let slow = fed(0x71);
        let deferred = fed(0x72);
        let slow_invite = invite_for(slow, "slow");
        let deferred_invite = invite_for(deferred, "deferred");
        backend.with_preview(
            &slow_invite,
            PreviewReply::SleepThenOk(
                Duration::from_millis(100),
                PreviewedCandidate {
                    id: slow,
                    facts: good_facts(slow),
                },
            ),
        );
        backend.with_preview(
            &deferred_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: deferred,
                facts: good_facts(deferred),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![
                    announcement(slow, slow_invite.clone(), DiscoverySource::Observer),
                    announcement(deferred, deferred_invite.clone(), DiscoverySource::Observer),
                ],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 50,
            per_preview_timeout_ms: 100,
            max_candidates_per_pass: 10,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            40_000,
            "0000000000000071",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 1);
        assert_eq!(outcome.report.progress.next_cursor, Some(slow));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 1);
        assert!(backend.get_record(slow).is_none());
        assert!(backend.get_record(deferred).is_none());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [slow_invite.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn deadline_before_preview_does_not_advance_source_cursor() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        backend.delay_get_candidate(Duration::from_millis(50));
        let id = fed(0x70);
        let invite = invite_for(id, "delayed-row");
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![announcement(id, invite, DiscoverySource::Observer)],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 10,
            per_preview_timeout_ms: 100,
            max_candidates_per_pass: 10,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            40_250,
            "0000000000000070",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 0);
        assert_eq!(outcome.report.progress.next_cursor, None);
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 1);
        assert!(backend.previewed.lock().expect("previewed lock").is_empty());
        assert!(backend.get_record(id).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn whole_pass_deadline_caps_alternate_invites_for_one_candidate() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x73);
        let slow_invite = invite_for(id, "slow-alt");
        let valid_invite = invite_for(id, "valid-alt");
        backend.with_preview(&slow_invite, PreviewReply::Pending);
        backend.with_preview(
            &valid_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![
                    announcement(id, slow_invite.clone(), DiscoverySource::Observer),
                    announcement(id, valid_invite.clone(), DiscoverySource::Observer),
                ],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 50,
            per_preview_timeout_ms: 100,
            max_candidates_per_pass: 10,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            40_500,
            "0000000000000073",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 1);
        assert_eq!(outcome.report.progress.next_cursor, Some(id));
        assert!(outcome.report.progress.wrapped);
        assert!(!outcome.report.progress.backlog);
        assert!(backend.get_record(id).is_none());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [slow_invite.to_string()],
            "the second invite must not get a fresh per-preview timeout after the pass deadline"
        );
        Ok(())
    }

    #[tokio::test]
    async fn auto_join_deadline_sets_backlog_even_after_source_window_wraps() -> anyhow::Result<()>
    {
        let backend = FakeBackend::default();
        let slow = fed(0x78);
        let deferred = fed(0x79);
        let slow_invite = invite_for(slow, "slow-auto-join");
        let deferred_invite = invite_for(deferred, "deferred-auto-join");
        backend.put_existing(candidate(
            slow,
            slow_invite.clone(),
            CandidateState::Discovered,
            1_000,
        ));
        backend.put_existing(candidate(
            deferred,
            deferred_invite.clone(),
            CandidateState::Discovered,
            1_000,
        ));
        backend.with_preview(
            &slow_invite,
            PreviewReply::SleepThenOk(
                Duration::from_millis(50),
                PreviewedCandidate {
                    id: slow,
                    facts: good_facts(slow),
                },
            ),
        );
        backend.with_preview(
            &deferred_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: deferred,
                facts: good_facts(deferred),
            }),
        );
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 20,
            per_preview_timeout_ms: 100,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            40_750,
            "0000000000000078",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.next_cursor, Some(slow));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 1);
        assert_eq!(outcome.report.auto_join.joined, 0);
        assert!(backend.joins.lock().expect("joins lock").is_empty());
        let previewed = backend.previewed.lock().expect("previewed lock").clone();
        assert!(previewed.len() <= 1);
        assert!(!previewed.contains(&deferred_invite.to_string()));

        let continuation = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            40_800,
            "0000000000000079",
            &watch,
            outcome.report.progress.next_cursor,
        )
        .await?;

        assert_eq!(
            backend.joins.lock().expect("joins lock").first().copied(),
            Some(deferred)
        );
        assert_eq!(continuation.report.auto_join.joined, 1);
        Ok(())
    }

    #[tokio::test]
    async fn auto_join_deadline_does_not_advance_cursor_past_deferred_auto_join(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let slow = fed(0x74);
        let deferred = fed(0x75);
        let slow_invite = invite_for(slow, "slow-source-auto-join");
        let deferred_invite = invite_for(deferred, "deferred-source-auto-join");
        backend.put_existing(candidate(
            slow,
            slow_invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.put_existing(candidate(
            deferred,
            deferred_invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.with_preview(
            &slow_invite,
            PreviewReply::SleepThenOk(
                Duration::from_millis(50),
                PreviewedCandidate {
                    id: slow,
                    facts: good_facts(slow),
                },
            ),
        );
        backend.with_preview(
            &deferred_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: deferred,
                facts: good_facts(deferred),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![
                    announcement(slow, slow_invite.clone(), DiscoverySource::Observer),
                    announcement(deferred, deferred_invite.clone(), DiscoverySource::Observer),
                ],
                status: SourceStatus::Ok,
            },
        })];
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 20,
            per_preview_timeout_ms: 100,
            max_candidates_per_pass: 10,
            ..WatchPolicy::default()
        };

        let first = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            40_850,
            "0000000000000074",
            &watch,
            None,
        )
        .await?;

        assert_eq!(first.report.progress.attempted, 1);
        assert_eq!(first.report.progress.next_cursor, Some(slow));
        assert!(!first.report.progress.wrapped);
        assert!(first.report.progress.backlog);
        assert_eq!(first.report.progress.deferred, 1);
        assert!(backend.joins.lock().expect("joins lock").is_empty());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [slow_invite.to_string()]
        );

        let second = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            40_900,
            "0000000000000075",
            &watch,
            first.report.progress.next_cursor,
        )
        .await?;

        assert_eq!(
            backend.joins.lock().expect("joins lock").first().copied(),
            Some(deferred)
        );
        assert_eq!(second.report.auto_join.joined, 1);
        Ok(())
    }

    #[tokio::test]
    async fn auto_join_deadline_stops_cursor_before_deferred_gap_despite_later_attempt(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let first_auto_join = fed(0x84);
        let deferred_auto_join = fed(0x85);
        let later_rejected = fed(0x86);
        let first_invite = invite_for(first_auto_join, "first-gap-auto-join");
        let deferred_invite = invite_for(deferred_auto_join, "deferred-gap-auto-join");
        let rejected_invite = invite_for(later_rejected, "later-gap-rejected");
        backend.put_existing(candidate(
            first_auto_join,
            first_invite.clone(),
            CandidateState::Discovered,
            10_000,
        ));
        backend.put_existing(candidate(
            deferred_auto_join,
            deferred_invite,
            CandidateState::Discovered,
            10_000,
        ));
        backend.with_preview(
            &first_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: first_auto_join,
                facts: good_facts(first_auto_join),
            }),
        );
        backend.with_join(first_auto_join, JoinReply::Pending);
        let mut rejected_facts = good_facts(later_rejected);
        rejected_facts.has_lnv2 = false;
        backend.with_preview(
            &rejected_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: later_rejected,
                facts: rejected_facts,
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![announcement(
                    later_rejected,
                    rejected_invite.clone(),
                    DiscoverySource::Observer,
                )],
                status: SourceStatus::Ok,
            },
        })];
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 20,
            per_preview_timeout_ms: 100,
            max_candidates_per_pass: 10,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            41_000,
            "0000000000000084",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 2);
        assert_eq!(outcome.report.progress.next_cursor, Some(first_auto_join));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 1);
        assert_eq!(
            backend
                .get_record(later_rejected)
                .expect("rejected row")
                .state,
            CandidateState::Rejected
        );
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [rejected_invite.to_string(), first_invite.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn auto_join_attempt_uses_remaining_pass_deadline() -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let id = fed(0x7A);
        let invite = invite_for(id, "slow-join");
        backend.put_existing(candidate(
            id,
            invite.clone(),
            CandidateState::Discovered,
            1_000,
        ));
        backend.with_preview(
            &invite,
            PreviewReply::Ok(PreviewedCandidate {
                id,
                facts: good_facts(id),
            }),
        );
        backend.with_join(id, JoinReply::Pending);
        let policy = DiscoveryPolicy {
            auto_join: true,
            ..DiscoveryPolicy::default()
        };
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 50,
            per_preview_timeout_ms: 100,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &policy,
            &backend,
            40_900,
            "000000000000007a",
            &watch,
            None,
        )
        .await?;

        assert_eq!(outcome.report.progress.next_cursor, Some(id));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.auto_join.considered, 1);
        assert_eq!(outcome.report.auto_join.joined, 0);
        assert_eq!(backend.joins.lock().expect("joins lock").as_slice(), [id]);
        assert_eq!(
            backend.get_record(id).expect("candidate remains").state,
            CandidateState::Discovered
        );
        Ok(())
    }

    #[tokio::test]
    async fn deadline_after_max_id_does_not_clear_backlog_before_window_completes(
    ) -> anyhow::Result<()> {
        let backend = FakeBackend::default();
        let a = fed(0x81);
        let b = fed(0x82);
        let c = fed(0x83);
        let a_invite = invite_for(a, "wrapped-a");
        let b_invite = invite_for(b, "wrapped-b");
        let c_invite = invite_for(c, "wrapped-c");
        backend.with_preview(
            &c_invite,
            PreviewReply::SleepThenOk(
                Duration::from_millis(100),
                PreviewedCandidate {
                    id: c,
                    facts: good_facts(c),
                },
            ),
        );
        backend.with_preview(
            &a_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: a,
                facts: good_facts(a),
            }),
        );
        backend.with_preview(
            &b_invite,
            PreviewReply::Ok(PreviewedCandidate {
                id: b,
                facts: good_facts(b),
            }),
        );
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(FakeSource {
            source: DiscoverySource::Observer,
            result: SourceResult {
                candidates: vec![
                    announcement(a, a_invite.clone(), DiscoverySource::Observer),
                    announcement(b, b_invite.clone(), DiscoverySource::Observer),
                    announcement(c, c_invite.clone(), DiscoverySource::Observer),
                ],
                status: SourceStatus::Ok,
            },
        })];
        let watch = WatchPolicy {
            discover_pass_deadline_ms: 50,
            per_preview_timeout_ms: 100,
            max_candidates_per_pass: 10,
            ..WatchPolicy::default()
        };

        let outcome = run_discover_pass_bounded(
            &sources,
            &DiscoveryPolicy::default(),
            &backend,
            41_000,
            "0000000000000081",
            &watch,
            Some(b),
        )
        .await?;

        assert_eq!(outcome.report.progress.attempted, 1);
        assert_eq!(outcome.report.progress.next_cursor, Some(c));
        assert!(!outcome.report.progress.wrapped);
        assert!(outcome.report.progress.backlog);
        assert_eq!(outcome.report.progress.deferred, 2);
        assert!(backend.get_record(c).is_none());
        assert!(backend.get_record(a).is_none());
        assert!(backend.get_record(b).is_none());
        assert_eq!(
            backend.previewed.lock().expect("previewed lock").as_slice(),
            [c_invite.to_string()]
        );
        Ok(())
    }
}
