//! Registered driver wrapper and actor-routed journal adapter.

use super::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::AbortHandle;
use wallet_core::{Action, ExecutionSummary, Journal};

pub(super) type Registry = Arc<Mutex<HashMap<IdempotencyKey, DriverEntry>>>;

#[derive(Clone, Debug)]
pub(super) enum DriverKind {
    Intent {
        external_admission: bool,
        probe_session_nonce: Option<String>,
    },
    Awaiter {
        external_admission: bool,
    },
    Probe {
        candidate: FederationId,
    },
}

pub(super) struct DriverEntry {
    pub(super) abort: AbortHandle,
    pub(super) generation: u64,
    pub(super) kind: DriverKind,
    redrive_requested: bool,
}

pub(super) struct FinishedDriver {
    pub(super) kind: DriverKind,
    pub(super) redrive_requested: bool,
}

struct RegistryGuard {
    registry: Registry,
    key: IdempotencyKey,
    generation: u64,
    armed: Arc<AtomicBool>,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        let mut registry = registry_lock(&self.registry);
        if registry
            .get(&self.key)
            .is_some_and(|entry| entry.generation == self.generation)
        {
            registry.remove(&self.key);
        }
    }
}

pub(super) fn contains(registry: &Registry, key: &IdempotencyKey) -> bool {
    registry_lock(registry).contains_key(key)
}

pub(super) fn len(registry: &Registry) -> usize {
    registry_lock(registry).len()
}

pub(super) fn external_len(registry: &Registry) -> usize {
    registry_lock(registry)
        .values()
        .filter(|entry| {
            matches!(
                entry.kind,
                DriverKind::Intent {
                    external_admission: true,
                    ..
                } | DriverKind::Awaiter {
                    external_admission: true,
                }
            )
        })
        .count()
}

pub(super) fn request_redrive(registry: &Registry, key: &IdempotencyKey) {
    if let Some(entry) = registry_lock(registry).get_mut(key) {
        entry.redrive_requested = true;
    }
}

pub(super) fn finish(
    registry: &Registry,
    key: &IdempotencyKey,
    generation: u64,
) -> Option<FinishedDriver> {
    let mut registry = registry_lock(registry);
    if registry
        .get(key)
        .is_none_or(|entry| entry.generation != generation)
    {
        return None;
    }
    registry.remove(key).map(|entry| FinishedDriver {
        kind: entry.kind,
        redrive_requested: entry.redrive_requested,
    })
}

pub(super) fn aborts(registry: &Registry) -> Vec<AbortHandle> {
    registry_lock(registry)
        .values()
        .map(|entry| entry.abort.clone())
        .collect()
}

pub(super) fn abort_probe_session(
    registry: &Registry,
    candidate: FederationId,
    session_nonce: &str,
) {
    let aborts: Vec<_> = registry_lock(registry)
        .values()
        .filter_map(|entry| match entry.kind {
            DriverKind::Probe { candidate: fed, .. } if fed == candidate => {
                Some(entry.abort.clone())
            }
            DriverKind::Intent {
                probe_session_nonce: Some(ref nonce),
                ..
            } if nonce == session_nonce => Some(entry.abort.clone()),
            DriverKind::Intent { .. } | DriverKind::Awaiter { .. } | DriverKind::Probe { .. } => {
                None
            }
        })
        .collect();
    for abort in aborts {
        abort.abort();
    }
}

pub(super) fn owns_intent(registry: &Registry, intent: &Intent) -> bool {
    contains(registry, &intent.idempotency_key)
}

pub(super) fn spawn_registered<F>(
    registry: &Registry,
    key: IdempotencyKey,
    generation: u64,
    kind: DriverKind,
    future: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    let armed = Arc::new(AtomicBool::new(false));
    let guard = RegistryGuard {
        registry: registry.clone(),
        key: key.clone(),
        generation,
        armed: armed.clone(),
    };
    let (start, started) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _guard = guard;
        if started.await.is_ok() {
            future.await;
        }
    });
    registry_lock(registry).insert(
        key,
        DriverEntry {
            abort: task.abort_handle(),
            generation,
            kind,
            redrive_requested: false,
        },
    );
    armed.store(true, Ordering::Release);
    let _ = start.send(());
}

struct ActorJournal {
    durable: Arc<FedimintJournal>,
    client: WalletClient,
}

#[async_trait]
impl Journal for ActorJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        self.client
            .journal_transition(
                intent.idempotency_key.clone(),
                JournalTransition::Upsert(Box::new(intent.clone())),
            )
            .await
            .map(|_| ())
            .map_err(service_exec_error)
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        self.durable.get(key).await
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        self.client
            .journal_transition(
                key.clone(),
                JournalTransition::SetStatus {
                    status,
                    error: error.map(str::to_owned),
                },
            )
            .await
            .map(|_| ())
            .map_err(service_exec_error)
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        match self
            .client
            .journal_transition(
                key.clone(),
                JournalTransition::CompareAndSet { expected, new },
            )
            .await
            .map_err(service_exec_error)?
        {
            TransitionResult::Compared(changed) => Ok(changed),
            TransitionResult::Applied => Err(ExecError::Permanent(
                "actor returned the wrong transition result".to_owned(),
            )),
        }
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        self.durable.pending().await
    }

    async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        self.durable.awaiting().await
    }

    async fn reservation_intents(&self) -> Result<Vec<Intent>, ExecError> {
        self.durable.reservation_intents().await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.durable.failed().await
    }

    async fn move_record(
        &self,
        key: &IdempotencyKey,
    ) -> Result<Option<wallet_core::MoveRecord>, ExecError> {
        self.durable.move_record(key).await
    }

    async fn set_operation_artifact(
        &self,
        key: &IdempotencyKey,
        operation_id: OperationId,
        invoice: Option<&Invoice>,
    ) -> Result<(), ExecError> {
        self.client
            .journal_transition(
                key.clone(),
                JournalTransition::OperationArtifact {
                    operation_id,
                    invoice: invoice.cloned(),
                },
            )
            .await
            .map(|_| ())
            .map_err(service_exec_error)
    }

    fn store_id(&self) -> usize {
        self.durable.store_id()
    }
}

fn service_exec_error(error: ServiceError) -> ExecError {
    ExecError::Permanent(format!("wallet service transition failed: {error}"))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_intent(
    registry: &Registry,
    generation: u64,
    intent: Intent,
    journal: Arc<FedimintJournal>,
    executor: Arc<dyn Executor>,
    client: WalletClient,
    perform_timeout: Option<std::time::Duration>,
    external_admission: bool,
    probe_session_nonce: Option<String>,
) {
    let key = intent.idempotency_key.clone();
    let finished_key = key.clone();
    let transition_client = client.clone();
    let future = async move {
        let actor_journal = ActorJournal {
            durable: journal,
            client,
        };
        let mut summary = ExecutionSummary::default();
        let drive = wallet_core::drive_intent_step(
            &actor_journal,
            executor.as_ref(),
            &intent,
            &mut summary,
        );
        let perform_timeout = if matches!(&intent.action, Action::Join { .. }) {
            None
        } else {
            perform_timeout
        };
        match perform_timeout {
            Some(deadline) => {
                let _ = tokio::time::timeout(deadline, drive).await;
            }
            None => {
                let _ = drive.await;
            }
        }
        let _ = transition_client
            .journal_transition(
                finished_key,
                JournalTransition::DriverFinished { generation },
            )
            .await;
    };
    spawn_registered(
        registry,
        key,
        generation,
        DriverKind::Intent {
            external_admission,
            probe_session_nonce,
        },
        future,
    );
}

pub(super) fn spawn_awaiter(
    registry: &Registry,
    generation: u64,
    intent: Intent,
    runtime: Option<Arc<Runtime>>,
    client: WalletClient,
    external_admission: bool,
) {
    let key = intent.idempotency_key.clone();
    let task_key = key.clone();
    let future = async move {
        if let Some(runtime) = runtime {
            let _ = runtime.service_await_intent(&intent).await;
            let _ = client
                .journal_transition(task_key, JournalTransition::DriverFinished { generation })
                .await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    spawn_registered(
        registry,
        key,
        generation,
        DriverKind::Awaiter { external_admission },
        future,
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_probe(
    registry: &Registry,
    generation: u64,
    decision: ProbeDecision,
    actor: Actor,
    policy: wallet_core::ProbePolicy,
    per_fed_cap: wallet_core::Msat,
    runtime: Option<Arc<Runtime>>,
    client: WalletClient,
) {
    let key = decision.key.clone();
    let candidate = decision.candidate;
    let source = decision.session.from;
    let refresh_key = key.clone();
    let finished_key = key.clone();
    let future = async move {
        if let Some(runtime) = runtime {
            let _ = runtime
                .service_active_probe(
                    candidate,
                    source,
                    &policy,
                    actor,
                    per_fed_cap,
                    client.clone(),
                )
                .await;
            let _ = client
                .journal_transition(refresh_key, JournalTransition::Refresh)
                .await;
            let _ = client
                .journal_transition(
                    finished_key,
                    JournalTransition::DriverFinished { generation },
                )
                .await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    spawn_registered(
        registry,
        key,
        generation,
        DriverKind::Probe { candidate },
        future,
    );
}
