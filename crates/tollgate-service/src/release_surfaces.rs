//! Release surfaces (staged-release-design.md section 11): the release status that `tg status`,
//! `tg release status`, and the desktop Release panel report, `tg release retry`, and the target
//! resolution behind `tg wait --released`.
//!
//! `tg release retry` reruns the newest `staging` tip as a `Release`-kind run (section 8.5). It
//! covers what the automatic trigger never requeues: a tip whose latest run failed or was
//! canceled, and a tip that `release` already holds without a passing run under the active
//! release-stage digest (after a release-stage configuration change or an uncertified
//! adoption), which `tg push` refuses to publish. The run and the command result commit in one
//! transaction, so a replay of the command after a crash returns the run it queued.

use super::*;
use tollgate_store::ReleaseTriggerCommand;

/// The most release runs `tg wait --released` inspects for one covering its target.
const RELEASE_WAIT_SCAN_LIMIT: usize = 64;

/// One release run as the release surfaces report it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRunSummary {
    pub item_id: QueueItemId,
    pub state: QueueItemState,
    pub terminal_reason: Option<String>,
    /// The exact `staging` OID the run tests.
    pub target_oid: GitOid,
    /// The first-parent range whose changed paths select the run's steps.
    pub range: Option<ReleaseRunRange>,
    /// Voting steps that failed or were selected and skipped; empty unless the run failed.
    pub failing_steps: Vec<String>,
    pub steps: Vec<ReleaseRunStep>,
    /// The earlier run on the same target that `tg release retry` reran.
    pub retry_of_item_id: Option<QueueItemId>,
    pub buildset_id: Option<BuildsetId>,
    pub elapsed_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRunRange {
    pub from: GitOid,
    pub to: GitOid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRunStep {
    pub name: String,
    pub result_class: String,
    pub elapsed_ms: u64,
}

impl ReleaseRunSummary {
    pub fn from_view(view: &QueueItemView) -> Self {
        let steps = view
            .buildset
            .as_ref()
            .map(|buildset| {
                buildset
                    .step_results
                    .iter()
                    .map(|result| ReleaseRunStep {
                        name: result.name.clone(),
                        result_class: result.result_class.clone(),
                        elapsed_ms: result.elapsed_ms,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let failed = matches!(
            view.item.state,
            QueueItemState::CheckFailed | QueueItemState::InfrastructureExhausted
        );
        let failing_steps = if !failed {
            Vec::new()
        } else if let Some(attribution) = view
            .failure_attribution
            .as_ref()
            .filter(|attribution| !attribution.steps.is_empty())
        {
            attribution
                .steps
                .iter()
                .map(|step| step.name.clone())
                .collect()
        } else {
            view.buildset
                .as_ref()
                .map(|buildset| {
                    buildset
                        .frozen_steps
                        .iter()
                        .filter(|step| step.voting)
                        .filter(|step| {
                            buildset
                                .step_results
                                .iter()
                                .find(|result| result.name == step.name)
                                .is_some_and(|result| {
                                    !matches!(result.result_class.as_str(), "success" | "skipped")
                                })
                        })
                        .map(|step| step.name.clone())
                        .collect()
                })
                .unwrap_or_default()
        };
        Self {
            item_id: view.item.id,
            state: view.item.state,
            terminal_reason: view.item.terminal_reason.clone(),
            target_oid: view.item.source_oid.clone(),
            range: view.generation.as_ref().map(|generation| ReleaseRunRange {
                from: generation.anchored_base_oid.clone(),
                to: generation.tested_oid.clone(),
            }),
            failing_steps,
            steps,
            retry_of_item_id: view.item.retry_of_item_id,
            buildset_id: view.buildset.as_ref().map(|buildset| buildset.id),
            elapsed_ms: view.elapsed_ms,
        }
    }
}

/// A Tollgate ref and the OID it holds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRefView {
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub oid: GitOid,
}

/// The release status of one repository (`tg release status --json`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseStatus {
    pub repository_id: RepositoryId,
    pub staging: ReleaseRefView,
    pub release: ReleaseRefView,
    pub lag: ReleaseLag,
    pub release_state: ReleaseState,
    /// Whether the active configuration has release-stage steps.
    pub release_stage: bool,
    pub release_block_reasons: Vec<BlockReason>,
    pub promotion_pause: Option<BlockReason>,
    /// The oldest unfinished release run: the one running or next to run.
    pub active_run: Option<ReleaseRunSummary>,
    /// The newest finished release run that was not superseded before it ran.
    pub last_run: Option<ReleaseRunSummary>,
    /// Unfinished runs newest first, then the most recent finished ones.
    pub runs: Vec<ReleaseRunSummary>,
}

/// The release status a repository snapshot describes.
pub fn release_status(repository: &RepositorySnapshot) -> ReleaseStatus {
    let runs = repository
        .release_runs
        .iter()
        .map(ReleaseRunSummary::from_view)
        .collect::<Vec<_>>();
    let active_run = repository
        .release_runs
        .iter()
        .filter(|view| !view.item.state.is_terminal())
        .min_by_key(|view| view.item.enqueue_sequence)
        .map(ReleaseRunSummary::from_view);
    let last_run = repository
        .release_runs
        .iter()
        .filter(|view| {
            view.item.state.is_terminal() && view.item.state != QueueItemState::Superseded
        })
        .max_by_key(|view| view.item.enqueue_sequence)
        .map(ReleaseRunSummary::from_view);
    let state = &repository.state;
    ReleaseStatus {
        repository_id: state.id,
        staging: ReleaseRefView {
            ref_name: state.staging_ref.clone(),
            oid: state.staging_oid.clone(),
        },
        release: ReleaseRefView {
            ref_name: state.release_ref.clone(),
            oid: state.release_oid.clone(),
        },
        lag: state.release_lag.clone(),
        release_state: state.release_state,
        release_stage: repository
            .configuration
            .steps
            .iter()
            .any(|step| step.stage == StepStage::Release),
        release_block_reasons: state.release_block_reasons.clone(),
        promotion_pause: state.promotion_pause.clone(),
        active_run,
        last_run,
        runs,
    }
}

/// Why `tg release retry` had nothing to run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseRetryRefusal {
    /// The active configuration has no release-stage steps.
    NoReleaseStage,
    /// The `staging` tip already passed the release stage under the active digest.
    AlreadyCertified,
}

impl ReleaseRetryRefusal {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoReleaseStage => "no-release-stage",
            Self::AlreadyCertified => "already-certified",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseRetryAction {
    /// A new release run was queued for the `staging` tip.
    Queued,
    /// A release run for the `staging` tip is already queued or running; nothing new was queued.
    AlreadyActive,
}

/// The result of `tg release retry`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRetryResult {
    pub repository_id: RepositoryId,
    pub action: ReleaseRetryAction,
    /// The release run that validates the target.
    pub item_id: QueueItemId,
    pub target_oid: GitOid,
    pub staging_oid: GitOid,
    pub release_oid: GitOid,
    /// The newest earlier run on the target, which the new run reruns.
    pub retry_of_item_id: Option<QueueItemId>,
    /// Queued runs for older tips that the retry superseded before they started.
    pub superseded_item_ids: Vec<QueueItemId>,
}

/// What `tg wait --released` waits for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ReleaseWaitTarget {
    /// A gate candidate; its promoted OID once it is promoted.
    Candidate { item_id: QueueItemId },
    /// A revision naming a `staging` commit.
    Revision { revision: String },
    /// The newest promotion from `worktree_path`, else the current `staging` tip.
    Latest { worktree_path: Option<String> },
}

/// How `tg wait --released` chose its target OID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseWaitSource {
    Candidate,
    Revision,
    WorktreePromotion,
    StagingTip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseWaitOutcome {
    /// The candidate is not promoted yet.
    AwaitingPromotion,
    /// The target is on `staging`; `release` does not contain it yet.
    Pending,
    /// `release` contains the target.
    Released,
    /// The newest release run covering the target failed.
    ReleaseFailed,
    /// The candidate ended without being promoted.
    NotPromoted,
    /// The target is not on `staging`, so no release run can cover it.
    NotOnStaging,
    /// The repository is blocked, or a release hold keeps `release` from advancing to the
    /// target's passing run.
    Blocked,
}

impl ReleaseWaitOutcome {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::AwaitingPromotion | Self::Pending)
    }
}

/// One `tg wait --released` observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseWaitStatus {
    pub repository_id: RepositoryId,
    pub source: ReleaseWaitSource,
    pub item_id: Option<QueueItemId>,
    /// Absent until a candidate is promoted.
    pub target_oid: Option<GitOid>,
    pub status: ReleaseWaitOutcome,
    pub staging_oid: GitOid,
    pub release_oid: GitOid,
    pub release_state: ReleaseState,
    pub release_lag: ReleaseLag,
    /// The newest release run whose target contains the target OID.
    pub covering_run: Option<ReleaseRunSummary>,
    pub candidate_state: Option<QueueItemState>,
    pub repository_execution_state: RepositoryExecutionState,
    pub block_reasons: Vec<BlockReason>,
    pub release_block_reasons: Vec<BlockReason>,
}

impl TollgateService {
    /// `tg release retry`: reruns the newest `staging` tip as a release run (section 8.5).
    ///
    /// Under the mutation lock, a normal release trigger pass first coalesces runs and retires
    /// those frozen under another release-stage digest. Then:
    ///
    /// - with no release stage, or when the tip already passed under the active digest, nothing
    ///   runs ([`ServiceError::ReleaseRetryUnavailable`]);
    /// - when a run for the tip is queued or running, that run is returned
    ///   ([`ReleaseRetryAction::AlreadyActive`]);
    /// - otherwise a run for the tip is queued, even when `release` already holds the tip, and a
    ///   queued run for an older tip that never started is superseded. A started run for an
    ///   older tip keeps running, so at most one started and one queued run remain.
    ///
    /// The queued run and the command result commit in one transaction: a replay of
    /// `command_id`, including the client's resend after a restart, returns the same run.
    pub async fn release_retry(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<ReleaseRetryResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
        }))?;
        let replay = || {
            runtime
                .store
                .checked_command_response::<ReleaseRetryResult>(
                    command_id,
                    "release-retry",
                    &request_digest,
                )
        };
        if let Some(response) = replay()? {
            return Ok(response);
        }
        let mutation = runtime.mutation.lock().await;
        if let Some(response) = replay()? {
            return Ok(response);
        }
        self.settle_release_trigger(&runtime).await?;
        let result = self
            .queue_release_retry(&runtime, command_id, &request_digest)
            .await;
        drop(mutation);
        self.spawn_eligible(repository_id, &runtime);
        result
    }

    async fn queue_release_retry(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        request_digest: &str,
    ) -> Result<ReleaseRetryResult, ServiceError> {
        let (config, state) = {
            let data = runtime.data.lock();
            (data.config.clone(), data.state.clone())
        };
        let tip = state.staging_oid.clone();
        let Some(release_digest) = config.release_step_graph_digest.clone() else {
            return Err(ServiceError::ReleaseRetryUnavailable {
                reason: ReleaseRetryRefusal::NoReleaseStage,
                message: "the active configuration has no release-stage steps; there is no release run to retry".into(),
                staging_oid: tip,
                release_oid: state.release_oid,
            });
        };
        let (active_on_tip, certified, previous_on_tip, unstarted_elsewhere, next_sequence) = {
            let data = runtime.data.lock();
            let releases = data
                .items
                .iter()
                .filter(|item| item.kind == QueueItemKind::Release)
                .collect::<Vec<_>>();
            let active_on_tip = releases
                .iter()
                .find(|item| !item.state.is_terminal() && item.source_oid == tip)
                .map(|item| (*item).clone());
            let certified = releases.iter().any(|item| {
                item.source_oid == tip
                    && item.state == QueueItemState::CheckPassed
                    && data.certificates.iter().any(|certificate| {
                        Some(certificate.id) == item.certificate_id
                            && certificate.tested_oid == tip
                            && certificate.checkout_verified
                            && certificate.step_graph_digest == release_digest
                    })
            });
            let previous_on_tip = releases
                .iter()
                .filter(|item| item.source_oid == tip && item.state.is_terminal())
                .max_by_key(|item| item.enqueue_sequence)
                .map(|item| {
                    let anchor = data
                        .generations
                        .iter()
                        .find(|generation| Some(generation.id) == item.current_generation_id)
                        .map(|generation| generation.anchored_base_oid.clone());
                    (item.id, anchor)
                });
            let unstarted_elsewhere = releases
                .iter()
                .filter(|item| {
                    item.source_oid != tip
                        && item.state == QueueItemState::Queued
                        && !data
                            .buildsets
                            .iter()
                            .any(|buildset| buildset.item_id == item.id)
                })
                .map(|item| (*item).clone())
                .collect::<Vec<_>>();
            let next_sequence = data
                .items
                .iter()
                .map(|item| item.enqueue_sequence)
                .max()
                .unwrap_or(0)
                + 1;
            (
                active_on_tip,
                certified,
                previous_on_tip,
                unstarted_elsewhere,
                next_sequence,
            )
        };
        if let Some(active) = active_on_tip {
            let result = ReleaseRetryResult {
                repository_id: state.id,
                action: ReleaseRetryAction::AlreadyActive,
                item_id: active.id,
                target_oid: tip,
                staging_oid: state.staging_oid.clone(),
                release_oid: state.release_oid.clone(),
                retry_of_item_id: active.retry_of_item_id,
                superseded_item_ids: Vec::new(),
            };
            runtime.store.record_command_result(
                state.id,
                command_id,
                "release-retry",
                request_digest,
                &result,
            )?;
            return Ok(result);
        }
        if certified {
            let message = if tip == state.release_oid {
                format!(
                    "release already holds staging tip {} and it passed the active release stage; there is nothing to retry",
                    tip.short()
                )
            } else {
                let holds = state
                    .release_block_reasons
                    .iter()
                    .map(|reason| reason.message.as_str())
                    .collect::<Vec<_>>();
                format!(
                    "staging tip {} already passed the active release stage; release advances to it {}",
                    tip.short(),
                    if holds.is_empty() {
                        "on the next release pass".to_owned()
                    } else {
                        format!("once its holds clear: {}", holds.join("; "))
                    }
                )
            };
            return Err(ServiceError::ReleaseRetryUnavailable {
                reason: ReleaseRetryRefusal::AlreadyCertified,
                message,
                staging_oid: tip,
                release_oid: state.release_oid,
            });
        }
        let mut retired = Vec::new();
        for mut item in unstarted_elsewhere {
            item.state = item
                .state
                .transition(ItemEvent::Superseded)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.terminal_reason = Some("superseded-by-newer-staging-tip".into());
            retired.push(item);
        }
        // While `release` already holds the tip, rerun the previous run's range.
        let anchor = previous_on_tip
            .as_ref()
            .and_then(|(_, anchor)| anchor.clone())
            .filter(|_| tip == state.release_oid);
        let retry_of_item_id = previous_on_tip.map(|(id, _)| id);
        let (item, generation) = self
            .prepare_release_run(
                runtime,
                &config,
                &state,
                next_sequence,
                anchor,
                retry_of_item_id,
            )
            .await?;
        let result = ReleaseRetryResult {
            repository_id: state.id,
            action: ReleaseRetryAction::Queued,
            item_id: item.id,
            target_oid: tip.clone(),
            staging_oid: state.staging_oid.clone(),
            release_oid: state.release_oid.clone(),
            retry_of_item_id,
            superseded_item_ids: retired.iter().map(|item| item.id).collect(),
        };
        let outcome = serde_json::json!({
            "action": "retried",
            "staging_oid": state.staging_oid,
            "release_oid": state.release_oid,
            "queued_item_id": item.id,
            "retry_of_item_id": retry_of_item_id,
            "retired_item_ids": result.superseded_item_ids,
        });
        let mut state = runtime.data.lock().state.clone();
        let events = runtime.store.record_release_trigger(&ReleaseTrigger {
            state: &state,
            retired: &retired,
            queued: Some((&item, &generation)),
            outcome,
            command: Some(ReleaseTriggerCommand {
                command_id,
                command_kind: "release-retry",
                request_digest,
                response: serde_json::to_value(&result)?,
            }),
        })?;
        if let Some(event) = events.last() {
            state.event_sequence = event.sequence;
        }
        self.apply_release_trigger(runtime, &state, retired, Some((item, generation)), events)
            .await?;
        Ok(result)
    }

    /// One `tg wait --released` observation of `target`.
    pub async fn release_wait_status(
        &self,
        repository_id: RepositoryId,
        target: ReleaseWaitTarget,
    ) -> Result<ReleaseWaitStatus, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let state = runtime.data.lock().state.clone();
        let revision_oid = match &target {
            ReleaseWaitTarget::Revision { revision } => {
                Some(runtime.git.resolve_oid(revision).await?)
            }
            _ => None,
        };
        let (source, item_id, target_oid, candidate_state) = {
            let data = runtime.data.lock();
            match &target {
                ReleaseWaitTarget::Candidate { item_id } => {
                    let view = data
                        .items
                        .iter()
                        .find(|item| item.id == *item_id && item.kind == QueueItemKind::Gate)
                        .map(|item| queue_item_view(&data, item))
                        .ok_or(ServiceError::ItemNotFound(*item_id))?;
                    let promoted = matches!(
                        view.item.state,
                        QueueItemState::Promoted
                            | QueueItemState::PromotedLocalPushPending
                            | QueueItemState::ExternallyIntegrated
                    );
                    let oid = promoted.then(|| promoted_oid(&view));
                    (
                        ReleaseWaitSource::Candidate,
                        Some(*item_id),
                        oid,
                        Some(view.item.state),
                    )
                }
                ReleaseWaitTarget::Revision { .. } => {
                    (ReleaseWaitSource::Revision, None, revision_oid, None)
                }
                ReleaseWaitTarget::Latest { worktree_path } => data
                    .items
                    .iter()
                    .filter(|item| {
                        item.kind == QueueItemKind::Gate
                            && item.state == QueueItemState::Promoted
                            && worktree_path.is_some()
                            && item.metadata.worktree_path == *worktree_path
                    })
                    .max_by_key(|item| item.enqueue_sequence)
                    .map(|item| {
                        (
                            ReleaseWaitSource::WorktreePromotion,
                            Some(item.id),
                            Some(promoted_oid(&queue_item_view(&data, item))),
                            Some(item.state),
                        )
                    })
                    .unwrap_or((
                        ReleaseWaitSource::StagingTip,
                        None,
                        Some(state.staging_oid.clone()),
                        None,
                    )),
            }
        };
        let mut status = ReleaseWaitStatus {
            repository_id,
            source,
            item_id,
            target_oid: target_oid.clone(),
            status: ReleaseWaitOutcome::Pending,
            staging_oid: state.staging_oid.clone(),
            release_oid: state.release_oid.clone(),
            release_state: state.release_state,
            release_lag: state.release_lag.clone(),
            covering_run: None,
            candidate_state,
            repository_execution_state: state.execution_state,
            block_reasons: state.block_reasons.clone(),
            release_block_reasons: state.release_block_reasons.clone(),
        };
        let Some(target_oid) = target_oid else {
            status.status = if candidate_state.is_some_and(QueueItemState::is_terminal) {
                ReleaseWaitOutcome::NotPromoted
            } else if state.execution_state == RepositoryExecutionState::Blocked {
                ReleaseWaitOutcome::Blocked
            } else {
                ReleaseWaitOutcome::AwaitingPromotion
            };
            return Ok(status);
        };
        if runtime
            .git
            .is_ancestor(&target_oid, &state.release_oid)
            .await?
        {
            status.status = ReleaseWaitOutcome::Released;
            return Ok(status);
        }
        if !runtime
            .git
            .is_ancestor(&target_oid, &state.staging_oid)
            .await?
        {
            status.status = ReleaseWaitOutcome::NotOnStaging;
            return Ok(status);
        }
        let mut runs = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .filter(|item| {
                    item.kind == QueueItemKind::Release
                        && !matches!(
                            item.state,
                            QueueItemState::Superseded | QueueItemState::Canceled
                        )
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        runs.sort_by_key(|item| std::cmp::Reverse(item.enqueue_sequence));
        let mut covering = None;
        for item in runs.into_iter().take(RELEASE_WAIT_SCAN_LIMIT) {
            if runtime
                .git
                .is_ancestor(&target_oid, &item.source_oid)
                .await?
            {
                covering = Some(item.id);
                break;
            }
        }
        status.covering_run = covering.and_then(|id| {
            let data = runtime.data.lock();
            data.items
                .iter()
                .find(|item| item.id == id)
                .map(|item| ReleaseRunSummary::from_view(&queue_item_view(&data, item)))
        });
        let run_state = status.covering_run.as_ref().map(|run| run.state);
        status.status = if matches!(
            run_state,
            Some(QueueItemState::CheckFailed | QueueItemState::InfrastructureExhausted)
        ) {
            ReleaseWaitOutcome::ReleaseFailed
        } else if state.execution_state == RepositoryExecutionState::Blocked
            || (run_state == Some(QueueItemState::CheckPassed)
                && !state.release_block_reasons.is_empty())
        {
            ReleaseWaitOutcome::Blocked
        } else {
            ReleaseWaitOutcome::Pending
        };
        Ok(status)
    }
}

/// The OID a promoted gate candidate put on `staging`.
fn promoted_oid(view: &QueueItemView) -> GitOid {
    view.certificate
        .as_ref()
        .map(|certificate| certificate.tested_oid.clone())
        .or_else(|| {
            view.generation
                .as_ref()
                .map(|generation| generation.tested_oid.clone())
        })
        .unwrap_or_else(|| view.item.source_oid.clone())
}
