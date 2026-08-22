#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use directories::ProjectDirs;
use globset::{Glob, GlobSetBuilder};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{RwLock, Semaphore, broadcast},
};
use tokio_util::sync::CancellationToken;
use tollgate_config::{CachePolicy, EffectiveConfig, EffectiveStep};
use tollgate_domain::*;
use tollgate_git::{
    FileIdentity, GitError, GitRepository, INTEGRATION_BRANCH, INTEGRATION_REF, USER_BRANCH,
    USER_BRANCH_REF, UserMasterSyncOutcome,
};
use tollgate_runner::apfs::{CloneManifest, force_clone_file, force_clone_tree, verify_clone_tree};
use tollgate_runner::{
    BuildsetExecution, EnvironmentSnapshot, RenderedLogFrame, StepResultClass,
    durable_log_tail_start, read_durable_log, run_buildset, run_buildset_scheduled,
    verify_durable_log,
};
use tollgate_scheduler::{
    DispatchRequest, GlobalScheduler, PriorityClass, ResourceCapacity, SchedulerError,
};
use tollgate_store::{
    ArtifactRecord, IntentState, RepositoryStore, SeedRecord, StepAttemptRecord, StoreError,
};

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Git error: {0}")]
    Git(#[from] GitError),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("configuration error: {0}")]
    Configuration(#[from] tollgate_config::ConfigError),
    #[error("runner error: {0}")]
    Runner(#[from] tollgate_runner::RunnerError),
    #[error("repository {0} is not registered")]
    RepositoryNotFound(RepositoryId),
    #[error("repository path is not valid UTF-8")]
    NonUtf8Path,
    #[error("repository cannot execute while it is {0:?}")]
    RepositoryUnavailable(RepositoryExecutionState),
    #[error("configuration file is missing; initialize the repository first")]
    MissingConfiguration,
    #[error("queue item {0} was not found")]
    ItemNotFound(QueueItemId),
    #[error("queue revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error(
        "candidate source parent {source_parent_oid} belongs to a stale speculative queue prefix; current promoted release {release_oid}, queue revision {queue_revision}, internal queue prefix {current_prefix_oid}. Rebase the single task commit onto promoted `release` {release_oid}, never onto the speculative prefix, resolve and regenerate, then resubmit"
    )]
    StaleQueuePrefix {
        source_parent_oid: GitOid,
        release_oid: GitOid,
        queue_revision: u64,
        current_prefix_oid: GitOid,
    },
    #[error(
        "candidate source has unknown unmerged ancestor {ancestor}; current promoted release {release_oid}, queue revision {queue_revision}, internal queue prefix {current_prefix_oid}. Rebase the single task commit onto promoted `release` {release_oid}, never onto the speculative prefix, then resubmit"
    )]
    UnknownSourceAncestor {
        ancestor: GitOid,
        release_oid: GitOid,
        queue_revision: u64,
        current_prefix_oid: GitOid,
    },
    #[error(
        "candidate source includes unpromoted ancestor {ancestor}; current promoted release is {release_oid}. Ordinary candidates must contain exactly one task commit based only on promoted `release`; speculative queue prefixes are internal Tollgate state and must never be incorporated into a source branch. Rebase the single task commit onto `release` {release_oid}, then resubmit"
    )]
    UnpromotedSourceAncestor {
        ancestor: GitOid,
        release_oid: GitOid,
    },
    #[error("internal service invariant failed: {0}")]
    Invariant(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("registry serialization error: {0}")]
    RegistryJson(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredRepository {
    pub id: RepositoryId,
    pub name: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub version: String,
    pub generated_at: OffsetDateTime,
    pub repositories: Vec<RepositorySnapshot>,
    pub unavailable_repositories: Vec<UnavailableRepository>,
    pub environment: EnvironmentView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ItemWaitStatus {
    pub item: QueueItem,
    pub repository_execution_state: RepositoryExecutionState,
    pub block_reasons: Vec<BlockReason>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnavailableRepository {
    pub id: RepositoryId,
    pub name: String,
    pub path: PathBuf,
    pub error: String,
    pub recovery_action: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnvironmentView {
    pub snapshot_id: String,
    pub fingerprint: String,
    pub path: String,
    pub variable_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiagnosticStatus {
    Healthy,
    Attention,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiagnosticCheck {
    pub name: String,
    pub status: DiagnosticStatus,
    pub detail: String,
    pub recovery_action: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DoctorReport {
    pub repository_id: RepositoryId,
    pub generated_at: OffsetDateTime,
    pub checks: Vec<DiagnosticCheck>,
    pub healthy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorktreeRemovalObservation {
    Intact,
    Removed,
    Residual,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositorySnapshot {
    pub state: RepositoryState,
    pub observed_master_oid: GitOid,
    pub queue: Vec<QueueItemView>,
    pub checks: Vec<QueueItemView>,
    #[serde(default)]
    pub master_push: Option<QueueItemView>,
    pub history_items: Vec<QueueItemView>,
    pub history: Vec<DomainEvent>,
    pub configuration: ConfigurationView,
    pub resources: ResourceView,
    pub slots: Vec<SlotView>,
    pub seeds: Vec<SeedView>,
    pub artifacts: Vec<ArtifactRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueItemView {
    pub item: QueueItem,
    pub generation: Option<ValidationGeneration>,
    pub buildset: Option<Buildset>,
    pub attempts: Vec<Buildset>,
    pub attempt_generations: Vec<ValidationGeneration>,
    pub certificate: Option<PassCertificate>,
    pub certificates: Vec<PassCertificate>,
    pub included_items: Vec<String>,
    pub elapsed_ms: Option<u64>,
    #[serde(default)]
    pub failure_attribution: Option<FailureAttribution>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailureOrigin {
    CandidateIntroduced,
    InheritedFromBase,
    FlakyOrNonHermetic,
    OriginUnknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailureAttribution {
    pub origin: FailureOrigin,
    pub candidate_buildset_id: BuildsetId,
    pub candidate_tested_oid: GitOid,
    pub base_oid: GitOid,
    pub configuration_digest: String,
    pub step_graph_digest: String,
    pub environment_fingerprint: String,
    pub steps: Vec<StepFailureAttribution>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepFailureAttribution {
    pub name: String,
    pub origin: FailureOrigin,
    pub candidate_result: String,
    pub baseline_result: Option<String>,
    pub baseline_buildset_id: Option<BuildsetId>,
    #[serde(default)]
    pub baseline_item_id: Option<QueueItemId>,
    pub diagnostics: Vec<StepDiagnostic>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiagnoseResult {
    pub item_id: QueueItemId,
    pub attribution: Option<FailureAttribution>,
    #[serde(default)]
    pub replay_item_ids: Vec<QueueItemId>,
    #[serde(default)]
    pub scheduled_replay_item_ids: Vec<QueueItemId>,
    #[serde(default)]
    pub reused_replay_item_ids: Vec<QueueItemId>,
    #[serde(default)]
    pub replay_reasons: Vec<String>,
    #[serde(default)]
    pub repair_artifact: Option<RepairArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepairArtifact {
    pub path: String,
    pub blake3: String,
    pub byte_length: u64,
    pub verified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryItemsPage {
    pub items: Vec<QueueItemView>,
    pub total: usize,
    pub offset: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigurationView {
    pub digest: String,
    pub step_graph_digest: String,
    pub steps: Vec<EffectiveStep>,
    pub remote_enabled: bool,
    pub remote_name: String,
    pub remote_branch: String,
    pub runner: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceView {
    pub max_buildsets: u16,
    pub repository_concurrency: u16,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub active_runs: usize,
    pub queued_runs: usize,
    pub cpu_reserved: u16,
    pub memory_reserved: u64,
    pub named_semaphores: BTreeMap<String, u16>,
    pub authoritative_volume_available: u64,
    pub recovery_reserve: u64,
    pub volumes: Vec<VolumeView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeView {
    pub id: String,
    pub roles: Vec<String>,
    pub available_bytes: u64,
    pub warning_threshold: u64,
    pub critical_threshold: u64,
    pub emergency_allowance: u64,
    pub state: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SlotView {
    pub id: SlotId,
    pub path: PathBuf,
    pub state: String,
    pub checkout_oid: Option<GitOid>,
    pub health: String,
    pub last_used: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedView {
    pub id: String,
    pub path: String,
    pub profile: String,
    pub generation: u64,
    pub logical_size: u64,
    pub state: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheOperationResult {
    pub action: String,
    pub seed_ids: Vec<String>,
    pub slots_reset: Vec<SlotId>,
    pub logical_bytes: u64,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationResult {
    pub repository_id: RepositoryId,
    pub action: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ArtifactRetentionEvidence {
    repository_id: RepositoryId,
    buildset_id: BuildsetId,
    staging_dir: PathBuf,
    destination_dir: PathBuf,
    records: Vec<ArtifactRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ArtifactPruneEvidence {
    repository_id: RepositoryId,
    request_digest: String,
    record: ArtifactRecord,
    original_path: PathBuf,
    quarantine_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SeedSnapshotEvidence {
    repository_id: RepositoryId,
    request_digest: String,
    seed_id: String,
    generation: u64,
    staging: PathBuf,
    destination: PathBuf,
    source_slot: SlotId,
    source_oid: Option<GitOid>,
    selected: Vec<PathBuf>,
    cache_epoch: u64,
    cache_policy_digest: String,
    configuration_digest: String,
    os: String,
    architecture: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SeedPruneEvidence {
    record: SeedRecord,
    original: PathBuf,
    quarantine: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SlotPruneEvidence {
    slot: SlotView,
    checkout: GitOid,
    quarantine: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CachePurgeEvidence {
    repository_id: RepositoryId,
    request_digest: String,
    seeds: Vec<SeedPruneEvidence>,
    slots: Vec<SlotPruneEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct BackupEvidence {
    repository_id: RepositoryId,
    temporary: PathBuf,
    destination: PathBuf,
    allowance: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct GlobalCommandJournal {
    records: HashMap<String, GlobalCommandRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GlobalCommandRecord {
    kind: String,
    request_digest: String,
    payload: serde_json::Value,
    state: String,
    response: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApproveResult {
    pub item_id: QueueItemId,
    pub queue_revision: u64,
    pub source_oid: GitOid,
    pub tested_oid: GitOid,
}

struct GateSubmission {
    purpose: Option<String>,
    cleanup_policy: CleanupPolicy,
    command_id: CommandId,
    promotion_authorized: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateAuthorizationResult {
    pub item_id: QueueItemId,
    #[serde(default)]
    pub already_authorized: bool,
    #[serde(default)]
    pub authorized_item_ids: Vec<QueueItemId>,
    #[serde(default)]
    pub restarted_item_ids: Vec<QueueItemId>,
    #[serde(default)]
    pub restored_item_ids: Vec<QueueItemId>,
    pub queue_revision: u64,
    pub source_oid: GitOid,
    pub validation_generation_id: ValidationGenerationId,
    pub tested_oid: GitOid,
    pub validation_complete: bool,
    pub evidence_reused: bool,
    pub authorized_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueReorderResult {
    pub queue_revision: u64,
    pub ordered_item_ids: Vec<QueueItemId>,
    pub restarted_item_ids: Vec<QueueItemId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteSyncAction {
    UpToDate,
    AdoptedRemote,
    LocalAhead,
    Pushed,
    ReconciledLocal,
    Diverged,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteSyncResult {
    pub action: RemoteSyncAction,
    pub local_master: GitOid,
    pub remote_master: Option<GitOid>,
    pub queue_revision: u64,
    pub affected_item_ids: Vec<QueueItemId>,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorktreeOperationResult {
    pub action: String,
    pub path: String,
    pub branch: Option<String>,
    pub old_oid: Option<GitOid>,
    pub new_oid: Option<GitOid>,
    pub message: String,
}

struct RuntimeData {
    state: RepositoryState,
    items: Vec<QueueItem>,
    generations: Vec<ValidationGeneration>,
    buildsets: Vec<Buildset>,
    certificates: Vec<PassCertificate>,
    config: EffectiveConfig,
    slots: HashMap<SlotId, SlotView>,
    seeds: Vec<SeedRecord>,
}

impl RuntimeData {
    fn push_generation(&mut self, generation: ValidationGeneration) {
        if let Some(current) = self.generations.iter_mut().rev().find(|current| {
            current.item_id == generation.item_id && current.invalidated_by.is_none()
        }) {
            current.invalidated_by = Some(generation.id);
        }
        self.generations.push(generation);
    }

    fn extend_generations(&mut self, generations: impl IntoIterator<Item = ValidationGeneration>) {
        for generation in generations {
            self.push_generation(generation);
        }
    }

    fn activate_generation(&mut self, generation: ValidationGeneration) {
        if let Some(current) = self.generations.iter_mut().rev().find(|current| {
            current.item_id == generation.item_id && current.invalidated_by.is_none()
        }) {
            current.invalidated_by = Some(generation.id);
        }
        if let Some(existing) = self
            .generations
            .iter_mut()
            .find(|existing| existing.id == generation.id)
        {
            *existing = generation;
        } else {
            self.generations.push(generation);
        }
    }
}

struct RepositoryRuntime {
    _ownership_lock: nix::fcntl::Flock<std::fs::File>,
    git: GitRepository,
    store: RepositoryStore,
    mirror: PathBuf,
    builder: PathBuf,
    slots_root: PathBuf,
    logs_root: PathBuf,
    data: Mutex<RuntimeData>,
    events: broadcast::Sender<DomainEvent>,
    cancellations: Mutex<HashMap<QueueItemId, CancellationToken>>,
    mutation: tokio::sync::Mutex<()>,
    diagnosis: tokio::sync::Mutex<()>,
    execution_permits: RwLock<Arc<Semaphore>>,
    scheduler_epoch: AtomicU64,
    dispatching: Mutex<HashSet<QueueItemId>>,
    cold_sources: Mutex<HashSet<GitOid>>,
    cold_items: Mutex<HashSet<QueueItemId>>,
}

#[derive(Clone, Copy)]
struct UserMasterSyncProjection<'a> {
    tested_oid: &'a GitOid,
    replace_tip: Option<&'a GitOid>,
    remote_oid: Option<&'a GitOid>,
    rebase_unsubmitted: bool,
}

enum CheckMode {
    Normal,
    RetainedCold(GitOid),
}

pub struct TollgateService {
    support_root: PathBuf,
    registry_path: PathBuf,
    runtimes: RwLock<HashMap<RepositoryId, Arc<RepositoryRuntime>>>,
    unavailable: RwLock<Vec<UnavailableRepository>>,
    environment: RwLock<EnvironmentSnapshot>,
    environment_error: RwLock<Option<String>>,
    global_scheduler: Arc<GlobalScheduler>,
    global_command_path: PathBuf,
    global_commands: tokio::sync::Mutex<GlobalCommandJournal>,
    volume_reservations: tokio::sync::Mutex<()>,
    shutting_down: AtomicBool,
}

fn failure_attribution(
    data: &RuntimeData,
    generation: &ValidationGeneration,
    buildset: &Buildset,
) -> Option<FailureAttribution> {
    let voting_names = buildset
        .frozen_steps
        .iter()
        .filter(|step| step.voting)
        .map(|step| step.name.as_str())
        .collect::<HashSet<_>>();
    let failed = buildset
        .step_results
        .iter()
        .filter(|result| {
            voting_names.contains(result.name.as_str()) && result.result_class != "success"
        })
        .collect::<Vec<_>>();
    if failed.is_empty() {
        return None;
    }

    let comparable_buildsets = |tested_oid: &GitOid, environment_fingerprint: &str| {
        data.buildsets
            .iter()
            .filter(|candidate| {
                candidate.state.is_terminal()
                    && &candidate.tested_oid == tested_oid
                    && candidate.environment_fingerprint == environment_fingerprint
                    && data
                        .generations
                        .iter()
                        .find(|entry| entry.id == candidate.validation_generation_id)
                        .is_some_and(|entry| {
                            entry.configuration_digest == generation.configuration_digest
                                && entry.step_graph_digest == generation.step_graph_digest
                                && entry.engine_epoch == generation.engine_epoch
                        })
            })
            .collect::<Vec<_>>()
    };
    let has_trusted_success = |candidate: &Buildset| {
        data.certificates
            .iter()
            .any(|certificate| certificate.buildset_id == candidate.id)
            || data.items.iter().any(|item| {
                item.id == candidate.item_id && item.state == QueueItemState::CheckPassed
            })
    };
    let diagnosis_prefix = format!("diagnose:{}:", generation.item_id);
    let latest_matrix = data
        .items
        .iter()
        .filter_map(|item| {
            let purpose = item.metadata.purpose.as_deref()?;
            purpose
                .starts_with(&diagnosis_prefix)
                .then_some((item.enqueue_sequence, purpose))
        })
        .max_by_key(|(sequence, _)| *sequence)
        .and_then(|(_, purpose)| {
            purpose
                .strip_suffix(":base")
                .or_else(|| purpose.rsplit_once(":candidate:").map(|(root, _)| root))
                .map(str::to_owned)
        });
    let matrix = latest_matrix.and_then(|root| {
        let item_ids = data
            .items
            .iter()
            .filter(|item| {
                item.metadata
                    .purpose
                    .as_deref()
                    .is_some_and(|purpose| purpose.starts_with(&format!("{root}:")))
            })
            .map(|item| item.id)
            .collect::<HashSet<_>>();
        let buildsets = item_ids
            .iter()
            .filter_map(|item_id| {
                data.buildsets
                    .iter()
                    .filter(|candidate| candidate.item_id == *item_id)
                    .max_by_key(|candidate| candidate.attempt)
            })
            .collect::<Vec<_>>();
        let environment = buildsets.first()?.environment_fingerprint.clone();
        (buildsets.len() == 3
            && buildsets.iter().all(|candidate| {
                candidate.state.is_terminal()
                    && candidate.environment_fingerprint == environment
                    && data
                        .generations
                        .iter()
                        .find(|entry| entry.id == candidate.validation_generation_id)
                        .is_some_and(|entry| {
                            entry.configuration_digest == generation.configuration_digest
                                && entry.step_graph_digest == generation.step_graph_digest
                                && entry.engine_epoch == generation.engine_epoch
                        })
            }))
        .then_some((environment, buildsets))
    });
    let (evidence_environment, candidate_replays, baseline_buildsets) =
        if let Some((environment, matrix_buildsets)) = matrix {
            let mut candidates = matrix_buildsets
                .iter()
                .copied()
                .filter(|candidate| candidate.tested_oid == generation.tested_oid)
                .collect::<Vec<_>>();
            for candidate in comparable_buildsets(&generation.tested_oid, &environment) {
                if !candidates
                    .iter()
                    .any(|existing| existing.id == candidate.id)
                {
                    candidates.push(candidate);
                }
            }
            let baselines = matrix_buildsets
                .iter()
                .copied()
                .filter(|candidate| candidate.tested_oid == generation.expected_parent_oid)
                .collect::<Vec<_>>();
            (environment, candidates, baselines)
        } else {
            (
                buildset.environment_fingerprint.clone(),
                comparable_buildsets(&generation.tested_oid, &buildset.environment_fingerprint),
                comparable_buildsets(
                    &generation.expected_parent_oid,
                    &buildset.environment_fingerprint,
                ),
            )
        };
    let mut steps = Vec::with_capacity(failed.len());
    for result in failed {
        let has_candidate_success = candidate_replays.iter().any(|candidate| {
            has_trusted_success(candidate)
                && candidate
                    .step_results
                    .iter()
                    .any(|entry| entry.name == result.name && entry.result_class == "success")
        });
        let baseline = baseline_buildsets.iter().rev().find_map(|candidate| {
            candidate
                .step_results
                .iter()
                .find(|entry| entry.name == result.name)
                .filter(|entry| entry.result_class != "success" || has_trusted_success(candidate))
                .map(|entry| (*candidate, entry))
        });
        let origin = if has_candidate_success {
            FailureOrigin::FlakyOrNonHermetic
        } else {
            match baseline.map(|(_, entry)| entry.result_class.as_str()) {
                Some("success") => FailureOrigin::CandidateIntroduced,
                Some("skipped") | None => FailureOrigin::OriginUnknown,
                Some(_) => FailureOrigin::InheritedFromBase,
            }
        };
        steps.push(StepFailureAttribution {
            name: result.name.clone(),
            origin,
            candidate_result: result.result_class.clone(),
            baseline_result: baseline.map(|(_, entry)| entry.result_class.clone()),
            baseline_buildset_id: baseline.map(|(candidate, _)| candidate.id),
            baseline_item_id: baseline.map(|(candidate, _)| candidate.item_id),
            diagnostics: result.diagnostics.clone(),
        });
    }
    let origin = if steps
        .iter()
        .any(|step| step.origin == FailureOrigin::FlakyOrNonHermetic)
    {
        FailureOrigin::FlakyOrNonHermetic
    } else if steps
        .iter()
        .all(|step| step.origin == FailureOrigin::CandidateIntroduced)
    {
        FailureOrigin::CandidateIntroduced
    } else if steps
        .iter()
        .all(|step| step.origin == FailureOrigin::InheritedFromBase)
    {
        FailureOrigin::InheritedFromBase
    } else {
        FailureOrigin::OriginUnknown
    };
    Some(FailureAttribution {
        origin,
        candidate_buildset_id: buildset.id,
        candidate_tested_oid: generation.tested_oid.clone(),
        base_oid: generation.expected_parent_oid.clone(),
        configuration_digest: generation.configuration_digest.clone(),
        step_graph_digest: generation.step_graph_digest.clone(),
        environment_fingerprint: evidence_environment,
        steps,
    })
}

fn queue_item_view(data: &RuntimeData, item: &QueueItem) -> QueueItemView {
    let generation = item
        .current_generation_id
        .and_then(|id| {
            data.generations
                .iter()
                .find(|generation| generation.id == id)
        })
        .cloned();
    let buildset = item
        .buildset_id
        .and_then(|id| data.buildsets.iter().find(|buildset| buildset.id == id))
        .cloned();
    let certificate = item
        .certificate_id
        .and_then(|id| {
            data.certificates
                .iter()
                .find(|certificate| certificate.id == id)
        })
        .cloned();
    let certificates = data
        .certificates
        .iter()
        .filter(|certificate| certificate.queue_item_id == item.id)
        .cloned()
        .collect::<Vec<_>>();
    let mut attempts = data
        .buildsets
        .iter()
        .filter(|buildset| buildset.item_id == item.id)
        .cloned()
        .collect::<Vec<_>>();
    attempts.sort_by_key(|buildset| (buildset.created_at, buildset.attempt));
    let attempt_generation_ids = attempts
        .iter()
        .map(|buildset| buildset.validation_generation_id)
        .collect::<HashSet<_>>();
    let attempt_generations = data
        .generations
        .iter()
        .filter(|generation| attempt_generation_ids.contains(&generation.id))
        .cloned()
        .collect::<Vec<_>>();
    let included_items = generation
        .as_ref()
        .map(|generation| {
            generation
                .ordered_item_ids
                .iter()
                .map(|id| id.short())
                .collect()
        })
        .unwrap_or_default();
    let elapsed_ms = buildset
        .as_ref()
        .and_then(|buildset| {
            buildset
                .started_at
                .map(|start| buildset.finished_at.unwrap_or_else(OffsetDateTime::now_utc) - start)
        })
        .map(|duration| duration.whole_milliseconds().max(0) as u64);
    let failure_attribution = generation
        .as_ref()
        .zip(buildset.as_ref())
        .and_then(|(generation, buildset)| failure_attribution(data, generation, buildset));
    QueueItemView {
        item: item.clone(),
        generation,
        buildset,
        attempts,
        attempt_generations,
        certificate,
        certificates,
        included_items,
        elapsed_ms,
        failure_attribution,
    }
}

fn is_master_push_item(item: &QueueItem) -> bool {
    item.kind == QueueItemKind::Gate
        && item.promotion_authorized
        && item.metadata.branch.as_deref() == Some(USER_BRANCH)
        && matches!(
            item.metadata.purpose.as_deref(),
            Some("push-master") | Some("gate")
        )
}

fn reuse_active_enqueue_sequences(current: &[QueueItem], ordered: &mut [QueueItem]) {
    let mut sequences = current
        .iter()
        .map(|item| item.enqueue_sequence)
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    for (item, sequence) in ordered.iter_mut().zip(sequences) {
        item.enqueue_sequence = sequence;
    }
}

fn admission_sequence(item: &QueueItem) -> u64 {
    item.admission_sequence.unwrap_or(item.enqueue_sequence)
}

fn restorable_admission_order(current: &[QueueItem]) -> Option<Vec<QueueItemId>> {
    let mut admission_order = current.iter().collect::<Vec<_>>();
    admission_order.sort_by_key(|item| (admission_sequence(item), item.id));

    let mut unauthorized_seen = false;
    for item in &admission_order {
        if item.promotion_authorized {
            if unauthorized_seen {
                return None;
            }
        } else {
            unauthorized_seen = true;
        }
    }

    let admission_ids = admission_order
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    let current_ids = current.iter().map(|item| item.id).collect::<Vec<_>>();
    (admission_ids != current_ids).then_some(admission_ids)
}

fn matching_retained_evidence(
    item: &QueueItem,
    desired: &ValidationGeneration,
    generations: &[ValidationGeneration],
    buildsets: &[Buildset],
    certificates: &[PassCertificate],
    config: &EffectiveConfig,
    engine_epoch: u64,
) -> Option<(ValidationGeneration, Buildset, PassCertificate)> {
    generations
        .iter()
        .filter(|generation| {
            generation.item_id == item.id
                && generation.identity_digest == desired.identity_digest
                && generation.anchored_base_oid == desired.anchored_base_oid
                && generation.ordered_item_ids == desired.ordered_item_ids
                && generation.ordered_source_oids == desired.ordered_source_oids
                && generation.prefix_oids == desired.prefix_oids
                && generation.expected_parent_oid == desired.expected_parent_oid
                && generation.tested_oid == desired.tested_oid
                && generation.configuration_digest == desired.configuration_digest
                && generation.step_graph_digest == desired.step_graph_digest
                && generation.engine_epoch == desired.engine_epoch
        })
        .find_map(|generation| {
            let certificate = certificates.iter().find(|certificate| {
                certificate.queue_item_id == item.id
                    && certificate.validation_generation_id == generation.id
            })?;
            let buildset = buildsets.iter().find(|buildset| {
                buildset.id == certificate.buildset_id
                    && buildset.item_id == item.id
                    && buildset.validation_generation_id == generation.id
                    && matches!(
                        buildset.state,
                        BuildsetState::Passed | BuildsetState::PassedWithWarnings
                    )
            })?;
            let mut projected = item.clone();
            projected.state = QueueItemState::Ready;
            projected.current_generation_id = Some(generation.id);
            projected.buildset_id = Some(buildset.id);
            projected.certificate_id = Some(certificate.id);
            certificate
                .validates_frozen_inputs(
                    &projected,
                    generation,
                    &config.digest,
                    &config.step_graph_digest,
                    engine_epoch,
                )
                .then(|| {
                    let mut generation = generation.clone();
                    generation.invalidated_by = None;
                    (generation, buildset.clone(), certificate.clone())
                })
        })
}

fn current_queue_prefix_oid(data: &RuntimeData) -> GitOid {
    current_queue_prefix_from(&data.state, &data.items, &data.generations)
}

fn current_queue_prefix_from(
    state: &RepositoryState,
    items: &[QueueItem],
    generations: &[ValidationGeneration],
) -> GitOid {
    let active_ids = items
        .iter()
        .filter(|item| item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state))
        .map(|item| item.id)
        .collect::<HashSet<_>>();
    items
        .iter()
        .filter(|item| item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state))
        .filter_map(|item| {
            let generation_id = item.current_generation_id?;
            let generation = generations
                .iter()
                .find(|generation| generation.id == generation_id)?;
            generation
                .ordered_item_ids
                .iter()
                .all(|item_id| active_ids.contains(item_id))
                .then_some((item, generation))
        })
        .max_by_key(|(item, _)| item.enqueue_sequence)
        .map(|(_, generation)| generation.tested_oid.clone())
        .unwrap_or_else(|| state.master_oid.clone())
}

fn active_prefix_dependencies(data: &RuntimeData, oid: &GitOid) -> Option<Vec<QueueItemId>> {
    let active_ids = data
        .items
        .iter()
        .filter(|item| item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state))
        .map(|item| item.id)
        .collect::<HashSet<_>>();
    data.items
        .iter()
        .filter(|item| item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state))
        .filter_map(|item| item.current_generation_id)
        .filter_map(|generation_id| {
            data.generations
                .iter()
                .find(|generation| generation.id == generation_id)
        })
        .find_map(|generation| {
            generation
                .prefix_oids
                .iter()
                .position(|prefix_oid| prefix_oid == oid)
                .and_then(|position| {
                    let dependencies = &generation.ordered_item_ids[..=position];
                    dependencies
                        .iter()
                        .all(|item_id| active_ids.contains(item_id))
                        .then(|| dependencies.to_vec())
                })
        })
}

fn is_historical_prefix(data: &RuntimeData, oid: &GitOid) -> bool {
    data.generations
        .iter()
        .any(|generation| generation.prefix_oids.contains(oid))
}

async fn retain_speculative_generations(
    runtime: &RepositoryRuntime,
    generations: &[ValidationGeneration],
) -> Result<(), ServiceError> {
    for generation in generations {
        runtime
            .git
            .retain_speculative_object(
                &runtime.mirror,
                &generation.id.to_string(),
                &generation.tested_oid,
            )
            .await?;
    }
    Ok(())
}

impl TollgateService {
    pub async fn open_default() -> Result<Arc<Self>, ServiceError> {
        let directories = ProjectDirs::from("dev", "Tollgate", "Tollgate").ok_or_else(|| {
            ServiceError::Invariant("application support directory is unavailable".into())
        })?;
        Self::open(directories.data_dir().to_owned()).await
    }

    pub async fn open(support_root: PathBuf) -> Result<Arc<Self>, ServiceError> {
        tokio::fs::create_dir_all(&support_root).await?;
        let (environment, environment_error) =
            match EnvironmentSnapshot::capture_login_shell().await {
                Ok(environment) => (environment, None),
                Err(error) => {
                    eprintln!("Tollgate login-shell capture failed: {error}");
                    (EnvironmentSnapshot::capture(), Some(error.to_string()))
                }
            };
        let global_command_path = support_root.join("global-command-results.json");
        let global_commands = if global_command_path.exists() {
            let bytes = tokio::fs::read(&global_command_path).await?;
            match serde_json::from_slice(&bytes) {
                Ok(journal) => journal,
                Err(error) => {
                    let preserved = global_command_path
                        .with_extension(format!("corrupt-{}-json", uuid::Uuid::now_v7()));
                    tokio::fs::rename(&global_command_path, &preserved).await?;
                    eprintln!(
                        "Preserved malformed global command journal at {}: {error}",
                        preserved.display()
                    );
                    GlobalCommandJournal::default()
                }
            }
        } else {
            GlobalCommandJournal::default()
        };
        let service = Arc::new(Self {
            registry_path: support_root.join("repositories.json"),
            support_root,
            runtimes: RwLock::new(HashMap::new()),
            unavailable: RwLock::new(Vec::new()),
            environment: RwLock::new(environment),
            environment_error: RwLock::new(environment_error),
            global_scheduler: Arc::new(GlobalScheduler::new(ResourceCapacity {
                max_buildsets: 4,
                cpu_tokens: 1,
                memory_bytes: 1,
                semaphores: BTreeMap::new(),
            })),
            global_command_path,
            global_commands: tokio::sync::Mutex::new(global_commands),
            volume_reservations: tokio::sync::Mutex::new(()),
            shutting_down: AtomicBool::new(false),
        });
        service.load_registry().await?;
        service.reconcile_global_commands().await?;
        service.spawn_maintenance();
        Ok(service)
    }

    fn spawn_maintenance(self: &Arc<Self>) {
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            let mut interval = tokio::time::interval_at(start, std::time::Duration::from_secs(2));
            let mut ticks = 0_u64;
            loop {
                interval.tick().await;
                let Some(service) = service.upgrade() else {
                    break;
                };
                let ids = service
                    .runtimes
                    .read()
                    .await
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                for repository_id in ids {
                    if let Err(error) = service.observe_configuration(repository_id).await {
                        eprintln!(
                            "Tollgate configuration observation failed for {repository_id}: {error}"
                        );
                    }
                    let remote_enabled = service
                        .runtime(repository_id)
                        .await
                        .ok()
                        .is_some_and(|runtime| runtime.data.lock().config.remote.enabled);
                    if ticks.is_multiple_of(30)
                        && remote_enabled
                        && let Err(error) = service.pull(repository_id, CommandId::new()).await
                    {
                        eprintln!(
                            "Tollgate periodic remote observation failed for {repository_id}: {error}"
                        );
                    }
                    if ticks.is_multiple_of(1_800)
                        && let Err(error) = service.prune_expired_artifacts(repository_id).await
                    {
                        eprintln!(
                            "Tollgate artifact retention sweep failed for {repository_id}: {error}"
                        );
                    }
                }
                ticks = ticks.wrapping_add(1);
            }
        });
    }

    async fn observe_configuration(
        self: &Arc<Self>,
        repository_id: RepositoryId,
    ) -> Result<(), ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let path = runtime.git.worktree_root.join(".tollgate/config.toml");
        let disk = tokio::fs::read_to_string(path)
            .await
            .ok()
            .and_then(|contents| EffectiveConfig::parse(&contents).ok());
        let state = {
            let mut data = runtime.data.lock();
            let matches_active = disk
                .as_ref()
                .is_some_and(|candidate| candidate.digest == data.config.digest);
            let desired = if matches_active {
                match data.state.execution_state {
                    RepositoryExecutionState::ConfigurationPending => {
                        if data.state.block_reasons.is_empty() {
                            RepositoryExecutionState::Active
                        } else {
                            RepositoryExecutionState::Blocked
                        }
                    }
                    current => current,
                }
            } else {
                RepositoryExecutionState::ConfigurationPending
            };
            (data.state.execution_state != desired).then(|| {
                data.state.execution_state = desired;
                data.state.clone()
            })
        };
        if let Some(state) = state {
            runtime.store.update_repository_state(&state)?;
            if state.execution_state == RepositoryExecutionState::Active {
                self.spawn_eligible(repository_id, &runtime);
            }
        }
        Ok(())
    }

    pub async fn initialize_repository(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
        command: Option<String>,
    ) -> Result<RepositorySnapshot, ServiceError> {
        self.initialize_repository_with_options(path, command, false)
            .await
    }

    pub async fn initialize_repository_with_options(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
        command: Option<String>,
        bootstrap: bool,
    ) -> Result<RepositorySnapshot, ServiceError> {
        self.initialize_repository_with_policy(path, command, bootstrap, true)
            .await
    }

    pub async fn initialize_repository_with_policy(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
        command: Option<String>,
        bootstrap: bool,
        _legacy_detach_master: bool,
    ) -> Result<RepositorySnapshot, ServiceError> {
        let git = GitRepository::discover(path).await?;
        if let Some(id) = self.registered_common_directory(&git.common_dir).await {
            return self.repository_snapshot(id).await;
        }
        let tollgate_root = git.common_dir.join("tollgate");
        tokio::fs::create_dir_all(&tollgate_root).await?;
        let ownership_lock = acquire_repository_lock(&git.common_dir)?;
        let existing_store = tollgate_root.join("state.sqlite3");
        let store = self.open_repository_store(&existing_store).await?;
        if existing_store.exists() {
            match store.repository_state() {
                Ok(_) => {
                    drop(store);
                    drop(ownership_lock);
                    let restored = self.register_existing(&git.worktree_root).await?;
                    self.save_registry().await?;
                    return Ok(restored);
                }
                // Opening/migrating the database precedes creation of the
                // repository identity. An empty schema is a recoverable init
                // boundary, not evidence that initialization completed.
                Err(StoreError::RepositoryMissing) => {}
                Err(error) => return Err(error.into()),
            }
        }
        git.initialize_integration_ref_from_master().await?;
        prepare_configuration_root(&git).await?;
        let generated = command.unwrap_or_else(|| detect_command(&git.worktree_root));
        let generated = format!(
            "version = 1\n\n[[step]]\nname = \"ci\"\nrun = {}\n",
            toml_string(&generated)
        );
        let config_text = read_or_migrate_configuration(&git, Some(&generated)).await?;
        let config = EffectiveConfig::parse(&config_text)?;
        mirror_legacy_configuration(&git, &config_text).await?;
        let master_oid = git.integration_oid().await?;
        let repository_id = RepositoryId::new();
        let name = git
            .worktree_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("Repository")
            .to_owned();
        let mut execution_state = RepositoryExecutionState::Active;
        let mut block_reasons = Vec::new();
        if let Err(GitError::IntegrationCheckedOut(path)) =
            git.ensure_integration_not_checked_out().await
        {
            execution_state = RepositoryExecutionState::Blocked;
            block_reasons.push(BlockReason { code: "release-checked-out".into(), message: format!("Tollgate's integration branch `release` is checked out at {path}"), recovery_action: "Switch that worktree back to the user-owned `master` branch or another feature branch, then resume the gate.".into() });
        }
        if let Some(error) = self.environment_error.read().await.clone() {
            execution_state = RepositoryExecutionState::Blocked;
            block_reasons.push(BlockReason {
                code: "environment-bootstrap-failed".into(),
                message: format!("Login-shell environment capture failed: {error}"),
                recovery_action: "Repair the login shell, then reload the environment from Doctor or `tg env reload`.".into(),
            });
        }
        let state = RepositoryState {
            id: repository_id,
            name: name.clone(),
            path: git.worktree_root.to_string_lossy().into_owned(),
            integration_ref: INTEGRATION_REF.into(),
            master_oid,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state,
            block_reasons,
            active_configuration_digest: config.digest.clone(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: config.remote.enabled,
        };
        store.initialize_repository_with_configuration(
            &state,
            &config.canonical_bytes()?,
            &config.step_graph_digest,
        )?;
        let runtime = self
            .make_runtime(git, store, state, config, ownership_lock)
            .await?;
        self.runtimes
            .write()
            .await
            .insert(repository_id, Arc::clone(&runtime));
        self.reconfigure_global_scheduler().await;
        self.save_registry().await?;
        if bootstrap
            && runtime.data.lock().state.execution_state == RepositoryExecutionState::Active
        {
            self.check_from_with_purpose(
                repository_id,
                INTEGRATION_REF.into(),
                Some(runtime.git.worktree_root.to_string_lossy().into_owned()),
                CommandId::new(),
                "bootstrap".into(),
                CheckMode::Normal,
            )
            .await?;
        }
        self.repository_snapshot(repository_id).await
    }

    pub async fn register_existing(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
    ) -> Result<RepositorySnapshot, ServiceError> {
        let git = GitRepository::discover(path).await?;
        if let Some(id) = self.registered_common_directory(&git.common_dir).await {
            return self.repository_snapshot(id).await;
        }
        let ownership_lock = acquire_repository_lock(&git.common_dir)?;
        let store_path = git.common_dir.join("tollgate/state.sqlite3");
        if !store_path.exists() {
            return Err(ServiceError::MissingConfiguration);
        }
        let store = self.open_repository_store(&store_path).await?;
        store.full_integrity_check()?;
        let mut state = store.repository_state()?;
        if state.integration_ref == USER_BRANCH_REF {
            git.migrate_integration_ref_from_master().await?;
            state.integration_ref = INTEGRATION_REF.into();
            state
                .block_reasons
                .retain(|reason| reason.code != "master-checked-out");
            if state.execution_state == RepositoryExecutionState::Blocked
                && state.block_reasons.is_empty()
            {
                state.execution_state = RepositoryExecutionState::Active;
            }
            store.update_repository_state(&state)?;
        } else if state.integration_ref != INTEGRATION_REF {
            return Err(ServiceError::Invariant(format!(
                "unsupported integration ref `{}`; expected `{INTEGRATION_REF}`",
                state.integration_ref
            )));
        }
        match git.ensure_integration_not_checked_out().await {
            Ok(()) => {
                state
                    .block_reasons
                    .retain(|reason| reason.code != "release-checked-out");
                if state.execution_state == RepositoryExecutionState::Blocked
                    && state.block_reasons.is_empty()
                {
                    state.execution_state = RepositoryExecutionState::Active;
                }
                store.update_repository_state(&state)?;
            }
            Err(GitError::IntegrationCheckedOut(path)) => {
                state.execution_state = RepositoryExecutionState::Blocked;
                if !state
                    .block_reasons
                    .iter()
                    .any(|reason| reason.code == "release-checked-out")
                {
                    state.block_reasons.push(BlockReason {
                        code: "release-checked-out".into(),
                        message: format!(
                            "Tollgate's integration branch `release` is checked out at {path}"
                        ),
                        recovery_action: "Switch that worktree back to the user-owned `master` branch or another feature branch, then reopen Tollgate.".into(),
                    });
                }
                store.update_repository_state(&state)?;
            }
            Err(error) => return Err(error.into()),
        }
        prepare_configuration_root(&git).await?;
        let disk_config_text = read_or_migrate_configuration(&git, None).await?;
        let disk_config = EffectiveConfig::parse(&disk_config_text)?;
        let config = if disk_config.digest == state.active_configuration_digest {
            if store
                .configuration_snapshot(&state.active_configuration_digest)?
                .is_none()
            {
                store.record_initial_configuration(
                    &disk_config.digest,
                    &disk_config.canonical_bytes()?,
                    &disk_config.step_graph_digest,
                )?;
            }
            mirror_legacy_configuration(&git, &disk_config_text).await?;
            disk_config
        } else {
            state.execution_state = RepositoryExecutionState::ConfigurationPending;
            store.update_repository_state(&state)?;
            let (canonical, step_graph_digest) = store
                .configuration_snapshot(&state.active_configuration_digest)?
                .ok_or_else(|| {
                    ServiceError::Invariant(
                        "active configuration snapshot is missing from durable state".into(),
                    )
                })?;
            EffectiveConfig::restore_canonical(
                &canonical,
                state.active_configuration_digest.clone(),
                step_graph_digest,
            )?
        };
        if let Some(error) = self.environment_error.read().await.clone() {
            state.execution_state = RepositoryExecutionState::Blocked;
            if !state
                .block_reasons
                .iter()
                .any(|reason| reason.code == "environment-bootstrap-failed")
            {
                state.block_reasons.push(BlockReason {
                    code: "environment-bootstrap-failed".into(),
                    message: format!("Login-shell environment capture failed: {error}"),
                    recovery_action: "Repair the login shell, then reload the environment from Doctor or `tg env reload`.".into(),
                });
            }
            store.update_repository_state(&state)?;
        }
        let id = state.id;
        let runtime = self
            .make_runtime(git, store, state, config, ownership_lock)
            .await?;
        self.runtimes.write().await.insert(id, Arc::clone(&runtime));
        self.unavailable
            .write()
            .await
            .retain(|entry| entry.id != id);
        self.reconfigure_global_scheduler().await;
        self.reconcile_seed_intents(&runtime).await?;
        self.reconcile_backup_intents(&runtime).await?;
        self.reconcile_cache_purge_intents(&runtime).await?;
        self.reconcile_artifact_intents(&runtime).await?;
        self.reconcile_promotion_intent(&runtime).await?;
        self.reconcile_remote_intents(&runtime).await?;
        self.reconcile_master(&runtime).await?;
        self.reconcile_approval_intents(&runtime).await?;
        let _resumable = self.recover_interrupted(&runtime)?;
        self.reconcile_mutation_intents(&runtime).await?;
        self.reconcile_failed_prefixes(id, &runtime).await?;
        self.recover_cold_retry_policy(&runtime)?;
        self.prune_expired_artifacts(id).await?;
        self.enforce_log_retention(&runtime).await?;
        if runtime.data.lock().state.execution_state == RepositoryExecutionState::Active {
            self.spawn_eligible(id, &runtime);
            self.promote_ready(id).await?;
        }
        self.repository_snapshot(id).await
    }

    async fn reconcile_mutation_intents(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, kind, evidence, intent_state) in
            runtime.store.recoverable_operations(&[
                "cancel",
                "pause",
                "resume",
                "config-apply",
                "config-regenerate",
                "worktree-create",
                "worktree-remove",
                "update",
                "slot-reset",
                "cleanup",
                "user-master-sync",
                "reconcile",
            ])?
        {
            if intent_state == IntentState::NeedsAttention
                && !matches!(kind.as_str(), "worktree-remove" | "cleanup")
            {
                continue;
            }
            match kind.as_str() {
                "config-regenerate" => {
                    self.reconcile_config_regeneration(runtime, command_id, &evidence)
                        .await?;
                    continue;
                }
                "worktree-create" | "worktree-remove" | "update" => {
                    self.reconcile_worktree_mutation(
                        runtime,
                        command_id,
                        &kind,
                        &evidence,
                        intent_state,
                    )
                    .await?;
                    continue;
                }
                "slot-reset" => {
                    self.reconcile_slot_reset(runtime, command_id, &evidence)
                        .await?;
                    continue;
                }
                "cleanup" => {
                    self.reconcile_source_cleanup(runtime, command_id, &evidence, intent_state)
                        .await?;
                    continue;
                }
                "user-master-sync" => {
                    let request_digest = evidence
                        .get("request_digest")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            ServiceError::Invariant(
                                "user master sync intent omitted request digest".into(),
                            )
                        })?;
                    let item_id = evidence
                        .get("item_id")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?
                        .flatten();
                    let tested_oid: GitOid = serde_json::from_value(
                        evidence.get("tested_oid").cloned().ok_or_else(|| {
                            ServiceError::Invariant(
                                "user master sync intent omitted tested OID".into(),
                            )
                        })?,
                    )?;
                    let replace_tip = evidence
                        .get("replace_tip")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?
                        .flatten()
                        .or_else(|| {
                            item_id.and_then(|id| {
                                runtime
                                    .data
                                    .lock()
                                    .items
                                    .iter()
                                    .find(|item| {
                                        item.id == id
                                            && item.metadata.branch.as_deref() == Some(USER_BRANCH)
                                    })
                                    .map(|item| item.source_oid.clone())
                            })
                        });
                    let remote_oid = if evidence.get("remote_oid").is_some() {
                        evidence
                            .get("remote_oid")
                            .cloned()
                            .map(serde_json::from_value)
                            .transpose()?
                            .flatten()
                    } else {
                        Some(tested_oid.clone())
                    };
                    let rebase_unsubmitted = evidence
                        .get("rebase_unsubmitted")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    self.complete_user_master_sync(
                        runtime,
                        command_id,
                        request_digest,
                        item_id,
                        UserMasterSyncProjection {
                            tested_oid: &tested_oid,
                            replace_tip: replace_tip.as_ref(),
                            remote_oid: remote_oid.as_ref(),
                            rebase_unsubmitted,
                        },
                        Actor::Recovery,
                    )
                    .await?;
                    continue;
                }
                "config-apply" => {
                    self.reconcile_configuration_apply(runtime, command_id, &evidence)
                        .await?;
                    continue;
                }
                "reconcile" => {
                    self.recover_reconciliation(runtime, command_id, &evidence)
                        .await?;
                    continue;
                }
                _ => {}
            }
            if kind != "cancel" {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "prepared-state-change-had-no-durable-effect"}),
                )?;
                continue;
            }
            let item_id: QueueItemId =
                serde_json::from_value(evidence.get("item_id").cloned().ok_or_else(|| {
                    ServiceError::Invariant("cancel intent omitted item ID".into())
                })?)?;
            let request_digest = evidence
                .get("request_digest")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ServiceError::Invariant("cancel intent omitted digest".into()))?;
            let item = runtime
                .data
                .lock()
                .items
                .iter()
                .find(|item| item.id == item_id)
                .cloned();
            let Some(item) = item else {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"recovery": "cancel-item-absent"}),
                )?;
                continue;
            };
            if item.state != QueueItemState::Canceled {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "cancel-projection-not-applied"}),
                )?;
                continue;
            }
            if item.kind == QueueItemKind::IndependentCheck {
                if runtime
                    .git
                    .optional_ref_oid(&item.source_ref)
                    .await?
                    .as_ref()
                    == Some(&item.source_oid)
                {
                    runtime
                        .git
                        .delete_source_ref(&item.source_ref, &item.source_oid)
                        .await?;
                }
            } else {
                self.rebuild_after_failure(item.repository_id, item.id)
                    .await?;
            }
            let mut state = runtime.data.lock().state.clone();
            let result = MutationResult {
                repository_id: item.repository_id,
                action: "cancel".into(),
                message: "Recovered a completed cancellation after restart.".into(),
            };
            let event = runtime.store.complete_operation(
                &state,
                "cancel",
                command_id,
                "cancel",
                request_digest,
                &result,
                "item.cancel-command-recovered",
                &serde_json::json!({"item_id": item.id, "state": "canceled"}),
                Actor::Recovery,
            )?;
            state.event_sequence = event.sequence;
            runtime.data.lock().state = state;
            let _ = runtime.events.send(event);
        }
        self.clear_worktree_removal_block_if_resolved(runtime)?;
        Ok(())
    }

    async fn recover_reconciliation(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
    ) -> Result<(), ServiceError> {
        let observed: GitOid =
            serde_json::from_value(evidence.get("observed_master").cloned().ok_or_else(|| {
                ServiceError::Invariant("reconcile omitted observed master".into())
            })?)?;
        if runtime.git.integration_oid().await? != observed {
            runtime.store.set_intent_state(
                command_id,
                IntentState::NeedsAttention,
                &serde_json::json!({"recovery": "reconcile-master-changed-again"}),
            )?;
            return Ok(());
        }
        let persisted: GitOid =
            serde_json::from_value(evidence.get("persisted_master").cloned().ok_or_else(
                || ServiceError::Invariant("reconcile omitted persisted master".into()),
            )?)?;
        let request_digest = evidence
            .get("request_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("reconcile omitted request digest".into()))?;
        let changed_base = persisted != observed;
        let mut state = runtime.data.lock().state.clone();
        if state.master_oid != observed {
            state.master_oid = observed.clone();
            state.queue_revision += 1;
        }
        state.block_reasons.retain(|reason| {
            !matches!(
                reason.code.as_str(),
                "external-master-movement"
                    | "remote-diverged"
                    | "remote-preflight-mismatch"
                    | "ambiguous-pull-recovery"
                    | "ambiguous-push-recovery"
            )
        });
        state.execution_state = if state.block_reasons.is_empty() {
            RepositoryExecutionState::Active
        } else {
            RepositoryExecutionState::Blocked
        };
        runtime.store.update_repository_state(&state)?;
        runtime.data.lock().state = state;
        let pending = runtime
            .data
            .lock()
            .items
            .iter()
            .filter(|item| item.state == QueueItemState::PromotedLocalPushPending)
            .cloned()
            .collect::<Vec<_>>();
        for mut item in pending {
            item.state = item
                .state
                .transition(ItemEvent::PushAbandoned)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.remote_state = RemoteState::Abandoned;
            item.terminal_reason = Some("remote-promise-abandoned-by-reconcile".into());
            self.replace_item(runtime, item.clone())?;
            self.finish_source_cleanup(runtime, item).await?;
        }
        for (intent_id, _, _, _) in runtime.store.unfinished_operations(&["push"])? {
            runtime.store.set_intent_state(
                intent_id,
                IntentState::Canceled,
                &serde_json::json!({"recovery": "abandoned-by-reconcile"}),
            )?;
        }
        let affected = if changed_base {
            self.rebuild_after_base_adoption(runtime, &observed).await?
        } else {
            Vec::new()
        };
        let mut completed_state = runtime.data.lock().state.clone();
        let result = RemoteSyncResult {
            action: RemoteSyncAction::ReconciledLocal,
            local_master: observed.clone(),
            remote_master: None,
            queue_revision: completed_state.queue_revision,
            affected_item_ids: affected,
            message: "Recovered the exact reconciliation projection after restart.".into(),
        };
        let event = runtime.store.complete_operation(
            &completed_state,
            "reconcile",
            command_id,
            "reconcile",
            request_digest,
            &result,
            "repository.reconcile-recovered",
            &serde_json::json!({"adopted_master": observed}),
            Actor::Recovery,
        )?;
        completed_state.event_sequence = event.sequence;
        runtime.data.lock().state = completed_state;
        let _ = runtime.events.send(event);
        self.reconcile_approval_intents(runtime).await?;
        Ok(())
    }

    async fn reconcile_configuration_apply(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
    ) -> Result<(), ServiceError> {
        let candidate_digest = evidence
            .get("candidate_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ServiceError::Invariant("config apply omitted candidate digest".into())
            })?;
        if runtime.data.lock().state.active_configuration_digest != candidate_digest {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"recovery": "configuration-activation-not-applied"}),
            )?;
            return Ok(());
        }
        let request_digest = evidence
            .get("request_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("config apply omitted request digest".into()))?;
        let state = runtime.data.lock().state.clone();
        self.rebuild_after_base_adoption(runtime, &state.master_oid)
            .await?;
        let independent = runtime
            .data
            .lock()
            .items
            .iter()
            .filter(|item| {
                item.kind == QueueItemKind::IndependentCheck
                    && is_rebuildable_gate_state(item.state)
            })
            .cloned()
            .collect::<Vec<_>>();
        let config = runtime.data.lock().config.clone();
        for mut item in independent {
            let parent = runtime.git.commit_parent_oid(&item.source_oid).await?;
            let generation = ValidationGeneration::derive(
                ValidationGenerationId::new(),
                item.id,
                parent.clone(),
                vec![item.id],
                vec![item.source_oid.clone()],
                vec![item.source_oid.clone()],
                parent,
                item.source_oid.clone(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
                state.engine_epoch,
            );
            if item.state != QueueItemState::Constructing {
                item.state = item
                    .state
                    .transition(ItemEvent::InputsChanged)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            }
            item.current_generation_id = Some(generation.id);
            item.buildset_id = None;
            item.certificate_id = None;
            item.state = item
                .state
                .transition(ItemEvent::GenerationPrepared)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            runtime.store.replace_generation(&generation)?;
            runtime.data.lock().push_generation(generation);
            self.replace_item(runtime, item)?;
        }
        let mut completed_state = runtime.data.lock().state.clone();
        let result = MutationResult {
            repository_id: completed_state.id,
            action: "config-apply".into(),
            message: format!(
                "Recovered configuration {} activation.",
                &candidate_digest[..12]
            ),
        };
        let event = runtime.store.complete_operation(
            &completed_state,
            "config-apply",
            command_id,
            "config-apply",
            request_digest,
            &result,
            "configuration.activation-recovered",
            &serde_json::json!({"digest": candidate_digest}),
            Actor::Recovery,
        )?;
        completed_state.event_sequence = event.sequence;
        runtime.data.lock().state = completed_state;
        let _ = runtime.events.send(event);
        Ok(())
    }

    fn recover_cold_retry_policy(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        let mut recoveries = Vec::new();
        for (_, _, evidence, _) in runtime.store.unfinished_operations(&["retry"])? {
            let result = evidence
                .get("child_command_id")
                .cloned()
                .and_then(|value| serde_json::from_value::<CommandId>(value).ok())
                .map(|command_id| runtime.store.command_response_json(command_id))
                .transpose()?
                .flatten();
            recoveries.push((evidence, result, false));
        }
        recoveries.extend(
            runtime
                .store
                .completed_operation_records("retry")?
                .into_iter()
                .map(|(expected, observed)| (expected, Some(observed), true)),
        );
        let data = runtime.data.lock();
        let mut cold_items = runtime.cold_items.lock();
        for (retry, result, completed_wrapper) in recoveries {
            if retry.get("cold").and_then(serde_json::Value::as_bool) != Some(true) {
                continue;
            }
            let Some(retried_id) = result
                .as_ref()
                .and_then(|value| {
                    value.get(if completed_wrapper {
                        "result_item_id"
                    } else {
                        "item_id"
                    })
                })
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse::<QueueItemId>().ok())
            else {
                continue;
            };
            let nonterminal = data
                .items
                .iter()
                .any(|item| item.id == retried_id && !item.state.is_terminal());
            let conclusive_attempt_exists = data.buildsets.iter().any(|buildset| {
                buildset.item_id == retried_id
                    && matches!(
                        buildset.state,
                        BuildsetState::Passed
                            | BuildsetState::PassedWithWarnings
                            | BuildsetState::Failed
                    )
            });
            if nonterminal && !conclusive_attempt_exists {
                cold_items.insert(retried_id);
            }
        }
        for item in data.items.iter().filter(|item| {
            item.metadata
                .purpose
                .as_deref()
                .is_some_and(|purpose| purpose.starts_with("diagnose:"))
                && !item.state.is_terminal()
        }) {
            let conclusive_attempt_exists = data.buildsets.iter().any(|buildset| {
                buildset.item_id == item.id
                    && matches!(
                        buildset.state,
                        BuildsetState::Passed
                            | BuildsetState::PassedWithWarnings
                            | BuildsetState::Failed
                    )
            });
            if !conclusive_attempt_exists {
                cold_items.insert(item.id);
            }
        }
        Ok(())
    }

    fn mark_ambiguous_mutation_recovery(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        code: &str,
        message: String,
    ) -> Result<(), ServiceError> {
        runtime.store.set_intent_state(
            command_id,
            IntentState::NeedsAttention,
            &serde_json::json!({"recovery": code, "message": message}),
        )?;
        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            if !data
                .state
                .block_reasons
                .iter()
                .any(|reason| reason.code == code)
            {
                data.state.block_reasons.push(BlockReason {
                    code: code.into(),
                    message,
                    recovery_action: if code == "worktree-remove-ambiguous" {
                        "Preserve the path. Restore its prepared identity or move a replacement aside, then restart Tollgate; exact worktree evidence will be reconciled automatically.".into()
                    } else {
                        "Preserve the affected paths and inspect the prepared operation evidence before recovery.".into()
                    },
                });
            }
            data.state.clone()
        };
        runtime.store.update_repository_state(&state)?;
        Ok(())
    }

    fn cancel_recovery_intent(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        intent_state: IntentState,
        reason: &str,
    ) -> Result<(), ServiceError> {
        let evidence = serde_json::json!({"recovery": reason});
        if intent_state == IntentState::NeedsAttention {
            runtime
                .store
                .cancel_attention_intent(command_id, &evidence)?;
        } else {
            runtime
                .store
                .set_intent_state(command_id, IntentState::Canceled, &evidence)?;
        }
        self.clear_worktree_removal_block_if_resolved(runtime)
    }

    fn clear_worktree_removal_block_if_resolved(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        let unresolved = runtime
            .store
            .recoverable_operations(&["worktree-remove"])?
            .into_iter()
            .any(|(_, _, _, state)| state == IntentState::NeedsAttention);
        if unresolved {
            return Ok(());
        }
        let state = {
            let mut data = runtime.data.lock();
            data.state
                .block_reasons
                .retain(|reason| reason.code != "worktree-remove-ambiguous");
            if data.state.execution_state == RepositoryExecutionState::Blocked
                && data.state.block_reasons.is_empty()
            {
                data.state.execution_state = RepositoryExecutionState::Active;
            }
            data.state.clone()
        };
        runtime.store.update_repository_state(&state)?;
        Ok(())
    }

    async fn observe_worktree_removal(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        evidence: &serde_json::Value,
        path: &Path,
        branch: &str,
        expected: &GitOid,
    ) -> Result<WorktreeRemovalObservation, String> {
        if !path.is_absolute() {
            return Err("Prepared worktree path is not absolute".into());
        }
        if matches!(branch, USER_BRANCH | INTEGRATION_BRANCH) {
            return Err("Prepared removal targets a protected master or release branch".into());
        }
        if let Some(common_dir) = evidence.get("common_dir") {
            let common_dir: PathBuf = serde_json::from_value(common_dir.clone())
                .map_err(|error| format!("Prepared common directory is malformed: {error}"))?;
            if common_dir != runtime.git.common_dir {
                return Err(
                    "Prepared removal belongs to a different repository common directory".into(),
                );
            }
        }
        let expected_identity: Option<FileIdentity> = evidence
            .get("path_identity")
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| format!("Prepared path identity is malformed: {error}"))?;
        let registration = runtime
            .git
            .registered_worktree(path)
            .await
            .map_err(|error| error.to_string())?;
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(format!("Prepared path cannot be inspected: {error}")),
        };
        if let Some(registration) = registration {
            let Some(metadata) = metadata else {
                return Err("Worktree is still registered but its path is absent".into());
            };
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err("Registered worktree path was replaced with an unsafe entry".into());
            }
            let identity =
                GitRepository::directory_identity(path).map_err(|error| error.to_string())?;
            if expected_identity.is_some_and(|expected| expected != identity) {
                return Err("Registered worktree path identity changed after preparation".into());
            }
            let canonical = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
            if registration.path != canonical
                || registration.branch.as_deref() != Some(branch)
                || registration.head != *expected
            {
                return Err(
                    "Registered worktree path, branch, or OID differs from prepared evidence"
                        .into(),
                );
            }
            let discovered = GitRepository::discover(path)
                .await
                .map_err(|error| error.to_string())?;
            if discovered.worktree_root != canonical
                || discovered.common_dir != runtime.git.common_dir
                || discovered
                    .current_branch()
                    .await
                    .map_err(|error| error.to_string())?
                    .as_deref()
                    != Some(branch)
                || discovered
                    .resolve_oid("HEAD")
                    .await
                    .map_err(|error| error.to_string())?
                    != *expected
            {
                return Err("Exact linked worktree identity no longer matches its evidence".into());
            }
            return Ok(WorktreeRemovalObservation::Intact);
        }

        match metadata {
            None => Ok(WorktreeRemovalObservation::Removed),
            Some(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(
                        "Unregistered worktree path was replaced with an unsafe entry".into(),
                    );
                }
                let identity =
                    GitRepository::directory_identity(path).map_err(|error| error.to_string())?;
                if expected_identity.is_some_and(|expected| expected != identity) {
                    return Err("Unregistered worktree path was replaced after preparation".into());
                }
                if std::fs::symlink_metadata(path.join(".git")).is_ok() {
                    return Err(
                        "Unregistered path now contains repository identity metadata".into(),
                    );
                }
                Ok(WorktreeRemovalObservation::Residual)
            }
        }
    }

    async fn finish_unregistered_worktree_branch(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        branch: &str,
        expected: &GitOid,
    ) -> Result<(), ServiceError> {
        let branch_ref = format!("refs/heads/{branch}");
        if let Some(observed) = runtime.git.optional_ref_oid(&branch_ref).await? {
            if observed != *expected {
                return Err(ServiceError::Invariant(
                    "Removed worktree's branch moved after preparation".into(),
                ));
            }
            runtime.git.delete_source_ref(&branch_ref, expected).await?;
        }
        Ok(())
    }

    async fn reconcile_config_regeneration(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
    ) -> Result<(), ServiceError> {
        let path: PathBuf =
            serde_json::from_value(evidence.get("path").cloned().ok_or_else(|| {
                ServiceError::Invariant("config regeneration omitted path".into())
            })?)?;
        let expected_path = runtime.git.worktree_root.join(".tollgate/config.toml");
        if path != expected_path {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "config-regeneration-path-mismatch",
                format!(
                    "Prepared configuration path {} is not authoritative",
                    path.display()
                ),
            );
        }
        let request_digest = evidence
            .get("request_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("config regeneration omitted digest".into()))?;
        let old_hash = evidence
            .get("old_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ServiceError::Invariant("config regeneration omitted old hash".into())
            })?;
        let new_digest = evidence
            .get("new_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ServiceError::Invariant("config regeneration omitted new digest".into())
            })?;
        let bytes = tokio::fs::read(&path).await?;
        if blake3::hash(&bytes).to_hex().as_str() == old_hash {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"recovery": "configuration-rename-not-applied"}),
            )?;
            return Ok(());
        }
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "config-regeneration-ambiguous",
                "Regenerated configuration is not valid UTF-8".into(),
            );
        };
        let Ok(candidate) = EffectiveConfig::parse(text) else {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "config-regeneration-ambiguous",
                "Regenerated configuration does not parse under the trusted schema".into(),
            );
        };
        if candidate.digest != new_digest {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "config-regeneration-ambiguous",
                "Configuration bytes differ from both the old and prepared new policy".into(),
            );
        }
        let mut state = runtime.data.lock().state.clone();
        state.execution_state = if candidate.digest == state.active_configuration_digest {
            if state.block_reasons.is_empty() {
                RepositoryExecutionState::Active
            } else {
                RepositoryExecutionState::Blocked
            }
        } else {
            RepositoryExecutionState::ConfigurationPending
        };
        let event = runtime.store.complete_operation(
            &state,
            "config-regenerate",
            command_id,
            "config-regenerate",
            request_digest,
            &candidate,
            "configuration.regenerated-recovered",
            &serde_json::json!({"path": path, "digest": candidate.digest, "recovery": "exact-bytes-verified"}),
            Actor::Recovery,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(())
    }

    async fn reconcile_worktree_mutation(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        kind: &str,
        evidence: &serde_json::Value,
        intent_state: IntentState,
    ) -> Result<(), ServiceError> {
        let request_digest = evidence
            .get("request_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("worktree intent omitted digest".into()))?;
        let path: PathBuf = serde_json::from_value(
            evidence
                .get(if kind == "worktree-create" {
                    "destination"
                } else {
                    "path"
                })
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("worktree intent omitted path".into()))?,
        )?;
        if kind == "worktree-create" {
            cleanup_worktree_hydration_staging(runtime, command_id)?;
            if !tokio::fs::try_exists(&path).await? {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "worktree-create-not-applied"}),
                )?;
                return Ok(());
            }
            let branch = evidence
                .get("branch")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ServiceError::Invariant("worktree create omitted branch".into()))?;
            let base: GitOid =
                serde_json::from_value(evidence.get("base").cloned().ok_or_else(|| {
                    ServiceError::Invariant("worktree create omitted base".into())
                })?)?;
            let observed = match GitRepository::discover(&path).await {
                Ok(observed) => observed,
                Err(error) => {
                    return self.mark_ambiguous_mutation_recovery(
                        runtime,
                        command_id,
                        "worktree-create-ambiguous",
                        format!("Created path cannot be identified as a worktree: {error}"),
                    );
                }
            };
            if observed.common_dir != runtime.git.common_dir
                || observed.current_branch().await?.as_deref() != Some(branch)
                || observed.resolve_oid("HEAD").await? != base
            {
                return self.mark_ambiguous_mutation_recovery(
                    runtime,
                    command_id,
                    "worktree-create-ambiguous",
                    "Created worktree identity, branch, or exact OID differs from prepared evidence"
                        .into(),
                );
            }
            let result = WorktreeOperationResult {
                action: "created".into(),
                path: path.to_string_lossy().into_owned(),
                branch: Some(branch.into()),
                old_oid: None,
                new_oid: Some(base.clone()),
                message: format!(
                    "Recovered a feature worktree created from gated release {}.",
                    base.short()
                ),
            };
            return self.complete_recovered_worktree_operation(
                runtime,
                command_id,
                kind,
                request_digest,
                result,
            );
        }

        if kind == "worktree-remove" {
            let branch = evidence
                .get("branch")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ServiceError::Invariant("worktree removal omitted branch".into()))?;
            let expected: GitOid =
                serde_json::from_value(evidence.get("expected_oid").cloned().ok_or_else(
                    || ServiceError::Invariant("worktree removal omitted OID".into()),
                )?)?;
            if matches!(branch, USER_BRANCH | INTEGRATION_BRANCH) {
                return self.cancel_recovery_intent(
                    runtime,
                    command_id,
                    intent_state,
                    "canceled-malformed-protected-worktree-removal",
                );
            }
            let observation = match self
                .observe_worktree_removal(runtime, evidence, &path, branch, &expected)
                .await
            {
                Ok(observation) => observation,
                Err(message) => {
                    return self.mark_ambiguous_mutation_recovery(
                        runtime,
                        command_id,
                        "worktree-remove-ambiguous",
                        message,
                    );
                }
            };
            if observation == WorktreeRemovalObservation::Intact {
                return self.cancel_recovery_intent(
                    runtime,
                    command_id,
                    intent_state,
                    "worktree-remove-not-applied",
                );
            }
            if let Err(error) = self
                .finish_unregistered_worktree_branch(runtime, branch, &expected)
                .await
            {
                return self.mark_ambiguous_mutation_recovery(
                    runtime,
                    command_id,
                    "worktree-remove-ambiguous",
                    error.to_string(),
                );
            }
            let result = WorktreeOperationResult {
                action: "removed".into(),
                path: path.to_string_lossy().into_owned(),
                branch: Some(branch.into()),
                old_oid: Some(expected),
                new_oid: None,
                message: if observation == WorktreeRemovalObservation::Residual {
                    "Recovered a verified Git worktree removal; unregistered residual files were preserved."
                        .into()
                } else {
                    "Recovered a verified worktree removal after restart.".into()
                },
            };
            self.complete_recovered_worktree_operation(
                runtime,
                command_id,
                kind,
                request_digest,
                result,
            )?;
            return self.clear_worktree_removal_block_if_resolved(runtime);
        }

        let old: GitOid =
            serde_json::from_value(evidence.get("old_oid").cloned().ok_or_else(|| {
                ServiceError::Invariant("worktree update omitted old OID".into())
            })?)?;
        let master: GitOid =
            serde_json::from_value(evidence.get("master").cloned().ok_or_else(|| {
                ServiceError::Invariant("worktree update omitted master".into())
            })?)?;
        if !tokio::fs::try_exists(&path).await? {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "worktree-update-ambiguous",
                "Prepared feature worktree disappeared during update".into(),
            );
        }
        let worktree = GitRepository::discover(&path).await?;
        if worktree.common_dir != runtime.git.common_dir {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "worktree-update-ambiguous",
                "Prepared feature path now belongs to another repository".into(),
            );
        }
        let current = worktree.resolve_oid("HEAD").await?;
        if current == old {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"recovery": "worktree-update-not-applied"}),
            )?;
            return Ok(());
        }
        let branch = worktree.current_branch().await?;
        let exact_branch = match branch.as_deref() {
            Some(branch) if branch != "master" => {
                runtime
                    .git
                    .optional_ref_oid(&format!("refs/heads/{branch}"))
                    .await?
                    .as_ref()
                    == Some(&current)
            }
            _ => false,
        };
        if worktree.commit_parent_oid(&current).await? != master || !exact_branch {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "worktree-update-ambiguous",
                "Updated feature commit does not have the prepared gated release and exact branch identity"
                    .into(),
            );
        }
        let result = WorktreeOperationResult {
            action: "updated".into(),
            path: path.to_string_lossy().into_owned(),
            branch,
            old_oid: Some(old),
            new_oid: Some(current),
            message: "Recovered a verified one-commit feature update after restart.".into(),
        };
        self.complete_recovered_worktree_operation(
            runtime,
            command_id,
            kind,
            request_digest,
            result,
        )
    }

    fn complete_recovered_worktree_operation(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        kind: &str,
        request_digest: &str,
        result: WorktreeOperationResult,
    ) -> Result<(), ServiceError> {
        let mut state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &state,
            kind,
            command_id,
            kind,
            request_digest,
            &result,
            &format!("{kind}.recovered"),
            &serde_json::json!({"result": result, "recovery": "exact-state-verified"}),
            Actor::Recovery,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(())
    }

    async fn reconcile_slot_reset(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
    ) -> Result<(), ServiceError> {
        let request_digest = evidence
            .get("request_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("slot reset omitted digest".into()))?;
        let slot: SlotView = serde_json::from_value(
            evidence
                .get("slot")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("slot reset omitted slot".into()))?,
        )?;
        let quarantine: PathBuf = serde_json::from_value(
            evidence
                .get("quarantine")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("slot reset omitted quarantine".into()))?,
        )?;
        let checkout: GitOid = serde_json::from_value(
            evidence
                .get("checkout")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("slot reset omitted checkout".into()))?,
        )?;
        let slot_path = PathBuf::from(&slot.path);
        let slot_exists = tokio::fs::try_exists(&slot_path).await?;
        let quarantine_exists = tokio::fs::try_exists(&quarantine).await?;
        if !slot_exists {
            if !quarantine_exists {
                return self.mark_ambiguous_mutation_recovery(
                    runtime,
                    command_id,
                    "slot-reset-ambiguous",
                    "Both the slot and its prepared quarantine evidence are absent".into(),
                );
            }
            runtime.git.initialize_mirror(&runtime.mirror).await?;
            runtime
                .git
                .provision_slot(&runtime.mirror, &slot_path, &checkout)
                .await?;
        }
        if let Err(error) = verify_slot_checkout(runtime, &slot_path, &checkout).await {
            return self.mark_ambiguous_mutation_recovery(
                runtime,
                command_id,
                "slot-reset-ambiguous",
                format!("Recovered slot does not match its exact checkout: {error}"),
            );
        }
        let reset = SlotView {
            id: slot.id,
            path: slot.path,
            state: "idle".into(),
            checkout_oid: Some(checkout),
            health: "healthy".into(),
            last_used: Some(OffsetDateTime::now_utc()),
        };
        let mut state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &state,
            "slot-reset",
            command_id,
            "slot-reset",
            request_digest,
            &reset,
            "slot.reset-recovered",
            &serde_json::json!({"slot": reset, "quarantine": quarantine, "recovery": "exact-checkout-verified"}),
            Actor::Recovery,
        )?;
        state.event_sequence = event.sequence;
        {
            let mut data = runtime.data.lock();
            data.state = state;
            data.slots.insert(reset.id, reset);
        }
        let _ = runtime.events.send(event);
        if quarantine_exists {
            let cache_root = runtime
                .slots_root
                .parent()
                .ok_or_else(|| ServiceError::Invariant("slot root has no cache parent".into()))?;
            remove_owned_quarantine(cache_root, &quarantine)?;
        }
        Ok(())
    }

    async fn reconcile_source_cleanup(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
        _intent_state: IntentState,
    ) -> Result<(), ServiceError> {
        let item_id: QueueItemId = serde_json::from_value(
            evidence
                .get("item_id")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("cleanup omitted item".into()))?,
        )?;
        let path: PathBuf = serde_json::from_value(
            evidence
                .get("path")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("cleanup omitted path".into()))?,
        )?;
        let branch = evidence
            .get("branch")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("cleanup omitted branch".into()))?;
        let expected_oid: GitOid = serde_json::from_value(
            evidence
                .get("expected_oid")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("cleanup omitted OID".into()))?,
        )?;
        let observation = match self
            .observe_worktree_removal(runtime, evidence, &path, branch, &expected_oid)
            .await
        {
            Ok(observation) => observation,
            Err(message) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"recovery": "cleanup-worktree-mismatch", "message": message}),
                )?;
                return self.set_cleanup_attention(runtime, item_id);
            }
        };
        if observation == WorktreeRemovalObservation::Intact {
            let worktree = GitRepository::discover(&path).await?;
            if let Err(error) = worktree.ensure_clean().await {
                if matches!(error, GitError::DirtyWorktree(_)) {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::NeedsAttention,
                        &serde_json::json!({"recovery": "cleanup-worktree-dirty"}),
                    )?;
                    return self.set_cleanup_attention(runtime, item_id);
                }
                return Err(error.into());
            }
            runtime
                .git
                .cleanup_linked_source_worktree(&path, branch, &expected_oid)
                .await?;
        } else if let Err(error) = self
            .finish_unregistered_worktree_branch(runtime, branch, &expected_oid)
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::NeedsAttention,
                &serde_json::json!({"recovery": "cleanup-branch-moved", "error": error.to_string()}),
            )?;
            return self.set_cleanup_attention(runtime, item_id);
        }
        self.complete_source_cleanup(runtime, item_id, command_id, evidence, Actor::Recovery)
    }

    fn set_cleanup_attention(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item_id: QueueItemId,
    ) -> Result<(), ServiceError> {
        let mut item = runtime
            .data
            .lock()
            .items
            .iter()
            .find(|item| item.id == item_id)
            .cloned()
            .ok_or(ServiceError::ItemNotFound(item_id))?;
        item.cleanup_state = CleanupState::NeedsAttention;
        self.replace_item(runtime, item)
    }

    fn complete_source_cleanup(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item_id: QueueItemId,
        command_id: CommandId,
        evidence: &serde_json::Value,
        actor: Actor,
    ) -> Result<(), ServiceError> {
        let request_digest = evidence
            .get("request_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ServiceError::Invariant("cleanup omitted digest".into()))?;
        let mut item = runtime
            .data
            .lock()
            .items
            .iter()
            .find(|item| item.id == item_id)
            .cloned()
            .ok_or(ServiceError::ItemNotFound(item_id))?;
        item.cleanup_state = CleanupState::Completed;
        self.replace_item(runtime, item)?;
        let mut state = runtime.data.lock().state.clone();
        let result = MutationResult {
            repository_id: state.id,
            action: "cleanup".into(),
            message: "Removed the exact clean source worktree and unchanged branch.".into(),
        };
        let event = runtime.store.complete_operation(
            &state,
            "cleanup",
            command_id,
            "cleanup",
            request_digest,
            &result,
            "source.cleanup-completed",
            &serde_json::json!({"item_id": item_id, "verified": true}),
            actor,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(())
    }

    async fn reconcile_artifact_intents(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, _, evidence, _) in runtime.store.unfinished_operations(&["artifact"])? {
            let evidence: ArtifactRetentionEvidence = serde_json::from_value(evidence)?;
            if evidence.repository_id != runtime.data.lock().state.id {
                return Err(ServiceError::Invariant(
                    "artifact intent belongs to another repository".into(),
                ));
            }
            let artifacts_root = runtime.git.common_dir.join("tollgate/artifacts");
            ensure_owned_artifact_path(&artifacts_root, &evidence.staging_dir)?;
            ensure_owned_artifact_path(&artifacts_root, &evidence.destination_dir)?;
            let staging_exists = tokio::fs::try_exists(&evidence.staging_dir).await?;
            let destination_exists = tokio::fs::try_exists(&evidence.destination_dir).await?;
            if destination_exists {
                verify_artifact_publication(&artifacts_root, &evidence).await?;
            } else if staging_exists {
                if verify_artifact_staging(&evidence).await.is_ok() {
                    tokio::fs::rename(&evidence.staging_dir, &evidence.destination_dir).await?;
                    sync_directory(&artifacts_root)?;
                    verify_artifact_publication(&artifacts_root, &evidence).await?;
                } else {
                    tokio::fs::remove_dir_all(&evidence.staging_dir).await?;
                    sync_directory(&artifacts_root)?;
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"recovery": "incomplete-artifact-staging-removed"}),
                    )?;
                    continue;
                }
            } else {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "artifact-publication-never-started"}),
                )?;
                continue;
            }
            let mut state = runtime.data.lock().state.clone();
            let event = runtime.store.complete_artifact_retention(
                &state,
                command_id,
                &evidence.records,
                &serde_json::json!({"recovery": "verified-publication"}),
            )?;
            state.event_sequence = event.sequence;
            runtime.data.lock().state = state;
            let _ = runtime.events.send(event);
        }
        for (command_id, _, evidence, _) in
            runtime.store.unfinished_operations(&["artifact-prune"])?
        {
            let evidence: ArtifactPruneEvidence = serde_json::from_value(evidence)?;
            if evidence.repository_id != runtime.data.lock().state.id {
                return Err(ServiceError::Invariant(
                    "artifact pruning intent belongs to another repository".into(),
                ));
            }
            let artifacts_root = runtime.git.common_dir.join("tollgate/artifacts");
            let original_exists = tokio::fs::try_exists(&evidence.original_path).await?;
            let quarantine_exists = tokio::fs::try_exists(&evidence.quarantine_path).await?;
            match (original_exists, quarantine_exists) {
                (true, false) => {
                    verify_retained_artifact(runtime, &evidence.record).await?;
                    if let Some(parent) = evidence.quarantine_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    tokio::fs::rename(&evidence.original_path, &evidence.quarantine_path).await?;
                    verify_quarantined_artifact(&artifacts_root, &evidence).await?;
                }
                (false, true) => {
                    verify_quarantined_artifact(&artifacts_root, &evidence).await?;
                }
                _ => {
                    let state = {
                        let mut data = runtime.data.lock();
                        data.state.execution_state = RepositoryExecutionState::Blocked;
                        data.state.block_reasons.push(BlockReason {
                            code: "artifact-prune-ambiguous".into(),
                            message: format!(
                                "Artifact pruning evidence for {} is ambiguous",
                                evidence.record.source_path
                            ),
                            recovery_action: "Preserve both paths and reconcile the exact hash recorded in the pruning intent.".into(),
                        });
                        data.state.clone()
                    };
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::NeedsAttention,
                        &serde_json::json!({
                            "original_exists": original_exists,
                            "quarantine_exists": quarantine_exists,
                        }),
                    )?;
                    runtime.store.update_repository_state(&state)?;
                    continue;
                }
            }
            runtime.store.set_intent_state(
                command_id,
                IntentState::ExternalApplied,
                &serde_json::json!({"recovery": "exact-quarantine-verified"}),
            )?;
            let result = MutationResult {
                repository_id: evidence.repository_id,
                action: "artifact-prune".into(),
                message: format!(
                    "Recovered pruning of retained artifact {}.",
                    evidence.record.source_path
                ),
            };
            let mut state = runtime.data.lock().state.clone();
            let event = runtime.store.complete_artifact_state_change(
                &state,
                &evidence.record.artifact_id,
                &["retained", "pinned"],
                "pruned",
                Some("artifact-prune"),
                command_id,
                "artifact-prune",
                &evidence.request_digest,
                &result,
                "artifact.prune-recovered",
                Actor::Recovery,
            )?;
            state.event_sequence = event.sequence;
            runtime.data.lock().state = state;
            let _ = runtime.events.send(event);
            tokio::fs::remove_file(&evidence.quarantine_path).await?;
            if let Some(parent) = evidence.quarantine_path.parent() {
                sync_directory(parent)?;
            }
        }
        for evidence in runtime
            .store
            .completed_operation_evidence("artifact-prune")?
        {
            let evidence: ArtifactPruneEvidence = serde_json::from_value(evidence)?;
            let original_exists = tokio::fs::try_exists(&evidence.original_path).await?;
            let quarantine_exists = tokio::fs::try_exists(&evidence.quarantine_path).await?;
            if original_exists {
                return Err(ServiceError::Invariant(format!(
                    "completed artifact prune {} still has an authoritative source file",
                    evidence.record.artifact_id
                )));
            }
            if quarantine_exists {
                let artifacts_root = runtime.git.common_dir.join("tollgate/artifacts");
                verify_quarantined_artifact(&artifacts_root, &evidence).await?;
                tokio::fs::remove_file(&evidence.quarantine_path).await?;
                if let Some(parent) = evidence.quarantine_path.parent() {
                    sync_directory(parent)?;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_seed_intents(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, _, evidence, _) in
            runtime.store.unfinished_operations(&["cache-snapshot"])?
        {
            let evidence: SeedSnapshotEvidence = serde_json::from_value(evidence)?;
            if evidence.repository_id != runtime.data.lock().state.id {
                return Err(ServiceError::Invariant(
                    "seed publication intent belongs to another repository".into(),
                ));
            }
            let staging_exists = tokio::fs::try_exists(&evidence.staging).await?;
            let destination_exists = tokio::fs::try_exists(&evidence.destination).await?;
            let record = match (staging_exists, destination_exists) {
                (false, false) => {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"recovery": "no-seed-path-created"}),
                    )?;
                    continue;
                }
                (true, false) => {
                    verify_seed_publication(&evidence.staging, &evidence)?;
                    tokio::fs::rename(&evidence.staging, &evidence.destination).await?;
                    if let Some(parent) = evidence.destination.parent() {
                        sync_directory(parent)?;
                    }
                    verify_seed_publication(&evidence.destination, &evidence)?
                }
                (false, true) => verify_seed_publication(&evidence.destination, &evidence)?,
                (true, true) => {
                    let state = {
                        let mut data = runtime.data.lock();
                        data.state.execution_state = RepositoryExecutionState::Blocked;
                        data.state.block_reasons.push(BlockReason {
                            code: "seed-publication-ambiguous".into(),
                            message: format!(
                                "Seed {} has both staging and final generations",
                                evidence.seed_id
                            ),
                            recovery_action: "Preserve both owned paths and compare them with the exact seed intent before choosing one.".into(),
                        });
                        data.state.clone()
                    };
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::NeedsAttention,
                        &serde_json::json!({"recovery": "both-seed-paths-exist"}),
                    )?;
                    runtime.store.update_repository_state(&state)?;
                    continue;
                }
            };
            runtime.store.set_intent_state(
                command_id,
                IntentState::ExternalApplied,
                &serde_json::json!({"recovery": "exact-seed-verified"}),
            )?;
            let result = CacheOperationResult {
                action: "snapshot".into(),
                seed_ids: vec![record.id.clone()],
                slots_reset: Vec::new(),
                logical_bytes: record.logical_size,
                message: format!("Recovered immutable APFS seed {} after restart.", record.id),
            };
            let mut state = runtime.data.lock().state.clone();
            let event = runtime.store.complete_seed_publication(
                &state,
                command_id,
                &evidence.request_digest,
                &record,
                &result,
                Actor::Recovery,
            )?;
            state.event_sequence = event.sequence;
            let mut data = runtime.data.lock();
            data.state = state;
            if !data.seeds.iter().any(|seed| seed.id == record.id) {
                data.seeds.push(record);
            }
            drop(data);
            let _ = runtime.events.send(event);
        }
        Ok(())
    }

    async fn reconcile_backup_intents(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, _, evidence, _) in runtime.store.unfinished_operations(&["backup"])? {
            let evidence: BackupEvidence = serde_json::from_value(evidence)?;
            if evidence.repository_id != runtime.data.lock().state.id {
                return Err(ServiceError::Invariant(
                    "backup intent belongs to another repository".into(),
                ));
            }
            let root = runtime.git.common_dir.join("tollgate/backups");
            std::fs::create_dir_all(&root)?;
            if std::fs::symlink_metadata(&root)?.file_type().is_symlink()
                || evidence.temporary.parent() != Some(root.as_path())
                || evidence.destination.parent() != Some(root.as_path())
            {
                return Err(ServiceError::Invariant(
                    "backup recovery path escaped its owned root".into(),
                ));
            }
            let destination_exists = tokio::fs::try_exists(&evidence.destination).await?;
            let temporary_exists = tokio::fs::try_exists(&evidence.temporary).await?;
            if destination_exists {
                let metadata = std::fs::symlink_metadata(&evidence.destination)?;
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(ServiceError::Invariant(
                        "backup recovery destination is not an owned regular file".into(),
                    ));
                }
                let hash = RepositoryStore::verified_backup_hash(&evidence.destination)?;
                runtime
                    .store
                    .complete_backup(command_id, &evidence.destination, &hash)?;
                if temporary_exists {
                    let metadata = std::fs::symlink_metadata(&evidence.temporary)?;
                    if metadata.file_type().is_symlink() || !metadata.is_file() {
                        return Err(ServiceError::Invariant(
                            "backup recovery temporary path is not an owned regular file".into(),
                        ));
                    }
                    tokio::fs::remove_file(&evidence.temporary).await?;
                }
                sync_directory(&root)?;
                continue;
            }
            if temporary_exists {
                let metadata = std::fs::symlink_metadata(&evidence.temporary)?;
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(ServiceError::Invariant(
                        "backup recovery temporary path is not an owned regular file".into(),
                    ));
                }
                tokio::fs::remove_file(&evidence.temporary).await?;
                sync_directory(&root)?;
            }
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"recovery": "backup-not-published"}),
            )?;
        }
        Ok(())
    }

    async fn reconcile_cache_purge_intents(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, _, evidence, _) in runtime.store.unfinished_operations(&["cache-purge"])? {
            let evidence: CachePurgeEvidence = serde_json::from_value(evidence)?;
            if evidence.repository_id != runtime.data.lock().state.id {
                return Err(ServiceError::Invariant(
                    "cache purge intent belongs to another repository".into(),
                ));
            }
            let cache_root = std::fs::canonicalize(
                runtime
                    .slots_root
                    .parent()
                    .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?,
            )?;
            let mut ambiguous = false;
            for seed in &evidence.seeds {
                let original_exists = seed.original.exists();
                let quarantine_exists = seed.quarantine.exists();
                match (original_exists, quarantine_exists) {
                    (true, false) => {
                        verify_seed_record_at(&seed.original, &seed.record)?;
                        std::fs::rename(&seed.original, &seed.quarantine)?;
                        verify_seed_record_at(&seed.quarantine, &seed.record)?;
                    }
                    (false, true) => verify_seed_record_at(&seed.quarantine, &seed.record)?,
                    _ => ambiguous = true,
                }
            }
            for slot in &evidence.slots {
                let original_exists = slot.slot.path.exists();
                let quarantine_exists = slot.quarantine.exists();
                match (original_exists, quarantine_exists) {
                    (true, false) => {
                        verify_slot_checkout(runtime, &slot.slot.path, &slot.checkout).await?;
                        runtime
                            .git
                            .quarantine_slot(&runtime.mirror, &slot.slot.path, &slot.quarantine)
                            .await?;
                        runtime
                            .git
                            .provision_slot(&runtime.mirror, &slot.slot.path, &slot.checkout)
                            .await?;
                    }
                    (false, true) => {
                        runtime
                            .git
                            .provision_slot(&runtime.mirror, &slot.slot.path, &slot.checkout)
                            .await?;
                    }
                    (true, true) => {
                        verify_slot_checkout(runtime, &slot.slot.path, &slot.checkout).await?;
                    }
                    (false, false) => ambiguous = true,
                }
            }
            if ambiguous {
                let state = {
                    let mut data = runtime.data.lock();
                    data.state.execution_state = RepositoryExecutionState::Blocked;
                    data.state.block_reasons.push(BlockReason {
                        code: "cache-purge-ambiguous".into(),
                        message: "A cache purge has missing or duplicate owned path evidence.".into(),
                        recovery_action: "Preserve cache paths and reconcile each exact seed/slot quarantine against the prepared intent.".into(),
                    });
                    data.state.clone()
                };
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"recovery": "cache-path-evidence-ambiguous"}),
                )?;
                runtime.store.update_repository_state(&state)?;
                continue;
            }
            runtime.store.set_intent_state(
                command_id,
                IntentState::ExternalApplied,
                &serde_json::json!({"recovery": "all-cache-paths-verified"}),
            )?;
            let result = CacheOperationResult {
                action: "purge".into(),
                seed_ids: evidence
                    .seeds
                    .iter()
                    .map(|seed| seed.record.id.clone())
                    .collect(),
                slots_reset: evidence.slots.iter().map(|slot| slot.slot.id).collect(),
                logical_bytes: evidence
                    .seeds
                    .iter()
                    .map(|seed| seed.record.logical_size)
                    .sum(),
                message: "Recovered an exact cache purge after restart.".into(),
            };
            let seeds = evidence
                .seeds
                .iter()
                .map(|seed| seed.record.clone())
                .collect::<Vec<_>>();
            let mut state = runtime.data.lock().state.clone();
            let event = runtime.store.complete_cache_purge(
                &state,
                command_id,
                &evidence.request_digest,
                &seeds,
                &result,
                Actor::Recovery,
            )?;
            state.event_sequence = event.sequence;
            let mut data = runtime.data.lock();
            data.state = state;
            for seed in &mut data.seeds {
                if result.seed_ids.contains(&seed.id) {
                    seed.state = "pruned".into();
                }
            }
            for slot in &evidence.slots {
                let mut reset = slot.slot.clone();
                reset.checkout_oid = Some(slot.checkout.clone());
                reset.health = "healthy".into();
                reset.state = "idle".into();
                reset.last_used = Some(OffsetDateTime::now_utc());
                data.slots.insert(reset.id, reset);
            }
            drop(data);
            let _ = runtime.events.send(event);
            for seed in &evidence.seeds {
                remove_owned_quarantine(&cache_root, &seed.quarantine)?;
            }
            for slot in &evidence.slots {
                remove_owned_quarantine(&cache_root, &slot.quarantine)?;
            }
        }
        for evidence in runtime.store.completed_operation_evidence("cache-purge")? {
            let evidence: CachePurgeEvidence = serde_json::from_value(evidence)?;
            let cache_root = std::fs::canonicalize(
                runtime
                    .slots_root
                    .parent()
                    .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?,
            )?;
            for seed in &evidence.seeds {
                if seed.original.exists() {
                    return Err(ServiceError::Invariant(format!(
                        "completed seed prune {} still has its original path",
                        seed.record.id
                    )));
                }
                if seed.quarantine.exists() {
                    verify_seed_record_at(&seed.quarantine, &seed.record)?;
                    remove_owned_quarantine(&cache_root, &seed.quarantine)?;
                }
            }
            for slot in &evidence.slots {
                verify_slot_checkout(runtime, &slot.slot.path, &slot.checkout).await?;
                if slot.quarantine.exists() {
                    remove_owned_quarantine(&cache_root, &slot.quarantine)?;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_master(&self, runtime: &Arc<RepositoryRuntime>) -> Result<(), ServiceError> {
        let observed = runtime.git.integration_oid().await?;
        let persisted = runtime.data.lock().state.master_oid.clone();
        if observed == persisted {
            return Ok(());
        }
        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "external-release-movement".into(),
                message: format!(
                    "release moved externally from {} to {} while Tollgate was stopped",
                    persisted.short(),
                    observed.short()
                ),
                recovery_action:
                    "Run a guided reconcile before dispatching or promoting queued work.".into(),
            });
            data.state.clone()
        };
        runtime.store.update_repository_state(&state)?;
        Ok(())
    }

    async fn reconcile_approval_intents(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        if runtime.data.lock().state.execution_state == RepositoryExecutionState::Blocked {
            return Ok(());
        }
        for (command_id, mut item, request_digest) in runtime.store.unfinished_approvals()? {
            let observed_source = match runtime.git.optional_ref_oid(&item.source_ref).await? {
                Some(oid) => oid,
                None => {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"source_ref": item.source_ref, "recovery": "ref-absent"}),
                    )?;
                    continue;
                }
            };
            if observed_source != item.source_oid {
                let state = {
                    let mut data = runtime.data.lock();
                    data.state.execution_state = RepositoryExecutionState::Blocked;
                    data.state.block_reasons.push(BlockReason {
                        code: "ambiguous-approval-recovery".into(),
                        message: format!("{} points to an unexpected object", item.source_ref),
                        recovery_action: "Inspect the owned retention ref and the prepared approval intent before reconciling it.".into(),
                    });
                    data.state.clone()
                };
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"source_ref": item.source_ref, "observed": observed_source}),
                )?;
                runtime.store.update_repository_state(&state)?;
                continue;
            }
            if item.kind == QueueItemKind::IndependentCheck {
                let state = runtime.data.lock().state.clone();
                let config = runtime.data.lock().config.clone();
                let parent = runtime.git.commit_parent_oid(&item.source_oid).await?;
                let generation = ValidationGeneration::derive(
                    ValidationGenerationId::new(),
                    item.id,
                    parent.clone(),
                    vec![item.id],
                    vec![item.source_oid.clone()],
                    vec![item.source_oid.clone()],
                    parent,
                    item.source_oid.clone(),
                    config.digest,
                    config.step_graph_digest,
                    state.engine_epoch,
                );
                item.current_generation_id = Some(generation.id);
                item.state = item
                    .state
                    .transition(ItemEvent::GenerationPrepared)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                let result = ApproveResult {
                    item_id: item.id,
                    queue_revision: state.queue_revision,
                    source_oid: item.source_oid.clone(),
                    tested_oid: generation.tested_oid.clone(),
                };
                let event = runtime.store.complete_check(
                    &item,
                    &generation,
                    Actor::Recovery,
                    command_id,
                    &request_digest,
                    &result,
                )?;
                let mut data = runtime.data.lock();
                data.state.event_sequence = event.sequence;
                data.items.push(item);
                data.push_generation(generation);
                let _ = runtime.events.send(event);
                continue;
            }
            let (state, config, mut ordered_items) = {
                let data = runtime.data.lock();
                (
                    data.state.clone(),
                    data.config.clone(),
                    data.items
                        .iter()
                        .filter(|candidate| {
                            candidate.kind == QueueItemKind::Gate && !candidate.state.is_terminal()
                        })
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };
            ordered_items.push(item.clone());
            ordered_items.sort_by_key(|candidate| candidate.enqueue_sequence);
            let position = ordered_items
                .iter()
                .position(|candidate| candidate.id == item.id)
                .ok_or_else(|| ServiceError::Invariant("recovering approval disappeared".into()))?;
            let prefix = &ordered_items[..=position];
            let ordered_ids = prefix
                .iter()
                .map(|candidate| candidate.id)
                .collect::<Vec<_>>();
            let sources = prefix
                .iter()
                .map(|candidate| candidate.source_oid.clone())
                .collect::<Vec<_>>();
            runtime.git.initialize_mirror(&runtime.mirror).await?;
            let synthetic = match runtime
                .git
                .construct_prefix(
                    &runtime.mirror,
                    &runtime.builder,
                    &state.master_oid,
                    &sources,
                )
                .await
            {
                Ok(synthetic) => synthetic,
                Err(error) if error.is_synthetic_rejection() => {
                    runtime
                        .git
                        .delete_source_ref(&item.source_ref, &item.source_oid)
                        .await?;
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"recovery": "prefix-unmergeable"}),
                    )?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let tested = synthetic
                .last()
                .ok_or_else(|| ServiceError::Invariant("recovered prefix is empty".into()))?;
            let generation = ValidationGeneration::derive(
                ValidationGenerationId::new(),
                item.id,
                state.master_oid.clone(),
                ordered_ids,
                sources,
                synthetic.iter().map(|commit| commit.oid.clone()).collect(),
                tested.parent_oid.clone(),
                tested.oid.clone(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
                state.engine_epoch,
            );
            item.current_generation_id = Some(generation.id);
            item.state = item
                .state
                .transition(ItemEvent::GenerationPrepared)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            let result = ApproveResult {
                item_id: item.id,
                queue_revision: state.queue_revision + 1,
                source_oid: item.source_oid.clone(),
                tested_oid: generation.tested_oid.clone(),
            };
            let command_kind = if item.kind == QueueItemKind::IndependentCheck {
                "check"
            } else if !item.promotion_authorized {
                "candidate"
            } else {
                "approve"
            };
            runtime
                .git
                .retain_speculative_object(
                    &runtime.mirror,
                    &generation.id.to_string(),
                    &generation.tested_oid,
                )
                .await?;
            let event = runtime.store.complete_approval(
                &item,
                &generation,
                state.queue_revision,
                Actor::Recovery,
                command_id,
                command_kind,
                &request_digest,
                &result,
            )?;
            let mut data = runtime.data.lock();
            data.state.queue_revision += 1;
            data.state.event_sequence = event.sequence;
            data.items.push(item);
            data.push_generation(generation);
            let _ = runtime.events.send(event);
        }
        Ok(())
    }

    async fn reconcile_remote_intents(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, kind, evidence, _) in
            runtime.store.unfinished_operations(&["pull", "push"])?
        {
            let remote = evidence
                .get("remote")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ServiceError::Invariant("remote intent omitted remote name".into())
                })?;
            let branch = evidence
                .get("branch")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ServiceError::Invariant("remote intent omitted branch".into()))?;
            let current_fetch_url = runtime.git.remote_url(remote, false).await?;
            let frozen_fetch_url = evidence
                .get("remote_fetch_url")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&current_fetch_url);
            let current_push_url = runtime.git.remote_url(remote, true).await?;
            let frozen_push_url = evidence
                .get("remote_push_url")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&current_push_url);
            if current_fetch_url != frozen_fetch_url
                || (kind == "push" && current_push_url != frozen_push_url)
            {
                let local = runtime.git.integration_oid().await?;
                self.block_remote_recovery(
                    runtime,
                    command_id,
                    "remote-identity-changed",
                    &local,
                    None,
                )?;
                continue;
            }
            let observation_url = if kind == "push" {
                frozen_push_url
            } else {
                frozen_fetch_url
            };
            let observed_remote = runtime
                .git
                .observe_remote_ref(observation_url, branch)
                .await?;
            let frozen_remote: Option<GitOid> = evidence
                .get("observed_remote_oid")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?
                .flatten();
            let observed_local = runtime.git.integration_oid().await?;
            if kind == "pull" {
                let expected_local: GitOid = serde_json::from_value(
                    evidence.get("expected_local").cloned().ok_or_else(|| {
                        ServiceError::Invariant("pull intent omitted expected local release".into())
                    })?,
                )?;
                if observed_local == expected_local {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"recovery": "local-cas-not-applied"}),
                    )?;
                    continue;
                }
                let adopted_target = frozen_remote.as_ref().or(observed_remote.as_ref());
                if adopted_target == Some(&observed_local)
                    && runtime
                        .git
                        .is_ancestor(&expected_local, &observed_local)
                        .await?
                {
                    let mut state = runtime.data.lock().state.clone();
                    if state.master_oid != observed_local {
                        state.master_oid = observed_local.clone();
                        state.queue_revision += 1;
                    }
                    let affected = evidence
                        .get("affected_item_ids")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?
                        .unwrap_or_else(|| {
                            runtime
                                .data
                                .lock()
                                .items
                                .iter()
                                .filter(|item| {
                                    item.kind == QueueItemKind::Gate && !item.state.is_terminal()
                                })
                                .map(|item| item.id)
                                .collect::<Vec<_>>()
                        });
                    let result = RemoteSyncResult {
                        action: RemoteSyncAction::AdoptedRemote,
                        local_master: observed_local.clone(),
                        remote_master: adopted_target.cloned(),
                        queue_revision: state.queue_revision,
                        affected_item_ids: affected.clone(),
                        message: "Recovered a completed remote fast-forward after restart.".into(),
                    };
                    let request_digest = evidence
                        .get("request_digest")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            ServiceError::Invariant("pull intent omitted request digest".into())
                        })?;
                    runtime.data.lock().state = state.clone();
                    let rebuilt = self
                        .rebuild_after_base_adoption(runtime, &observed_local)
                        .await?;
                    if rebuilt.iter().any(|item_id| !affected.contains(item_id)) {
                        return Err(ServiceError::Invariant(
                            "recovered base-adoption included an item outside the frozen impact"
                                .into(),
                        ));
                    }
                    // Rebuilding advances the repository event sequence through
                    // item projection events. Complete the pull from that fresh
                    // state so the operation event cannot reuse a sequence.
                    state = runtime.data.lock().state.clone();
                    let event = runtime.store.complete_operation(
                        &state,
                        "pull",
                        command_id,
                        "pull",
                        request_digest,
                        &result,
                        "remote.pull-recovered",
                        &serde_json::json!({"local": observed_local}),
                        Actor::Recovery,
                    )?;
                    state.event_sequence = event.sequence;
                    runtime.data.lock().state = state;
                    let _ = runtime.events.send(event);
                    continue;
                }
                self.block_remote_recovery(
                    runtime,
                    command_id,
                    "ambiguous-pull-recovery",
                    &observed_local,
                    observed_remote.as_ref(),
                )?;
                continue;
            }

            let new_oid: GitOid =
                serde_json::from_value(evidence.get("new_oid").cloned().ok_or_else(|| {
                    ServiceError::Invariant("push intent omitted new OID".into())
                })?)?;
            if observed_remote.as_ref() == Some(&new_oid) {
                let pending = runtime
                    .data
                    .lock()
                    .items
                    .iter()
                    .filter(|item| item.state == QueueItemState::PromotedLocalPushPending)
                    .cloned()
                    .collect::<Vec<_>>();
                for mut item in pending {
                    let matches_push = item.certificate_id.is_some_and(|certificate_id| {
                        runtime.data.lock().certificates.iter().any(|certificate| {
                            certificate.id == certificate_id && certificate.tested_oid == new_oid
                        })
                    });
                    if matches_push {
                        if runtime.data.lock().config.sync_user_master {
                            self.sync_user_master_after_promotion(
                                runtime,
                                Some(item.id),
                                &new_oid,
                                Actor::Recovery,
                            )
                            .await?;
                        }
                        item.state = item
                            .state
                            .transition(ItemEvent::PushCompleted)
                            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                        item.remote_state = RemoteState::Synchronized;
                        self.replace_item(runtime, item.clone())?;
                        self.finish_source_cleanup(runtime, item).await?;
                    }
                }
                if let Some(request_digest) = evidence
                    .get("request_digest")
                    .and_then(serde_json::Value::as_str)
                {
                    let mut state = runtime.data.lock().state.clone();
                    let result = RemoteSyncResult {
                        action: RemoteSyncAction::Pushed,
                        local_master: observed_local.clone(),
                        remote_master: observed_remote.clone(),
                        queue_revision: state.queue_revision,
                        affected_item_ids: Vec::new(),
                        message: "Recovered an exact leased push after restart.".into(),
                    };
                    let event = runtime.store.complete_operation(
                        &state,
                        "push",
                        command_id,
                        "push",
                        request_digest,
                        &result,
                        "remote.push-recovered",
                        &serde_json::json!({"remote": new_oid}),
                        Actor::Recovery,
                    )?;
                    state.event_sequence = event.sequence;
                    runtime.data.lock().state = state;
                    let _ = runtime.events.send(event);
                } else {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Completed,
                        &serde_json::json!({"remote": new_oid, "recovery": "push-observed"}),
                    )?;
                }
                continue;
            }
            let explicit_push =
                evidence.get("request_digest").is_some() && evidence.get("item_id").is_none();
            if let Some(expected_remote) = evidence
                .get("expected_remote")
                .or_else(|| evidence.get("observed_remote_oid"))
                .filter(|value| !value.is_null())
                .cloned()
                .map(serde_json::from_value::<GitOid>)
                .transpose()?
                && observed_remote.as_ref() == Some(&expected_remote)
                && (observed_local == expected_remote
                    || (explicit_push && observed_local == new_oid))
            {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "prepared-push-had-no-external-effect"}),
                )?;
                continue;
            }
            if observed_local == new_oid {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({
                        "recovery": "push-not-observed",
                        "remote": observed_remote,
                    }),
                )?;
                let pending = runtime
                    .data
                    .lock()
                    .items
                    .iter()
                    .filter(|item| item.state == QueueItemState::PromotedLocalPushPending)
                    .cloned()
                    .collect::<Vec<_>>();
                for mut item in pending {
                    item.remote_state = RemoteState::PushBlocked;
                    item.terminal_reason = Some("push-interrupted-before-observation".into());
                    self.replace_item(runtime, item)?;
                }
                continue;
            }
            self.block_remote_recovery(
                runtime,
                command_id,
                "ambiguous-push-recovery",
                &observed_local,
                observed_remote.as_ref(),
            )?;
        }
        Ok(())
    }

    fn block_remote_recovery(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        code: &str,
        local: &GitOid,
        remote: Option<&GitOid>,
    ) -> Result<(), ServiceError> {
        runtime.store.set_intent_state(
            command_id,
            IntentState::NeedsAttention,
            &serde_json::json!({"local": local, "remote": remote}),
        )?;
        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            if !data
                .state
                .block_reasons
                .iter()
                .any(|reason| reason.code == code)
            {
                data.state.block_reasons.push(BlockReason {
                    code: code.into(),
                    message: "Remote operation recovery found identities that match neither safe outcome.".into(),
                    recovery_action: "Inspect the recorded intent and exact local/remote refs, then reconcile explicitly.".into(),
                });
            }
            data.state.clone()
        };
        runtime.store.update_repository_state(&state)?;
        Ok(())
    }

    async fn reconcile_promotion_intent(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        let Some((command_id, certificate, _intent_state)) =
            runtime.store.unfinished_promotion()?
        else {
            let orphan = {
                let data = runtime.data.lock();
                data.items
                    .iter()
                    .find(|item| item.state == QueueItemState::Promoting)
                    .cloned()
            };
            let Some(mut item) = orphan else {
                return Ok(());
            };
            let certificate = {
                let data = runtime.data.lock();
                data.certificates
                    .iter()
                    .find(|certificate| Some(certificate.id) == item.certificate_id)
                    .cloned()
                    .ok_or_else(|| {
                        ServiceError::Invariant(
                            "orphan promoting item has no pass certificate".into(),
                        )
                    })?
            };
            let observed_master = runtime.git.integration_oid().await?;
            if observed_master == certificate.expected_parent_oid {
                item.state = item
                    .state
                    .transition(ItemEvent::PromotionDeferred)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                self.replace_item(runtime, item)?;
                return Ok(());
            }
            let state = {
                let mut data = runtime.data.lock();
                data.state.execution_state = RepositoryExecutionState::Blocked;
                data.state.block_reasons.push(BlockReason {
                    code: "orphan-promotion-state".into(),
                    message: "A promoting item has no durable promotion intent and release has moved.".into(),
                    recovery_action: "Inspect release and the item certificate, then reconcile the external movement.".into(),
                });
                data.state.clone()
            };
            runtime.store.update_repository_state(&state)?;
            return Ok(());
        };
        let observed_master = runtime.git.integration_oid().await?;
        if observed_master == certificate.expected_parent_oid {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"master": observed_master, "recovery": "cas-not-applied"}),
            )?;
            let item = {
                let data = runtime.data.lock();
                data.items
                    .iter()
                    .find(|item| item.id == certificate.queue_item_id)
                    .cloned()
            };
            if let Some(mut item) = item
                && item.state == QueueItemState::Promoting
            {
                item.state = item
                    .state
                    .transition(ItemEvent::PromotionDeferred)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                self.replace_item(runtime, item)?;
            }
            return Ok(());
        }
        if observed_master == certificate.tested_oid {
            let observed_tree = runtime.git.tree_oid(&observed_master).await?;
            let observed_parent = runtime.git.commit_parent_oid(&observed_master).await?;
            if observed_tree != certificate.tree_oid
                || observed_parent != certificate.expected_parent_oid
            {
                return self.block_for_ambiguous_promotion(runtime, &observed_master, command_id);
            }
            let mut item = {
                let data = runtime.data.lock();
                data.items
                    .iter()
                    .find(|item| item.id == certificate.queue_item_id)
                    .cloned()
                    .ok_or(ServiceError::ItemNotFound(certificate.queue_item_id))?
            };
            if item.state != QueueItemState::Promoting
                || item.certificate_id != Some(certificate.id)
            {
                return self.block_for_ambiguous_promotion(runtime, &observed_master, command_id);
            }
            let config = runtime.data.lock().config.clone();
            let remote_enabled = config.remote.enabled;
            if config.sync_user_master && !remote_enabled {
                self.sync_user_master_after_promotion(
                    runtime,
                    Some(item.id),
                    &certificate.tested_oid,
                    Actor::Recovery,
                )
                .await?;
            }
            item.state = item
                .state
                .transition(if remote_enabled {
                    ItemEvent::PromotedWithPush
                } else {
                    ItemEvent::PromotedWithoutPush
                })
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.cleanup_state = match item.cleanup_policy {
                CleanupPolicy::Automatic => CleanupState::Pending,
                CleanupPolicy::RetainWorktree => CleanupState::NotEligible,
            };
            let mut state = runtime.data.lock().state.clone();
            state.master_oid = observed_master;
            state.queue_revision += 1;
            state.event_sequence += 1;
            runtime.store.record_promotion(
                &state,
                &item,
                &certificate,
                certificate.expected_parent_oid.as_bytes(),
            )?;
            {
                let mut data = runtime.data.lock();
                data.state = state;
                if let Some(existing) = data
                    .items
                    .iter_mut()
                    .find(|candidate| candidate.id == item.id)
                {
                    *existing = item.clone();
                }
            }
            if !remote_enabled {
                self.finish_source_cleanup(runtime, item).await?;
            }
            return Ok(());
        }
        self.block_for_ambiguous_promotion(runtime, &observed_master, command_id)
    }

    fn block_for_ambiguous_promotion(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        observed_master: &GitOid,
        command_id: CommandId,
    ) -> Result<(), ServiceError> {
        runtime.store.set_intent_state(
            command_id,
            IntentState::NeedsAttention,
            &serde_json::json!({"master": observed_master, "recovery": "ambiguous"}),
        )?;
        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "ambiguous-promotion-recovery".into(),
                message: "release matches neither side of an unfinished promotion intent.".into(),
                recovery_action: "Inspect the recorded certificate and reconcile the external release movement before resuming.".into(),
            });
            data.state.clone()
        };
        runtime.store.update_repository_state(&state)?;
        Ok(())
    }

    async fn make_runtime(
        &self,
        git: GitRepository,
        store: RepositoryStore,
        state: RepositoryState,
        config: EffectiveConfig,
        ownership_lock: nix::fcntl::Flock<std::fs::File>,
    ) -> Result<Arc<RepositoryRuntime>, ServiceError> {
        let cache = self.support_root.join("cache").join(state.id.to_string());
        let mirror = cache.join("mirror.git");
        let builder = cache.join("builder");
        let slots_root = cache.join("slots");
        tokio::fs::create_dir_all(&slots_root).await?;
        let logs_root = git.common_dir.join("tollgate/logs");
        tokio::fs::create_dir_all(&logs_root).await?;
        if tokio::fs::symlink_metadata(&logs_root)
            .await?
            .file_type()
            .is_symlink()
            || !tokio::fs::canonicalize(&logs_root)
                .await?
                .starts_with(&git.common_dir)
        {
            return Err(ServiceError::Invariant(
                "owned log root escaped the repository common directory".into(),
            ));
        }
        let items = store.queue_items()?;
        let generations = store.generations()?;
        let buildsets = store.buildsets()?;
        let certificates = store.certificates()?;
        let slots = load_existing_slots(&slots_root, &mirror).await;
        let seeds = store.seed_records(state.id)?;
        let (events, _) = broadcast::channel(512);
        let repository_limit = usize::from(
            config
                .resources
                .repository_concurrency
                .min(config.resources.max_buildsets)
                .max(1),
        );
        Ok(Arc::new(RepositoryRuntime {
            _ownership_lock: ownership_lock,
            git,
            store,
            mirror,
            builder,
            slots_root,
            logs_root,
            data: Mutex::new(RuntimeData {
                state,
                items,
                generations,
                buildsets,
                certificates,
                config,
                slots,
                seeds,
            }),
            events,
            cancellations: Mutex::new(HashMap::new()),
            mutation: tokio::sync::Mutex::new(()),
            diagnosis: tokio::sync::Mutex::new(()),
            execution_permits: RwLock::new(Arc::new(Semaphore::new(repository_limit))),
            scheduler_epoch: AtomicU64::new(0),
            dispatching: Mutex::new(HashSet::new()),
            cold_sources: Mutex::new(HashSet::new()),
            cold_items: Mutex::new(HashSet::new()),
        }))
    }

    pub async fn snapshot(&self) -> Result<AppSnapshot, ServiceError> {
        let ids = self
            .runtimes
            .read()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut repositories = Vec::new();
        for id in ids {
            repositories.push(self.repository_snapshot(id).await?);
        }
        repositories.sort_by(|left, right| {
            left.state
                .name
                .to_lowercase()
                .cmp(&right.state.name.to_lowercase())
        });
        let environment = self.environment.read().await;
        Ok(AppSnapshot {
            version: env!("CARGO_PKG_VERSION").into(),
            generated_at: OffsetDateTime::now_utc(),
            repositories,
            unavailable_repositories: self.unavailable.read().await.clone(),
            environment: EnvironmentView {
                snapshot_id: environment.id.clone(),
                fingerprint: environment.fingerprint.clone(),
                path: environment
                    .variables
                    .get("PATH")
                    .cloned()
                    .unwrap_or_default(),
                variable_count: environment.variables.len(),
            },
        })
    }

    pub async fn repository_snapshot(
        &self,
        id: RepositoryId,
    ) -> Result<RepositorySnapshot, ServiceError> {
        let runtime = self.runtime(id).await?;
        let scheduler_usage = self.global_scheduler.usage();
        let scheduler_capacity = self.global_scheduler.capacity();
        let volumes = observe_volumes(&runtime)?;
        let authoritative_volume_available = volumes
            .iter()
            .find(|volume| volume.roles.iter().any(|role| role == "authoritative"))
            .map_or(0, |volume| volume.available_bytes);
        let observed_master_oid = runtime.git.integration_oid().await?;
        let data = runtime.data.lock();
        let history = runtime
            .store
            .events_after(data.state.event_sequence.saturating_sub(500), 500)?;
        let queue = data
            .items
            .iter()
            .filter(|item| {
                item.kind == QueueItemKind::Gate
                    && (!item.state.is_terminal()
                        || item.state == QueueItemState::PromotedLocalPushPending)
            })
            .map(|item| queue_item_view(&data, item))
            .collect();
        let mut check_items = data
            .items
            .iter()
            .filter(|item| item.kind == QueueItemKind::IndependentCheck)
            .collect::<Vec<_>>();
        check_items.sort_by_key(|item| std::cmp::Reverse(item.enqueue_sequence));
        let checks = check_items
            .into_iter()
            .filter(|item| !item.state.is_terminal())
            .chain(
                data.items
                    .iter()
                    .filter(|item| {
                        item.kind == QueueItemKind::IndependentCheck && item.state.is_terminal()
                    })
                    .rev()
                    .take(24),
            )
            .map(|item| queue_item_view(&data, item))
            .collect();
        let master_push = data
            .items
            .iter()
            .filter(|item| is_master_push_item(item))
            .max_by_key(|item| item.enqueue_sequence)
            .map(|item| queue_item_view(&data, item));
        let history_items = data
            .items
            .iter()
            .rev()
            .filter(|item| item.state.is_terminal())
            .take(24)
            .map(|item| queue_item_view(&data, item))
            .collect();
        let active_runs = data
            .buildsets
            .iter()
            .filter(|buildset| {
                matches!(
                    buildset.state,
                    BuildsetState::Preparing | BuildsetState::Running
                )
            })
            .count();
        let queued_runs = data
            .buildsets
            .iter()
            .filter(|buildset| buildset.state == BuildsetState::Pending)
            .count();
        Ok(RepositorySnapshot {
            state: data.state.clone(),
            observed_master_oid,
            queue,
            checks,
            master_push,
            history_items,
            history,
            configuration: ConfigurationView {
                digest: data.config.digest.clone(),
                step_graph_digest: data.config.step_graph_digest.clone(),
                steps: data.config.steps.clone(),
                remote_enabled: data.config.remote.enabled,
                remote_name: data.config.remote.name.clone(),
                remote_branch: data.config.remote.branch.clone(),
                runner: data.config.runner.clone(),
            },
            resources: ResourceView {
                max_buildsets: scheduler_capacity.max_buildsets,
                repository_concurrency: data.config.resources.repository_concurrency,
                cpu_tokens: scheduler_capacity.cpu_tokens,
                memory_bytes: scheduler_capacity.memory_bytes,
                active_runs,
                queued_runs,
                cpu_reserved: scheduler_usage.cpu_reserved,
                memory_reserved: scheduler_usage.memory_reserved,
                named_semaphores: scheduler_usage.semaphore_reserved,
                authoritative_volume_available,
                recovery_reserve: data.config.resources.volume_critical_bytes,
                volumes,
            },
            slots: data.slots.values().cloned().collect(),
            seeds: data
                .seeds
                .iter()
                .map(|seed| SeedView {
                    id: seed.id.clone(),
                    path: seed.path.clone(),
                    profile: seed.profile.clone(),
                    generation: seed.generation,
                    logical_size: seed.logical_size,
                    state: seed.state.clone(),
                })
                .collect(),
            artifacts: runtime.store.retained_artifacts()?,
        })
    }

    pub async fn item_status(
        &self,
        repository_id: RepositoryId,
        item_id: QueueItemId,
    ) -> Result<QueueItem, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        runtime
            .data
            .lock()
            .items
            .iter()
            .find(|item| item.id == item_id)
            .cloned()
            .ok_or(ServiceError::ItemNotFound(item_id))
    }

    pub async fn item_wait_status(
        &self,
        repository_id: RepositoryId,
        item_id: QueueItemId,
    ) -> Result<ItemWaitStatus, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let data = runtime.data.lock();
        let item = data
            .items
            .iter()
            .find(|item| item.id == item_id)
            .cloned()
            .ok_or(ServiceError::ItemNotFound(item_id))?;
        Ok(ItemWaitStatus {
            item,
            repository_execution_state: data.state.execution_state,
            block_reasons: data.state.block_reasons.clone(),
        })
    }

    pub async fn item_details(
        &self,
        repository_id: RepositoryId,
        item_id: QueueItemId,
    ) -> Result<QueueItemView, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let data = runtime.data.lock();
        let item = data
            .items
            .iter()
            .find(|item| item.id == item_id)
            .ok_or(ServiceError::ItemNotFound(item_id))?;
        Ok(queue_item_view(&data, item))
    }

    pub async fn item_details_by_id(
        &self,
        repository_id: Option<RepositoryId>,
        item_id: QueueItemId,
    ) -> Result<QueueItemView, ServiceError> {
        if let Some(repository_id) = repository_id {
            return self.item_details(repository_id, item_id).await;
        }
        let runtimes = self
            .runtimes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut matched_repository = None;
        for runtime in runtimes {
            let repository_id = {
                let data = runtime.data.lock();
                data.items
                    .iter()
                    .any(|item| item.id == item_id)
                    .then_some(data.state.id)
            };
            if let Some(repository_id) = repository_id
                && matched_repository.replace(repository_id).is_some()
            {
                return Err(ServiceError::Invariant(format!(
                    "queue item {item_id} exists in more than one registered repository"
                )));
            }
        }
        match matched_repository {
            Some(repository_id) => self.item_details(repository_id, item_id).await,
            None => Err(ServiceError::ItemNotFound(item_id)),
        }
    }

    pub async fn diagnose_failure(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        replay: bool,
        verify_repair: bool,
        command_id: CommandId,
    ) -> Result<DiagnoseResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let (generation, buildset, source_oid, attribution) = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            let view = queue_item_view(&data, item);
            let generation = view.generation.ok_or_else(|| {
                ServiceError::Invariant(
                    "diagnosis requires a prepared validation generation".into(),
                )
            })?;
            let buildset = view.buildset;
            if (replay || verify_repair)
                && (generation.configuration_digest != data.config.digest
                    || generation.step_graph_digest != data.config.step_graph_digest)
            {
                return Err(ServiceError::Invariant(
                    "the active configuration changed; the original failure cannot be replayed under its frozen step graph".into(),
                ));
            }
            (
                generation,
                buildset,
                item.source_oid.clone(),
                view.failure_attribution,
            )
        };
        let repair_artifact = if verify_repair {
            Some(
                self.verify_diagnostic_repair(
                    &runtime,
                    item_id,
                    &source_oid,
                    &generation,
                    attribution.as_ref(),
                )
                .await?,
            )
        } else {
            None
        };
        if !replay {
            return Ok(DiagnoseResult {
                item_id,
                attribution,
                replay_item_ids: Vec::new(),
                scheduled_replay_item_ids: Vec::new(),
                reused_replay_item_ids: Vec::new(),
                replay_reasons: vec![
                    "retained comparable evidence selected; no full-gate execution scheduled"
                        .into(),
                ],
                repair_artifact,
            });
        }

        let buildset = buildset.ok_or_else(|| {
            ServiceError::Invariant("diagnostic replay requires retained buildset evidence".into())
        })?;
        let _diagnosis = runtime.diagnosis.lock().await;
        let environment = self.environment.read().await.clone();
        if environment.fingerprint != buildset.environment_fingerprint {
            return Err(ServiceError::Invariant(
                "the active environment changed; replay evidence would not be comparable to the original failure".into(),
            ));
        }

        let (base_retained, candidate_probe_retained, base_in_flight, candidate_in_flight) = {
            let data = runtime.data.lock();
            let failed_steps = buildset
                .step_results
                .iter()
                .filter(|result| result.result_class != "success")
                .map(|result| result.name.as_str())
                .collect::<Vec<_>>();
            let comparable = |candidate: &Buildset, tested_oid: &GitOid| {
                candidate.tested_oid == *tested_oid
                    && candidate.environment_fingerprint == buildset.environment_fingerprint
                    && data
                        .generations
                        .iter()
                        .find(|entry| entry.id == candidate.validation_generation_id)
                        .is_some_and(|entry| {
                            entry.configuration_digest == generation.configuration_digest
                                && entry.step_graph_digest == generation.step_graph_digest
                                && entry.engine_epoch == generation.engine_epoch
                        })
            };
            let has_results = |candidate: &Buildset, expected: &str| {
                candidate.state.is_terminal()
                    && failed_steps.iter().all(|name| {
                        candidate
                            .step_results
                            .iter()
                            .any(|result| result.name == *name && result.result_class == expected)
                    })
            };
            let has_trusted_success = |candidate: &Buildset| {
                data.certificates
                    .iter()
                    .any(|certificate| certificate.buildset_id == candidate.id)
                    || data.items.iter().any(|item| {
                        item.id == candidate.item_id && item.state == QueueItemState::CheckPassed
                    })
            };
            let base_retained = data.buildsets.iter().any(|candidate| {
                comparable(candidate, &generation.expected_parent_oid)
                    && has_results(candidate, "success")
                    && has_trusted_success(candidate)
            });
            let candidate_probe_retained = data.buildsets.iter().any(|candidate| {
                candidate.id != buildset.id
                    && comparable(candidate, &generation.tested_oid)
                    && candidate.state.is_terminal()
                    && failed_steps.iter().all(|name| {
                        candidate.step_results.iter().any(|result| {
                            result.name == *name
                                && result.result_class != "skipped"
                                && (result.result_class != "success"
                                    || has_trusted_success(candidate))
                        })
                    })
            });
            let in_flight = |tested_oid: &GitOid| {
                data.items.iter().find_map(|item| {
                    if item.id == item_id || item.state.is_terminal() {
                        return None;
                    }
                    let replay_generation = item
                        .current_generation_id
                        .and_then(|id| data.generations.iter().find(|entry| entry.id == id))?;
                    let inputs_match = replay_generation.tested_oid == *tested_oid
                        && replay_generation.configuration_digest
                            == generation.configuration_digest
                        && replay_generation.step_graph_digest == generation.step_graph_digest
                        && replay_generation.engine_epoch == generation.engine_epoch;
                    let environment_matches = item.buildset_id.is_none_or(|id| {
                        data.buildsets
                            .iter()
                            .find(|entry| entry.id == id)
                            .is_some_and(|entry| {
                                entry.environment_fingerprint == buildset.environment_fingerprint
                            })
                    });
                    (inputs_match && environment_matches).then_some(item.id)
                })
            };
            (
                base_retained,
                candidate_probe_retained,
                in_flight(&generation.expected_parent_oid),
                in_flight(&generation.tested_oid),
            )
        };

        let mut replay_item_ids = Vec::new();
        let mut scheduled_replay_item_ids = Vec::new();
        let mut reused_replay_item_ids = Vec::new();
        let mut replay_reasons = Vec::new();
        if base_retained {
            replay_reasons.push(
                "base replay omitted: a matching successful base buildset is retained".into(),
            );
        } else if let Some(existing) = base_in_flight {
            replay_item_ids.push(existing);
            reused_replay_item_ids.push(existing);
            replay_reasons.push(format!(
                "base replay coalesced with matching in-flight check {existing}"
            ));
        } else {
            let baseline = self
                .check_from_with_purpose(
                    repository_id,
                    generation.expected_parent_oid.to_hex(),
                    None,
                    command_id,
                    format!("diagnose:{item_id}:base"),
                    CheckMode::RetainedCold(generation.expected_parent_oid.clone()),
                )
                .await?;
            replay_item_ids.push(baseline.item_id);
            scheduled_replay_item_ids.push(baseline.item_id);
            replay_reasons.push(
                "base replay scheduled: no comparable successful base evidence exists".into(),
            );
        }
        if candidate_probe_retained {
            replay_reasons.push(
                "candidate replay omitted: a matching repeat candidate result is retained".into(),
            );
        } else if let Some(existing) = candidate_in_flight {
            replay_item_ids.push(existing);
            reused_replay_item_ids.push(existing);
            replay_reasons.push(format!(
                "candidate stability probe coalesced with matching in-flight check {existing}"
            ));
        } else {
            let candidate = self
                .check_from_with_purpose(
                    repository_id,
                    generation.tested_oid.to_hex(),
                    None,
                    derived_command_id(command_id, "diagnose-candidate-stability"),
                    format!("diagnose:{item_id}:candidate"),
                    CheckMode::RetainedCold(generation.tested_oid.clone()),
                )
                .await?;
            replay_item_ids.push(candidate.item_id);
            scheduled_replay_item_ids.push(candidate.item_id);
            replay_reasons.push(
                "candidate stability probe scheduled: only the original failure is retained".into(),
            );
        }
        Ok(DiagnoseResult {
            item_id,
            attribution,
            replay_item_ids,
            scheduled_replay_item_ids,
            reused_replay_item_ids,
            replay_reasons,
            repair_artifact,
        })
    }

    async fn verify_diagnostic_repair(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item_id: QueueItemId,
        source_oid: &GitOid,
        generation: &ValidationGeneration,
        attribution: Option<&FailureAttribution>,
    ) -> Result<RepairArtifact, ServiceError> {
        let attribution = attribution.ok_or_else(|| {
            ServiceError::Invariant("the failed run has no structured repair diagnostic".into())
        })?;
        let mut repairs = attribution
            .steps
            .iter()
            .flat_map(|step| {
                step.diagnostics.iter().filter_map(move |diagnostic| {
                    diagnostic
                        .repair
                        .as_ref()
                        .map(|repair| (step.name.clone(), repair.clone()))
                })
            })
            .collect::<Vec<_>>();
        repairs.sort_by(|left, right| left.0.cmp(&right.0));
        repairs.dedup();
        let [(step_name, RepairCommand::Argv { argv })] = repairs.as_slice() else {
            return Err(ServiceError::Invariant(
                "repair verification requires exactly one unambiguous structured argv repair"
                    .into(),
            ));
        };
        let environment = self.environment.read().await.clone();
        let (config, step) = {
            let data = runtime.data.lock();
            let step = data
                .config
                .steps
                .iter()
                .find(|step| step.name == *step_name)
                .ok_or_else(|| {
                    ServiceError::Invariant("repair step is no longer configured".into())
                })?;
            (data.config.clone(), step.clone())
        };
        let repair_id = SlotId::new();
        let slot = runtime
            .slots_root
            .join(format!("diagnose-repair-{repair_id}"));
        let logs = runtime
            .logs_root
            .join("diagnoses")
            .join(repair_id.to_string());
        runtime
            .git
            .provision_slot(&runtime.mirror, &slot, &generation.tested_oid)
            .await?;
        let attempt = async {
            tokio::fs::create_dir_all(&logs).await?;
            let changed_paths = runtime.git.changed_paths(source_oid).await?;
            let context = BTreeMap::from([
                ("CI".into(), "1".into()),
                ("TOLLGATE_ITEM_ID".into(), item_id.to_string()),
                ("TOLLGATE_TESTED_OID".into(), generation.tested_oid.to_hex()),
                (
                    "TOLLGATE_VALIDATION_GENERATION_ID".into(),
                    generation.id.to_string(),
                ),
            ]);
            let execution = |directory: &str| BuildsetExecution {
                tested_oid: generation.tested_oid.clone(),
                slot_root: slot.clone(),
                log_directory: logs.join(directory),
                environment: (*environment.variables).clone(),
                context: context.clone(),
            };
            let before = run_buildset(
                &config,
                execution("before"),
                &changed_paths,
                CancellationToken::new(),
            )
            .await?;
            if !before
                .steps
                .iter()
                .any(|(name, result)| name == step_name && result.class != StepResultClass::Success)
            {
                return Err(ServiceError::Invariant(
                    "the clean candidate replay did not reproduce the diagnosed failure".into(),
                ));
            }

            let mut repair_environment = (*environment.variables).clone();
            repair_environment.extend(step.environment.clone());
            for name in &step.remove_environment {
                repair_environment.remove(name);
            }
            repair_environment.extend(context.clone());
            repair_environment.insert("TOLLGATE_REPAIR_VERIFY".into(), "1".into());
            let status = tokio::process::Command::new(&argv[0])
                .args(&argv[1..])
                .current_dir(slot.join(&step.working_directory))
                .env_clear()
                .envs(repair_environment)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await?;
            if !status.success() {
                return Err(ServiceError::Invariant(format!(
                    "structured repair command exited with {status}"
                )));
            }

            let after = run_buildset(
                &config,
                execution("after"),
                &changed_paths,
                CancellationToken::new(),
            )
            .await?;
            let applicable_voting = config
                .applicable_steps(&changed_paths)?
                .into_iter()
                .filter(|candidate| candidate.voting)
                .map(|candidate| candidate.name.as_str())
                .collect::<HashSet<_>>();
            let verified = !applicable_voting.is_empty()
                && applicable_voting.iter().all(|name| {
                    after.steps.iter().any(|(result_name, result)| {
                        result_name == name && result.class == StepResultClass::Success
                    })
                });
            if !verified {
                return Err(ServiceError::Invariant(
                    "the repair did not make every applicable voting step pass".into(),
                ));
            }
            const MAX_REPAIR_PATCH_BYTES: u64 = 64 * 1024 * 1024;
            const ARTIFACT_RETENTION_BUDGET: u64 = 50 * 1024 * 1024 * 1024;
            let patch = runtime
                .git
                .worktree_patch(&slot, MAX_REPAIR_PATCH_BYTES)
                .await?;
            if patch.is_empty() {
                return Err(ServiceError::Invariant(
                    "the verified repair produced no source patch".into(),
                ));
            }
            let hash = blake3::hash(&patch).to_hex().to_string();
            let directory = runtime.git.common_dir.join("tollgate/artifacts/diagnoses");
            let path = directory.join(format!("{item_id}-{hash}.patch"));
            let _mutation = runtime.mutation.lock().await;
            let retained = runtime.store.retained_artifacts()?;
            let already_recorded = retained
                .iter()
                .any(|record| record.retained_path == path.to_string_lossy());
            if !already_recorded
                && runtime
                    .store
                    .retained_artifact_bytes()?
                    .saturating_add(patch.len() as u64)
                    > ARTIFACT_RETENTION_BUDGET
            {
                return Err(ServiceError::Invariant(
                    "repair artifact retention would exceed the repository budget".into(),
                ));
            }
            tokio::fs::create_dir_all(&directory).await?;
            match tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .await
            {
                Ok(mut file) => {
                    file.write_all(&patch).await?;
                    file.sync_all().await?;
                    sync_directory(&directory)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = tokio::fs::symlink_metadata(&path).await?;
                    if !metadata.file_type().is_file() || hash_file(&path).await? != hash {
                        return Err(ServiceError::Invariant(
                            "an existing repair artifact does not match its content address".into(),
                        ));
                    }
                }
                Err(error) => return Err(error.into()),
            }
            if !already_recorded {
                runtime.store.record_artifact(
                    attribution.candidate_buildset_id,
                    Path::new("diagnosis/repair.patch"),
                    &path,
                    &hash,
                    patch.len() as u64,
                )?;
            }
            Ok(RepairArtifact {
                path: path.to_string_lossy().into_owned(),
                blake3: hash,
                byte_length: patch.len() as u64,
                verified: true,
            })
        }
        .await;
        let cleanup = runtime.git.remove_slot(&runtime.mirror, &slot).await;
        match (attempt, cleanup) {
            (Ok(artifact), Ok(())) => Ok(artifact),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
        }
    }

    pub async fn history_items_page(
        &self,
        repository_id: RepositoryId,
        offset: usize,
        limit: usize,
    ) -> Result<HistoryItemsPage, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let data = runtime.data.lock();
        let terminal = data
            .items
            .iter()
            .rev()
            .filter(|item| item.state.is_terminal())
            .collect::<Vec<_>>();
        let total = terminal.len();
        let items = terminal
            .into_iter()
            .skip(offset)
            .take(limit.clamp(1, 100))
            .map(|item| queue_item_view(&data, item))
            .collect();
        Ok(HistoryItemsPage {
            items,
            total,
            offset,
        })
    }

    pub async fn approve(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.approve_from(repository_id, revision, None, command_id)
            .await
    }

    pub async fn approve_from(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.approve_from_with_purpose(repository_id, revision, worktree_path, None, command_id)
            .await
    }

    pub async fn approve_from_with_purpose(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        purpose: Option<String>,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.approve_from_with_cleanup_policy(
            repository_id,
            revision,
            worktree_path,
            purpose,
            CleanupPolicy::Automatic,
            command_id,
        )
        .await
    }

    pub async fn approve_from_with_cleanup_policy(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        purpose: Option<String>,
        cleanup_policy: CleanupPolicy,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.enqueue_gate_from(
            repository_id,
            revision,
            worktree_path,
            GateSubmission {
                purpose,
                cleanup_policy,
                command_id,
                promotion_authorized: true,
            },
        )
        .await
    }

    pub async fn submit_candidate(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.submit_candidate_from(repository_id, revision, None, command_id)
            .await
    }

    pub async fn submit_candidate_from(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.submit_candidate_from_with_cleanup_policy(
            repository_id,
            revision,
            worktree_path,
            CleanupPolicy::Automatic,
            command_id,
        )
        .await
    }

    pub async fn submit_candidate_from_with_cleanup_policy(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        cleanup_policy: CleanupPolicy,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.enqueue_gate_from(
            repository_id,
            revision,
            worktree_path,
            GateSubmission {
                purpose: None,
                cleanup_policy,
                command_id,
                promotion_authorized: false,
            },
        )
        .await
    }

    async fn enqueue_gate_from(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        submission: GateSubmission,
    ) -> Result<ApproveResult, ServiceError> {
        let GateSubmission {
            purpose,
            cleanup_policy,
            command_id,
            promotion_authorized,
        } = submission;
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let requested_revision = revision.clone();
        let requested_worktree = worktree_path.clone();
        {
            let data = runtime.data.lock();
            if data.state.execution_state != RepositoryExecutionState::Active {
                return Err(ServiceError::RepositoryUnavailable(
                    data.state.execution_state,
                ));
            }
        }
        let config_text =
            tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?;
        let current_config = EffectiveConfig::parse(&config_text);
        if current_config.is_err() {
            let state = {
                let mut data = runtime.data.lock();
                data.state.execution_state = RepositoryExecutionState::ConfigurationPending;
                data.state.clone()
            };
            runtime.store.update_repository_state(&state)?;
            return Err(ServiceError::RepositoryUnavailable(
                RepositoryExecutionState::ConfigurationPending,
            ));
        }
        let current_config = current_config?;
        if runtime.data.lock().config.digest != current_config.digest {
            let state = {
                let mut data = runtime.data.lock();
                data.state.execution_state = RepositoryExecutionState::ConfigurationPending;
                data.state.clone()
            };
            runtime.store.update_repository_state(&state)?;
            return Err(ServiceError::RepositoryUnavailable(
                RepositoryExecutionState::ConfigurationPending,
            ));
        }
        let approval_git = match worktree_path {
            Some(path) => {
                let discovered = GitRepository::discover(path).await?;
                if discovered.common_dir != runtime.git.common_dir {
                    return Err(ServiceError::Invariant(
                        "invoking worktree does not belong to the selected repository".into(),
                    ));
                }
                discovered
            }
            None => runtime.git.clone(),
        };
        let probe = approval_git.probe_approval(&revision).await?;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "kind": QueueItemKind::Gate,
            "revision": requested_revision,
            "worktree_path": requested_worktree,
            "purpose": purpose,
            "cleanup_policy": cleanup_policy,
            "promotion_authorized": promotion_authorized,
        }))?;
        let command_kind = if promotion_authorized {
            "approve"
        } else {
            "candidate"
        };
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, command_kind, &request_digest)?
        {
            return Ok(response);
        }
        let prepared = runtime.store.unfinished_approvals()?;
        if let Some((_, _, frozen_digest)) = prepared
            .iter()
            .find(|(prepared_id, _, _)| *prepared_id == command_id)
            && frozen_digest != &request_digest
        {
            return Err(StoreError::CommandReplayMismatch.into());
        }
        if !prepared.is_empty() {
            self.reconcile_approval_intents(&runtime).await?;
            if let Some(response) =
                runtime
                    .store
                    .checked_command_response(command_id, command_kind, &request_digest)?
            {
                return Ok(response);
            }
        }
        let item_id = QueueItemId::new();
        let (
            state,
            active_items,
            existing_ids,
            existing_oids,
            enqueue_sequence,
            current_prefix_oid,
        ) = {
            let data = runtime.data.lock();
            if let Some(existing) = data.items.iter().find(|item| {
                item.kind == QueueItemKind::Gate
                    && !item.state.is_terminal()
                    && item.source_oid == probe.source_oid
            }) {
                return Err(ServiceError::Invariant(format!(
                    "source is already queued as {}",
                    existing.id
                )));
            }
            let active_items = data
                .items
                .iter()
                .filter(|item| {
                    item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state)
                })
                .cloned()
                .collect::<Vec<_>>();
            (
                data.state.clone(),
                active_items.clone(),
                active_items.iter().map(|item| item.id).collect::<Vec<_>>(),
                active_items
                    .iter()
                    .map(|item| item.source_oid.clone())
                    .collect::<Vec<_>>(),
                data.items
                    .iter()
                    .map(|item| item.enqueue_sequence)
                    .max()
                    .unwrap_or(0)
                    + 1,
                current_queue_prefix_oid(&data),
            )
        };
        let unmerged_ancestors = runtime
            .git
            .unmerged_first_parent_ancestors(&probe.parent_oid, &state.master_oid)
            .await?;
        let allow_unpromoted_source_ancestry = purpose.as_deref() == Some("push-master");
        if !allow_unpromoted_source_ancestry {
            for ancestor in &unmerged_ancestors {
                let satisfied = if let Some(bytes) = runtime.store.promoted_oid_bytes(ancestor)? {
                    let promoted = GitOid::new(ancestor.format, bytes)
                        .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                    runtime
                        .git
                        .is_ancestor(&promoted, &state.master_oid)
                        .await?
                } else {
                    false
                };
                if !satisfied {
                    return Err(ServiceError::UnpromotedSourceAncestor {
                        ancestor: ancestor.clone(),
                        release_oid: state.master_oid.clone(),
                    });
                }
            }
        }
        let mut dependency_ids = HashSet::new();
        for ancestor in unmerged_ancestors
            .iter()
            .filter(|_| allow_unpromoted_source_ancestry)
        {
            if let Some(item) = active_items
                .iter()
                .find(|item| item.source_oid == *ancestor)
            {
                dependency_ids.insert(item.id);
                continue;
            }
            if let Some(prefix_dependencies) = {
                let data = runtime.data.lock();
                active_prefix_dependencies(&data, ancestor)
            } {
                dependency_ids.extend(prefix_dependencies);
                continue;
            }
            if let Some(bytes) = runtime.store.promoted_oid_bytes(ancestor)? {
                let promoted = GitOid::new(ancestor.format, bytes)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                if runtime
                    .git
                    .is_ancestor(&promoted, &state.master_oid)
                    .await?
                {
                    continue;
                }
            }
            let historical_prefix = {
                let data = runtime.data.lock();
                is_historical_prefix(&data, ancestor)
            };
            if historical_prefix {
                return Err(ServiceError::StaleQueuePrefix {
                    source_parent_oid: probe.parent_oid.clone(),
                    release_oid: state.master_oid.clone(),
                    queue_revision: state.queue_revision,
                    current_prefix_oid,
                });
            }
            return Err(ServiceError::UnknownSourceAncestor {
                ancestor: ancestor.clone(),
                release_oid: state.master_oid.clone(),
                queue_revision: state.queue_revision,
                current_prefix_oid,
            });
        }
        let dependencies = active_items
            .iter()
            .filter(|item| dependency_ids.contains(&item.id))
            .map(|item| item.id)
            .collect::<Vec<_>>();
        let mut ordered_ids = existing_ids;
        let mut sources = existing_oids;
        ordered_ids.push(item_id);
        sources.push(probe.source_oid.clone());
        let mut item = QueueItem {
            id: item_id,
            repository_id,
            kind: QueueItemKind::Gate,
            admission_sequence: Some(enqueue_sequence),
            enqueue_sequence,
            source_oid: probe.source_oid.clone(),
            source_ref: format!("refs/tollgate/sources/{item_id}"),
            metadata: SourceMetadata {
                subject: probe.subject,
                message_hash: probe.message_hash,
                author_name: probe.author_name,
                author_email: probe.author_email,
                branch: probe.branch,
                worktree_path: Some(approval_git.worktree_root.to_string_lossy().into_owned()),
                signature_state: SignatureState::Unknown,
                approved_at: OffsetDateTime::now_utc(),
                purpose: purpose.or_else(|| {
                    Some(
                        if promotion_authorized {
                            "gate"
                        } else {
                            "candidate"
                        }
                        .into(),
                    )
                }),
            },
            state: QueueItemState::Constructing,
            terminal_reason: None,
            remote_state: if current_config.remote.enabled {
                RemoteState::PreflightPending
            } else {
                RemoteState::Disabled
            },
            cleanup_state: CleanupState::NotEligible,
            cleanup_policy,
            dependencies,
            promotion_authorized,
            promotion_authorized_at: promotion_authorized.then(OffsetDateTime::now_utc),
            promotion_authorized_by: promotion_authorized.then_some(command_id),
            current_generation_id: None,
            buildset_id: None,
            certificate_id: None,
        };
        runtime
            .store
            .prepare_approval(repository_id, &item, command_id, &request_digest)?;
        self.reserve_runtime_volume(
            &runtime,
            command_id,
            &runtime.git.common_dir,
            current_config.resources.volume_emergency_bytes,
        )
        .await?;
        runtime
            .git
            .create_source_ref(item_id, &item.source_oid)
            .await?;
        runtime.git.initialize_mirror(&runtime.mirror).await?;
        let synthetic = match runtime
            .git
            .construct_prefix(
                &runtime.mirror,
                &runtime.builder,
                &state.master_oid,
                &sources,
            )
            .await
        {
            Ok(synthetic) => synthetic,
            Err(error) if error.is_synthetic_rejection() && item.dependencies.is_empty() => {
                // A candidate that conflicts with an earlier speculative item is still a
                // valid contender for promotion before that item. Retain it and validate
                // its source directly on the promoted base. Authorization can then move
                // either contender to the head without requiring the source worktree to
                // adopt an internal speculative prefix.
                ordered_ids = vec![item_id];
                sources = vec![item.source_oid.clone()];
                runtime
                    .git
                    .construct_prefix(
                        &runtime.mirror,
                        &runtime.builder,
                        &state.master_oid,
                        &sources,
                    )
                    .await
                    .map_err(|standalone_error| {
                        if standalone_error.is_synthetic_rejection() {
                            error
                        } else {
                            standalone_error
                        }
                    })?
            }
            Err(error) => return Err(error.into()),
        };
        let tested = synthetic
            .last()
            .ok_or_else(|| ServiceError::Invariant("synthetic prefix is empty".into()))?;
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            state.master_oid.clone(),
            ordered_ids,
            sources,
            synthetic.iter().map(|commit| commit.oid.clone()).collect(),
            tested.parent_oid.clone(),
            tested.oid.clone(),
            current_config.digest.clone(),
            current_config.step_graph_digest.clone(),
            state.engine_epoch,
        );
        item.current_generation_id = Some(generation.id);
        item.state = item
            .state
            .transition(ItemEvent::GenerationPrepared)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let result = ApproveResult {
            item_id,
            queue_revision: state.queue_revision + 1,
            source_oid: item.source_oid.clone(),
            tested_oid: generation.tested_oid.clone(),
        };
        let speculative_ref = runtime
            .git
            .retain_speculative_object(
                &runtime.mirror,
                &generation.id.to_string(),
                &generation.tested_oid,
            )
            .await?;
        let event = match runtime.store.complete_approval(
            &item,
            &generation,
            state.queue_revision,
            if promotion_authorized {
                Actor::Ui
            } else {
                Actor::Cli
            },
            command_id,
            command_kind,
            &request_digest,
            &result,
        ) {
            Ok(event) => event,
            Err(StoreError::RevisionConflict { .. }) => {
                runtime
                    .git
                    .delete_source_ref(&speculative_ref, &generation.tested_oid)
                    .await?;
                runtime
                    .git
                    .delete_source_ref(&item.source_ref, &item.source_oid)
                    .await?;
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "stale-queue-during-enqueue"}),
                )?;
                let current_state = runtime.store.repository_state()?;
                let current_items = runtime.store.queue_items()?;
                let current_generations = runtime.store.generations()?;
                return Err(ServiceError::StaleQueuePrefix {
                    source_parent_oid: probe.parent_oid,
                    release_oid: current_state.master_oid.clone(),
                    queue_revision: current_state.queue_revision,
                    current_prefix_oid: current_queue_prefix_from(
                        &current_state,
                        &current_items,
                        &current_generations,
                    ),
                });
            }
            Err(error) => return Err(error.into()),
        };
        {
            let mut data = runtime.data.lock();
            data.state.queue_revision += 1;
            data.state.event_sequence = event.sequence;
            data.items.push(item);
            data.push_generation(generation);
        }
        let _ = runtime.events.send(event);
        self.spawn_eligible(repository_id, &runtime);
        Ok(result)
    }

    pub async fn authorize_candidate(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        expected_revision: u64,
        command_id: CommandId,
    ) -> Result<CandidateAuthorizationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "item_id": item_id,
            "expected_revision": expected_revision,
        }))?;
        if let Some(response) = runtime.store.checked_command_response(
            command_id,
            "candidate-authorize",
            &request_digest,
        )? {
            return Ok(response);
        }
        let observed_before_lock = runtime.git.integration_oid().await?;
        let (persisted_master, current_revision) = {
            let data = runtime.data.lock();
            (data.state.master_oid.clone(), data.state.queue_revision)
        };
        if current_revision != expected_revision {
            return Err(ServiceError::RevisionConflict {
                expected: expected_revision,
                actual: current_revision,
            });
        }
        if observed_before_lock != persisted_master {
            let refreshed = self
                .reconcile_expected(
                    repository_id,
                    Some(observed_before_lock),
                    Some(current_revision),
                    CommandId::new(),
                )
                .await?;
            return Err(ServiceError::RevisionConflict {
                expected: expected_revision,
                actual: refreshed.queue_revision,
            });
        }
        let mutation = runtime.mutation.lock().await;
        if let Some(response) = runtime.store.checked_command_response(
            command_id,
            "candidate-authorize",
            &request_digest,
        )? {
            return Ok(response);
        }
        let disk_config = EffectiveConfig::parse(
            &tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?,
        )?;
        let observed_master = runtime.git.integration_oid().await?;
        let (
            item,
            mut current,
            items_to_authorize,
            original_generation,
            mut state,
            config,
            retained_generations,
            retained_buildsets,
            retained_certificates,
        ) = {
            let data = runtime.data.lock();
            if data.state.execution_state != RepositoryExecutionState::Active {
                return Err(ServiceError::RepositoryUnavailable(
                    data.state.execution_state,
                ));
            }
            if data.state.queue_revision != expected_revision {
                return Err(ServiceError::RevisionConflict {
                    expected: expected_revision,
                    actual: data.state.queue_revision,
                });
            }
            if observed_master != data.state.master_oid {
                return Err(ServiceError::Invariant(
                    "release moved during candidate authorization; retry so Tollgate can invalidate and rebuild the affected validation".into(),
                ));
            }
            if disk_config.digest != data.config.digest {
                return Err(ServiceError::Invariant(
                    "configuration changed since the candidate generation was frozen; apply it to invalidate and rebuild affected validation before authorizing it".into(),
                ));
            }
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .cloned()
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            if item.kind != QueueItemKind::Gate || item.state.is_terminal() {
                return Err(ServiceError::Invariant(
                    "promotion authority can only be granted to an active gate candidate".into(),
                ));
            }

            fn append_active_dependencies(
                item_id: QueueItemId,
                by_id: &HashMap<QueueItemId, QueueItem>,
                ordered: &mut Vec<QueueItem>,
                visiting: &mut HashSet<QueueItemId>,
            ) -> Result<(), ServiceError> {
                if !visiting.insert(item_id) {
                    return Err(ServiceError::Invariant(
                        "candidate dependency cycle prevents authorization".into(),
                    ));
                }
                let item = by_id
                    .get(&item_id)
                    .ok_or(ServiceError::ItemNotFound(item_id))?;
                for dependency_id in &item.dependencies {
                    let dependency = by_id
                        .get(dependency_id)
                        .ok_or(ServiceError::ItemNotFound(*dependency_id))?;
                    if dependency.state.is_terminal() {
                        if matches!(
                            dependency.state,
                            QueueItemState::Promoted | QueueItemState::ExternallyIntegrated
                        ) {
                            continue;
                        }
                        return Err(ServiceError::Invariant(format!(
                            "candidate dependency {dependency_id} is terminal in state {:?}",
                            dependency.state
                        )));
                    }
                    if dependency.kind != QueueItemKind::Gate {
                        return Err(ServiceError::Invariant(format!(
                            "candidate dependency {dependency_id} is not a gate"
                        )));
                    }
                    append_active_dependencies(*dependency_id, by_id, ordered, visiting)?;
                }
                visiting.remove(&item_id);
                if !ordered.iter().any(|candidate| candidate.id == item_id) {
                    ordered.push(item.clone());
                }
                Ok(())
            }

            let by_id = data
                .items
                .iter()
                .cloned()
                .map(|candidate| (candidate.id, candidate))
                .collect::<HashMap<_, _>>();
            let mut authorization_closure = Vec::new();
            append_active_dependencies(
                item_id,
                &by_id,
                &mut authorization_closure,
                &mut HashSet::new(),
            )?;
            let items_to_authorize = authorization_closure
                .into_iter()
                .filter(|candidate| !candidate.promotion_authorized)
                .collect::<Vec<_>>();
            let generation = data
                .generations
                .iter()
                .find(|generation| Some(generation.id) == item.current_generation_id)
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("candidate generation missing".into()))?;
            (
                item,
                data.items
                    .iter()
                    .filter(|candidate| {
                        candidate.kind == QueueItemKind::Gate
                            && is_rebuildable_gate_state(candidate.state)
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
                items_to_authorize,
                generation,
                data.state.clone(),
                data.config.clone(),
                data.generations.clone(),
                data.buildsets.clone(),
                data.certificates.clone(),
            )
        };
        if items_to_authorize.is_empty() {
            let result = CandidateAuthorizationResult {
                item_id,
                already_authorized: true,
                authorized_item_ids: Vec::new(),
                restarted_item_ids: Vec::new(),
                restored_item_ids: Vec::new(),
                queue_revision: state.queue_revision,
                source_oid: item.source_oid.clone(),
                validation_generation_id: original_generation.id,
                tested_oid: original_generation.tested_oid.clone(),
                validation_complete: item.state == QueueItemState::Ready,
                evidence_reused: item.state == QueueItemState::Ready
                    && item.certificate_id.is_some(),
                authorized_at: item.promotion_authorized_at.ok_or_else(|| {
                    ServiceError::Invariant(
                        "authorized candidate is missing its authorization timestamp".into(),
                    )
                })?,
            };
            runtime.store.record_command_result(
                state.id,
                command_id,
                "candidate-authorize",
                &request_digest,
                &result,
            )?;
            return Ok(result);
        }
        if runtime.git.optional_ref_oid(&item.source_ref).await? != Some(item.source_oid.clone()) {
            return Err(ServiceError::Invariant(
                "candidate source retention ref no longer names the exact submitted commit".into(),
            ));
        }
        for candidate in &items_to_authorize {
            if candidate.id == item.id {
                continue;
            }
            if runtime.git.optional_ref_oid(&candidate.source_ref).await?
                != Some(candidate.source_oid.clone())
            {
                return Err(ServiceError::Invariant(format!(
                    "candidate source retention ref for {} no longer names the exact submitted commit",
                    candidate.id
                )));
            }
        }
        let authorized_at = OffsetDateTime::now_utc();
        let authorized_item_ids = items_to_authorize
            .iter()
            .map(|candidate| candidate.id)
            .collect::<Vec<_>>();
        let authorized_ids = authorized_item_ids.iter().copied().collect::<HashSet<_>>();
        for candidate in &mut current {
            if authorized_ids.contains(&candidate.id) {
                candidate.promotion_authorized = true;
                candidate.promotion_authorized_at = Some(authorized_at);
                candidate.promotion_authorized_by = Some(command_id);
            }
        }

        let by_id = current
            .iter()
            .cloned()
            .map(|candidate| (candidate.id, candidate))
            .collect::<HashMap<_, _>>();
        let mut ordered_ids = Vec::with_capacity(current.len());
        let mut visiting = HashSet::new();
        fn append_with_active_dependencies(
            id: QueueItemId,
            by_id: &HashMap<QueueItemId, QueueItem>,
            ordered: &mut Vec<QueueItemId>,
            visiting: &mut HashSet<QueueItemId>,
        ) -> Result<(), ServiceError> {
            if ordered.contains(&id) {
                return Ok(());
            }
            if !visiting.insert(id) {
                return Err(ServiceError::Invariant(
                    "candidate dependency cycle prevents authorization priority".into(),
                ));
            }
            let candidate = by_id.get(&id).ok_or(ServiceError::ItemNotFound(id))?;
            for dependency in &candidate.dependencies {
                if by_id.contains_key(dependency) {
                    append_with_active_dependencies(*dependency, by_id, ordered, visiting)?;
                }
            }
            visiting.remove(&id);
            ordered.push(id);
            Ok(())
        }
        for candidate in current
            .iter()
            .filter(|candidate| candidate.promotion_authorized)
        {
            append_with_active_dependencies(candidate.id, &by_id, &mut ordered_ids, &mut visiting)?;
        }
        for candidate in &current {
            append_with_active_dependencies(candidate.id, &by_id, &mut ordered_ids, &mut visiting)?;
        }
        let restored_admission_order = restorable_admission_order(&current);
        if let Some(admission_order) = &restored_admission_order {
            let positions = admission_order
                .iter()
                .enumerate()
                .map(|(position, id)| (*id, position))
                .collect::<HashMap<_, _>>();
            if current.iter().any(|candidate| {
                candidate.dependencies.iter().any(|dependency| {
                    positions
                        .get(dependency)
                        .zip(positions.get(&candidate.id))
                        .is_some_and(|(dependency, candidate)| dependency >= candidate)
                })
            }) {
                return Err(ServiceError::Invariant(
                    "retained admission order violates an active dependency".into(),
                ));
            }
            ordered_ids.clone_from(admission_order);
        }
        let first_changed = current
            .iter()
            .map(|candidate| candidate.id)
            .zip(&ordered_ids)
            .position(|(before, after)| before != *after)
            .unwrap_or(current.len());
        let mut ordered = ordered_ids
            .iter()
            .map(|id| {
                by_id
                    .get(id)
                    .cloned()
                    .ok_or(ServiceError::ItemNotFound(*id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut generations = Vec::new();
        let mut restored_generations = Vec::new();
        let mut restored_item_ids = Vec::new();
        let mut restarted_item_ids = Vec::new();
        if first_changed < ordered.len() {
            runtime.git.initialize_mirror(&runtime.mirror).await?;
            let sources = ordered
                .iter()
                .map(|candidate| candidate.source_oid.clone())
                .collect::<Vec<_>>();
            // Conflicting candidates may hold release-anchored evidence in separate
            // speculative lanes. Reordering only needs to construct the longest viable
            // head prefix; candidates after its first conflict keep their existing lane
            // and evidence until the promoted head advances `release`.
            let mut composable_len = sources.len();
            let synthetic = loop {
                match runtime
                    .git
                    .construct_prefix(
                        &runtime.mirror,
                        &runtime.builder,
                        &state.master_oid,
                        &sources[..composable_len],
                    )
                    .await
                {
                    Ok(synthetic) => break synthetic,
                    Err(error) if error.is_synthetic_rejection() && composable_len > 1 => {
                        composable_len -= 1;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            reuse_active_enqueue_sequences(&current, &mut ordered);
            for (index, candidate) in ordered.iter_mut().enumerate() {
                if index < first_changed || index >= composable_len {
                    continue;
                }
                let commit = &synthetic[index];
                let desired_generation = ValidationGeneration::derive(
                    ValidationGenerationId::new(),
                    candidate.id,
                    state.master_oid.clone(),
                    ordered_ids[..=index].to_vec(),
                    sources[..=index].to_vec(),
                    synthetic[..=index]
                        .iter()
                        .map(|commit| commit.oid.clone())
                        .collect(),
                    commit.parent_oid.clone(),
                    commit.oid.clone(),
                    config.digest.clone(),
                    config.step_graph_digest.clone(),
                    state.engine_epoch,
                );
                if let Some(token) = runtime.cancellations.lock().get(&candidate.id) {
                    token.cancel();
                }
                if let Some((generation, buildset, certificate)) = matching_retained_evidence(
                    candidate,
                    &desired_generation,
                    &retained_generations,
                    &retained_buildsets,
                    &retained_certificates,
                    &config,
                    state.engine_epoch,
                ) {
                    candidate.state = QueueItemState::Ready;
                    candidate.current_generation_id = Some(generation.id);
                    candidate.buildset_id = Some(buildset.id);
                    candidate.certificate_id = Some(certificate.id);
                    restored_item_ids.push(candidate.id);
                    restored_generations.push(generation);
                    continue;
                }
                candidate.state = candidate
                    .state
                    .transition(ItemEvent::InputsChanged)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                let generation = desired_generation;
                candidate.current_generation_id = Some(generation.id);
                candidate.buildset_id = None;
                candidate.certificate_id = None;
                candidate.state = candidate
                    .state
                    .transition(ItemEvent::GenerationPrepared)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                restarted_item_ids.push(candidate.id);
                generations.push(generation);
            }
        }
        let prioritized_item = ordered
            .iter()
            .find(|candidate| candidate.id == item_id)
            .ok_or(ServiceError::ItemNotFound(item_id))?;
        let generation = generations
            .iter()
            .chain(&restored_generations)
            .chain(&retained_generations)
            .find(|generation| {
                generation.item_id == item_id
                    && Some(generation.id) == prioritized_item.current_generation_id
            })
            .unwrap_or(&original_generation);
        let validation_complete = prioritized_item.state == QueueItemState::Ready;
        let evidence_reused = validation_complete && prioritized_item.certificate_id.is_some();
        let result = CandidateAuthorizationResult {
            item_id,
            already_authorized: false,
            authorized_item_ids,
            restarted_item_ids,
            restored_item_ids,
            queue_revision: expected_revision + 1,
            source_oid: prioritized_item.source_oid.clone(),
            validation_generation_id: generation.id,
            tested_oid: generation.tested_oid.clone(),
            validation_complete,
            evidence_reused,
            authorized_at,
        };
        retain_speculative_generations(&runtime, &generations).await?;
        let event = runtime.store.authorize_candidate(
            &state,
            &ordered,
            &generations,
            &restored_generations,
            expected_revision,
            command_id,
            &request_digest,
            &result,
        )?;
        state.queue_revision += 1;
        state.event_sequence = event.sequence;
        {
            let mut data = runtime.data.lock();
            data.state = state;
            for candidate in ordered {
                if let Some(existing) = data.items.iter_mut().find(|entry| entry.id == candidate.id)
                {
                    *existing = candidate;
                }
            }
            data.items
                .sort_by_key(|candidate| candidate.enqueue_sequence);
            for generation in restored_generations {
                data.activate_generation(generation);
            }
            data.extend_generations(generations);
        }
        let _ = runtime.events.send(event);
        drop(mutation);
        self.spawn_eligible(repository_id, &runtime);
        self.promote_ready(repository_id).await?;
        Ok(result)
    }

    pub async fn check_from(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.check_from_with_purpose(
            repository_id,
            revision,
            worktree_path,
            command_id,
            "check".into(),
            CheckMode::Normal,
        )
        .await
    }

    async fn check_from_with_purpose(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
        purpose: String,
        mode: CheckMode,
    ) -> Result<ApproveResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let requested_revision = revision.clone();
        let requested_worktree = worktree_path.clone();
        let retained_source_oid = match &mode {
            CheckMode::Normal => None,
            CheckMode::RetainedCold(source_oid) => Some(source_oid.clone()),
        };
        let cold = matches!(mode, CheckMode::RetainedCold(_));
        let (mut state, mut config, mut enqueue_sequence) = {
            let data = runtime.data.lock();
            if data.state.execution_state != RepositoryExecutionState::Active {
                return Err(ServiceError::RepositoryUnavailable(
                    data.state.execution_state,
                ));
            }
            (
                data.state.clone(),
                data.config.clone(),
                data.items
                    .iter()
                    .map(|item| item.enqueue_sequence)
                    .max()
                    .unwrap_or(0)
                    + 1,
            )
        };
        let disk_config = EffectiveConfig::parse(
            &tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?,
        )?;
        if disk_config.digest != config.digest {
            let pending = {
                let mut data = runtime.data.lock();
                data.state.execution_state = RepositoryExecutionState::ConfigurationPending;
                data.state.clone()
            };
            runtime.store.update_repository_state(&pending)?;
            return Err(ServiceError::RepositoryUnavailable(
                RepositoryExecutionState::ConfigurationPending,
            ));
        }
        let approval_git = match worktree_path {
            Some(path) => {
                let discovered = GitRepository::discover(path).await?;
                if discovered.common_dir != runtime.git.common_dir {
                    return Err(ServiceError::Invariant(
                        "invoking worktree does not belong to the selected repository".into(),
                    ));
                }
                discovered
            }
            None => runtime.git.clone(),
        };
        let probe = match retained_source_oid.as_ref() {
            Some(source_oid) => approval_git.probe_retained_check(source_oid).await?,
            None => approval_git.probe_check(&revision).await?,
        };
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "kind": QueueItemKind::IndependentCheck,
            "revision": requested_revision,
            "worktree_path": requested_worktree,
            "purpose": purpose.clone(),
            "cold": cold,
            "retained_source_oid": retained_source_oid,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "check", &request_digest)?
        {
            return Ok(response);
        }
        let prepared = runtime.store.unfinished_approvals()?;
        if let Some((_, _, frozen_digest)) = prepared
            .iter()
            .find(|(prepared_id, _, _)| *prepared_id == command_id)
            && frozen_digest != &request_digest
        {
            return Err(StoreError::CommandReplayMismatch.into());
        }
        if !prepared.is_empty() {
            self.reconcile_approval_intents(&runtime).await?;
            if let Some(response) =
                runtime
                    .store
                    .checked_command_response(command_id, "check", &request_digest)?
            {
                return Ok(response);
            }
            let data = runtime.data.lock();
            state = data.state.clone();
            config = data.config.clone();
            enqueue_sequence = data
                .items
                .iter()
                .map(|item| item.enqueue_sequence)
                .max()
                .unwrap_or(0)
                + 1;
        }
        let item_id = QueueItemId::new();
        let mut item = QueueItem {
            id: item_id,
            repository_id,
            kind: QueueItemKind::IndependentCheck,
            admission_sequence: Some(enqueue_sequence),
            enqueue_sequence,
            source_oid: probe.source_oid.clone(),
            source_ref: format!("refs/tollgate/sources/{item_id}"),
            metadata: SourceMetadata {
                subject: probe.subject,
                message_hash: probe.message_hash,
                author_name: probe.author_name,
                author_email: probe.author_email,
                branch: probe.branch,
                worktree_path: Some(approval_git.worktree_root.to_string_lossy().into_owned()),
                signature_state: SignatureState::Unknown,
                approved_at: OffsetDateTime::now_utc(),
                purpose: Some(purpose),
            },
            state: QueueItemState::Constructing,
            terminal_reason: None,
            remote_state: RemoteState::Disabled,
            cleanup_state: CleanupState::NotEligible,
            cleanup_policy: CleanupPolicy::Automatic,
            dependencies: Vec::new(),
            promotion_authorized: false,
            promotion_authorized_at: None,
            promotion_authorized_by: None,
            current_generation_id: None,
            buildset_id: None,
            certificate_id: None,
        };
        runtime
            .store
            .prepare_approval(repository_id, &item, command_id, &request_digest)?;
        self.reserve_runtime_volume(
            &runtime,
            command_id,
            &runtime.git.common_dir,
            config.resources.volume_emergency_bytes,
        )
        .await?;
        runtime
            .git
            .create_source_ref(item_id, &item.source_oid)
            .await?;
        runtime.git.initialize_mirror(&runtime.mirror).await?;
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            probe.parent_oid.clone(),
            vec![item_id],
            vec![item.source_oid.clone()],
            vec![item.source_oid.clone()],
            probe.parent_oid,
            item.source_oid.clone(),
            config.digest.clone(),
            config.step_graph_digest.clone(),
            state.engine_epoch,
        );
        item.current_generation_id = Some(generation.id);
        item.state = item
            .state
            .transition(ItemEvent::GenerationPrepared)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let result = ApproveResult {
            item_id,
            queue_revision: state.queue_revision,
            source_oid: item.source_oid.clone(),
            tested_oid: generation.tested_oid.clone(),
        };
        let event = runtime.store.complete_check(
            &item,
            &generation,
            Actor::Ui,
            command_id,
            &request_digest,
            &result,
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.event_sequence = event.sequence;
            data.items.push(item);
            data.push_generation(generation);
        }
        let _ = runtime.events.send(event);
        if cold {
            runtime.cold_items.lock().insert(item_id);
        }
        self.spawn_eligible(repository_id, &runtime);
        Ok(result)
    }

    pub async fn pull(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<RemoteSyncResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let (state, config) = {
            let data = runtime.data.lock();
            (data.state.clone(), data.config.clone())
        };
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "pull", &request_digest)?
        {
            return Ok(response);
        }
        let frozen_affected = runtime
            .data
            .lock()
            .items
            .iter()
            .filter(|item| {
                item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state)
            })
            .map(|item| item.id)
            .collect::<Vec<_>>();
        let remote_fetch_url = runtime.git.remote_url(&config.remote.name, false).await?;
        let evidence = serde_json::json!({
            "request_digest": request_digest,
            "remote": config.remote.name,
            "remote_fetch_url": remote_fetch_url,
            "branch": config.remote.branch,
            "expected_local": state.master_oid,
            "affected_item_ids": frozen_affected,
        });
        runtime
            .store
            .prepare_operation(repository_id, "pull", command_id, &evidence)?;
        if let Err(error) = self
            .reserve_runtime_volume(
                &runtime,
                command_id,
                &runtime.git.common_dir,
                config.resources.volume_critical_bytes,
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "pull-volume-admission", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        if let Err(error) = self
            .require_global_volume_allowance(
                &runtime,
                &runtime.git.common_dir,
                config.resources.volume_emergency_bytes,
                "remote pull observation",
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "pull-pre-observation", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        let observation_ref = remote_observation_ref(&config.remote.name, &config.remote.branch);
        let remote = match runtime
            .git
            .fetch_remote_ref(&remote_fetch_url, &config.remote.branch, &observation_ref)
            .await
        {
            Ok(remote) => remote,
            Err(error) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"stage": "pull-fetch", "error": error.to_string()}),
                )?;
                return Err(error.into());
            }
        };
        runtime.store.record_remote_observation(
            repository_id,
            command_id,
            &config.remote.name,
            &format!("refs/heads/{}", config.remote.branch),
            remote.as_ref(),
            "fetch-exact-refspec",
        )?;
        if let Err(error) = self
            .require_global_volume_allowance(
                &runtime,
                &runtime.git.common_dir,
                0,
                "remote pull adoption",
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "pull-pre-adoption", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        let observed_local = match runtime.git.integration_oid().await {
            Ok(oid) => oid,
            Err(error) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"stage": "pull-local-observation", "error": error.to_string()}),
                )?;
                return Err(error.into());
            }
        };
        if observed_local != state.master_oid {
            runtime.store.set_intent_state(
                command_id,
                IntentState::NeedsAttention,
                &serde_json::json!({"observed_local": observed_local}),
            )?;
            return Err(ServiceError::Invariant(
                "release moved while pull held the repository mutation boundary".into(),
            ));
        }

        let mut next_state = state.clone();
        let mut affected = Vec::new();
        let (action, message) = if let Some(remote_oid) = remote.as_ref() {
            if remote_oid == &observed_local {
                (
                    RemoteSyncAction::UpToDate,
                    "Local release and remote master already match exactly.".to_owned(),
                )
            } else {
                let local_is_ancestor = match runtime
                    .git
                    .is_ancestor(&observed_local, remote_oid)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "pull-ancestry-check", "error": error.to_string()}),
                    )?;
                        return Err(error.into());
                    }
                };
                if local_is_ancestor {
                    if let Err(error) = runtime
                        .git
                        .compare_and_swap_integration(&observed_local, remote_oid)
                        .await
                    {
                        let current = runtime.git.integration_oid().await.ok();
                        runtime.store.set_intent_state(
                        command_id,
                        IntentState::NeedsAttention,
                        &serde_json::json!({"stage": "pull-local-cas", "error": error.to_string(), "observed_local": current}),
                    )?;
                        return Err(error.into());
                    }
                    next_state.master_oid = remote_oid.clone();
                    next_state.queue_revision += 1;
                    next_state.block_reasons.retain(|reason| {
                        !matches!(
                            reason.code.as_str(),
                            "remote-diverged"
                                | "external-master-movement"
                                | "external-release-movement"
                                | "remote-missing"
                        )
                    });
                    next_state.execution_state = if next_state.block_reasons.is_empty() {
                        RepositoryExecutionState::Active
                    } else {
                        RepositoryExecutionState::Blocked
                    };
                    (
                        RemoteSyncAction::AdoptedRemote,
                        format!("Adopted remote fast-forward {}.", remote_oid.short()),
                    )
                } else {
                    let remote_is_ancestor = match runtime
                        .git
                        .is_ancestor(remote_oid, &observed_local)
                        .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            runtime.store.set_intent_state(
                            command_id,
                            IntentState::Canceled,
                            &serde_json::json!({"stage": "pull-reverse-ancestry-check", "error": error.to_string()}),
                        )?;
                            return Err(error.into());
                        }
                    };
                    if remote_is_ancestor {
                        (
                            RemoteSyncAction::LocalAhead,
                            "Local release is ahead by a non-divergent certified chain.".to_owned(),
                        )
                    } else {
                        next_state.execution_state = RepositoryExecutionState::Blocked;
                        if !next_state
                            .block_reasons
                            .iter()
                            .any(|reason| reason.code == "remote-diverged")
                        {
                            next_state.block_reasons.push(BlockReason {
                        code: "remote-diverged".into(),
                        message: "Local release and remote master have diverged; Tollgate did not merge or rebase them.".into(),
                        recovery_action: "Inspect both exact tips and run `tg reconcile` after choosing the authoritative history.".into(),
                    });
                        }
                        (
                            RemoteSyncAction::Diverged,
                            "Remote divergence was recorded and the gate was blocked.".to_owned(),
                        )
                    }
                }
            }
        } else {
            (
                RemoteSyncAction::LocalAhead,
                "The configured remote branch does not exist; local release was left unchanged."
                    .to_owned(),
            )
        };
        if matches!(action, RemoteSyncAction::AdoptedRemote) {
            affected = runtime
                .data
                .lock()
                .items
                .iter()
                .filter(|item| {
                    item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state)
                })
                .map(|item| item.id)
                .collect();
        }
        let provisional = RemoteSyncResult {
            action: action.clone(),
            local_master: next_state.master_oid.clone(),
            remote_master: remote.clone(),
            queue_revision: next_state.queue_revision,
            affected_item_ids: affected.clone(),
            message,
        };
        if matches!(action, RemoteSyncAction::AdoptedRemote) {
            runtime.data.lock().state = next_state.clone();
            let rebuilt = self
                .rebuild_after_base_adoption(&runtime, &next_state.master_oid)
                .await?;
            if rebuilt != affected {
                return Err(ServiceError::Invariant(
                    "base-adoption impact changed before durable command completion".into(),
                ));
            }
        }
        // A base adoption rebuild persists item projection events. Use the
        // resulting state when allocating the command-completion sequence.
        if matches!(action, RemoteSyncAction::AdoptedRemote) {
            next_state = runtime.data.lock().state.clone();
        }
        let event = runtime.store.complete_operation(
            &next_state,
            "pull",
            command_id,
            "pull",
            &request_digest,
            &provisional,
            "remote.pull-completed",
            &serde_json::json!({"local": next_state.master_oid, "remote": remote}),
            Actor::Cli,
        )?;
        next_state.event_sequence = event.sequence;
        runtime.data.lock().state = next_state.clone();
        let _ = runtime.events.send(event);
        if matches!(action, RemoteSyncAction::AdoptedRemote) {
            self.spawn_eligible(repository_id, &runtime);
        }
        Ok(provisional)
    }

    pub async fn push(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<RemoteSyncResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let (state, config) = {
            let data = runtime.data.lock();
            (data.state.clone(), data.config.clone())
        };
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "push", &request_digest)?
        {
            return Ok(response);
        }
        let remote_fetch_url = runtime.git.remote_url(&config.remote.name, false).await?;
        let remote_push_url = runtime.git.remote_url(&config.remote.name, true).await?;
        let evidence = serde_json::json!({
            "request_digest": request_digest,
            "remote": config.remote.name,
            "remote_fetch_url": remote_fetch_url,
            "remote_push_url": remote_push_url,
            "branch": config.remote.branch,
            "new_oid": state.master_oid,
        });
        runtime
            .store
            .prepare_operation(repository_id, "push", command_id, &evidence)?;
        if let Err(error) = self
            .reserve_runtime_volume(
                &runtime,
                command_id,
                &runtime.git.common_dir,
                config.resources.volume_critical_bytes,
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "push-volume-admission", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        if let Err(error) = self
            .require_global_volume_allowance(
                &runtime,
                &runtime.git.common_dir,
                config.resources.volume_emergency_bytes,
                "remote push observation",
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "push-pre-observation", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        let remote = match runtime
            .git
            .fetch_remote_ref(
                &remote_push_url,
                &config.remote.branch,
                &remote_observation_ref(&config.remote.name, &config.remote.branch),
            )
            .await
        {
            Ok(remote) => remote,
            Err(error) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"stage": "push-fetch", "error": error.to_string()}),
                )?;
                return Err(error.into());
            }
        };
        runtime.store.record_remote_observation(
            repository_id,
            command_id,
            &config.remote.name,
            &format!("refs/heads/{}", config.remote.branch),
            remote.as_ref(),
            "push-preflight-fetch",
        )?;
        if let Err(error) = self
            .require_global_volume_allowance(
                &runtime,
                &runtime.git.common_dir,
                0,
                "remote push transition",
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "push-pre-transition", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        let action;
        let message;
        if remote.as_ref() == Some(&state.master_oid) {
            action = RemoteSyncAction::UpToDate;
            message = "Remote master already equals local release.".to_owned();
        } else {
            let Some(remote_oid) = remote.as_ref() else {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"stage": "push-missing-remote-ref"}),
                )?;
                return Err(ServiceError::Invariant(
                    "refusing to create a missing remote master without a recorded certified base"
                        .into(),
                ));
            };
            let ancestor = match runtime.git.is_ancestor(remote_oid, &state.master_oid).await {
                Ok(ancestor) => ancestor,
                Err(error) => {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "push-ancestry-check", "error": error.to_string()}),
                    )?;
                    return Err(error.into());
                }
            };
            if !ancestor {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"remote": remote_oid, "local": state.master_oid}),
                )?;
                return Err(ServiceError::Invariant(
                    "remote master is not an ancestor of local release; leased push refused".into(),
                ));
            }
            let outbound = match runtime
                .git
                .first_parent_commits_between(remote_oid, &state.master_oid)
                .await
            {
                Ok(outbound) => outbound,
                Err(error) => {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "push-chain-inspection", "error": error.to_string()}),
                    )?;
                    return Err(error.into());
                }
            };
            let promotion_edges = runtime.store.promotion_edges()?;
            let mut expected_parent = remote_oid.as_bytes().to_vec();
            let certified_chain = !outbound.is_empty()
                && outbound.iter().all(|oid| {
                    let found = promotion_edges.iter().any(|(old, promoted)| {
                        old == &expected_parent && promoted.as_slice() == oid.as_bytes()
                    });
                    if found {
                        expected_parent = oid.as_bytes().to_vec();
                    }
                    found
                });
            if !certified_chain || expected_parent.as_slice() != state.master_oid.as_bytes() {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"unpromoted_chain": outbound}),
                )?;
                return Err(ServiceError::Invariant(
                    "local release contains a commit without exact Tollgate promotion evidence"
                        .into(),
                ));
            }
            let push_error = runtime
                .git
                .push_with_lease(
                    &remote_push_url,
                    &config.remote.branch,
                    Some(remote_oid),
                    &state.master_oid,
                )
                .await
                .err();
            if let Some(push_error) = push_error {
                let observed = runtime
                    .git
                    .fetch_remote_ref(
                        &remote_push_url,
                        &config.remote.branch,
                        &remote_observation_ref(&config.remote.name, &config.remote.branch),
                    )
                    .await;
                match observed {
                    Ok(observed) => {
                        runtime.store.record_remote_observation(
                            repository_id,
                            command_id,
                            &config.remote.name,
                            &format!("refs/heads/{}", config.remote.branch),
                            observed.as_ref(),
                            "push-error-reobservation",
                        )?;
                        if observed.as_ref() == Some(&state.master_oid) {
                            runtime.store.set_intent_state(
                                command_id,
                                IntentState::ExternalApplied,
                                &serde_json::json!({"remote": state.master_oid, "recovered_after_error": push_error.to_string()}),
                            )?;
                        } else if observed.as_ref() == Some(remote_oid) {
                            runtime.store.set_intent_state(
                                command_id,
                                IntentState::Canceled,
                                &serde_json::json!({"remote": remote_oid, "push_error": push_error.to_string()}),
                            )?;
                            return Err(push_error.into());
                        } else {
                            runtime.store.set_intent_state(
                                command_id,
                                IntentState::NeedsAttention,
                                &serde_json::json!({"remote": observed, "push_error": push_error.to_string()}),
                            )?;
                            let blocked = block_for_remote_push_ambiguity(
                                &runtime,
                                "The push returned an error and the remote now matches neither frozen side of its exact lease.",
                            );
                            runtime.store.update_repository_state(&blocked)?;
                            return Err(push_error.into());
                        }
                    }
                    Err(observation_error) => {
                        runtime.store.set_intent_state(
                            command_id,
                            IntentState::NeedsAttention,
                            &serde_json::json!({"push_error": push_error.to_string(), "observation_error": observation_error.to_string()}),
                        )?;
                        let blocked = block_for_remote_push_ambiguity(
                            &runtime,
                            "The push returned an error and Tollgate could not re-observe the frozen remote ref.",
                        );
                        runtime.store.update_repository_state(&blocked)?;
                        return Err(push_error.into());
                    }
                }
            } else {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::ExternalApplied,
                    &serde_json::json!({"remote": state.master_oid}),
                )?;
            }
            action = RemoteSyncAction::Pushed;
            message = format!(
                "Pushed {} certified commit(s) with an exact lease.",
                outbound.len()
            );
        }
        let result = RemoteSyncResult {
            action,
            local_master: state.master_oid.clone(),
            remote_master: Some(state.master_oid.clone()),
            queue_revision: state.queue_revision,
            affected_item_ids: Vec::new(),
            message,
        };
        let pending = runtime
            .data
            .lock()
            .items
            .iter()
            .filter(|item| item.state == QueueItemState::PromotedLocalPushPending)
            .cloned()
            .collect::<Vec<_>>();
        if config.sync_user_master && !pending.is_empty() {
            self.sync_user_master_after_promotion(
                &runtime,
                pending.last().map(|item| item.id),
                &state.master_oid,
                Actor::Cli,
            )
            .await?;
        }
        for mut item in pending {
            item.state = item
                .state
                .transition(ItemEvent::PushCompleted)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.remote_state = RemoteState::Synchronized;
            self.replace_item(&runtime, item.clone())?;
            self.finish_source_cleanup(&runtime, item).await?;
        }
        let mut completion_state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &completion_state,
            "push",
            command_id,
            "push",
            &request_digest,
            &result,
            "remote.push-completed",
            &serde_json::json!({"remote": state.master_oid, "barriers": "synchronized"}),
            Actor::Cli,
        )?;
        completion_state.event_sequence = event.sequence;
        runtime.data.lock().state = completion_state;
        let _ = runtime.events.send(event);
        self.spawn_eligible(repository_id, &runtime);
        Ok(result)
    }

    pub async fn reconcile(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<RemoteSyncResult, ServiceError> {
        self.reconcile_expected(repository_id, None, None, command_id)
            .await
    }

    pub async fn reconcile_expected(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        expected_observed_master: Option<GitOid>,
        expected_queue_revision: Option<u64>,
        command_id: CommandId,
    ) -> Result<RemoteSyncResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "choice": "adopt-observed-local-and-abandon-pending-push",
            "expected_observed_master": expected_observed_master,
            "expected_queue_revision": expected_queue_revision,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "reconcile", &request_digest)?
        {
            return Ok(response);
        }
        let observed = runtime.git.integration_oid().await?;
        if expected_observed_master
            .as_ref()
            .is_some_and(|expected| expected != &observed)
        {
            return Err(ServiceError::Invariant(
                "reconciliation preview is stale because the observed master changed".into(),
            ));
        }
        let (mut state, pending_pushes) = {
            let data = runtime.data.lock();
            (
                data.state.clone(),
                data.items
                    .iter()
                    .filter(|item| item.state == QueueItemState::PromotedLocalPushPending)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        };
        if expected_queue_revision.is_some_and(|expected| expected != state.queue_revision) {
            return Err(ServiceError::RevisionConflict {
                expected: expected_queue_revision.unwrap_or_default(),
                actual: state.queue_revision,
            });
        }
        runtime.store.prepare_operation(
            repository_id,
            "reconcile",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "persisted_master": state.master_oid,
                "observed_master": observed,
            }),
        )?;
        let changed_base = state.master_oid != observed;
        if changed_base {
            state.master_oid = observed.clone();
            state.queue_revision += 1;
        }
        state.block_reasons.retain(|reason| {
            !matches!(
                reason.code.as_str(),
                "external-master-movement"
                    | "external-release-movement"
                    | "remote-diverged"
                    | "remote-preflight-mismatch"
                    | "ambiguous-pull-recovery"
                    | "ambiguous-push-recovery"
            )
        });
        state.execution_state = if state.block_reasons.is_empty() {
            RepositoryExecutionState::Active
        } else {
            RepositoryExecutionState::Blocked
        };
        let affected = if changed_base {
            runtime
                .data
                .lock()
                .items
                .iter()
                .filter(|item| {
                    item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state)
                })
                .map(|item| item.id)
                .collect()
        } else {
            Vec::new()
        };
        let result = RemoteSyncResult {
            action: RemoteSyncAction::ReconciledLocal,
            local_master: observed.clone(),
            remote_master: None,
            queue_revision: state.queue_revision,
            affected_item_ids: affected.clone(),
            message: if changed_base {
                "Adopted the observed local release as an unvalidated external base and rebuilt active prefixes."
                    .into()
            } else {
                "Confirmed the observed local release and cleared resolved reconciliation blocks."
                    .into()
            },
        };
        runtime.store.update_repository_state(&state)?;
        runtime.store.set_intent_state(
            command_id,
            IntentState::ExternalApplied,
            &serde_json::json!({"adopted_master": observed}),
        )?;
        runtime.data.lock().state = state;
        for mut item in pending_pushes {
            item.state = item
                .state
                .transition(ItemEvent::PushAbandoned)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.remote_state = RemoteState::Abandoned;
            item.terminal_reason = Some("remote-promise-abandoned-by-reconcile".into());
            self.replace_item(&runtime, item.clone())?;
            self.finish_source_cleanup(&runtime, item).await?;
        }
        for (intent_id, _, _, _) in runtime.store.unfinished_operations(&["push"])? {
            runtime.store.set_intent_state(
                intent_id,
                IntentState::Canceled,
                &serde_json::json!({"recovery": "abandoned-by-reconcile"}),
            )?;
        }
        if changed_base {
            let rebuilt = self
                .rebuild_after_base_adoption(&runtime, &observed)
                .await?;
            if rebuilt != affected {
                return Err(ServiceError::Invariant(
                    "reconciliation impact changed after durable preview".into(),
                ));
            }
        }
        let mut completed_state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &completed_state,
            "reconcile",
            command_id,
            "reconcile",
            &request_digest,
            &result,
            "repository.reconciled",
            &serde_json::json!({"adopted_master": observed}),
            Actor::Cli,
        )?;
        completed_state.event_sequence = event.sequence;
        runtime.data.lock().state = completed_state;
        let _ = runtime.events.send(event);
        self.reconcile_approval_intents(&runtime).await?;
        self.spawn_eligible(repository_id, &runtime);
        Ok(result)
    }

    pub async fn create_worktree(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        branch: String,
        destination: Option<String>,
        warm: bool,
        command_id: CommandId,
    ) -> Result<WorktreeOperationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let state = runtime.data.lock().state.clone();
        let destination = match destination {
            Some(path) => PathBuf::from(path),
            None => {
                let repository = Path::new(&state.path);
                let parent = repository.parent().ok_or_else(|| {
                    ServiceError::Invariant("repository has no parent directory".into())
                })?;
                let base = repository
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("worktree");
                parent.join(format!("{base}-{}", branch.replace('/', "-")))
            }
        };
        if !destination.is_absolute()
            || destination
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(ServiceError::Invariant(
                "worktree destination must be an absolute normalized path".into(),
            ));
        }
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "branch": branch,
            "destination": destination,
            "warm": warm,
        }))?;
        if let Some(response) = runtime.store.checked_command_response(
            command_id,
            "worktree-create",
            &request_digest,
        )? {
            return Ok(response);
        }
        runtime.store.prepare_operation(
            repository_id,
            "worktree-create",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "branch": branch,
                "destination": destination,
                "base": state.master_oid,
                "warm": warm,
            }),
        )?;
        let oid = runtime
            .git
            .create_feature_worktree(&branch, &destination)
            .await?;
        let hydration = if warm {
            let seed = {
                let data = runtime.data.lock();
                compatible_seed(&data)?
            };
            match seed {
                Some(seed) => match self
                    .require_global_volume_warning(&runtime, &destination, "warm worktree creation")
                    .await
                {
                    Ok(()) => match self
                        .hydrate_feature_worktree(&runtime, &seed, &destination, command_id)
                        .await
                    {
                        Ok(()) => format!(
                            " Hydrated {} logical bytes from APFS seed {}.",
                            seed.logical_size, seed.id
                        ),
                        Err(error) => format!(
                            " Cache hydration was unavailable, so the worktree remains cold: {error}."
                        ),
                    },
                    Err(error) => format!(
                        " Cache hydration was unavailable, so the worktree remains cold: {error}."
                    ),
                },
                None => {
                    " No compatible cache seed is published, so the worktree remains cold.".into()
                }
            }
        } else {
            String::new()
        };
        let result = WorktreeOperationResult {
            action: "created".into(),
            path: destination.to_string_lossy().into_owned(),
            branch: Some(branch),
            old_oid: None,
            new_oid: Some(oid.clone()),
            message: format!(
                "Created a feature worktree from gated release {}.{hydration}",
                oid.short(),
            ),
        };
        let event = runtime.store.complete_operation(
            &state,
            "worktree-create",
            command_id,
            "worktree-create",
            &request_digest,
            &result,
            "worktree.created",
            &result,
            Actor::Cli,
        )?;
        runtime.data.lock().state.event_sequence = event.sequence;
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn remove_worktree(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        path: String,
        command_id: CommandId,
    ) -> Result<WorktreeOperationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let requested = PathBuf::from(path);
        let path_identity = GitRepository::directory_identity(&requested)?;
        let path = std::fs::canonicalize(&requested)?;
        let registration = runtime
            .git
            .registered_worktree(&path)
            .await?
            .ok_or_else(|| {
                ServiceError::Invariant(
                    "requested path is not an exact registered linked worktree".into(),
                )
            })?;
        let branch = registration.branch.ok_or_else(|| {
            ServiceError::Invariant("refusing to remove a detached worktree".into())
        })?;
        let oid = registration.head;
        if path == runtime.git.worktree_root
            || matches!(branch.as_str(), USER_BRANCH | INTEGRATION_BRANCH)
        {
            return Err(ServiceError::Invariant(
                "primary, master, or release worktrees cannot be removed by Tollgate".into(),
            ));
        }
        let worktree = GitRepository::discover(&path).await?;
        if worktree.worktree_root != path {
            return Err(ServiceError::Invariant(
                "requested path only discovers an ancestor repository".into(),
            ));
        }
        if worktree.common_dir != runtime.git.common_dir {
            return Err(ServiceError::Invariant(
                "worktree belongs to a different registered repository".into(),
            ));
        }
        if worktree.current_branch().await?.as_deref() != Some(&branch)
            || worktree.resolve_oid("HEAD").await? != oid
        {
            return Err(ServiceError::Invariant(
                "registered worktree identity changed while preparing removal".into(),
            ));
        }
        if runtime.data.lock().items.iter().any(|item| {
            !item.state.is_terminal()
                && (item.source_oid == oid
                    || item.metadata.worktree_path.as_deref()
                        == Some(path.to_string_lossy().as_ref()))
        }) {
            return Err(ServiceError::Invariant(
                "worktree is the retained source of an active queue item or check".into(),
            ));
        }
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "path": path,
        }))?;
        if let Some(response) = runtime.store.checked_command_response(
            command_id,
            "worktree-remove",
            &request_digest,
        )? {
            return Ok(response);
        }
        runtime.store.prepare_operation(
            repository_id,
            "worktree-remove",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "path": path,
                "branch": branch,
                "expected_oid": oid,
                "common_dir": runtime.git.common_dir,
                "path_identity": path_identity,
            }),
        )?;
        if !runtime
            .git
            .cleanup_linked_source_worktree(&path, &branch, &oid)
            .await?
        {
            return Err(ServiceError::Invariant(
                "primary or master worktrees cannot be removed by Tollgate".into(),
            ));
        }
        let state = runtime.data.lock().state.clone();
        let result = WorktreeOperationResult {
            action: "removed".into(),
            path: path.to_string_lossy().into_owned(),
            branch: Some(branch),
            old_oid: Some(oid),
            new_oid: None,
            message: "Removed the verified clean linked worktree and its unchanged branch.".into(),
        };
        let event = runtime.store.complete_operation(
            &state,
            "worktree-remove",
            command_id,
            "worktree-remove",
            &request_digest,
            &result,
            "worktree.removed",
            &result,
            Actor::Cli,
        )?;
        runtime.data.lock().state.event_sequence = event.sequence;
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn update_feature_worktree(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        path: String,
        command_id: CommandId,
    ) -> Result<WorktreeOperationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let worktree = GitRepository::discover(&path).await?;
        if worktree.common_dir != runtime.git.common_dir {
            return Err(ServiceError::Invariant(
                "invoking worktree belongs to a different registered repository".into(),
            ));
        }
        let old = worktree.resolve_oid("HEAD").await?;
        if runtime
            .data
            .lock()
            .items
            .iter()
            .any(|item| !item.state.is_terminal() && item.source_oid == old)
        {
            return Err(ServiceError::Invariant(
                "an approved immutable source cannot be rewritten; cancel it or create a new change"
                    .into(),
            ));
        }
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "path": worktree.worktree_root,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "update", &request_digest)?
        {
            return Ok(response);
        }
        runtime.store.prepare_operation(
            repository_id,
            "update",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "path": worktree.worktree_root,
                "old_oid": old,
                "master": runtime.data.lock().state.master_oid,
            }),
        )?;
        let (old, new) = worktree.update_one_commit_feature().await?;
        let state = runtime.data.lock().state.clone();
        let result = WorktreeOperationResult {
            action: if old == new {
                "already-current"
            } else {
                "updated"
            }
            .into(),
            path: worktree.worktree_root.to_string_lossy().into_owned(),
            branch: worktree.current_branch().await?,
            old_oid: Some(old.clone()),
            new_oid: Some(new.clone()),
            message: if old == new {
                "Feature commit already has current gated release as its parent.".into()
            } else {
                format!("Rebased one clean feature commit to {}.", new.short())
            },
        };
        let event = runtime.store.complete_operation(
            &state,
            "update",
            command_id,
            "update",
            &request_digest,
            &result,
            "worktree.updated",
            &result,
            Actor::Cli,
        )?;
        runtime.data.lock().state.event_sequence = event.sequence;
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn retry(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        cold: bool,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "item_id": item_id,
            "cold": cold,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "retry", &request_digest)?
        {
            return Ok(response);
        }
        let (source, kind, promotion_authorized, cleanup_policy, worktree_path, branch, purpose) = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            if !matches!(
                item.state,
                QueueItemState::Failed
                    | QueueItemState::MergeConflict
                    | QueueItemState::Canceled
                    | QueueItemState::CheckFailed
                    | QueueItemState::InfrastructureExhausted
            ) {
                return Err(ServiceError::Invariant(format!(
                    "queue item {item_id} is not retryable from {:?}",
                    item.state
                )));
            }
            (
                item.source_oid.to_hex(),
                item.kind,
                item.promotion_authorized,
                item.cleanup_policy,
                item.metadata.worktree_path.clone(),
                item.metadata.branch.clone(),
                item.metadata.purpose.clone(),
            )
        };
        let source_oid = runtime.git.resolve_oid(&source).await?;
        if let Some(path) = worktree_path.as_deref() {
            let registration = runtime
                .git
                .registered_worktree(Path::new(path))
                .await?
                .ok_or_else(|| {
                    ServiceError::Invariant(
                        "retry source path is no longer an exact registered worktree".into(),
                    )
                })?;
            if registration.head != source_oid || registration.branch != branch {
                return Err(ServiceError::Invariant(
                    "retry source worktree no longer matches its recorded branch and OID".into(),
                ));
            }
        }
        let child_command_id = if let Some(evidence) =
            runtime.store.operation_evidence(command_id, "retry")?
        {
            if evidence
                .get("request_digest")
                .and_then(|value| value.as_str())
                != Some(request_digest.as_str())
            {
                return Err(StoreError::CommandReplayMismatch.into());
            }
            serde_json::from_value(evidence.get("child_command_id").cloned().ok_or_else(|| {
                ServiceError::Invariant("retry intent is missing its child command UUID".into())
            })?)?
        } else {
            let child_command_id = CommandId::new();
            runtime.store.prepare_operation(
                repository_id,
                "retry",
                command_id,
                &serde_json::json!({
                    "request_digest": request_digest,
                    "item_id": item_id,
                    "cold": cold,
                    "source": source,
                    "kind": kind,
                    "cleanup_policy": cleanup_policy,
                    "worktree_path": worktree_path.clone(),
                    "branch": branch.clone(),
                    "child_command_id": child_command_id,
                }),
            )?;
            child_command_id
        };
        if cold {
            runtime.cold_sources.lock().insert(source_oid.clone());
        }
        let result = if kind == QueueItemKind::IndependentCheck {
            self.check_from(repository_id, source, worktree_path, child_command_id)
                .await
        } else if promotion_authorized {
            self.approve_from_with_cleanup_policy(
                repository_id,
                source,
                worktree_path,
                purpose.filter(|purpose| purpose == "push-master"),
                cleanup_policy,
                child_command_id,
            )
            .await
        } else {
            self.submit_candidate_from_with_cleanup_policy(
                repository_id,
                source,
                worktree_path,
                cleanup_policy,
                child_command_id,
            )
            .await
        };
        if result.is_err() {
            runtime.cold_sources.lock().remove(&source_oid);
        }
        let result = result?;
        if cold {
            // The retry intent is already durable. Keying the execution policy
            // to the exact newly-created item also lets startup reconstruct it
            // if the process exits before dispatch.
            runtime.cold_items.lock().insert(result.item_id);
        }
        let _mutation = runtime.mutation.lock().await;
        let state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &state,
            "retry",
            command_id,
            "retry",
            &request_digest,
            &result,
            "queue.retried",
            &serde_json::json!({
                "item_id": item_id,
                "cold": cold,
                "child_command_id": child_command_id,
                "result_item_id": result.item_id,
            }),
            Actor::Cli,
        )?;
        runtime.data.lock().state.event_sequence = event.sequence;
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn reorder_queue(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        selected_ids: Vec<QueueItemId>,
        expected_revision: u64,
        command_id: CommandId,
    ) -> Result<QueueReorderResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "selected_ids": selected_ids,
            "expected_revision": expected_revision,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "reorder", &request_digest)?
        {
            return Ok(response);
        }
        let (mut state, current, config) = {
            let data = runtime.data.lock();
            if data.state.execution_state != RepositoryExecutionState::Active {
                return Err(ServiceError::RepositoryUnavailable(
                    data.state.execution_state,
                ));
            }
            if data.state.queue_revision != expected_revision {
                return Err(ServiceError::RevisionConflict {
                    expected: expected_revision,
                    actual: data.state.queue_revision,
                });
            }
            (
                data.state.clone(),
                data.items
                    .iter()
                    .filter(|item| {
                        item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state)
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
                data.config.clone(),
            )
        };
        if selected_ids.is_empty() {
            return Err(ServiceError::Invariant(
                "reorder requires at least one queue item".into(),
            ));
        }
        let by_id = current
            .iter()
            .map(|item| (item.id, item.clone()))
            .collect::<HashMap<_, _>>();
        if selected_ids.iter().any(|id| !by_id.contains_key(id)) {
            return Err(ServiceError::Invariant(
                "reorder selected an item outside the active queue".into(),
            ));
        }
        let mut ordered_ids = Vec::with_capacity(current.len());
        let mut visiting = HashSet::new();
        fn append_with_dependencies(
            id: QueueItemId,
            by_id: &HashMap<QueueItemId, QueueItem>,
            ordered: &mut Vec<QueueItemId>,
            visiting: &mut HashSet<QueueItemId>,
        ) -> Result<(), ServiceError> {
            if ordered.contains(&id) {
                return Ok(());
            }
            if !visiting.insert(id) {
                return Err(ServiceError::Invariant(
                    "queue dependency cycle prevents reorder".into(),
                ));
            }
            let item = by_id.get(&id).ok_or(ServiceError::ItemNotFound(id))?;
            for dependency in &item.dependencies {
                if by_id.contains_key(dependency) {
                    append_with_dependencies(*dependency, by_id, ordered, visiting)?;
                }
            }
            visiting.remove(&id);
            ordered.push(id);
            Ok(())
        }
        for id in selected_ids {
            append_with_dependencies(id, &by_id, &mut ordered_ids, &mut visiting)?;
        }
        for item in &current {
            append_with_dependencies(item.id, &by_id, &mut ordered_ids, &mut visiting)?;
        }
        let first_changed = current
            .iter()
            .map(|item| item.id)
            .zip(&ordered_ids)
            .position(|(before, after)| before != *after)
            .unwrap_or(current.len());
        let mut ordered = ordered_ids
            .iter()
            .map(|id| {
                by_id
                    .get(id)
                    .cloned()
                    .ok_or(ServiceError::ItemNotFound(*id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        runtime.git.initialize_mirror(&runtime.mirror).await?;
        let sources = ordered
            .iter()
            .map(|item| item.source_oid.clone())
            .collect::<Vec<_>>();
        let synthetic = runtime
            .git
            .construct_prefix(
                &runtime.mirror,
                &runtime.builder,
                &state.master_oid,
                &sources,
            )
            .await?;
        let mut generations = Vec::new();
        reuse_active_enqueue_sequences(&current, &mut ordered);
        for item in &mut ordered {
            item.admission_sequence = Some(item.enqueue_sequence);
        }
        for (index, item) in ordered.iter_mut().enumerate() {
            if index < first_changed {
                continue;
            }
            if let Some(token) = runtime.cancellations.lock().get(&item.id) {
                token.cancel();
            }
            item.state = item
                .state
                .transition(ItemEvent::InputsChanged)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            let commit = &synthetic[index];
            let generation = ValidationGeneration::derive(
                ValidationGenerationId::new(),
                item.id,
                state.master_oid.clone(),
                ordered_ids[..=index].to_vec(),
                sources[..=index].to_vec(),
                synthetic[..=index]
                    .iter()
                    .map(|commit| commit.oid.clone())
                    .collect(),
                commit.parent_oid.clone(),
                commit.oid.clone(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
                state.engine_epoch,
            );
            item.current_generation_id = Some(generation.id);
            item.buildset_id = None;
            item.certificate_id = None;
            item.state = item
                .state
                .transition(ItemEvent::GenerationPrepared)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            generations.push(generation);
        }
        state.queue_revision += 1;
        let response = QueueReorderResult {
            queue_revision: state.queue_revision,
            ordered_item_ids: ordered_ids,
            restarted_item_ids: ordered[first_changed..]
                .iter()
                .map(|item| item.id)
                .collect(),
        };
        retain_speculative_generations(&runtime, &generations).await?;
        let event = runtime.store.replace_queue_structure(
            &state,
            &ordered,
            &generations,
            command_id,
            "reorder",
            &request_digest,
            &response,
        )?;
        {
            let mut data = runtime.data.lock();
            data.state = state;
            data.state.event_sequence = event.sequence;
            for item in ordered {
                if let Some(existing) = data.items.iter_mut().find(|entry| entry.id == item.id) {
                    *existing = item;
                }
            }
            data.items.sort_by_key(|item| item.enqueue_sequence);
            data.extend_generations(generations);
        }
        let _ = runtime.events.send(event);
        self.spawn_eligible(repository_id, &runtime);
        Ok(response)
    }

    pub async fn unregister_repository(
        self: &Arc<Self>,
        repository_id: RepositoryId,
    ) -> Result<(), ServiceError> {
        self.unregister_repository_command(repository_id, CommandId::new())
            .await?;
        Ok(())
    }

    pub async fn unregister_repository_command(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<MutationResult, ServiceError> {
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
        }))?;
        let command_prepared = self
            .global_commands
            .lock()
            .await
            .records
            .contains_key(&command_id.to_string());
        let runtime = self.runtime(repository_id).await.ok();
        if runtime.is_none()
            && !command_prepared
            && !self
                .unavailable
                .read()
                .await
                .iter()
                .any(|repository| repository.id == repository_id)
        {
            return Err(ServiceError::RepositoryNotFound(repository_id));
        }
        if let Some(runtime) = &runtime
            && runtime
                .data
                .lock()
                .items
                .iter()
                .any(|item| !item.state.is_terminal())
        {
            return Err(ServiceError::Invariant(
                "pause and drain or cancel the active queue before removing the repository".into(),
            ));
        }
        if let Some(response) = self
            .prepare_global_command(
                "remove-repository",
                command_id,
                &request_digest,
                serde_json::json!({"repository_id": repository_id}),
            )
            .await?
        {
            return Ok(serde_json::from_value(response)?);
        }
        let mutation = if let Some(runtime) = &runtime {
            Some(runtime.mutation.lock().await)
        } else {
            None
        };
        if runtime.is_some() {
            self.runtimes.write().await.remove(&repository_id);
            self.reconfigure_global_scheduler().await;
        }
        self.unavailable
            .write()
            .await
            .retain(|entry| entry.id != repository_id);
        self.save_registry().await?;
        drop(mutation);
        let result = MutationResult {
            repository_id,
            action: "remove-repository".into(),
            message: "Repository removed from the explicit Tollgate registry; repository-local state was preserved.".into(),
        };
        self.complete_global_command(command_id, &result).await?;
        Ok(result)
    }

    pub async fn apply_configuration(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<RepositorySnapshot, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let path = runtime.git.worktree_root.join(".tollgate/config.toml");
        let config_text = tokio::fs::read_to_string(path).await?;
        let candidate = EffectiveConfig::parse(&config_text)?;
        let active_digest = runtime.data.lock().config.digest.clone();
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "active_digest": active_digest,
            "candidate_digest": candidate.digest,
        }))?;
        if runtime
            .store
            .checked_command_response::<MutationResult>(
                command_id,
                "config-apply",
                &request_digest,
            )?
            .is_some()
        {
            return self.repository_snapshot(repository_id).await;
        }
        runtime.store.prepare_operation(
            repository_id,
            "config-apply",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "active_digest": active_digest,
                "candidate_digest": candidate.digest,
            }),
        )?;
        let unchanged_state = {
            let mut data = runtime.data.lock();
            (data.config.digest == candidate.digest).then(|| {
                data.state.execution_state = if data.state.block_reasons.is_empty() {
                    RepositoryExecutionState::Active
                } else {
                    RepositoryExecutionState::Blocked
                };
                data.state.clone()
            })
        };
        if let Some(mut state) = unchanged_state {
            mirror_legacy_configuration(&runtime.git, &config_text).await?;
            let result = MutationResult {
                repository_id,
                action: "config-apply".into(),
                message: "Configuration already matches the active policy.".into(),
            };
            let event = runtime.store.complete_operation(
                &state,
                "config-apply",
                command_id,
                "config-apply",
                &request_digest,
                &result,
                "configuration.confirmed",
                &result,
                Actor::App,
            )?;
            state.event_sequence = event.sequence;
            runtime.data.lock().state = state;
            let _ = runtime.events.send(event);
            return self.repository_snapshot(repository_id).await;
        }
        let (items, old_digest, state) = {
            let mut data = runtime.data.lock();
            if data
                .items
                .iter()
                .any(|item| item.state == QueueItemState::PromotedLocalPushPending)
                && (data.config.remote != candidate.remote)
            {
                return Err(ServiceError::Invariant(
                    "configuration cannot change a frozen remote promise until push or reconcile completes"
                        .into(),
                ));
            }
            let items = data
                .items
                .iter()
                .filter(|item| {
                    matches!(
                        item.state,
                        QueueItemState::Constructing
                            | QueueItemState::Queued
                            | QueueItemState::Preparing
                            | QueueItemState::Running
                            | QueueItemState::Ready
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let old_digest = data.config.digest.clone();
            data.state.queue_revision += 1;
            data.state.active_configuration_digest = candidate.digest.clone();
            data.state.remote_enabled = candidate.remote.enabled;
            data.state.execution_state = if data.state.block_reasons.is_empty() {
                RepositoryExecutionState::Active
            } else {
                RepositoryExecutionState::Blocked
            };
            data.config = candidate.clone();
            (items, old_digest, data.state.clone())
        };
        let tokens = runtime
            .cancellations
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for token in tokens {
            token.cancel();
        }
        runtime.scheduler_epoch.fetch_add(1, Ordering::AcqRel);
        *runtime.execution_permits.write().await = Arc::new(Semaphore::new(usize::from(
            candidate.resources.repository_concurrency,
        )));
        self.reconfigure_global_scheduler().await;
        let result = MutationResult {
            repository_id,
            action: "config-apply".into(),
            message: format!("Activated configuration {}.", &candidate.digest[..12]),
        };
        runtime.store.stage_configuration(
            &state,
            &candidate.canonical_bytes()?,
            &candidate.step_graph_digest,
            &old_digest,
            command_id,
            &request_digest,
        )?;

        if !items.is_empty() {
            runtime.git.initialize_mirror(&runtime.mirror).await?;
            let gate_items = items
                .iter()
                .filter(|item| item.kind == QueueItemKind::Gate)
                .cloned()
                .collect::<Vec<_>>();
            let item_ids = gate_items.iter().map(|item| item.id).collect::<Vec<_>>();
            let sources = gate_items
                .iter()
                .map(|item| item.source_oid.clone())
                .collect::<Vec<_>>();
            let chain = if sources.is_empty() {
                Vec::new()
            } else {
                runtime
                    .git
                    .construct_prefix(
                        &runtime.mirror,
                        &runtime.builder,
                        &state.master_oid,
                        &sources,
                    )
                    .await?
            };
            for (position, mut item) in gate_items.into_iter().enumerate() {
                let synthetic = &chain[position];
                let generation = ValidationGeneration::derive(
                    ValidationGenerationId::new(),
                    item.id,
                    state.master_oid.clone(),
                    item_ids[..=position].to_vec(),
                    sources[..=position].to_vec(),
                    chain[..=position]
                        .iter()
                        .map(|commit| commit.oid.clone())
                        .collect(),
                    synthetic.parent_oid.clone(),
                    synthetic.oid.clone(),
                    candidate.digest.clone(),
                    candidate.step_graph_digest.clone(),
                    state.engine_epoch,
                );
                item.state = item
                    .state
                    .transition(ItemEvent::InputsChanged)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.current_generation_id = Some(generation.id);
                item.buildset_id = None;
                item.certificate_id = None;
                item.state = item
                    .state
                    .transition(ItemEvent::GenerationPrepared)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                retain_speculative_generations(&runtime, std::slice::from_ref(&generation)).await?;
                runtime.store.replace_generation(&generation)?;
                runtime.data.lock().push_generation(generation);
                self.replace_item(&runtime, item.clone())?;
            }
            for mut item in items
                .into_iter()
                .filter(|item| item.kind == QueueItemKind::IndependentCheck)
            {
                let parent = runtime.git.commit_parent_oid(&item.source_oid).await?;
                let generation = ValidationGeneration::derive(
                    ValidationGenerationId::new(),
                    item.id,
                    parent.clone(),
                    vec![item.id],
                    vec![item.source_oid.clone()],
                    vec![item.source_oid.clone()],
                    parent,
                    item.source_oid.clone(),
                    candidate.digest.clone(),
                    candidate.step_graph_digest.clone(),
                    state.engine_epoch,
                );
                item.state = item
                    .state
                    .transition(ItemEvent::InputsChanged)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.current_generation_id = Some(generation.id);
                item.buildset_id = None;
                item.certificate_id = None;
                item.state = item
                    .state
                    .transition(ItemEvent::GenerationPrepared)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                runtime.store.replace_generation(&generation)?;
                runtime.data.lock().push_generation(generation);
                self.replace_item(&runtime, item)?;
            }
            self.spawn_eligible(repository_id, &runtime);
        }
        mirror_legacy_configuration(&runtime.git, &config_text).await?;
        let mut completed_state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &completed_state,
            "config-apply",
            command_id,
            "config-apply",
            &request_digest,
            &result,
            "configuration.activated",
            &serde_json::json!({
                "digest": candidate.digest,
                "step_graph_digest": candidate.step_graph_digest,
                "supersedes": old_digest,
            }),
            Actor::Ui,
        )?;
        completed_state.event_sequence = event.sequence;
        runtime.data.lock().state = completed_state;
        let _ = runtime.events.send(event);
        self.repository_snapshot(repository_id).await
    }

    pub async fn validate_configuration(
        &self,
        repository_id: RepositoryId,
    ) -> Result<EffectiveConfig, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        Ok(EffectiveConfig::parse(
            &tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?,
        )?)
    }

    pub async fn doctor(&self, repository_id: RepositoryId) -> Result<DoctorReport, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let (state, config, slots) = {
            let data = runtime.data.lock();
            (
                data.state.clone(),
                data.config.clone(),
                data.slots.values().cloned().collect::<Vec<_>>(),
            )
        };
        let mut checks = Vec::new();
        let mut push =
            |name: &str, healthy: bool, detail: String, recovery_action: Option<String>| {
                checks.push(DiagnosticCheck {
                    name: name.into(),
                    status: if healthy {
                        DiagnosticStatus::Healthy
                    } else {
                        DiagnosticStatus::Attention
                    },
                    detail,
                    recovery_action,
                });
            };

        let execution_healthy = state.execution_state != RepositoryExecutionState::Blocked;
        let execution_detail = if state.block_reasons.is_empty() {
            format!("{:?}", state.execution_state)
        } else {
            state
                .block_reasons
                .iter()
                .map(|reason| format!("{}: {}", reason.code, reason.message))
                .collect::<Vec<_>>()
                .join("; ")
        };
        let execution_recovery = (!execution_healthy).then(|| {
            state
                .block_reasons
                .iter()
                .map(|reason| reason.recovery_action.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        });
        push(
            "Repository execution",
            execution_healthy,
            execution_detail,
            execution_recovery,
        );

        let integrity = runtime.store.integrity_check()?;
        let sqlite_healthy = integrity.len() == 1 && integrity[0] == "ok";
        push(
            "SQLite integrity",
            sqlite_healthy,
            integrity.join("; "),
            (!sqlite_healthy).then(|| {
                "Pause the repository and preserve this database before restoring a verified backup."
                    .into()
            }),
        );
        let observed_master = runtime.git.integration_oid().await?;
        let master_healthy = observed_master == state.master_oid;
        push(
            "Authoritative release",
            master_healthy,
            format!(
                "observed {} · recorded {}",
                observed_master.short(),
                state.master_oid.short()
            ),
            (!master_healthy).then(|| "Run `tg reconcile` before resuming dispatch.".into()),
        );
        let mirror_healthy = runtime
            .git
            .mirror_tree_oid(&runtime.mirror, &state.master_oid)
            .await
            .is_ok();
        push(
            "Execution mirror",
            mirror_healthy,
            if mirror_healthy {
                "Recorded release is reachable in the isolated execution mirror.".into()
            } else {
                "Recorded release is not provable in the execution mirror.".into()
            },
            (!mirror_healthy).then(|| {
                "Recreate the mirror from authoritative retained refs before running CI.".into()
            }),
        );
        let config_on_disk = EffectiveConfig::parse(
            &tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?,
        );
        let config_healthy = config_on_disk
            .as_ref()
            .is_ok_and(|candidate| candidate.digest == config.digest);
        push(
            "Trusted configuration",
            config_healthy,
            match config_on_disk {
                Ok(candidate) => format!(
                    "disk {} · active {}",
                    &candidate.digest[..12],
                    &config.digest[..12]
                ),
                Err(error) => error.to_string(),
            },
            (!config_healthy).then(|| {
                "Validate, review, and explicitly apply the pending configuration.".into()
            }),
        );
        let runner = &config.runner[0];
        let runner_path = resolve_executable(runner, &self.environment.read().await.variables);
        push(
            "Runner executable",
            runner_path.is_some(),
            runner_path
                .as_ref()
                .map_or_else(|| format!("`{runner}` was not found"), |path| path.display().to_string()),
            runner_path.is_none().then(|| {
                "Repair the login-shell PATH or configure an absolute runner path, then reload the environment."
                    .into()
            }),
        );
        let mut unhealthy_slots = Vec::new();
        for slot in &slots {
            let healthy = !std::fs::symlink_metadata(&slot.path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
                && match GitRepository::discover(&slot.path).await {
                    Ok(repository) => {
                        paths_identical(&repository.common_dir, &runtime.mirror).await
                    }
                    Err(_) => false,
                };
            if !healthy {
                unhealthy_slots.push(slot.id.to_string());
            }
        }
        push(
            "Persistent slots",
            unhealthy_slots.is_empty(),
            if unhealthy_slots.is_empty() {
                format!("{} registered slot(s) passed ownership checks", slots.len())
            } else {
                format!("unhealthy slot(s): {}", unhealthy_slots.join(", "))
            },
            (!unhealthy_slots.is_empty())
                .then(|| "Reset each unhealthy slot cold before reuse.".into()),
        );

        let cache_root = runtime
            .slots_root
            .parent()
            .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?;
        let probe_id = uuid::Uuid::now_v7();
        let probe_source = cache_root.join(format!(".doctor-source-{probe_id}"));
        let probe_destination = cache_root.join(format!(".doctor-clone-{probe_id}"));
        std::fs::create_dir(&probe_source)?;
        std::fs::write(probe_source.join("probe"), b"tollgate-clone-probe")?;
        let clone_result = force_clone_tree(&probe_source, &probe_destination);
        let _ = std::fs::remove_dir_all(&probe_source);
        let _ = std::fs::remove_dir_all(&probe_destination);
        push(
            "APFS force-clone",
            clone_result.is_ok(),
            clone_result
                .as_ref()
                .map_or_else(|error| error.to_string(), |_| "force-clone probe succeeded".into()),
            clone_result.is_err().then(|| {
                "Move Tollgate cache storage to a clone-capable APFS volume before publishing seeds."
                    .into()
            }),
        );
        let volume = nix::sys::statvfs::statvfs(&runtime.git.common_dir)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let available = u64::from(volume.blocks_available()).saturating_mul(volume.fragment_size());
        let space_healthy = available >= 10 * 1024 * 1024 * 1024;
        push(
            "Authoritative volume reserve",
            space_healthy,
            format!("{} bytes available", available),
            (!space_healthy).then(|| {
                "Free at least 10 GiB on the Git/SQLite volume before admitting new work.".into()
            }),
        );
        if config.remote.enabled {
            let remote = runtime
                .git
                .observe_remote_ref(&config.remote.name, &config.remote.branch)
                .await;
            let remote_healthy = remote.is_ok();
            push(
                "Remote exact ref",
                remote_healthy,
                match remote {
                    Ok(Some(oid)) => format!("observed {}", oid.short()),
                    Ok(None) => "configured remote ref is absent".into(),
                    Err(error) => error.to_string(),
                },
                (!remote_healthy).then(|| {
                    "Repair remote reachability or credentials; Tollgate will not guess lease state."
                        .into()
                }),
            );
        }
        let healthy = checks
            .iter()
            .all(|check| matches!(check.status, DiagnosticStatus::Healthy));
        Ok(DoctorReport {
            repository_id,
            generated_at: OffsetDateTime::now_utc(),
            checks,
            healthy,
        })
    }

    pub async fn regenerate_configuration(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<EffectiveConfig, ServiceError> {
        use tokio::io::AsyncWriteExt;

        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "template": "auto-detect-v1",
        }))?;
        if let Some(response) = runtime.store.checked_command_response(
            command_id,
            "config-regenerate",
            &request_digest,
        )? {
            return Ok(response);
        }
        let command = detect_command(&runtime.git.worktree_root);
        let contents = format!(
            "version = 1\n\n[[step]]\nname = \"ci\"\nrun = {}\n",
            toml_string(&command)
        );
        let candidate = EffectiveConfig::parse(&contents)?;
        let path = runtime.git.worktree_root.join(".tollgate/config.toml");
        let old_bytes = tokio::fs::read(&path).await.unwrap_or_default();
        runtime.store.prepare_operation(
            repository_id,
            "config-regenerate",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "path": path,
                "old_hash": blake3::hash(&old_bytes).to_hex().to_string(),
                "new_digest": candidate.digest,
            }),
        )?;
        let temporary = path.with_extension(format!("toml.{}.tmp", uuid::Uuid::now_v7()));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(contents.as_bytes()).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temporary, &path).await?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        let mut state = runtime.data.lock().state.clone();
        state.execution_state = if candidate.digest == state.active_configuration_digest {
            if state.block_reasons.is_empty() {
                RepositoryExecutionState::Active
            } else {
                RepositoryExecutionState::Blocked
            }
        } else {
            RepositoryExecutionState::ConfigurationPending
        };
        let event = runtime.store.complete_operation(
            &state,
            "config-regenerate",
            command_id,
            "config-regenerate",
            &request_digest,
            &candidate,
            "configuration.regenerated",
            &serde_json::json!({"path": path, "digest": candidate.digest}),
            Actor::Cli,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(candidate)
    }

    pub async fn set_artifact_pinned(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        artifact_id: String,
        pinned: bool,
        command_id: CommandId,
    ) -> Result<MutationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "artifact_id": artifact_id,
            "pinned": pinned,
        }))?;
        if let Some(result) =
            runtime
                .store
                .checked_command_response(command_id, "artifact-pin", &request_digest)?
        {
            return Ok(result);
        }
        let record = runtime.store.artifact(&artifact_id)?.ok_or_else(|| {
            ServiceError::Invariant(format!("artifact {artifact_id} was not found"))
        })?;
        verify_retained_artifact(&runtime, &record).await?;
        let result = MutationResult {
            repository_id,
            action: if pinned {
                "artifact-pin".into()
            } else {
                "artifact-unpin".into()
            },
            message: if pinned {
                format!("Pinned retained artifact {}.", record.source_path)
            } else {
                format!(
                    "Returned artifact {} to timed retention.",
                    record.source_path
                )
            },
        };
        let mut state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_artifact_state_change(
            &state,
            &artifact_id,
            &["retained", "pinned"],
            if pinned { "pinned" } else { "retained" },
            None,
            command_id,
            "artifact-pin",
            &request_digest,
            &result,
            if pinned {
                "artifact.pinned"
            } else {
                "artifact.unpinned"
            },
            Actor::App,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn prune_artifact(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        artifact_id: String,
        command_id: CommandId,
    ) -> Result<MutationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        self.prune_artifact_locked(&runtime, artifact_id, command_id, Actor::App)
            .await
    }

    async fn prune_expired_artifacts(
        self: &Arc<Self>,
        repository_id: RepositoryId,
    ) -> Result<(), ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let expired = runtime.store.expired_artifacts(OffsetDateTime::now_utc())?;
        for record in expired {
            let protects_active_certificate = runtime.data.lock().items.iter().any(|item| {
                item.buildset_id == Some(record.buildset_id) && !item.state.is_terminal()
            });
            if protects_active_certificate {
                continue;
            }
            let _mutation = runtime.mutation.lock().await;
            self.prune_artifact_locked(
                &runtime,
                record.artifact_id,
                CommandId::new(),
                Actor::Recovery,
            )
            .await?;
        }
        Ok(())
    }

    async fn prune_artifact_locked(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        artifact_id: String,
        command_id: CommandId,
        actor: Actor,
    ) -> Result<MutationResult, ServiceError> {
        let repository_id = runtime.data.lock().state.id;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "artifact_id": artifact_id,
        }))?;
        if let Some(result) =
            runtime
                .store
                .checked_command_response(command_id, "artifact-prune", &request_digest)?
        {
            return Ok(result);
        }
        let record = runtime.store.artifact(&artifact_id)?.ok_or_else(|| {
            ServiceError::Invariant(format!("artifact {artifact_id} was not found"))
        })?;
        if runtime
            .data
            .lock()
            .items
            .iter()
            .any(|item| item.buildset_id == Some(record.buildset_id) && !item.state.is_terminal())
        {
            return Err(ServiceError::Invariant(
                "artifacts bound to an active queue certificate cannot be pruned".into(),
            ));
        }
        verify_retained_artifact(runtime, &record).await?;
        let artifacts_root = runtime.git.common_dir.join("tollgate/artifacts");
        let quarantine_root = artifacts_root.join(".quarantine");
        tokio::fs::create_dir_all(&quarantine_root).await?;
        verify_owned_directory(&artifacts_root, &quarantine_root)?;
        let original_path = PathBuf::from(&record.retained_path);
        let quarantine_path = quarantine_root.join(format!("{}-{command_id}", record.artifact_id));
        if tokio::fs::try_exists(&quarantine_path).await? {
            return Err(ServiceError::Invariant(
                "artifact pruning quarantine destination already exists".into(),
            ));
        }
        let evidence = ArtifactPruneEvidence {
            repository_id,
            request_digest: request_digest.clone(),
            record: record.clone(),
            original_path: original_path.clone(),
            quarantine_path: quarantine_path.clone(),
        };
        runtime
            .store
            .prepare_operation(repository_id, "artifact-prune", command_id, &evidence)?;
        tokio::fs::rename(&original_path, &quarantine_path).await?;
        sync_directory(
            original_path
                .parent()
                .ok_or_else(|| ServiceError::Invariant("artifact has no parent".into()))?,
        )?;
        sync_directory(&quarantine_root)?;
        verify_quarantined_artifact(&artifacts_root, &evidence).await?;
        runtime.store.set_intent_state(
            command_id,
            IntentState::ExternalApplied,
            &serde_json::json!({"quarantine": quarantine_path}),
        )?;
        let result = MutationResult {
            repository_id,
            action: "artifact-prune".into(),
            message: format!("Pruned retained artifact {}.", record.source_path),
        };
        let mut state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_artifact_state_change(
            &state,
            &artifact_id,
            &["retained", "pinned"],
            "pruned",
            Some("artifact-prune"),
            command_id,
            "artifact-prune",
            &request_digest,
            &result,
            "artifact.pruned",
            actor,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        tokio::fs::remove_file(&quarantine_path).await?;
        sync_directory(&quarantine_root)?;
        Ok(result)
    }

    pub async fn reset_slot(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        slot_id: SlotId,
        command_id: CommandId,
    ) -> Result<SlotView, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let slot = runtime
            .data
            .lock()
            .slots
            .get(&slot_id)
            .cloned()
            .ok_or_else(|| ServiceError::Invariant(format!("slot {slot_id} does not exist")))?;
        if !matches!(slot.state.as_str(), "idle" | "quarantined") {
            return Err(ServiceError::Invariant(
                "slot reset requires an idle slot; cancel and wait for its worker first".into(),
            ));
        }
        let slots_root = std::fs::canonicalize(&runtime.slots_root)?;
        let slot_path = std::fs::canonicalize(&slot.path)?;
        if slot_path.parent() != Some(slots_root.as_path())
            || std::fs::symlink_metadata(&slot_path)?
                .file_type()
                .is_symlink()
        {
            return Err(ServiceError::Invariant(
                "slot path is not an owned direct child of the configured slot root".into(),
            ));
        }
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "slot_id": slot_id,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "slot-reset", &request_digest)?
        {
            return Ok(response);
        }
        let checkout = slot
            .checkout_oid
            .clone()
            .unwrap_or_else(|| runtime.data.lock().state.master_oid.clone());
        let quarantine = runtime
            .slots_root
            .parent()
            .ok_or_else(|| ServiceError::Invariant("slot root has no cache parent".into()))?
            .join("quarantine")
            .join(format!("{slot_id}-{}", uuid::Uuid::now_v7()));
        runtime.store.prepare_operation(
            repository_id,
            "slot-reset",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "slot": slot,
                "quarantine": quarantine,
                "checkout": checkout,
            }),
        )?;
        runtime
            .git
            .quarantine_slot(&runtime.mirror, &slot_path, &quarantine)
            .await?;
        runtime.git.initialize_mirror(&runtime.mirror).await?;
        runtime
            .git
            .provision_slot(&runtime.mirror, &slot.path, &checkout)
            .await?;
        let reset = SlotView {
            id: slot_id,
            path: slot.path,
            state: "idle".into(),
            checkout_oid: Some(checkout),
            health: "healthy".into(),
            last_used: Some(OffsetDateTime::now_utc()),
        };
        let state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &state,
            "slot-reset",
            command_id,
            "slot-reset",
            &request_digest,
            &reset,
            "slot.reset",
            &serde_json::json!({"slot": reset, "quarantine": quarantine}),
            Actor::Cli,
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.event_sequence = event.sequence;
            data.slots.insert(slot_id, reset.clone());
        }
        let _ = runtime.events.send(event);
        Ok(reset)
    }

    pub async fn snapshot_cache(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        command_id: CommandId,
    ) -> Result<CacheOperationResult, ServiceError> {
        use tokio::io::AsyncWriteExt;

        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let (state, config, donor) = {
            let data = runtime.data.lock();
            let donor = data
                .slots
                .values()
                .filter(|slot| slot.state == "idle" && slot.health == "healthy")
                .max_by_key(|slot| slot.last_used)
                .cloned()
                .ok_or_else(|| {
                    ServiceError::Invariant(
                        "cache snapshot requires a healthy idle donor slot".into(),
                    )
                })?;
            (data.state.clone(), data.config.clone(), donor)
        };
        let mut selected = config
            .cache
            .paths
            .iter()
            .filter(|entry| entry.policy == CachePolicy::Clone)
            .map(|entry| PathBuf::from(&entry.path))
            .collect::<Vec<_>>();
        if selected.is_empty() {
            selected = runtime.git.ignored_directories(&donor.path).await?;
        }
        if selected.is_empty() {
            return Err(ServiceError::Invariant(
                "the idle donor contains no eligible ignored cache directories".into(),
            ));
        }
        selected.sort();
        selected.dedup();
        const SEED_LOGICAL_BUDGET: u64 = 200 * 1024 * 1024 * 1024;
        let donor_root = std::fs::canonicalize(&donor.path)?;
        let mut estimated_size = 0u64;
        for relative in &selected {
            let source = std::fs::canonicalize(donor.path.join(relative))?;
            if !source.starts_with(&donor_root) || !source.is_dir() {
                return Err(ServiceError::Invariant(format!(
                    "cache path `{}` is not a real directory beneath the donor slot",
                    relative.display()
                )));
            }
            estimated_size = estimated_size.saturating_add(tree_logical_size(&source)?);
        }
        let published_size = runtime
            .data
            .lock()
            .seeds
            .iter()
            .filter(|seed| seed.state == "published")
            .map(|seed| seed.logical_size)
            .sum::<u64>();
        if published_size.saturating_add(estimated_size) > SEED_LOGICAL_BUDGET {
            return Err(ServiceError::Invariant(
                "cache snapshot would exceed the 200 GiB repository seed budget".into(),
            ));
        }
        let cache_root = runtime
            .slots_root
            .parent()
            .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?;
        let cache_policy_digest = command_digest(&config.cache)?;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "slot_id": donor.id,
            "cache_epoch": config.cache.epoch,
            "selected": selected,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "cache-snapshot", &request_digest)?
        {
            return Ok(response);
        }
        let generation = runtime
            .data
            .lock()
            .seeds
            .iter()
            .map(|seed| seed.generation)
            .max()
            .unwrap_or(0)
            + 1;
        let seed_id = uuid::Uuid::now_v7().to_string();
        let seeds_root = runtime
            .slots_root
            .parent()
            .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?
            .join("seeds/default");
        tokio::fs::create_dir_all(&seeds_root).await?;
        let staging = seeds_root.join(format!(".staging-{seed_id}"));
        let destination = seeds_root.join(format!("{}-{seed_id}", config.cache.epoch));
        let evidence = SeedSnapshotEvidence {
            repository_id,
            request_digest: request_digest.clone(),
            seed_id: seed_id.clone(),
            generation,
            staging: staging.clone(),
            destination: destination.clone(),
            source_slot: donor.id,
            source_oid: donor.checkout_oid.clone(),
            selected: selected.clone(),
            cache_epoch: config.cache.epoch,
            cache_policy_digest: cache_policy_digest.clone(),
            configuration_digest: config.digest.clone(),
            os: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
        };
        runtime
            .store
            .prepare_operation(repository_id, "cache-snapshot", command_id, &evidence)?;
        if let Err(error) = self
            .reserve_runtime_volume(&runtime, command_id, cache_root, estimated_size)
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "cache-snapshot-volume-admission", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        if let Err(error) = self
            .require_global_volume_warning(&runtime, cache_root, "cache snapshot")
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "cache-snapshot-warning-admission", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        tokio::fs::create_dir(&staging).await?;
        let mut entries = Vec::new();
        let mut logical_size = 0u64;
        for relative in &selected {
            let source = std::fs::canonicalize(donor.path.join(relative))?;
            if !source.starts_with(&donor_root) || !source.is_dir() {
                return Err(ServiceError::Invariant(format!(
                    "cache path `{}` is not a real directory beneath the donor slot",
                    relative.display()
                )));
            }
            let target = staging.join(relative);
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let manifest = force_clone_tree(&source, &target)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            logical_size = logical_size.saturating_add(manifest.logical_size);
            entries.push(serde_json::json!({
                "path": relative,
                "clone_manifest": manifest,
            }));
        }
        let manifest = serde_json::json!({
            "version": 1,
            "seed_id": seed_id,
            "generation": generation,
            "repository_id": repository_id,
            "profile": "default",
            "cache_epoch": config.cache.epoch,
            "cache_policy_digest": cache_policy_digest,
            "configuration_digest": config.digest,
            "os": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
            "source_slot": donor.id,
            "source_oid": donor.checkout_oid,
            "logical_size": logical_size,
            "entries": entries,
        });
        let manifest_path = staging.join(".tollgate-seed-manifest.json");
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&manifest_path)
            .await?;
        file.write_all(&serde_json::to_vec_pretty(&manifest)?)
            .await?;
        file.sync_all().await?;
        sync_tree_directories(&staging)?;
        let staged_record = verify_seed_publication(&staging, &evidence)?;
        tokio::fs::rename(&staging, &destination).await?;
        sync_directory(&seeds_root)?;
        runtime.store.set_intent_state(
            command_id,
            IntentState::ExternalApplied,
            &serde_json::json!({"destination": destination}),
        )?;
        let record = verify_seed_publication(&destination, &evidence)?;
        if record.logical_size != logical_size || record.manifest != staged_record.manifest {
            return Err(ServiceError::Invariant(
                "published seed differs from its verified staging generation".into(),
            ));
        }
        let result = CacheOperationResult {
            action: "snapshot".into(),
            seed_ids: vec![seed_id],
            slots_reset: Vec::new(),
            logical_bytes: logical_size,
            message: format!(
                "Published an immutable APFS seed containing {} logical bytes.",
                logical_size
            ),
        };
        let event = runtime.store.complete_seed_publication(
            &state,
            command_id,
            &request_digest,
            &record,
            &result,
            Actor::Cli,
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.event_sequence = event.sequence;
            data.seeds.push(record);
        }
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn purge_cache(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        all_slots: bool,
        command_id: CommandId,
    ) -> Result<CacheOperationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let state = runtime.data.lock().state.clone();
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "all_slots": all_slots,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "cache-purge", &request_digest)?
        {
            return Ok(response);
        }
        let seeds = runtime
            .data
            .lock()
            .seeds
            .iter()
            .filter(|seed| seed.state == "published")
            .cloned()
            .collect::<Vec<_>>();
        let idle_slots = if all_slots {
            runtime
                .data
                .lock()
                .slots
                .values()
                .filter(|slot| slot.state == "idle")
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let cache_root = std::fs::canonicalize(
            runtime
                .slots_root
                .parent()
                .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?,
        )?;
        let quarantine_root = cache_root.join("quarantine");
        tokio::fs::create_dir_all(&quarantine_root).await?;
        verify_owned_directory(&cache_root, &quarantine_root)?;
        let mut seed_evidence = Vec::new();
        for seed in &seeds {
            let original = std::fs::canonicalize(&seed.path)?;
            if !original.starts_with(&cache_root)
                || std::fs::symlink_metadata(&original)?
                    .file_type()
                    .is_symlink()
            {
                return Err(ServiceError::Invariant(
                    "seed path failed owned-cache identity verification".into(),
                ));
            }
            verify_seed_record_at(&original, seed)?;
            seed_evidence.push(SeedPruneEvidence {
                record: seed.clone(),
                original,
                quarantine: quarantine_root.join(format!("seed-{}-{}", seed.id, command_id)),
            });
        }
        let slot_evidence = idle_slots
            .iter()
            .map(|slot| SlotPruneEvidence {
                checkout: slot
                    .checkout_oid
                    .clone()
                    .unwrap_or_else(|| state.master_oid.clone()),
                quarantine: quarantine_root.join(format!("slot-{}-{command_id}", slot.id)),
                slot: slot.clone(),
            })
            .collect();
        let evidence = CachePurgeEvidence {
            repository_id,
            request_digest: request_digest.clone(),
            seeds: seed_evidence,
            slots: slot_evidence,
        };
        runtime
            .store
            .prepare_operation(repository_id, "cache-purge", command_id, &evidence)?;
        let mut pruned_ids = Vec::new();
        let mut logical_bytes = 0u64;
        for seed in &evidence.seeds {
            if tokio::fs::try_exists(&seed.quarantine).await? {
                return Err(ServiceError::Invariant(
                    "seed quarantine destination already exists".into(),
                ));
            }
            tokio::fs::rename(&seed.original, &seed.quarantine).await?;
            verify_seed_record_at(&seed.quarantine, &seed.record)?;
            pruned_ids.push(seed.record.id.clone());
            logical_bytes = logical_bytes.saturating_add(seed.record.logical_size);
        }
        let mut slots_reset = Vec::new();
        for slot_evidence in &evidence.slots {
            let slot = &slot_evidence.slot;
            let path = std::fs::canonicalize(&slot.path)?;
            if path.parent() != Some(std::fs::canonicalize(&runtime.slots_root)?.as_path()) {
                return Err(ServiceError::Invariant(
                    "idle slot path escaped the owned slot root".into(),
                ));
            }
            runtime
                .git
                .quarantine_slot(&runtime.mirror, &path, &slot_evidence.quarantine)
                .await?;
            runtime.git.initialize_mirror(&runtime.mirror).await?;
            runtime
                .git
                .provision_slot(&runtime.mirror, &slot.path, &slot_evidence.checkout)
                .await?;
            let mut reset = slot.clone();
            reset.checkout_oid = Some(slot_evidence.checkout.clone());
            reset.last_used = Some(OffsetDateTime::now_utc());
            reset.health = "healthy".into();
            runtime.data.lock().slots.insert(reset.id, reset);
            slots_reset.push(slot.id);
        }
        let result = CacheOperationResult {
            action: "purge".into(),
            seed_ids: pruned_ids.clone(),
            slots_reset,
            logical_bytes,
            message: format!(
                "Purged {} seed generation(s){}.",
                pruned_ids.len(),
                if all_slots {
                    " and recreated every idle slot cold"
                } else {
                    ""
                }
            ),
        };
        runtime.store.set_intent_state(
            command_id,
            IntentState::ExternalApplied,
            &serde_json::json!({"quarantined_seeds": result.seed_ids, "reset_slots": result.slots_reset}),
        )?;
        let event = runtime.store.complete_cache_purge(
            &state,
            command_id,
            &request_digest,
            &seeds,
            &result,
            Actor::Cli,
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.event_sequence = event.sequence;
            for seed in &mut data.seeds {
                if pruned_ids.contains(&seed.id) {
                    seed.state = "pruned".into();
                }
            }
        }
        let _ = runtime.events.send(event);
        for seed in &evidence.seeds {
            tokio::fs::remove_dir_all(&seed.quarantine).await?;
        }
        for slot in &evidence.slots {
            remove_owned_quarantine(&cache_root, &slot.quarantine)?;
        }
        sync_directory(&quarantine_root)?;
        Ok(result)
    }

    async fn execute_item(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
    ) -> Result<(), ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let buildset_id = BuildsetId::new();
        let cancellation = CancellationToken::new();
        runtime
            .cancellations
            .lock()
            .insert(item_id, cancellation.clone());
        loop {
            let volumes = observe_volumes(&runtime)?;
            if volumes
                .iter()
                .all(|volume| volume.available_bytes >= volume.warning_threshold)
            {
                break;
            }
            if runtime.data.lock().state.execution_state != RepositoryExecutionState::Active {
                return Ok(());
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }
        }
        let dispatch_request = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            if item.state != QueueItemState::Queued
                || data.state.execution_state != RepositoryExecutionState::Active
            {
                return Ok(());
            }
            let queue_position = data
                .items
                .iter()
                .filter(|candidate| {
                    candidate.kind == QueueItemKind::Gate && !candidate.state.is_terminal()
                })
                .position(|candidate| candidate.id == item_id)
                .unwrap_or(0);
            DispatchRequest {
                repository_id,
                buildset_id,
                priority: match item.kind {
                    QueueItemKind::Gate if queue_position == 0 => PriorityClass::GateHead,
                    QueueItemKind::Gate => PriorityClass::Speculative,
                    QueueItemKind::IndependentCheck => PriorityClass::Independent,
                },
                queue_position: u16::try_from(queue_position).unwrap_or(u16::MAX),
                repository_weight: data.config.resources.scheduler_weight,
                affinity_score: i64::from(
                    data.slots
                        .values()
                        .any(|slot| slot.state == "idle" && slot.health == "healthy"),
                ),
            }
        };
        let scheduler_epoch = runtime.scheduler_epoch.load(Ordering::Acquire);
        let repository_scheduler = runtime.execution_permits.read().await.clone();
        let _repository_permit = tokio::select! {
            permit = repository_scheduler.acquire_owned() => permit.map_err(|_| {
                ServiceError::Invariant("repository execution scheduler closed".into())
            })?,
            _ = cancellation.cancelled() => return Ok(()),
        };
        if scheduler_epoch != runtime.scheduler_epoch.load(Ordering::Acquire) {
            return Ok(());
        }
        let _global_allocation = match self
            .global_scheduler
            .acquire_buildset(dispatch_request, &cancellation)
            .await
        {
            Ok(allocation) => allocation,
            Err(SchedulerError::Canceled) => return Ok(()),
            Err(error) => {
                return Err(ServiceError::Invariant(format!(
                    "global buildset admission failed: {error}"
                )));
            }
        };
        if scheduler_epoch != runtime.scheduler_epoch.load(Ordering::Acquire) {
            return Ok(());
        }
        let dispatch_guard = runtime.mutation.lock().await;
        let config_text =
            tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?;
        let disk_config = EffectiveConfig::parse(&config_text);
        if disk_config.is_err() {
            let state = {
                let mut data = runtime.data.lock();
                data.state.execution_state = RepositoryExecutionState::ConfigurationPending;
                data.state.clone()
            };
            runtime.store.update_repository_state(&state)?;
            return Ok(());
        }
        let disk_config = disk_config?;
        let pending_state = {
            let mut data = runtime.data.lock();
            (disk_config.digest != data.config.digest).then(|| {
                data.state.execution_state = RepositoryExecutionState::ConfigurationPending;
                data.state.clone()
            })
        };
        if let Some(state) = pending_state {
            runtime.store.update_repository_state(&state)?;
            return Ok(());
        }
        let (mut item, generation, config, retry_of, attempt) = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .cloned()
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            if item.state != QueueItemState::Queued {
                return Ok(());
            }
            if data.state.execution_state != RepositoryExecutionState::Active {
                return Ok(());
            }
            if item.kind == QueueItemKind::Gate {
                let active_position = data
                    .items
                    .iter()
                    .filter(|candidate| {
                        candidate.kind == QueueItemKind::Gate && !candidate.state.is_terminal()
                    })
                    .position(|candidate| candidate.id == item_id)
                    .ok_or(ServiceError::ItemNotFound(item_id))?;
                if active_position >= data.state.active_window as usize {
                    return Ok(());
                }
            }
            let generation = data
                .generations
                .iter()
                .find(|generation| Some(generation.id) == item.current_generation_id)
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("current generation missing".into()))?;
            let previous = data
                .buildsets
                .iter()
                .filter(|buildset| {
                    buildset.item_id == item_id
                        && buildset.validation_generation_id == generation.id
                })
                .max_by_key(|buildset| buildset.attempt);
            (
                item,
                generation,
                data.config.clone(),
                previous.map(|buildset| buildset.id),
                previous.map_or(1, |buildset| buildset.attempt + 1),
            )
        };
        let environment = self.environment.read().await.clone();
        let cold_item = runtime.cold_items.lock().remove(&item.id);
        let cold_source = runtime.cold_sources.lock().remove(&item.source_oid);
        let cold = cold_item || cold_source;
        let (slot_id, slot_path, seed) = {
            let mut data = runtime.data.lock();
            if !cold
                && let Some(slot) = data
                    .slots
                    .values_mut()
                    .find(|slot| slot.state == "idle" && slot.health == "healthy")
            {
                slot.state = "preparing".into();
                (slot.id, slot.path.clone(), None)
            } else {
                let id = SlotId::new();
                let path = runtime.slots_root.join(id.to_string());
                let seed = if cold { None } else { compatible_seed(&data)? };
                data.slots.insert(
                    id,
                    SlotView {
                        id,
                        path: path.clone(),
                        state: "preparing".into(),
                        checkout_oid: None,
                        health: "healthy".into(),
                        last_used: None,
                    },
                );
                (id, path, seed)
            }
        };
        let mut buildset = Buildset {
            id: buildset_id,
            item_id,
            validation_generation_id: generation.id,
            tested_oid: generation.tested_oid.clone(),
            expected_parent_oid: generation.expected_parent_oid.clone(),
            environment_fingerprint: environment.fingerprint.clone(),
            slot_id: Some(slot_id),
            state: BuildsetState::Pending,
            retry_of,
            attempt,
            created_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            frozen_steps: freeze_steps(buildset_id, &config),
            step_results: Vec::new(),
        };
        runtime.store.insert_buildset(&buildset)?;
        buildset.state = buildset
            .state
            .transition(BuildsetEvent::PreparationStarted)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        runtime.store.update_buildset(&buildset)?;
        runtime.data.lock().buildsets.push(buildset.clone());
        item.buildset_id = Some(buildset_id);
        item.state = item
            .state
            .transition(ItemEvent::PreparationStarted)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        self.replace_item(&runtime, item.clone())?;
        drop(dispatch_guard);
        runtime
            .git
            .provision_slot(&runtime.mirror, &slot_path, &generation.tested_oid)
            .await?;
        if let Some(seed) = seed
            && let Err(seed_error) = self.import_seed_into_slot(&runtime, &seed, &slot_path)
        {
            let quarantine = runtime
                .slots_root
                .parent()
                .ok_or_else(|| ServiceError::Invariant("slot root has no cache parent".into()))?
                .join("quarantine")
                .join(format!("seed-import-{slot_id}-{}", uuid::Uuid::now_v7()));
            runtime
                .git
                .quarantine_slot(&runtime.mirror, &slot_path, &quarantine)
                .await?;
            runtime
                .git
                .provision_slot(&runtime.mirror, &slot_path, &generation.tested_oid)
                .await?;
            eprintln!(
                "Tollgate seed {} could not seed slot {} and cold provisioning was used: {}",
                seed.id, slot_id, seed_error
            );
        }
        let start_guard = runtime.mutation.lock().await;
        let fresh_item = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .find(|candidate| candidate.id == item_id)
                .cloned()
                .ok_or(ServiceError::ItemNotFound(item_id))?
        };
        let can_start = {
            let data = runtime.data.lock();
            data.state.execution_state == RepositoryExecutionState::Active
                && fresh_item.state == QueueItemState::Preparing
                && fresh_item.current_generation_id == Some(generation.id)
                && !cancellation.is_cancelled()
        };
        if !can_start {
            buildset.state = BuildsetState::Invalidated;
            buildset.finished_at = Some(OffsetDateTime::now_utc());
            runtime.store.update_buildset(&buildset)?;
            {
                let mut data = runtime.data.lock();
                if let Some(existing) = data
                    .buildsets
                    .iter_mut()
                    .find(|candidate| candidate.id == buildset.id)
                {
                    *existing = buildset;
                }
                if let Some(slot) = data.slots.get_mut(&slot_id) {
                    slot.state = "idle".into();
                    slot.last_used = Some(OffsetDateTime::now_utc());
                }
            }
            if fresh_item.state == QueueItemState::Preparing {
                let mut queued = fresh_item;
                queued.state = queued
                    .state
                    .transition(ItemEvent::InfrastructureRetry)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                queued.buildset_id = None;
                self.replace_item(&runtime, queued)?;
            }
            runtime.cancellations.lock().remove(&item_id);
            return Ok(());
        }
        item = fresh_item;
        buildset.state = buildset
            .state
            .transition(BuildsetEvent::WorkerStarted)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        buildset.started_at = Some(OffsetDateTime::now_utc());
        runtime.store.update_buildset(&buildset)?;
        item.state = item
            .state
            .transition(ItemEvent::WorkerStarted)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        self.replace_item(&runtime, item.clone())?;
        {
            let mut data = runtime.data.lock();
            if let Some(existing) = data
                .buildsets
                .iter_mut()
                .find(|candidate| candidate.id == buildset.id)
            {
                *existing = buildset.clone();
            }
            data.slots.insert(
                slot_id,
                SlotView {
                    id: slot_id,
                    path: slot_path.clone(),
                    state: "running".into(),
                    checkout_oid: Some(generation.tested_oid.clone()),
                    health: "healthy".into(),
                    last_used: Some(OffsetDateTime::now_utc()),
                },
            );
        }
        drop(start_guard);
        let changed_paths = runtime.git.changed_paths(&item.source_oid).await?;
        let execution = BuildsetExecution {
            tested_oid: generation.tested_oid.clone(),
            slot_root: slot_path.clone(),
            log_directory: runtime.logs_root.join(buildset_id.to_string()),
            environment: (*environment.variables).clone(),
            context: BTreeMap::from([
                ("CI".into(), "1".into()),
                ("TOLLGATE_ITEM_ID".into(), item_id.to_string()),
                ("TOLLGATE_TESTED_OID".into(), generation.tested_oid.to_hex()),
                (
                    "TOLLGATE_VALIDATION_GENERATION_ID".into(),
                    generation.id.to_string(),
                ),
            ]),
        };
        let mut sleep_assertion = IdleSleepAssertion::acquire().await;
        let volume_monitor_stop = CancellationToken::new();
        let volume_monitor_failed = Arc::new(AtomicBool::new(false));
        let volume_monitor = {
            let runtime = Arc::clone(&runtime);
            let stop = volume_monitor_stop.clone();
            let cancel = cancellation.clone();
            let failed = Arc::clone(&volume_monitor_failed);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = stop.cancelled() => break,
                        () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                            match observe_volumes(&runtime) {
                                Ok(volumes) if volumes.iter().all(|volume| volume.available_bytes >= volume.critical_threshold) => {}
                                Ok(_) | Err(_) => {
                                    failed.store(true, Ordering::Release);
                                    cancel.cancel();
                                    break;
                                }
                            }
                        }
                    }
                }
            })
        };
        let outcome = run_buildset_scheduled(
            &config,
            execution,
            &changed_paths,
            cancellation,
            Some(Arc::clone(&self.global_scheduler)),
        )
        .await;
        volume_monitor_stop.cancel();
        let _ = volume_monitor.await;
        sleep_assertion.release().await;
        if volume_monitor_failed.load(Ordering::Acquire) {
            return Err(ServiceError::Invariant(
                "execution was interrupted because a Tollgate volume crossed its critical recovery reserve"
                    .into(),
            ));
        }
        let mut outcome = outcome?;
        let missing_artifact_steps = self
            .retain_artifacts(
                &runtime,
                buildset_id,
                &slot_path,
                &config,
                &outcome.skipped,
                outcome.passed,
            )
            .await?;
        for step_name in missing_artifact_steps {
            if let Some((_, result)) = outcome
                .steps
                .iter_mut()
                .find(|(name, _)| name == &step_name)
            {
                result.class = StepResultClass::ExitFailure;
            }
            outcome.passed = false;
            outcome.passed_with_warnings = false;
        }
        let attempt_records = outcome
            .steps
            .iter()
            .filter_map(|(name, result)| {
                config
                    .steps
                    .iter()
                    .find(|step| step.name == *name)
                    .map(|step| StepAttemptRecord {
                        step_id: stable_step_id(buildset.id, name),
                        attempt_id: result.attempt_id,
                        name: name.clone(),
                        frozen: serde_json::to_value(step).unwrap_or(serde_json::Value::Null),
                        retry_number: buildset.attempt,
                        result_class: serde_json::to_value(&result.class)
                            .ok()
                            .and_then(|value| value.as_str().map(str::to_owned))
                            .unwrap_or_else(|| "unknown".into()),
                        result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
                        stdout_end: result.log.stdout_end,
                        stderr_end: result.log.stderr_end,
                        broker_sequence_end: result.log.broker_sequence_end,
                        log_hash: result.log.hash.clone(),
                        log_path: result.log.path.clone(),
                    })
            })
            .collect::<Vec<_>>();
        runtime
            .store
            .record_step_attempts(buildset.id, &attempt_records)?;
        buildset.step_results = outcome
            .steps
            .iter()
            .map(|(name, result)| BuildsetStepResult {
                name: name.clone(),
                result_class: serde_json::to_value(&result.class)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into()),
                exit_code: result.exit_code,
                signal: result.signal,
                elapsed_ms: result.elapsed_ms,
                log_hash: result.log.hash.clone(),
                stdout_end: result.log.stdout_end,
                stderr_end: result.log.stderr_end,
                diagnostics: result.diagnostics.clone(),
            })
            .chain(outcome.skipped.iter().map(|name| BuildsetStepResult {
                name: name.clone(),
                result_class: "skipped".into(),
                exit_code: None,
                signal: None,
                elapsed_ms: 0,
                log_hash: String::new(),
                stdout_end: 0,
                stderr_end: 0,
                diagnostics: Vec::new(),
            }))
            .collect();
        runtime.cancellations.lock().remove(&item_id);
        buildset.finished_at = Some(OffsetDateTime::now_utc());
        let passed_tree = if outcome.passed {
            Some(
                runtime
                    .git
                    .mirror_tree_oid(&runtime.mirror, &generation.tested_oid)
                    .await?,
            )
        } else {
            None
        };
        let finalization_guard = runtime.mutation.lock().await;
        let current_item = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .find(|candidate| candidate.id == item_id)
                .cloned()
        };
        let generation_is_current = current_item.as_ref().is_some_and(|candidate| {
            candidate.current_generation_id == Some(generation.id)
                && candidate.state == QueueItemState::Running
        });
        if !generation_is_current {
            buildset.state = BuildsetState::Invalidated;
            runtime.store.update_buildset(&buildset)?;
            if let Some(existing) = runtime
                .data
                .lock()
                .buildsets
                .iter_mut()
                .find(|candidate| candidate.id == buildset.id)
            {
                *existing = buildset;
            }
            if let Some(slot) = runtime.data.lock().slots.get_mut(&slot_id) {
                slot.state = "idle".into();
                slot.last_used = Some(OffsetDateTime::now_utc());
            }
            return Ok(());
        }
        item = current_item.ok_or(ServiceError::ItemNotFound(item_id))?;
        if item.kind == QueueItemKind::IndependentCheck {
            let bootstrap = item.metadata.purpose.as_deref() == Some("bootstrap");
            let bootstrap_passed = bootstrap && outcome.passed;
            if outcome.passed {
                buildset.state = buildset
                    .state
                    .transition(if outcome.passed_with_warnings {
                        BuildsetEvent::PassedWithWarnings
                    } else {
                        BuildsetEvent::Passed
                    })
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.state = item
                    .state
                    .transition(ItemEvent::IndependentCheckPassed)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                if outcome.passed_with_warnings {
                    item.terminal_reason = Some("independent-check-passed-with-warnings".into());
                }
            } else {
                buildset.state = buildset
                    .state
                    .transition(BuildsetEvent::Failed)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.state = item
                    .state
                    .transition(ItemEvent::IndependentCheckFailed)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.terminal_reason = Some(
                    outcome
                        .workspace_verification_error
                        .as_ref()
                        .map(|reason| format!("checkout-verification-failed: {reason}"))
                        .unwrap_or_else(|| {
                            if bootstrap {
                                "baseline-failing"
                            } else {
                                "independent-check-failed"
                            }
                            .into()
                        }),
                );
            }
            runtime.store.update_buildset(&buildset)?;
            {
                let mut data = runtime.data.lock();
                if let Some(existing) = data
                    .buildsets
                    .iter_mut()
                    .find(|candidate| candidate.id == buildset.id)
                {
                    *existing = buildset;
                }
                if let Some(slot) = data.slots.get_mut(&slot_id) {
                    slot.state = "idle".into();
                    slot.last_used = Some(OffsetDateTime::now_utc());
                }
            }
            self.replace_item(&runtime, item.clone())?;
            drop(finalization_guard);
            if let Err(error) = runtime
                .git
                .delete_source_ref(&item.source_ref, &item.source_oid)
                .await
            {
                let mut attention = item;
                attention.cleanup_state = CleanupState::NeedsAttention;
                attention.terminal_reason = Some(format!("check-source-cleanup-failed:{error}"));
                self.replace_item(&runtime, attention)?;
            }
            if bootstrap_passed
                && let Err(error) = self.snapshot_cache(repository_id, CommandId::new()).await
            {
                eprintln!(
                    "Tollgate bootstrap passed but no reusable cache seed was published: {error}"
                );
            }
            self.spawn_eligible(repository_id, &runtime);
            return Ok(());
        }
        let mut conclusive_failure = false;
        if outcome.passed {
            buildset.state = buildset
                .state
                .transition(if outcome.passed_with_warnings {
                    BuildsetEvent::PassedWithWarnings
                } else {
                    BuildsetEvent::Passed
                })
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            let tree_oid = passed_tree.clone().ok_or_else(|| {
                ServiceError::Invariant("passing buildset tree is missing".into())
            })?;
            let voting_results = outcome
                .steps
                .iter()
                .filter(|(name, result)| {
                    config
                        .steps
                        .iter()
                        .any(|step| &step.name == name && step.voting)
                        && result.class == StepResultClass::Success
                })
                .map(|(name, result)| SuccessfulStepResult {
                    step_id: stable_step_id(buildset_id, name),
                    attempt_id: result.attempt_id,
                    log_stdout_end: result.log.stdout_end,
                    log_stderr_end: result.log.stderr_end,
                    log_hash: result.log.hash.clone(),
                })
                .collect::<Vec<_>>();
            let certificate = PassCertificate {
                id: CertificateId::new(),
                buildset_id,
                queue_item_id: item_id,
                validation_generation_id: generation.id,
                tested_oid: generation.tested_oid.clone(),
                tree_oid,
                expected_parent_oid: generation.expected_parent_oid.clone(),
                configuration_digest: config.digest.clone(),
                step_graph_digest: config.step_graph_digest.clone(),
                engine_epoch: { runtime.data.lock().state.engine_epoch },
                environment_fingerprint: environment.fingerprint,
                voting_results,
                warnings: if outcome.passed_with_warnings {
                    vec!["One or more non-voting steps failed".into()]
                } else {
                    Vec::new()
                },
                checkout_verified: outcome.workspace_verified,
                completed_event_sequence: runtime.data.lock().state.event_sequence + 1,
                created_at: OffsetDateTime::now_utc(),
            };
            runtime.store.insert_certificate(&certificate)?;
            item.certificate_id = Some(certificate.id);
            item.state = item
                .state
                .transition(ItemEvent::BuildPassed)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            {
                let mut data = runtime.data.lock();
                data.certificates.push(certificate);
            }
        } else {
            buildset.state = buildset
                .state
                .transition(BuildsetEvent::Failed)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.state = item
                .state
                .transition(ItemEvent::VotingFailed)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.terminal_reason = Some(
                outcome
                    .workspace_verification_error
                    .as_ref()
                    .map(|reason| format!("checkout-verification-failed: {reason}"))
                    .unwrap_or_else(|| "voting-validation-failed".into()),
            );
            conclusive_failure = true;
        }
        runtime.store.update_buildset(&buildset)?;
        {
            let mut data = runtime.data.lock();
            if let Some(existing) = data
                .buildsets
                .iter_mut()
                .find(|candidate| candidate.id == buildset.id)
            {
                *existing = buildset;
            }
            if let Some(slot) = data.slots.get_mut(&slot_id) {
                slot.state = "idle".into();
                slot.last_used = Some(OffsetDateTime::now_utc());
            }
        }
        self.replace_item(&runtime, item)?;
        drop(finalization_guard);
        if conclusive_failure {
            self.rebuild_after_failure(repository_id, item_id).await?;
        }
        if let Err(error) = self.enforce_log_retention(&runtime).await {
            eprintln!("Tollgate log retention maintenance failed: {error}");
        }
        self.promote_ready(repository_id).await
    }

    async fn enforce_log_retention(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        const LOG_BUDGET: u64 = 10 * 1024 * 1024 * 1024;
        let mut log_entries = tokio::fs::read_dir(&runtime.logs_root).await?;
        while let Some(entry) = log_entries.next_entry().await? {
            if !entry.file_name().to_string_lossy().starts_with(".pruned-") {
                continue;
            }
            let kind = entry.file_type().await?;
            if kind.is_symlink() || !kind.is_dir() {
                return Err(ServiceError::Invariant(
                    "log-pruning quarantine was replaced with an unsafe entry".into(),
                ));
            }
            tokio::fs::remove_dir_all(entry.path()).await?;
        }
        let cutoff = OffsetDateTime::now_utc() - Duration::days(90);
        let (protected, mut completed) = {
            let data = runtime.data.lock();
            let protected = data
                .items
                .iter()
                .filter(|item| !item.state.is_terminal())
                .filter_map(|item| item.buildset_id)
                .collect::<HashSet<_>>();
            let completed = data
                .buildsets
                .iter()
                .filter_map(|buildset| buildset.finished_at.map(|finished| (buildset.id, finished)))
                .collect::<Vec<_>>();
            (protected, completed)
        };
        let mut retained = Vec::new();
        let mut total = 0u64;
        for (buildset_id, finished_at) in completed.drain(..) {
            let path = runtime.logs_root.join(buildset_id.to_string());
            if !path.is_dir() {
                continue;
            }
            let size = tree_logical_size(&path)?;
            total = total.saturating_add(size);
            retained.push((buildset_id, finished_at, path, size));
        }
        retained.sort_by_key(|(_, finished_at, _, _)| *finished_at);
        for (buildset_id, finished_at, path, size) in retained {
            if protected.contains(&buildset_id) {
                continue;
            }
            if finished_at >= cutoff && total <= LOG_BUDGET {
                continue;
            }
            runtime.store.mark_buildset_logs_pruned(buildset_id)?;
            let quarantine =
                runtime
                    .logs_root
                    .join(format!(".pruned-{}-{}", buildset_id, uuid::Uuid::now_v7()));
            tokio::fs::rename(&path, &quarantine).await?;
            sync_directory(&runtime.logs_root)?;
            tokio::fs::remove_dir_all(quarantine).await?;
            total = total.saturating_sub(size);
        }
        Ok(())
    }

    async fn retain_artifacts(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        buildset_id: BuildsetId,
        slot_root: &Path,
        config: &EffectiveConfig,
        skipped_steps: &[String],
        require_complete: bool,
    ) -> Result<Vec<String>, ServiceError> {
        if config.steps.iter().all(|step| step.artifacts.is_empty()) {
            return Ok(Vec::new());
        }
        const RETENTION_BUDGET: u64 = 50 * 1024 * 1024 * 1024;
        const MAX_DISCOVERED_FILES: usize = 100_000;
        const MAX_RETAINED_FILES: usize = 10_000;
        const FILE_METADATA_ALLOWANCE: u64 = 8 * 1024;
        let retained_bytes = runtime.store.retained_artifact_bytes()?;
        let mut files = Vec::new();
        let mut directories = vec![slot_root.to_owned()];
        while let Some(directory) = directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory).await?;
            while let Some(entry) = entries.next_entry().await? {
                let kind = entry.file_type().await?;
                if entry.file_name() == ".git" || kind.is_symlink() {
                    continue;
                }
                if kind.is_dir() {
                    directories.push(entry.path());
                } else if kind.is_file() {
                    if files.len() >= MAX_DISCOVERED_FILES {
                        return Err(ServiceError::Invariant(format!(
                            "artifact discovery exceeded the {MAX_DISCOVERED_FILES}-file safety limit"
                        )));
                    }
                    files.push(entry.path());
                }
            }
        }
        let artifacts_root = runtime.git.common_dir.join("tollgate/artifacts");
        tokio::fs::create_dir_all(&artifacts_root).await?;
        if tokio::fs::symlink_metadata(&artifacts_root)
            .await?
            .file_type()
            .is_symlink()
        {
            return Err(ServiceError::Invariant(
                "owned artifact root was replaced by a symlink".into(),
            ));
        }
        let destination_dir = artifacts_root.join(buildset_id.to_string());
        if tokio::fs::try_exists(&destination_dir).await? {
            return Err(ServiceError::Invariant(format!(
                "artifact destination for buildset {buildset_id} already exists without a completed record"
            )));
        }
        let staging_dir = artifacts_root.join(format!(".staging-{}", uuid::Uuid::now_v7()));
        let mut candidates = Vec::new();
        let mut missing_required_steps = Vec::new();
        let mut publication_bytes = 0u64;
        for step in config
            .steps
            .iter()
            .filter(|step| !skipped_steps.contains(&step.name))
        {
            for artifact in &step.artifacts {
                let mut matched = false;
                let mut builder = GlobSetBuilder::new();
                for pattern in &artifact.patterns {
                    builder.add(Glob::new(pattern).map_err(|error| {
                        ServiceError::Invariant(format!(
                            "validated artifact glob became invalid: {error}"
                        ))
                    })?);
                }
                let matcher = builder
                    .build()
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                for source in &files {
                    let relative = source.strip_prefix(slot_root).map_err(|_| {
                        ServiceError::Invariant("artifact escaped its execution slot".into())
                    })?;
                    if !matcher.is_match(relative) {
                        continue;
                    }
                    let metadata = tokio::fs::symlink_metadata(source).await?;
                    if !metadata.file_type().is_file() {
                        return Err(ServiceError::Invariant(
                            "artifact changed type after execution".into(),
                        ));
                    }
                    let source_size = metadata.len();
                    publication_bytes = publication_bytes.saturating_add(source_size);
                    let retained = destination_dir
                        .join(&step.name)
                        .join(&artifact.name)
                        .join(relative);
                    if candidates.len() >= MAX_RETAINED_FILES {
                        return Err(ServiceError::Invariant(format!(
                            "artifact publication exceeded the {MAX_RETAINED_FILES}-file safety limit"
                        )));
                    }
                    candidates.push((
                        source.clone(),
                        ArtifactRecord {
                            artifact_id: uuid::Uuid::now_v7().to_string(),
                            buildset_id,
                            source_path: relative.to_string_lossy().into_owned(),
                            retained_path: retained.to_string_lossy().into_owned(),
                            hash: hash_file(source).await?,
                            size: source_size,
                            retention_state: "retained".into(),
                            created_at: OffsetDateTime::now_utc(),
                            expires_at: OffsetDateTime::now_utc()
                                + Duration::days(i64::from(artifact.retention_days)),
                        },
                    ));
                    matched = true;
                }
                if artifact.required && !matched && require_complete {
                    missing_required_steps.push(step.name.clone());
                }
            }
        }
        if !missing_required_steps.is_empty() {
            missing_required_steps.sort();
            missing_required_steps.dedup();
            return Ok(missing_required_steps);
        }
        if retained_bytes.saturating_add(publication_bytes) > RETENTION_BUDGET {
            return Err(ServiceError::Invariant(
                "artifact retention would exceed the 50 GiB repository budget".into(),
            ));
        }
        let command_id = CommandId::new();
        let evidence = ArtifactRetentionEvidence {
            repository_id: runtime.data.lock().state.id,
            buildset_id,
            staging_dir: staging_dir.clone(),
            destination_dir: destination_dir.clone(),
            records: candidates
                .iter()
                .map(|(_, record)| record.clone())
                .collect(),
        };
        let manifest_allowance = u64::try_from(serde_json::to_vec_pretty(&evidence)?.len())
            .map_err(|_| ServiceError::Invariant("artifact manifest is too large".into()))?
            .saturating_mul(2);
        let record_allowance = u64::try_from(candidates.len())
            .map_err(|_| ServiceError::Invariant("artifact candidate count overflowed".into()))?
            .saturating_mul(FILE_METADATA_ALLOWANCE);
        let publication_allowance = publication_bytes
            .saturating_add(manifest_allowance)
            .saturating_add(record_allowance);
        runtime.store.prepare_operation(
            evidence.repository_id,
            "artifact",
            command_id,
            &evidence,
        )?;
        if let Err(error) = self
            .reserve_runtime_volume(
                runtime,
                command_id,
                &runtime.git.common_dir,
                publication_allowance,
            )
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "artifact-volume-admission", "error": error.to_string()}),
            )?;
            return Err(error);
        }
        tokio::fs::create_dir(&staging_dir).await?;
        for (source, record) in &candidates {
            let retained = Path::new(&record.retained_path);
            let relative = retained.strip_prefix(&destination_dir).map_err(|_| {
                ServiceError::Invariant("artifact destination escaped publication root".into())
            })?;
            let staged = staging_dir.join(relative);
            if let Some(parent) = staged.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            copy_artifact_exclusive(source, &staged, record).await?;
        }
        let manifest_path = staging_dir.join(".tollgate-artifact-manifest.json");
        let manifest_bytes = serde_json::to_vec_pretty(&evidence)?;
        let mut manifest = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&manifest_path)
            .await?;
        manifest.write_all(&manifest_bytes).await?;
        manifest.sync_all().await?;
        sync_tree_directories(&staging_dir)?;
        verify_artifact_staging(&evidence).await?;
        tokio::fs::rename(&staging_dir, &destination_dir).await?;
        sync_directory(&artifacts_root)?;
        runtime.store.set_intent_state(
            command_id,
            IntentState::ExternalApplied,
            &serde_json::json!({"destination": destination_dir}),
        )?;
        verify_artifact_publication(&artifacts_root, &evidence).await?;
        let mut state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_artifact_retention(
            &state,
            command_id,
            &evidence.records,
            &serde_json::json!({"destination": destination_dir, "verified": true}),
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(Vec::new())
    }

    fn import_seed_into_slot(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        seed: &SeedRecord,
        slot: &Path,
    ) -> Result<(), ServiceError> {
        let seed_root = std::fs::canonicalize(&seed.path)?;
        let cache_root = std::fs::canonicalize(
            runtime
                .slots_root
                .parent()
                .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?,
        )?;
        if !seed_root.starts_with(&cache_root)
            || std::fs::symlink_metadata(&seed_root)?
                .file_type()
                .is_symlink()
        {
            return Err(ServiceError::Invariant(
                "published seed escaped the owned cache root".into(),
            ));
        }
        let entries = seed
            .manifest
            .get("entries")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| ServiceError::Invariant("seed manifest omitted entries".into()))?;
        for entry in entries {
            let relative: PathBuf = serde_json::from_value(
                entry
                    .get("path")
                    .cloned()
                    .ok_or_else(|| ServiceError::Invariant("seed entry omitted path".into()))?,
            )?;
            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err(ServiceError::Invariant(
                    "seed entry path is not normalized and relative".into(),
                ));
            }
            let source = std::fs::canonicalize(seed_root.join(&relative))?;
            if !source.starts_with(&seed_root) {
                return Err(ServiceError::Invariant(
                    "seed entry resolves outside its immutable generation".into(),
                ));
            }
            let destination = slot.join(&relative);
            if destination.exists() {
                return Err(ServiceError::Invariant(format!(
                    "seed entry `{}` would overwrite an existing worktree path",
                    relative.display()
                )));
            }
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
            force_clone_tree(&source, &destination)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        }
        Ok(())
    }

    async fn hydrate_feature_worktree(
        &self,
        runtime: &RepositoryRuntime,
        seed: &SeedRecord,
        worktree: &Path,
        command_id: CommandId,
    ) -> Result<(), ServiceError> {
        verify_seed_record_at(Path::new(&seed.path), seed)?;
        let seed_root = std::fs::canonicalize(&seed.path)?;
        let worktree_root = std::fs::canonicalize(worktree)?;
        let checkout = GitRepository::discover(&worktree_root).await?;
        if checkout.common_dir != runtime.git.common_dir {
            return Err(ServiceError::Invariant(
                "warm cache destination belongs to another repository".into(),
            ));
        }
        let entries = seed
            .manifest
            .get("entries")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| ServiceError::Invariant("seed manifest omitted entries".into()))?;
        let mut selected = Vec::with_capacity(entries.len());
        for entry in entries {
            let relative: PathBuf = serde_json::from_value(
                entry
                    .get("path")
                    .cloned()
                    .ok_or_else(|| ServiceError::Invariant("seed entry omitted path".into()))?,
            )?;
            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err(ServiceError::Invariant(
                    "seed entry path is not normalized and relative".into(),
                ));
            }
            let source = std::fs::canonicalize(seed_root.join(&relative))?;
            if !source.starts_with(&seed_root) || !source.is_dir() {
                return Err(ServiceError::Invariant(
                    "seed entry escaped its immutable generation".into(),
                ));
            }
            let destination = worktree_root.join(&relative);
            if destination.exists() {
                return Err(ServiceError::Invariant(format!(
                    "cache path `{}` already exists",
                    relative.display()
                )));
            }
            let parent = destination
                .parent()
                .ok_or_else(|| ServiceError::Invariant("cache destination has no parent".into()))?;
            let metadata = std::fs::symlink_metadata(parent)?;
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || !std::fs::canonicalize(parent)?.starts_with(&worktree_root)
            {
                return Err(ServiceError::Invariant(format!(
                    "cache path `{}` has an unsafe parent",
                    relative.display()
                )));
            }
            if !runtime
                .git
                .directory_is_ignored(&worktree_root, &relative)
                .await?
            {
                return Err(ServiceError::Invariant(format!(
                    "cache path `{}` is not ignored in the new worktree",
                    relative.display()
                )));
            }
            selected.push((source, destination));
        }

        let staging = worktree_hydration_staging(runtime, command_id)?;
        if staging.exists() {
            return Err(ServiceError::Invariant(
                "cache hydration staging path already exists".into(),
            ));
        }
        std::fs::create_dir(&staging)?;
        let mut staged = Vec::with_capacity(selected.len());
        for (index, (source, destination)) in selected.into_iter().enumerate() {
            let staged_path = staging.join(index.to_string());
            if let Err(error) = force_clone_tree(&source, &staged_path) {
                cleanup_worktree_hydration_staging(runtime, command_id)?;
                return Err(ServiceError::Invariant(error.to_string()));
            }
            staged.push((staged_path, destination));
        }
        let mut imported = Vec::with_capacity(staged.len());
        for (staged_path, destination) in staged {
            if let Err(error) = std::fs::rename(&staged_path, &destination) {
                rollback_hydrated_paths(&worktree_root, &imported)?;
                cleanup_worktree_hydration_staging(runtime, command_id)?;
                return Err(error.into());
            }
            imported.push(destination);
        }
        if let Err(error) = checkout.ensure_clean().await {
            rollback_hydrated_paths(&worktree_root, &imported)?;
            cleanup_worktree_hydration_staging(runtime, command_id)?;
            return Err(error.into());
        }
        cleanup_worktree_hydration_staging(runtime, command_id)?;
        Ok(())
    }

    async fn rebuild_after_base_adoption(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        new_base: &GitOid,
    ) -> Result<Vec<QueueItemId>, ServiceError> {
        let (active, config, state) = {
            let data = runtime.data.lock();
            (
                data.items
                    .iter()
                    .filter(|item| {
                        item.kind == QueueItemKind::Gate && is_rebuildable_gate_state(item.state)
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
                data.config.clone(),
                data.state.clone(),
            )
        };
        let affected = active.iter().map(|item| item.id).collect::<Vec<_>>();
        for token in runtime.cancellations.lock().values() {
            token.cancel();
        }
        let mut survivors = Vec::new();
        for mut item in active {
            let tested = item.current_generation_id.and_then(|id| {
                runtime
                    .data
                    .lock()
                    .generations
                    .iter()
                    .find(|generation| generation.id == id)
                    .map(|generation| generation.tested_oid.clone())
            });
            let source_integrated = runtime.git.is_ancestor(&item.source_oid, new_base).await?;
            let exact_prefix_integrated = if let Some(tested) = tested {
                runtime.git.is_ancestor(&tested, new_base).await?
            } else {
                false
            };
            if source_integrated || exact_prefix_integrated {
                item.state = item
                    .state
                    .transition(ItemEvent::ExternallyIntegrated)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.terminal_reason = Some(if exact_prefix_integrated {
                    "exact-tested-object-integrated-externally".into()
                } else {
                    "source-object-integrated-externally".into()
                });
                self.replace_item(runtime, item)?;
            } else {
                survivors.push(item);
            }
        }
        if survivors.is_empty() {
            return Ok(affected);
        }
        runtime.git.initialize_mirror(&runtime.mirror).await?;
        let mut viable = Vec::new();
        let mut removed = HashSet::new();
        for mut item in survivors.drain(..) {
            if item
                .dependencies
                .iter()
                .any(|dependency| removed.contains(dependency))
            {
                removed.insert(item.id);
                item.state = item
                    .state
                    .transition(ItemEvent::DependencyLost)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.terminal_reason = Some("dependency-conflicted-with-adopted-base".into());
                self.replace_item(runtime, item)?;
                continue;
            }
            let candidate_sources = viable
                .iter()
                .map(|candidate: &QueueItem| candidate.source_oid.clone())
                .chain(std::iter::once(item.source_oid.clone()))
                .collect::<Vec<_>>();
            match runtime
                .git
                .construct_prefix(
                    &runtime.mirror,
                    &runtime.builder,
                    new_base,
                    &candidate_sources,
                )
                .await
            {
                Ok(_) => viable.push(item),
                Err(error) if error.is_synthetic_rejection() => {
                    removed.insert(item.id);
                    item.state = item
                        .state
                        .transition(ItemEvent::InputsChanged)
                        .and_then(|state| state.transition(ItemEvent::MergeConflict))
                        .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                    item.terminal_reason = Some("merge-conflict-with-adopted-base".into());
                    self.replace_item(runtime, item)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        if viable.is_empty() {
            return Ok(affected);
        }
        let sources = viable
            .iter()
            .map(|item| item.source_oid.clone())
            .collect::<Vec<_>>();
        let survivor_ids = viable.iter().map(|item| item.id).collect::<Vec<_>>();
        let chain = runtime
            .git
            .construct_prefix(&runtime.mirror, &runtime.builder, new_base, &sources)
            .await?;
        for (position, mut item) in viable.into_iter().enumerate() {
            let synthetic = &chain[position];
            let generation = ValidationGeneration::derive(
                ValidationGenerationId::new(),
                item.id,
                new_base.clone(),
                survivor_ids[..=position].to_vec(),
                sources[..=position].to_vec(),
                chain[..=position]
                    .iter()
                    .map(|commit| commit.oid.clone())
                    .collect(),
                synthetic.parent_oid.clone(),
                synthetic.oid.clone(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
                state.engine_epoch,
            );
            item.state = item
                .state
                .transition(ItemEvent::InputsChanged)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.current_generation_id = Some(generation.id);
            item.buildset_id = None;
            item.certificate_id = None;
            item.state = item
                .state
                .transition(ItemEvent::GenerationPrepared)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            retain_speculative_generations(runtime, std::slice::from_ref(&generation)).await?;
            runtime.store.replace_generation(&generation)?;
            runtime.data.lock().push_generation(generation);
            self.replace_item(runtime, item)?;
        }
        self.project_active_user_master(runtime, Actor::App).await?;
        Ok(affected)
    }

    async fn rebuild_after_failure(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        failed_id: QueueItemId,
    ) -> Result<(), ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let (all_items, failed_index, state, config) = {
            let mut data = runtime.data.lock();
            let failed_index = data
                .items
                .iter()
                .position(|item| item.id == failed_id)
                .ok_or(ServiceError::ItemNotFound(failed_id))?;
            data.state.queue_revision += 1;
            data.state.active_window =
                (data.state.active_window / 2).max(data.state.active_window_floor);
            (
                data.items.clone(),
                failed_index,
                data.state.clone(),
                data.config.clone(),
            )
        };
        let mut removed = std::collections::HashSet::from([failed_id]);
        runtime.store.update_repository_state(&state)?;
        loop {
            let before = removed.len();
            for item in &all_items {
                if item
                    .dependencies
                    .iter()
                    .any(|dependency| removed.contains(dependency))
                {
                    removed.insert(item.id);
                }
            }
            if before == removed.len() {
                break;
            }
        }
        for dependent in all_items.iter().filter(|item| {
            item.id != failed_id && removed.contains(&item.id) && !item.state.is_terminal()
        }) {
            let token = { runtime.cancellations.lock().get(&dependent.id).cloned() };
            if let Some(token) = token {
                token.cancel();
            }
            let mut dependent = dependent.clone();
            dependent.state = dependent
                .state
                .transition(ItemEvent::DependencyLost)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            dependent.terminal_reason = Some(format!("dependency-failed:{failed_id}"));
            self.replace_item(&runtime, dependent)?;
        }
        let survivors = all_items
            .iter()
            .filter(|item| !removed.contains(&item.id) && !item.state.is_terminal())
            .cloned()
            .collect::<Vec<_>>();
        let affected_ids = all_items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                *index > failed_index && !removed.contains(&item.id) && !item.state.is_terminal()
            })
            .map(|(_, item)| item.id)
            .collect::<Vec<_>>();
        if affected_ids.is_empty() {
            return Ok(());
        }
        let tokens = {
            let cancellations = runtime.cancellations.lock();
            affected_ids
                .iter()
                .filter_map(|id| cancellations.get(id).cloned())
                .collect::<Vec<_>>()
        };
        for token in tokens {
            token.cancel();
        }
        let sources = survivors
            .iter()
            .map(|item| item.source_oid.clone())
            .collect::<Vec<_>>();
        runtime.git.initialize_mirror(&runtime.mirror).await?;
        let chain = runtime
            .git
            .construct_prefix(
                &runtime.mirror,
                &runtime.builder,
                &state.master_oid,
                &sources,
            )
            .await?;
        let mut restart = Vec::new();
        for id in affected_ids {
            let position = survivors
                .iter()
                .position(|item| item.id == id)
                .ok_or_else(|| {
                    ServiceError::Invariant(
                        "affected descendant missing from surviving prefix".into(),
                    )
                })?;
            let synthetic = &chain[position];
            let generation = ValidationGeneration::derive(
                ValidationGenerationId::new(),
                id,
                state.master_oid.clone(),
                survivors[..=position].iter().map(|item| item.id).collect(),
                survivors[..=position]
                    .iter()
                    .map(|item| item.source_oid.clone())
                    .collect(),
                chain[..=position]
                    .iter()
                    .map(|commit| commit.oid.clone())
                    .collect(),
                synthetic.parent_oid.clone(),
                synthetic.oid.clone(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
                state.engine_epoch,
            );
            retain_speculative_generations(&runtime, std::slice::from_ref(&generation)).await?;
            runtime.store.replace_generation(&generation)?;
            let mut item = survivors[position].clone();
            item.state = item
                .state
                .transition(ItemEvent::InputsChanged)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.current_generation_id = Some(generation.id);
            item.buildset_id = None;
            item.certificate_id = None;
            item.state = item
                .state
                .transition(ItemEvent::GenerationPrepared)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            {
                let mut data = runtime.data.lock();
                data.push_generation(generation);
            }
            self.replace_item(&runtime, item)?;
            restart.push(id);
        }
        drop(restart);
        self.spawn_eligible(repository_id, &runtime);
        Ok(())
    }

    async fn reconcile_failed_prefixes(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        loop {
            let inconsistent_failure = {
                let data = runtime.data.lock();
                let failed = data
                    .items
                    .iter()
                    .filter(|item| {
                        item.kind == QueueItemKind::Gate
                            && matches!(
                                item.state,
                                QueueItemState::Failed
                                    | QueueItemState::MergeConflict
                                    | QueueItemState::DependencyFailed
                                    | QueueItemState::Canceled
                                    | QueueItemState::Superseded
                                    | QueueItemState::InfrastructureExhausted
                            )
                    })
                    .collect::<Vec<_>>();
                failed
                    .into_iter()
                    .find(|failed| {
                        data.items.iter().any(|item| {
                            item.kind == QueueItemKind::Gate
                                && !item.state.is_terminal()
                                && item.current_generation_id.is_some_and(|generation_id| {
                                    data.generations
                                        .iter()
                                        .find(|generation| generation.id == generation_id)
                                        .is_some_and(|generation| {
                                            generation.ordered_item_ids.contains(&failed.id)
                                        })
                                })
                        })
                    })
                    .map(|item| item.id)
            };
            let Some(failed_id) = inconsistent_failure else {
                return Ok(());
            };
            self.rebuild_after_failure(repository_id, failed_id).await?;
        }
    }

    fn spawn_eligible(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        runtime: &Arc<RepositoryRuntime>,
    ) {
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let eligible = {
            let data = runtime.data.lock();
            let mut eligible = data
                .items
                .iter()
                .filter(|item| item.kind == QueueItemKind::Gate && !item.state.is_terminal())
                .take(data.state.active_window as usize)
                .filter(|item| item.state == QueueItemState::Queued)
                .map(|item| item.id)
                .collect::<Vec<_>>();
            eligible.extend(
                data.items
                    .iter()
                    .filter(|item| {
                        item.kind == QueueItemKind::IndependentCheck
                            && item.state == QueueItemState::Queued
                    })
                    .map(|item| item.id),
            );
            eligible
        };
        for item_id in eligible {
            if runtime.dispatching.lock().insert(item_id) {
                self.spawn_item(repository_id, item_id, Arc::clone(runtime));
            }
        }
    }

    fn spawn_item(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        runtime: Arc<RepositoryRuntime>,
    ) {
        let service = Arc::clone(self);
        let future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                if let Err(error) = service.execute_item(repository_id, item_id).await {
                    service
                        .handle_background_error(repository_id, item_id, error)
                        .await;
                }
                runtime.cancellations.lock().remove(&item_id);
                runtime.dispatching.lock().remove(&item_id);
                if runtime.data.lock().state.execution_state == RepositoryExecutionState::Active {
                    service.spawn_eligible(repository_id, &runtime);
                }
            });
        tokio::spawn(future);
    }

    fn recover_interrupted(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<Vec<QueueItemId>, ServiceError> {
        let now = OffsetDateTime::now_utc();
        let interrupted_buildsets = {
            let data = runtime.data.lock();
            data.buildsets
                .iter()
                .filter(|buildset| {
                    matches!(
                        buildset.state,
                        BuildsetState::Preparing | BuildsetState::Running
                    )
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for mut buildset in interrupted_buildsets {
            if let Some(slot_id) = buildset.slot_id
                && let Some(slot) = runtime.data.lock().slots.get_mut(&slot_id)
            {
                slot.state = "quarantined".into();
                slot.health = "interrupted-owner-unreaped".into();
            }
            buildset.state = buildset
                .state
                .transition(BuildsetEvent::Interrupted)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            buildset.finished_at = Some(now);
            runtime.store.update_buildset(&buildset)?;
            if let Some(existing) = runtime
                .data
                .lock()
                .buildsets
                .iter_mut()
                .find(|candidate| candidate.id == buildset.id)
            {
                *existing = buildset;
            }
        }

        let recoverable = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .filter(|item| {
                    matches!(
                        item.state,
                        QueueItemState::Preparing | QueueItemState::Running
                    )
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for mut item in recoverable {
            item.state = item
                .state
                .transition(ItemEvent::InfrastructureRetry)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.buildset_id = None;
            item.terminal_reason = Some("interrupted-by-previous-app-lifetime".into());
            self.replace_item(runtime, item)?;
        }

        let data = runtime.data.lock();
        Ok(data
            .items
            .iter()
            .filter(|item| item.state == QueueItemState::Queued)
            .map(|item| item.id)
            .collect())
    }

    async fn handle_background_error(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        error: ServiceError,
    ) {
        let Ok(runtime) = self.runtime(repository_id).await else {
            return;
        };
        runtime.cancellations.lock().remove(&item_id);
        let item = {
            let data = runtime.data.lock();
            data.items.iter().find(|item| item.id == item_id).cloned()
        };
        let Some(mut item) = item else {
            return;
        };
        if matches!(
            item.state,
            QueueItemState::Preparing | QueueItemState::Running
        ) {
            let mut failed_buildset = item.buildset_id.and_then(|id| {
                runtime
                    .data
                    .lock()
                    .buildsets
                    .iter()
                    .find(|buildset| buildset.id == id)
                    .cloned()
            });
            let attempt = failed_buildset
                .as_ref()
                .map(|buildset| buildset.attempt)
                .unwrap_or(1);
            if let Some(buildset) = failed_buildset.as_mut()
                && matches!(
                    buildset.state,
                    BuildsetState::Preparing | BuildsetState::Running
                )
                && let Ok(next) = buildset.state.transition(BuildsetEvent::Interrupted)
            {
                buildset.state = next;
                buildset.finished_at = Some(OffsetDateTime::now_utc());
                if let Err(persist_error) = runtime.store.update_buildset(buildset) {
                    self.block_background_persistence_failure(
                        &runtime,
                        item_id,
                        &persist_error.to_string(),
                    );
                    return;
                }
                let mut data = runtime.data.lock();
                if let Some(existing) = data
                    .buildsets
                    .iter_mut()
                    .find(|candidate| candidate.id == buildset.id)
                {
                    *existing = buildset.clone();
                }
                if let Some(slot_id) = buildset.slot_id
                    && let Some(slot) = data.slots.get_mut(&slot_id)
                {
                    slot.state = "quarantined".into();
                    slot.health = "interrupted".into();
                }
            }
            if attempt < 3 {
                if let Ok(next) = item.state.transition(ItemEvent::InfrastructureRetry) {
                    item.state = next;
                    item.buildset_id = None;
                    item.terminal_reason = Some(format!("infrastructure-retry:{error}"));
                    if let Err(persist_error) = self.replace_item(&runtime, item) {
                        self.block_background_persistence_failure(
                            &runtime,
                            item_id,
                            &persist_error.to_string(),
                        );
                    }
                    return;
                }
            } else if let Ok(next) = item.state.transition(ItemEvent::InfrastructureExhausted) {
                item.state = next;
                item.terminal_reason = Some(format!("infrastructure-exhausted:{error}"));
                let gate_item = item.kind == QueueItemKind::Gate;
                if let Err(persist_error) = self.replace_item(&runtime, item) {
                    self.block_background_persistence_failure(
                        &runtime,
                        item_id,
                        &persist_error.to_string(),
                    );
                    return;
                }
                if gate_item
                    && let Err(rebuild_error) =
                        self.rebuild_after_failure(repository_id, item_id).await
                {
                    let state = {
                        let mut data = runtime.data.lock();
                        data.state.execution_state = RepositoryExecutionState::Blocked;
                        data.state.block_reasons.push(BlockReason {
                            code: "failure-rebuild-failed".into(),
                            message: format!(
                                "Queue dependencies could not be rebuilt after infrastructure exhaustion: {rebuild_error}"
                            ),
                            recovery_action: "Inspect the failed item and reconcile the queue before resuming.".into(),
                        });
                        data.state.clone()
                    };
                    if let Err(persist_error) = runtime.store.update_repository_state(&state) {
                        eprintln!(
                            "Tollgate could not persist a failure-rebuild block for {item_id}: {persist_error}"
                        );
                    }
                }
                return;
            }
        }

        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "background-operation-failed".into(),
                message: format!("Queue item {item_id} stopped: {error}"),
                recovery_action: "Inspect Doctor and repository history, resolve the underlying problem, then resume the gate.".into(),
            });
            data.state.clone()
        };
        if let Err(persist_error) = runtime.store.update_repository_state(&state) {
            eprintln!(
                "Tollgate could not persist a background-operation block for {item_id}: {persist_error}"
            );
        }
    }

    fn block_background_persistence_failure(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item_id: QueueItemId,
        persistence_error: &str,
    ) {
        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "background-persistence-failed".into(),
                message: format!(
                    "Queue item {item_id} could not durably record its worker outcome: {persistence_error}"
                ),
                recovery_action: "Stop dispatch, repair the authoritative database volume, and restart for exact recovery.".into(),
            });
            data.state.clone()
        };
        if let Err(error) = runtime.store.update_repository_state(&state) {
            eprintln!(
                "Tollgate could not persist the background persistence-failure block for {item_id}: {error}"
            );
        }
    }

    async fn promote_ready(
        self: &Arc<Self>,
        repository_id: RepositoryId,
    ) -> Result<(), ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        loop {
            let (mut item, generation, certificate, config, state) = {
                let data = runtime.data.lock();
                if data.state.execution_state != RepositoryExecutionState::Active {
                    return Ok(());
                }
                let Some(item) = data
                    .items
                    .iter()
                    .filter(|item| item.kind == QueueItemKind::Gate && !item.state.is_terminal())
                    .min_by_key(|item| item.enqueue_sequence)
                    .cloned()
                else {
                    return Ok(());
                };
                if !item.promotion_authorized {
                    return Ok(());
                }
                if item.state != QueueItemState::Ready {
                    return Ok(());
                }
                let generation = data
                    .generations
                    .iter()
                    .find(|generation| Some(generation.id) == item.current_generation_id)
                    .cloned()
                    .ok_or_else(|| {
                        ServiceError::Invariant("promotion generation missing".into())
                    })?;
                let certificate = data
                    .certificates
                    .iter()
                    .find(|certificate| Some(certificate.id) == item.certificate_id)
                    .cloned()
                    .ok_or_else(|| {
                        ServiceError::Invariant("promotion certificate missing".into())
                    })?;
                (
                    item,
                    generation,
                    certificate,
                    data.config.clone(),
                    data.state.clone(),
                )
            };
            for result in &certificate.voting_results {
                let step = config
                    .steps
                    .iter()
                    .find(|step| {
                        stable_step_id(certificate.buildset_id, &step.name) == result.step_id
                    })
                    .ok_or_else(|| {
                        ServiceError::Invariant(
                            "certificate refers to an unknown voting step".into(),
                        )
                    })?;
                let log_path = runtime
                    .logs_root
                    .join(certificate.buildset_id.to_string())
                    .join(format!("{}.tlog", step.name));
                if !verify_durable_log(
                    log_path,
                    &result.log_hash,
                    result.log_stdout_end,
                    result.log_stderr_end,
                )
                .await?
                {
                    return Err(ServiceError::Invariant(format!(
                        "sealed log evidence for step {} failed integrity verification",
                        step.name
                    )));
                }
            }
            let disk_config = EffectiveConfig::parse(
                &tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                    .await?,
            )?;
            let observed_master = runtime.git.integration_oid().await?;
            if disk_config.digest != config.digest
                || !certificate.validates(
                    &item,
                    &generation,
                    &observed_master,
                    &config.digest,
                    &config.step_graph_digest,
                    state.engine_epoch,
                )
            {
                return Err(ServiceError::Invariant(
                    "certificate failed synchronous promotion revalidation".into(),
                ));
            }
            let push_intent = if config.remote.enabled {
                let remote_fetch_url = runtime.git.remote_url(&config.remote.name, false).await?;
                let remote_push_url = runtime.git.remote_url(&config.remote.name, true).await?;
                let command_id = CommandId::new();
                runtime.store.prepare_operation(
                    repository_id,
                    "push",
                    command_id,
                    &serde_json::json!({
                        "remote": config.remote.name,
                        "remote_fetch_url": remote_fetch_url,
                        "remote_push_url": remote_push_url,
                        "branch": config.remote.branch,
                        "expected_remote": observed_master,
                        "new_oid": certificate.tested_oid,
                        "item_id": item.id,
                    }),
                )?;
                if let Err(error) = self
                    .reserve_runtime_volume(
                        &runtime,
                        command_id,
                        &runtime.git.common_dir,
                        config.resources.volume_critical_bytes,
                    )
                    .await
                {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "promotion-volume-admission", "error": error.to_string()}),
                    )?;
                    eprintln!(
                        "Tollgate deferred promotion while storage admission is unavailable: {error}"
                    );
                    return Ok(());
                }
                let remote = match runtime
                    .git
                    .fetch_remote_ref(
                        &remote_push_url,
                        &config.remote.branch,
                        &remote_observation_ref(&config.remote.name, &config.remote.branch),
                    )
                    .await
                {
                    Ok(remote) => remote,
                    Err(error) => {
                        runtime.store.set_intent_state(
                            command_id,
                            IntentState::Canceled,
                            &serde_json::json!({"stage": "promotion-fetch", "error": error.to_string()}),
                        )?;
                        eprintln!(
                            "Tollgate deferred promotion because the remote could not be observed: {error}"
                        );
                        return Ok(());
                    }
                };
                runtime.store.record_remote_observation(
                    repository_id,
                    command_id,
                    &config.remote.name,
                    &format!("refs/heads/{}", config.remote.branch),
                    remote.as_ref(),
                    "promotion-preflight-fetch",
                )?;
                if remote.as_ref() != Some(&observed_master) {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"recovery": "promotion-remote-preflight-mismatch"}),
                    )?;
                    if remote_fetch_url == remote_push_url
                        && let Some(remote_oid) = remote.as_ref()
                        && runtime
                            .git
                            .is_ancestor(&observed_master, remote_oid)
                            .await?
                    {
                        let adoption_command = CommandId::new();
                        let adoption_digest = command_digest(&serde_json::json!({
                            "repository_id": repository_id,
                            "source": "promotion-preflight",
                            "expected_local": observed_master,
                            "remote_oid": remote_oid,
                        }))?;
                        runtime.store.prepare_operation(
                            repository_id,
                            "pull",
                            adoption_command,
                            &serde_json::json!({
                                "request_digest": adoption_digest,
                                "remote": config.remote.name,
                                "remote_fetch_url": remote_fetch_url,
                                "branch": config.remote.branch,
                                "expected_local": observed_master,
                                "source": "promotion-preflight",
                            }),
                        )?;
                        self.reserve_runtime_volume(
                            &runtime,
                            adoption_command,
                            &runtime.git.common_dir,
                            config.resources.volume_critical_bytes,
                        )
                        .await?;
                        runtime.store.record_remote_observation(
                            repository_id,
                            adoption_command,
                            &config.remote.name,
                            &format!("refs/heads/{}", config.remote.branch),
                            Some(remote_oid),
                            "promotion-preflight-adoption",
                        )?;
                        runtime
                            .git
                            .compare_and_swap_integration(&observed_master, remote_oid)
                            .await?;
                        let mut adopted = runtime.data.lock().state.clone();
                        adopted.master_oid = remote_oid.clone();
                        adopted.queue_revision += 1;
                        let affected = runtime
                            .data
                            .lock()
                            .items
                            .iter()
                            .filter(|item| {
                                item.kind == QueueItemKind::Gate
                                    && is_rebuildable_gate_state(item.state)
                            })
                            .map(|item| item.id)
                            .collect::<Vec<_>>();
                        runtime.data.lock().state = adopted.clone();
                        let rebuilt = self
                            .rebuild_after_base_adoption(&runtime, remote_oid)
                            .await?;
                        if rebuilt != affected {
                            return Err(ServiceError::Invariant(
                                "promotion preflight adoption impact changed".into(),
                            ));
                        }
                        let result = RemoteSyncResult {
                            action: RemoteSyncAction::AdoptedRemote,
                            local_master: remote_oid.clone(),
                            remote_master: Some(remote_oid.clone()),
                            queue_revision: adopted.queue_revision,
                            affected_item_ids: affected,
                            message: "Adopted a remote fast-forward before promotion.".into(),
                        };
                        // Rebuilding emits item projection events, so complete
                        // the adoption against the post-rebuild event sequence.
                        adopted = runtime.data.lock().state.clone();
                        let event = runtime.store.complete_operation(
                            &adopted,
                            "pull",
                            adoption_command,
                            "pull",
                            &adoption_digest,
                            &result,
                            "remote.promotion-preflight-adopted",
                            &serde_json::json!({"local": remote_oid, "source": "promotion-preflight"}),
                            Actor::App,
                        )?;
                        adopted.event_sequence = event.sequence;
                        runtime.data.lock().state = adopted;
                        let _ = runtime.events.send(event);
                        self.spawn_eligible(repository_id, &runtime);
                        return Ok(());
                    }
                    let blocked = {
                        let mut data = runtime.data.lock();
                        data.state.execution_state = RepositoryExecutionState::Blocked;
                        if !data
                            .state
                            .block_reasons
                            .iter()
                            .any(|reason| reason.code == "remote-preflight-mismatch")
                        {
                            data.state.block_reasons.push(BlockReason {
                                code: "remote-preflight-mismatch".into(),
                                message: "Remote master does not equal the exact expected promotion base.".into(),
                                recovery_action: "Fetch and inspect the exact remote tip, then pull or reconcile before promotion.".into(),
                            });
                        }
                        data.state.clone()
                    };
                    runtime.store.update_repository_state(&blocked)?;
                    return Ok(());
                }
                Some((command_id, remote_push_url))
            } else {
                None
            };
            let command_id = CommandId::new();
            if let Err(error) = self
                .require_global_volume_allowance(
                    &runtime,
                    &runtime.git.common_dir,
                    config.resources.volume_emergency_bytes,
                    "promotion evidence retention",
                )
                .await
            {
                if let Some((push_command, _)) = &push_intent {
                    runtime.store.set_intent_state(
                        *push_command,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "promotion-pre-admission", "error": error.to_string()}),
                    )?;
                }
                eprintln!(
                    "Tollgate deferred promotion while storage admission is unavailable: {error}"
                );
                return Ok(());
            }
            if let Err(error) =
                runtime
                    .store
                    .prepare_promotion(repository_id, command_id, &certificate)
            {
                if let Some((push_command, _)) = &push_intent {
                    runtime.store.set_intent_state(
                        *push_command,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "promotion-intent-prepare", "error": error.to_string()}),
                    )?;
                }
                return Err(error.into());
            }
            if let Err(error) = self
                .reserve_runtime_volume(
                    &runtime,
                    command_id,
                    &runtime.git.common_dir,
                    config.resources.volume_critical_bytes,
                )
                .await
            {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"stage": "promotion-volume-admission", "error": error.to_string()}),
                )?;
                if let Some((push_command, _)) = &push_intent {
                    runtime.store.set_intent_state(
                        *push_command,
                        IntentState::Canceled,
                        &serde_json::json!({"stage": "promotion-volume-admission", "error": error.to_string()}),
                    )?;
                }
                eprintln!(
                    "Tollgate deferred promotion while storage admission is unavailable: {error}"
                );
                return Ok(());
            }
            item.state = item
                .state
                .transition(ItemEvent::PromotionStarted)
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            self.replace_item(&runtime, item.clone())?;
            runtime
                .git
                .retain_tested_object(
                    &runtime.mirror,
                    &certificate.buildset_id.to_string(),
                    &certificate.tested_oid,
                )
                .await?;
            self.require_global_volume_allowance(
                &runtime,
                &runtime.git.common_dir,
                0,
                "authoritative release update",
            )
            .await?;
            if runtime.data.lock().state.execution_state != RepositoryExecutionState::Active {
                item.state = item
                    .state
                    .transition(ItemEvent::PromotionDeferred)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                self.replace_item(&runtime, item)?;
                return Ok(());
            }
            runtime
                .git
                .compare_and_swap_integration(&observed_master, &certificate.tested_oid)
                .await?;
            if config.sync_user_master && !config.remote.enabled {
                self.sync_user_master_after_promotion(
                    &runtime,
                    Some(item.id),
                    &certificate.tested_oid,
                    Actor::App,
                )
                .await?;
            }
            item.state = item
                .state
                .transition(if config.remote.enabled {
                    ItemEvent::PromotedWithPush
                } else {
                    ItemEvent::PromotedWithoutPush
                })
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.cleanup_state = match item.cleanup_policy {
                CleanupPolicy::Automatic => CleanupState::Pending,
                CleanupPolicy::RetainWorktree => CleanupState::NotEligible,
            };
            let mut new_state = runtime.data.lock().state.clone();
            new_state.master_oid = certificate.tested_oid.clone();
            new_state.queue_revision += 1;
            new_state.event_sequence += 1;
            new_state.active_window =
                (new_state.active_window + 1).min(new_state.active_window_ceiling);
            runtime.store.record_promotion(
                &new_state,
                &item,
                &certificate,
                observed_master.as_bytes(),
            )?;
            {
                let mut data = runtime.data.lock();
                data.state = new_state;
                if let Some(existing) = data
                    .items
                    .iter_mut()
                    .find(|candidate| candidate.id == item.id)
                {
                    *existing = item.clone();
                }
            }
            if let Err(error) = create_verified_backup(self, &runtime).await {
                eprintln!("Tollgate could not create a post-promotion backup: {error}");
            }
            if let Some((push_command, remote_push_url)) = push_intent {
                item.remote_state = RemoteState::Pushing;
                self.replace_item(&runtime, item.clone())?;
                match runtime
                    .git
                    .push_with_lease(
                        &remote_push_url,
                        &config.remote.branch,
                        Some(&observed_master),
                        &certificate.tested_oid,
                    )
                    .await
                {
                    Ok(()) => {
                        runtime.store.set_intent_state(
                            push_command,
                            IntentState::ExternalApplied,
                            &serde_json::json!({"remote": certificate.tested_oid}),
                        )?;
                        if config.sync_user_master {
                            self.sync_user_master_after_promotion(
                                &runtime,
                                Some(item.id),
                                &certificate.tested_oid,
                                Actor::App,
                            )
                            .await?;
                        }
                        item.state = item
                            .state
                            .transition(ItemEvent::PushCompleted)
                            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                        item.remote_state = RemoteState::Synchronized;
                        self.replace_item(&runtime, item.clone())?;
                        runtime.store.set_intent_state(
                            push_command,
                            IntentState::Completed,
                            &serde_json::json!({
                                "remote": certificate.tested_oid,
                                "projection": "synchronized",
                            }),
                        )?;
                    }
                    Err(error) => {
                        runtime.store.set_intent_state(
                            push_command,
                            IntentState::NeedsAttention,
                            &serde_json::json!({"error": error.to_string()}),
                        )?;
                        item.remote_state = RemoteState::PushBlocked;
                        item.terminal_reason = Some(format!("remote-push-blocked:{error}"));
                        self.replace_item(&runtime, item)?;
                        return Ok(());
                    }
                }
            }
            self.finish_source_cleanup(&runtime, item).await?;
            let active_head_needs_rebuild = {
                let data = runtime.data.lock();
                data.items
                    .iter()
                    .filter(|candidate| {
                        candidate.kind == QueueItemKind::Gate && !candidate.state.is_terminal()
                    })
                    .min_by_key(|candidate| candidate.enqueue_sequence)
                    .and_then(|candidate| {
                        candidate.current_generation_id.and_then(|generation_id| {
                            data.generations
                                .iter()
                                .find(|generation| generation.id == generation_id)
                        })
                    })
                    .is_some_and(|generation| {
                        generation.expected_parent_oid != data.state.master_oid
                    })
            };
            if active_head_needs_rebuild {
                let promoted_base = runtime.data.lock().state.master_oid.clone();
                self.rebuild_after_base_adoption(&runtime, &promoted_base)
                    .await?;
            }
            self.spawn_eligible(repository_id, &runtime);
        }
    }

    async fn finish_source_cleanup(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        mut item: QueueItem,
    ) -> Result<(), ServiceError> {
        if item.cleanup_policy == CleanupPolicy::RetainWorktree {
            item.cleanup_state = CleanupState::NotEligible;
            return self.replace_item(runtime, item);
        }
        let Some(path) = item.metadata.worktree_path.clone() else {
            item.cleanup_state = CleanupState::NotEligible;
            return self.replace_item(runtime, item);
        };
        let Some(branch) = item.metadata.branch.clone() else {
            item.cleanup_state = CleanupState::NotEligible;
            return self.replace_item(runtime, item);
        };
        let command_id = CommandId::new();
        let path_identity = GitRepository::directory_identity(Path::new(&path)).ok();
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": item.repository_id,
            "item_id": item.id,
            "path": path,
            "branch": branch,
            "expected_oid": item.source_oid,
        }))?;
        let evidence = serde_json::json!({
            "request_digest": request_digest,
            "item_id": item.id,
            "path": path,
            "branch": branch,
            "expected_oid": item.source_oid,
            "common_dir": runtime.git.common_dir,
            "path_identity": path_identity,
        });
        runtime
            .store
            .prepare_operation(item.repository_id, "cleanup", command_id, &evidence)?;
        match runtime
            .git
            .cleanup_linked_source_worktree(Path::new(&path), &branch, &item.source_oid)
            .await
        {
            Ok(true) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::ExternalApplied,
                    &serde_json::json!({"path_absent": true, "branch_absent": true}),
                )?;
                self.complete_source_cleanup(runtime, item.id, command_id, &evidence, Actor::App)
            }
            Ok(false) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "source-not-eligible"}),
                )?;
                item.cleanup_state = CleanupState::NotEligible;
                self.replace_item(runtime, item)
            }
            Err(error) => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"error": error.to_string()}),
                )?;
                item.cleanup_state = CleanupState::NeedsAttention;
                self.replace_item(runtime, item)
            }
        }
    }

    async fn sync_user_master_after_promotion(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item_id: Option<QueueItemId>,
        certified_oid: &GitOid,
        actor: Actor,
    ) -> Result<UserMasterSyncOutcome, ServiceError> {
        let projection = self.active_user_master_projection(runtime).await?;
        let (tested_oid, replace_tip, rebase_unsubmitted) =
            if let Some((_, generation_id, target, current)) = projection {
                runtime
                    .git
                    .retain_projected_object(&runtime.mirror, &generation_id.to_string(), &target)
                    .await?;
                (target, Some(current), false)
            } else {
                let replace_tip = item_id.and_then(|id| {
                    runtime
                        .data
                        .lock()
                        .items
                        .iter()
                        .find(|item| {
                            item.id == id && item.metadata.branch.as_deref() == Some(USER_BRANCH)
                        })
                        .map(|item| item.source_oid.clone())
                });
                let has_active_user_master = runtime.data.lock().items.iter().any(|item| {
                    is_rebuildable_gate_state(item.state)
                        && item.metadata.branch.as_deref() == Some(USER_BRANCH)
                });
                let rebase_unsubmitted = replace_tip.is_none() && !has_active_user_master;
                (certified_oid.clone(), replace_tip, rebase_unsubmitted)
            };
        self.sync_user_master_operation(
            runtime,
            item_id,
            &tested_oid,
            replace_tip.as_ref(),
            Some(certified_oid),
            rebase_unsubmitted,
            actor,
        )
        .await
    }

    async fn project_active_user_master(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        actor: Actor,
    ) -> Result<Option<UserMasterSyncOutcome>, ServiceError> {
        if !runtime.data.lock().config.sync_user_master {
            return Ok(None);
        }
        let Some((item_id, generation_id, tested_oid, replace_tip)) =
            self.active_user_master_projection(runtime).await?
        else {
            return Ok(None);
        };
        if tested_oid == replace_tip {
            return Ok(None);
        }
        runtime
            .git
            .retain_projected_object(&runtime.mirror, &generation_id.to_string(), &tested_oid)
            .await?;
        let outcome = self
            .sync_user_master_operation(
                runtime,
                Some(item_id),
                &tested_oid,
                Some(&replace_tip),
                None,
                false,
                actor,
            )
            .await?;
        Ok(Some(outcome))
    }

    async fn active_user_master_projection(
        &self,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<Option<(QueueItemId, ValidationGenerationId, GitOid, GitOid)>, ServiceError> {
        let Some(current) = runtime.git.optional_ref_oid(USER_BRANCH_REF).await? else {
            return Ok(None);
        };
        let projection = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .rev()
                .filter(|item| {
                    item.kind == QueueItemKind::Gate
                        && is_rebuildable_gate_state(item.state)
                        && item.metadata.branch.as_deref() == Some(USER_BRANCH)
                })
                .find_map(|item| {
                    let generation_id = item.current_generation_id?;
                    let generation = data
                        .generations
                        .iter()
                        .find(|generation| generation.id == generation_id)?;
                    let recognized = item.source_oid == current
                        || data.generations.iter().any(|historical| {
                            historical.item_id == item.id && historical.tested_oid == current
                        });
                    recognized.then(|| {
                        (
                            item.id,
                            generation.id,
                            generation.tested_oid.clone(),
                            current.clone(),
                        )
                    })
                })
        };
        Ok(projection)
    }

    async fn sync_user_master_operation(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item_id: Option<QueueItemId>,
        tested_oid: &GitOid,
        replace_tip: Option<&GitOid>,
        remote_oid: Option<&GitOid>,
        rebase_unsubmitted: bool,
        actor: Actor,
    ) -> Result<UserMasterSyncOutcome, ServiceError> {
        let command_id = CommandId::new();
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": runtime.data.lock().state.id,
            "item_id": item_id,
            "tested_oid": tested_oid,
            "replace_tip": replace_tip,
            "remote_oid": remote_oid,
            "rebase_unsubmitted": rebase_unsubmitted,
        }))?;
        runtime.store.prepare_operation(
            runtime.data.lock().state.id,
            "user-master-sync",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "item_id": item_id,
                "tested_oid": tested_oid,
                "replace_tip": replace_tip,
                "remote_oid": remote_oid,
                "rebase_unsubmitted": rebase_unsubmitted,
            }),
        )?;
        self.complete_user_master_sync(
            runtime,
            command_id,
            &request_digest,
            item_id,
            UserMasterSyncProjection {
                tested_oid,
                replace_tip,
                remote_oid,
                rebase_unsubmitted,
            },
            actor,
        )
        .await
    }

    async fn complete_user_master_sync(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        request_digest: &str,
        item_id: Option<QueueItemId>,
        projection: UserMasterSyncProjection<'_>,
        actor: Actor,
    ) -> Result<UserMasterSyncOutcome, ServiceError> {
        let UserMasterSyncProjection {
            tested_oid,
            replace_tip,
            remote_oid,
            rebase_unsubmitted,
        } = projection;
        let config = runtime.data.lock().config.clone();
        let remote_tracking = config
            .remote
            .enabled
            .then(|| {
                remote_oid.map(|oid| {
                    (
                        config.remote.name.as_str(),
                        config.remote.branch.as_str(),
                        oid,
                    )
                })
            })
            .flatten();
        let outcome = runtime
            .git
            .sync_user_master(tested_oid, replace_tip, remote_tracking, rebase_unsubmitted)
            .await
            .unwrap_or_else(|error| UserMasterSyncOutcome::NeedsAttention {
                path: None,
                reason: error.to_string(),
                status_entries: Vec::new(),
            });
        let needs_attention = matches!(&outcome, UserMasterSyncOutcome::NeedsAttention { .. });
        if needs_attention {
            eprintln!(
                "Tollgate could not synchronize local master with the current certified projection: {outcome:?}"
            );
        }
        let mut state = runtime.data.lock().state.clone();
        let event = runtime.store.complete_operation(
            &state,
            "user-master-sync",
            command_id,
            "user-master-sync",
            request_digest,
            &outcome,
            if needs_attention {
                "user-master.sync-needs-attention"
            } else {
                "user-master.synchronized"
            },
            &serde_json::json!({
                "item_id": item_id,
                "tested_oid": tested_oid,
                "replace_tip": replace_tip,
                "remote_oid": remote_oid,
                "outcome": outcome,
            }),
            actor,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(outcome)
    }

    pub async fn cancel(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        expected_revision: u64,
    ) -> Result<(), ServiceError> {
        self.cancel_command(repository_id, item_id, expected_revision, CommandId::new())
            .await?;
        Ok(())
    }

    pub async fn cancel_command(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        expected_revision: u64,
        command_id: CommandId,
    ) -> Result<MutationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let mutation = runtime.mutation.lock().await;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "item_id": item_id,
            "expected_revision": expected_revision,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "cancel", &request_digest)?
        {
            return Ok(response);
        }
        runtime.store.prepare_operation(
            repository_id,
            "cancel",
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "item_id": item_id,
                "expected_revision": expected_revision,
            }),
        )?;
        let mut item = {
            let data = runtime.data.lock();
            if data.state.queue_revision != expected_revision {
                return Err(ServiceError::RevisionConflict {
                    expected: expected_revision,
                    actual: data.state.queue_revision,
                });
            }
            data.items
                .iter()
                .find(|item| item.id == item_id)
                .cloned()
                .ok_or(ServiceError::ItemNotFound(item_id))?
        };
        if let Some(token) = runtime.cancellations.lock().get(&item_id) {
            token.cancel();
        }
        item.state = item
            .state
            .transition(ItemEvent::Canceled)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        item.terminal_reason = Some("canceled-by-user".into());
        let is_check = item.kind == QueueItemKind::IndependentCheck;
        self.replace_item(&runtime, item.clone())?;
        drop(mutation);
        if is_check {
            runtime
                .git
                .delete_source_ref(&item.source_ref, &item.source_oid)
                .await?;
        } else {
            self.rebuild_after_failure(repository_id, item_id).await?;
        }
        let mut state = runtime.data.lock().state.clone();
        let result = MutationResult {
            repository_id,
            action: "cancel".into(),
            message: if is_check {
                "Independent check canceled.".into()
            } else {
                "Queue item canceled and affected prefixes rebuilt.".into()
            },
        };
        let event = runtime.store.complete_operation(
            &state,
            "cancel",
            command_id,
            "cancel",
            &request_digest,
            &result,
            "item.cancel-command-completed",
            &serde_json::json!({"item_id": item_id, "state": "canceled"}),
            Actor::App,
        )?;
        state.event_sequence = event.sequence;
        runtime.data.lock().state = state;
        let _ = runtime.events.send(event);
        Ok(result)
    }

    pub async fn set_paused(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        paused: bool,
    ) -> Result<(), ServiceError> {
        self.set_paused_command(repository_id, paused, CommandId::new())
            .await?;
        Ok(())
    }

    pub async fn set_paused_command(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        paused: bool,
        command_id: CommandId,
    ) -> Result<MutationResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let mutation = runtime.mutation.lock().await;
        let command_kind = if paused { "pause" } else { "resume" };
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "paused": paused,
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, command_kind, &request_digest)?
        {
            return Ok(response);
        }
        runtime.store.prepare_operation(
            repository_id,
            command_kind,
            command_id,
            &serde_json::json!({
                "request_digest": request_digest,
                "paused": paused,
            }),
        )?;
        let state = {
            let mut data = runtime.data.lock();
            if (paused && data.state.execution_state != RepositoryExecutionState::Active)
                || (!paused && data.state.execution_state != RepositoryExecutionState::Paused)
            {
                return Err(ServiceError::RepositoryUnavailable(
                    data.state.execution_state,
                ));
            }
            data.state.execution_state = if paused {
                RepositoryExecutionState::Paused
            } else {
                RepositoryExecutionState::Active
            };
            data.state.clone()
        };
        let result = MutationResult {
            repository_id,
            action: command_kind.into(),
            message: if paused {
                "Gate paused; active commands may finish.".into()
            } else {
                "Gate resumed after structural revalidation.".into()
            },
        };
        let event = runtime.store.complete_operation(
            &state,
            command_kind,
            command_id,
            command_kind,
            &request_digest,
            &result,
            if paused {
                "repository.paused"
            } else {
                "repository.resumed"
            },
            &result,
            Actor::App,
        )?;
        runtime.data.lock().state.event_sequence = event.sequence;
        let _ = runtime.events.send(event);
        drop(mutation);
        if state.execution_state == RepositoryExecutionState::Active {
            self.spawn_eligible(repository_id, &runtime);
            self.promote_ready(repository_id).await?;
        }
        Ok(result)
    }

    pub async fn reload_environment(self: &Arc<Self>) -> Result<EnvironmentView, ServiceError> {
        self.reload_environment_command(CommandId::new()).await
    }

    pub async fn reload_environment_command(
        self: &Arc<Self>,
        command_id: CommandId,
    ) -> Result<EnvironmentView, ServiceError> {
        let request_digest = command_digest(&serde_json::json!({"action": "reload-environment"}))?;
        if let Some(response) = self
            .prepare_global_command(
                "reload-environment",
                command_id,
                &request_digest,
                serde_json::json!({}),
            )
            .await?
        {
            return Ok(serde_json::from_value(response)?);
        }
        let snapshot = EnvironmentSnapshot::capture_login_shell().await?;
        let view = EnvironmentView {
            snapshot_id: snapshot.id.clone(),
            fingerprint: snapshot.fingerprint.clone(),
            path: snapshot.variables.get("PATH").cloned().unwrap_or_default(),
            variable_count: snapshot.variables.len(),
        };
        *self.environment.write().await = snapshot;
        *self.environment_error.write().await = None;
        let runtimes = self
            .runtimes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for runtime in runtimes {
            let _mutation = runtime.mutation.lock().await;
            let state = {
                let mut data = runtime.data.lock();
                data.state
                    .block_reasons
                    .retain(|reason| reason.code != "environment-bootstrap-failed");
                if data.state.execution_state == RepositoryExecutionState::Blocked
                    && data.state.block_reasons.is_empty()
                {
                    data.state.execution_state = RepositoryExecutionState::Active;
                }
                data.state.clone()
            };
            runtime.store.update_repository_state(&state)?;
            if state.execution_state == RepositoryExecutionState::Active {
                self.spawn_eligible(state.id, &runtime);
            }
        }
        self.complete_global_command(command_id, &view).await?;
        Ok(view)
    }

    pub async fn shutdown(self: &Arc<Self>) -> Result<(), ServiceError> {
        self.shutting_down.store(true, Ordering::Release);
        let runtimes = self
            .runtimes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for runtime in &runtimes {
            let _mutation = runtime.mutation.lock().await;
            let (state, interrupted) = {
                let data = runtime.data.lock();
                let interrupted = data
                    .items
                    .iter()
                    .filter(|item| {
                        matches!(
                            item.state,
                            QueueItemState::Preparing | QueueItemState::Running
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                (data.state.clone(), interrupted)
            };
            runtime.store.update_repository_state(&state)?;
            for mut item in interrupted {
                item.state = item
                    .state
                    .transition(ItemEvent::InfrastructureRetry)
                    .map_err(|error| ServiceError::Invariant(error.to_string()))?;
                item.buildset_id = None;
                item.terminal_reason = Some("interrupted-by-orderly-shutdown".into());
                self.replace_item(runtime, item)?;
            }
            for token in runtime.cancellations.lock().values() {
                token.cancel();
            }
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(12);
        while tokio::time::Instant::now() < deadline
            && runtimes
                .iter()
                .any(|runtime| !runtime.cancellations.lock().is_empty())
        {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if runtimes
            .iter()
            .any(|runtime| !runtime.cancellations.lock().is_empty())
        {
            return Err(ServiceError::Invariant(
                "workers did not finish bounded termination; shutdown remains unclean".into(),
            ));
        }
        for runtime in &runtimes {
            runtime.store.checkpoint()?;
        }
        let marker = self.support_root.join("clean-shutdown");
        tokio::fs::write(&marker, OffsetDateTime::now_utc().to_string()).await?;
        std::fs::File::open(&marker)?.sync_all()?;
        if let Some(parent) = marker.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub async fn logs(
        &self,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        requested_buildset_id: Option<BuildsetId>,
        step: Option<String>,
        start_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RenderedLogFrame>, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let (buildset_id, step_name) = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            let buildset_id = requested_buildset_id
                .or(item.buildset_id)
                .ok_or_else(|| ServiceError::Invariant("item has no buildset yet".into()))?;
            let buildset = data
                .buildsets
                .iter()
                .find(|candidate| candidate.id == buildset_id && candidate.item_id == item_id)
                .ok_or_else(|| ServiceError::Invariant("item buildset is missing".into()))?;
            let step_names = if buildset.frozen_steps.is_empty() {
                if buildset.step_results.is_empty() {
                    data.config
                        .steps
                        .iter()
                        .map(|step| step.name.as_str())
                        .collect::<Vec<_>>()
                } else {
                    buildset
                        .step_results
                        .iter()
                        .map(|step| step.name.as_str())
                        .collect::<Vec<_>>()
                }
            } else {
                buildset
                    .frozen_steps
                    .iter()
                    .map(|step| step.name.as_str())
                    .collect::<Vec<_>>()
            };
            let step_name = match step {
                Some(name) if step_names.contains(&name.as_str()) => name,
                Some(name) => {
                    return Err(ServiceError::Invariant(format!("unknown step `{name}`")));
                }
                None => step_names
                    .first()
                    .map(|step| (*step).to_owned())
                    .ok_or_else(|| ServiceError::Invariant("configuration has no steps".into()))?,
            };
            (buildset_id, step_name)
        };
        let path = runtime
            .logs_root
            .join(buildset_id.to_string())
            .join(format!("{step_name}.tlog"));
        if runtime
            .store
            .step_log_state(buildset_id, &step_name)?
            .as_deref()
            == Some("pruned")
        {
            return Err(ServiceError::Invariant(format!(
                "log range for buildset {buildset_id} step `{step_name}` was pruned by retention policy"
            )));
        }
        Ok(read_durable_log(path, start_sequence, limit.min(10_000)).await?)
    }

    pub async fn raw_log_path(
        &self,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        requested_buildset_id: Option<BuildsetId>,
        step: Option<String>,
    ) -> Result<PathBuf, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let (buildset_id, step_name) = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == item_id)
                .ok_or(ServiceError::ItemNotFound(item_id))?;
            let buildset_id = requested_buildset_id
                .or(item.buildset_id)
                .ok_or_else(|| ServiceError::Invariant("item has no buildset yet".into()))?;
            let buildset = data
                .buildsets
                .iter()
                .find(|candidate| candidate.id == buildset_id && candidate.item_id == item_id)
                .ok_or_else(|| ServiceError::Invariant("item buildset is missing".into()))?;
            let step_names = if buildset.frozen_steps.is_empty() {
                if buildset.step_results.is_empty() {
                    data.config
                        .steps
                        .iter()
                        .map(|step| step.name.as_str())
                        .collect::<Vec<_>>()
                } else {
                    buildset
                        .step_results
                        .iter()
                        .map(|step| step.name.as_str())
                        .collect::<Vec<_>>()
                }
            } else {
                buildset
                    .frozen_steps
                    .iter()
                    .map(|step| step.name.as_str())
                    .collect::<Vec<_>>()
            };
            let step_name = match step {
                Some(name) if step_names.contains(&name.as_str()) => name,
                Some(name) => {
                    return Err(ServiceError::Invariant(format!("unknown step `{name}`")));
                }
                None => step_names
                    .first()
                    .map(|step| (*step).to_owned())
                    .ok_or_else(|| ServiceError::Invariant("configuration has no steps".into()))?,
            };
            (buildset_id, step_name)
        };
        if runtime
            .store
            .step_log_state(buildset_id, &step_name)?
            .as_deref()
            == Some("pruned")
        {
            return Err(ServiceError::Invariant(format!(
                "log range for buildset {buildset_id} step `{step_name}` was pruned by retention policy"
            )));
        }
        let buildset_root = runtime.logs_root.join(buildset_id.to_string());
        let path = buildset_root.join(format!("{step_name}.tlog"));
        let canonical_root = tokio::fs::canonicalize(&buildset_root).await?;
        let canonical_path = tokio::fs::canonicalize(&path).await?;
        if canonical_path.parent() != Some(canonical_root.as_path())
            || tokio::fs::symlink_metadata(&canonical_path)
                .await?
                .file_type()
                .is_symlink()
        {
            return Err(ServiceError::Invariant(
                "raw log path escaped its owned buildset directory".into(),
            ));
        }
        Ok(canonical_path)
    }

    pub async fn log_tail_sequence(
        &self,
        repository_id: RepositoryId,
        item_id: QueueItemId,
        requested_buildset_id: Option<BuildsetId>,
        step: Option<String>,
        frame_count: u64,
    ) -> Result<u64, ServiceError> {
        let path = self
            .raw_log_path(repository_id, item_id, requested_buildset_id, step)
            .await?;
        Ok(durable_log_tail_start(path, frame_count).await?)
    }

    pub async fn events(
        &self,
        repository_id: RepositoryId,
    ) -> Result<broadcast::Receiver<DomainEvent>, ServiceError> {
        Ok(self.runtime(repository_id).await?.events.subscribe())
    }

    fn replace_item(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        item: QueueItem,
    ) -> Result<(), ServiceError> {
        let state = runtime.data.lock().state.clone();
        let event = runtime.store.save_item_projection(&state, &item)?;
        {
            let mut data = runtime.data.lock();
            if let Some(existing) = data
                .items
                .iter_mut()
                .find(|candidate| candidate.id == item.id)
            {
                *existing = item;
            }
            data.state.event_sequence = event.sequence;
        }
        let _ = runtime.events.send(event);
        Ok(())
    }

    async fn runtime(&self, id: RepositoryId) -> Result<Arc<RepositoryRuntime>, ServiceError> {
        self.runtimes
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or(ServiceError::RepositoryNotFound(id))
    }

    async fn open_repository_store(&self, path: &Path) -> Result<RepositoryStore, ServiceError> {
        let allowance = RepositoryStore::migration_allowance(path)?;
        if allowance == 0 {
            return Ok(RepositoryStore::open(path)?);
        }
        let _coordinator = self.volume_reservations.lock().await;
        let parent = path
            .parent()
            .ok_or_else(|| ServiceError::Invariant("repository database has no parent".into()))?;
        let volume = nix::sys::statvfs::statvfs(parent)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let available = u64::from(volume.blocks_available()).saturating_mul(volume.fragment_size());
        let volume_id = format!("fs-{:x}", volume.filesystem_id());
        let (reserved, configured_critical, _) = self.global_volume_commitment(&volume_id).await?;
        let required = configured_critical
            .max(10_u64 * 1024 * 1024 * 1024)
            .saturating_add(reserved)
            .saturating_add(allowance);
        if available < required {
            return Err(ServiceError::Invariant(format!(
                "database migration requires {required} bytes free including the shared recovery reserve, but only {available} bytes are available"
            )));
        }
        Ok(RepositoryStore::open(path)?)
    }

    async fn global_volume_commitment(
        &self,
        volume_id: &str,
    ) -> Result<(u64, u64, u64), ServiceError> {
        let runtimes = self
            .runtimes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut reserved = 0u64;
        let mut critical = 0u64;
        let mut warning = 0u64;
        for runtime in runtimes {
            reserved = reserved.saturating_add(runtime.store.active_volume_reservation(volume_id)?);
            if let Some((runtime_warning, runtime_critical)) =
                runtime.store.volume_thresholds(volume_id)?
            {
                critical = critical.max(runtime_critical);
                warning = warning.max(runtime_warning);
            }
        }
        Ok((reserved, critical, warning))
    }

    async fn require_global_volume_allowance(
        &self,
        runtime: &RepositoryRuntime,
        path: &Path,
        allowance: u64,
        operation: &str,
    ) -> Result<(), ServiceError> {
        let volume = nix::sys::statvfs::statvfs(path)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let available = u64::from(volume.blocks_available()).saturating_mul(volume.fragment_size());
        let volume_id = format!("fs-{:x}", volume.filesystem_id());
        let (reserved, global_critical, _) = self.global_volume_commitment(&volume_id).await?;
        let required = runtime
            .data
            .lock()
            .config
            .resources
            .volume_critical_bytes
            .max(global_critical)
            .saturating_add(reserved)
            .saturating_add(allowance);
        if available < required {
            return Err(ServiceError::Invariant(format!(
                "{operation} requires {required} bytes free across shared-volume reservations, but only {available} bytes are available"
            )));
        }
        Ok(())
    }

    async fn require_global_volume_warning(
        &self,
        runtime: &RepositoryRuntime,
        path: &Path,
        operation: &str,
    ) -> Result<(), ServiceError> {
        let volume = nix::sys::statvfs::statvfs(path)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let available = u64::from(volume.blocks_available()).saturating_mul(volume.fragment_size());
        let volume_id = format!("fs-{:x}", volume.filesystem_id());
        let (reserved, _, global_warning) = self.global_volume_commitment(&volume_id).await?;
        let required = runtime
            .data
            .lock()
            .config
            .resources
            .volume_warning_bytes
            .max(global_warning)
            .saturating_add(reserved);
        if available < required {
            return Err(ServiceError::Invariant(format!(
                "{operation} is disabled below the shared warning threshold: {required} bytes are required but only {available} are available"
            )));
        }
        Ok(())
    }

    async fn reserve_runtime_volume(
        &self,
        runtime: &RepositoryRuntime,
        command_id: CommandId,
        path: &Path,
        allowance: u64,
    ) -> Result<(), ServiceError> {
        let _coordinator = self.volume_reservations.lock().await;
        observe_volumes(runtime)?;
        let volume = nix::sys::statvfs::statvfs(path)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        runtime.store.reserve_volume(
            command_id,
            &format!("fs-{:x}", volume.filesystem_id()),
            allowance,
        )?;
        if let Err(error) = self
            .require_global_volume_allowance(runtime, path, 0, "volume reservation")
            .await
        {
            runtime.store.deactivate_volume_reservation(command_id)?;
            return Err(error);
        }
        Ok(())
    }

    async fn registered_common_directory(&self, common_dir: &Path) -> Option<RepositoryId> {
        self.runtimes.read().await.values().find_map(|runtime| {
            (runtime.git.common_dir == common_dir).then(|| runtime.data.lock().state.id)
        })
    }

    async fn prepare_global_command(
        &self,
        kind: &str,
        command_id: CommandId,
        request_digest: &str,
        payload: serde_json::Value,
    ) -> Result<Option<serde_json::Value>, ServiceError> {
        let mut journal = self.global_commands.lock().await;
        let key = command_id.to_string();
        if let Some(record) = journal.records.get(&key) {
            if record.kind != kind || record.request_digest != request_digest {
                return Err(StoreError::CommandReplayMismatch.into());
            }
            if record.state == "completed" {
                return record.response.clone().map(Some).ok_or_else(|| {
                    ServiceError::Invariant(
                        "completed global command omitted its durable response".into(),
                    )
                });
            }
            return Ok(None);
        }
        journal.records.insert(
            key,
            GlobalCommandRecord {
                kind: kind.into(),
                request_digest: request_digest.into(),
                payload,
                state: "prepared".into(),
                response: None,
            },
        );
        self.persist_global_commands(&journal).await?;
        Ok(None)
    }

    async fn complete_global_command(
        &self,
        command_id: CommandId,
        response: &impl Serialize,
    ) -> Result<(), ServiceError> {
        let mut journal = self.global_commands.lock().await;
        let record = journal
            .records
            .get_mut(&command_id.to_string())
            .ok_or_else(|| ServiceError::Invariant("global command intent is missing".into()))?;
        record.state = "completed".into();
        record.response = Some(serde_json::to_value(response)?);
        self.persist_global_commands(&journal).await
    }

    async fn persist_global_commands(
        &self,
        journal: &GlobalCommandJournal,
    ) -> Result<(), ServiceError> {
        let parent = self.global_command_path.parent().ok_or_else(|| {
            ServiceError::Invariant("global command journal has no parent".into())
        })?;
        let temporary = parent.join(format!(
            ".global-command-results-{}.json.tmp",
            uuid::Uuid::now_v7()
        ));
        let bytes = serde_json::to_vec_pretty(journal)?;
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temporary, &self.global_command_path).await?;
        sync_directory(parent)?;
        Ok(())
    }

    async fn reconcile_global_commands(self: &Arc<Self>) -> Result<(), ServiceError> {
        let prepared = self
            .global_commands
            .lock()
            .await
            .records
            .iter()
            .filter(|(_, record)| record.state == "prepared")
            .map(|(id, record)| (id.clone(), record.clone()))
            .collect::<Vec<_>>();
        for (id, record) in prepared {
            let command_id: CommandId = id.parse().map_err(|error| {
                ServiceError::Invariant(format!("invalid global command ID: {error}"))
            })?;
            match record.kind.as_str() {
                "remove-repository" => {
                    let repository_id: RepositoryId = serde_json::from_value(
                        record
                            .payload
                            .get("repository_id")
                            .cloned()
                            .ok_or_else(|| {
                                ServiceError::Invariant(
                                    "remove-repository intent omitted repository ID".into(),
                                )
                            })?,
                    )?;
                    if let Ok(runtime) = self.runtime(repository_id).await {
                        if runtime
                            .data
                            .lock()
                            .items
                            .iter()
                            .any(|item| !item.state.is_terminal())
                        {
                            eprintln!(
                                "Tollgate preserved prepared repository removal {command_id}: active work now exists"
                            );
                            continue;
                        }
                        self.runtimes.write().await.remove(&repository_id);
                        self.reconfigure_global_scheduler().await;
                        self.save_registry().await?;
                    }
                    let result = MutationResult {
                        repository_id,
                        action: "remove-repository".into(),
                        message: "Recovered repository registry removal after restart; repository-local state was preserved.".into(),
                    };
                    self.complete_global_command(command_id, &result).await?;
                }
                "reload-environment" => {
                    self.global_commands.lock().await.records.remove(&id);
                    let journal = self.global_commands.lock().await;
                    self.persist_global_commands(&journal).await?;
                }
                other => {
                    return Err(ServiceError::Invariant(format!(
                        "unknown prepared global command kind {other}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Rebuild shared admission so every request accepted by a repository's frozen policy remains
    /// satisfiable. Per-repository concurrency is independently epoch-guarded; a repository with
    /// no CPU or memory requests must not shrink another repository's declared pool.
    async fn reconfigure_global_scheduler(&self) {
        let runtimes = self
            .runtimes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let max_buildsets = runtimes
            .iter()
            .map(|runtime| runtime.data.lock().config.resources.max_buildsets)
            .max()
            .unwrap_or(4)
            .max(1);
        let cpu_tokens = runtimes
            .iter()
            .map(|runtime| runtime.data.lock().config.resources.cpu_tokens)
            .max()
            .unwrap_or(1);
        let memory_bytes = runtimes
            .iter()
            .map(|runtime| runtime.data.lock().config.resources.memory_bytes)
            .max()
            .unwrap_or(1);
        self.global_scheduler.reconfigure(ResourceCapacity {
            max_buildsets,
            cpu_tokens: cpu_tokens.max(1),
            memory_bytes: memory_bytes.max(1),
            semaphores: BTreeMap::new(),
        });
    }

    async fn load_registry(self: &Arc<Self>) -> Result<(), ServiceError> {
        if !self.registry_path.exists() {
            return Ok(());
        }
        let bytes = tokio::fs::read(&self.registry_path).await?;
        let records: Vec<RegisteredRepository> = match serde_json::from_slice(&bytes) {
            Ok(records) => records,
            Err(error) => {
                let preserved = self
                    .registry_path
                    .with_extension(format!("corrupt-{}-json", uuid::Uuid::now_v7()));
                tokio::fs::rename(&self.registry_path, &preserved).await?;
                eprintln!(
                    "Preserved malformed repository registry at {}: {error}",
                    preserved.display()
                );
                Vec::new()
            }
        };
        for record in records {
            if let Err(error) = self.register_existing(&record.path).await {
                self.runtimes.write().await.remove(&record.id);
                self.unavailable.write().await.push(UnavailableRepository {
                    id: record.id,
                    name: record.name.clone(),
                    path: record.path.clone(),
                    error: error.to_string(),
                    recovery_action: "Preserve the repository-local tollgate directory, repair the reported condition, then reopen the repository or run Doctor.".into(),
                });
                eprintln!(
                    "Tollgate could not activate repository {} at {}: {error}",
                    record.id,
                    record.path.display()
                );
            }
        }
        Ok(())
    }

    async fn save_registry(&self) -> Result<(), ServiceError> {
        let mut records = {
            let runtimes = self.runtimes.read().await;
            runtimes
                .values()
                .map(|runtime| {
                    let data = runtime.data.lock();
                    RegisteredRepository {
                        id: data.state.id,
                        name: data.state.name.clone(),
                        path: runtime.git.worktree_root.clone(),
                    }
                })
                .collect::<Vec<_>>()
        };
        records.extend(
            self.unavailable
                .read()
                .await
                .iter()
                .map(|entry| RegisteredRepository {
                    id: entry.id,
                    name: entry.name.clone(),
                    path: entry.path.clone(),
                }),
        );
        records.sort_by_key(|record| record.id);
        let parent = self
            .registry_path
            .parent()
            .ok_or_else(|| ServiceError::Invariant("registry has no parent".into()))?;
        let temporary = parent.join(format!(".repositories-{}.json.tmp", uuid::Uuid::now_v7()));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(&serde_json::to_vec_pretty(&records)?)
            .await?;
        file.sync_all().await?;
        tokio::fs::rename(temporary, &self.registry_path).await?;
        sync_directory(parent)?;
        Ok(())
    }
}

async fn load_existing_slots(root: &Path, expected_common_dir: &Path) -> HashMap<SlotId, SlotView> {
    let mut slots = HashMap::new();
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return slots;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let Ok(id) = name.parse::<SlotId>() else {
            continue;
        };
        let path = entry.path();
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let checkout_oid = match GitRepository::discover(&path).await {
            Ok(repository)
                if paths_identical(&repository.common_dir, expected_common_dir).await =>
            {
                repository.resolve_oid("HEAD").await.ok()
            }
            Err(_) => None,
            Ok(_) => None,
        };
        slots.insert(
            id,
            SlotView {
                id,
                path,
                state: "idle".into(),
                checkout_oid: checkout_oid.clone(),
                health: if checkout_oid.is_some() {
                    "healthy".into()
                } else {
                    "interrupted".into()
                },
                last_used: None,
            },
        );
    }
    slots
}

fn acquire_repository_lock(
    common_dir: &Path,
) -> Result<nix::fcntl::Flock<std::fs::File>, ServiceError> {
    use nix::fcntl::{Flock, FlockArg};
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    let path = common_dir.join("tollgate/repository-authority.lock");
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(&path)?;
    Flock::lock(file, FlockArg::LockExclusiveNonblock).map_err(|(_, error)| {
        ServiceError::Invariant(format!(
            "another Tollgate authority owns {}: {error}",
            path.display()
        ))
    })
}

async fn paths_identical(left: &Path, right: &Path) -> bool {
    match (
        tokio::fs::canonicalize(left).await,
        tokio::fs::canonicalize(right).await,
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn block_for_remote_push_ambiguity(runtime: &RepositoryRuntime, message: &str) -> RepositoryState {
    let mut data = runtime.data.lock();
    data.state.execution_state = RepositoryExecutionState::Blocked;
    if !data
        .state
        .block_reasons
        .iter()
        .any(|reason| reason.code == "push-outcome-ambiguous")
    {
        data.state.block_reasons.push(BlockReason {
            code: "push-outcome-ambiguous".into(),
            message: message.into(),
            recovery_action:
                "Inspect the exact frozen remote ref and run reconcile before any further push."
                    .into(),
        });
    }
    data.state.clone()
}

fn detect_command(root: &Path) -> String {
    if root.join("ci").exists() {
        "./ci".into()
    } else if root.join("Cargo.toml").exists() {
        "cargo test --all-targets".into()
    } else if root.join("package.json").exists() {
        "npm test".into()
    } else if root.join("Makefile").exists() {
        "make test".into()
    } else {
        "git diff --check HEAD^ HEAD".into()
    }
}

async fn prepare_configuration_root(git: &GitRepository) -> Result<(), ServiceError> {
    tokio::fs::create_dir_all(git.worktree_root.join(".tollgate")).await?;
    let exclude_path = git.common_dir.join("info/exclude");
    let mut excludes = tokio::fs::read_to_string(&exclude_path)
        .await
        .unwrap_or_default();
    if !excludes.lines().any(|line| line.trim() == ".tollgate/") {
        if !excludes.is_empty() && !excludes.ends_with('\n') {
            excludes.push('\n');
        }
        excludes.push_str(".tollgate/\n");
        if let Some(parent) = exclude_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(exclude_path, excludes).await?;
    }
    Ok(())
}

async fn read_or_migrate_configuration(
    git: &GitRepository,
    generated: Option<&str>,
) -> Result<String, ServiceError> {
    let authoritative = git.worktree_root.join(".tollgate/config.toml");
    match tokio::fs::read_to_string(&authoritative).await {
        Ok(contents) => return Ok(contents),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        Err(_) => {}
    }
    let legacy = git.common_dir.join("tollgate/config.toml");
    let contents = match tokio::fs::read_to_string(&legacy).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => generated
            .ok_or(ServiceError::MissingConfiguration)?
            .to_owned(),
        Err(error) => return Err(error.into()),
    };
    tokio::fs::write(authoritative, &contents).await?;
    Ok(contents)
}

async fn mirror_legacy_configuration(
    git: &GitRepository,
    contents: &str,
) -> Result<(), ServiceError> {
    let legacy = git.common_dir.join("tollgate/config.toml");
    if tokio::fs::read(&legacy)
        .await
        .is_ok_and(|current| current == contents.as_bytes())
    {
        return Ok(());
    }
    let parent = legacy
        .parent()
        .ok_or_else(|| ServiceError::Invariant("legacy configuration path has no parent".into()))?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = parent.join(format!(".config.{}.toml.tmp", uuid::Uuid::now_v7()));
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await?;
    file.write_all(contents.as_bytes()).await?;
    file.sync_all().await?;
    tokio::fs::rename(&temporary, &legacy).await?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn resolve_executable(executable: &str, environment: &BTreeMap<String, String>) -> Option<PathBuf> {
    let path = Path::new(executable);
    if path.is_absolute() {
        return path.is_file().then(|| path.to_owned());
    }
    environment.get("PATH").and_then(|search| {
        std::env::split_paths(search)
            .map(|directory| directory.join(executable))
            .find(|candidate| candidate.is_file())
    })
}

async fn create_verified_backup(
    service: &TollgateService,
    runtime: &RepositoryRuntime,
) -> Result<(), ServiceError> {
    let root = runtime.git.common_dir.join("tollgate/backups");
    std::fs::create_dir_all(&root)?;
    if std::fs::symlink_metadata(&root)?.file_type().is_symlink() {
        return Err(ServiceError::Invariant(
            "repository backup root was replaced by a symlink".into(),
        ));
    }
    let database_size =
        std::fs::metadata(runtime.git.common_dir.join("tollgate/state.sqlite3"))?.len();
    let emergency_allowance = runtime.data.lock().config.resources.volume_emergency_bytes;
    let allowance = database_size.saturating_add(emergency_allowance);
    let identity = format!(
        "{}-{}",
        OffsetDateTime::now_utc().unix_timestamp(),
        uuid::Uuid::now_v7()
    );
    let temporary = root.join(format!(".{identity}.sqlite3.tmp"));
    let destination = root.join(format!("{identity}.sqlite3"));
    let command_id = CommandId::new();
    let evidence = BackupEvidence {
        repository_id: runtime.data.lock().state.id,
        temporary: temporary.clone(),
        destination: destination.clone(),
        allowance,
    };
    runtime
        .store
        .prepare_operation(evidence.repository_id, "backup", command_id, &evidence)?;
    if let Err(error) = service
        .reserve_runtime_volume(runtime, command_id, &runtime.git.common_dir, allowance)
        .await
    {
        runtime.store.set_intent_state(
            command_id,
            IntentState::Canceled,
            &serde_json::json!({"stage": "backup-volume-admission", "error": error.to_string()}),
        )?;
        return Err(error);
    }
    let publication = (|| -> Result<String, ServiceError> {
        runtime.store.backup_to(&temporary)?;
        std::fs::File::open(&temporary)?.sync_all()?;
        std::fs::rename(&temporary, &destination)?;
        sync_directory(&root)?;
        Ok(RepositoryStore::verified_backup_hash(&destination)?)
    })();
    let hash = match publication {
        Ok(hash) => hash,
        Err(error) => {
            let destination_exists = destination.exists();
            if temporary.exists()
                && std::fs::symlink_metadata(&temporary).is_ok_and(|meta| meta.is_file())
            {
                let _ = std::fs::remove_file(&temporary);
                let _ = sync_directory(&root);
            }
            runtime.store.set_intent_state(
                command_id,
                if destination_exists {
                    IntentState::NeedsAttention
                } else {
                    IntentState::Canceled
                },
                &serde_json::json!({"stage": "backup-publication", "error": error.to_string()}),
            )?;
            return Err(error);
        }
    };
    runtime
        .store
        .complete_backup(command_id, &destination, &hash)?;
    let mut backups = std::fs::read_dir(&root)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_type()
                .is_ok_and(|kind| kind.is_file() && !kind.is_symlink())
                && entry.path().extension().and_then(|value| value.to_str()) == Some("sqlite3")
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    backups.sort();
    let remove_count = backups.len().saturating_sub(5);
    for old in backups.into_iter().take(remove_count) {
        if old.parent() == Some(root.as_path()) {
            std::fs::remove_file(old)?;
        }
    }
    sync_directory(&root)?;
    Ok(())
}

fn toml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn stable_step_id(buildset: BuildsetId, name: &str) -> StepId {
    let mut bytes = *buildset.0.as_bytes();
    for (index, byte) in name.as_bytes().iter().enumerate() {
        bytes[index % 16] ^= byte;
    }
    StepId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

fn freeze_steps(buildset: BuildsetId, config: &EffectiveConfig) -> Vec<FrozenStep> {
    let ids = config
        .steps
        .iter()
        .map(|step| (step.name.as_str(), stable_step_id(buildset, &step.name)))
        .collect::<BTreeMap<_, _>>();
    config
        .steps
        .iter()
        .map(|step| FrozenStep {
            id: ids[step.name.as_str()],
            name: step.name.clone(),
            command: match &step.command {
                tollgate_config::EffectiveCommand::Shell { script } => FrozenCommand::Shell {
                    runner: config.runner.clone(),
                    script: script.clone(),
                },
                tollgate_config::EffectiveCommand::Argv { argv } => {
                    FrozenCommand::Argv { argv: argv.clone() }
                }
            },
            working_directory: step.working_directory.clone(),
            needs: step
                .needs
                .iter()
                .filter_map(|name| ids.get(name.as_str()).copied())
                .collect(),
            soft_needs: step
                .soft_needs
                .iter()
                .filter_map(|name| ids.get(name.as_str()).copied())
                .collect(),
            voting: step.voting,
            final_step: step.final_step,
            timeout_ns: step.timeout_ns,
            cpu_tokens: step.cpu_tokens,
            memory_bytes: step.memory_bytes,
            rss_limit_bytes: step.rss_limit_bytes,
            semaphores: step.semaphores.clone(),
        })
        .collect()
}

fn command_digest(value: &impl Serialize) -> Result<String, ServiceError> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}

fn compatible_seed(data: &RuntimeData) -> Result<Option<SeedRecord>, ServiceError> {
    let cache_policy_digest = command_digest(&data.config.cache)?;
    Ok(data
        .seeds
        .iter()
        .filter(|seed| {
            seed.state == "published"
                && seed.repository_id == data.state.id
                && seed
                    .manifest
                    .get("cache_epoch")
                    .and_then(serde_json::Value::as_u64)
                    == Some(data.config.cache.epoch)
                && seed.manifest.get("os").and_then(serde_json::Value::as_str)
                    == Some(std::env::consts::OS)
                && seed
                    .manifest
                    .get("architecture")
                    .and_then(serde_json::Value::as_str)
                    == Some(std::env::consts::ARCH)
                && seed
                    .manifest
                    .get("cache_policy_digest")
                    .and_then(serde_json::Value::as_str)
                    == Some(cache_policy_digest.as_str())
        })
        .max_by_key(|seed| seed.generation)
        .cloned())
}

fn rollback_hydrated_paths(worktree: &Path, paths: &[PathBuf]) -> Result<(), ServiceError> {
    for path in paths.iter().rev() {
        if !path.starts_with(worktree) || path == worktree {
            return Err(ServiceError::Invariant(
                "cache hydration rollback escaped the new worktree".into(),
            ));
        }
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ServiceError::Invariant(
                "cache hydration rollback found an unsafe destination".into(),
            ));
        }
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn worktree_hydration_staging(
    runtime: &RepositoryRuntime,
    command_id: CommandId,
) -> Result<PathBuf, ServiceError> {
    let cache_root = runtime
        .slots_root
        .parent()
        .ok_or_else(|| ServiceError::Invariant("cache root has no parent".into()))?;
    let hydration_root = cache_root.join("worktree-hydration");
    std::fs::create_dir_all(&hydration_root)?;
    verify_owned_directory(cache_root, &hydration_root)?;
    Ok(hydration_root.join(command_id.to_string()))
}

fn cleanup_worktree_hydration_staging(
    runtime: &RepositoryRuntime,
    command_id: CommandId,
) -> Result<(), ServiceError> {
    let staging = worktree_hydration_staging(runtime, command_id)?;
    if staging.exists() {
        let parent = staging
            .parent()
            .ok_or_else(|| ServiceError::Invariant("hydration staging has no parent".into()))?;
        verify_owned_directory(parent, &staging)?;
        std::fs::remove_dir_all(staging)?;
    }
    Ok(())
}

fn derived_command_id(parent: CommandId, label: &str) -> CommandId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(parent.0.as_bytes());
    hasher.update(&[0]);
    hasher.update(label.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    CommandId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

fn remote_observation_ref(remote: &str, branch: &str) -> String {
    let identity = blake3::hash(format!("{remote}\0{branch}").as_bytes())
        .to_hex()
        .to_string();
    format!("refs/tollgate/remotes/{}", &identity[..24])
}

const fn is_rebuildable_gate_state(state: QueueItemState) -> bool {
    matches!(
        state,
        QueueItemState::Constructing
            | QueueItemState::Queued
            | QueueItemState::Preparing
            | QueueItemState::Running
            | QueueItemState::Ready
    )
}

fn ensure_owned_artifact_path(root: &Path, candidate: &Path) -> Result<(), ServiceError> {
    if candidate.parent() != Some(root) || candidate.file_name().is_none() {
        return Err(ServiceError::Invariant(
            "artifact intent escaped its owned publication root".into(),
        ));
    }
    if candidate.exists() {
        let metadata = std::fs::symlink_metadata(candidate)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ServiceError::Invariant(
                "artifact publication path is not an owned directory".into(),
            ));
        }
        let canonical_root = std::fs::canonicalize(root)?;
        let canonical_candidate = std::fs::canonicalize(candidate)?;
        if !canonical_candidate.starts_with(&canonical_root) {
            return Err(ServiceError::Invariant(
                "artifact publication resolved outside its owned root".into(),
            ));
        }
    }
    Ok(())
}

fn verify_owned_directory(root: &Path, candidate: &Path) -> Result<(), ServiceError> {
    let root_metadata = std::fs::symlink_metadata(root)?;
    let candidate_metadata = std::fs::symlink_metadata(candidate)?;
    if root_metadata.file_type().is_symlink()
        || candidate_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
        || !candidate_metadata.is_dir()
    {
        return Err(ServiceError::Invariant(
            "owned artifact directory identity is unsafe".into(),
        ));
    }
    let canonical_root = std::fs::canonicalize(root)?;
    let canonical_candidate = std::fs::canonicalize(candidate)?;
    if !canonical_candidate.starts_with(&canonical_root) {
        return Err(ServiceError::Invariant(
            "owned artifact directory escaped its root".into(),
        ));
    }
    Ok(())
}

async fn verify_retained_artifact(
    runtime: &RepositoryRuntime,
    record: &ArtifactRecord,
) -> Result<(), ServiceError> {
    let root = runtime.git.common_dir.join("tollgate/artifacts");
    let path = PathBuf::from(&record.retained_path);
    if !path.starts_with(&root) || path == root {
        return Err(ServiceError::Invariant(
            "retained artifact path escaped its authoritative root".into(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| ServiceError::Invariant("retained artifact has no parent".into()))?;
    verify_owned_directory(&root, parent)?;
    let metadata = tokio::fs::symlink_metadata(&path).await?;
    if !metadata.file_type().is_file()
        || metadata.len() != record.size
        || hash_file(&path).await? != record.hash
    {
        return Err(ServiceError::Invariant(format!(
            "retained artifact {} no longer matches its immutable record",
            record.artifact_id
        )));
    }
    Ok(())
}

async fn verify_quarantined_artifact(
    artifacts_root: &Path,
    evidence: &ArtifactPruneEvidence,
) -> Result<(), ServiceError> {
    let parent = evidence
        .quarantine_path
        .parent()
        .ok_or_else(|| ServiceError::Invariant("artifact quarantine has no parent".into()))?;
    verify_owned_directory(artifacts_root, parent)?;
    let metadata = tokio::fs::symlink_metadata(&evidence.quarantine_path).await?;
    if !metadata.file_type().is_file()
        || metadata.len() != evidence.record.size
        || hash_file(&evidence.quarantine_path).await? != evidence.record.hash
    {
        return Err(ServiceError::Invariant(
            "quarantined artifact does not match the pruning intent".into(),
        ));
    }
    Ok(())
}

fn verify_seed_publication(
    actual_root: &Path,
    evidence: &SeedSnapshotEvidence,
) -> Result<SeedRecord, ServiceError> {
    let expected_parent = evidence
        .destination
        .parent()
        .ok_or_else(|| ServiceError::Invariant("seed destination has no parent".into()))?;
    if actual_root.parent() != Some(expected_parent) {
        return Err(ServiceError::Invariant(
            "seed generation escaped its owned profile root".into(),
        ));
    }
    verify_owned_directory(expected_parent, actual_root)?;
    let manifest_path = actual_root.join(".tollgate-seed-manifest.json");
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    let expected_string = |name: &str, expected: &str| -> Result<(), ServiceError> {
        if manifest.get(name).and_then(serde_json::Value::as_str) != Some(expected) {
            return Err(ServiceError::Invariant(format!(
                "seed manifest field `{name}` differs from its intent"
            )));
        }
        Ok(())
    };
    expected_string("seed_id", &evidence.seed_id)?;
    expected_string("profile", "default")?;
    expected_string("cache_policy_digest", &evidence.cache_policy_digest)?;
    expected_string("configuration_digest", &evidence.configuration_digest)?;
    expected_string("os", &evidence.os)?;
    expected_string("architecture", &evidence.architecture)?;
    if manifest.get("version").and_then(serde_json::Value::as_u64) != Some(1)
        || manifest
            .get("repository_id")
            .and_then(serde_json::Value::as_str)
            != Some(evidence.repository_id.to_string().as_str())
        || manifest
            .get("generation")
            .and_then(serde_json::Value::as_u64)
            != Some(evidence.generation)
        || manifest
            .get("cache_epoch")
            .and_then(serde_json::Value::as_u64)
            != Some(evidence.cache_epoch)
        || manifest
            .get("source_slot")
            .and_then(serde_json::Value::as_str)
            != Some(evidence.source_slot.to_string().as_str())
    {
        return Err(ServiceError::Invariant(
            "seed manifest identity differs from its durable intent".into(),
        ));
    }
    let observed_source_oid: Option<GitOid> = manifest
        .get("source_oid")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?;
    if observed_source_oid != evidence.source_oid {
        return Err(ServiceError::Invariant(
            "seed source OID differs from its durable intent".into(),
        ));
    }
    let entries = manifest
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ServiceError::Invariant("seed manifest omitted entries".into()))?;
    if entries.len() != evidence.selected.len() {
        return Err(ServiceError::Invariant(
            "seed manifest selection differs from its intent".into(),
        ));
    }
    let mut expected_paths = HashSet::from([PathBuf::from(".tollgate-seed-manifest.json")]);
    let mut logical_size = 0_u64;
    for (entry, expected_relative) in entries.iter().zip(&evidence.selected) {
        let relative: PathBuf = serde_json::from_value(
            entry
                .get("path")
                .cloned()
                .ok_or_else(|| ServiceError::Invariant("seed entry omitted path".into()))?,
        )?;
        if &relative != expected_relative
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(ServiceError::Invariant(
                "seed entry path differs from its normalized selection".into(),
            ));
        }
        let clone_manifest: CloneManifest =
            serde_json::from_value(entry.get("clone_manifest").cloned().ok_or_else(|| {
                ServiceError::Invariant("seed entry omitted clone manifest".into())
            })?)?;
        verify_clone_tree(&actual_root.join(&relative), &clone_manifest)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        logical_size = logical_size.saturating_add(clone_manifest.logical_size);
        for ancestor in relative
            .ancestors()
            .filter(|path| !path.as_os_str().is_empty())
        {
            expected_paths.insert(ancestor.to_owned());
        }
        for clone_entry in &clone_manifest.entries {
            let path = relative.join(&clone_entry.relative_path);
            for ancestor in path.ancestors().filter(|path| !path.as_os_str().is_empty()) {
                expected_paths.insert(ancestor.to_owned());
            }
        }
    }
    let mut observed_paths = HashSet::new();
    let mut directories = vec![PathBuf::new()];
    while let Some(relative) = directories.pop() {
        let mut children =
            std::fs::read_dir(actual_root.join(&relative))?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let child_relative = relative.join(child.file_name());
            let metadata = std::fs::symlink_metadata(child.path())?;
            observed_paths.insert(child_relative.clone());
            if metadata.file_type().is_dir() {
                directories.push(child_relative);
            } else if !metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
                return Err(ServiceError::Invariant(
                    "seed publication contains an unsafe special file".into(),
                ));
            }
        }
    }
    if observed_paths != expected_paths
        || manifest
            .get("logical_size")
            .and_then(serde_json::Value::as_u64)
            != Some(logical_size)
    {
        return Err(ServiceError::Invariant(
            "seed publication tree differs from its exact manifest".into(),
        ));
    }
    Ok(SeedRecord {
        id: evidence.seed_id.clone(),
        repository_id: evidence.repository_id,
        profile: "default".into(),
        generation: evidence.generation,
        path: actual_root.to_string_lossy().into_owned(),
        logical_size,
        state: "published".into(),
        manifest,
    })
}

fn verify_seed_record_at(actual_root: &Path, record: &SeedRecord) -> Result<(), ServiceError> {
    let manifest = &record.manifest;
    let string = |name: &str| -> Result<String, ServiceError> {
        manifest
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| ServiceError::Invariant(format!("seed record omitted `{name}`")))
    };
    let id = string("seed_id")?;
    let repository_id = string("repository_id")?.parse().map_err(|error| {
        ServiceError::Invariant(format!("seed record repository ID is invalid: {error}"))
    })?;
    let source_slot = string("source_slot")?.parse().map_err(|error| {
        ServiceError::Invariant(format!("seed record source slot is invalid: {error}"))
    })?;
    let selected = manifest
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ServiceError::Invariant("seed record omitted entries".into()))?
        .iter()
        .map(|entry| {
            serde_json::from_value(
                entry
                    .get("path")
                    .cloned()
                    .ok_or_else(|| ServiceError::Invariant("seed entry omitted path".into()))?,
            )
            .map_err(ServiceError::from)
        })
        .collect::<Result<Vec<PathBuf>, _>>()?;
    let source_oid = manifest
        .get("source_oid")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?;
    let evidence = SeedSnapshotEvidence {
        repository_id,
        request_digest: String::new(),
        seed_id: id,
        generation: manifest
            .get("generation")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| ServiceError::Invariant("seed record omitted generation".into()))?,
        staging: actual_root.with_file_name(format!(".staging-{}", record.id)),
        destination: actual_root.to_owned(),
        source_slot,
        source_oid,
        selected,
        cache_epoch: manifest
            .get("cache_epoch")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| ServiceError::Invariant("seed record omitted cache epoch".into()))?,
        cache_policy_digest: string("cache_policy_digest")?,
        configuration_digest: string("configuration_digest")?,
        os: string("os")?,
        architecture: string("architecture")?,
    };
    let verified = verify_seed_publication(actual_root, &evidence)?;
    if verified.id != record.id
        || verified.repository_id != record.repository_id
        || verified.generation != record.generation
        || verified.logical_size != record.logical_size
        || verified.manifest != record.manifest
    {
        return Err(ServiceError::Invariant(
            "seed generation differs from its durable record".into(),
        ));
    }
    Ok(())
}

async fn verify_slot_checkout(
    runtime: &RepositoryRuntime,
    path: &Path,
    expected: &GitOid,
) -> Result<(), ServiceError> {
    let slots_root = std::fs::canonicalize(&runtime.slots_root)?;
    let path = std::fs::canonicalize(path)?;
    if path.parent() != Some(slots_root.as_path())
        || std::fs::symlink_metadata(&path)?.file_type().is_symlink()
    {
        return Err(ServiceError::Invariant(
            "slot path escaped its owned direct-child root".into(),
        ));
    }
    let repository = GitRepository::discover(&path).await?;
    if !paths_identical(&repository.common_dir, &runtime.mirror).await
        || repository.resolve_oid("HEAD").await? != *expected
    {
        return Err(ServiceError::Invariant(
            "slot checkout differs from its cache operation evidence".into(),
        ));
    }
    Ok(())
}

fn remove_owned_quarantine(cache_root: &Path, path: &Path) -> Result<(), ServiceError> {
    let quarantine_root = std::fs::canonicalize(cache_root.join("quarantine"))?;
    let path = std::fs::canonicalize(path)?;
    let metadata = std::fs::symlink_metadata(&path)?;
    if path.parent() != Some(quarantine_root.as_path())
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
    {
        return Err(ServiceError::Invariant(
            "cache quarantine cleanup rejected an unowned path".into(),
        ));
    }
    std::fs::remove_dir_all(&path)?;
    sync_directory(&quarantine_root)?;
    Ok(())
}

async fn hash_file(path: &Path) -> Result<String, ServiceError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

async fn copy_artifact_exclusive(
    source: &Path,
    destination: &Path,
    record: &ArtifactRecord,
) -> Result<(), ServiceError> {
    let metadata = tokio::fs::symlink_metadata(source).await?;
    if !metadata.file_type().is_file() || metadata.len() != record.size {
        return Err(ServiceError::Invariant(
            "artifact changed after its retention manifest was prepared".into(),
        ));
    }
    if force_clone_file(source, destination).is_ok() {
        std::fs::File::open(destination)?.sync_all()?;
        let cloned = tokio::fs::symlink_metadata(destination).await?;
        if cloned.file_type().is_file()
            && cloned.len() == record.size
            && hash_file(destination).await? == record.hash
        {
            return Ok(());
        }
        tokio::fs::remove_file(destination).await?;
        return Err(ServiceError::Invariant(
            "artifact APFS clone did not preserve the frozen size and hash".into(),
        ));
    } else if tokio::fs::try_exists(destination).await? {
        let partial = tokio::fs::symlink_metadata(destination).await?;
        if !partial.file_type().is_file() {
            return Err(ServiceError::Invariant(
                "failed artifact clone left an unsafe destination".into(),
            ));
        }
        tokio::fs::remove_file(destination).await?;
    }
    Err(ServiceError::Invariant(
        "artifact retention requires a verified APFS clone and does not fall back to a physical copy"
            .into(),
    ))
}

async fn verify_artifact_staging(evidence: &ArtifactRetentionEvidence) -> Result<(), ServiceError> {
    let root = evidence
        .staging_dir
        .parent()
        .ok_or_else(|| ServiceError::Invariant("artifact staging has no parent".into()))?;
    ensure_owned_artifact_path(root, &evidence.staging_dir)?;
    verify_artifact_tree(&evidence.staging_dir, &evidence.destination_dir, evidence).await
}

async fn verify_artifact_publication(
    artifacts_root: &Path,
    evidence: &ArtifactRetentionEvidence,
) -> Result<(), ServiceError> {
    ensure_owned_artifact_path(artifacts_root, &evidence.destination_dir)?;
    verify_artifact_tree(
        &evidence.destination_dir,
        &evidence.destination_dir,
        evidence,
    )
    .await
}

async fn verify_artifact_tree(
    actual_root: &Path,
    recorded_root: &Path,
    evidence: &ArtifactRetentionEvidence,
) -> Result<(), ServiceError> {
    let manifest_path = actual_root.join(".tollgate-artifact-manifest.json");
    let manifest: ArtifactRetentionEvidence =
        serde_json::from_slice(&tokio::fs::read(&manifest_path).await?)?;
    if manifest != *evidence {
        return Err(ServiceError::Invariant(
            "artifact publication manifest differs from its durable intent".into(),
        ));
    }
    let expected = evidence
        .records
        .iter()
        .map(|record| {
            Path::new(&record.retained_path)
                .strip_prefix(recorded_root)
                .map(Path::to_owned)
                .map_err(|_| {
                    ServiceError::Invariant(
                        "artifact manifest contains an escaping destination".into(),
                    )
                })
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let mut observed = HashSet::new();
    let mut directories = vec![actual_root.to_owned()];
    while let Some(directory) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let kind = entry.file_type().await?;
            if kind.is_symlink() {
                return Err(ServiceError::Invariant(
                    "artifact publication contains a symlink".into(),
                ));
            }
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file() && entry.path() != manifest_path {
                observed.insert(
                    entry
                        .path()
                        .strip_prefix(actual_root)
                        .map_err(|_| ServiceError::Invariant("artifact escaped root".into()))?
                        .to_owned(),
                );
            }
        }
    }
    if observed != expected {
        return Err(ServiceError::Invariant(
            "artifact publication does not exactly match its manifest".into(),
        ));
    }
    for record in &evidence.records {
        let relative = Path::new(&record.retained_path)
            .strip_prefix(recorded_root)
            .map_err(|_| ServiceError::Invariant("artifact escaped manifest root".into()))?;
        let actual = actual_root.join(relative);
        let metadata = tokio::fs::symlink_metadata(&actual).await?;
        if !metadata.file_type().is_file()
            || metadata.len() != record.size
            || hash_file(&actual).await? != record.hash
        {
            return Err(ServiceError::Invariant(format!(
                "artifact {} failed publication verification",
                relative.display()
            )));
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), ServiceError> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_tree_directories(root: &Path) -> Result<(), ServiceError> {
    let mut directories = vec![root.to_owned()];
    let mut ordered = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(ServiceError::Invariant(
                    "artifact staging contains a symlink".into(),
                ));
            }
            if kind.is_dir() {
                directories.push(entry.path());
            }
        }
        ordered.push(directory);
    }
    for directory in ordered.into_iter().rev() {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn tree_logical_size(root: &Path) -> Result<u64, ServiceError> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let root_metadata = std::fs::symlink_metadata(root)?;
    #[cfg(unix)]
    let root_device = root_metadata.dev();
    let mut total = 0u64;
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            #[cfg(unix)]
            if metadata.dev() != root_device {
                return Err(ServiceError::Invariant(
                    "cache snapshot cannot cross a mounted filesystem boundary".into(),
                ));
            }
            let kind = metadata.file_type();
            if kind.is_symlink() {
                let target = std::fs::read_link(entry.path())?;
                if target.is_absolute()
                    || lexical_relative_escapes(
                        entry
                            .path()
                            .parent()
                            .and_then(|parent| parent.strip_prefix(root).ok())
                            .unwrap_or(Path::new("")),
                        &target,
                    )
                {
                    return Err(ServiceError::Invariant(
                        "cache snapshot contains an escaping symbolic link".into(),
                    ));
                }
            } else if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file() {
                total = total.saturating_add(metadata.len());
            } else {
                return Err(ServiceError::Invariant(
                    "cache snapshot contains an unsupported filesystem entry".into(),
                ));
            }
        }
    }
    Ok(total)
}

fn lexical_relative_escapes(parent: &Path, target: &Path) -> bool {
    use std::path::Component;
    let mut depth = parent.components().count() as isize;
    for component in target.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            Component::RootDir | Component::Prefix(_) => return true,
        }
    }
    false
}

fn observe_volumes(runtime: &RepositoryRuntime) -> Result<Vec<VolumeView>, ServiceError> {
    let resources = runtime.data.lock().config.resources.clone();
    let paths = [
        (&runtime.git.common_dir, "authoritative"),
        (&runtime.git.common_dir, "database"),
        (&runtime.logs_root, "logs"),
        (&runtime.git.common_dir, "artifacts"),
        (&runtime.slots_root, "execution-cache"),
    ];
    let mut volumes = BTreeMap::<String, VolumeView>::new();
    for (path, role) in paths {
        let state = nix::sys::statvfs::statvfs(path)
            .map_err(|error| ServiceError::Invariant(error.to_string()))?;
        let id = format!("fs-{:x}", state.filesystem_id());
        let available_bytes =
            u64::from(state.blocks_available()).saturating_mul(state.fragment_size());
        let volume = volumes.entry(id.clone()).or_insert_with(|| VolumeView {
            id,
            roles: Vec::new(),
            available_bytes,
            warning_threshold: resources.volume_warning_bytes,
            critical_threshold: resources.volume_critical_bytes,
            emergency_allowance: resources.volume_emergency_bytes,
            state: "healthy".into(),
        });
        volume.available_bytes = volume.available_bytes.min(available_bytes);
        if !volume.roles.iter().any(|candidate| candidate == role) {
            volume.roles.push(role.into());
        }
    }
    for volume in volumes.values_mut() {
        volume.roles.sort();
        volume.state = if volume.available_bytes < volume.critical_threshold {
            "critical"
        } else if volume.available_bytes < volume.warning_threshold {
            "warning"
        } else {
            "healthy"
        }
        .into();
        runtime.store.upsert_volume_state(
            &volume.id,
            &volume.roles,
            volume.warning_threshold,
            volume.critical_threshold,
            volume.emergency_allowance,
            volume.available_bytes,
        )?;
    }
    Ok(volumes.into_values().collect())
}

struct IdleSleepAssertion(Option<tokio::process::Child>);

impl IdleSleepAssertion {
    async fn acquire() -> Self {
        #[cfg(target_os = "macos")]
        {
            let child = tokio::process::Command::new("/usr/bin/caffeinate")
                .arg("-i")
                .arg("-w")
                .arg(std::process::id().to_string())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .ok();
            Self(child)
        }
        #[cfg(not(target_os = "macos"))]
        Self(None)
    }

    async fn release(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        self.0 = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use tollgate_git::USER_BRANCH;

    fn git_output(directory: &Path, args: &[&str]) -> std::process::Output {
        StdCommand::new("git")
            .current_dir(directory)
            .args(args)
            .env("GIT_AUTHOR_NAME", "Tollgate Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Tollgate Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .output()
            .unwrap()
    }

    fn git(directory: &Path, args: &[&str]) -> String {
        let output = git_output(directory, args);
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().into()
    }

    #[tokio::test]
    async fn no_bootstrap_preserves_the_requested_voting_policy() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        std::fs::write(repository.join(".gitignore"), "target/\n").unwrap();
        git(&repository, &["add", "base.txt", ".gitignore"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("false".into()), false)
            .await
            .unwrap();
        assert_eq!(initialized.state.integration_ref, INTEGRATION_REF);
        assert_eq!(
            git(&repository, &["branch", "--show-current"]),
            USER_BRANCH_REF.trim_start_matches("refs/heads/")
        );
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            git(&repository, &["rev-parse", USER_BRANCH_REF])
        );
        assert!(initialized.checks.is_empty());
        assert_eq!(initialized.configuration.steps.len(), 1);
        assert!(initialized.configuration.steps[0].voting);
        assert!(matches!(
            &initialized.configuration.steps[0].command,
            tollgate_config::EffectiveCommand::Shell { script } if script == "false"
        ));
    }

    #[tokio::test]
    async fn initialization_prefers_authoritative_policy_and_keeps_legacy_readers_compatible() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        std::fs::create_dir(repository.join(".tollgate")).unwrap();
        std::fs::create_dir_all(repository.join(".git/tollgate")).unwrap();
        let authoritative = "version = 1\n\n[[step]]\nname = \"authoritative\"\nrun = \"true\"\n";
        std::fs::write(repository.join(".tollgate/config.toml"), authoritative).unwrap();
        std::fs::write(
            repository.join(".git/tollgate/config.toml"),
            "version = 1\n\n[[step]]\nname = \"stale\"\nrun = \"false\"\n",
        )
        .unwrap();

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, None, false)
            .await
            .unwrap();
        assert_eq!(initialized.configuration.steps[0].name, "authoritative");
        assert_eq!(
            std::fs::read_to_string(repository.join(".git/tollgate/config.toml")).unwrap(),
            authoritative
        );

        let replacement = "version = 1\n\n[[step]]\nname = \"replacement\"\nrun = \"true\"\n";
        std::fs::write(repository.join(".tollgate/config.toml"), replacement).unwrap();
        service
            .apply_configuration(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(repository.join(".git/tollgate/config.toml")).unwrap(),
            replacement
        );
    }

    #[tokio::test]
    async fn initialization_migrates_a_legacy_policy_instead_of_auto_detecting_a_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        std::fs::create_dir_all(repository.join(".git/tollgate")).unwrap();
        let legacy = "version = 1\n\n[[step]]\nname = \"legacy\"\nrun = \"false\"\n";
        std::fs::write(repository.join(".git/tollgate/config.toml"), legacy).unwrap();

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        assert_eq!(initialized.configuration.steps[0].name, "legacy");
        assert_eq!(
            std::fs::read_to_string(repository.join(".tollgate/config.toml")).unwrap(),
            legacy
        );
    }

    #[tokio::test]
    async fn existing_master_integration_state_migrates_to_release_without_switching_checkout() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let support = temporary.path().join("support");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let mut legacy_state = runtime.data.lock().state.clone();
        legacy_state.integration_ref = USER_BRANCH_REF.into();
        runtime
            .store
            .update_repository_state(&legacy_state)
            .unwrap();
        git(&repository, &["update-ref", "-d", INTEGRATION_REF]);
        drop(runtime);
        drop(service);

        let migrated = TollgateService::open(support).await.unwrap();
        let snapshot = migrated
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(snapshot.state.integration_ref, INTEGRATION_REF);
        assert_eq!(git(&repository, &["branch", "--show-current"]), "master");
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            git(&repository, &["rev-parse", USER_BRANCH_REF])
        );
    }

    #[tokio::test]
    async fn doctor_accepts_a_slot_owned_by_the_execution_mirror() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        runtime
            .git
            .initialize_mirror(&runtime.mirror)
            .await
            .unwrap();
        let slot_id = SlotId::new();
        let slot_path = runtime.slots_root.join(slot_id.to_string());
        runtime
            .git
            .provision_slot(&runtime.mirror, &slot_path, &initialized.state.master_oid)
            .await
            .unwrap();
        let slot_repository = GitRepository::discover(&slot_path).await.unwrap();
        assert!(paths_identical(&slot_repository.common_dir, &runtime.mirror).await);
        assert!(!paths_identical(&slot_repository.common_dir, &runtime.git.common_dir).await);
        runtime.data.lock().slots.insert(
            slot_id,
            SlotView {
                id: slot_id,
                path: slot_path,
                state: "idle".into(),
                checkout_oid: Some(initialized.state.master_oid),
                health: "healthy".into(),
                last_used: None,
            },
        );

        let report = service.doctor(initialized.state.id).await.unwrap();
        let slots = report
            .checks
            .iter()
            .find(|check| check.name == "Persistent slots")
            .unwrap();
        assert!(matches!(slots.status, DiagnosticStatus::Healthy));
        assert_eq!(slots.detail, "1 registered slot(s) passed ownership checks");
    }

    #[tokio::test]
    async fn doctor_reports_repository_blocks_and_their_recovery_actions() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let blocked = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "worktree-remove-ambiguous".into(),
                message: "Prepared path was replaced".into(),
                recovery_action: "Move the replacement aside and restart Tollgate.".into(),
            });
            data.state.clone()
        };
        runtime.store.update_repository_state(&blocked).unwrap();

        let report = service.doctor(initialized.state.id).await.unwrap();
        assert!(!report.healthy);
        let execution = report
            .checks
            .iter()
            .find(|check| check.name == "Repository execution")
            .unwrap();
        assert!(matches!(execution.status, DiagnosticStatus::Attention));
        assert!(execution.detail.contains("worktree-remove-ambiguous"));
        assert_eq!(
            execution.recovery_action.as_deref(),
            Some("Move the replacement aside and restart Tollgate.")
        );
    }

    #[tokio::test]
    async fn worktree_remove_rejects_orphans_and_primary_checkout_before_preparing_intents() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let orphan = repository.join(".worktrees/orphan/ui/.vite/deps");
        std::fs::create_dir_all(&orphan).unwrap();
        std::fs::write(orphan.join("_metadata.json"), "{}").unwrap();
        let orphan_root = repository.join(".worktrees/orphan");
        let orphan_command = CommandId::new();
        let error = service
            .remove_worktree(
                initialized.state.id,
                orphan_root.to_string_lossy().into_owned(),
                orphan_command,
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not an exact registered linked worktree")
        );
        let primary_command = CommandId::new();
        let error = service
            .remove_worktree(
                initialized.state.id,
                repository.to_string_lossy().into_owned(),
                primary_command,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("primary, master, or release"));
        let store = &service.runtime(initialized.state.id).await.unwrap().store;
        assert!(
            store
                .operation_evidence(orphan_command, "worktree-remove")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .operation_evidence(primary_command, "worktree-remove")
                .unwrap()
                .is_none()
        );
        assert!(orphan.join("_metadata.json").exists());
    }

    #[tokio::test]
    async fn removal_observation_distinguishes_intact_removed_residual_and_replaced_paths() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
            ],
        );
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let source = runtime.git.resolve_oid("refs/heads/feature").await.unwrap();
        let exact_evidence = serde_json::json!({
            "common_dir": runtime.git.common_dir,
            "path_identity": GitRepository::directory_identity(&feature).unwrap(),
        });
        assert_eq!(
            service
                .observe_worktree_removal(&runtime, &exact_evidence, &feature, "feature", &source,)
                .await
                .unwrap(),
            WorktreeRemovalObservation::Intact
        );

        git(
            &repository,
            &["worktree", "remove", "--force", feature.to_str().unwrap()],
        );
        git(&repository, &["branch", "-D", "feature"]);
        assert_eq!(
            service
                .observe_worktree_removal(&runtime, &exact_evidence, &feature, "feature", &source,)
                .await
                .unwrap(),
            WorktreeRemovalObservation::Removed
        );

        std::fs::create_dir_all(feature.join("ui/.vite/deps")).unwrap();
        std::fs::write(feature.join("ui/.vite/deps/package.json"), "{}").unwrap();
        assert_eq!(
            service
                .observe_worktree_removal(
                    &runtime,
                    &serde_json::json!({}),
                    &feature,
                    "feature",
                    &source,
                )
                .await
                .unwrap(),
            WorktreeRemovalObservation::Residual
        );
        let replacement = service
            .observe_worktree_removal(&runtime, &exact_evidence, &feature, "feature", &source)
            .await
            .unwrap_err();
        assert!(replacement.contains("replaced after preparation"));
        assert!(feature.join("ui/.vite/deps/package.json").exists());
    }

    #[tokio::test]
    async fn restart_resolves_legacy_cleanup_residual_and_malformed_master_intent_idempotently() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = repository.join(".worktrees/feature");
        let support = temporary.path().join("support");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
            ],
        );
        std::fs::write(feature.join("feature.txt"), "feature\n").unwrap();
        git(&feature, &["add", "feature.txt"]);
        git(&feature, &["commit", "-m", "feature"]);

        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        service.shutting_down.store(true, Ordering::Release);
        let candidate = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let mut item = runtime
            .data
            .lock()
            .items
            .iter()
            .find(|item| item.id == candidate.item_id)
            .cloned()
            .unwrap();
        item.state = QueueItemState::Promoted;
        item.cleanup_state = CleanupState::NeedsAttention;
        service.replace_item(&runtime, item.clone()).unwrap();

        let cleanup_command = CommandId::new();
        let cleanup_digest = "legacy-cleanup-digest";
        runtime
            .store
            .prepare_operation(
                initialized.state.id,
                "cleanup",
                cleanup_command,
                &serde_json::json!({
                    "request_digest": cleanup_digest,
                    "item_id": item.id,
                    "path": feature,
                    "branch": "feature",
                    "expected_oid": item.source_oid,
                }),
            )
            .unwrap();
        runtime
            .store
            .set_intent_state(
                cleanup_command,
                IntentState::NeedsAttention,
                &serde_json::json!({"error": "Directory not empty"}),
            )
            .unwrap();
        git(
            &repository,
            &["worktree", "remove", "--force", feature.to_str().unwrap()],
        );
        git(&repository, &["branch", "-D", "feature"]);
        let residual = feature.join("ui/.vite/deps");
        std::fs::create_dir_all(&residual).unwrap();
        std::fs::write(residual.join("_metadata.json"), "{}").unwrap();
        std::fs::write(residual.join("package.json"), "{}").unwrap();

        let malformed_command = CommandId::new();
        runtime
            .store
            .prepare_operation(
                initialized.state.id,
                "worktree-remove",
                malformed_command,
                &serde_json::json!({
                    "request_digest": "malformed-master-digest",
                    "path": feature,
                    "branch": USER_BRANCH,
                    "expected_oid": item.source_oid,
                }),
            )
            .unwrap();
        runtime
            .store
            .set_intent_state(
                malformed_command,
                IntentState::NeedsAttention,
                &serde_json::json!({"recovery": "worktree-remove-ambiguous"}),
            )
            .unwrap();
        let blocked = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "worktree-remove-ambiguous".into(),
                message: "Worktree still exists but no longer matches evidence".into(),
                recovery_action: "legacy unavailable guidance".into(),
            });
            data.state.clone()
        };
        runtime.store.update_repository_state(&blocked).unwrap();
        std::fs::write(repository.join("later.txt"), "later\n").unwrap();
        git(&repository, &["add", "later.txt"]);
        git(&repository, &["commit", "-m", "master advanced"]);
        drop(runtime);
        drop(service);

        let reopened = TollgateService::open(support.clone()).await.unwrap();
        let snapshot = reopened
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(
            snapshot.state.execution_state,
            RepositoryExecutionState::Active
        );
        assert!(snapshot.state.block_reasons.is_empty());
        let recovered_item = snapshot
            .history_items
            .iter()
            .find(|view| view.item.id == item.id)
            .unwrap();
        assert_eq!(recovered_item.item.cleanup_state, CleanupState::Completed);
        assert!(residual.join("_metadata.json").exists());
        let recovered_runtime = reopened.runtime(initialized.state.id).await.unwrap();
        assert!(
            recovered_runtime
                .store
                .checked_command_response::<MutationResult>(
                    cleanup_command,
                    "cleanup",
                    cleanup_digest,
                )
                .unwrap()
                .is_some()
        );
        assert!(
            recovered_runtime
                .store
                .recoverable_operations(&["cleanup", "worktree-remove"])
                .unwrap()
                .is_empty()
        );
        drop(recovered_runtime);
        drop(reopened);

        let reopened_again = TollgateService::open(support).await.unwrap();
        let second_snapshot = reopened_again
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(
            second_snapshot.state.execution_state,
            RepositoryExecutionState::Active
        );
        assert_eq!(
            second_snapshot
                .history_items
                .iter()
                .find(|view| view.item.id == item.id)
                .unwrap()
                .item
                .cleanup_state,
            CleanupState::Completed
        );
        assert!(residual.join("package.json").exists());
    }

    #[tokio::test]
    async fn restart_blocks_when_a_prepared_worktree_path_was_replaced() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        let displaced = temporary.path().join("displaced-feature");
        let support = temporary.path().join("support");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
            ],
        );
        let source = GitOid::from_hex(&git(&feature, &["rev-parse", "HEAD"])).unwrap();

        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let command_id = CommandId::new();
        runtime
            .store
            .prepare_operation(
                initialized.state.id,
                "worktree-remove",
                command_id,
                &serde_json::json!({
                    "request_digest": "replacement-digest",
                    "path": std::fs::canonicalize(&feature).unwrap(),
                    "branch": "feature",
                    "expected_oid": source,
                    "common_dir": runtime.git.common_dir,
                    "path_identity": GitRepository::directory_identity(&feature).unwrap(),
                }),
            )
            .unwrap();
        runtime
            .store
            .set_intent_state(
                command_id,
                IntentState::NeedsAttention,
                &serde_json::json!({"recovery": "simulated-crash"}),
            )
            .unwrap();
        std::fs::rename(&feature, &displaced).unwrap();
        std::fs::create_dir(&feature).unwrap();
        std::fs::write(feature.join("replacement.txt"), "preserve me\n").unwrap();
        drop(runtime);
        drop(service);

        let reopened = TollgateService::open(support).await.unwrap();
        let snapshot = reopened
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(
            snapshot.state.execution_state,
            RepositoryExecutionState::Blocked
        );
        assert!(
            snapshot
                .state
                .block_reasons
                .iter()
                .any(|reason| reason.code == "worktree-remove-ambiguous"
                    && reason.message.contains("identity changed"))
        );
        let report = reopened.doctor(initialized.state.id).await.unwrap();
        assert!(!report.healthy);
        assert!(feature.join("replacement.txt").exists());
        assert!(displaced.join(".git").exists());
        assert_eq!(
            git(&repository, &["rev-parse", "refs/heads/feature"]),
            source.to_hex()
        );
    }

    #[tokio::test]
    async fn retained_candidate_promotes_without_cleaning_source_worktree() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("feature.txt"), "feature\n").unwrap();
        git(&feature, &["add", "feature.txt"]);
        git(&feature, &["commit", "-m", "feature"]);
        let source = git(&feature, &["rev-parse", "HEAD"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(
                &repository,
                Some("test -f feature.txt".into()),
                false,
            )
            .await
            .unwrap();
        let candidate = service
            .submit_candidate_from_with_cleanup_policy(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CleanupPolicy::RetainWorktree,
                CommandId::new(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let revision = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .queue
                .iter()
                .find(|view| view.item.id == candidate.item_id)
                .unwrap();
            assert_eq!(view.item.cleanup_policy, CleanupPolicy::RetainWorktree);
            if view.item.state == QueueItemState::Ready {
                break snapshot.state.queue_revision;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };
        service
            .authorize_candidate(
                initialized.state.id,
                candidate.item_id,
                revision,
                CommandId::new(),
            )
            .await
            .unwrap();

        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if let Some(view) = snapshot
                .history_items
                .iter()
                .find(|view| view.item.id == candidate.item_id)
            {
                assert_eq!(view.item.state, QueueItemState::Promoted);
                assert_eq!(view.item.cleanup_policy, CleanupPolicy::RetainWorktree);
                assert_eq!(view.item.cleanup_state, CleanupState::NotEligible);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(feature.exists());
        assert_eq!(git(&feature, &["rev-parse", "HEAD"]), source);
        assert_eq!(
            git(&repository, &["rev-parse", "refs/heads/feature"]),
            source
        );
    }

    #[tokio::test]
    async fn retry_preserves_worktree_provenance_through_promotion_and_cleanup() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        let pass_after_retry = temporary.path().join("pass-after-retry");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("feature.txt"), "feature\n").unwrap();
        git(&feature, &["add", "feature.txt"]);
        git(&feature, &["commit", "-m", "feature"]);
        let canonical_feature = std::fs::canonicalize(&feature).unwrap();
        let command = format!(
            "if test -f '{}'; then true; else touch '{}'; false; fi",
            pass_after_retry.display(),
            pass_after_retry.display()
        );
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some(command), false)
            .await
            .unwrap();
        let first = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let state = service
                .item_status(initialized.state.id, first.item_id)
                .await
                .unwrap()
                .state;
            if state == QueueItemState::Failed {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let command_id = CommandId::new();
        let retried = service
            .retry(initialized.state.id, first.item_id, false, command_id)
            .await
            .unwrap();
        let replay = service
            .retry(initialized.state.id, first.item_id, false, command_id)
            .await
            .unwrap();
        assert_eq!(retried.item_id, replay.item_id);
        assert!(matches!(
            service
                .retry(initialized.state.id, first.item_id, true, command_id)
                .await,
            Err(ServiceError::Store(StoreError::CommandReplayMismatch))
        ));

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let revision = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .queue
                .iter()
                .find(|view| view.item.id == retried.item_id)
                .unwrap();
            assert_eq!(view.item.metadata.branch.as_deref(), Some("feature"));
            assert_eq!(
                view.item.metadata.worktree_path.as_deref(),
                Some(canonical_feature.to_string_lossy().as_ref())
            );
            if view.item.state == QueueItemState::Ready {
                break snapshot.state.queue_revision;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };
        service
            .authorize_candidate(
                initialized.state.id,
                retried.item_id,
                revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if let Some(view) = snapshot
                .history_items
                .iter()
                .find(|view| view.item.id == retried.item_id)
            {
                assert_eq!(view.item.state, QueueItemState::Promoted);
                assert_eq!(view.item.cleanup_state, CleanupState::Completed);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(!feature.exists());
        assert!(
            !StdCommand::new("git")
                .current_dir(&repository)
                .args(["show-ref", "--verify", "--quiet", "refs/heads/feature"])
                .status()
                .unwrap()
                .success()
        );
    }

    #[tokio::test]
    async fn retry_rejects_a_worktree_that_moved_from_its_recorded_source() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("feature.txt"), "feature\n").unwrap();
        git(&feature, &["add", "feature.txt"]);
        git(&feature, &["commit", "-m", "feature"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("false".into()), false)
            .await
            .unwrap();
        let candidate = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, candidate.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::Failed {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        git(&feature, &["switch", "--detach", "master"]);

        let error = service
            .retry(
                initialized.state.id,
                candidate.item_id,
                false,
                CommandId::new(),
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no longer matches its recorded branch and OID")
        );
    }

    #[tokio::test]
    async fn repository_removal_replays_after_runtime_and_restart_are_gone() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let support = temporary.path().join("support");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let command_id = CommandId::new();
        let first = service
            .unregister_repository_command(initialized.state.id, command_id)
            .await
            .unwrap();
        let replay = service
            .unregister_repository_command(initialized.state.id, command_id)
            .await
            .unwrap();
        assert_eq!(first.action, replay.action);
        assert!(service.snapshot().await.unwrap().repositories.is_empty());
        drop(service);

        let reopened = TollgateService::open(support).await.unwrap();
        assert!(reopened.snapshot().await.unwrap().repositories.is_empty());
        let replay_after_restart = reopened
            .unregister_repository_command(initialized.state.id, command_id)
            .await
            .unwrap();
        assert_eq!(replay_after_restart.action, "remove-repository");
        let mismatch = reopened
            .unregister_repository_command(RepositoryId::new(), command_id)
            .await
            .unwrap_err();
        assert!(matches!(
            mismatch,
            ServiceError::Store(StoreError::CommandReplayMismatch)
        ));
    }

    #[tokio::test]
    async fn restart_recovers_an_exact_regenerated_configuration() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let support = temporary.path().join("support");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("true".into()), false)
            .await
            .unwrap();
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let path = runtime.git.worktree_root.join(".tollgate/config.toml");
        let old_bytes = std::fs::read(&path).unwrap();
        let contents = "version = 1\n\n[[step]]\nname = \"ci\"\nrun = \"false\"\n";
        let candidate = EffectiveConfig::parse(contents).unwrap();
        let command_id = CommandId::new();
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": initialized.state.id,
            "template": "auto-detect-v1",
        }))
        .unwrap();
        runtime
            .store
            .prepare_operation(
                initialized.state.id,
                "config-regenerate",
                command_id,
                &serde_json::json!({
                    "request_digest": request_digest,
                    "path": path,
                    "old_hash": blake3::hash(&old_bytes).to_hex().to_string(),
                    "new_digest": candidate.digest,
                }),
            )
            .unwrap();
        std::fs::write(&path, contents).unwrap();
        std::fs::File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        drop(runtime);
        drop(service);

        let reopened = TollgateService::open(support).await.unwrap();
        let recovered = reopened
            .runtime(initialized.state.id)
            .await
            .unwrap()
            .store
            .checked_command_response::<EffectiveConfig>(
                command_id,
                "config-regenerate",
                &request_digest,
            )
            .unwrap()
            .expect("recovery should publish the original command response");
        assert_eq!(recovered.digest, candidate.digest);
        let snapshot = reopened
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(
            snapshot.state.execution_state,
            RepositoryExecutionState::ConfigurationPending
        );
        assert_ne!(snapshot.configuration.digest, recovered.digest);
    }

    #[tokio::test]
    async fn failing_bootstrap_is_recorded_without_blocking_the_gate() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("false".into()), true)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if let Some(check) = snapshot.checks.first()
                && check.item.state == QueueItemState::CheckFailed
            {
                assert_eq!(
                    check.item.terminal_reason.as_deref(),
                    Some("baseline-failing")
                );
                assert_eq!(
                    snapshot.state.execution_state,
                    RepositoryExecutionState::Active
                );
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn rebuilt_master_candidate_is_projected_onto_the_new_certified_base() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let adopted_worktree = temporary.path().join("adopted");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 30".into()))
            .await
            .unwrap();
        let old_release = initialized.state.master_oid;

        std::fs::write(repository.join("master.txt"), "master\n").unwrap();
        git(&repository, &["add", "master.txt"]);
        git(&repository, &["commit", "-m", "master work"]);
        let original_master = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();
        service
            .submit_candidate(initialized.state.id, "HEAD".into(), CommandId::new())
            .await
            .unwrap();

        git(
            &repository,
            &[
                "worktree",
                "add",
                "--detach",
                adopted_worktree.to_str().unwrap(),
                &old_release.to_hex(),
            ],
        );
        std::fs::write(adopted_worktree.join("certified.txt"), "certified\n").unwrap();
        git(&adopted_worktree, &["add", "certified.txt"]);
        git(&adopted_worktree, &["commit", "-m", "certified elsewhere"]);
        let new_release =
            GitOid::from_hex(&git(&adopted_worktree, &["rev-parse", "HEAD"])).unwrap();

        let runtime = service.runtime(initialized.state.id).await.unwrap();
        runtime
            .git
            .compare_and_swap_integration(&old_release, &new_release)
            .await
            .unwrap();
        let mut state = runtime.data.lock().state.clone();
        state.master_oid = new_release.clone();
        state.queue_revision += 1;
        runtime.store.update_repository_state(&state).unwrap();
        runtime.data.lock().state = state;

        service
            .rebuild_after_base_adoption(&runtime, &new_release)
            .await
            .unwrap();

        let projected_master =
            GitOid::from_hex(&git(&repository, &["rev-parse", USER_BRANCH_REF])).unwrap();
        assert_ne!(projected_master, original_master);
        assert_eq!(
            runtime
                .git
                .commit_parent_oid(&projected_master)
                .await
                .unwrap(),
            new_release
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("certified.txt")).unwrap(),
            "certified\n"
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("master.txt")).unwrap(),
            "master\n"
        );
    }

    #[tokio::test]
    async fn later_master_commits_are_not_selected_for_projection() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 30".into()))
            .await
            .unwrap();
        std::fs::write(repository.join("submitted.txt"), "submitted\n").unwrap();
        git(&repository, &["add", "submitted.txt"]);
        git(&repository, &["commit", "-m", "submitted"]);
        service
            .submit_candidate(initialized.state.id, "HEAD".into(), CommandId::new())
            .await
            .unwrap();

        std::fs::write(repository.join("later.txt"), "later\n").unwrap();
        git(&repository, &["add", "later.txt"]);
        git(&repository, &["commit", "-m", "later"]);
        let later = git(&repository, &["rev-parse", "HEAD"]);
        let runtime = service.runtime(initialized.state.id).await.unwrap();

        assert!(
            service
                .active_user_master_projection(&runtime)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(git(&repository, &["rev-parse", USER_BRANCH_REF]), later);
    }

    #[tokio::test]
    async fn successful_large_stderr_candidate_names_checkout_failure_and_diagnoses_dirty_checkout()
    {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("tracked"), "base\n").unwrap();
        git(&repository, &["add", "tracked"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let command = r#"awk 'BEGIN { for (i = 0; i < 409600; i++) printf "x" }' >&2; printf 'restored asset\n' > tracked"#;
        let initialized = service
            .initialize_repository_with_options(&repository, Some(command.into()), false)
            .await
            .unwrap();
        std::fs::write(repository.join("feature"), "feature\n").unwrap();
        git(&repository, &["add", "feature"]);
        git(&repository, &["commit", "-m", "feature"]);
        let candidate = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, candidate.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::Failed {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "state={:?}",
                item.state
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let details = service
            .item_details(initialized.state.id, candidate.item_id)
            .await
            .unwrap();
        assert_eq!(
            details.item.terminal_reason.as_deref(),
            Some(
                "checkout-verification-failed: workspace verification failed: tracked worktree differs from index"
            )
        );
        assert!(details.certificate.is_none());
        assert!(details.failure_attribution.is_none());
        assert!(
            details
                .buildset
                .as_ref()
                .unwrap()
                .step_results
                .iter()
                .all(|step| step.result_class == "success" && step.exit_code == Some(0))
        );
        assert!(details.buildset.as_ref().unwrap().step_results[0].stderr_end >= 400_000);

        std::fs::write(repository.join("tracked"), "unrelated formatting edit\n").unwrap();
        let diagnosis = service
            .diagnose_failure(
                initialized.state.id,
                candidate.item_id,
                true,
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(diagnosis.replay_item_ids.len(), 2);
        assert_eq!(diagnosis.scheduled_replay_item_ids.len(), 2);
        for replay_id in diagnosis.replay_item_ids {
            loop {
                let replay = service
                    .item_status(initialized.state.id, replay_id)
                    .await
                    .unwrap();
                if replay.state.is_terminal() {
                    assert_eq!(
                        replay.terminal_reason.as_deref(),
                        Some(
                            "checkout-verification-failed: workspace verification failed: tracked worktree differs from index"
                        )
                    );
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        assert_eq!(
            std::fs::read_to_string(repository.join("tracked")).unwrap(),
            "unrelated formatting edit\n"
        );
    }

    #[tokio::test]
    async fn latest_failed_master_push_remains_in_repository_snapshot() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("false".into()))
            .await
            .unwrap();
        std::fs::write(repository.join("master.txt"), "master\n").unwrap();
        git(&repository, &["add", "master.txt"]);
        git(&repository, &["commit", "-m", "master push"]);
        let approved = service
            .approve_from_with_purpose(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                Some("push-master".into()),
                CommandId::new(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let Some(master_push) = snapshot.master_push else {
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                continue;
            };
            if master_push.item.state == QueueItemState::Failed {
                assert_eq!(master_push.item.id, approved.item_id);
                assert_eq!(
                    master_push.item.metadata.purpose.as_deref(),
                    Some("push-master")
                );
                assert_eq!(
                    master_push.item.terminal_reason.as_deref(),
                    Some("voting-validation-failed")
                );
                assert!(
                    master_push
                        .failure_attribution
                        .as_ref()
                        .is_some_and(|attribution| attribution.steps[0].name == "ci")
                );
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn approves_validates_certifies_and_promotes_an_exact_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature_worktree = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        std::fs::write(repository.join(".gitignore"), "target/\n").unwrap();
        git(&repository, &["add", "base.txt", ".gitignore"]);
        git(&repository, &["commit", "-m", "base"]);
        let old_master = git(&repository, &["rev-parse", "master"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature_worktree.join("feature.txt"), "feature\n").unwrap();
        git(&feature_worktree, &["add", "feature.txt"]);
        git(&feature_worktree, &["commit", "-m", "feature"]);
        let source = git(&feature_worktree, &["rev-parse", "HEAD"]);
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(
                &repository,
                Some(
                    "mkdir -p target; printf cache > target/cache; printf retained > result.txt; test -f feature.txt"
                        .into(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            initialized.state.execution_state,
            RepositoryExecutionState::Active
        );
        std::fs::write(
            repository.join(".tollgate/config.toml"),
            r#"version = 1

[[step]]
name = "ci"
run = "mkdir -p target; printf cache > target/cache; printf retained > result.txt; test -f feature.txt"

[[step.artifact]]
name = "result"
patterns = ["result.txt"]
required = true

[[cache.paths]]
path = "target"
policy = "clone"
"#,
        )
        .unwrap();
        service
            .apply_configuration(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        let result = service
            .approve(initialized.state.id, "feature".into(), CommandId::new())
            .await
            .unwrap();
        assert_eq!(result.source_oid.to_hex(), source);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "queue did not promote: {:?}",
                snapshot
                    .queue
                    .iter()
                    .map(|view| view.item.state)
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let promoted = git(&repository, &["rev-parse", INTEGRATION_REF]);
        let parent = git(&repository, &["show", "-s", "--format=%P", INTEGRATION_REF]);
        assert_eq!(promoted, result.tested_oid.to_hex());
        assert_eq!(parent, old_master);
        assert_eq!(git(&repository, &["rev-parse", USER_BRANCH_REF]), promoted);
        assert!(
            service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap()
                .history
                .iter()
                .any(|event| event.kind == "user-master.synchronized")
        );
        assert_eq!(
            promoted, source,
            "a direct-parent source with the same tree is reused byte-for-byte"
        );
        let artifact_root = repository.join(".git/tollgate/artifacts");
        let buildset_directory = std::fs::read_dir(&artifact_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(
            std::fs::read_to_string(buildset_directory.join("ci/result/result.txt")).unwrap(),
            "retained"
        );
        let retained = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap()
            .artifacts;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].source_path, "result.txt");
        assert_eq!(retained[0].size, 8);
        assert_eq!(
            retained[0].hash,
            blake3::hash(b"retained").to_hex().to_string()
        );
        let artifact_id = retained[0].artifact_id.clone();
        service
            .set_artifact_pinned(
                initialized.state.id,
                artifact_id.clone(),
                true,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap()
                .artifacts[0]
                .retention_state,
            "pinned"
        );
        service
            .set_artifact_pinned(
                initialized.state.id,
                artifact_id.clone(),
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        service
            .prune_artifact(initialized.state.id, artifact_id, CommandId::new())
            .await
            .unwrap();
        assert!(
            service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap()
                .artifacts
                .is_empty()
        );
        let seed = service
            .snapshot_cache(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        assert_eq!(seed.seed_ids.len(), 1);
        assert_eq!(seed.logical_bytes, 5);
        let cold_worktree = temporary.path().join("cold-worktree");
        let cold = service
            .create_worktree(
                initialized.state.id,
                "cold-worktree".into(),
                Some(cold_worktree.to_string_lossy().into_owned()),
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(!cold_worktree.join("target").exists());
        assert!(!cold.message.contains("Hydrated"));
        let warm_worktree = temporary.path().join("warm-worktree");
        let warm = service
            .create_worktree(
                initialized.state.id,
                "warm-worktree".into(),
                Some(warm_worktree.to_string_lossy().into_owned()),
                true,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(
            warm.message
                .contains("Hydrated 5 logical bytes from APFS seed"),
            "{}",
            warm.message
        );
        assert_eq!(
            std::fs::read_to_string(warm_worktree.join("target/cache")).unwrap(),
            "cache"
        );
        GitRepository::discover(&warm_worktree)
            .await
            .unwrap()
            .ensure_clean()
            .await
            .unwrap();
        drop(service);
        let reopened = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let recovered = reopened
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(recovered.state.master_oid.to_hex(), promoted);
        assert!(recovered.queue.is_empty());
        assert_eq!(recovered.seeds.len(), 1);
        assert_eq!(recovered.seeds[0].logical_size, 5);
        reopened
            .purge_cache(initialized.state.id, false, CommandId::new())
            .await
            .unwrap();
        let after_purge = reopened
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(after_purge.seeds.len(), 1);
        assert_eq!(after_purge.seeds[0].state, "pruned");
    }

    #[tokio::test]
    async fn promotion_leaves_user_master_unchanged_when_synchronization_is_disabled() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature_worktree = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let old_master = git(&repository, &["rev-parse", USER_BRANCH_REF]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(feature_worktree.join("feature.txt"), "feature\n").unwrap();
        git(&feature_worktree, &["add", "feature.txt"]);
        git(&feature_worktree, &["commit", "-m", "feature"]);
        let source = git(&feature_worktree, &["rev-parse", "HEAD"]);
        git(&repository, &["switch", "--detach", USER_BRANCH]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f feature.txt".into()))
            .await
            .unwrap();
        std::fs::write(
            repository.join(".tollgate/config.toml"),
            "version = 1\nsync_user_master = false\n\n[[step]]\nname = \"ci\"\nrun = \"test -f feature.txt\"\n",
        )
        .unwrap();
        service
            .apply_configuration(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        service
            .approve(initialized.state.id, "feature".into(), CommandId::new())
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.is_empty() {
                assert!(
                    !snapshot
                        .history
                        .iter()
                        .any(|event| event.kind.starts_with("user-master.sync"))
                );
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(git(&repository, &["rev-parse", INTEGRATION_REF]), source);
        assert_eq!(
            git(&repository, &["rev-parse", USER_BRANCH_REF]),
            old_master
        );
    }

    #[tokio::test]
    async fn promotion_rebases_clean_unsubmitted_master_onto_certified_release() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature_worktree = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();

        std::fs::write(repository.join("local.txt"), "local\n").unwrap();
        git(&repository, &["add", "local.txt"]);
        git(&repository, &["commit", "-m", "unsubmitted local work"]);
        let old_master = git(&repository, &["rev-parse", USER_BRANCH_REF]);

        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature_worktree.to_str().unwrap(),
                INTEGRATION_BRANCH,
            ],
        );
        std::fs::write(feature_worktree.join("feature.txt"), "feature\n").unwrap();
        git(&feature_worktree, &["add", "feature.txt"]);
        git(&feature_worktree, &["commit", "-m", "certified feature"]);
        service
            .approve_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let snapshot = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.is_empty()
                && snapshot
                    .history
                    .iter()
                    .any(|event| event.kind == "user-master.synchronized")
            {
                break snapshot;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let certified = snapshot.state.master_oid;
        let rebased_master =
            GitOid::from_hex(&git(&repository, &["rev-parse", USER_BRANCH_REF])).unwrap();
        assert_ne!(rebased_master.to_hex(), old_master);
        assert_eq!(
            service
                .runtime(initialized.state.id)
                .await
                .unwrap()
                .git
                .commit_parent_oid(&rebased_master)
                .await
                .unwrap(),
            certified
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("feature.txt")).unwrap(),
            "feature\n"
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("local.txt")).unwrap(),
            "local\n"
        );
    }

    #[tokio::test]
    async fn promotion_syncs_clean_master_with_unignored_registered_source_worktree() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let remote = temporary.path().join("remote.git");
        let candidate_worktree = repository.join(".worktrees/candidate");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            temporary.path(),
            &["init", "--bare", remote.to_str().unwrap()],
        );
        git(
            &repository,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&repository, &["push", "-u", "origin", USER_BRANCH]);
        std::fs::create_dir(repository.join(".worktrees")).unwrap();
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate",
                candidate_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(candidate_worktree.join("feature.txt"), "feature\n").unwrap();
        git(&candidate_worktree, &["add", "feature.txt"]);
        git(&candidate_worktree, &["commit", "-m", "feature"]);
        assert_eq!(
            git(&repository, &["ls-files", "--others", "--exclude-standard"]),
            ".worktrees/candidate/"
        );

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f feature.txt".into()))
            .await
            .unwrap();
        std::fs::write(
            repository.join(".tollgate/config.toml"),
            r#"version = 1
sync_user_master = true

[remote]
enabled = true
name = "origin"
branch = "master"

[[step]]
name = "ci"
run = "test -f feature.txt"
"#,
        )
        .unwrap();
        service
            .apply_configuration(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        let approved = service
            .approve_from(
                initialized.state.id,
                "HEAD".into(),
                Some(candidate_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.history_items.iter().any(|view| {
                view.item.id == approved.item_id
                    && view.item.cleanup_state == CleanupState::Completed
            }) {
                assert!(
                    snapshot
                        .history
                        .iter()
                        .any(|event| event.kind == "user-master.synchronized")
                );
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        let promoted = approved.tested_oid.to_hex();
        assert_eq!(git(&repository, &["rev-parse", INTEGRATION_REF]), promoted);
        assert_eq!(git(&repository, &["rev-parse", USER_BRANCH_REF]), promoted);
        assert_eq!(
            git(&repository, &["ls-remote", "origin", "refs/heads/master"])
                .split_whitespace()
                .next()
                .unwrap(),
            promoted
        );
        assert!(!candidate_worktree.exists());
    }

    #[tokio::test]
    async fn promotion_does_not_sync_master_over_a_genuine_untracked_user_file() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let remote = temporary.path().join("remote.git");
        let candidate_worktree = repository.join(".worktrees/candidate");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let old_master = git(&repository, &["rev-parse", USER_BRANCH_REF]);
        git(
            temporary.path(),
            &["init", "--bare", remote.to_str().unwrap()],
        );
        git(
            &repository,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&repository, &["push", "-u", "origin", USER_BRANCH]);
        std::fs::create_dir(repository.join(".worktrees")).unwrap();
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate",
                candidate_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(candidate_worktree.join("feature.txt"), "feature\n").unwrap();
        git(&candidate_worktree, &["add", "feature.txt"]);
        git(&candidate_worktree, &["commit", "-m", "feature"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f feature.txt".into()))
            .await
            .unwrap();
        std::fs::write(
            repository.join(".tollgate/config.toml"),
            r#"version = 1
sync_user_master = true

[remote]
enabled = true
name = "origin"
branch = "master"

[[step]]
name = "ci"
run = "test -f feature.txt"
"#,
        )
        .unwrap();
        service
            .apply_configuration(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        std::fs::write(repository.join("user-notes.txt"), "do not overwrite\n").unwrap();
        let approved = service
            .approve_from(
                initialized.state.id,
                "HEAD".into(),
                Some(candidate_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let attention = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.history_items.iter().any(|view| {
                view.item.id == approved.item_id
                    && view.item.cleanup_state == CleanupState::Completed
            }) {
                break snapshot
                    .history
                    .iter()
                    .find(|event| event.kind == "user-master.sync-needs-attention")
                    .cloned()
                    .expect("dirty master synchronization should need attention");
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let promoted = approved.tested_oid.to_hex();
        assert_eq!(git(&repository, &["rev-parse", INTEGRATION_REF]), promoted);
        assert_eq!(
            git(&repository, &["rev-parse", USER_BRANCH_REF]),
            old_master
        );
        assert_eq!(
            git(&repository, &["ls-remote", "origin", "refs/heads/master"])
                .split_whitespace()
                .next()
                .unwrap(),
            promoted
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("user-notes.txt")).unwrap(),
            "do not overwrite\n"
        );
        assert_eq!(
            attention.payload["status_entries"],
            serde_json::json!(["untracked: user-notes.txt"])
        );
    }

    #[tokio::test]
    async fn restart_completes_a_promotion_only_when_release_matches_the_intent() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("change"), "change").unwrap();
        git(&feature, &["add", "change"]);
        git(&feature, &["commit", "-m", "change"]);
        git(&repository, &["switch", "--detach", "master"]);

        let support = temporary.path().join("support");
        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 0.2; test -f change".into()))
            .await
            .unwrap();
        let approved = service
            .approve(initialized.state.id, "feature".into(), CommandId::new())
            .await
            .unwrap();
        let running_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, approved.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::Running {
                break;
            }
            assert!(tokio::time::Instant::now() < running_deadline);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        service
            .set_paused(initialized.state.id, true)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, approved.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::Ready {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let (mut item, certificate, old_master) = {
            let data = runtime.data.lock();
            let item = data
                .items
                .iter()
                .find(|item| item.id == approved.item_id)
                .cloned()
                .unwrap();
            let certificate = data
                .certificates
                .iter()
                .find(|certificate| Some(certificate.id) == item.certificate_id)
                .cloned()
                .unwrap();
            (item, certificate, data.state.master_oid.clone())
        };
        item.state = item.state.transition(ItemEvent::PromotionStarted).unwrap();
        service.replace_item(&runtime, item).unwrap();
        runtime
            .store
            .prepare_promotion(initialized.state.id, CommandId::new(), &certificate)
            .unwrap();
        runtime
            .git
            .retain_tested_object(
                &runtime.mirror,
                &certificate.buildset_id.to_string(),
                &certificate.tested_oid,
            )
            .await
            .unwrap();
        runtime
            .git
            .compare_and_swap_integration(&old_master, &certificate.tested_oid)
            .await
            .unwrap();
        drop(runtime);
        drop(service);

        let recovered = TollgateService::open(support).await.unwrap();
        let snapshot = recovered
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(snapshot.state.master_oid, certificate.tested_oid);
        assert!(snapshot.queue.is_empty());
        assert_eq!(
            recovered
                .item_status(initialized.state.id, approved.item_id)
                .await
                .unwrap()
                .state,
            QueueItemState::Promoted
        );
    }

    #[tokio::test]
    async fn reorder_moves_independent_items_and_replays_idempotently() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a = temporary.path().join("a");
        let b = temporary.path().join("b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        for (name, path) in [("a", &a), ("b", &b)] {
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "-b",
                    name,
                    path.to_str().unwrap(),
                    "master",
                ],
            );
            std::fs::write(path.join(name), name).unwrap();
            git(path, &["add", name]);
            git(path, &["commit", "-m", name]);
        }
        git(&repository, &["switch", "--detach", "master"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 5".into()))
            .await
            .unwrap();
        let approved_a = service
            .approve(initialized.state.id, "a".into(), CommandId::new())
            .await
            .unwrap();
        let approved_b = service
            .approve(initialized.state.id, "b".into(), CommandId::new())
            .await
            .unwrap();
        let command_id = CommandId::new();
        let reordered = service
            .reorder_queue(
                initialized.state.id,
                vec![approved_b.item_id],
                2,
                command_id,
            )
            .await
            .unwrap();
        assert_eq!(
            reordered.ordered_item_ids,
            [approved_b.item_id, approved_a.item_id]
        );
        assert_eq!(reordered.restarted_item_ids.len(), 2);
        assert_eq!(reordered.queue_revision, 3);
        let reordered_snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        let reordered_items = reordered_snapshot
            .queue
            .iter()
            .map(|view| &view.item)
            .collect::<Vec<_>>();
        assert_eq!(reordered_items[0].id, approved_b.item_id);
        assert_eq!(reordered_items[1].id, approved_a.item_id);
        assert!(
            reordered_items[0].admission_sequence.unwrap()
                < reordered_items[1].admission_sequence.unwrap(),
            "an explicit reorder must replace the admission-order baseline"
        );
        let replay = service
            .reorder_queue(
                initialized.state.id,
                vec![approved_b.item_id],
                2,
                command_id,
            )
            .await
            .unwrap();
        assert_eq!(replay.queue_revision, 3);
        assert!(
            service
                .reorder_queue(
                    initialized.state.id,
                    vec![approved_a.item_id],
                    3,
                    command_id,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn independent_check_runs_without_queue_or_promotion_authority() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        let master = git(&repository, &["rev-parse", "master"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("feature"), "feature").unwrap();
        git(&feature, &["add", "feature"]);
        git(&feature, &["commit", "-m", "feature"]);
        git(&repository, &["switch", "--detach", "master"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f feature".into()))
            .await
            .unwrap();
        let run = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, run.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::CheckPassed {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "state={:?}",
                item.state
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert!(snapshot.queue.is_empty());
        assert_eq!(snapshot.checks.len(), 1);
        assert!(snapshot.checks[0].certificate.is_none());
        assert_eq!(git(&repository, &["rev-parse", "master"]), master);
        assert_eq!(git(&repository, &["rev-parse", INTEGRATION_REF]), master);
    }

    #[tokio::test]
    async fn diagnoses_candidate_generated_drift_and_verifies_an_immutable_repair_patch() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base\n").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let command = r#"if test -f broken; then printf '%s\n' '{"code":"generated-output-drift","message":"generated state is stale","paths":["broken"],"repair":{"kind":"argv","argv":["rm","broken"]}}' > "$TOLLGATE_DIAGNOSTICS_FILE"; sleep 1; exit 1; fi"#;
        let initialized = service
            .initialize_repository_with_options(&repository, Some(command.into()), false)
            .await
            .unwrap();

        let base = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, base.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::CheckPassed {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "state={:?}",
                item.state
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        std::fs::write(repository.join("broken"), "stale\n").unwrap();
        git(&repository, &["add", "broken"]);
        git(&repository, &["commit", "-m", "break generated state"]);
        let candidate_oid = git(&repository, &["rev-parse", "HEAD"]);
        let candidate = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let item = service
                .item_status(initialized.state.id, candidate.item_id)
                .await
                .unwrap();
            if item.state == QueueItemState::CheckFailed {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "state={:?}",
                item.state
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let details = service
            .item_details(initialized.state.id, candidate.item_id)
            .await
            .unwrap();
        let attribution = details.failure_attribution.unwrap();
        assert_eq!(attribution.origin, FailureOrigin::CandidateIntroduced);
        assert_eq!(
            attribution.steps[0].diagnostics[0].code,
            "generated-output-drift"
        );
        let retained = service
            .diagnose_failure(
                initialized.state.id,
                candidate.item_id,
                false,
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            retained.attribution.unwrap().origin,
            FailureOrigin::CandidateIntroduced
        );
        assert!(retained.replay_item_ids.is_empty());
        assert!(retained.scheduled_replay_item_ids.is_empty());

        let repair = service
            .diagnose_failure(
                initialized.state.id,
                candidate.item_id,
                false,
                true,
                CommandId::new(),
            )
            .await
            .unwrap()
            .repair_artifact
            .unwrap();
        assert!(repair.verified);
        assert!(
            std::fs::read_to_string(&repair.path)
                .unwrap()
                .contains("deleted file mode")
        );
        assert_eq!(git(&repository, &["rev-parse", "HEAD"]), candidate_oid);
        assert!(repository.join("broken").exists());
        assert!(
            service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap()
                .artifacts
                .iter()
                .any(|artifact| artifact.retained_path == repair.path)
        );

        let matrix = service
            .diagnose_failure(
                initialized.state.id,
                candidate.item_id,
                true,
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(matrix.replay_item_ids.len(), 1);
        assert_eq!(matrix.scheduled_replay_item_ids.len(), 1);
        assert!(
            matrix
                .replay_reasons
                .iter()
                .any(|reason| reason.contains("base replay omitted"))
        );
        let coalesced = service
            .diagnose_failure(
                initialized.state.id,
                candidate.item_id,
                true,
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(coalesced.scheduled_replay_item_ids.is_empty());
        assert_eq!(coalesced.replay_item_ids, matrix.replay_item_ids);
        assert_eq!(coalesced.reused_replay_item_ids, matrix.replay_item_ids);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        for replay_id in &matrix.replay_item_ids {
            loop {
                let item = service
                    .item_status(initialized.state.id, *replay_id)
                    .await
                    .unwrap();
                if item.state.is_terminal() {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "state={:?}",
                    item.state
                );
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        let diagnosed = service
            .diagnose_failure(
                initialized.state.id,
                candidate.item_id,
                false,
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            diagnosed.attribution.unwrap().origin,
            FailureOrigin::CandidateIntroduced
        );
    }

    #[tokio::test]
    async fn attributes_failures_inherited_from_an_exact_failing_base() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base\n").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("false".into()), false)
            .await
            .unwrap();
        let base = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        std::fs::write(repository.join("feature"), "feature\n").unwrap();
        git(&repository, &["add", "feature"]);
        git(&repository, &["commit", "-m", "feature"]);
        let candidate = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        for item_id in [base.item_id, candidate.item_id] {
            loop {
                let item = service
                    .item_status(initialized.state.id, item_id)
                    .await
                    .unwrap();
                if item.state.is_terminal() {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        let details = service
            .item_details(initialized.state.id, candidate.item_id)
            .await
            .unwrap();
        let attribution = details.failure_attribution.unwrap();
        assert_eq!(attribution.origin, FailureOrigin::InheritedFromBase);
        assert_eq!(attribution.steps[0].baseline_item_id, Some(base.item_id));
        assert_eq!(
            attribution.steps[0].baseline_buildset_id,
            Some(
                service
                    .item_details(initialized.state.id, base.item_id)
                    .await
                    .unwrap()
                    .buildset
                    .unwrap()
                    .id
            )
        );
    }

    #[tokio::test]
    async fn contradictory_exact_candidate_results_are_flaky_or_non_hermetic() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let external_allow = temporary.path().join("external-allow");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base\n").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        let command = format!(
            "if test -f broken && ! test -f '{}'; then exit 1; fi",
            external_allow.display()
        );
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some(command), false)
            .await
            .unwrap();
        let base = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        std::fs::write(repository.join("broken"), "broken\n").unwrap();
        git(&repository, &["add", "broken"]);
        git(&repository, &["commit", "-m", "candidate"]);
        let failed = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        for item_id in [base.item_id, failed.item_id] {
            loop {
                let item = service
                    .item_status(initialized.state.id, item_id)
                    .await
                    .unwrap();
                if item.state.is_terminal() {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        let matrix = service
            .diagnose_failure(
                initialized.state.id,
                failed.item_id,
                true,
                false,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(matrix.replay_item_ids.len(), 1);
        assert_eq!(matrix.scheduled_replay_item_ids.len(), 1);
        for replay_id in &matrix.replay_item_ids {
            loop {
                let item = service
                    .item_status(initialized.state.id, *replay_id)
                    .await
                    .unwrap();
                if item.state.is_terminal() {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        std::fs::write(&external_allow, "allowed\n").unwrap();
        let passed = service
            .check_from(
                initialized.state.id,
                "HEAD".into(),
                Some(repository.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        loop {
            let item = service
                .item_status(initialized.state.id, passed.item_id)
                .await
                .unwrap();
            if item.state.is_terminal() {
                assert_eq!(item.state, QueueItemState::CheckPassed);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let details = service
            .item_details(initialized.state.id, failed.item_id)
            .await
            .unwrap();
        assert_eq!(
            details.failure_attribution.unwrap().origin,
            FailureOrigin::FlakyOrNonHermetic
        );
    }

    #[tokio::test]
    async fn pull_adopts_only_a_remote_fast_forward_and_replays() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let remote = temporary.path().join("remote.git");
        let writer = temporary.path().join("writer");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            temporary.path(),
            &[
                "init",
                "--bare",
                "-b",
                USER_BRANCH,
                remote.to_str().unwrap(),
            ],
        );
        git(
            &repository,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&repository, &["push", "-u", "origin", "master"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("feature"), "feature").unwrap();
        git(&feature, &["add", "feature"]);
        git(&feature, &["commit", "-m", "feature"]);
        git(
            temporary.path(),
            &["clone", remote.to_str().unwrap(), writer.to_str().unwrap()],
        );
        std::fs::write(writer.join("remote"), "remote").unwrap();
        git(&writer, &["add", "remote"]);
        git(&writer, &["commit", "-m", "remote"]);
        let remote_tip = git(&writer, &["rev-parse", "HEAD"]);
        git(&writer, &["push", "origin", "master"]);
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 30".into()))
            .await
            .unwrap();
        let approved = service
            .approve_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if service
                .item_status(initialized.state.id, approved.item_id)
                .await
                .unwrap()
                .state
                == QueueItemState::Running
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let command_id = CommandId::new();
        let result = service
            .pull(initialized.state.id, command_id)
            .await
            .unwrap();
        assert!(matches!(result.action, RemoteSyncAction::AdoptedRemote));
        assert_eq!(result.affected_item_ids, vec![approved.item_id]);
        assert_eq!(result.local_master.to_hex(), remote_tip);
        assert_eq!(
            service
                .pull(initialized.state.id, command_id)
                .await
                .unwrap()
                .local_master,
            result.local_master
        );
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            remote_tip
        );
    }

    #[tokio::test]
    async fn push_sends_only_the_certified_local_chain_with_an_exact_lease() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        let remote = temporary.path().join("remote.git");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base"), "base").unwrap();
        git(&repository, &["add", "base"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            temporary.path(),
            &["init", "--bare", remote.to_str().unwrap()],
        );
        git(
            &repository,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&repository, &["push", "-u", "origin", "master"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(feature.join("feature"), "feature").unwrap();
        git(&feature, &["add", "feature"]);
        git(&feature, &["commit", "-m", "feature"]);
        git(&repository, &["switch", "--detach", "master"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f feature".into()))
            .await
            .unwrap();
        let approved = service
            .approve_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if service
                .item_status(initialized.state.id, approved.item_id)
                .await
                .unwrap()
                .state
                == QueueItemState::Promoted
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let local = git(&repository, &["rev-parse", INTEGRATION_REF]);
        let result = service
            .push(initialized.state.id, CommandId::new())
            .await
            .unwrap();
        assert!(matches!(result.action, RemoteSyncAction::Pushed));
        assert_eq!(
            git(&repository, &["ls-remote", "origin", "refs/heads/master"])
                .split_whitespace()
                .next()
                .unwrap(),
            local
        );
    }

    #[tokio::test]
    async fn failed_head_is_removed_and_independent_descendant_rebuilds_without_it() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("feature-a");
        let b_worktree = temporary.path().join("feature-b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = git(&repository, &["rev-parse", "master"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature-a",
                a_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(a_worktree.join("fail.txt"), "fail\n").unwrap();
        git(&a_worktree, &["add", "fail.txt"]);
        git(&a_worktree, &["commit", "-m", "failing a"]);
        let source_a = git(&a_worktree, &["rev-parse", "HEAD"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature-b",
                b_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(b_worktree.join("b.txt"), "b\n").unwrap();
        git(&b_worktree, &["add", "b.txt"]);
        git(&b_worktree, &["commit", "-m", "passing b"]);
        let source_b = git(&b_worktree, &["rev-parse", "HEAD"]);
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(
                &repository,
                Some("if test -f b.txt; then sleep 1; fi; test ! -f fail.txt".into()),
            )
            .await
            .unwrap();
        service
            .approve(initialized.state.id, "feature-a".into(), CommandId::new())
            .await
            .unwrap();
        service
            .approve(initialized.state.id, "feature-b".into(), CommandId::new())
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "queue did not settle: {:?}",
                snapshot
                    .queue
                    .iter()
                    .map(|view| view.item.state)
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let master = git(&repository, &["rev-parse", INTEGRATION_REF]);
        assert_eq!(
            master, source_b,
            "independent B should land directly on the unchanged base"
        );
        assert_eq!(
            git(&repository, &["show", "-s", "--format=%P", INTEGRATION_REF]),
            base
        );
        let contains_a = StdCommand::new("git")
            .current_dir(&repository)
            .args(["merge-base", "--is-ancestor", &source_a, INTEGRATION_REF])
            .status()
            .unwrap();
        assert!(
            !contains_a.success(),
            "failed A must not be in promoted history"
        );
    }

    #[tokio::test]
    async fn passing_descendant_promotes_without_rerun_when_exact_parent_just_landed() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("feature-a");
        let b_worktree = temporary.path().join("feature-b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature-a",
                a_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(a_worktree.join("a.txt"), "a\n").unwrap();
        git(&a_worktree, &["add", "a.txt"]);
        git(&a_worktree, &["commit", "-m", "a"]);
        let source_a = git(&a_worktree, &["rev-parse", "HEAD"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature-b",
                b_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(b_worktree.join("b.txt"), "b\n").unwrap();
        git(&b_worktree, &["add", "b.txt"]);
        git(&b_worktree, &["commit", "-m", "b"]);
        git(&repository, &["switch", "--detach", "master"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 1; test -f base.txt".into()))
            .await
            .unwrap();
        service
            .approve(initialized.state.id, "feature-a".into(), CommandId::new())
            .await
            .unwrap();
        let b = service
            .approve(initialized.state.id, "feature-b".into(), CommandId::new())
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        while !service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap()
            .queue
            .is_empty()
        {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            assert!(
                tokio::time::Instant::now() < deadline,
                "queue stalled: {:?}",
                snapshot
                    .queue
                    .iter()
                    .map(|view| (
                        view.item.state,
                        view.buildset.as_ref().map(|buildset| buildset.state)
                    ))
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let b_buildsets = runtime
            .store
            .buildsets()
            .unwrap()
            .into_iter()
            .filter(|buildset| buildset.item_id == b.item_id)
            .count();
        assert_eq!(
            b_buildsets, 1,
            "an exactly-parented passing descendant must not rerun"
        );
        assert_eq!(
            git(&repository, &["show", "-s", "--format=%P", INTEGRATION_REF]),
            source_a
        );
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            b.tested_oid.to_hex()
        );
    }

    #[tokio::test]
    async fn authorizing_a_later_independent_candidate_prioritizes_it_for_promotion() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("candidate-a");
        let discarded_worktree = temporary.path().join("candidate-discarded");
        let b_worktree = temporary.path().join("candidate-b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        for (branch, path, file) in [
            ("candidate-a", &a_worktree, "a.txt"),
            ("candidate-discarded", &discarded_worktree, "discarded.txt"),
            ("candidate-b", &b_worktree, "b.txt"),
        ] {
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                    "master",
                ],
            );
            std::fs::write(path.join(file), format!("{branch}\n")).unwrap();
            git(path, &["add", file]);
            git(path, &["commit", "-m", branch]);
        }
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();
        let a = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(a_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let discarded = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(discarded_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let b = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(b_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let revision = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap()
            .state
            .queue_revision;
        service
            .cancel(initialized.state.id, discarded.item_id, revision)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let (revision, old_b_generation, old_b_certificate) = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.iter().all(|view| {
                view.item.state == QueueItemState::Ready && !view.item.promotion_authorized
            }) {
                let b_view = snapshot
                    .queue
                    .iter()
                    .find(|view| view.item.id == b.item_id)
                    .unwrap();
                let active_sequences = snapshot
                    .queue
                    .iter()
                    .map(|view| view.item.enqueue_sequence)
                    .collect::<Vec<_>>();
                assert_eq!(active_sequences.len(), 2);
                assert!(active_sequences[1] > active_sequences[0] + 1);
                break (
                    snapshot.state.queue_revision,
                    b_view.generation.as_ref().unwrap().id,
                    b_view.certificate.as_ref().unwrap().id,
                );
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "independent candidates did not finish speculative validation"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let authorized = service
            .authorize_candidate(initialized.state.id, b.item_id, revision, CommandId::new())
            .await
            .unwrap();
        assert_eq!(authorized.authorized_item_ids, vec![b.item_id]);
        assert_eq!(authorized.restarted_item_ids, vec![b.item_id, a.item_id]);
        assert!(!authorized.validation_complete);
        assert!(!authorized.evidence_reused);
        assert_ne!(authorized.validation_generation_id, old_b_generation);

        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let b_promoted = snapshot.history_items.iter().any(|view| {
                view.item.id == b.item_id && view.item.state == QueueItemState::Promoted
            });
            if b_promoted {
                let a_view = snapshot
                    .queue
                    .iter()
                    .find(|view| view.item.id == a.item_id)
                    .unwrap();
                assert!(!a_view.item.promotion_authorized);
                assert_eq!(snapshot.queue.len(), 1);
                assert_eq!(snapshot.state.master_oid, authorized.tested_oid);
                assert_eq!(
                    a_view.generation.as_ref().unwrap().expected_parent_oid,
                    authorized.tested_oid
                );
                let runtime = service.runtime(initialized.state.id).await.unwrap();
                assert_eq!(
                    runtime
                        .data
                        .lock()
                        .generations
                        .iter()
                        .find(|generation| generation.id == old_b_generation)
                        .unwrap()
                        .invalidated_by,
                    Some(authorized.validation_generation_id)
                );
                assert!(runtime.data.lock().certificates.iter().any(|certificate| {
                    certificate.id == old_b_certificate && certificate.queue_item_id == b.item_id
                }));
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "authorized later candidate did not bypass the unapproved head: queue={:?}, history={:?}, blocks={:?}, dispatching={:?}",
                snapshot
                    .queue
                    .iter()
                    .map(|view| (
                        view.item.id,
                        view.item.state,
                        view.item.promotion_authorized
                    ))
                    .collect::<Vec<_>>(),
                snapshot
                    .history_items
                    .iter()
                    .map(|view| (view.item.id, view.item.state))
                    .collect::<Vec<_>>(),
                snapshot.state.block_reasons,
                service
                    .runtime(initialized.state.id)
                    .await
                    .unwrap()
                    .dispatching
                    .lock()
                    .clone()
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn distributed_authorizations_restore_admission_order_and_exact_evidence() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("candidate-a");
        let b_worktree = temporary.path().join("candidate-b");
        let c_worktree = temporary.path().join("candidate-c");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        for (branch, path, file) in [
            ("candidate-a", &a_worktree, "a.txt"),
            ("candidate-b", &b_worktree, "b.txt"),
            ("candidate-c", &c_worktree, "c.txt"),
        ] {
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                    "master",
                ],
            );
            std::fs::write(path.join(file), format!("{branch}\n")).unwrap();
            git(path, &["add", file]);
            git(path, &["commit", "-m", branch]);
        }
        git(&repository, &["switch", "--detach", "master"]);

        let support = temporary.path().join("support");
        let service = TollgateService::open(support.clone()).await.unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 1; true".into()))
            .await
            .unwrap();
        let a = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(a_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let b = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(b_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let c = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(c_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let (
            revision,
            a_generation,
            a_buildset,
            a_certificate,
            b_generation,
            b_certificate,
            c_generation,
            c_certificate,
        ) = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.iter().all(|view| {
                view.item.state == QueueItemState::Ready && !view.item.promotion_authorized
            }) {
                let a_view = snapshot
                    .queue
                    .iter()
                    .find(|view| view.item.id == a.item_id)
                    .unwrap();
                let b_view = snapshot
                    .queue
                    .iter()
                    .find(|view| view.item.id == b.item_id)
                    .unwrap();
                let c_view = snapshot
                    .queue
                    .iter()
                    .find(|view| view.item.id == c.item_id)
                    .unwrap();
                break (
                    snapshot.state.queue_revision,
                    a_view.generation.as_ref().unwrap().id,
                    a_view.buildset.as_ref().unwrap().id,
                    a_view.certificate.as_ref().unwrap().id,
                    b_view.generation.as_ref().unwrap().id,
                    b_view.certificate.as_ref().unwrap().id,
                    c_view.generation.as_ref().unwrap().id,
                    c_view.certificate.as_ref().unwrap().id,
                );
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "initial speculative validations did not finish"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let c_authorized = service
            .authorize_candidate(initialized.state.id, c.item_id, revision, CommandId::new())
            .await
            .unwrap();
        assert_eq!(
            c_authorized.restarted_item_ids,
            vec![c.item_id, a.item_id, b.item_id]
        );
        assert!(c_authorized.restored_item_ids.is_empty());

        let after_c = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(
            after_c
                .queue
                .iter()
                .map(|view| view.item.id)
                .collect::<Vec<_>>(),
            vec![c.item_id, a.item_id, b.item_id]
        );
        let a_authorized = service
            .authorize_candidate(
                initialized.state.id,
                a.item_id,
                after_c.state.queue_revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(a_authorized.restarted_item_ids.is_empty());
        assert!(a_authorized.restored_item_ids.is_empty());
        let after_a = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(
            after_a
                .queue
                .iter()
                .map(|view| view.item.id)
                .collect::<Vec<_>>(),
            vec![c.item_id, a.item_id, b.item_id]
        );
        let b_authorized = service
            .authorize_candidate(
                initialized.state.id,
                b.item_id,
                after_a.state.queue_revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(b_authorized.restarted_item_ids.is_empty());
        assert_eq!(
            b_authorized.restored_item_ids,
            vec![a.item_id, b.item_id, c.item_id]
        );
        assert!(b_authorized.validation_complete);
        assert!(b_authorized.evidence_reused);
        assert_eq!(b_authorized.validation_generation_id, b_generation);

        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert!(snapshot.queue.is_empty());
        assert_eq!(snapshot.state.master_oid, c.tested_oid);
        let a_view = snapshot
            .history_items
            .iter()
            .find(|view| view.item.id == a.item_id)
            .unwrap();
        let b_view = snapshot
            .history_items
            .iter()
            .find(|view| view.item.id == b.item_id)
            .unwrap();
        let c_view = snapshot
            .history_items
            .iter()
            .find(|view| view.item.id == c.item_id)
            .unwrap();
        assert_eq!(a_view.generation.as_ref().unwrap().id, a_generation);
        assert_eq!(a_view.buildset.as_ref().unwrap().id, a_buildset);
        assert_eq!(a_view.certificate.as_ref().unwrap().id, a_certificate);
        assert_eq!(b_view.generation.as_ref().unwrap().id, b_generation);
        assert_eq!(b_view.certificate.as_ref().unwrap().id, b_certificate);
        assert_eq!(c_view.generation.as_ref().unwrap().id, c_generation);
        assert_eq!(c_view.certificate.as_ref().unwrap().id, c_certificate);
        assert_eq!(
            service
                .runtime(initialized.state.id)
                .await
                .unwrap()
                .data
                .lock()
                .certificates
                .len(),
            3,
            "bypass work must not mint replacement certificates after exact evidence restoration"
        );
        service.shutdown().await.unwrap();
        drop(service);
        let reopened = TollgateService::open(support).await.unwrap();
        let reopened_a = reopened
            .item_details(initialized.state.id, a.item_id)
            .await
            .unwrap();
        let reopened_b = reopened
            .item_details(initialized.state.id, b.item_id)
            .await
            .unwrap();
        let reopened_c = reopened
            .item_details(initialized.state.id, c.item_id)
            .await
            .unwrap();
        assert_eq!(reopened_a.generation.as_ref().unwrap().id, a_generation);
        assert_eq!(reopened_a.certificate.as_ref().unwrap().id, a_certificate);
        assert_eq!(reopened_b.generation.as_ref().unwrap().id, b_generation);
        assert_eq!(reopened_b.certificate.as_ref().unwrap().id, b_certificate);
        assert_eq!(reopened_c.generation.as_ref().unwrap().id, c_generation);
        assert_eq!(reopened_c.certificate.as_ref().unwrap().id, c_certificate);
    }

    #[tokio::test]
    async fn candidates_validate_without_promotion_and_reuse_exact_predicted_prefixes() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("candidate-a");
        let b_worktree = temporary.path().join("candidate-b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let original_master = git(&repository, &["rev-parse", "master"]);
        for (branch, path, file) in [
            ("candidate-a", &a_worktree, "a.txt"),
            ("candidate-b", &b_worktree, "b.txt"),
        ] {
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                    "master",
                ],
            );
            std::fs::write(path.join(file), format!("{branch}\n")).unwrap();
            git(path, &["add", file]);
            git(path, &["commit", "-m", branch]);
        }
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f a.txt".into()))
            .await
            .unwrap();
        let a = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(a_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let b = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(b_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.iter().all(|view| {
                view.item.state == QueueItemState::Ready && !view.item.promotion_authorized
            }) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "candidates did not finish validation: {:?}",
                snapshot
                    .queue
                    .iter()
                    .map(|view| view.item.state)
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            original_master
        );
        let before = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        let b_before = before
            .queue
            .iter()
            .find(|view| view.item.id == b.item_id)
            .unwrap();
        let b_generation = b_before.generation.as_ref().unwrap().id;
        let b_buildset = b_before.buildset.as_ref().unwrap().id;
        let b_certificate = b_before.certificate.as_ref().unwrap().id;

        let authorize_a_command = CommandId::new();
        let a_authorized = service
            .authorize_candidate(
                initialized.state.id,
                a.item_id,
                before.state.queue_revision,
                authorize_a_command,
            )
            .await
            .unwrap();
        assert!(a_authorized.validation_complete);
        assert!(a_authorized.evidence_reused);
        assert_eq!(
            service
                .authorize_candidate(
                    initialized.state.id,
                    a.item_id,
                    before.state.queue_revision,
                    authorize_a_command,
                )
                .await
                .unwrap()
                .validation_generation_id,
            a_authorized.validation_generation_id
        );
        let after_a = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        let b_after_a = after_a
            .queue
            .iter()
            .find(|view| view.item.id == b.item_id)
            .unwrap();
        assert_eq!(b_after_a.item.state, QueueItemState::Ready);
        assert_eq!(b_after_a.generation.as_ref().unwrap().id, b_generation);
        assert_eq!(b_after_a.buildset.as_ref().unwrap().id, b_buildset);
        assert_eq!(b_after_a.certificate.as_ref().unwrap().id, b_certificate);

        let b_authorized = service
            .authorize_candidate(
                initialized.state.id,
                b.item_id,
                after_a.state.queue_revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(b_authorized.evidence_reused);
        assert_eq!(b_authorized.validation_generation_id, b_generation);
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            b.tested_oid.to_hex()
        );
        let history = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap()
            .history;
        assert_eq!(
            history
                .iter()
                .filter(|event| event.kind == "candidate.created")
                .count(),
            2
        );
        assert_eq!(
            history
                .iter()
                .filter(|event| event.kind == "candidate.promotion-authorized")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn candidate_conflict_reports_paths_and_stale_base_without_creating_an_item() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("messages.json"), "base\n").unwrap();
        git(&repository, &["add", "messages.json"]);
        git(&repository, &["commit", "-m", "base"]);
        let source_base = git(&repository, &["rev-parse", "HEAD"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(feature.join("messages.json"), "feature\n").unwrap();
        git(&feature, &["commit", "-am", "regenerate messages"]);

        std::fs::write(repository.join("messages.json"), "release\n").unwrap();
        git(&repository, &["commit", "-am", "advance release"]);
        let release = git(&repository, &["rev-parse", "HEAD"]);
        git(&repository, &["switch", "--detach", USER_BRANCH]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();
        let error = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(feature.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("merge conflicts"));
        assert!(message.contains("messages.json"));
        assert!(message.contains(&source_base));
        assert!(message.contains(&release));
        assert!(message.contains("based only on promoted `release`"));
        assert!(message.contains("never rebase it onto a speculative prefix"));
        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert!(snapshot.queue.is_empty());
        assert_eq!(snapshot.state.queue_revision, 0);
    }

    #[tokio::test]
    async fn conflicting_candidate_validates_in_its_own_lane_and_can_be_prioritized() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let queued_worktree = temporary.path().join("queued");
        let task_worktree = temporary.path().join("task");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("messages.json"), "base\n").unwrap();
        git(&repository, &["add", "messages.json"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = git(&repository, &["rev-parse", "HEAD"]);
        for (branch, path) in [("queued", &queued_worktree), ("task", &task_worktree)] {
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                    &base,
                ],
            );
        }
        std::fs::write(queued_worktree.join("messages.json"), "queued\n").unwrap();
        git(
            &queued_worktree,
            &["commit", "-am", "update generated messages"],
        );
        std::fs::remove_file(task_worktree.join("messages.json")).unwrap();
        git(&task_worktree, &["add", "messages.json"]);
        git(
            &task_worktree,
            &["commit", "-m", "remove generated messages"],
        );
        std::fs::write(repository.join("release.txt"), "release\n").unwrap();
        git(&repository, &["add", "release.txt"]);
        git(&repository, &["commit", "-m", "advance release"]);
        git(&repository, &["switch", "--detach", USER_BRANCH]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();
        let queued = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(queued_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_ne!(
            queued.source_oid, queued.tested_oid,
            "fixture must exercise a synthetic prefix rather than an active source OID"
        );
        let queued_view = service
            .item_details(initialized.state.id, queued.item_id)
            .await
            .unwrap();
        let generation = queued_view.generation.unwrap();
        assert_eq!(
            git(
                &repository,
                &[
                    "rev-parse",
                    &format!("refs/tollgate/speculative/{}", generation.id)
                ],
            ),
            queued.tested_oid.to_hex()
        );

        let contender = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(task_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            git(&task_worktree, &["show", "-s", "--format=%P", "HEAD"]),
            base
        );
        let contender_view = service
            .item_details(initialized.state.id, contender.item_id)
            .await
            .unwrap();
        assert_eq!(
            contender_view.generation.unwrap().ordered_item_ids,
            vec![contender.item_id],
            "a conflicting contender must initially validate directly on release"
        );

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let revision = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot
                .queue
                .iter()
                .all(|view| view.item.state == QueueItemState::Ready)
            {
                break snapshot.state.queue_revision;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "conflicting candidates did not finish their independent speculative lanes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let authorized = service
            .authorize_candidate(
                initialized.state.id,
                contender.item_id,
                revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(authorized.evidence_reused);

        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert!(snapshot.history_items.iter().any(|view| {
            view.item.id == contender.item_id && view.item.state == QueueItemState::Promoted
        }));
        assert!(snapshot.history_items.iter().any(|view| {
            view.item.id == queued.item_id && view.item.state == QueueItemState::MergeConflict
        }));
        assert!(snapshot.queue.is_empty());
    }

    #[tokio::test]
    async fn ordinary_candidate_rejects_an_active_candidate_as_its_source_parent() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let prerequisite_worktree = temporary.path().join("prerequisite");
        let dependent_worktree = temporary.path().join("dependent");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "prerequisite",
                prerequisite_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(prerequisite_worktree.join("a.txt"), "a\n").unwrap();
        git(&prerequisite_worktree, &["add", "a.txt"]);
        git(&prerequisite_worktree, &["commit", "-m", "prerequisite"]);
        git(&repository, &["switch", "--detach", USER_BRANCH]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();
        let prerequisite = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(prerequisite_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "dependent",
                dependent_worktree.to_str().unwrap(),
                &prerequisite.source_oid.to_hex(),
            ],
        );
        std::fs::write(dependent_worktree.join("b.txt"), "b\n").unwrap();
        git(&dependent_worktree, &["add", "b.txt"]);
        git(&dependent_worktree, &["commit", "-m", "dependent"]);

        let error = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(dependent_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::UnpromotedSourceAncestor { ancestor, release_oid }
                if ancestor == prerequisite.source_oid
                    && release_oid == initialized.state.master_oid
        ));
        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(snapshot.queue.len(), 1);
        assert_eq!(snapshot.queue[0].item.id, prerequisite.item_id);
    }

    #[tokio::test]
    async fn promoted_source_parent_is_accepted_as_authoritative_history() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let queued_worktree = temporary.path().join("queued");
        let task_worktree = temporary.path().join("task");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "queued",
                queued_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(queued_worktree.join("queued.txt"), "queued\n").unwrap();
        git(&queued_worktree, &["add", "queued.txt"]);
        git(&queued_worktree, &["commit", "-m", "queued"]);
        std::fs::write(repository.join("release.txt"), "release\n").unwrap();
        git(&repository, &["add", "release.txt"]);
        git(&repository, &["commit", "-m", "advance release"]);
        git(&repository, &["switch", "--detach", USER_BRANCH]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();
        let queued = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(queued_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "task",
                task_worktree.to_str().unwrap(),
                &queued.source_oid.to_hex(),
            ],
        );
        std::fs::write(task_worktree.join("task.txt"), "task\n").unwrap();
        git(&task_worktree, &["add", "task.txt"]);
        git(&task_worktree, &["commit", "-m", "task"]);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let revision = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .queue
                .iter()
                .find(|view| view.item.id == queued.item_id)
                .unwrap();
            if view.item.state == QueueItemState::Ready {
                break snapshot.state.queue_revision;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        service
            .authorize_candidate(
                initialized.state.id,
                queued.item_id,
                revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.history_items.iter().any(|view| {
                view.item.id == queued.item_id && view.item.state == QueueItemState::Promoted
            }) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let submitted = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(task_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let view = service
            .item_details(initialized.state.id, submitted.item_id)
            .await
            .unwrap();
        assert!(view.item.dependencies.is_empty());
        assert_ne!(view.item.source_oid, submitted.tested_oid);
        assert_eq!(
            view.generation.unwrap().expected_parent_oid,
            queued.tested_oid
        );
    }

    #[tokio::test]
    async fn speculative_parent_is_rejected_in_favor_of_promoted_release() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let queued_worktree = temporary.path().join("queued");
        let extension_worktree = temporary.path().join("extension");
        let task_worktree = temporary.path().join("task");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "queued",
                queued_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(queued_worktree.join("queued.txt"), "queued\n").unwrap();
        git(&queued_worktree, &["add", "queued.txt"]);
        git(&queued_worktree, &["commit", "-m", "queued"]);
        std::fs::write(repository.join("release.txt"), "release\n").unwrap();
        git(&repository, &["add", "release.txt"]);
        git(&repository, &["commit", "-m", "advance release"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "extension",
                extension_worktree.to_str().unwrap(),
                USER_BRANCH,
            ],
        );
        std::fs::write(extension_worktree.join("extension.txt"), "extension\n").unwrap();
        git(&extension_worktree, &["add", "extension.txt"]);
        git(&extension_worktree, &["commit", "-m", "extension"]);
        git(&repository, &["switch", "--detach", USER_BRANCH]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("true".into()))
            .await
            .unwrap();
        service.shutting_down.store(true, Ordering::Release);
        let queued = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(queued_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let extension = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(extension_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .item_details(initialized.state.id, extension.item_id)
                .await
                .unwrap()
                .generation
                .unwrap()
                .ordered_item_ids,
            vec![queued.item_id, extension.item_id]
        );
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "task",
                task_worktree.to_str().unwrap(),
                &queued.tested_oid.to_hex(),
            ],
        );
        std::fs::write(task_worktree.join("task.txt"), "task\n").unwrap();
        git(&task_worktree, &["add", "task.txt"]);
        git(&task_worktree, &["commit", "-m", "task"]);
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        let mut canceled = runtime
            .data
            .lock()
            .items
            .iter()
            .find(|item| item.id == queued.item_id)
            .cloned()
            .unwrap();
        canceled.state = canceled.state.transition(ItemEvent::Canceled).unwrap();
        canceled.terminal_reason = Some("canceled-by-user".into());
        service.replace_item(&runtime, canceled).unwrap();
        drop(runtime);
        let after_cancel = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert_eq!(after_cancel.queue.len(), 1);
        assert_eq!(after_cancel.queue[0].item.id, extension.item_id);
        let error = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(task_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap_err();
        match error {
            ServiceError::UnpromotedSourceAncestor {
                ancestor,
                release_oid,
            } => {
                assert_eq!(ancestor, queued.tested_oid);
                assert_eq!(release_oid, after_cancel.state.master_oid);
            }
            error => panic!("expected unpromoted source ancestor, got {error}"),
        }
    }

    #[tokio::test]
    async fn authorizing_an_already_authorized_dependency_is_an_idempotent_success() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("candidate-a");
        let b_worktree = temporary.path().join("candidate-b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate-a",
                a_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(a_worktree.join("a.txt"), "a\n").unwrap();
        git(&a_worktree, &["add", "a.txt"]);
        git(&a_worktree, &["commit", "-m", "candidate-a"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate-b",
                b_worktree.to_str().unwrap(),
                "candidate-a",
            ],
        );
        std::fs::write(b_worktree.join("b.txt"), "b\n").unwrap();
        git(&b_worktree, &["add", "b.txt"]);
        git(&b_worktree, &["commit", "-m", "candidate-b"]);
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 1; test -f a.txt".into()))
            .await
            .unwrap();
        let a = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(a_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let b = service
            .enqueue_gate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(b_worktree.to_string_lossy().into_owned()),
                GateSubmission {
                    purpose: Some("push-master".into()),
                    cleanup_policy: CleanupPolicy::Automatic,
                    command_id: CommandId::new(),
                    promotion_authorized: false,
                },
            )
            .await
            .unwrap();
        let details = service.item_details_by_id(None, a.item_id).await.unwrap();
        assert_eq!(details.item.id, a.item_id);
        assert_eq!(details.item.repository_id, initialized.state.id);
        let revision = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap()
            .state
            .queue_revision;
        let dependent_authorization = service
            .authorize_candidate(initialized.state.id, b.item_id, revision, CommandId::new())
            .await
            .unwrap();
        assert_eq!(
            dependent_authorization.authorized_item_ids,
            vec![a.item_id, b.item_id]
        );

        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        let retry_command = CommandId::new();
        let converged = service
            .authorize_candidate(
                initialized.state.id,
                a.item_id,
                snapshot.state.queue_revision,
                retry_command,
            )
            .await
            .unwrap();
        assert!(converged.already_authorized);
        assert!(converged.authorized_item_ids.is_empty());
        assert!(converged.restarted_item_ids.is_empty());
        assert_eq!(converged.queue_revision, snapshot.state.queue_revision);
        assert_eq!(
            converged.authorized_at,
            dependent_authorization.authorized_at
        );

        let replayed = service
            .authorize_candidate(
                initialized.state.id,
                a.item_id,
                snapshot.state.queue_revision,
                retry_command,
            )
            .await
            .unwrap();
        assert!(replayed.already_authorized);
        assert_eq!(replayed.queue_revision, converged.queue_revision);
        assert_eq!(
            replayed.validation_generation_id,
            converged.validation_generation_id
        );
        assert_eq!(
            service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap()
                .history
                .iter()
                .filter(|event| event.kind == "candidate.promotion-authorized")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn authorizing_a_candidate_atomically_authorizes_its_active_dependencies() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let a_worktree = temporary.path().join("candidate-a");
        let b_worktree = temporary.path().join("candidate-b");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let original_master = git(&repository, &["rev-parse", "master"]);

        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate-a",
                a_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(a_worktree.join("a.txt"), "a\n").unwrap();
        git(&a_worktree, &["add", "a.txt"]);
        git(&a_worktree, &["commit", "-m", "candidate-a"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate-b",
                b_worktree.to_str().unwrap(),
                "candidate-a",
            ],
        );
        std::fs::write(b_worktree.join("b.txt"), "b\n").unwrap();
        git(&b_worktree, &["add", "b.txt"]);
        git(&b_worktree, &["commit", "-m", "candidate-b"]);
        git(&repository, &["switch", "--detach", "master"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f a.txt".into()))
            .await
            .unwrap();
        let a = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(a_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let b = service
            .enqueue_gate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(b_worktree.to_string_lossy().into_owned()),
                GateSubmission {
                    purpose: Some("push-master".into()),
                    cleanup_policy: CleanupPolicy::Automatic,
                    command_id: CommandId::new(),
                    promotion_authorized: false,
                },
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let revision = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if snapshot.queue.iter().all(|view| {
                view.item.state == QueueItemState::Ready && !view.item.promotion_authorized
            }) {
                assert_eq!(
                    snapshot
                        .queue
                        .iter()
                        .find(|view| view.item.id == b.item_id)
                        .unwrap()
                        .item
                        .dependencies,
                    vec![a.item_id]
                );
                break snapshot.state.queue_revision;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dependent candidates did not finish validation"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let authorization_command = CommandId::new();
        let authorized = service
            .authorize_candidate(
                initialized.state.id,
                b.item_id,
                revision,
                authorization_command,
            )
            .await
            .unwrap();
        assert_eq!(authorized.item_id, b.item_id);
        assert_eq!(authorized.authorized_item_ids, vec![a.item_id, b.item_id]);
        assert!(authorized.validation_complete);
        assert!(authorized.evidence_reused);
        assert_eq!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            b.tested_oid.to_hex()
        );
        assert_ne!(
            git(&repository, &["rev-parse", INTEGRATION_REF]),
            original_master
        );

        let snapshot = service
            .repository_snapshot(initialized.state.id)
            .await
            .unwrap();
        assert!(snapshot.queue.is_empty());
        for item_id in [a.item_id, b.item_id] {
            let view = snapshot
                .history_items
                .iter()
                .find(|view| view.item.id == item_id)
                .unwrap();
            assert_eq!(view.item.state, QueueItemState::Promoted);
            assert!(view.item.promotion_authorized);
            assert_eq!(
                view.item.promotion_authorized_by,
                Some(authorization_command)
            );
            assert_eq!(
                view.item.promotion_authorized_at,
                Some(authorized.authorized_at)
            );
        }
        assert_eq!(
            snapshot
                .history
                .iter()
                .filter(|event| event.kind == "candidate.promotion-authorized")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn authorization_granted_while_running_survives_worker_completion() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let candidate_worktree = temporary.path().join("candidate");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "candidate",
                candidate_worktree.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(candidate_worktree.join("candidate.txt"), "candidate\n").unwrap();
        git(&candidate_worktree, &["add", "candidate.txt"]);
        git(&candidate_worktree, &["commit", "-m", "candidate"]);

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("sleep 2; false".into()))
            .await
            .unwrap();
        let candidate = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(candidate_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let revision = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .queue
                .iter()
                .find(|view| view.item.id == candidate.item_id)
                .unwrap();
            if view.item.state == QueueItemState::Running {
                break snapshot.state.queue_revision;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        let authorization_command = CommandId::new();
        service
            .authorize_candidate(
                initialized.state.id,
                candidate.item_id,
                revision,
                authorization_command,
            )
            .await
            .unwrap();

        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .history_items
                .iter()
                .find(|view| view.item.id == candidate.item_id);
            if let Some(view) = view {
                assert_eq!(view.item.state, QueueItemState::Failed);
                assert!(view.item.promotion_authorized);
                assert_eq!(
                    view.item.promotion_authorized_by,
                    Some(authorization_command)
                );
                assert!(view.item.promotion_authorized_at.is_some());
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shell_wrapped_termination_retries_the_authorized_candidate() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let marker = temporary.path().join("interrupted-once");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(&repository, &["switch", "-c", "feature"]);
        std::fs::write(repository.join("feature.txt"), "feature\n").unwrap();
        git(&repository, &["add", "feature.txt"]);
        git(&repository, &["commit", "-m", "feature"]);
        let command = format!(
            "if test -f '{}'; then true; else touch '{}'; exit 143; fi",
            marker.display(),
            marker.display()
        );

        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some(command), false)
            .await
            .unwrap();
        let approved = service
            .approve(initialized.state.id, "feature".into(), CommandId::new())
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            if let Some(view) = snapshot
                .history_items
                .iter()
                .find(|view| view.item.id == approved.item_id)
            {
                assert_eq!(view.item.state, QueueItemState::Promoted);
                assert!(view.item.promotion_authorized);
                assert_eq!(view.attempts.len(), 2);
                assert_eq!(view.attempts[0].state, BuildsetState::Interrupted);
                assert_eq!(view.attempts[1].state, BuildsetState::Passed);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn candidate_authorization_adopts_external_master_and_invalidates_stale_evidence() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let candidate_worktree = temporary.path().join("candidate");
        let external_worktree = temporary.path().join("external");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        for (branch, path, file) in [
            ("candidate", &candidate_worktree, "candidate.txt"),
            ("external", &external_worktree, "external.txt"),
        ] {
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                    "master",
                ],
            );
            std::fs::write(path.join(file), format!("{branch}\n")).unwrap();
            git(path, &["add", file]);
            git(path, &["commit", "-m", branch]);
        }
        let external_oid = git(&external_worktree, &["rev-parse", "HEAD"]);
        git(&repository, &["switch", "--detach", "master"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(&repository, Some("test -f candidate.txt".into()))
            .await
            .unwrap();
        let candidate = service
            .submit_candidate_from(
                initialized.state.id,
                "HEAD".into(),
                Some(candidate_worktree.to_string_lossy().into_owned()),
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let (old_generation, old_revision) = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .queue
                .iter()
                .find(|view| view.item.id == candidate.item_id)
                .unwrap();
            if view.item.state == QueueItemState::Ready {
                break (
                    view.generation.as_ref().unwrap().id,
                    snapshot.state.queue_revision,
                );
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };

        git(&repository, &["update-ref", INTEGRATION_REF, &external_oid]);
        let error = service
            .authorize_candidate(
                initialized.state.id,
                candidate.item_id,
                old_revision,
                CommandId::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ServiceError::RevisionConflict { .. }));
        let (new_generation, new_revision) = loop {
            let snapshot = service
                .repository_snapshot(initialized.state.id)
                .await
                .unwrap();
            let view = snapshot
                .queue
                .iter()
                .find(|view| view.item.id == candidate.item_id)
                .unwrap();
            if view.item.state == QueueItemState::Ready
                && view.generation.as_ref().unwrap().id != old_generation
            {
                assert_eq!(
                    view.generation.as_ref().unwrap().anchored_base_oid.to_hex(),
                    external_oid
                );
                break (
                    view.generation.as_ref().unwrap().id,
                    snapshot.state.queue_revision,
                );
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };
        let runtime = service.runtime(initialized.state.id).await.unwrap();
        assert_eq!(
            runtime
                .data
                .lock()
                .generations
                .iter()
                .find(|generation| generation.id == old_generation)
                .unwrap()
                .invalidated_by,
            Some(new_generation)
        );
        drop(runtime);
        let authorized = service
            .authorize_candidate(
                initialized.state.id,
                candidate.item_id,
                new_revision,
                CommandId::new(),
            )
            .await
            .unwrap();
        assert!(authorized.evidence_reused);
        assert_eq!(authorized.validation_generation_id, new_generation);
        assert_eq!(
            git(&repository, &["show", "-s", "--format=%P", INTEGRATION_REF]),
            external_oid
        );
    }

    #[tokio::test]
    async fn hard_dependent_leaves_with_failed_prerequisite() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let stacked = temporary.path().join("stacked");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = git(&repository, &["rev-parse", "master"]);
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature-a",
                stacked.to_str().unwrap(),
                "master",
            ],
        );
        std::fs::write(stacked.join("fail.txt"), "fail\n").unwrap();
        git(&stacked, &["add", "fail.txt"]);
        git(&stacked, &["commit", "-m", "a"]);
        git(&stacked, &["switch", "-c", "feature-b"]);
        std::fs::write(stacked.join("b.txt"), "b\n").unwrap();
        git(&stacked, &["add", "b.txt"]);
        git(&stacked, &["commit", "-m", "b"]);
        git(&repository, &["switch", "--detach", "master"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository(
                &repository,
                Some("if test -f b.txt; then sleep 3; else sleep 1; fi; test ! -f fail.txt".into()),
            )
            .await
            .unwrap();
        service
            .approve(initialized.state.id, "feature-a".into(), CommandId::new())
            .await
            .unwrap();
        let b = service
            .approve_from_with_cleanup_policy(
                initialized.state.id,
                "feature-b".into(),
                None,
                Some("push-master".into()),
                CleanupPolicy::Automatic,
                CommandId::new(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let runtime = service.runtime(initialized.state.id).await.unwrap();
            let state = runtime
                .data
                .lock()
                .items
                .iter()
                .find(|item| item.id == b.item_id)
                .map(|item| item.state)
                .unwrap();
            if state == QueueItemState::DependencyFailed {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dependent state stalled at {state:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(git(&repository, &["rev-parse", INTEGRATION_REF]), base);
    }
}
