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
use tollgate_git::{GitError, GitRepository};
use tollgate_runner::apfs::{CloneManifest, force_clone_file, force_clone_tree, verify_clone_tree};
use tollgate_runner::{
    BuildsetExecution, EnvironmentSnapshot, RenderedLogFrame, StepResultClass,
    durable_log_tail_start, read_durable_log, run_buildset_scheduled, verify_durable_log,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositorySnapshot {
    pub state: RepositoryState,
    pub observed_master_oid: GitOid,
    pub queue: Vec<QueueItemView>,
    pub checks: Vec<QueueItemView>,
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
    execution_permits: RwLock<Arc<Semaphore>>,
    scheduler_epoch: AtomicU64,
    dispatching: Mutex<HashSet<QueueItemId>>,
    cold_sources: Mutex<HashSet<GitOid>>,
    cold_items: Mutex<HashSet<QueueItemId>>,
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
    }
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
        let path = runtime.git.common_dir.join("tollgate/config.toml");
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
        detach_master: bool,
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
        if git.current_branch().await?.as_deref() == Some("master") {
            if !detach_master {
                return Err(ServiceError::Invariant(
                    "the selected worktree currently owns `master`; choose the explicit detach-master option or switch this worktree to a feature branch before initialization".into(),
                ));
            }
            git.detach_current_master_if_clean().await?;
        }
        let config_path = tollgate_root.join("config.toml");
        if !config_path.exists() {
            let command = command.unwrap_or_else(|| detect_command(&git.worktree_root));
            let contents = format!(
                "version = 1\n\n[[step]]\nname = \"ci\"\nrun = {}\n",
                toml_string(&command)
            );
            tokio::fs::write(&config_path, contents).await?;
        }
        let config = EffectiveConfig::parse(&tokio::fs::read_to_string(&config_path).await?)?;
        let master_oid = git.master_oid().await?;
        let repository_id = RepositoryId::new();
        let name = git
            .worktree_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("Repository")
            .to_owned();
        let mut execution_state = RepositoryExecutionState::Active;
        let mut block_reasons = Vec::new();
        if let Err(GitError::MasterCheckedOut(path)) = git.ensure_master_not_checked_out().await {
            execution_state = RepositoryExecutionState::Blocked;
            block_reasons.push(BlockReason { code: "master-checked-out".into(), message: format!("master is checked out at {path}"), recovery_action: "Detach that clean worktree at the current commit, then resume the gate.".into() });
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
            integration_ref: "refs/heads/master".into(),
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
                "refs/heads/master".into(),
                Some(runtime.git.worktree_root.to_string_lossy().into_owned()),
                CommandId::new(),
                true,
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
        let disk_config = EffectiveConfig::parse(
            &tokio::fs::read_to_string(git.common_dir.join("tollgate/config.toml")).await?,
        )?;
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
        for (command_id, kind, evidence, _) in runtime.store.unfinished_operations(&[
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
            "reconcile",
        ])? {
            match kind.as_str() {
                "config-regenerate" => {
                    self.reconcile_config_regeneration(runtime, command_id, &evidence)
                        .await?;
                    continue;
                }
                "worktree-create" | "worktree-remove" | "update" => {
                    self.reconcile_worktree_mutation(runtime, command_id, &kind, &evidence)
                        .await?;
                    continue;
                }
                "slot-reset" => {
                    self.reconcile_slot_reset(runtime, command_id, &evidence)
                        .await?;
                    continue;
                }
                "cleanup" => {
                    self.reconcile_source_cleanup(runtime, command_id, &evidence)
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
        if runtime.git.master_oid().await? != observed {
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
            runtime.data.lock().generations.push(generation);
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
                    recovery_action: "Preserve the affected paths and use guided reconciliation after inspecting the prepared operation evidence.".into(),
                });
            }
            data.state.clone()
        };
        runtime.store.update_repository_state(&state)?;
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
        let expected_path = runtime.git.common_dir.join("tollgate/config.toml");
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
                    "Recovered a feature worktree created from gated master {}.",
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
            if tokio::fs::try_exists(&path).await? {
                let observed = GitRepository::discover(&path).await?;
                if observed.common_dir == runtime.git.common_dir
                    && observed.current_branch().await?.as_deref() == Some(branch)
                    && observed.resolve_oid("HEAD").await? == expected
                {
                    runtime.store.set_intent_state(
                        command_id,
                        IntentState::Canceled,
                        &serde_json::json!({"recovery": "worktree-remove-not-applied"}),
                    )?;
                    return Ok(());
                }
                return self.mark_ambiguous_mutation_recovery(
                    runtime,
                    command_id,
                    "worktree-remove-ambiguous",
                    "Worktree still exists but no longer matches the prepared removal evidence"
                        .into(),
                );
            }
            let branch_ref = format!("refs/heads/{branch}");
            if let Some(observed) = runtime.git.optional_ref_oid(&branch_ref).await? {
                if observed != expected {
                    return self.mark_ambiguous_mutation_recovery(
                        runtime,
                        command_id,
                        "worktree-remove-ambiguous",
                        "Removed worktree's branch moved after the operation was prepared".into(),
                    );
                }
                runtime
                    .git
                    .delete_source_ref(&branch_ref, &expected)
                    .await?;
            }
            let result = WorktreeOperationResult {
                action: "removed".into(),
                path: path.to_string_lossy().into_owned(),
                branch: Some(branch.into()),
                old_oid: Some(expected),
                new_oid: None,
                message: "Recovered a verified worktree removal after restart.".into(),
            };
            return self.complete_recovered_worktree_operation(
                runtime,
                command_id,
                kind,
                request_digest,
                result,
            );
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
                "Updated feature commit does not have the prepared gated master and exact branch identity"
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
        let branch_ref = format!("refs/heads/{branch}");
        if tokio::fs::try_exists(&path).await? {
            let worktree = GitRepository::discover(&path).await?;
            if worktree.common_dir != runtime.git.common_dir
                || worktree.current_branch().await?.as_deref() != Some(branch)
                || worktree.resolve_oid("HEAD").await? != expected_oid
            {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"recovery": "cleanup-worktree-mismatch"}),
                )?;
                return self.set_cleanup_attention(runtime, item_id);
            }
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
        } else if let Some(observed) = runtime.git.optional_ref_oid(&branch_ref).await? {
            if observed != expected_oid {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::NeedsAttention,
                    &serde_json::json!({"recovery": "cleanup-branch-moved", "observed": observed}),
                )?;
                return self.set_cleanup_attention(runtime, item_id);
            }
            runtime
                .git
                .delete_source_ref(&branch_ref, &expected_oid)
                .await?;
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
        let observed = runtime.git.master_oid().await?;
        let persisted = runtime.data.lock().state.master_oid.clone();
        if observed == persisted {
            return Ok(());
        }
        let state = {
            let mut data = runtime.data.lock();
            data.state.execution_state = RepositoryExecutionState::Blocked;
            data.state.block_reasons.push(BlockReason {
                code: "external-master-movement".into(),
                message: format!(
                    "master moved externally from {} to {} while Tollgate was stopped",
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
                data.generations.push(generation);
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
                Err(GitError::Unmergeable) => {
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
            } else {
                "approve"
            };
            let event = runtime.store.complete_approval(
                &item,
                &generation,
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
            data.generations.push(generation);
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
                let local = runtime.git.master_oid().await?;
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
            let observed_local = runtime.git.master_oid().await?;
            if kind == "pull" {
                let expected_local: GitOid = serde_json::from_value(
                    evidence.get("expected_local").cloned().ok_or_else(|| {
                        ServiceError::Invariant("pull intent omitted expected local master".into())
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
            let observed_master = runtime.git.master_oid().await?;
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
                    message: "A promoting item has no durable promotion intent and master has moved.".into(),
                    recovery_action: "Inspect master and the item certificate, then reconcile the external movement.".into(),
                });
                data.state.clone()
            };
            runtime.store.update_repository_state(&state)?;
            return Ok(());
        };
        let observed_master = runtime.git.master_oid().await?;
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
            let (mut item, mut state) = {
                let data = runtime.data.lock();
                let item = data
                    .items
                    .iter()
                    .find(|item| item.id == certificate.queue_item_id)
                    .cloned()
                    .ok_or(ServiceError::ItemNotFound(certificate.queue_item_id))?;
                (item, data.state.clone())
            };
            if item.state != QueueItemState::Promoting
                || item.certificate_id != Some(certificate.id)
            {
                return self.block_for_ambiguous_promotion(runtime, &observed_master, command_id);
            }
            let remote_enabled = runtime.data.lock().config.remote.enabled;
            item.state = item
                .state
                .transition(if remote_enabled {
                    ItemEvent::PromotedWithPush
                } else {
                    ItemEvent::PromotedWithoutPush
                })
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.cleanup_state = CleanupState::Pending;
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
                message: "master matches neither side of an unfinished promotion intent.".into(),
                recovery_action: "Inspect the recorded certificate and reconcile the external master movement before resuming.".into(),
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
        let observed_master_oid = runtime.git.master_oid().await?;
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
            history_items,
            history,
            configuration: ConfigurationView {
                digest: data.config.digest.clone(),
                step_graph_digest: data.config.step_graph_digest.clone(),
                steps: data.config.steps.clone(),
                remote_enabled: data.config.remote.enabled,
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
            tokio::fs::read_to_string(runtime.git.common_dir.join("tollgate/config.toml")).await?;
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
        }))?;
        if let Some(response) =
            runtime
                .store
                .checked_command_response(command_id, "approve", &request_digest)?
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
                    .checked_command_response(command_id, "approve", &request_digest)?
            {
                return Ok(response);
            }
        }
        let item_id = QueueItemId::new();
        let (state, active_items, existing_ids, existing_oids, enqueue_sequence) = {
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
            )
        };
        let unmerged_ancestors = runtime
            .git
            .unmerged_first_parent_ancestors(&probe.parent_oid, &state.master_oid)
            .await?;
        let mut dependencies = Vec::new();
        for ancestor in &unmerged_ancestors {
            if let Some(item) = active_items
                .iter()
                .find(|item| item.source_oid == *ancestor)
            {
                dependencies.push(item.id);
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
            return Err(ServiceError::Invariant(format!(
                "unknown unmerged source ancestor {ancestor}"
            )));
        }
        let mut ordered_ids = existing_ids;
        let mut sources = existing_oids;
        ordered_ids.push(item_id);
        sources.push(probe.source_oid.clone());
        let mut item = QueueItem {
            id: item_id,
            repository_id,
            kind: QueueItemKind::Gate,
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
                purpose: Some("gate".into()),
            },
            state: QueueItemState::Constructing,
            terminal_reason: None,
            remote_state: if current_config.remote.enabled {
                RemoteState::PreflightPending
            } else {
                RemoteState::Disabled
            },
            cleanup_state: CleanupState::NotEligible,
            dependencies,
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
            Err(GitError::Unmergeable) => {
                runtime
                    .git
                    .delete_source_ref(&item.source_ref, &item.source_oid)
                    .await?;
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"approval": "unmergeable"}),
                )?;
                return Err(GitError::Unmergeable.into());
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
        let event = runtime.store.complete_approval(
            &item,
            &generation,
            Actor::Ui,
            command_id,
            "approve",
            &request_digest,
            &result,
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.queue_revision += 1;
            data.state.event_sequence = event.sequence;
            data.items.push(item);
            data.generations.push(generation);
        }
        let _ = runtime.events.send(event);
        self.spawn_eligible(repository_id, &runtime);
        Ok(result)
    }

    pub async fn check_from(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
    ) -> Result<ApproveResult, ServiceError> {
        self.check_from_with_purpose(repository_id, revision, worktree_path, command_id, false)
            .await
    }

    async fn check_from_with_purpose(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
        bootstrap: bool,
    ) -> Result<ApproveResult, ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let _mutation = runtime.mutation.lock().await;
        let requested_revision = revision.clone();
        let requested_worktree = worktree_path.clone();
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
            &tokio::fs::read_to_string(runtime.git.common_dir.join("tollgate/config.toml")).await?,
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
        let probe = approval_git.probe_check(&revision).await?;
        let request_digest = command_digest(&serde_json::json!({
            "repository_id": repository_id,
            "kind": QueueItemKind::IndependentCheck,
            "revision": requested_revision,
            "worktree_path": requested_worktree,
            "bootstrap": bootstrap,
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
                purpose: Some(if bootstrap { "bootstrap" } else { "check" }.into()),
            },
            state: QueueItemState::Constructing,
            terminal_reason: None,
            remote_state: RemoteState::Disabled,
            cleanup_state: CleanupState::NotEligible,
            dependencies: Vec::new(),
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
            data.generations.push(generation);
        }
        let _ = runtime.events.send(event);
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
        let observed_local = match runtime.git.master_oid().await {
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
                "master moved while pull held the repository mutation boundary".into(),
            ));
        }

        let mut next_state = state.clone();
        let mut affected = Vec::new();
        let (action, message) = if let Some(remote_oid) = remote.as_ref() {
            if remote_oid == &observed_local {
                (
                    RemoteSyncAction::UpToDate,
                    "Local and remote master already match exactly.".to_owned(),
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
                        .compare_and_swap_master(&observed_local, remote_oid)
                        .await
                    {
                        let current = runtime.git.master_oid().await.ok();
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
                            "remote-diverged" | "external-master-movement" | "remote-missing"
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
                            "Local master is ahead by a non-divergent certified chain.".to_owned(),
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
                        message: "Local and remote master have diverged; Tollgate did not merge or rebase them.".into(),
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
                "The configured remote branch does not exist; local master was left unchanged."
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
            message = "Remote master already equals local master.".to_owned();
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
                    "remote master is not an ancestor of local master; leased push refused".into(),
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
                    "local master contains a commit without exact Tollgate promotion evidence"
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
        let observed = runtime.git.master_oid().await?;
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
                "Adopted the observed local master as an unvalidated external base and rebuilt active prefixes."
                    .into()
            } else {
                "Confirmed the observed local master and cleared resolved reconciliation blocks."
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
            }),
        )?;
        let oid = runtime
            .git
            .create_feature_worktree(&branch, &destination)
            .await?;
        let result = WorktreeOperationResult {
            action: "created".into(),
            path: destination.to_string_lossy().into_owned(),
            branch: Some(branch),
            old_oid: None,
            new_oid: Some(oid.clone()),
            message: format!(
                "Created a feature worktree from gated master {}.",
                oid.short()
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
        let path = std::fs::canonicalize(path)?;
        let worktree = GitRepository::discover(&path).await?;
        if worktree.common_dir != runtime.git.common_dir {
            return Err(ServiceError::Invariant(
                "worktree belongs to a different registered repository".into(),
            ));
        }
        let branch = worktree.current_branch().await?.ok_or_else(|| {
            ServiceError::Invariant("refusing to remove a detached worktree".into())
        })?;
        let oid = worktree.resolve_oid("HEAD").await?;
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
                "Feature commit already has current gated master as its parent.".into()
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
        let (source, kind) = {
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
            (item.source_oid.to_hex(), item.kind)
        };
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
                    "child_command_id": child_command_id,
                }),
            )?;
            child_command_id
        };
        let source_oid = runtime.git.resolve_oid(&source).await?;
        if cold {
            runtime.cold_sources.lock().insert(source_oid.clone());
        }
        let result = if kind == QueueItemKind::IndependentCheck {
            self.check_from(repository_id, source, None, child_command_id)
                .await
        } else {
            self.approve(repository_id, source, child_command_id).await
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
        let base_sequence = current
            .iter()
            .map(|item| item.enqueue_sequence)
            .min()
            .unwrap_or(1);
        for (index, item) in ordered.iter_mut().enumerate() {
            item.enqueue_sequence = base_sequence + index as u64;
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
            data.generations.extend(generations);
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
        let path = runtime.git.common_dir.join("tollgate/config.toml");
        let candidate = EffectiveConfig::parse(&tokio::fs::read_to_string(path).await?)?;
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
                runtime.store.replace_generation(&generation)?;
                runtime.data.lock().generations.push(generation);
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
                runtime.data.lock().generations.push(generation);
                self.replace_item(&runtime, item)?;
            }
            self.spawn_eligible(repository_id, &runtime);
        }
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
            &tokio::fs::read_to_string(runtime.git.common_dir.join("tollgate/config.toml")).await?,
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
        let observed_master = runtime.git.master_oid().await?;
        let master_healthy = observed_master == state.master_oid;
        push(
            "Authoritative master",
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
                "Recorded master is reachable in the isolated execution mirror.".into()
            } else {
                "Recorded master is not provable in the execution mirror.".into()
            },
            (!mirror_healthy).then(|| {
                "Recreate the mirror from authoritative retained refs before running CI.".into()
            }),
        );
        let config_on_disk = EffectiveConfig::parse(
            &tokio::fs::read_to_string(runtime.git.common_dir.join("tollgate/config.toml")).await?,
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
        let path = runtime.git.common_dir.join("tollgate/config.toml");
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
            tokio::fs::read_to_string(runtime.git.common_dir.join("tollgate/config.toml")).await?;
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
        let cache_policy_digest = command_digest(&config.cache)?;
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
                let seed = (!cold)
                    .then(|| {
                        data.seeds
                            .iter()
                            .filter(|seed| {
                                seed.state == "published"
                                    && seed.repository_id == repository_id
                                    && seed
                                        .manifest
                                        .get("cache_epoch")
                                        .and_then(serde_json::Value::as_u64)
                                        == Some(config.cache.epoch)
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
                            .cloned()
                    })
                    .flatten();
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
        let generation_is_current = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .find(|candidate| candidate.id == item_id)
                .is_some_and(|candidate| {
                    candidate.current_generation_id == Some(generation.id)
                        && candidate.state == QueueItemState::Running
                })
        };
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
                    if bootstrap {
                        "baseline-failing"
                    } else {
                        "independent-check-failed"
                    }
                    .into(),
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
            item.terminal_reason = Some("voting-validation-failed".into());
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
                Err(GitError::Unmergeable) => {
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
            runtime.store.replace_generation(&generation)?;
            runtime.data.lock().generations.push(generation);
            self.replace_item(runtime, item)?;
        }
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
                data.generations.push(generation);
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
                &tokio::fs::read_to_string(runtime.git.common_dir.join("tollgate/config.toml"))
                    .await?,
            )?;
            let observed_master = runtime.git.master_oid().await?;
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
                            .compare_and_swap_master(&observed_master, remote_oid)
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
                "authoritative master update",
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
                .compare_and_swap_master(&observed_master, &certificate.tested_oid)
                .await?;
            item.state = item
                .state
                .transition(if config.remote.enabled {
                    ItemEvent::PromotedWithPush
                } else {
                    ItemEvent::PromotedWithoutPush
                })
                .map_err(|error| ServiceError::Invariant(error.to_string()))?;
            item.cleanup_state = CleanupState::Pending;
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
            self.spawn_eligible(repository_id, &runtime);
        }
    }

    async fn finish_source_cleanup(
        &self,
        runtime: &Arc<RepositoryRuntime>,
        mut item: QueueItem,
    ) -> Result<(), ServiceError> {
        let Some(path) = item.metadata.worktree_path.clone() else {
            item.cleanup_state = CleanupState::NotEligible;
            return self.replace_item(runtime, item);
        };
        let Some(branch) = item.metadata.branch.clone() else {
            item.cleanup_state = CleanupState::NotEligible;
            return self.replace_item(runtime, item);
        };
        let command_id = CommandId::new();
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

    fn git(directory: &Path, args: &[&str]) -> String {
        let output = StdCommand::new("git")
            .current_dir(directory)
            .args(args)
            .env("GIT_AUTHOR_NAME", "Tollgate Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Tollgate Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .output()
            .unwrap();
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
        assert!(initialized.checks.is_empty());
        assert_eq!(initialized.configuration.steps.len(), 1);
        assert!(initialized.configuration.steps[0].voting);
        assert!(matches!(
            &initialized.configuration.steps[0].command,
            tollgate_config::EffectiveCommand::Shell { script } if script == "false"
        ));
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
    async fn retry_uuid_covers_the_source_item_and_cold_policy() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(&repository, &["switch", "-c", "feature"]);
        std::fs::write(repository.join("feature.txt"), "feature\n").unwrap();
        git(&repository, &["add", "feature.txt"]);
        git(&repository, &["commit", "-m", "feature"]);
        let service = TollgateService::open(temporary.path().join("support"))
            .await
            .unwrap();
        let initialized = service
            .initialize_repository_with_options(&repository, Some("false".into()), false)
            .await
            .unwrap();
        let first = service
            .approve(initialized.state.id, "feature".into(), CommandId::new())
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
        let path = runtime.git.common_dir.join("tollgate/config.toml");
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
    async fn approves_validates_certifies_and_promotes_an_exact_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let feature_worktree = temporary.path().join("feature");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
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
            repository.join(".git/tollgate/config.toml"),
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
        let promoted = git(&repository, &["rev-parse", "master"]);
        let parent = git(&repository, &["show", "-s", "--format=%P", "master"]);
        assert_eq!(promoted, result.tested_oid.to_hex());
        assert_eq!(parent, old_master);
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
    async fn restart_completes_a_promotion_only_when_master_matches_the_intent() {
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
            .compare_and_swap_master(&old_master, &certificate.tested_oid)
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
        assert_eq!(git(&repository, &["rev-parse", "master"]), remote_tip);
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
        let local = git(&repository, &["rev-parse", "master"]);
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
        let master = git(&repository, &["rev-parse", "master"]);
        assert_eq!(
            master, source_b,
            "independent B should land directly on the unchanged base"
        );
        assert_eq!(
            git(&repository, &["show", "-s", "--format=%P", "master"]),
            base
        );
        let contains_a = StdCommand::new("git")
            .current_dir(&repository)
            .args(["merge-base", "--is-ancestor", &source_a, "master"])
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
            git(&repository, &["show", "-s", "--format=%P", "master"]),
            source_a
        );
        assert_eq!(
            git(&repository, &["rev-parse", "master"]),
            b.tested_oid.to_hex()
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
            .approve(initialized.state.id, "feature-b".into(), CommandId::new())
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
        assert_eq!(git(&repository, &["rev-parse", "master"]), base);
    }
}
