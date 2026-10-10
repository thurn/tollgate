#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use time::OffsetDateTime;
use tollgate_domain::{
    Actor, Buildset, CommandId, DomainEvent, EventId, GitOid, PassCertificate, QueueItem,
    RepositoryId, RepositoryState, StepAttemptId, StepId, ValidationGeneration,
};

const SCHEMA_VERSION: i64 = 4;
pub type PromotionEdge = (Vec<u8>, Vec<u8>);

/// Operation-intent kind for one batch of artifact pruning.
pub const ARTIFACT_PRUNE_BATCH_KIND: &str = "artifact-prune-batch";

/// Operation-intent kind a `staging` promotion records when release-stage steps exist: the
/// durable `release.run-requested` intent the release trigger settles.
pub const RELEASE_RUN_INTENT_KIND: &str = "release-run";

/// Operation-intent kind of one `release` advance (technical-design.md 10.9): the release
/// intent (expected old `release`, new OID, release certificate) together with the frozen push
/// of the new OID when pushing is enabled. It is `prepared` before the local compare-and-swap,
/// `external-applied` once `release` advanced locally while the push is still owed, `completed`
/// once the advance (and push) finished, and `needs-attention` while push-blocked.
pub const RELEASE_ADVANCE_INTENT_KIND: &str = "release-advance";

/// What one release trigger did with the outstanding release-run intents.
pub struct ReleaseTrigger<'a> {
    /// The repository projection to persist.
    pub state: &'a RepositoryState,
    /// Release runs retired before queueing, each with its terminal projection.
    pub retired: &'a [QueueItem],
    /// The release run queued for the newest `staging` tip, with its generation.
    pub queued: Option<(&'a QueueItem, &'a ValidationGeneration)>,
    /// Why the trigger settled the intents, recorded as their observed evidence.
    pub outcome: serde_json::Value,
    /// The command whose result commits with the trigger (`tg release retry`), so a replay of
    /// its command ID after a crash returns the run it queued instead of queueing another.
    pub command: Option<ReleaseTriggerCommand<'a>>,
}

/// A command result recorded in the same transaction as a release trigger.
pub struct ReleaseTriggerCommand<'a> {
    pub command_id: CommandId,
    pub command_kind: &'a str,
    pub request_digest: &'a str,
    pub response: serde_json::Value,
}

const ARTIFACT_COLUMNS: &str = "artifact_id, buildset_id, source_path, retained_path, hash, size, retention_state, created_at, expires_at";
type ArtifactRow = (
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    String,
    String,
);

#[derive(Clone, Debug, Serialize)]
pub struct StepAttemptRecord {
    pub step_id: StepId,
    pub attempt_id: StepAttemptId,
    pub name: String,
    pub frozen: serde_json::Value,
    pub retry_number: u16,
    pub result_class: String,
    pub result: serde_json::Value,
    pub stdout_end: u64,
    pub stderr_end: u64,
    pub broker_sequence_end: u64,
    pub log_hash: String,
    pub log_path: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SqliteStorageStats {
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_pages: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BackupCopyStats {
    pub page_size: u64,
    pub pages_copied: u64,
    pub source_freelist_pages: u64,
    pub bytes_copied: u64,
    pub successful_steps: u64,
    pub contention_waits: u64,
    pub copy_ms: u64,
    pub integrity_check_ms: u64,
}

struct BackupStepStats {
    pages_copied: u64,
    successful_steps: u64,
    contention_waits: u64,
    copy_ms: u64,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("database integrity check failed: {0}")]
    Integrity(String),
    #[error("queue revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("repository state has not been initialized")]
    RepositoryMissing,
    #[error("SQLite {actual} is too old; Tollgate requires 3.51.3 or newer")]
    SqliteTooOld { actual: String },
    #[error("artifact is too large to record in SQLite: {0} bytes")]
    ArtifactTooLarge(u64),
    #[error("command UUID was replayed with a different kind or payload")]
    CommandReplayMismatch,
    #[error("command UUID {0} already names a recorded operation")]
    CommandAlreadyRecorded(String),
    #[error("another {kind} operation is already in progress")]
    OperationInProgress { kind: String },
    #[error("database schema version {actual} is newer than this Tollgate supports ({supported})")]
    SchemaTooNew { actual: i64, supported: i64 },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct RepositoryStore {
    connection: Arc<Mutex<Connection>>,
    /// Whether the active configuration has release-stage steps, which lets `release` trail
    /// `staging`. The service sets it; persisted projections are checked against it.
    release_stage: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntentState {
    Prepared,
    ExternalApplied,
    Completed,
    Canceled,
    NeedsAttention,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
pub struct SeedRecord {
    pub id: String,
    pub repository_id: RepositoryId,
    pub profile: String,
    pub generation: u64,
    pub path: String,
    pub logical_size: u64,
    pub state: String,
    pub manifest: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub buildset_id: tollgate_domain::BuildsetId,
    pub source_path: String,
    pub retained_path: String,
    pub hash: String,
    pub size: u64,
    pub retention_state: String,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

/// An artifact whose pruning failed. Automatic pruning skips it until a later explicit prune
/// of that artifact succeeds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
pub struct ArtifactAttention {
    pub artifact_id: String,
    pub detail: String,
}

impl IntentState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::ExternalApplied => "external-applied",
            Self::Completed => "completed",
            Self::Canceled => "canceled",
            Self::NeedsAttention => "needs-attention",
        }
    }
}

impl RepositoryStore {
    pub fn migration_allowance(path: impl AsRef<Path>) -> Result<u64, StoreError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(0);
        }
        let connection = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version == SCHEMA_VERSION {
            return Ok(0);
        }
        let database_bytes = std::fs::metadata(path)?.len();
        // Keep room for the crash-recovery backup and, before version 3, SQLite's VACUUM
        // rebuild. In the worst case neither is smaller than the source database.
        let copies = if version < 3 { 2 } else { 1 };
        Ok(database_bytes
            .saturating_mul(copies)
            .saturating_add(512 * 1024 * 1024))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_owned();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                StoreError::Sql(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
        }
        let connection = Connection::open(&path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        verify_sqlite_version(&connection)?;
        migrate(&connection, Some(&path))?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
            release_stage: Arc::new(AtomicBool::new(false)),
        };
        store.quick_integrity_check()?;
        Ok(store)
    }

    pub fn integrity_check(&self) -> Result<Vec<String>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare("PRAGMA quick_check")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        migrate(&connection, None)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            release_stage: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Records whether the active configuration has release-stage steps. Without them the
    /// repository is opt-out, and every persisted projection must keep `staging` and `release`
    /// at the same OID with nothing unreleased; debug builds, and with them every test, check
    /// that after each state transition. With them, `release` may trail `staging`, and the
    /// projection must only agree with itself (`RepositoryState::release_projection_consistent`).
    pub fn set_release_stage_configured(&self, configured: bool) {
        self.release_stage.store(configured, Ordering::Release);
    }

    pub fn release_stage_configured(&self) -> bool {
        self.release_stage.load(Ordering::Acquire)
    }

    /// Encodes the repository projection for its single durable row.
    fn encode_state(&self, state: &RepositoryState) -> Result<String, StoreError> {
        debug_assert!(
            state.release_projection_consistent(),
            "release projection inconsistent: staging {}, release {}, lag {:?}, state {:?}",
            state.staging_oid,
            state.release_oid,
            state.release_lag,
            state.release_state,
        );
        debug_assert!(
            self.release_stage_configured() || state.opt_out_equivalence_holds(),
            "opt-out equivalence violated: staging {} and release {} must be equal with nothing unreleased",
            state.staging_oid,
            state.release_oid,
        );
        encode(state)
    }

    pub fn quick_integrity_check(&self) -> Result<(), StoreError> {
        let result: String = self
            .connection
            .lock()
            .query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(StoreError::Integrity(result));
        }
        Ok(())
    }

    pub fn full_integrity_check(&self) -> Result<(), StoreError> {
        let result: String =
            self.connection
                .lock()
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(StoreError::Integrity(result));
        }
        Ok(())
    }

    pub fn initialize_repository(&self, state: &RepositoryState) -> Result<(), StoreError> {
        let json = self.encode_state(state)?;
        self.connection.lock().execute(
            "INSERT INTO repository_state (repository_id, state_json, queue_revision, event_sequence, schema_version, engine_epoch, active_configuration_digest, updated_at)\n             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)\n             ON CONFLICT(repository_id) DO UPDATE SET state_json=excluded.state_json, queue_revision=excluded.queue_revision, event_sequence=excluded.event_sequence, engine_epoch=excluded.engine_epoch, active_configuration_digest=excluded.active_configuration_digest, updated_at=excluded.updated_at",
            params![state.id.to_string(), json, state.queue_revision as i64, state.event_sequence as i64, SCHEMA_VERSION, state.engine_epoch as i64, state.active_configuration_digest, now()],
        )?;
        Ok(())
    }

    pub fn initialize_repository_with_configuration(
        &self,
        state: &RepositoryState,
        canonical_bytes: &[u8],
        step_graph_digest: &str,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO repository_state (repository_id, state_json, queue_revision, event_sequence, schema_version, engine_epoch, active_configuration_digest, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![state.id.to_string(), self.encode_state(state)?, state.queue_revision as i64, state.event_sequence as i64, SCHEMA_VERSION, state.engine_epoch as i64, state.active_configuration_digest, now()],
        )?;
        transaction.execute(
            "INSERT INTO configuration_snapshots (digest, schema_version, canonical_bytes, step_graph_digest, activation_sequence, supersedes_digest) VALUES (?1, ?2, ?3, ?4, 0, NULL)",
            params![state.active_configuration_digest, SCHEMA_VERSION, canonical_bytes, step_graph_digest],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn repository_state(&self) -> Result<RepositoryState, StoreError> {
        let connection = self.connection.lock();
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM repository_state", [], |row| {
                row.get(0)
            })?;
        if count > 1 {
            return Err(StoreError::Integrity(format!(
                "repository database contains {count} competing identities"
            )));
        }
        let json: Option<String> = connection
            .query_row("SELECT state_json FROM repository_state", [], |row| {
                row.get(0)
            })
            .optional()?;
        json.map(|value| decode(&value))
            .transpose()?
            .ok_or(StoreError::RepositoryMissing)
    }

    pub fn update_repository_state(&self, state: &RepositoryState) -> Result<(), StoreError> {
        let changed = self.connection.lock().execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, engine_epoch=?5, active_configuration_digest=?6, updated_at=?7 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(state)?, state.queue_revision as i64, state.event_sequence as i64, state.engine_epoch as i64, state.active_configuration_digest, now()],
        )?;
        if changed == 0 {
            return Err(StoreError::RepositoryMissing);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn stage_configuration(
        &self,
        state: &RepositoryState,
        canonical_bytes: &[u8],
        step_graph_digest: &str,
        supersedes_digest: &str,
        command_id: CommandId,
        request_digest: &str,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT OR IGNORE INTO configuration_snapshots (digest, schema_version, canonical_bytes, step_graph_digest, activation_sequence, supersedes_digest) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![state.active_configuration_digest, SCHEMA_VERSION, canonical_bytes, step_graph_digest, state.event_sequence as i64, supersedes_digest],
        )?;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, active_configuration_digest=?5, updated_at=?6 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(state)?, state.queue_revision as i64, state.event_sequence as i64, state.active_configuration_digest, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='external-applied', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='config-apply' AND state='prepared'",
            params![state.id.to_string(), serde_json::json!({"digest": state.active_configuration_digest, "request_digest": request_digest}).to_string(), now(), command_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_configuration_snapshot(
        &self,
        digest: &str,
        canonical_bytes: &[u8],
        step_graph_digest: &str,
    ) -> Result<(), StoreError> {
        self.connection.lock().execute(
            "INSERT OR IGNORE INTO configuration_snapshots (digest, schema_version, canonical_bytes, step_graph_digest, activation_sequence, supersedes_digest) VALUES (?1, ?2, ?3, ?4, 0, NULL)",
            params![digest, SCHEMA_VERSION, canonical_bytes, step_graph_digest],
        )?;
        Ok(())
    }

    pub fn configuration_snapshot(
        &self,
        digest: &str,
    ) -> Result<Option<(Vec<u8>, String)>, StoreError> {
        self.connection
            .lock()
            .query_row(
                "SELECT canonical_bytes, step_graph_digest FROM configuration_snapshots WHERE digest=?1",
                [digest],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn prepare_approval(
        &self,
        repository_id: RepositoryId,
        item: &QueueItem,
        command_id: CommandId,
        request_digest: &str,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT state, expected_json FROM operation_intents WHERE command_id=?1 ORDER BY created_at DESC LIMIT 1",
                [command_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((state, expected)) = existing {
            let value: serde_json::Value = decode(&expected)?;
            let existing_digest = value
                .get("request_digest")
                .and_then(serde_json::Value::as_str);
            if existing_digest != Some(request_digest) || state != "canceled" {
                return Err(StoreError::CommandReplayMismatch);
            }
        }
        transaction.execute(
            "INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, created_at, updated_at) VALUES (?1, ?2, 'approval', 'prepared', ?3, ?4, ?5, ?5)",
            params![uuid::Uuid::now_v7().to_string(), repository_id.to_string(), command_id.to_string(), encode(&serde_json::json!({"item": item, "request_digest": request_digest}))?, now()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn unfinished_approvals(&self) -> Result<Vec<(CommandId, QueueItem, String)>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT command_id, expected_json FROM operation_intents WHERE kind='approval' AND state='prepared' ORDER BY created_at",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.map(|row| {
            let (command_id, expected) = row?;
            let command_id = command_id.parse().map_err(|error| {
                StoreError::Integrity(format!("invalid approval command ID: {error}"))
            })?;
            let value: serde_json::Value = decode(&expected)?;
            let (item, request_digest) = if let Some(item) = value.get("item") {
                (
                    serde_json::from_value(item.clone())?,
                    value
                        .get("request_digest")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            StoreError::Integrity("approval intent omitted request digest".into())
                        })?
                        .to_owned(),
                )
            } else {
                let item: QueueItem = serde_json::from_value(value)?;
                let request_digest = blake3::hash(&serde_json::to_vec(&item)?)
                    .to_hex()
                    .to_string();
                (item, request_digest)
            };
            Ok((command_id, item, request_digest))
        })
        .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_approval(
        &self,
        item: &QueueItem,
        generation: Option<&ValidationGeneration>,
        expected_revision: u64,
        actor: Actor,
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
        response: &impl Serialize,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (revision, sequence, state_json): (i64, i64, String) = transaction.query_row(
            "SELECT queue_revision, event_sequence, state_json FROM repository_state WHERE repository_id=?1",
            [item.repository_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if revision as u64 != expected_revision {
            return Err(StoreError::RevisionConflict {
                expected: expected_revision,
                actual: revision as u64,
            });
        }
        let new_revision = revision + 1;
        let new_sequence = sequence + 1;
        transaction.execute(
            "INSERT INTO queue_items (item_id, repository_id, source_format, source_oid, enqueue_sequence, state, remote_state, cleanup_state, current_generation_id, item_json, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![item.id.to_string(), item.repository_id.to_string(), format!("{:?}", item.source_oid.format).to_lowercase(), item.source_oid.as_bytes(), item.enqueue_sequence as i64, enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, item.current_generation_id.map(|id| id.to_string()), encode(item)?, (!item.state.is_terminal()) as i64],
        )?;
        for dependency in &item.dependencies {
            transaction.execute(
                "INSERT INTO item_dependencies (item_id, dependency_item_id) VALUES (?1, ?2)",
                params![item.id.to_string(), dependency.to_string()],
            )?;
        }
        if let Some(generation) = generation {
            transaction.execute(
                "INSERT INTO validation_generations (generation_id, item_id, identity_digest, tested_format, tested_oid, expected_parent_format, expected_parent_oid, configuration_digest, step_graph_digest, engine_epoch, generation_json, current) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
                params![generation.id.to_string(), generation.item_id.to_string(), generation.identity_digest, format!("{:?}", generation.tested_oid.format).to_lowercase(), generation.tested_oid.as_bytes(), format!("{:?}", generation.expected_parent_oid.format).to_lowercase(), generation.expected_parent_oid.as_bytes(), generation.configuration_digest, generation.step_graph_digest, generation.engine_epoch as i64, encode(generation)?],
            )?;
        }
        let mut persisted_state: RepositoryState = decode(&state_json)?;
        persisted_state.queue_revision = new_revision as u64;
        persisted_state.event_sequence = new_sequence as u64;
        transaction.execute("UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1", params![item.repository_id.to_string(), self.encode_state(&persisted_state)?, new_revision, new_sequence, now()])?;
        transaction.execute("UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='approval' AND state='prepared'", params![item.repository_id.to_string(), encode(item)?, now(), command_id.to_string()])?;
        transaction.execute("UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)", [command_id.to_string()])?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: item.repository_id,
            sequence: new_sequence as u64,
            actor,
            command_id: Some(command_id),
            kind: if command_kind == "candidate" {
                "candidate.created".into()
            } else {
                "queue.item-enqueued".into()
            },
            payload: serde_json::to_value(item)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute("INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![command_id.to_string(), command_kind, request_digest, encode(response)?, new_sequence, now()])?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn complete_check(
        &self,
        item: &QueueItem,
        generation: &ValidationGeneration,
        actor: Actor,
        command_id: CommandId,
        request_digest: &str,
        response: &impl Serialize,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (revision, sequence, state_json): (i64, i64, String) = transaction.query_row(
            "SELECT queue_revision, event_sequence, state_json FROM repository_state WHERE repository_id=?1",
            [item.repository_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let new_sequence = sequence + 1;
        transaction.execute(
            "INSERT INTO queue_items (item_id, repository_id, source_format, source_oid, enqueue_sequence, state, remote_state, cleanup_state, current_generation_id, item_json, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 1)",
            params![item.id.to_string(), item.repository_id.to_string(), format!("{:?}", item.source_oid.format).to_lowercase(), item.source_oid.as_bytes(), item.enqueue_sequence as i64, enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, generation.id.to_string(), encode(item)?],
        )?;
        transaction.execute(
            "INSERT INTO validation_generations (generation_id, item_id, identity_digest, tested_format, tested_oid, expected_parent_format, expected_parent_oid, configuration_digest, step_graph_digest, engine_epoch, generation_json, current) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
            params![generation.id.to_string(), generation.item_id.to_string(), generation.identity_digest, format!("{:?}", generation.tested_oid.format).to_lowercase(), generation.tested_oid.as_bytes(), format!("{:?}", generation.expected_parent_oid.format).to_lowercase(), generation.expected_parent_oid.as_bytes(), generation.configuration_digest, generation.step_graph_digest, generation.engine_epoch as i64, encode(generation)?],
        )?;
        let mut persisted_state: RepositoryState = decode(&state_json)?;
        persisted_state.event_sequence = new_sequence as u64;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1",
            params![item.repository_id.to_string(), self.encode_state(&persisted_state)?, revision, new_sequence, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='approval' AND state='prepared'",
            params![item.repository_id.to_string(), encode(item)?, now(), command_id.to_string()],
        )?;
        transaction.execute("UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)", [command_id.to_string()])?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: item.repository_id,
            sequence: new_sequence as u64,
            actor,
            command_id: Some(command_id),
            kind: "check.started".into(),
            payload: serde_json::to_value(item)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, 'check', ?2, ?3, ?4, ?5)",
            params![command_id.to_string(), request_digest, encode(response)?, new_sequence, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn queue_items(&self) -> Result<Vec<QueueItem>, StoreError> {
        let connection = self.connection.lock();
        let mut statement =
            connection.prepare("SELECT item_json FROM queue_items ORDER BY enqueue_sequence")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| decode(&row?)).collect()
    }

    pub fn generations(&self) -> Result<Vec<ValidationGeneration>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection
            .prepare("SELECT generation_json FROM validation_generations ORDER BY rowid")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| decode(&row?)).collect()
    }

    pub fn replace_generation(&self, generation: &ValidationGeneration) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        invalidate_current_generation(&transaction, generation)?;
        transaction.execute(
            "INSERT INTO validation_generations (generation_id, item_id, identity_digest, tested_format, tested_oid, expected_parent_format, expected_parent_oid, configuration_digest, step_graph_digest, engine_epoch, generation_json, current) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
            params![generation.id.to_string(), generation.item_id.to_string(), generation.identity_digest, format!("{:?}", generation.tested_oid.format).to_lowercase(), generation.tested_oid.as_bytes(), format!("{:?}", generation.expected_parent_oid.format).to_lowercase(), generation.expected_parent_oid.as_bytes(), generation.configuration_digest, generation.step_graph_digest, generation.engine_epoch as i64, encode(generation)?],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn save_item_projection(
        &self,
        state: &RepositoryState,
        item: &QueueItem,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let persisted_sequence: i64 = transaction.query_row(
            "SELECT event_sequence FROM repository_state WHERE repository_id=?1",
            [state.id.to_string()],
            |row| row.get(0),
        )?;
        let sequence =
            state
                .event_sequence
                .max(u64::try_from(persisted_sequence).map_err(|_| {
                    StoreError::Integrity("repository event sequence is negative".into())
                })?)
                + 1;
        transaction.execute(
            "UPDATE queue_items SET state=?2, remote_state=?3, cleanup_state=?4, current_generation_id=?5, item_json=?6, active=?7, enqueue_sequence=?8 WHERE item_id=?1",
            params![item.id.to_string(), enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, item.current_generation_id.map(|id| id.to_string()), encode(item)?, (!item.state.is_terminal()) as i64, item.enqueue_sequence as i64],
        )?;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute("UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1", params![state.id.to_string(), self.encode_state(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()])?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor: Actor::App,
            command_id: None,
            kind: "queue.item-updated".into(),
            payload: serde_json::to_value(item)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.commit()?;
        Ok(event)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn authorize_candidate(
        &self,
        state: &RepositoryState,
        items: &[QueueItem],
        generations: &[ValidationGeneration],
        restored_generations: &[ValidationGeneration],
        expected_revision: u64,
        command_id: CommandId,
        request_digest: &str,
        response: &impl Serialize,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (revision, sequence): (i64, i64) = transaction.query_row(
            "SELECT queue_revision, event_sequence FROM repository_state WHERE repository_id=?1",
            [state.id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if revision as u64 != expected_revision {
            return Err(StoreError::Integrity(format!(
                "candidate authorization revision changed from {expected_revision} to {revision}"
            )));
        }
        let new_revision = revision + 1;
        let new_sequence = sequence + 1;
        if items.is_empty() {
            return Err(StoreError::Integrity(
                "candidate authorization did not include any active items".into(),
            ));
        }
        for generation in generations {
            invalidate_current_generation(&transaction, generation)?;
            transaction.execute(
                "INSERT INTO validation_generations (generation_id, item_id, identity_digest, tested_format, tested_oid, expected_parent_format, expected_parent_oid, configuration_digest, step_graph_digest, engine_epoch, generation_json, current) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
                params![generation.id.to_string(), generation.item_id.to_string(), generation.identity_digest, format!("{:?}", generation.tested_oid.format).to_lowercase(), generation.tested_oid.as_bytes(), format!("{:?}", generation.expected_parent_oid.format).to_lowercase(), generation.expected_parent_oid.as_bytes(), generation.configuration_digest, generation.step_graph_digest, generation.engine_epoch as i64, encode(generation)?],
            )?;
        }
        for generation in restored_generations {
            activate_retained_generation(&transaction, generation)?;
        }
        for (index, item) in items.iter().enumerate() {
            transaction.execute(
                "UPDATE queue_items SET enqueue_sequence=?2 WHERE item_id=?1",
                params![item.id.to_string(), -(index as i64) - 1],
            )?;
        }
        for item in items {
            let changed = transaction.execute(
                "UPDATE queue_items SET state=?2, remote_state=?3, cleanup_state=?4, current_generation_id=?5, item_json=?6, active=?7, enqueue_sequence=?8 WHERE item_id=?1 AND active=1",
                params![item.id.to_string(), enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, item.current_generation_id.map(|id| id.to_string()), encode(item)?, (!item.state.is_terminal()) as i64, item.enqueue_sequence as i64],
            )?;
            if changed != 1 {
                return Err(StoreError::Integrity(format!(
                    "candidate authorization did not update active item {} exactly once",
                    item.id
                )));
            }
        }
        let mut persisted_state = state.clone();
        persisted_state.queue_revision = new_revision as u64;
        persisted_state.event_sequence = new_sequence as u64;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, new_revision, new_sequence, now()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence: new_sequence as u64,
            actor: Actor::Cli,
            command_id: Some(command_id),
            kind: "candidate.promotion-authorized".into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, 'candidate-authorize', ?2, ?3, ?4, ?5)",
            params![command_id.to_string(), request_digest, encode(response)?, new_sequence, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replace_queue_structure(
        &self,
        state: &RepositoryState,
        items: &[QueueItem],
        generations: &[ValidationGeneration],
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
        response: &impl Serialize,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sequence = state.event_sequence + 1;
        for generation in generations {
            invalidate_current_generation(&transaction, generation)?;
            transaction.execute(
                "INSERT INTO validation_generations (generation_id, item_id, identity_digest, tested_format, tested_oid, expected_parent_format, expected_parent_oid, configuration_digest, step_graph_digest, engine_epoch, generation_json, current) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
                params![generation.id.to_string(), generation.item_id.to_string(), generation.identity_digest, format!("{:?}", generation.tested_oid.format).to_lowercase(), generation.tested_oid.as_bytes(), format!("{:?}", generation.expected_parent_oid.format).to_lowercase(), generation.expected_parent_oid.as_bytes(), generation.configuration_digest, generation.step_graph_digest, generation.engine_epoch as i64, encode(generation)?],
            )?;
        }
        for (index, item) in items.iter().enumerate() {
            transaction.execute(
                "UPDATE queue_items SET enqueue_sequence=?2 WHERE item_id=?1",
                params![item.id.to_string(), -(index as i64) - 1],
            )?;
        }
        for item in items {
            transaction.execute(
                "UPDATE queue_items SET state=?2, remote_state=?3, cleanup_state=?4, current_generation_id=?5, item_json=?6, active=?7, enqueue_sequence=?8 WHERE item_id=?1",
                params![item.id.to_string(), enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, item.current_generation_id.map(|id| id.to_string()), encode(item)?, (!item.state.is_terminal()) as i64, item.enqueue_sequence as i64],
            )?;
        }
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor: Actor::Ui,
            command_id: Some(command_id),
            kind: "queue.reordered".into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![command_id.to_string(), command_kind, request_digest, encode(response)?, sequence as i64, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn insert_buildset(&self, buildset: &Buildset) -> Result<(), StoreError> {
        self.connection.lock().execute(
            "INSERT INTO buildsets (buildset_id, item_id, generation_id, tested_format, tested_oid, expected_parent_oid, environment_fingerprint, slot_id, status, retry_of_buildset_id, attempt, buildset_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![buildset.id.to_string(), buildset.item_id.to_string(), buildset.validation_generation_id.to_string(), format!("{:?}", buildset.tested_oid.format).to_lowercase(), buildset.tested_oid.as_bytes(), buildset.expected_parent_oid.as_bytes(), buildset.environment_fingerprint, buildset.slot_id.map(|id| id.to_string()), enum_json(&buildset.state)?, buildset.retry_of.map(|id| id.to_string()), buildset.attempt as i64, encode(buildset)?],
        )?;
        Ok(())
    }

    pub fn update_buildset(&self, buildset: &Buildset) -> Result<(), StoreError> {
        self.connection.lock().execute(
            "UPDATE buildsets SET slot_id=?2, status=?3, buildset_json=?4 WHERE buildset_id=?1",
            params![
                buildset.id.to_string(),
                buildset.slot_id.map(|id| id.to_string()),
                enum_json(&buildset.state)?,
                encode(buildset)?
            ],
        )?;
        Ok(())
    }

    pub fn record_step_attempts(
        &self,
        buildset_id: tollgate_domain::BuildsetId,
        attempts: &[StepAttemptRecord],
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for attempt in attempts {
            transaction.execute(
                "INSERT OR REPLACE INTO steps (step_id,buildset_id,name,frozen_json) VALUES (?1,?2,?3,?4)",
                params![attempt.step_id.to_string(), buildset_id.to_string(), attempt.name, encode(&attempt.frozen)?],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO step_attempts (attempt_id,step_id,retry_number,result_class,attempt_json) VALUES (?1,?2,?3,?4,?5)",
                params![attempt.attempt_id.to_string(), attempt.step_id.to_string(), i64::from(attempt.retry_number), attempt.result_class, encode(&attempt.result)?],
            )?;
            for (stream, retained_end) in [
                ("stdout", attempt.stdout_end),
                ("stderr", attempt.stderr_end),
            ] {
                let stream_id = uuid::Uuid::now_v7().to_string();
                transaction.execute(
                    "INSERT INTO log_streams (stream_id,attempt_id,stream,retained_start,retained_end,sealed_hash,state) VALUES (?1,?2,?3,0,?4,?5,'sealed')",
                    params![stream_id, attempt.attempt_id.to_string(), stream, i64::try_from(retained_end).map_err(|_| StoreError::Integrity("log offset exceeds SQLite INTEGER range".into()))?, attempt.log_hash],
                )?;
                if retained_end > 0 {
                    transaction.execute(
                        "INSERT INTO log_chunks (chunk_id,stream_id,start_offset,end_offset,broker_sequence_start,broker_sequence_end,hash,storage_path,compressed) VALUES (?1,?2,0,?3,0,?4,?5,?6,0)",
                        params![uuid::Uuid::now_v7().to_string(), stream_id, i64::try_from(retained_end).map_err(|_| StoreError::Integrity("log offset exceeds SQLite INTEGER range".into()))?, i64::try_from(attempt.broker_sequence_end).map_err(|_| StoreError::Integrity("broker sequence exceeds SQLite INTEGER range".into()))?, attempt.log_hash, attempt.log_path.to_string_lossy()],
                    )?;
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_buildset_logs_pruned(
        &self,
        buildset_id: tollgate_domain::BuildsetId,
    ) -> Result<(), StoreError> {
        self.connection.lock().execute(
            "UPDATE log_streams SET state='pruned' WHERE attempt_id IN (SELECT step_attempts.attempt_id FROM step_attempts JOIN steps ON steps.step_id=step_attempts.step_id WHERE steps.buildset_id=?1)",
            [buildset_id.to_string()],
        )?;
        Ok(())
    }

    pub fn step_log_state(
        &self,
        buildset_id: tollgate_domain::BuildsetId,
        step_name: &str,
    ) -> Result<Option<String>, StoreError> {
        Ok(self.connection.lock().query_row(
            "SELECT log_streams.state FROM log_streams JOIN step_attempts ON step_attempts.attempt_id=log_streams.attempt_id JOIN steps ON steps.step_id=step_attempts.step_id WHERE steps.buildset_id=?1 AND steps.name=?2 ORDER BY step_attempts.retry_number DESC LIMIT 1",
            params![buildset_id.to_string(), step_name],
            |row| row.get(0),
        ).optional()?)
    }

    pub fn successful_step_result(
        &self,
        buildset_id: tollgate_domain::BuildsetId,
        step_name: &str,
    ) -> Result<Option<(StepAttemptId, serde_json::Value)>, StoreError> {
        let value: Option<(String, String)> = self.connection.lock().query_row(
            "SELECT step_attempts.attempt_id, step_attempts.attempt_json FROM step_attempts JOIN steps ON steps.step_id=step_attempts.step_id WHERE steps.buildset_id=?1 AND steps.name=?2 AND step_attempts.result_class='success' ORDER BY step_attempts.retry_number DESC LIMIT 1",
            params![buildset_id.to_string(), step_name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        value
            .map(|(attempt_id, result)| {
                Ok((
                    attempt_id.parse().map_err(|error| {
                        StoreError::Integrity(format!("invalid retained step attempt ID: {error}"))
                    })?,
                    decode(&result)?,
                ))
            })
            .transpose()
    }

    pub fn record_artifact(
        &self,
        buildset_id: tollgate_domain::BuildsetId,
        source_path: &Path,
        retained_path: &Path,
        hash: &str,
        size: u64,
        expires_at: OffsetDateTime,
    ) -> Result<(), StoreError> {
        let size = i64::try_from(size).map_err(|_| StoreError::ArtifactTooLarge(size))?;
        let created_at = OffsetDateTime::now_utc();
        self.connection.lock().execute(
            "INSERT INTO artifacts (artifact_id, buildset_id, step_id, source_path, retained_path, hash, size, retention_state, created_at, expires_at) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, 'retained', ?7, ?8)",
            params![uuid::Uuid::now_v7().to_string(), buildset_id.to_string(), source_path.to_string_lossy(), retained_path.to_string_lossy(), hash, size, encode_time(created_at), encode_time(expires_at)],
        )?;
        Ok(())
    }

    pub fn complete_artifact_retention(
        &self,
        state: &RepositoryState,
        command_id: CommandId,
        records: &[ArtifactRecord],
        observed: &impl Serialize,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for record in records {
            let size = i64::try_from(record.size)
                .map_err(|_| StoreError::ArtifactTooLarge(record.size))?;
            transaction.execute(
                "INSERT OR IGNORE INTO artifacts (artifact_id, buildset_id, step_id, source_path, retained_path, hash, size, retention_state, created_at, expires_at) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![record.artifact_id, record.buildset_id.to_string(), record.source_path, record.retained_path, record.hash, size, record.retention_state, encode_time(record.created_at), encode_time(record.expires_at)],
            )?;
        }
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, event_sequence=?3, updated_at=?4 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, sequence as i64, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='artifact' AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(observed)?, now(), command_id.to_string()],
        )?;
        compact_terminal_intent(&transaction, command_id)?;
        transaction.execute(
            "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
            [command_id.to_string()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor: Actor::App,
            command_id: Some(command_id),
            kind: "artifact.published".into(),
            payload: artifact_audit_payload(records)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn retained_artifact_bytes(&self) -> Result<u64, StoreError> {
        let bytes: i64 = self.connection.lock().query_row(
            "SELECT COALESCE(SUM(size), 0) FROM artifacts WHERE retention_state='retained'",
            [],
            |row| row.get(0),
        )?;
        u64::try_from(bytes)
            .map_err(|_| StoreError::Integrity("retained artifact byte total is negative".into()))
    }

    pub fn retained_artifacts(&self) -> Result<Vec<ArtifactRecord>, StoreError> {
        self.retained_artifacts_page(0, usize::MAX)
    }

    pub fn retained_artifact_count(&self) -> Result<usize, StoreError> {
        let count: i64 = self.connection.lock().query_row(
            "SELECT COUNT(*) FROM artifacts WHERE retention_state IN ('retained','pinned')",
            [],
            |row| row.get(0),
        )?;
        usize::try_from(count)
            .map_err(|_| StoreError::Integrity("retained artifact count is negative".into()))
    }

    pub fn retained_artifacts_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ArtifactRecord>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(&format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifacts WHERE retention_state IN ('retained','pinned') ORDER BY retained_path LIMIT ?1 OFFSET ?2"
        ))?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let rows = statement.query_map(params![limit, offset], artifact_row)?;
        rows.map(|row| decode_artifact(row?)).collect()
    }

    /// The retained or pinned artifact with this ID, read by primary key.
    pub fn artifact(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, StoreError> {
        let row = self
            .connection
            .lock()
            .query_row(
                &format!(
                    "SELECT {ARTIFACT_COLUMNS} FROM artifacts WHERE artifact_id=?1 AND retention_state IN ('retained','pinned')"
                ),
                [artifact_id],
                artifact_row,
            )
            .optional()?;
        row.map(decode_artifact).transpose()
    }

    /// The artifact with this ID in any retention state, including `pruned`.
    pub fn artifact_record(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, StoreError> {
        let row = self
            .connection
            .lock()
            .query_row(
                &format!("SELECT {ARTIFACT_COLUMNS} FROM artifacts WHERE artifact_id=?1"),
                [artifact_id],
                artifact_row,
            )
            .optional()?;
        row.map(decode_artifact).transpose()
    }

    /// At most `limit` timed-retention artifacts that expired at or before `now`, oldest expiry
    /// first. Artifacts that need attention and artifacts of the excluded buildsets are skipped,
    /// so every returned artifact is eligible for automatic pruning.
    pub fn expired_artifacts(
        &self,
        now: OffsetDateTime,
        excluded_buildsets: &[tollgate_domain::BuildsetId],
        limit: usize,
    ) -> Result<Vec<ArtifactRecord>, StoreError> {
        let excluded = std::iter::repeat_n("?", excluded_buildsets.len())
            .collect::<Vec<_>>()
            .join(",");
        // Timestamps are decimal Unix nanoseconds, which have 19 digits for every date between
        // 2001 and 2286, so text order is time order and the expiry index serves this range.
        let sql = format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifacts
             WHERE retention_state='retained' AND expires_at <= ?
               AND artifact_id NOT IN (SELECT artifact_id FROM artifact_attention)
               AND buildset_id NOT IN ({excluded})
             ORDER BY expires_at LIMIT ?"
        );
        let mut values = vec![rusqlite::types::Value::Text(encode_time(now))];
        values.extend(
            excluded_buildsets
                .iter()
                .map(|buildset| rusqlite::types::Value::Text(buildset.to_string())),
        );
        values.push(rusqlite::types::Value::Integer(
            i64::try_from(limit).unwrap_or(i64::MAX),
        ));
        let connection = self.connection.lock();
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values), artifact_row)?;
        rows.map(|row| decode_artifact(row?)).collect()
    }

    /// Artifacts whose pruning failed, in the order they were recorded, plus their total count.
    pub fn artifacts_needing_attention(
        &self,
        limit: usize,
    ) -> Result<(u64, Vec<ArtifactAttention>), StoreError> {
        let connection = self.connection.lock();
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM artifact_attention", [], |row| {
                row.get(0)
            })?;
        let mut statement = connection.prepare(
            "SELECT artifact_id, detail FROM artifact_attention ORDER BY recorded_at, artifact_id LIMIT ?1",
        )?;
        let rows = statement.query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
            Ok(ArtifactAttention {
                artifact_id: row.get(0)?,
                detail: row.get(1)?,
            })
        })?;
        Ok((
            nonnegative_sqlite_count("artifact attention count", count)?,
            rows.collect::<Result<Vec<_>, _>>()?,
        ))
    }

    /// Records a prepared operation whose command ID must not already name any operation.
    /// Batched pruning acts on the filesystem immediately afterward, so a replayed command ID
    /// must fail rather than repeat external effects of an earlier intent.
    pub fn prepare_unique_operation(
        &self,
        repository_id: RepositoryId,
        kind: &str,
        command_id: CommandId,
        evidence: &impl Serialize,
    ) -> Result<(), StoreError> {
        let created = self.connection.lock().execute(
            "INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, created_at, updated_at)
             SELECT ?1, ?2, ?3, 'prepared', ?4, ?5, ?6, ?6
             WHERE NOT EXISTS (SELECT 1 FROM operation_intents WHERE command_id=?4)",
            params![uuid::Uuid::now_v7().to_string(), repository_id.to_string(), kind, command_id.to_string(), encode(evidence)?, now()],
        )?;
        if created != 1 {
            return Err(StoreError::CommandAlreadyRecorded(command_id.to_string()));
        }
        Ok(())
    }

    /// Completes one artifact pruning batch in a single transaction: every pruned artifact
    /// becomes `pruned` (clearing any earlier attention record), every failed artifact is
    /// recorded as needing attention, the batch intent completes, and one `artifact.pruned`
    /// event is appended. A replayable client command also records its result.
    #[allow(clippy::too_many_arguments)]
    pub fn complete_artifact_prune_batch<R: Serialize>(
        &self,
        state: &RepositoryState,
        command_id: CommandId,
        pruned: &[String],
        attention: &[ArtifactAttention],
        request_digest: Option<&str>,
        response: &R,
        actor: Actor,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let recorded_at = now();
        {
            let mut prune = transaction.prepare(
                "UPDATE artifacts SET retention_state='pruned' WHERE artifact_id=?1 AND retention_state IN ('retained','pinned')",
            )?;
            let mut clear =
                transaction.prepare("DELETE FROM artifact_attention WHERE artifact_id=?1")?;
            for artifact_id in pruned {
                if prune.execute([artifact_id])? != 1 {
                    return Err(StoreError::Integrity(format!(
                        "artifact {artifact_id} was not in the expected retention state"
                    )));
                }
                clear.execute([artifact_id])?;
            }
            let mut record = transaction.prepare(
                "INSERT INTO artifact_attention (artifact_id, command_id, detail, recorded_at) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(artifact_id) DO UPDATE SET command_id=excluded.command_id, detail=excluded.detail, recorded_at=excluded.recorded_at",
            )?;
            for entry in attention {
                record.execute(params![
                    entry.artifact_id,
                    command_id.to_string(),
                    entry.detail,
                    recorded_at
                ])?;
            }
        }
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, event_sequence=?3, updated_at=?4 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, sequence as i64, recorded_at],
        )?;
        let completed = transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind=?5 AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(response)?, recorded_at, command_id.to_string(), ARTIFACT_PRUNE_BATCH_KIND],
        )?;
        if completed != 1 {
            return Err(StoreError::Integrity(format!(
                "artifact pruning batch {command_id} has no unfinished intent"
            )));
        }
        compact_terminal_intent(&transaction, command_id)?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor,
            command_id: Some(command_id),
            kind: "artifact.pruned".into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        if let Some(request_digest) = request_digest {
            transaction.execute(
                "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, 'artifact-prune', ?2, ?3, ?4, ?5)",
                params![command_id.to_string(), request_digest, encode(response)?, sequence as i64, recorded_at],
            )?;
        }
        transaction.commit()?;
        Ok(event)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_artifact_state_change<R: Serialize>(
        &self,
        state: &RepositoryState,
        artifact_id: &str,
        expected_states: &[&str],
        new_state: &str,
        intent_kind: Option<&str>,
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
        response: &R,
        event_kind: &str,
        actor: Actor,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let placeholders = std::iter::repeat_n("?", expected_states.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE artifacts SET retention_state=?1 WHERE artifact_id=?2 AND retention_state IN ({placeholders})"
        );
        let mut values = vec![
            rusqlite::types::Value::Text(new_state.into()),
            rusqlite::types::Value::Text(artifact_id.into()),
        ];
        values.extend(
            expected_states
                .iter()
                .map(|state| rusqlite::types::Value::Text((*state).into())),
        );
        let changed = transaction.execute(&sql, rusqlite::params_from_iter(values))?;
        if changed != 1 {
            return Err(StoreError::Integrity(format!(
                "artifact {artifact_id} was not in the expected retention state"
            )));
        }
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, event_sequence=?3, updated_at=?4 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, sequence as i64, now()],
        )?;
        if let Some(intent_kind) = intent_kind {
            transaction.execute(
                "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind=?5 AND state IN ('prepared','external-applied')",
                params![state.id.to_string(), encode(response)?, now(), command_id.to_string(), intent_kind],
            )?;
        }
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor,
            command_id: Some(command_id),
            kind: event_kind.into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![command_id.to_string(), command_kind, request_digest, encode(response)?, sequence as i64, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn record_remote_observation(
        &self,
        repository_id: RepositoryId,
        command_id: CommandId,
        remote_identity: &str,
        exact_ref: &str,
        oid: Option<&GitOid>,
        method: &str,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent_id: String = transaction.query_row(
            "SELECT intent_id FROM operation_intents WHERE repository_id=?1 AND command_id=?2 AND state IN ('prepared','external-applied') ORDER BY created_at DESC LIMIT 1",
            params![repository_id.to_string(), command_id.to_string()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE operation_intents SET observed_json=?2, updated_at=?3 WHERE intent_id=?1",
            params![
                intent_id,
                encode(&serde_json::json!({"observed_remote_oid": oid}))?,
                now()
            ],
        )?;
        transaction.execute(
            "INSERT INTO remote_observations (observation_id, repository_id, remote_identity, exact_ref, oid, method, observed_at, intent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![uuid::Uuid::now_v7().to_string(), repository_id.to_string(), remote_identity, exact_ref, oid.map(GitOid::as_bytes), method, now(), intent_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_seed(&self, seed: &SeedRecord) -> Result<(), StoreError> {
        let generation = i64::try_from(seed.generation)
            .map_err(|_| StoreError::Integrity("seed generation exceeds SQLite range".into()))?;
        let logical_size = i64::try_from(seed.logical_size)
            .map_err(|_| StoreError::ArtifactTooLarge(seed.logical_size))?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO seed_generations (seed_id, repository_id, profile, generation, ownership_path, logical_size, state, manifest_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![seed.id, seed.repository_id.to_string(), seed.profile, generation, seed.path, logical_size, seed.state, encode(seed)?],
        )?;
        let manifest_hash = blake3::hash(&serde_json::to_vec(&seed.manifest)?)
            .to_hex()
            .to_string();
        let entry_count = seed
            .manifest
            .get("entries")
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len) as i64;
        transaction.execute(
            "INSERT INTO cache_manifests (seed_id, hash, entry_count) VALUES (?1, ?2, ?3)",
            params![seed.id, manifest_hash, entry_count],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_seed_publication<R: Serialize>(
        &self,
        state: &RepositoryState,
        command_id: CommandId,
        request_digest: &str,
        seed: &SeedRecord,
        response: &R,
        actor: Actor,
    ) -> Result<DomainEvent, StoreError> {
        let generation = i64::try_from(seed.generation)
            .map_err(|_| StoreError::Integrity("seed generation exceeds SQLite range".into()))?;
        let logical_size = i64::try_from(seed.logical_size)
            .map_err(|_| StoreError::ArtifactTooLarge(seed.logical_size))?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT manifest_json FROM seed_generations WHERE seed_id=?1",
                [&seed.id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if decode::<SeedRecord>(&existing)? != *seed {
                return Err(StoreError::Integrity(format!(
                    "seed {} conflicts with its recovery evidence",
                    seed.id
                )));
            }
        } else {
            transaction.execute(
                "INSERT INTO seed_generations (seed_id, repository_id, profile, generation, ownership_path, logical_size, state, manifest_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![seed.id, seed.repository_id.to_string(), seed.profile, generation, seed.path, logical_size, seed.state, encode(seed)?],
            )?;
            let manifest_hash = blake3::hash(&serde_json::to_vec(&seed.manifest)?)
                .to_hex()
                .to_string();
            let entry_count = seed
                .manifest
                .get("entries")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len) as i64;
            transaction.execute(
                "INSERT INTO cache_manifests (seed_id, hash, entry_count) VALUES (?1, ?2, ?3)",
                params![seed.id, manifest_hash, entry_count],
            )?;
        }
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, event_sequence=?3, updated_at=?4 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, sequence as i64, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='cache-snapshot' AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(seed)?, now(), command_id.to_string()],
        )?;
        compact_terminal_intent(&transaction, command_id)?;
        transaction.execute(
            "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
            [command_id.to_string()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor,
            command_id: Some(command_id),
            kind: "cache.seed-published".into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, 'cache-snapshot', ?2, ?3, ?4, ?5)",
            params![command_id.to_string(), request_digest, encode(response)?, sequence as i64, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn seed_records(&self, repository_id: RepositoryId) -> Result<Vec<SeedRecord>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT seed_id, repository_id, profile, generation, ownership_path, logical_size, state, manifest_json FROM seed_generations WHERE repository_id=?1 ORDER BY generation DESC",
        )?;
        let rows = statement.query_map([repository_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;
        rows.map(|row| {
            let (id, repository_id, profile, generation, path, logical_size, state, encoded) = row?;
            if state == "published" {
                return decode(&encoded);
            }
            Ok(SeedRecord {
                id,
                repository_id: repository_id.parse().map_err(|error| {
                    StoreError::Integrity(format!("invalid seed repository ID: {error}"))
                })?,
                profile,
                generation: u64::try_from(generation)
                    .map_err(|_| StoreError::Integrity("seed generation is negative".into()))?,
                path,
                logical_size: u64::try_from(logical_size)
                    .map_err(|_| StoreError::Integrity("seed logical size is negative".into()))?,
                state,
                manifest: serde_json::Value::Null,
            })
        })
        .collect()
    }

    pub fn mark_seed_pruned(&self, seed_id: &str) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let encoded: String = transaction.query_row(
            "SELECT manifest_json FROM seed_generations WHERE seed_id=?1",
            [seed_id],
            |row| row.get(0),
        )?;
        let mut seed: SeedRecord = decode(&encoded)?;
        seed.state = "pruned".into();
        seed.manifest = serde_json::Value::Null;
        transaction.execute(
            "UPDATE seed_generations SET state='pruned', manifest_json=?2 WHERE seed_id=?1",
            params![seed_id, encode(&seed)?],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_cache_purge<R: Serialize>(
        &self,
        state: &RepositoryState,
        command_id: CommandId,
        request_digest: &str,
        seeds: &[SeedRecord],
        response: &R,
        actor: Actor,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for expected in seeds {
            let encoded: String = transaction.query_row(
                "SELECT manifest_json FROM seed_generations WHERE seed_id=?1 AND state='published'",
                [&expected.id],
                |row| row.get(0),
            )?;
            let mut observed: SeedRecord = decode(&encoded)?;
            if observed != *expected {
                return Err(StoreError::Integrity(format!(
                    "seed {} differs from its pruning intent",
                    expected.id
                )));
            }
            observed.state = "pruned".into();
            observed.manifest = serde_json::Value::Null;
            transaction.execute(
                "UPDATE seed_generations SET state='pruned', manifest_json=?2 WHERE seed_id=?1 AND state='published'",
                params![expected.id, encode(&observed)?],
            )?;
        }
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, event_sequence=?3, updated_at=?4 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, sequence as i64, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='cache-purge' AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(response)?, now(), command_id.to_string()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor,
            command_id: Some(command_id),
            kind: "cache.purged".into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, 'cache-purge', ?2, ?3, ?4, ?5)",
            params![command_id.to_string(), request_digest, encode(response)?, sequence as i64, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn buildsets(&self) -> Result<Vec<Buildset>, StoreError> {
        let connection = self.connection.lock();
        let mut statement =
            connection.prepare("SELECT buildset_json FROM buildsets ORDER BY rowid DESC")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| decode(&row?)).collect()
    }

    pub fn insert_certificate(&self, certificate: &PassCertificate) -> Result<(), StoreError> {
        self.connection.lock().execute("INSERT INTO pass_certificates (certificate_id, buildset_id, generation_id, tested_oid, certificate_json) VALUES (?1, ?2, ?3, ?4, ?5)", params![certificate.id.to_string(), certificate.buildset_id.to_string(), certificate.validation_generation_id.to_string(), certificate.tested_oid.as_bytes(), encode(certificate)?])?;
        Ok(())
    }

    pub fn certificates(&self) -> Result<Vec<PassCertificate>, StoreError> {
        let connection = self.connection.lock();
        let mut statement =
            connection.prepare("SELECT certificate_json FROM pass_certificates ORDER BY rowid")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| decode(&row?)).collect()
    }

    pub fn prepare_promotion(
        &self,
        repository_id: RepositoryId,
        command_id: CommandId,
        evidence: &impl Serialize,
    ) -> Result<(), StoreError> {
        let expected = encode(evidence)?;
        let connection = self.connection.lock();
        let existing: Option<(String, String)> = connection
            .query_row(
                "SELECT command_id, expected_json FROM operation_intents WHERE repository_id=?1 AND kind='promotion' AND state IN ('prepared','external-applied') LIMIT 1",
                [repository_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_command, existing_expected)) = existing {
            return if existing_command == command_id.to_string() && existing_expected == expected {
                Ok(())
            } else {
                Err(StoreError::OperationInProgress {
                    kind: "promotion".into(),
                })
            };
        }
        connection.execute("INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, created_at, updated_at) VALUES (?1, ?2, 'promotion', 'prepared', ?3, ?4, ?5, ?5)", params![uuid::Uuid::now_v7().to_string(), repository_id.to_string(), command_id.to_string(), expected, now()])?;
        Ok(())
    }

    pub fn prepare_operation(
        &self,
        repository_id: RepositoryId,
        kind: &str,
        command_id: CommandId,
        evidence: &impl Serialize,
    ) -> Result<(), StoreError> {
        let expected = encode(evidence)?;
        let connection = self.connection.lock();
        let existing: Option<(String, String)> = connection
            .query_row(
                "SELECT kind, expected_json FROM operation_intents WHERE command_id=?1 ORDER BY created_at DESC LIMIT 1",
                [command_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_kind, existing_expected)) = existing {
            return if existing_kind == kind && existing_expected == expected {
                Ok(())
            } else {
                Err(StoreError::CommandReplayMismatch)
            };
        }
        connection.execute(
            "INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, created_at, updated_at) VALUES (?1, ?2, ?3, 'prepared', ?4, ?5, ?6, ?6)",
            params![uuid::Uuid::now_v7().to_string(), repository_id.to_string(), kind, command_id.to_string(), expected, now()],
        )?;
        Ok(())
    }

    pub fn operation_evidence(
        &self,
        command_id: CommandId,
        kind: &str,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        let row = self
            .connection
            .lock()
            .query_row(
                "SELECT expected_json FROM operation_intents WHERE command_id=?1 AND kind=?2 ORDER BY created_at DESC LIMIT 1",
                params![command_id.to_string(), kind],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        row.map(|value| Ok(serde_json::from_str(&value)?))
            .transpose()
    }

    /// Returns the state and observed evidence of the latest intent recorded for a command.
    pub fn operation_outcome(
        &self,
        command_id: CommandId,
    ) -> Result<Option<(IntentState, Option<serde_json::Value>)>, StoreError> {
        let row = self
            .connection
            .lock()
            .query_row(
                "SELECT state, observed_json FROM operation_intents WHERE command_id=?1 ORDER BY created_at DESC LIMIT 1",
                [command_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        row.map(|(state, observed)| {
            let state = [
                IntentState::Prepared,
                IntentState::ExternalApplied,
                IntentState::Completed,
                IntentState::Canceled,
                IntentState::NeedsAttention,
            ]
            .into_iter()
            .find(|candidate| candidate.as_str() == state)
            .ok_or_else(|| StoreError::Integrity(format!("invalid operation state {state}")))?;
            Ok((
                state,
                observed
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
            ))
        })
        .transpose()
    }

    pub fn has_command_result(&self, command_id: CommandId) -> Result<bool, StoreError> {
        Ok(self.connection.lock().query_row(
            "SELECT EXISTS(SELECT 1 FROM command_results WHERE command_id=?1)",
            [command_id.to_string()],
            |row| row.get(0),
        )?)
    }

    pub fn record_command_result<R: Serialize>(
        &self,
        repository_id: RepositoryId,
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
        response: &R,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let event_sequence: i64 = transaction.query_row(
            "SELECT event_sequence FROM repository_state WHERE repository_id=?1",
            [repository_id.to_string()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![command_id.to_string(), command_kind, request_digest, encode(response)?, event_sequence, now()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_operation<R: Serialize>(
        &self,
        state: &RepositoryState,
        intent_kind: &str,
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
        response: &R,
        event_kind: &str,
        observed: &impl Serialize,
        actor: Actor,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind=?5 AND state IN ('prepared','external-applied','needs-attention')",
            params![state.id.to_string(), encode(observed)?, now(), command_id.to_string(), intent_kind],
        )?;
        transaction.execute(
            "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
            [command_id.to_string()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor,
            command_id: Some(command_id),
            kind: event_kind.into(),
            payload: serde_json::to_value(response)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.execute(
            "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![command_id.to_string(), command_kind, request_digest, encode(response)?, sequence as i64, now()],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn unfinished_operations(
        &self,
        kinds: &[&str],
    ) -> Result<Vec<(CommandId, String, serde_json::Value, IntentState)>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT command_id, kind, expected_json, observed_json, state FROM operation_intents WHERE state IN ('prepared','external-applied') ORDER BY created_at",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (command_id, kind, expected, observed, state) = row?;
            if !kinds.contains(&kind.as_str()) {
                continue;
            }
            let command_id = command_id.parse().map_err(|error| {
                StoreError::Integrity(format!("invalid operation command ID: {error}"))
            })?;
            let state = match state.as_str() {
                "prepared" => IntentState::Prepared,
                "external-applied" => IntentState::ExternalApplied,
                other => {
                    return Err(StoreError::Integrity(format!(
                        "invalid unfinished operation state {other}"
                    )));
                }
            };
            let mut evidence: serde_json::Value = serde_json::from_str(&expected)?;
            if let Some(observed) = observed {
                let observed: serde_json::Value = serde_json::from_str(&observed)?;
                if let Some(remote_oid) = observed.get("observed_remote_oid") {
                    evidence["observed_remote_oid"] = remote_oid.clone();
                }
            }
            result.push((command_id, kind, evidence, state));
        }
        Ok(result)
    }

    pub fn recoverable_operations(
        &self,
        kinds: &[&str],
    ) -> Result<Vec<(CommandId, String, serde_json::Value, IntentState)>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT command_id, kind, expected_json, observed_json, state FROM operation_intents WHERE state IN ('prepared','external-applied','needs-attention') ORDER BY created_at",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (command_id, kind, expected, observed, state) = row?;
            if !kinds.contains(&kind.as_str()) {
                continue;
            }
            let command_id = command_id.parse().map_err(|error| {
                StoreError::Integrity(format!("invalid operation command ID: {error}"))
            })?;
            let state = match state.as_str() {
                "prepared" => IntentState::Prepared,
                "external-applied" => IntentState::ExternalApplied,
                "needs-attention" => IntentState::NeedsAttention,
                other => {
                    return Err(StoreError::Integrity(format!(
                        "invalid recoverable operation state {other}"
                    )));
                }
            };
            let mut evidence: serde_json::Value = serde_json::from_str(&expected)?;
            if let Some(observed) = observed {
                let observed: serde_json::Value = serde_json::from_str(&observed)?;
                if let Some(remote_oid) = observed.get("observed_remote_oid") {
                    evidence["observed_remote_oid"] = remote_oid.clone();
                }
            }
            result.push((command_id, kind, evidence, state));
        }
        Ok(result)
    }

    pub fn cancel_attention_intent(
        &self,
        command_id: CommandId,
        evidence: &impl Serialize,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE operation_intents SET state='canceled', observed_json=?2, updated_at=?3 WHERE command_id=?1 AND state='needs-attention'",
            params![command_id.to_string(), encode(evidence)?, now()],
        )?;
        transaction.execute(
            "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
            [command_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn unsettled_completed_operation_evidence(
        &self,
        kind: &str,
    ) -> Result<Vec<(CommandId, serde_json::Value)>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT command_id, expected_json FROM operation_intents WHERE kind=?1 AND state='completed' AND COALESCE(json_extract(observed_json, '$.cleanup'), '') <> 'complete' ORDER BY created_at",
        )?;
        let rows = statement.query_map([kind], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.map(|row| {
            let (command_id, evidence) = row?;
            Ok((
                command_id.parse().map_err(|error| {
                    StoreError::Integrity(format!("invalid operation command ID: {error}"))
                })?,
                serde_json::from_str(&evidence)?,
            ))
        })
        .collect()
    }

    pub fn mark_completed_operation_cleanup(
        &self,
        command_id: CommandId,
        kind: &str,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE operation_intents SET observed_json=json_set(COALESCE(observed_json, '{}'), '$.cleanup', 'complete'), updated_at=?3 WHERE command_id=?1 AND kind=?2 AND state='completed'",
            params![command_id.to_string(), kind, now()],
        )?;
        if changed != 1 {
            return Err(StoreError::Integrity(format!(
                "completed {kind} cleanup did not match exactly one operation"
            )));
        }
        compact_terminal_intent(&transaction, command_id)?;
        transaction.commit()?;
        if kind == "cache-purge" {
            // Cache cleanup can release hundreds of MiB of manifest and recovery payload pages.
            // Reclaim them here, off the promotion path, instead of carrying them into every
            // subsequent backup. This is crash-safe: the durable cleanup marker committed first.
            connection.execute_batch("PRAGMA incremental_vacuum")?;
        }
        Ok(())
    }

    pub fn completed_operation_records(
        &self,
        kind: &str,
    ) -> Result<Vec<(serde_json::Value, serde_json::Value)>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT expected_json, observed_json FROM operation_intents WHERE kind=?1 AND state='completed' ORDER BY created_at",
        )?;
        let rows = statement.query_map([kind], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.map(|row| {
            let (expected, observed) = row?;
            Ok((
                serde_json::from_str(&expected)?,
                serde_json::from_str(&observed)?,
            ))
        })
        .collect()
    }

    pub fn command_response_json(
        &self,
        command_id: CommandId,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        let encoded = self
            .connection
            .lock()
            .query_row(
                "SELECT response_json FROM command_results WHERE command_id=?1",
                [command_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded
            .map(|value| Ok(serde_json::from_str(&value)?))
            .transpose()
    }

    pub fn unfinished_promotion(
        &self,
    ) -> Result<Option<(CommandId, PassCertificate, IntentState)>, StoreError> {
        let row: Option<(String, String, String)> = self
            .connection
            .lock()
            .query_row(
                "SELECT command_id, expected_json, state FROM operation_intents WHERE kind='promotion' AND state IN ('prepared','external-applied') LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        row.map(|(command_id, expected, state)| {
            let command_id = command_id.parse().map_err(|error| {
                StoreError::Integrity(format!("invalid promotion command ID: {error}"))
            })?;
            let state = match state.as_str() {
                "prepared" => IntentState::Prepared,
                "external-applied" => IntentState::ExternalApplied,
                other => {
                    return Err(StoreError::Integrity(format!(
                        "invalid unfinished intent state {other}"
                    )));
                }
            };
            Ok((command_id, decode(&expected)?, state))
        })
        .transpose()
    }

    /// Completes a promotion: the source promotion edge, the promoted item, the new repository
    /// projection, and the completed promotion intent, in one transaction. A promotion in a
    /// repository with release-stage steps passes `release_run_request`, which records a durable
    /// `release-run` intent in the same transaction; the release trigger settles it outside the
    /// repository mutation lock, and recovery replays it.
    pub fn record_promotion(
        &self,
        state: &RepositoryState,
        item: &QueueItem,
        certificate: &PassCertificate,
        old_master: &[u8],
        release_run_request: Option<CommandId>,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(command_id) = release_run_request {
            transaction.execute(
                "INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, created_at, updated_at) VALUES (?1, ?2, ?3, 'prepared', ?4, ?5, ?6, ?6)",
                params![
                    uuid::Uuid::now_v7().to_string(),
                    state.id.to_string(),
                    RELEASE_RUN_INTENT_KIND,
                    command_id.to_string(),
                    encode(&serde_json::json!({
                        "staging_oid": certificate.tested_oid,
                        "promoted_item_id": item.id,
                    }))?,
                    now()
                ],
            )?;
        }
        transaction.execute("INSERT OR REPLACE INTO source_promotions (item_id, source_oid, promoted_oid, old_master_oid, certificate_id, event_sequence) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![item.id.to_string(), item.source_oid.as_bytes(), certificate.tested_oid.as_bytes(), old_master, certificate.id.to_string(), state.event_sequence as i64])?;
        transaction.execute("UPDATE queue_items SET state=?2, remote_state=?3, cleanup_state=?4, item_json=?5, active=0 WHERE item_id=?1", params![item.id.to_string(), enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, encode(item)?])?;
        transaction.execute("UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1", params![state.id.to_string(), self.encode_state(state)?, state.queue_revision as i64, state.event_sequence as i64, now()])?;
        transaction.execute("UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND kind='promotion' AND state IN ('prepared','external-applied')", params![state.id.to_string(), encode(item)?, now()])?;
        transaction.execute("UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE repository_id=?1 AND kind='promotion' AND state='completed')", [state.id.to_string()])?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence: state.event_sequence,
            actor: Actor::App,
            command_id: None,
            kind: "promotion.completed".into(),
            payload: serde_json::to_value(item)?,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.commit()?;
        Ok(())
    }

    /// Applies one release trigger in a single transaction: retires superseded release runs,
    /// queues the new one with its generation, completes every outstanding release-run intent,
    /// persists `trigger.state`, and appends one event per change: `release.run-superseded` for
    /// each retired run and `release.run-queued` for the new one. Returns the appended events;
    /// with no change, it only completes the intents.
    pub fn record_release_trigger(
        &self,
        trigger: &ReleaseTrigger<'_>,
    ) -> Result<Vec<DomainEvent>, StoreError> {
        let state = trigger.state;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let persisted_sequence: i64 = transaction.query_row(
            "SELECT event_sequence FROM repository_state WHERE repository_id=?1",
            [state.id.to_string()],
            |row| row.get(0),
        )?;
        let mut sequence =
            state
                .event_sequence
                .max(u64::try_from(persisted_sequence).map_err(|_| {
                    StoreError::Integrity("repository event sequence is negative".into())
                })?);
        let mut events = Vec::new();
        let mut push_event = |kind: &str, payload: serde_json::Value| {
            sequence += 1;
            events.push(DomainEvent {
                id: EventId::new(),
                repository_id: state.id,
                sequence,
                actor: Actor::App,
                command_id: None,
                kind: kind.into(),
                payload,
                created_at: OffsetDateTime::now_utc(),
            });
        };
        for item in trigger.retired {
            transaction.execute(
                "UPDATE queue_items SET state=?2, remote_state=?3, cleanup_state=?4, current_generation_id=?5, item_json=?6, active=?7 WHERE item_id=?1",
                params![item.id.to_string(), enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, item.current_generation_id.map(|id| id.to_string()), encode(item)?, (!item.state.is_terminal()) as i64],
            )?;
            push_event(
                "release.run-superseded",
                serde_json::json!({
                    "item": item,
                    "superseded_target": item.source_oid,
                    "reason": item.terminal_reason,
                    "outcome": trigger.outcome,
                }),
            );
        }
        if let Some((item, generation)) = trigger.queued {
            transaction.execute(
                "INSERT INTO queue_items (item_id, repository_id, source_format, source_oid, enqueue_sequence, state, remote_state, cleanup_state, current_generation_id, item_json, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 1)",
                params![item.id.to_string(), item.repository_id.to_string(), format!("{:?}", item.source_oid.format).to_lowercase(), item.source_oid.as_bytes(), item.enqueue_sequence as i64, enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, generation.id.to_string(), encode(item)?],
            )?;
            transaction.execute(
                "INSERT INTO validation_generations (generation_id, item_id, identity_digest, tested_format, tested_oid, expected_parent_format, expected_parent_oid, configuration_digest, step_graph_digest, engine_epoch, generation_json, current) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
                params![generation.id.to_string(), generation.item_id.to_string(), generation.identity_digest, format!("{:?}", generation.tested_oid.format).to_lowercase(), generation.tested_oid.as_bytes(), format!("{:?}", generation.expected_parent_oid.format).to_lowercase(), generation.expected_parent_oid.as_bytes(), generation.configuration_digest, generation.step_graph_digest, generation.engine_epoch as i64, encode(generation)?],
            )?;
            push_event(
                "release.run-queued",
                serde_json::json!({
                    "item": item,
                    "tested_oid": generation.tested_oid,
                    "range": {
                        "from": generation.anchored_base_oid,
                        "to": generation.tested_oid,
                    },
                    "outcome": trigger.outcome,
                }),
            );
        }
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND kind=?4 AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(&trigger.outcome)?, now(), RELEASE_RUN_INTENT_KIND],
        )?;
        if !events.is_empty() {
            let mut persisted_state = state.clone();
            persisted_state.event_sequence = sequence;
            transaction.execute(
                "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1",
                params![state.id.to_string(), self.encode_state(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()],
            )?;
            for event in &events {
                insert_event(&transaction, event)?;
            }
        }
        if let Some(command) = &trigger.command {
            transaction.execute(
                "INSERT INTO command_results (command_id, command_kind, request_digest, response_json, event_sequence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![command.command_id.to_string(), command.command_kind, command.request_digest, encode(&command.response)?, sequence as i64, now()],
            )?;
        }
        transaction.commit()?;
        Ok(events)
    }

    pub fn promoted_oid_bytes(&self, source_oid: &GitOid) -> Result<Option<Vec<u8>>, StoreError> {
        self.connection
            .lock()
            .query_row(
                "SELECT promoted_oid FROM source_promotions WHERE source_oid=?1 ORDER BY event_sequence DESC LIMIT 1",
                [source_oid.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn promotion_edges(&self) -> Result<Vec<PromotionEdge>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT old_master_oid, promoted_oid FROM source_promotions ORDER BY event_sequence",
        )?;
        let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Persists `state` and one event describing the change in a single transaction. The event
    /// takes the next sequence, which the returned event and the persisted state both carry.
    pub fn record_state_event(
        &self,
        state: &RepositoryState,
        actor: Actor,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = state.event_sequence + 1;
        let changed = transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, engine_epoch=?5, active_configuration_digest=?6, updated_at=?7 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, persisted_state.queue_revision as i64, persisted_state.event_sequence as i64, persisted_state.engine_epoch as i64, persisted_state.active_configuration_digest, now()],
        )?;
        if changed == 0 {
            return Err(StoreError::RepositoryMissing);
        }
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence: persisted_state.event_sequence,
            actor,
            command_id: None,
            kind: kind.into(),
            payload,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.commit()?;
        Ok(event)
    }

    /// Moves one unfinished release-advance intent to `to` and persists `state` with one event, in
    /// a single transaction. The local half of an advance completes here (`external-applied` when
    /// a frozen push is still owed, `completed` otherwise), and so does the push half
    /// (`completed`, or `needs-attention` when push-blocked). Exactly one unfinished intent must
    /// match, so a transition never resurrects an intent another operation already settled.
    #[allow(clippy::too_many_arguments)]
    pub fn record_release_advance(
        &self,
        state: &RepositoryState,
        command_id: CommandId,
        to: IntentState,
        observed: &serde_json::Value,
        actor: Actor,
        event_kind: &str,
        payload: serde_json::Value,
    ) -> Result<DomainEvent, StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE operation_intents SET state=?2, observed_json=?3, updated_at=?4 WHERE command_id=?1 AND kind=?5 AND state IN ('prepared','external-applied','needs-attention')",
            params![command_id.to_string(), to.as_str(), encode(observed)?, now(), RELEASE_ADVANCE_INTENT_KIND],
        )?;
        if changed != 1 {
            return Err(StoreError::Integrity(format!(
                "release advance {command_id} is not unfinished"
            )));
        }
        if matches!(
            to,
            IntentState::Completed | IntentState::Canceled | IntentState::NeedsAttention
        ) {
            transaction.execute(
                "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
                [command_id.to_string()],
            )?;
        }
        let persisted_sequence: i64 = transaction.query_row(
            "SELECT event_sequence FROM repository_state WHERE repository_id=?1",
            [state.id.to_string()],
            |row| row.get(0),
        )?;
        let sequence =
            state
                .event_sequence
                .max(u64::try_from(persisted_sequence).map_err(|_| {
                    StoreError::Integrity("repository event sequence is negative".into())
                })?)
                + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1",
            params![state.id.to_string(), self.encode_state(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()],
        )?;
        let event = DomainEvent {
            id: EventId::new(),
            repository_id: state.id,
            sequence,
            actor,
            command_id: Some(command_id),
            kind: event_kind.into(),
            payload,
            created_at: OffsetDateTime::now_utc(),
        };
        insert_event(&transaction, &event)?;
        transaction.commit()?;
        Ok(event)
    }

    pub fn append_event(&self, event: &DomainEvent) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_event(&transaction, event)?;
        transaction.execute("UPDATE repository_state SET event_sequence=?2, updated_at=?3 WHERE repository_id=?1 AND event_sequence < ?2", params![event.repository_id.to_string(), event.sequence as i64, now()])?;
        transaction.commit()?;
        Ok(())
    }

    pub fn events_after(&self, sequence: u64, limit: u32) -> Result<Vec<DomainEvent>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT event_json FROM events WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
        )?;
        let rows = statement.query_map(params![sequence as i64, limit as i64], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| decode(&row?)).collect()
    }

    pub fn stored_command_response<T: DeserializeOwned>(
        &self,
        command_id: CommandId,
    ) -> Result<Option<T>, StoreError> {
        let json: Option<String> = self
            .connection
            .lock()
            .query_row(
                "SELECT response_json FROM command_results WHERE command_id=?1",
                [command_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|value| decode(&value)).transpose()
    }

    pub fn checked_command_response<T: DeserializeOwned>(
        &self,
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
    ) -> Result<Option<T>, StoreError> {
        let row: Option<(String, String, String)> = self
            .connection
            .lock()
            .query_row(
                "SELECT command_kind, request_digest, response_json FROM command_results WHERE command_id=?1",
                [command_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some((kind, digest, response)) if kind == command_kind && digest == request_digest => {
                Ok(Some(decode(&response)?))
            }
            Some(_) => Err(StoreError::CommandReplayMismatch),
        }
    }

    pub fn replace_command_response(
        &self,
        command_id: CommandId,
        command_kind: &str,
        request_digest: &str,
        response: &impl Serialize,
    ) -> Result<(), StoreError> {
        let changed = self.connection.lock().execute(
            "UPDATE command_results SET response_json=?4 WHERE command_id=?1 AND command_kind=?2 AND request_digest=?3",
            params![
                command_id.to_string(),
                command_kind,
                request_digest,
                encode(response)?
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Integrity(
                "command response replacement did not match exactly one result".into(),
            ));
        }
        Ok(())
    }

    pub fn set_intent_state(
        &self,
        command_id: CommandId,
        state: IntentState,
        evidence: &impl Serialize,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("UPDATE operation_intents SET state=?2, observed_json=?3, updated_at=?4 WHERE command_id=?1 AND state NOT IN ('completed','canceled','needs-attention')", params![command_id.to_string(), state.as_str(), encode(evidence)?, now()])?;
        if matches!(
            state,
            IntentState::Completed | IntentState::Canceled | IntentState::NeedsAttention
        ) {
            transaction.execute(
                "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
                [command_id.to_string()],
            )?;
        }
        compact_terminal_intent(&transaction, command_id)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), StoreError> {
        self.connection
            .lock()
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_volume_state(
        &self,
        volume_id: &str,
        roles: &[String],
        warning_threshold: u64,
        critical_threshold: u64,
        emergency_allowance: u64,
        observed_free: u64,
    ) -> Result<(), StoreError> {
        let to_sql = |value: u64| {
            i64::try_from(value).map_err(|_| {
                StoreError::Integrity("volume byte count exceeds SQLite INTEGER range".into())
            })
        };
        self.connection.lock().execute(
            "INSERT INTO volume_state (volume_id,roles_json,warning_threshold,critical_threshold,emergency_allowance,observed_free) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(volume_id) DO UPDATE SET roles_json=excluded.roles_json,warning_threshold=excluded.warning_threshold,critical_threshold=excluded.critical_threshold,emergency_allowance=excluded.emergency_allowance,observed_free=excluded.observed_free",
            params![
                volume_id,
                encode(&roles.to_vec())?,
                to_sql(warning_threshold)?,
                to_sql(critical_threshold)?,
                to_sql(emergency_allowance)?,
                to_sql(observed_free)?,
            ],
        )?;
        Ok(())
    }

    pub fn reserve_volume(
        &self,
        command_id: CommandId,
        volume_id: &str,
        allowance: u64,
    ) -> Result<(), StoreError> {
        let allowance = i64::try_from(allowance)
            .map_err(|_| StoreError::Integrity("volume allowance exceeds SQLite range".into()))?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent_id: String = transaction.query_row(
            "SELECT intent_id FROM operation_intents WHERE command_id=?1 AND state IN ('prepared','external-applied') ORDER BY created_at DESC LIMIT 1",
            [command_id.to_string()],
            |row| row.get(0),
        )?;
        let reservation_id = format!("{}:{volume_id}", command_id);
        let (observed_free, critical_threshold): (i64, i64) = transaction.query_row(
            "SELECT observed_free, critical_threshold FROM volume_state WHERE volume_id=?1",
            [volume_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let already_reserved: i64 = transaction.query_row(
            "SELECT COALESCE(SUM(allowance), 0) FROM volume_reservations WHERE volume_id=?1 AND active=1 AND reservation_id<>?2",
            params![volume_id, reservation_id],
            |row| row.get(0),
        )?;
        let required = critical_threshold
            .checked_add(already_reserved)
            .and_then(|value| value.checked_add(allowance))
            .ok_or_else(|| StoreError::Integrity("volume reservation total overflowed".into()))?;
        if observed_free < required {
            return Err(StoreError::Integrity(format!(
                "volume {volume_id} has {observed_free} bytes free but the reservation requires {required}"
            )));
        }
        transaction.execute(
            "INSERT INTO volume_reservations (reservation_id, volume_id, intent_id, allowance, active) VALUES (?1, ?2, ?3, ?4, 1) ON CONFLICT(reservation_id) DO UPDATE SET allowance=excluded.allowance, active=1",
            params![reservation_id, volume_id, intent_id, allowance],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn active_volume_reservation(&self, volume_id: &str) -> Result<u64, StoreError> {
        let reserved: i64 = self.connection.lock().query_row(
            "SELECT COALESCE(SUM(allowance), 0) FROM volume_reservations WHERE volume_id=?1 AND active=1",
            [volume_id],
            |row| row.get(0),
        )?;
        u64::try_from(reserved)
            .map_err(|_| StoreError::Integrity("negative volume reservation total".into()))
    }

    pub fn volume_thresholds(&self, volume_id: &str) -> Result<Option<(u64, u64)>, StoreError> {
        let thresholds: Option<(i64, i64)> = self
            .connection
            .lock()
            .query_row(
                "SELECT warning_threshold, critical_threshold FROM volume_state WHERE volume_id=?1",
                [volume_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        thresholds
            .map(|(warning, critical)| {
                Ok((
                    u64::try_from(warning).map_err(|_| {
                        StoreError::Integrity("negative volume warning threshold".into())
                    })?,
                    u64::try_from(critical).map_err(|_| {
                        StoreError::Integrity("negative volume critical threshold".into())
                    })?,
                ))
            })
            .transpose()
    }

    pub fn deactivate_volume_reservation(&self, command_id: CommandId) -> Result<(), StoreError> {
        self.connection.lock().execute(
            "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
            [command_id.to_string()],
        )?;
        Ok(())
    }

    pub fn sqlite_storage_stats(&self) -> Result<SqliteStorageStats, StoreError> {
        let connection = self.connection.lock();
        let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let page_count: i64 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let freelist_pages: i64 =
            connection.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
        Ok(SqliteStorageStats {
            page_size: nonnegative_sqlite_count("page size", page_size)?,
            page_count: nonnegative_sqlite_count("page count", page_count)?,
            freelist_pages: nonnegative_sqlite_count("freelist page count", freelist_pages)?,
        })
    }

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<BackupCopyStats, StoreError> {
        let destination_path = destination.as_ref();
        let source = self.connection.lock();
        let page_size: i64 = source.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let page_size = nonnegative_sqlite_count("page size", page_size)?;
        let freelist_pages: i64 =
            source.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
        let source_freelist_pages =
            nonnegative_sqlite_count("freelist page count", freelist_pages)?;
        let mut destination = Connection::open(destination_path)?;
        let copied = copy_online_backup(&source, &mut destination)?;
        let integrity_started = Instant::now();
        let integrity: String =
            destination.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        let integrity_check_ms = elapsed_millis(integrity_started.elapsed());
        if integrity != "ok" {
            return Err(StoreError::Integrity(format!(
                "online backup integrity check failed: {integrity}"
            )));
        }
        Ok(BackupCopyStats {
            page_size,
            pages_copied: copied.pages_copied,
            source_freelist_pages,
            bytes_copied: copied.pages_copied.saturating_mul(page_size),
            successful_steps: copied.successful_steps,
            contention_waits: copied.contention_waits,
            copy_ms: copied.copy_ms,
            integrity_check_ms,
        })
    }

    pub fn record_backup(&self, path: &Path, hash: &str) -> Result<(), StoreError> {
        self.connection.lock().execute(
            "INSERT INTO backup_records (backup_id, database_identity, schema_version, path, hash, verified, migration_id) VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL)",
            params![uuid::Uuid::now_v7().to_string(), "repository-state", SCHEMA_VERSION, path.to_string_lossy(), hash],
        )?;
        Ok(())
    }

    pub fn complete_backup(
        &self,
        command_id: CommandId,
        path: &Path,
        hash: &str,
        observed: &impl Serialize,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO backup_records (backup_id, database_identity, schema_version, path, hash, verified, migration_id) VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL)",
            params![uuid::Uuid::now_v7().to_string(), "repository-state", SCHEMA_VERSION, path.to_string_lossy(), hash],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE command_id=?1 AND kind='backup' AND state IN ('prepared','external-applied')",
            params![command_id.to_string(), encode(observed)?, now()],
        )?;
        transaction.execute(
            "UPDATE volume_reservations SET active=0 WHERE intent_id IN (SELECT intent_id FROM operation_intents WHERE command_id=?1)",
            [command_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn verified_backup_hash(path: &Path) -> Result<String, StoreError> {
        let connection = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(StoreError::Integrity(format!(
                "backup integrity check failed: {integrity}"
            )));
        }
        drop(connection);
        let mut input = std::fs::File::open(path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0_u8; 128 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hasher.finalize().to_hex().to_string())
    }
}

fn insert_event(
    transaction: &rusqlite::Transaction<'_>,
    event: &DomainEvent,
) -> Result<(), StoreError> {
    transaction.execute("INSERT INTO events (event_id, repository_id, sequence, actor, command_id, kind, event_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)", params![event.id.to_string(), event.repository_id.to_string(), event.sequence as i64, enum_json(&event.actor)?, event.command_id.map(|id| id.to_string()), event.kind, encode(event)?, event.created_at.unix_timestamp_nanos().to_string()])?;
    Ok(())
}

fn invalidate_current_generation(
    transaction: &rusqlite::Transaction<'_>,
    replacement: &ValidationGeneration,
) -> Result<(), StoreError> {
    let current: Option<(String, String)> = transaction
        .query_row(
            "SELECT generation_id, generation_json FROM validation_generations WHERE item_id=?1 AND current=1",
            [replacement.item_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((generation_id, encoded)) = current {
        let mut generation: ValidationGeneration = decode(&encoded)?;
        generation.invalidated_by = Some(replacement.id);
        transaction.execute(
            "UPDATE validation_generations SET current=0, generation_json=?2 WHERE generation_id=?1",
            params![generation_id, encode(&generation)?],
        )?;
    }
    Ok(())
}

fn activate_retained_generation(
    transaction: &rusqlite::Transaction<'_>,
    restored: &ValidationGeneration,
) -> Result<(), StoreError> {
    let current: Option<(String, String)> = transaction
        .query_row(
            "SELECT generation_id, generation_json FROM validation_generations WHERE item_id=?1 AND current=1",
            [restored.item_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((generation_id, encoded)) = current {
        if generation_id == restored.id.to_string() {
            return Ok(());
        }
        let mut generation: ValidationGeneration = decode(&encoded)?;
        generation.invalidated_by = Some(restored.id);
        transaction.execute(
            "UPDATE validation_generations SET current=0, generation_json=?2 WHERE generation_id=?1",
            params![generation_id, encode(&generation)?],
        )?;
    }
    let changed = transaction.execute(
        "UPDATE validation_generations SET current=1, generation_json=?2 WHERE generation_id=?1 AND item_id=?3",
        params![
            restored.id.to_string(),
            encode(restored)?,
            restored.item_id.to_string()
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Integrity(format!(
            "retained generation {} for item {} was not available for reactivation",
            restored.id, restored.item_id
        )));
    }
    Ok(())
}

fn compact_terminal_intent(
    transaction: &rusqlite::Transaction<'_>,
    command_id: CommandId,
) -> Result<(), StoreError> {
    let record: Option<(String, String, String, Option<String>)> = transaction
        .query_row(
            "SELECT kind, state, expected_json, observed_json FROM operation_intents WHERE command_id=?1 ORDER BY created_at DESC LIMIT 1",
            [command_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((kind, state, expected, observed)) = record else {
        return Ok(());
    };
    if state != "completed" && state != "canceled" {
        return Ok(());
    }
    match kind.as_str() {
        "cache-snapshot" => {
            if let Some(observed) = observed {
                transaction.execute(
                    "UPDATE operation_intents SET observed_json=?2 WHERE command_id=?1",
                    params![command_id.to_string(), compacted_payload(&observed)?],
                )?;
            }
        }
        "cache-purge" => {
            let cleanup_complete = state == "canceled"
                || observed.as_ref().is_some_and(|value| {
                    serde_json::from_str::<serde_json::Value>(value)
                        .ok()
                        .and_then(|value| value.get("cleanup").cloned())
                        .is_some_and(|value| value == "complete")
                });
            if cleanup_complete {
                transaction.execute(
                    "UPDATE operation_intents SET expected_json=?2 WHERE command_id=?1",
                    params![command_id.to_string(), compacted_payload(&expected)?],
                )?;
            }
        }
        "artifact" | ARTIFACT_PRUNE_BATCH_KIND => {
            transaction.execute(
                "UPDATE operation_intents SET expected_json=?2 WHERE command_id=?1",
                params![command_id.to_string(), compacted_payload(&expected)?],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn artifact_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArtifactRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
    ))
}

fn decode_artifact(row: ArtifactRow) -> Result<ArtifactRecord, StoreError> {
    let (
        artifact_id,
        buildset_id,
        source_path,
        retained_path,
        hash,
        size,
        retention_state,
        created_at,
        expires_at,
    ) = row;
    Ok(ArtifactRecord {
        artifact_id,
        buildset_id: buildset_id.parse().map_err(|error| {
            StoreError::Integrity(format!("invalid artifact buildset ID: {error}"))
        })?,
        source_path,
        retained_path,
        hash,
        size: u64::try_from(size)
            .map_err(|_| StoreError::Integrity("artifact has a negative retained size".into()))?,
        retention_state,
        created_at: decode_time(&created_at)?,
        expires_at: decode_time(&expires_at)?,
    })
}

fn compacted_payload(encoded: &str) -> Result<String, StoreError> {
    if serde_json::from_str::<serde_json::Value>(encoded)?
        .get("compacted")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Ok(encoded.to_owned());
    }
    encode(&serde_json::json!({
        "compacted": true,
        "original_bytes": encoded.len(),
        "blake3": blake3::hash(encoded.as_bytes()).to_hex().to_string(),
    }))
}

fn compact_all_terminal_intents(transaction: &rusqlite::Transaction<'_>) -> Result<(), StoreError> {
    let command_ids = {
        let mut statement = transaction.prepare(
            "SELECT command_id FROM operation_intents WHERE state IN ('completed','canceled') AND kind IN ('cache-snapshot','cache-purge','artifact','artifact-prune-batch')",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for command_id in command_ids {
        compact_terminal_intent(
            transaction,
            command_id.parse().map_err(|error| {
                StoreError::Integrity(format!("invalid operation command ID: {error}"))
            })?,
        )?;
    }
    Ok(())
}

fn compact_pruned_seed_manifests(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), StoreError> {
    let seeds = {
        let mut statement = transaction
            .prepare("SELECT seed_id, manifest_json FROM seed_generations WHERE state='pruned'")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (seed_id, encoded) in seeds {
        let mut seed: SeedRecord = decode(&encoded)?;
        seed.state = "pruned".into();
        seed.manifest = serde_json::Value::Null;
        transaction.execute(
            "UPDATE seed_generations SET manifest_json=?2 WHERE seed_id=?1",
            params![seed_id, encode(&seed)?],
        )?;
    }
    Ok(())
}

fn artifact_audit_payload(records: &[ArtifactRecord]) -> Result<serde_json::Value, StoreError> {
    let encoded = serde_json::to_string(records)?;
    let artifact_bytes = records
        .iter()
        .fold(0_u64, |total, record| total.saturating_add(record.size));
    let buildset_id = records.first().map(|record| record.buildset_id.to_string());
    Ok(serde_json::json!({
        "compacted": true,
        "artifact_count": records.len(),
        "artifact_bytes": artifact_bytes,
        "buildset_id": buildset_id,
        "original_bytes": encoded.len(),
        "blake3": blake3::hash(encoded.as_bytes()).to_hex().to_string(),
    }))
}

fn compact_artifact_events(transaction: &rusqlite::Transaction<'_>) -> Result<(), StoreError> {
    let event_ids = {
        let mut statement = transaction.prepare(
            "SELECT event_id FROM events
             WHERE kind='artifact.published'
               AND COALESCE(json_extract(event_json, '$.payload.compacted'), 0) <> 1",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for event_id in event_ids {
        let encoded: String = transaction.query_row(
            "SELECT event_json FROM events WHERE event_id=?1",
            [&event_id],
            |row| row.get(0),
        )?;
        let mut event: DomainEvent = decode(&encoded)?;
        let records: Vec<ArtifactRecord> = serde_json::from_value(event.payload)?;
        event.payload = artifact_audit_payload(&records)?;
        transaction.execute(
            "UPDATE events SET event_json=?2 WHERE event_id=?1",
            params![event_id, encode(&event)?],
        )?;
    }
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, StoreError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<HashSet<_>, _>>()?;
    Ok(columns.contains(column))
}

fn copy_online_backup(
    source: &Connection,
    destination: &mut Connection,
) -> Result<BackupStepStats, StoreError> {
    let backup = rusqlite::backup::Backup::new(source, destination)?;
    let copy_started = Instant::now();
    let mut successful_steps = 0_u64;
    let mut contention_waits = 0_u64;
    // Keep bounded steps and back off for actual contention, but do not sleep after every
    // successful step: that pause scales with database pages and dominated large backups.
    loop {
        match backup.step(4_096)? {
            rusqlite::backup::StepResult::More => {
                successful_steps = successful_steps.saturating_add(1);
            }
            rusqlite::backup::StepResult::Busy | rusqlite::backup::StepResult::Locked => {
                contention_waits = contention_waits.saturating_add(1);
                std::thread::sleep(Duration::from_millis(10));
            }
            rusqlite::backup::StepResult::Done => {
                successful_steps = successful_steps.saturating_add(1);
                break;
            }
            _ => {}
        }
    }
    let progress = backup.progress();
    let pages_copied = nonnegative_sqlite_count("backup page count", progress.pagecount.into())?;
    Ok(BackupStepStats {
        pages_copied,
        successful_steps,
        contention_waits,
        copy_ms: elapsed_millis(copy_started.elapsed()),
    })
}

fn migrate(connection: &Connection, database_path: Option<&Path>) -> Result<(), StoreError> {
    let mut version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(StoreError::SchemaTooNew {
            actual: version,
            supported: SCHEMA_VERSION,
        });
    }
    if version == 0 {
        let has_legacy_schema: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='repository_state')",
            [],
            |row| row.get(0),
        )?;
        if !has_legacy_schema {
            connection.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
            connection.execute_batch(SCHEMA)?;
            connection.execute_batch("DROP INDEX IF EXISTS queue_active_source")?;
            connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            return Ok(());
        }
        // Early development databases predated PRAGMA user_version but have the v1 table shape.
        // Treating them as fresh would stamp a schema that CREATE IF NOT EXISTS cannot upgrade.
        version = 1;
    }
    if version == SCHEMA_VERSION {
        connection.execute_batch(SCHEMA)?;
        connection.execute_batch("DROP INDEX IF EXISTS queue_active_source")?;
        return Ok(());
    }

    let backup = database_path.map(|path| create_migration_backup(connection, path, version));
    let backup = backup.transpose()?;
    if version < 3 {
        migrate_to_compacted_payloads(connection, version)?;
    }

    // Version 4 adds the operation-intent and artifact-expiry indexes and the artifact attention
    // table through SCHEMA. ANALYZE records planner statistics for the new indexes, so pruning
    // and intent lookups on large existing databases read single rows instead of scanning.
    let transaction = connection.unchecked_transaction()?;
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch("DROP INDEX IF EXISTS queue_active_source")?;
    transaction.execute_batch("ANALYZE")?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    if let Some((path, hash)) = backup {
        transaction.execute(
            "INSERT INTO backup_records (backup_id, database_identity, schema_version, path, hash, verified, migration_id) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![uuid::Uuid::now_v7().to_string(), "repository-state", version, path.to_string_lossy(), hash, format!("schema-{version}-to-{SCHEMA_VERSION}")],
        )?;
    }
    transaction.commit()?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(StoreError::Integrity(format!(
            "post-migration integrity check failed: {integrity}"
        )));
    }
    Ok(())
}

/// Versions 1 and 2 carried disposable payloads (seed manifests, terminal intent evidence, and
/// artifact event bodies) inline. This compacts them, converts the database to incremental
/// auto-vacuum, and rebuilds it once. `user_version` is advanced only afterward by the caller,
/// so an interrupted VACUUM retries this idempotent step on the next open.
fn migrate_to_compacted_payloads(connection: &Connection, version: i64) -> Result<(), StoreError> {
    let artifact_columns = if version == 1 {
        let mut statement = connection.prepare("PRAGMA table_info(artifacts)")?;
        statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<HashSet<_>, _>>()?
    } else {
        HashSet::new()
    };
    let cache_manifest_has_payload =
        table_has_column(connection, "cache_manifests", "manifest_json")?;
    let transaction = connection.unchecked_transaction()?;
    if version == 1 {
        if !artifact_columns.contains("created_at") {
            transaction.execute_batch(
                "ALTER TABLE artifacts ADD COLUMN created_at TEXT NOT NULL DEFAULT '0';",
            )?;
        }
        if !artifact_columns.contains("expires_at") {
            transaction.execute_batch(
                "ALTER TABLE artifacts ADD COLUMN expires_at TEXT NOT NULL DEFAULT '0';",
            )?;
        }
        let migrated_at = OffsetDateTime::now_utc();
        transaction.execute(
            "UPDATE artifacts SET created_at=?1, expires_at=?2 WHERE created_at='0' OR expires_at='0'",
            params![encode_time(migrated_at), encode_time(migrated_at + time::Duration::days(30))],
        )?;
    }
    if cache_manifest_has_payload {
        transaction.execute_batch(
            "CREATE TABLE cache_manifests_v3 (
               seed_id TEXT PRIMARY KEY REFERENCES seed_generations(seed_id),
               hash TEXT NOT NULL,
               entry_count INTEGER NOT NULL
             ) STRICT;
             INSERT INTO cache_manifests_v3 (seed_id, hash, entry_count)
               SELECT seed_id, hash, entry_count FROM cache_manifests;
             DROP TABLE cache_manifests;
             ALTER TABLE cache_manifests_v3 RENAME TO cache_manifests;",
        )?;
    }
    compact_pruned_seed_manifests(&transaction)?;
    compact_all_terminal_intents(&transaction)?;
    compact_artifact_events(&transaction)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch("DROP INDEX IF EXISTS queue_active_source")?;
    transaction.commit()?;

    // Converting to incremental auto-vacuum plus this one-time rebuild releases the historical
    // duplicate payload pages immediately.
    connection.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    connection.execute_batch("VACUUM")?;

    Ok(())
}

fn create_migration_backup(
    source: &Connection,
    database_path: &Path,
    schema_version: i64,
) -> Result<(PathBuf, String), StoreError> {
    let parent = database_path
        .parent()
        .ok_or_else(|| StoreError::Integrity("database path has no parent".into()))?;
    let root = parent.join("backups");
    std::fs::create_dir_all(&root)?;
    if std::fs::symlink_metadata(&root)?.file_type().is_symlink() {
        return Err(StoreError::Integrity(
            "migration backup root was replaced by a symlink".into(),
        ));
    }
    let token = uuid::Uuid::now_v7();
    let temporary = root.join(format!(".migration-v{schema_version}-{token}.sqlite3.tmp"));
    let destination = root.join(format!("migration-v{schema_version}-{token}.sqlite3"));
    let mut output = Connection::open(&temporary)?;
    copy_online_backup(source, &mut output)?;
    let integrity: String = output.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(StoreError::Integrity(format!(
            "migration backup integrity check failed: {integrity}"
        )));
    }
    drop(output);
    std::fs::File::open(&temporary)?.sync_all()?;
    std::fs::rename(&temporary, &destination)?;
    std::fs::File::open(&root)?.sync_all()?;
    let mut input = std::fs::File::open(&destination)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((destination, hasher.finalize().to_hex().to_string()))
}

fn verify_sqlite_version(connection: &Connection) -> Result<(), StoreError> {
    let version: String = connection.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let parts = version
        .split('.')
        .map(|part| part.parse::<u32>().unwrap_or(0))
        .collect::<Vec<_>>();
    let current = (
        parts.first().copied().unwrap_or(0),
        parts.get(1).copied().unwrap_or(0),
        parts.get(2).copied().unwrap_or(0),
    );
    if current < (3, 51, 3) {
        return Err(StoreError::SqliteTooOld { actual: version });
    }
    Ok(())
}

fn encode(value: &impl Serialize) -> Result<String, StoreError> {
    Ok(serde_json::to_string(value)?)
}
fn decode<T: DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    Ok(serde_json::from_str(value)?)
}
fn enum_json(value: &impl Serialize) -> Result<String, StoreError> {
    encode(value).map(|value| value.trim_matches('"').to_owned())
}
fn nonnegative_sqlite_count(name: &str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::Integrity(format!("negative SQLite {name}: {value}")))
}
fn elapsed_millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}
fn now() -> String {
    OffsetDateTime::now_utc().unix_timestamp_nanos().to_string()
}

fn encode_time(value: OffsetDateTime) -> String {
    value.unix_timestamp_nanos().to_string()
}

fn decode_time(value: &str) -> Result<OffsetDateTime, StoreError> {
    let nanos = value
        .parse::<i128>()
        .map_err(|error| StoreError::Integrity(format!("invalid persisted timestamp: {error}")))?;
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|error| StoreError::Integrity(format!("persisted timestamp is invalid: {error}")))
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS repository_state (
  repository_id TEXT PRIMARY KEY, state_json TEXT NOT NULL, queue_revision INTEGER NOT NULL,
  event_sequence INTEGER NOT NULL, schema_version INTEGER NOT NULL, engine_epoch INTEGER NOT NULL,
  active_configuration_digest TEXT NOT NULL, updated_at TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS queue_items (
  item_id TEXT PRIMARY KEY, repository_id TEXT NOT NULL REFERENCES repository_state(repository_id),
  source_format TEXT NOT NULL CHECK(source_format IN ('sha1','sha256')), source_oid BLOB NOT NULL,
  enqueue_sequence INTEGER NOT NULL, state TEXT NOT NULL, remote_state TEXT NOT NULL,
  cleanup_state TEXT NOT NULL, current_generation_id TEXT, item_json TEXT NOT NULL,
  active INTEGER NOT NULL CHECK(active IN (0,1)), UNIQUE(repository_id, enqueue_sequence)
) STRICT;
CREATE TABLE IF NOT EXISTS item_dependencies (
  item_id TEXT NOT NULL REFERENCES queue_items(item_id), dependency_item_id TEXT NOT NULL REFERENCES queue_items(item_id),
  PRIMARY KEY(item_id, dependency_item_id), CHECK(item_id <> dependency_item_id)
) STRICT;
CREATE TABLE IF NOT EXISTS validation_generations (
  generation_id TEXT PRIMARY KEY, item_id TEXT NOT NULL REFERENCES queue_items(item_id), identity_digest TEXT NOT NULL,
  tested_format TEXT NOT NULL, tested_oid BLOB NOT NULL, expected_parent_format TEXT NOT NULL,
  expected_parent_oid BLOB NOT NULL, configuration_digest TEXT NOT NULL, step_graph_digest TEXT NOT NULL,
  engine_epoch INTEGER NOT NULL, generation_json TEXT NOT NULL, current INTEGER NOT NULL CHECK(current IN (0,1))
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS generation_current_item ON validation_generations(item_id) WHERE current=1;
CREATE TABLE IF NOT EXISTS buildsets (
  buildset_id TEXT PRIMARY KEY, item_id TEXT REFERENCES queue_items(item_id), generation_id TEXT REFERENCES validation_generations(generation_id),
  tested_format TEXT NOT NULL, tested_oid BLOB NOT NULL, expected_parent_oid BLOB NOT NULL,
  environment_fingerprint TEXT NOT NULL, slot_id TEXT, status TEXT NOT NULL, retry_of_buildset_id TEXT REFERENCES buildsets(buildset_id),
  attempt INTEGER NOT NULL, buildset_json TEXT NOT NULL
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS buildset_nonterminal_generation ON buildsets(generation_id) WHERE status IN ('pending','preparing','running');
CREATE TABLE IF NOT EXISTS steps (step_id TEXT PRIMARY KEY, buildset_id TEXT NOT NULL REFERENCES buildsets(buildset_id), name TEXT NOT NULL, frozen_json TEXT NOT NULL, UNIQUE(buildset_id,name)) STRICT;
CREATE TABLE IF NOT EXISTS step_attempts (attempt_id TEXT PRIMARY KEY, step_id TEXT NOT NULL REFERENCES steps(step_id), retry_number INTEGER NOT NULL, result_class TEXT, attempt_json TEXT NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS pass_certificates (certificate_id TEXT PRIMARY KEY, buildset_id TEXT UNIQUE NOT NULL REFERENCES buildsets(buildset_id), generation_id TEXT NOT NULL REFERENCES validation_generations(generation_id), tested_oid BLOB NOT NULL, certificate_json TEXT NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS source_promotions (item_id TEXT PRIMARY KEY REFERENCES queue_items(item_id), source_oid BLOB NOT NULL, promoted_oid BLOB NOT NULL, old_master_oid BLOB NOT NULL, certificate_id TEXT NOT NULL REFERENCES pass_certificates(certificate_id), event_sequence INTEGER NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS configuration_snapshots (digest TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, canonical_bytes BLOB NOT NULL, step_graph_digest TEXT NOT NULL, activation_sequence INTEGER NOT NULL, supersedes_digest TEXT) STRICT;
CREATE TABLE IF NOT EXISTS operation_intents (intent_id TEXT PRIMARY KEY, repository_id TEXT NOT NULL REFERENCES repository_state(repository_id), kind TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','external-applied','completed','canceled','needs-attention')), command_id TEXT NOT NULL, expected_json TEXT NOT NULL, observed_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS unfinished_promotion ON operation_intents(repository_id) WHERE kind='promotion' AND state IN ('prepared','external-applied');
CREATE UNIQUE INDEX IF NOT EXISTS unfinished_push ON operation_intents(repository_id) WHERE kind='push' AND state IN ('prepared','external-applied');
CREATE TABLE IF NOT EXISTS slots (slot_id TEXT PRIMARY KEY, repository_id TEXT NOT NULL REFERENCES repository_state(repository_id), ownership_path TEXT NOT NULL, state TEXT NOT NULL, metadata_json TEXT NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS seed_generations (seed_id TEXT PRIMARY KEY, repository_id TEXT NOT NULL REFERENCES repository_state(repository_id), profile TEXT NOT NULL, generation INTEGER NOT NULL, ownership_path TEXT NOT NULL, logical_size INTEGER NOT NULL, state TEXT NOT NULL, manifest_json TEXT NOT NULL, UNIQUE(repository_id,profile,generation)) STRICT;
CREATE TABLE IF NOT EXISTS cache_manifests (seed_id TEXT PRIMARY KEY REFERENCES seed_generations(seed_id), hash TEXT NOT NULL, entry_count INTEGER NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS artifacts (artifact_id TEXT PRIMARY KEY, buildset_id TEXT NOT NULL REFERENCES buildsets(buildset_id), step_id TEXT REFERENCES steps(step_id), source_path TEXT NOT NULL, retained_path TEXT NOT NULL, hash TEXT NOT NULL, size INTEGER NOT NULL, retention_state TEXT NOT NULL CHECK(retention_state IN ('retained','pinned','pruned')), created_at TEXT NOT NULL, expires_at TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS artifact_buildset_path ON artifacts(buildset_id,retained_path);
CREATE TABLE IF NOT EXISTS log_streams (stream_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES step_attempts(attempt_id), stream TEXT NOT NULL CHECK(stream IN ('stdout','stderr')), retained_start INTEGER NOT NULL DEFAULT 0, retained_end INTEGER NOT NULL DEFAULT 0, sealed_hash TEXT, state TEXT NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS log_chunks (chunk_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL REFERENCES log_streams(stream_id), start_offset INTEGER NOT NULL, end_offset INTEGER NOT NULL, broker_sequence_start INTEGER NOT NULL, broker_sequence_end INTEGER NOT NULL, hash TEXT NOT NULL, storage_path TEXT NOT NULL, compressed INTEGER NOT NULL CHECK(compressed IN (0,1))) STRICT;
CREATE TABLE IF NOT EXISTS remote_observations (observation_id TEXT PRIMARY KEY, repository_id TEXT NOT NULL REFERENCES repository_state(repository_id), remote_identity TEXT NOT NULL, exact_ref TEXT NOT NULL, oid BLOB, method TEXT NOT NULL, observed_at TEXT NOT NULL, intent_id TEXT REFERENCES operation_intents(intent_id)) STRICT;
CREATE TABLE IF NOT EXISTS volume_state (volume_id TEXT PRIMARY KEY, roles_json TEXT NOT NULL, warning_threshold INTEGER NOT NULL, critical_threshold INTEGER NOT NULL, emergency_allowance INTEGER NOT NULL, observed_free INTEGER NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS volume_reservations (reservation_id TEXT PRIMARY KEY, volume_id TEXT NOT NULL REFERENCES volume_state(volume_id), intent_id TEXT NOT NULL REFERENCES operation_intents(intent_id), allowance INTEGER NOT NULL, active INTEGER NOT NULL CHECK(active IN (0,1))) STRICT;
CREATE TABLE IF NOT EXISTS command_results (command_id TEXT PRIMARY KEY, command_kind TEXT NOT NULL, request_digest TEXT NOT NULL, response_json TEXT NOT NULL, event_sequence INTEGER NOT NULL, created_at TEXT NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS backup_records (backup_id TEXT PRIMARY KEY, database_identity TEXT NOT NULL, schema_version INTEGER NOT NULL, path TEXT NOT NULL, hash TEXT NOT NULL, verified INTEGER NOT NULL CHECK(verified IN (0,1)), migration_id TEXT) STRICT;
CREATE TABLE IF NOT EXISTS events (event_id TEXT PRIMARY KEY, repository_id TEXT NOT NULL REFERENCES repository_state(repository_id), sequence INTEGER NOT NULL, actor TEXT NOT NULL, command_id TEXT, kind TEXT NOT NULL, event_json TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(repository_id,sequence)) STRICT;
CREATE INDEX IF NOT EXISTS events_kind_time ON events(kind,created_at);
CREATE INDEX IF NOT EXISTS operation_intent_command ON operation_intents(command_id,created_at);
CREATE INDEX IF NOT EXISTS operation_intent_state ON operation_intents(state,kind);
CREATE INDEX IF NOT EXISTS artifact_retention_expiry ON artifacts(retention_state,expires_at);
CREATE TABLE IF NOT EXISTS artifact_attention (artifact_id TEXT PRIMARY KEY REFERENCES artifacts(artifact_id), command_id TEXT NOT NULL, detail TEXT NOT NULL, recorded_at TEXT NOT NULL) STRICT;
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use tollgate_domain::{
        BlockReason, CleanupPolicy, CleanupState, GitOid, ObjectFormat, QueueItemId, QueueItemKind,
        QueueItemState, ReleaseLag, ReleaseState, RemoteState, RepositoryExecutionState,
        SignatureState, SourceMetadata, ValidationGenerationId,
    };

    fn oid(value: u8) -> GitOid {
        GitOid::new(ObjectFormat::Sha1, vec![value; 20]).unwrap()
    }

    fn test_state(name: &str) -> RepositoryState {
        RepositoryState {
            id: RepositoryId::new(),
            name: name.into(),
            path: format!("/{name}"),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(1),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(1),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        }
    }

    #[test]
    fn initializes_and_reads_repository_state() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = RepositoryState {
            id: RepositoryId::new(),
            name: "demo".into(),
            path: "/demo".into(),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(1),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(1),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::<BlockReason>::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        };
        store.initialize_repository(&state).unwrap();
        assert_eq!(store.repository_state().unwrap(), state);
        store.quick_integrity_check().unwrap();
    }

    #[test]
    fn online_backup_is_standalone_consistent_and_reports_copied_pages() {
        let temporary = tempfile::tempdir().unwrap();
        let source_path = temporary.path().join("state.sqlite3");
        let backup_path = temporary.path().join("backup.sqlite3");
        let store = RepositoryStore::open(&source_path).unwrap();
        let mut state = RepositoryState {
            id: RepositoryId::new(),
            name: "backup-test".into(),
            path: "/backup-test".into(),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(1),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(1),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        };
        store.initialize_repository(&state).unwrap();
        store
            .connection
            .lock()
            .execute_batch(
                "CREATE TABLE backup_payload (payload BLOB NOT NULL) STRICT;
                 INSERT INTO backup_payload VALUES (zeroblob(33554432));",
            )
            .unwrap();
        let source_stats = store.sqlite_storage_stats().unwrap();
        assert!(source_stats.page_count > 4_096);

        let copied = store.backup_to(&backup_path).unwrap();
        assert_eq!(copied.page_size, source_stats.page_size);
        assert_eq!(copied.pages_copied, source_stats.page_count);
        assert_eq!(copied.bytes_copied, copied.pages_copied * copied.page_size);
        assert!(copied.successful_steps > 1);
        assert_eq!(copied.contention_waits, 0);

        state.queue_revision = 1;
        store.update_repository_state(&state).unwrap();
        drop(store);
        std::fs::remove_file(&source_path).unwrap();
        let restored = RepositoryStore::open(&backup_path).unwrap();
        restored.quick_integrity_check().unwrap();
        assert_eq!(restored.repository_state().unwrap().queue_revision, 0);
    }

    #[test]
    fn promotion_intent_uniqueness_is_idempotent_and_never_exposes_sqlite() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = RepositoryState {
            id: RepositoryId::new(),
            name: "promotion-intent".into(),
            path: "/promotion-intent".into(),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(1),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(1),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        };
        store.initialize_repository(&state).unwrap();
        let first = CommandId::new();
        let evidence = serde_json::json!({"certificate": "first"});
        store.prepare_promotion(state.id, first, &evidence).unwrap();
        store.prepare_promotion(state.id, first, &evidence).unwrap();

        let error = store
            .prepare_promotion(
                state.id,
                CommandId::new(),
                &serde_json::json!({"certificate": "second"}),
            )
            .unwrap_err();
        assert!(matches!(
            &error,
            StoreError::OperationInProgress { kind } if kind == "promotion"
        ));
        assert!(!error.to_string().contains("UNIQUE constraint"));

        store
            .set_intent_state(
                first,
                IntentState::Canceled,
                &serde_json::json!({"reason": "retry"}),
            )
            .unwrap();
        store
            .prepare_promotion(
                state.id,
                CommandId::new(),
                &serde_json::json!({"certificate": "second"}),
            )
            .unwrap();
    }

    #[test]
    fn migration_creates_and_verifies_an_online_backup_before_schema_change() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("state.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection.execute_batch("DROP TABLE artifacts; CREATE TABLE artifacts (artifact_id TEXT PRIMARY KEY, buildset_id TEXT NOT NULL REFERENCES buildsets(buildset_id), step_id TEXT REFERENCES steps(step_id), source_path TEXT NOT NULL, retained_path TEXT NOT NULL, hash TEXT NOT NULL, size INTEGER NOT NULL, retention_state TEXT NOT NULL) STRICT;").unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        drop(connection);
        assert!(RepositoryStore::migration_allowance(&path).unwrap() >= 512 * 1024 * 1024);

        let store = RepositoryStore::open(&path).unwrap();
        assert_eq!(RepositoryStore::migration_allowance(&path).unwrap(), 0);
        assert!(store.retained_artifacts().unwrap().is_empty());
        store.quick_integrity_check().unwrap();
        let backups = std::fs::read_dir(temporary.path().join("backups"))
            .unwrap()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert_eq!(backups.len(), 1);
        assert!(
            backups[0]
                .file_name()
                .to_string_lossy()
                .starts_with("migration-v1-")
        );
        let backup = Connection::open(backups[0].path()).unwrap();
        let integrity: String = backup
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
    }

    #[test]
    fn v3_migration_compacts_disposable_payloads_without_removing_audit_rows() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("state.sqlite3");
        let store = RepositoryStore::open(&path).unwrap();
        let state = test_state("v3-compaction");
        store.initialize_repository(&state).unwrap();
        let seed = SeedRecord {
            id: "seed-pruned".into(),
            repository_id: state.id,
            profile: "default".into(),
            generation: 1,
            path: "/cache/seed-pruned".into(),
            logical_size: 1,
            state: "pruned".into(),
            manifest: serde_json::json!({"entries": ["x".repeat(2 * 1024 * 1024)]}),
        };
        store.record_seed(&seed).unwrap();
        let artifact = ArtifactRecord {
            artifact_id: "artifact-audit".into(),
            buildset_id: tollgate_domain::BuildsetId::new(),
            source_path: "report.json".into(),
            retained_path: "z".repeat(2 * 1024 * 1024),
            hash: "artifact-hash".into(),
            size: 42,
            retention_state: "pruned".into(),
            created_at: OffsetDateTime::now_utc(),
            expires_at: OffsetDateTime::now_utc(),
        };
        {
            let mut connection = store.connection.lock();
            let transaction = connection.transaction().unwrap();
            insert_event(
                &transaction,
                &DomainEvent {
                    id: EventId::new(),
                    repository_id: state.id,
                    sequence: 1,
                    actor: Actor::App,
                    command_id: Some(CommandId::new()),
                    kind: "artifact.published".into(),
                    payload: serde_json::to_value([artifact]).unwrap(),
                    created_at: OffsetDateTime::now_utc(),
                },
            )
            .unwrap();
            transaction.commit().unwrap();
        }
        drop(store);

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE cache_manifests
                   ADD COLUMN manifest_json TEXT NOT NULL DEFAULT '{}';",
            )
            .unwrap();
        let encoded_seed = encode(&seed).unwrap();
        connection
            .execute(
                "UPDATE cache_manifests SET manifest_json=?1 WHERE seed_id=?2",
                params![encode(&seed.manifest).unwrap(), seed.id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE seed_generations SET manifest_json=?1 WHERE seed_id=?2",
                params![encoded_seed, seed.id],
            )
            .unwrap();
        let large = encode(&serde_json::json!({
            "entries": ["y".repeat(2 * 1024 * 1024)]
        }))
        .unwrap();
        let cases = [
            ("cache-snapshot", "completed", "{}", Some(large.as_str())),
            ("artifact", "canceled", large.as_str(), None),
            (
                "cache-purge",
                "completed",
                large.as_str(),
                Some(r#"{"cleanup":"complete"}"#),
            ),
        ];
        for (kind, intent_state, expected, observed) in cases {
            connection
                .execute(
                    "INSERT INTO operation_intents
                     (intent_id, repository_id, kind, state, command_id, expected_json,
                      observed_json, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
                    params![
                        uuid::Uuid::now_v7().to_string(),
                        state.id.to_string(),
                        kind,
                        intent_state,
                        CommandId::new().to_string(),
                        expected,
                        observed,
                        now(),
                    ],
                )
                .unwrap();
        }
        connection.pragma_update(None, "user_version", 2).unwrap();
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(connection);
        let bytes_before = std::fs::metadata(&path).unwrap().len();

        let store = RepositoryStore::open(&path).unwrap();
        store.quick_integrity_check().unwrap();
        let seeds = store.seed_records(state.id).unwrap();
        assert_eq!(seeds.len(), 1);
        assert_eq!(seeds[0].state, "pruned");
        assert_eq!(seeds[0].manifest, serde_json::Value::Null);
        drop(store);

        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            connection
                .query_row("PRAGMA auto_vacuum", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert!(!table_has_column(&connection, "cache_manifests", "manifest_json").unwrap());
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM operation_intents", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            3
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM operation_intents
                     WHERE json_extract(expected_json, '$.compacted')=1
                        OR json_extract(observed_json, '$.compacted')=1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            3
        );
        let artifact_event: String = connection
            .query_row(
                "SELECT event_json FROM events WHERE kind='artifact.published'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let artifact_event: DomainEvent = decode(&artifact_event).unwrap();
        assert_eq!(artifact_event.payload["compacted"], true);
        assert_eq!(artifact_event.payload["artifact_count"], 1);
        assert_eq!(artifact_event.payload["artifact_bytes"], 42);
        let bytes_after = std::fs::metadata(&path).unwrap().len();
        assert!(
            bytes_after < bytes_before / 2,
            "{bytes_before} -> {bytes_after}"
        );
    }

    #[test]
    fn cache_purge_recovery_payload_survives_until_cleanup_is_durable() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = test_state("cache-purge-recovery");
        store.initialize_repository(&state).unwrap();
        let command_id = CommandId::new();
        let expected = serde_json::json!({"quarantine_paths": ["/cache/quarantine"]});
        store
            .prepare_operation(state.id, "cache-purge", command_id, &expected)
            .unwrap();
        store
            .set_intent_state(
                command_id,
                IntentState::Completed,
                &serde_json::json!({"seed_count": 1}),
            )
            .unwrap();

        let unsettled = store
            .unsettled_completed_operation_evidence("cache-purge")
            .unwrap();
        assert_eq!(unsettled, vec![(command_id, expected.clone())]);

        store
            .mark_completed_operation_cleanup(command_id, "cache-purge")
            .unwrap();
        assert!(
            store
                .unsettled_completed_operation_evidence("cache-purge")
                .unwrap()
                .is_empty()
        );
        let records = store.completed_operation_records("cache-purge").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0["compacted"], true);
        assert_eq!(
            records[0].0["blake3"],
            blake3::hash(encode(&expected).unwrap().as_bytes())
                .to_hex()
                .to_string()
        );
        assert_eq!(records[0].1["cleanup"], "complete");
    }

    #[test]
    fn migrates_an_unversioned_legacy_database_instead_of_stamping_it_fresh() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("state.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection.execute_batch("DROP TABLE artifacts; CREATE TABLE artifacts (artifact_id TEXT PRIMARY KEY, buildset_id TEXT NOT NULL REFERENCES buildsets(buildset_id), step_id TEXT REFERENCES steps(step_id), source_path TEXT NOT NULL, retained_path TEXT NOT NULL, hash TEXT NOT NULL, size INTEGER NOT NULL, retention_state TEXT NOT NULL) STRICT;").unwrap();
        connection.pragma_update(None, "user_version", 0).unwrap();
        drop(connection);

        let store = RepositoryStore::open(&path).unwrap();
        assert!(store.retained_artifacts().unwrap().is_empty());
        store.quick_integrity_check().unwrap();
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let columns = connection
            .prepare("PRAGMA table_info(artifacts)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<HashSet<_>, _>>()
            .unwrap();
        assert!(columns.contains("created_at"));
        assert!(columns.contains("expires_at"));
    }

    #[test]
    fn volume_reservations_are_admitted_and_released_transactionally() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = RepositoryState {
            id: RepositoryId::new(),
            name: "reservation-test".into(),
            path: "/reservation-test".into(),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(7),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(7),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        };
        store.initialize_repository(&state).unwrap();
        store
            .upsert_volume_state("shared", &["git".into()], 750, 500, 50, 1_000)
            .unwrap();
        let first = CommandId::new();
        let second = CommandId::new();
        store
            .prepare_operation(
                state.id,
                "reservation-test",
                first,
                &serde_json::json!({"oid": "a"}),
            )
            .unwrap();
        store
            .prepare_operation(
                state.id,
                "reservation-test",
                second,
                &serde_json::json!({"oid": "b"}),
            )
            .unwrap();
        store.reserve_volume(first, "shared", 400).unwrap();
        assert!(store.reserve_volume(second, "shared", 200).is_err());
        assert_eq!(store.active_volume_reservation("shared").unwrap(), 400);
        store.deactivate_volume_reservation(first).unwrap();
        store.reserve_volume(second, "shared", 200).unwrap();
        assert_eq!(store.active_volume_reservation("shared").unwrap(), 200);
    }

    #[test]
    fn approval_completion_rejects_queue_change_after_preflight_atomically() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let mut state = RepositoryState {
            id: RepositoryId::new(),
            name: "candidate-race".into(),
            path: "/candidate-race".into(),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(1),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(1),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        };
        store.initialize_repository(&state).unwrap();
        let item_id = QueueItemId::new();
        let source_oid = oid(2);
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            state.staging_oid.clone(),
            vec![item_id],
            vec![source_oid.clone()],
            vec![source_oid.clone()],
            state.staging_oid.clone(),
            source_oid.clone(),
            "digest".into(),
            "steps".into(),
            state.engine_epoch,
        );
        let item = QueueItem {
            id: item_id,
            repository_id: state.id,
            kind: QueueItemKind::Gate,
            admission_sequence: Some(1),
            enqueue_sequence: 1,
            source_oid: source_oid.clone(),
            source_ref: format!("refs/tollgate/sources/{item_id}"),
            metadata: SourceMetadata {
                subject: "candidate".into(),
                message_hash: "message".into(),
                author_name: "Tollgate Test".into(),
                author_email: "test@example.com".into(),
                branch: Some("task".into()),
                worktree_path: Some("/candidate-race/task".into()),
                signature_state: SignatureState::Unknown,
                approved_at: OffsetDateTime::now_utc(),
                purpose: Some("candidate".into()),
            },
            state: QueueItemState::Queued,
            terminal_reason: None,
            remote_state: RemoteState::Disabled,
            cleanup_state: CleanupState::NotEligible,
            cleanup_policy: CleanupPolicy::Automatic,
            dependencies: Vec::new(),
            retry_of_item_id: None,
            promotion_authorized: false,
            promotion_authorized_at: None,
            promotion_authorized_by: None,
            current_generation_id: Some(generation.id),
            buildset_id: None,
            certificate_id: None,
            release_fix: false,
        };
        let command_id = CommandId::new();
        store
            .prepare_approval(state.id, &item, command_id, "request")
            .unwrap();

        state.queue_revision = 1;
        store.update_repository_state(&state).unwrap();
        let error = store
            .complete_approval(
                &item,
                Some(&generation),
                0,
                Actor::Cli,
                command_id,
                "candidate",
                "request",
                &serde_json::json!({"item_id": item_id}),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            StoreError::RevisionConflict {
                expected: 0,
                actual: 1
            }
        ));
        assert!(store.queue_items().unwrap().is_empty());
        assert_eq!(store.unfinished_approvals().unwrap().len(), 1);
    }

    fn lagging_state(name: &str) -> RepositoryState {
        let mut state = test_state(name);
        state.staging_oid = oid(2);
        state.release_lag = ReleaseLag {
            commits: 1,
            since: Some(OffsetDateTime::now_utc()),
        };
        state.release_state = ReleaseState::Pending;
        state
    }

    fn release_run(state: &RepositoryState, sequence: u64) -> (QueueItem, ValidationGeneration) {
        let item_id = QueueItemId::new();
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            state.release_oid.clone(),
            vec![item_id],
            vec![state.staging_oid.clone()],
            vec![state.staging_oid.clone()],
            state.release_oid.clone(),
            state.staging_oid.clone(),
            "digest".into(),
            "release-steps".into(),
            state.engine_epoch,
        );
        let item = QueueItem {
            id: item_id,
            repository_id: state.id,
            kind: QueueItemKind::Release,
            admission_sequence: Some(sequence),
            enqueue_sequence: sequence,
            source_oid: state.staging_oid.clone(),
            source_ref: format!("refs/tollgate/sources/{item_id}"),
            metadata: SourceMetadata {
                subject: "tip".into(),
                message_hash: "message".into(),
                author_name: "Tollgate Test".into(),
                author_email: "test@example.com".into(),
                branch: None,
                worktree_path: None,
                signature_state: SignatureState::Unknown,
                approved_at: OffsetDateTime::now_utc(),
                purpose: Some("release".into()),
            },
            state: QueueItemState::Queued,
            terminal_reason: None,
            remote_state: RemoteState::Disabled,
            cleanup_state: CleanupState::NotEligible,
            cleanup_policy: CleanupPolicy::Automatic,
            dependencies: Vec::new(),
            retry_of_item_id: None,
            promotion_authorized: false,
            promotion_authorized_at: None,
            promotion_authorized_by: None,
            current_generation_id: Some(generation.id),
            buildset_id: None,
            certificate_id: None,
            release_fix: false,
        };
        (item, generation)
    }

    #[test]
    fn release_advances_settle_their_single_intent_with_the_projection_and_one_event() {
        let store = RepositoryStore::open_in_memory().unwrap();
        store.set_release_stage_configured(true);
        let mut state = lagging_state("release-advance");
        store.initialize_repository(&state).unwrap();
        let command = CommandId::new();
        store
            .prepare_operation(
                state.id,
                RELEASE_ADVANCE_INTENT_KIND,
                command,
                &serde_json::json!({"new": state.staging_oid}),
            )
            .unwrap();
        // The local advance: `release` reaches `staging` while the push is still owed.
        state.release_oid = state.staging_oid.clone();
        state.release_lag = ReleaseLag::default();
        state.release_state = ReleaseState::Green;
        let advanced = store
            .record_release_advance(
                &state,
                command,
                IntentState::ExternalApplied,
                &serde_json::json!({"release": state.release_oid}),
                Actor::App,
                "release.advanced",
                serde_json::json!({}),
            )
            .unwrap();
        assert_eq!(advanced.sequence, state.event_sequence + 1);
        assert_eq!(
            store.repository_state().unwrap().release_oid,
            state.release_oid
        );
        let unfinished = store
            .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])
            .unwrap();
        assert_eq!(unfinished.len(), 1);
        assert_eq!(unfinished[0].3, IntentState::ExternalApplied);
        // A blocked push stays recoverable, then completes.
        state.event_sequence = advanced.sequence;
        let blocked = store
            .record_release_advance(
                &state,
                command,
                IntentState::NeedsAttention,
                &serde_json::json!({"push": "blocked"}),
                Actor::App,
                "release.push-blocked",
                serde_json::json!({}),
            )
            .unwrap();
        assert_eq!(blocked.sequence, advanced.sequence + 1);
        assert_eq!(
            store
                .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])
                .unwrap()[0]
                .3,
            IntentState::NeedsAttention
        );
        store
            .record_release_advance(
                &state,
                command,
                IntentState::Completed,
                &serde_json::json!({"remote": state.release_oid}),
                Actor::App,
                "release.pushed",
                serde_json::json!({}),
            )
            .unwrap();
        assert!(
            store
                .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])
                .unwrap()
                .is_empty()
        );
        // A settled intent never transitions again, and nothing is written.
        let sequence = store.repository_state().unwrap().event_sequence;
        assert!(
            store
                .record_release_advance(
                    &state,
                    command,
                    IntentState::Completed,
                    &serde_json::json!({}),
                    Actor::App,
                    "release.pushed",
                    serde_json::json!({}),
                )
                .is_err()
        );
        assert_eq!(store.repository_state().unwrap().event_sequence, sequence);
    }

    #[test]
    fn release_triggers_retire_queue_and_settle_every_intent_in_one_transaction() {
        let store = RepositoryStore::open_in_memory().unwrap();
        store.set_release_stage_configured(true);
        let mut state = lagging_state("release-trigger");
        store.initialize_repository(&state).unwrap();
        for _ in 0..2 {
            store
                .prepare_operation(
                    state.id,
                    RELEASE_RUN_INTENT_KIND,
                    CommandId::new(),
                    &serde_json::json!({"staging_oid": state.staging_oid}),
                )
                .unwrap();
        }
        let (first, first_generation) = release_run(&state, 1);
        let events = store
            .record_release_trigger(&ReleaseTrigger {
                command: None,
                state: &state,
                retired: &[],
                queued: Some((&first, &first_generation)),
                outcome: serde_json::json!({"action": "queued"}),
            })
            .unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["release.run-queued"]
        );
        assert_eq!(
            events[0].payload["range"]["from"],
            serde_json::json!(oid(1))
        );
        assert_eq!(events[0].payload["range"]["to"], serde_json::json!(oid(2)));
        assert!(
            store
                .unfinished_operations(&[RELEASE_RUN_INTENT_KIND])
                .unwrap()
                .is_empty(),
            "one trigger settles every outstanding intent"
        );
        state.event_sequence = events[0].sequence;
        assert_eq!(store.repository_state().unwrap(), state);

        state.staging_oid = oid(3);
        state.release_lag.commits = 2;
        store.update_repository_state(&state).unwrap();
        store
            .prepare_operation(
                state.id,
                RELEASE_RUN_INTENT_KIND,
                CommandId::new(),
                &serde_json::json!({"staging_oid": state.staging_oid}),
            )
            .unwrap();
        let mut superseded = first.clone();
        superseded.state = QueueItemState::Superseded;
        superseded.terminal_reason = Some("superseded-by-newer-staging-tip".into());
        let (second, second_generation) = release_run(&state, 2);
        let events = store
            .record_release_trigger(&ReleaseTrigger {
                command: None,
                state: &state,
                retired: std::slice::from_ref(&superseded),
                queued: Some((&second, &second_generation)),
                outcome: serde_json::json!({"action": "retargeted"}),
            })
            .unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["release.run-superseded", "release.run-queued"]
        );
        assert_eq!(events[1].sequence, events[0].sequence + 1);
        let items = store.queue_items().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].state, QueueItemState::Superseded);
        assert_eq!(items[1].source_oid, oid(3));
        assert!(
            store
                .unfinished_operations(&[RELEASE_RUN_INTENT_KIND])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.repository_state().unwrap().event_sequence,
            events[1].sequence
        );
    }

    #[test]
    fn a_release_trigger_commits_its_command_result_with_the_queued_run() {
        let store = RepositoryStore::open_in_memory().unwrap();
        store.set_release_stage_configured(true);
        let state = lagging_state("release-retry");
        store.initialize_repository(&state).unwrap();
        let (run, generation) = release_run(&state, 1);
        let command_id = CommandId::new();
        let response = serde_json::json!({"action": "queued", "item_id": run.id});
        let events = store
            .record_release_trigger(&ReleaseTrigger {
                command: Some(ReleaseTriggerCommand {
                    command_id,
                    command_kind: "release-retry",
                    request_digest: "digest",
                    response: response.clone(),
                }),
                state: &state,
                retired: &[],
                queued: Some((&run, &generation)),
                outcome: serde_json::json!({"action": "retried"}),
            })
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(store.queue_items().unwrap().len(), 1);
        assert_eq!(
            store
                .checked_command_response::<serde_json::Value>(
                    command_id,
                    "release-retry",
                    "digest"
                )
                .unwrap(),
            Some(response)
        );
        assert!(matches!(
            store.checked_command_response::<serde_json::Value>(
                command_id,
                "release-retry",
                "other"
            ),
            Err(StoreError::CommandReplayMismatch)
        ));
    }

    #[test]
    fn a_release_stage_lets_release_trail_staging_in_persisted_state() {
        let store = RepositoryStore::open_in_memory().unwrap();
        store.set_release_stage_configured(true);
        let state = lagging_state("release-stage");
        store.initialize_repository(&state).unwrap();
        assert_eq!(store.repository_state().unwrap(), state);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "opt-out equivalence violated")]
    fn opt_out_repositories_never_persist_a_trailing_release() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let _ = store.initialize_repository(&lagging_state("opt-out"));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "release projection inconsistent")]
    fn persisted_release_projections_agree_with_the_refs() {
        let store = RepositoryStore::open_in_memory().unwrap();
        store.set_release_stage_configured(true);
        let mut state = lagging_state("inconsistent");
        state.release_state = ReleaseState::Green;
        let _ = store.initialize_repository(&state);
    }

    #[test]
    fn stale_item_projections_allocate_event_sequences_from_durable_state() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = RepositoryState {
            id: RepositoryId::new(),
            name: "projection-race".into(),
            path: "/projection-race".into(),
            staging_ref: "refs/heads/staging".into(),
            staging_oid: oid(1),
            release_ref: "refs/heads/release".into(),
            release_oid: oid(1),
            release_lag: ReleaseLag::default(),
            release_state: ReleaseState::Green,
            queue_revision: 0,
            event_sequence: 0,
            engine_epoch: 1,
            execution_state: RepositoryExecutionState::Active,
            block_reasons: Vec::new(),
            active_configuration_digest: "digest".into(),
            active_window: 20,
            active_window_floor: 3,
            active_window_ceiling: 20,
            remote_enabled: false,
            release_block_reasons: Vec::new(),
            promotion_pause: None,
        };
        store.initialize_repository(&state).unwrap();
        let item_id = QueueItemId::new();
        let source_oid = oid(2);
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            state.staging_oid.clone(),
            vec![item_id],
            vec![source_oid.clone()],
            vec![source_oid.clone()],
            state.staging_oid.clone(),
            source_oid.clone(),
            "digest".into(),
            "steps".into(),
            state.engine_epoch,
        );
        let mut item = QueueItem {
            id: item_id,
            repository_id: state.id,
            kind: QueueItemKind::Gate,
            admission_sequence: Some(1),
            enqueue_sequence: 1,
            source_oid,
            source_ref: format!("refs/tollgate/sources/{item_id}"),
            metadata: SourceMetadata {
                subject: "candidate".into(),
                message_hash: "message".into(),
                author_name: "Tollgate Test".into(),
                author_email: "test@example.com".into(),
                branch: Some("task".into()),
                worktree_path: Some("/projection-race/task".into()),
                signature_state: SignatureState::Unknown,
                approved_at: OffsetDateTime::now_utc(),
                purpose: Some("candidate".into()),
            },
            state: QueueItemState::Queued,
            terminal_reason: None,
            remote_state: RemoteState::Disabled,
            cleanup_state: CleanupState::NotEligible,
            cleanup_policy: CleanupPolicy::Automatic,
            dependencies: Vec::new(),
            retry_of_item_id: None,
            promotion_authorized: false,
            promotion_authorized_at: None,
            promotion_authorized_by: None,
            current_generation_id: Some(generation.id),
            buildset_id: None,
            certificate_id: None,
            release_fix: false,
        };
        let command_id = CommandId::new();
        store
            .prepare_approval(state.id, &item, command_id, "request")
            .unwrap();
        store
            .complete_approval(
                &item,
                Some(&generation),
                0,
                Actor::Cli,
                command_id,
                "candidate",
                "request",
                &serde_json::json!({"item_id": item_id}),
            )
            .unwrap();

        item.terminal_reason = Some("first".into());
        let first = store.save_item_projection(&state, &item).unwrap();
        item.terminal_reason = Some("second".into());
        let second = store.save_item_projection(&state, &item).unwrap();

        assert_eq!(first.sequence, 2);
        assert_eq!(second.sequence, 3);
        assert_eq!(store.repository_state().unwrap().event_sequence, 3);
    }

    /// Statement-level counters from SQLite's trace hook for the current thread.
    #[derive(Clone, Copy, Debug, Default)]
    struct StatementTally {
        statements: u64,
        rows: u64,
        fullscan_steps: u64,
        vm_steps: u64,
    }

    thread_local! {
        static TALLY: std::cell::Cell<StatementTally> = std::cell::Cell::new(StatementTally::default());
    }

    fn tally_statement(event: rusqlite::trace::TraceEvent<'_>) {
        TALLY.with(|cell| {
            let mut tally = cell.get();
            match event {
                rusqlite::trace::TraceEvent::Profile(statement, _) => {
                    tally.statements += 1;
                    tally.fullscan_steps += u64::try_from(
                        statement.get_status(rusqlite::StatementStatus::FullscanStep),
                    )
                    .unwrap_or(0);
                    tally.vm_steps +=
                        u64::try_from(statement.get_status(rusqlite::StatementStatus::VmStep))
                            .unwrap_or(0);
                }
                rusqlite::trace::TraceEvent::Row(_) => tally.rows += 1,
                _ => {}
            }
            cell.set(tally);
        });
    }

    /// Runs `operation` with statement tracing enabled and returns what SQLite executed. Status
    /// counters of a statement executed repeatedly accumulate, so `vm_steps` is exact only for
    /// operations that run each prepared statement once; `fullscan_steps` is zero either way
    /// when no statement scans a table.
    fn traced<T>(store: &RepositoryStore, operation: impl FnOnce() -> T) -> (T, StatementTally) {
        TALLY.with(|cell| cell.set(StatementTally::default()));
        store.connection.lock().trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_PROFILE
                | rusqlite::trace::TraceEventCodes::SQLITE_TRACE_ROW,
            Some(tally_statement),
        );
        let result = operation();
        store
            .connection
            .lock()
            .trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
        (result, TALLY.with(std::cell::Cell::get))
    }

    struct SyntheticArtifacts {
        buildset_id: tollgate_domain::BuildsetId,
        expired: Vec<String>,
        unexpired: Vec<String>,
        completed_commands: Vec<CommandId>,
    }

    /// Loads `artifacts` retained artifacts (the first `expired` of them past expiry) and
    /// `intents` completed single-artifact pruning intents, the shape of a long-lived store.
    fn load_synthetic_artifacts(
        store: &RepositoryStore,
        state: &RepositoryState,
        artifacts: usize,
        expired: usize,
        intents: usize,
    ) -> SyntheticArtifacts {
        let buildset_id = tollgate_domain::BuildsetId::new();
        let now = OffsetDateTime::now_utc();
        let mut result = SyntheticArtifacts {
            buildset_id,
            expired: Vec::new(),
            unexpired: Vec::new(),
            completed_commands: Vec::new(),
        };
        let mut connection = store.connection.lock();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "INSERT INTO buildsets (buildset_id, item_id, generation_id, tested_format, tested_oid, expected_parent_oid, environment_fingerprint, slot_id, status, retry_of_buildset_id, attempt, buildset_json) VALUES (?1, NULL, NULL, 'sha1', x'00', x'00', 'environment', NULL, 'succeeded', NULL, 1, '{}')",
                [buildset_id.to_string()],
            )
            .unwrap();
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO artifacts (artifact_id, buildset_id, step_id, source_path, retained_path, hash, size, retention_state, created_at, expires_at) VALUES (?1, ?2, NULL, ?3, ?4, ?5, 8, 'retained', ?6, ?7)",
                )
                .unwrap();
            for index in 0..artifacts {
                let artifact_id = uuid::Uuid::now_v7().to_string();
                let expires_at = if index < expired {
                    now - time::Duration::days(1) - time::Duration::seconds(index as i64)
                } else {
                    now + time::Duration::days(30) + time::Duration::seconds(index as i64)
                };
                insert
                    .execute(params![
                        artifact_id,
                        buildset_id.to_string(),
                        format!("artifacts/{index}.txt"),
                        format!("/artifacts/{buildset_id}/{index}.txt"),
                        format!("{index:064x}"),
                        encode_time(now - time::Duration::days(31)),
                        encode_time(expires_at),
                    ])
                    .unwrap();
                if index < expired {
                    result.expired.push(artifact_id);
                } else {
                    result.unexpired.push(artifact_id);
                }
            }
            let mut insert = transaction
                .prepare(
                    "INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, observed_json, created_at, updated_at) VALUES (?1, ?2, 'artifact-prune', 'completed', ?3, '{}', '{}', ?4, ?4)",
                )
                .unwrap();
            for index in 0..intents {
                let command_id = CommandId::new();
                insert
                    .execute(params![
                        uuid::Uuid::now_v7().to_string(),
                        state.id.to_string(),
                        command_id.to_string(),
                        (index as i64).to_string(),
                    ])
                    .unwrap();
                if index % 10_000 == 0 {
                    result.completed_commands.push(command_id);
                }
            }
        }
        transaction.commit().unwrap();
        result
    }

    /// Returns the database to its version 3 shape: no pruning indexes, no attention table, and
    /// no planner statistics.
    fn downgrade_to_version_three(path: &Path) {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "DROP INDEX operation_intent_command;
                 DROP INDEX operation_intent_state;
                 DROP INDEX artifact_retention_expiry;
                 DROP TABLE artifact_attention;
                 DROP TABLE IF EXISTS sqlite_stat1;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 3).unwrap();
    }

    fn assert_bounded_pruning_statements(
        store: &RepositoryStore,
        state: &mut RepositoryState,
        synthetic: &SyntheticArtifacts,
        expired_offset: usize,
    ) {
        let target = &synthetic.unexpired[synthetic.unexpired.len() / 2];
        let (record, lookup) = traced(store, || store.artifact(target).unwrap());
        assert_eq!(record.unwrap().artifact_id, *target);
        assert_eq!(lookup.statements, 1, "{lookup:?}");
        assert_eq!(lookup.rows, 1, "a lookup returns its one row: {lookup:?}");
        assert_eq!(lookup.fullscan_steps, 0, "a lookup never scans: {lookup:?}");
        assert!(
            lookup.vm_steps < 100,
            "a lookup reads one row, not the table: {lookup:?}"
        );

        let command = synthetic.completed_commands[1];
        let (evidence, intent_lookup) = traced(store, || {
            store.operation_evidence(command, "artifact-prune").unwrap()
        });
        assert!(evidence.is_some());
        assert_eq!(intent_lookup.statements, 1, "{intent_lookup:?}");
        assert_eq!(intent_lookup.fullscan_steps, 0, "{intent_lookup:?}");
        assert!(intent_lookup.vm_steps < 100, "{intent_lookup:?}");

        let batch_size = 200;
        let command_id = CommandId::new();
        let (pruned, prune) = traced(store, || {
            let batch = store
                .expired_artifacts(OffsetDateTime::now_utc(), &[], batch_size)
                .unwrap();
            store
                .prepare_unique_operation(
                    state.id,
                    ARTIFACT_PRUNE_BATCH_KIND,
                    command_id,
                    &serde_json::json!({"artifacts": batch}),
                )
                .unwrap();
            let pruned = batch
                .iter()
                .map(|record| record.artifact_id.clone())
                .collect::<Vec<_>>();
            let event = store
                .complete_artifact_prune_batch(
                    state,
                    command_id,
                    &pruned,
                    &[],
                    None,
                    &serde_json::json!({"pruned": pruned}),
                    Actor::App,
                )
                .unwrap();
            state.event_sequence = event.sequence;
            pruned
        });
        assert_eq!(pruned.len(), batch_size);
        assert_eq!(
            pruned,
            synthetic.expired[expired_offset..expired_offset + batch_size]
                .iter()
                .rev()
                .cloned()
                .collect::<Vec<_>>(),
            "the oldest expiry is pruned first"
        );
        let per_artifact = 2;
        let per_batch = 16;
        assert!(
            prune.statements <= (per_artifact * batch_size + per_batch) as u64,
            "a batch prune issues a bounded number of statements per artifact: {prune:?}"
        );
        assert_eq!(
            prune.fullscan_steps, 0,
            "no pruning statement scans a table: {prune:?}"
        );
        assert!(
            prune.rows <= (batch_size + per_batch) as u64,
            "only selecting the batch returns a row per artifact: {prune:?}"
        );
    }

    #[test]
    fn large_store_lookups_and_batch_prunes_issue_bounded_indexed_statements() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("state.sqlite3");
        let mut state = test_state("large");
        let synthetic = {
            let store = RepositoryStore::open(&path).unwrap();
            store.initialize_repository(&state).unwrap();
            let synthetic = load_synthetic_artifacts(&store, &state, 100_000, 23_000, 90_000);
            // A database created at the current schema has its indexes but no statistics.
            assert_bounded_pruning_statements(&store, &mut state, &synthetic, 22_800);
            synthetic
        };
        downgrade_to_version_three(&path);

        let store = RepositoryStore::open(&path).unwrap();
        // The version 4 migration adds the indexes and ANALYZE statistics.
        assert_bounded_pruning_statements(&store, &mut state, &synthetic, 22_600);
    }

    #[test]
    fn version_three_migration_adds_pruning_indexes_statistics_and_a_backup() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("state.sqlite3");
        {
            let store = RepositoryStore::open(&path).unwrap();
            let state = test_state("migrated");
            store.initialize_repository(&state).unwrap();
            load_synthetic_artifacts(&store, &state, 50, 10, 50);
        }
        downgrade_to_version_three(&path);
        assert!(RepositoryStore::migration_allowance(&path).unwrap() > 0);

        let store = RepositoryStore::open(&path).unwrap();
        assert_eq!(RepositoryStore::migration_allowance(&path).unwrap(), 0);
        let connection = store.connection.lock();
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        for index in [
            "operation_intent_command",
            "operation_intent_state",
            "artifact_retention_expiry",
        ] {
            let analyzed: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_stat1 WHERE idx=?1)",
                    [index],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(analyzed, "{index} has planner statistics");
        }
        let attention: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='artifact_attention')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(attention);
        let migration: String = connection
            .query_row(
                "SELECT migration_id FROM backup_records WHERE migration_id IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migration, format!("schema-3-to-{SCHEMA_VERSION}"));
        drop(connection);
        let backups = std::fs::read_dir(temporary.path().join("backups"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("migration-v3-")
            })
            .count();
        assert_eq!(backups, 1);
    }

    #[test]
    fn pruning_batches_record_attention_and_skip_it_until_an_explicit_prune_succeeds() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let mut state = test_state("attention");
        store.initialize_repository(&state).unwrap();
        let synthetic = load_synthetic_artifacts(&store, &state, 6, 4, 0);
        let now = OffsetDateTime::now_utc();
        assert_eq!(
            store.expired_artifacts(now, &[], 10).unwrap().len(),
            4,
            "only expired artifacts are eligible"
        );
        assert!(
            store
                .expired_artifacts(now, &[synthetic.buildset_id], 10)
                .unwrap()
                .is_empty(),
            "artifacts of an excluded buildset are never selected"
        );

        let failing = synthetic.expired[0].clone();
        let pruned = synthetic.expired[1..].to_vec();
        let command_id = CommandId::new();
        store
            .prepare_unique_operation(
                state.id,
                ARTIFACT_PRUNE_BATCH_KIND,
                command_id,
                &serde_json::json!({}),
            )
            .unwrap();
        assert!(matches!(
            store.prepare_unique_operation(
                state.id,
                ARTIFACT_PRUNE_BATCH_KIND,
                command_id,
                &serde_json::json!({}),
            ),
            Err(StoreError::CommandAlreadyRecorded(_))
        ));
        let attention = vec![ArtifactAttention {
            artifact_id: failing.clone(),
            detail: "hash mismatch".into(),
        }];
        let event = store
            .complete_artifact_prune_batch(
                &state,
                command_id,
                &pruned,
                &attention,
                None,
                &serde_json::json!({}),
                Actor::App,
            )
            .unwrap();
        state.event_sequence = event.sequence;
        assert_eq!(event.kind, "artifact.pruned");
        assert!(
            store
                .unfinished_operations(&[ARTIFACT_PRUNE_BATCH_KIND])
                .unwrap()
                .is_empty()
        );
        assert!(
            store.expired_artifacts(now, &[], 10).unwrap().is_empty(),
            "an artifact that needs attention is not retried automatically"
        );
        assert_eq!(
            store.artifacts_needing_attention(10).unwrap(),
            (1, attention)
        );
        assert_eq!(
            store.artifact(&failing).unwrap().unwrap().retention_state,
            "retained"
        );
        assert!(store.artifact(&pruned[0]).unwrap().is_none());
        assert_eq!(
            store
                .artifact_record(&pruned[0])
                .unwrap()
                .unwrap()
                .retention_state,
            "pruned"
        );

        let explicit = CommandId::new();
        store
            .prepare_unique_operation(
                state.id,
                ARTIFACT_PRUNE_BATCH_KIND,
                explicit,
                &serde_json::json!({}),
            )
            .unwrap();
        store
            .complete_artifact_prune_batch(
                &state,
                explicit,
                std::slice::from_ref(&failing),
                &[],
                Some("digest"),
                &serde_json::json!({"message": "pruned"}),
                Actor::Cli,
            )
            .unwrap();
        assert_eq!(store.artifacts_needing_attention(10).unwrap().0, 0);
        assert_eq!(
            store
                .checked_command_response::<serde_json::Value>(explicit, "artifact-prune", "digest")
                .unwrap(),
            Some(serde_json::json!({"message": "pruned"}))
        );
    }

    /// Measures batched pruning throughput on a file-backed synthetic store shaped like the
    /// 2026-10-05 incident (100,000 retained artifacts, 23,000 expired, 90,000 completed
    /// intents). It times database work only, so it is a measurement rather than a gate:
    /// `cargo test --release -p tollgate-store measure_artifact_prune_throughput -- --ignored --nocapture`
    #[test]
    #[ignore = "measurement; run manually"]
    fn measure_artifact_prune_throughput() {
        let temporary = tempfile::tempdir().unwrap();
        let store = RepositoryStore::open(temporary.path().join("state.sqlite3")).unwrap();
        let mut state = test_state("measured");
        store.initialize_repository(&state).unwrap();
        load_synthetic_artifacts(&store, &state, 100_000, 23_000, 90_000);
        let batches = 100;
        let started = Instant::now();
        let mut pruned_total = 0;
        for _ in 0..batches {
            assert!(
                store
                    .unfinished_operations(&[ARTIFACT_PRUNE_BATCH_KIND])
                    .unwrap()
                    .is_empty()
            );
            let batch = store
                .expired_artifacts(OffsetDateTime::now_utc(), &[], 200)
                .unwrap();
            let command_id = CommandId::new();
            store
                .prepare_unique_operation(
                    state.id,
                    ARTIFACT_PRUNE_BATCH_KIND,
                    command_id,
                    &serde_json::json!({"entries": batch}),
                )
                .unwrap();
            let pruned = batch
                .iter()
                .map(|record| record.artifact_id.clone())
                .collect::<Vec<_>>();
            let event = store
                .complete_artifact_prune_batch(
                    &state,
                    command_id,
                    &pruned,
                    &[],
                    None,
                    &serde_json::json!({"pruned": pruned}),
                    Actor::App,
                )
                .unwrap();
            state.event_sequence = event.sequence;
            pruned_total += pruned.len();
        }
        let elapsed = started.elapsed();
        println!(
            "batched pruning: {pruned_total} artifacts in {:.3} s = {:.0} prunes/s",
            elapsed.as_secs_f64(),
            pruned_total as f64 / elapsed.as_secs_f64()
        );
    }
}
