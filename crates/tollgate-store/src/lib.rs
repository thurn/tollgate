#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
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

const SCHEMA_VERSION: i64 = 2;
pub type PromotionEdge = (Vec<u8>, Vec<u8>);

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
    #[error("database schema version {actual} is newer than this Tollgate supports ({supported})")]
    SchemaTooNew { actual: i64, supported: i64 },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct RepositoryStore {
    connection: Arc<Mutex<Connection>>,
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
        Ok(database_bytes.saturating_add(512 * 1024 * 1024))
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
        })
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
        let json = encode(state)?;
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
            params![state.id.to_string(), encode(state)?, state.queue_revision as i64, state.event_sequence as i64, SCHEMA_VERSION, state.engine_epoch as i64, state.active_configuration_digest, now()],
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
            params![state.id.to_string(), encode(state)?, state.queue_revision as i64, state.event_sequence as i64, state.engine_epoch as i64, state.active_configuration_digest, now()],
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
            params![state.id.to_string(), encode(state)?, state.queue_revision as i64, state.event_sequence as i64, state.active_configuration_digest, now()],
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
        transaction.execute("UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1", params![item.repository_id.to_string(), encode(&persisted_state)?, new_revision, new_sequence, now()])?;
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
            params![item.repository_id.to_string(), encode(&persisted_state)?, revision, new_sequence, now()],
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
        transaction.execute("UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1", params![state.id.to_string(), encode(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()])?;
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
            params![state.id.to_string(), encode(&persisted_state)?, new_revision, new_sequence, now()],
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
            params![state.id.to_string(), encode(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()],
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

    pub fn record_artifact(
        &self,
        buildset_id: tollgate_domain::BuildsetId,
        source_path: &Path,
        retained_path: &Path,
        hash: &str,
        size: u64,
    ) -> Result<(), StoreError> {
        let size = i64::try_from(size).map_err(|_| StoreError::ArtifactTooLarge(size))?;
        let created_at = OffsetDateTime::now_utc();
        let expires_at = created_at + time::Duration::days(30);
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
            params![state.id.to_string(), encode(&persisted_state)?, sequence as i64, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='artifact' AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(observed)?, now(), command_id.to_string()],
        )?;
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
            payload: serde_json::to_value(records)?,
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
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT artifact_id, buildset_id, source_path, retained_path, hash, size, retention_state, created_at, expires_at FROM artifacts WHERE retention_state IN ('retained','pinned') ORDER BY retained_path",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;
        let mut records = Vec::new();
        for row in rows {
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
            ) = row?;
            records.push(ArtifactRecord {
                artifact_id,
                buildset_id: buildset_id.parse().map_err(|error| {
                    StoreError::Integrity(format!("invalid artifact buildset ID: {error}"))
                })?,
                source_path,
                retained_path,
                hash,
                size: u64::try_from(size).map_err(|_| {
                    StoreError::Integrity("artifact has a negative retained size".into())
                })?,
                retention_state,
                created_at: decode_time(&created_at)?,
                expires_at: decode_time(&expires_at)?,
            });
        }
        Ok(records)
    }

    pub fn artifact(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, StoreError> {
        Ok(self
            .retained_artifacts()?
            .into_iter()
            .find(|record| record.artifact_id == artifact_id))
    }

    pub fn expired_artifacts(
        &self,
        now: OffsetDateTime,
    ) -> Result<Vec<ArtifactRecord>, StoreError> {
        Ok(self
            .retained_artifacts()?
            .into_iter()
            .filter(|record| record.retention_state == "retained" && record.expires_at <= now)
            .collect())
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
            params![state.id.to_string(), encode(&persisted_state)?, sequence as i64, now()],
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
            "INSERT INTO cache_manifests (seed_id, hash, entry_count, manifest_json) VALUES (?1, ?2, ?3, ?4)",
            params![seed.id, manifest_hash, entry_count, encode(&seed.manifest)?],
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
                "INSERT INTO cache_manifests (seed_id, hash, entry_count, manifest_json) VALUES (?1, ?2, ?3, ?4)",
                params![seed.id, manifest_hash, entry_count, encode(&seed.manifest)?],
            )?;
        }
        let sequence = state.event_sequence + 1;
        let mut persisted_state = state.clone();
        persisted_state.event_sequence = sequence;
        transaction.execute(
            "UPDATE repository_state SET state_json=?2, event_sequence=?3, updated_at=?4 WHERE repository_id=?1",
            params![state.id.to_string(), encode(&persisted_state)?, sequence as i64, now()],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE repository_id=?1 AND command_id=?4 AND kind='cache-snapshot' AND state IN ('prepared','external-applied')",
            params![state.id.to_string(), encode(seed)?, now(), command_id.to_string()],
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
            "SELECT manifest_json FROM seed_generations WHERE repository_id=?1 ORDER BY generation DESC",
        )?;
        let rows =
            statement.query_map([repository_id.to_string()], |row| row.get::<_, String>(0))?;
        rows.map(|row| decode(&row?)).collect()
    }

    pub fn mark_seed_pruned(&self, seed_id: &str) -> Result<(), StoreError> {
        let connection = self.connection.lock();
        let encoded: String = connection.query_row(
            "SELECT manifest_json FROM seed_generations WHERE seed_id=?1",
            [seed_id],
            |row| row.get(0),
        )?;
        let mut seed: SeedRecord = decode(&encoded)?;
        seed.state = "pruned".into();
        connection.execute(
            "UPDATE seed_generations SET state='pruned', manifest_json=?2 WHERE seed_id=?1",
            params![seed_id, encode(&seed)?],
        )?;
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
            params![state.id.to_string(), encode(&persisted_state)?, sequence as i64, now()],
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
        self.connection.lock().execute("INSERT INTO operation_intents (intent_id, repository_id, kind, state, command_id, expected_json, created_at, updated_at) VALUES (?1, ?2, 'promotion', 'prepared', ?3, ?4, ?5, ?5)", params![uuid::Uuid::now_v7().to_string(), repository_id.to_string(), command_id.to_string(), encode(evidence)?, now()])?;
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
            params![state.id.to_string(), encode(&persisted_state)?, state.queue_revision as i64, sequence as i64, now()],
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

    pub fn completed_operation_evidence(
        &self,
        kind: &str,
    ) -> Result<Vec<serde_json::Value>, StoreError> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT expected_json FROM operation_intents WHERE kind=?1 AND state='completed' ORDER BY created_at",
        )?;
        let rows = statement.query_map([kind], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
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

    pub fn record_promotion(
        &self,
        state: &RepositoryState,
        item: &QueueItem,
        certificate: &PassCertificate,
        old_master: &[u8],
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("INSERT OR REPLACE INTO source_promotions (item_id, source_oid, promoted_oid, old_master_oid, certificate_id, event_sequence) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![item.id.to_string(), item.source_oid.as_bytes(), certificate.tested_oid.as_bytes(), old_master, certificate.id.to_string(), state.event_sequence as i64])?;
        transaction.execute("UPDATE queue_items SET state=?2, remote_state=?3, cleanup_state=?4, item_json=?5, active=0 WHERE item_id=?1", params![item.id.to_string(), enum_json(&item.state)?, enum_json(&item.remote_state)?, enum_json(&item.cleanup_state)?, encode(item)?])?;
        transaction.execute("UPDATE repository_state SET state_json=?2, queue_revision=?3, event_sequence=?4, updated_at=?5 WHERE repository_id=?1", params![state.id.to_string(), encode(state)?, state.queue_revision as i64, state.event_sequence as i64, now()])?;
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

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<(), StoreError> {
        let source = self.connection.lock();
        let mut destination = Connection::open(destination)?;
        let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
        backup.run_to_completion(128, std::time::Duration::from_millis(10), None)?;
        drop(backup);
        let integrity: String =
            destination.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(StoreError::Integrity(format!(
                "online backup integrity check failed: {integrity}"
            )));
        }
        Ok(())
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
    ) -> Result<(), StoreError> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO backup_records (backup_id, database_identity, schema_version, path, hash, verified, migration_id) VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL)",
            params![uuid::Uuid::now_v7().to_string(), "repository-state", SCHEMA_VERSION, path.to_string_lossy(), hash],
        )?;
        transaction.execute(
            "UPDATE operation_intents SET state='completed', observed_json=?2, updated_at=?3 WHERE command_id=?1 AND kind='backup' AND state IN ('prepared','external-applied')",
            params![command_id.to_string(), encode(&serde_json::json!({"path": path, "hash": hash}))?, now()],
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
    let artifact_columns = if version == 1 {
        let mut statement = connection.prepare("PRAGMA table_info(artifacts)")?;
        statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<HashSet<_>, _>>()?
    } else {
        HashSet::new()
    };
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
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch("DROP INDEX IF EXISTS queue_active_source")?;
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
    let backup = rusqlite::backup::Backup::new(source, &mut output)?;
    backup.run_to_completion(128, std::time::Duration::from_millis(10), None)?;
    drop(backup);
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
CREATE TABLE IF NOT EXISTS cache_manifests (seed_id TEXT PRIMARY KEY REFERENCES seed_generations(seed_id), hash TEXT NOT NULL, entry_count INTEGER NOT NULL, manifest_json TEXT NOT NULL) STRICT;
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
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use tollgate_domain::{
        BlockReason, CleanupPolicy, CleanupState, GitOid, ObjectFormat, QueueItemId, QueueItemKind,
        QueueItemState, RemoteState, RepositoryExecutionState, SignatureState, SourceMetadata,
        ValidationGenerationId,
    };

    fn oid(value: u8) -> GitOid {
        GitOid::new(ObjectFormat::Sha1, vec![value; 20]).unwrap()
    }

    #[test]
    fn initializes_and_reads_repository_state() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = RepositoryState {
            id: RepositoryId::new(),
            name: "demo".into(),
            path: "/demo".into(),
            integration_ref: "refs/heads/master".into(),
            master_oid: oid(1),
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
        };
        store.initialize_repository(&state).unwrap();
        assert_eq!(store.repository_state().unwrap(), state);
        store.quick_integrity_check().unwrap();
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
            integration_ref: "refs/heads/master".into(),
            master_oid: oid(7),
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
            integration_ref: "refs/heads/release".into(),
            master_oid: oid(1),
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
        };
        store.initialize_repository(&state).unwrap();
        let item_id = QueueItemId::new();
        let source_oid = oid(2);
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            state.master_oid.clone(),
            vec![item_id],
            vec![source_oid.clone()],
            vec![source_oid.clone()],
            state.master_oid.clone(),
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
            promotion_authorized: false,
            promotion_authorized_at: None,
            promotion_authorized_by: None,
            current_generation_id: Some(generation.id),
            buildset_id: None,
            certificate_id: None,
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

    #[test]
    fn stale_item_projections_allocate_event_sequences_from_durable_state() {
        let store = RepositoryStore::open_in_memory().unwrap();
        let state = RepositoryState {
            id: RepositoryId::new(),
            name: "projection-race".into(),
            path: "/projection-race".into(),
            integration_ref: "refs/heads/release".into(),
            master_oid: oid(1),
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
        };
        store.initialize_repository(&state).unwrap();
        let item_id = QueueItemId::new();
        let source_oid = oid(2);
        let generation = ValidationGeneration::derive(
            ValidationGenerationId::new(),
            item_id,
            state.master_oid.clone(),
            vec![item_id],
            vec![source_oid.clone()],
            vec![source_oid.clone()],
            state.master_oid.clone(),
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
            promotion_authorized: false,
            promotion_authorized_at: None,
            promotion_authorized_by: None,
            current_generation_id: Some(generation.id),
            buildset_id: None,
            certificate_id: None,
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
}
