use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    BuildsetId, BuildsetState, CertificateId, CleanupState, GitOid, QueueItemId, QueueItemState,
    RemoteState, RepositoryExecutionState, RepositoryId, SlotId, StepAttemptId, StepId,
    ValidationGenerationId,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueItemKind {
    #[default]
    Gate,
    IndependentCheck,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryState {
    pub id: RepositoryId,
    pub name: String,
    pub path: String,
    pub integration_ref: String,
    pub master_oid: GitOid,
    pub queue_revision: u64,
    pub event_sequence: u64,
    pub engine_epoch: u64,
    pub execution_state: RepositoryExecutionState,
    pub block_reasons: Vec<BlockReason>,
    pub active_configuration_digest: String,
    pub active_window: u16,
    pub active_window_floor: u16,
    pub active_window_ceiling: u16,
    pub remote_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockReason {
    pub code: String,
    pub message: String,
    pub recovery_action: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub subject: String,
    pub message_hash: String,
    pub author_name: String,
    pub author_email: String,
    pub branch: Option<String>,
    pub worktree_path: Option<String>,
    pub signature_state: SignatureState,
    pub approved_at: OffsetDateTime,
    #[serde(default)]
    pub purpose: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SignatureState {
    Verified,
    Invalid,
    Unknown,
    Unsigned,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueItem {
    pub id: QueueItemId,
    pub repository_id: RepositoryId,
    #[serde(default)]
    pub kind: QueueItemKind,
    pub enqueue_sequence: u64,
    pub source_oid: GitOid,
    pub source_ref: String,
    pub metadata: SourceMetadata,
    pub state: QueueItemState,
    pub terminal_reason: Option<String>,
    pub remote_state: RemoteState,
    pub cleanup_state: CleanupState,
    pub dependencies: Vec<QueueItemId>,
    pub current_generation_id: Option<ValidationGenerationId>,
    pub buildset_id: Option<BuildsetId>,
    pub certificate_id: Option<CertificateId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationGeneration {
    pub id: ValidationGenerationId,
    pub item_id: QueueItemId,
    pub anchored_base_oid: GitOid,
    pub ordered_item_ids: Vec<QueueItemId>,
    pub ordered_source_oids: Vec<GitOid>,
    pub prefix_oids: Vec<GitOid>,
    pub expected_parent_oid: GitOid,
    pub tested_oid: GitOid,
    pub configuration_digest: String,
    pub step_graph_digest: String,
    pub engine_epoch: u64,
    pub identity_digest: String,
    pub invalidated_by: Option<ValidationGenerationId>,
}

impl ValidationGeneration {
    #[allow(clippy::too_many_arguments)]
    pub fn derive(
        id: ValidationGenerationId,
        item_id: QueueItemId,
        anchored_base_oid: GitOid,
        ordered_item_ids: Vec<QueueItemId>,
        ordered_source_oids: Vec<GitOid>,
        prefix_oids: Vec<GitOid>,
        expected_parent_oid: GitOid,
        tested_oid: GitOid,
        configuration_digest: String,
        step_graph_digest: String,
        engine_epoch: u64,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(anchored_base_oid.as_bytes());
        for (id, oid) in ordered_item_ids.iter().zip(&ordered_source_oids) {
            hasher.update(id.0.as_bytes());
            hasher.update(oid.as_bytes());
        }
        for oid in &prefix_oids {
            hasher.update(oid.as_bytes());
        }
        hasher.update(configuration_digest.as_bytes());
        hasher.update(step_graph_digest.as_bytes());
        hasher.update(&engine_epoch.to_be_bytes());
        let identity_digest = hasher.finalize().to_hex().to_string();
        Self {
            id,
            item_id,
            anchored_base_oid,
            ordered_item_ids,
            ordered_source_oids,
            prefix_oids,
            expected_parent_oid,
            tested_oid,
            configuration_digest,
            step_graph_digest,
            engine_epoch,
            identity_digest,
            invalidated_by: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Buildset {
    pub id: BuildsetId,
    pub item_id: QueueItemId,
    pub validation_generation_id: ValidationGenerationId,
    pub tested_oid: GitOid,
    pub expected_parent_oid: GitOid,
    pub environment_fingerprint: String,
    pub slot_id: Option<SlotId>,
    pub state: BuildsetState,
    pub retry_of: Option<BuildsetId>,
    pub attempt: u16,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub frozen_steps: Vec<FrozenStep>,
    #[serde(default)]
    pub step_results: Vec<BuildsetStepResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildsetStepResult {
    pub name: String,
    pub result_class: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub elapsed_ms: u64,
    pub log_hash: String,
    pub stdout_end: u64,
    pub stderr_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrozenStep {
    pub id: StepId,
    pub name: String,
    pub command: FrozenCommand,
    pub working_directory: String,
    pub needs: Vec<StepId>,
    pub soft_needs: Vec<StepId>,
    pub voting: bool,
    pub final_step: bool,
    pub timeout_ns: u64,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub rss_limit_bytes: Option<u64>,
    pub semaphores: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FrozenCommand {
    Shell { runner: Vec<String>, script: String },
    Argv { argv: Vec<String> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SuccessfulStepResult {
    pub step_id: StepId,
    pub attempt_id: StepAttemptId,
    pub log_stdout_end: u64,
    pub log_stderr_end: u64,
    pub log_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PassCertificate {
    pub id: CertificateId,
    pub buildset_id: BuildsetId,
    pub queue_item_id: QueueItemId,
    pub validation_generation_id: ValidationGenerationId,
    pub tested_oid: GitOid,
    pub tree_oid: GitOid,
    pub expected_parent_oid: GitOid,
    pub configuration_digest: String,
    pub step_graph_digest: String,
    pub engine_epoch: u64,
    pub environment_fingerprint: String,
    pub voting_results: Vec<SuccessfulStepResult>,
    pub warnings: Vec<String>,
    pub checkout_verified: bool,
    pub completed_event_sequence: u64,
    pub created_at: OffsetDateTime,
}

impl PassCertificate {
    pub fn validates(
        &self,
        item: &QueueItem,
        generation: &ValidationGeneration,
        observed_master: &GitOid,
        config_digest: &str,
        step_graph_digest: &str,
        engine_epoch: u64,
    ) -> bool {
        item.id == self.queue_item_id
            && item.current_generation_id == Some(self.validation_generation_id)
            && generation.id == self.validation_generation_id
            && self.tested_oid == generation.tested_oid
            && self.expected_parent_oid == generation.expected_parent_oid
            && self.configuration_digest == generation.configuration_digest
            && self.step_graph_digest == generation.step_graph_digest
            && self.expected_parent_oid == *observed_master
            && self.configuration_digest == config_digest
            && self.step_graph_digest == step_graph_digest
            && self.engine_epoch == engine_epoch
            && self.checkout_verified
    }
}
