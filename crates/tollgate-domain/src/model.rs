use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    BuildsetId, BuildsetState, CertificateId, CleanupPolicy, CleanupState, CommandId, GitOid,
    QueueItemId, QueueItemState, RemoteState, RepositoryExecutionState, RepositoryId, SlotId,
    StepAttemptId, StepId, ValidationGenerationId,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueItemKind {
    #[default]
    Gate,
    IndependentCheck,
    /// A release run: the release-stage steps on one exact `staging` OID. It never enters the
    /// dependent gate queue and never promotes; it ends in `check-passed` or `check-failed`.
    Release,
}

/// The ref every repository registered before staged release used as its single integration
/// ref. Persisted state that still names it as `staging_ref` predates the `staging` ref.
pub const LEGACY_INTEGRATION_REF: &str = "refs/heads/release";

/// Durable repository projection.
///
/// `staging_ref` is the Tollgate-owned integration ref every promotion advances and new work
/// bases on. `release_ref` is the Tollgate-owned ref the remote push publishes. A repository
/// without release-stage steps keeps both at the same OID: every promotion updates them together
/// in one ref transaction (opt-out equivalence). A repository with release-stage steps promotes
/// to `staging` alone, so `release` may trail it; `release_lag` and `release_state` then describe
/// the unreleased range.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "PersistedRepositoryState")]
pub struct RepositoryState {
    pub id: RepositoryId,
    pub name: String,
    pub path: String,
    pub staging_ref: String,
    pub staging_oid: GitOid,
    pub release_ref: String,
    pub release_oid: GitOid,
    pub release_lag: ReleaseLag,
    pub release_state: ReleaseState,
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
    /// Conditions that hold back `release` advances without blocking the repository:
    /// `push-blocked` (a release push failed or diverged after the local advance) and
    /// `remote-preflight-mismatch` (the remote is not in Tollgate-certified history below the
    /// release target). `staging` promotions continue meanwhile.
    pub release_block_reasons: Vec<BlockReason>,
    /// Set while promotion to `staging` waits on `max_release_lag` (`release-lag`); gate
    /// validation continues.
    pub promotion_pause: Option<BlockReason>,
}

impl RepositoryState {
    /// Moves both Tollgate refs' projections to `oid`, as the opt-out two-ref transaction does
    /// for the refs themselves: `release` equals `staging`, so nothing is unreleased.
    pub fn set_staging_and_release(&mut self, oid: GitOid) {
        self.release_oid = oid.clone();
        self.staging_oid = oid;
        self.release_lag = ReleaseLag::default();
        self.release_state = ReleaseState::Green;
    }

    /// Opt-out equivalence: with no release-stage steps, `staging` and `release` name the same
    /// OID and nothing is unreleased.
    pub fn opt_out_equivalence_holds(&self) -> bool {
        self.staging_oid == self.release_oid
            && self.release_lag == ReleaseLag::default()
            && self.release_state == ReleaseState::Green
    }

    /// Whether `release` trails `staging`.
    pub fn has_unreleased_commits(&self) -> bool {
        self.staging_oid != self.release_oid
    }

    /// Recomputes `release_lag` and `release_state` from the number of first-parent commits on
    /// `staging` that `release` does not contain and the verdict of the latest conclusive release
    /// run. Nothing unreleased is green. Otherwise the state is failing when the latest release
    /// run failed and pending when it passed or none has finished; `since` keeps the moment
    /// `staging` first advanced past `release`, or starts now.
    pub fn refresh_release_projection(
        &mut self,
        unreleased_commits: u64,
        latest_release_run_failed: bool,
        now: OffsetDateTime,
    ) {
        if !self.has_unreleased_commits() {
            self.release_lag = ReleaseLag::default();
            self.release_state = ReleaseState::Green;
            return;
        }
        self.release_lag = ReleaseLag {
            commits: unreleased_commits.max(1),
            since: Some(self.release_lag.since.unwrap_or(now)),
        };
        self.release_state = if latest_release_run_failed {
            ReleaseState::Failing
        } else {
            ReleaseState::Pending
        };
    }

    /// The release projection agrees with the refs: `release` equal to `staging` is exactly no
    /// lag and green; a trailing `release` has a positive lag with its start time and is pending
    /// or failing. Every persisted state satisfies this whether or not the repository has
    /// release-stage steps.
    pub fn release_projection_consistent(&self) -> bool {
        if self.has_unreleased_commits() {
            self.release_lag.commits > 0
                && self.release_lag.since.is_some()
                && self.release_state != ReleaseState::Green
        } else {
            self.opt_out_equivalence_holds()
        }
    }
}

/// How far `release` trails `staging`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseLag {
    /// First-parent commits on `staging` that `release` does not contain.
    pub commits: u64,
    /// When `staging` first advanced past `release`; absent when nothing is unreleased.
    pub since: Option<OffsetDateTime>,
}

/// The release stage's verdict on the newest `staging` tip.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseState {
    /// `release` equals `staging`.
    #[default]
    Green,
    /// `release` trails `staging` and no release run has failed on the unreleased range.
    Pending,
    /// The latest release run failed; `release` stays at its last certified OID.
    Failing,
}

/// Wire shape that also accepts state persisted before the `staging` ref existed, which named the
/// integration ref `integration_ref`, its OID `master_oid`, and had no release fields. Such state
/// decodes with `staging_ref` set to [`LEGACY_INTEGRATION_REF`] and `release` at the same OID, and
/// startup reconciliation migrates it.
#[derive(Deserialize)]
struct PersistedRepositoryState {
    id: RepositoryId,
    name: String,
    path: String,
    #[serde(alias = "integration_ref")]
    staging_ref: String,
    #[serde(alias = "master_oid")]
    staging_oid: GitOid,
    #[serde(default)]
    release_ref: Option<String>,
    #[serde(default)]
    release_oid: Option<GitOid>,
    #[serde(default)]
    release_lag: ReleaseLag,
    #[serde(default)]
    release_state: ReleaseState,
    queue_revision: u64,
    event_sequence: u64,
    engine_epoch: u64,
    execution_state: RepositoryExecutionState,
    block_reasons: Vec<BlockReason>,
    active_configuration_digest: String,
    active_window: u16,
    active_window_floor: u16,
    active_window_ceiling: u16,
    remote_enabled: bool,
    #[serde(default)]
    release_block_reasons: Vec<BlockReason>,
    #[serde(default)]
    promotion_pause: Option<BlockReason>,
}

impl From<PersistedRepositoryState> for RepositoryState {
    fn from(state: PersistedRepositoryState) -> Self {
        Self {
            id: state.id,
            name: state.name,
            path: state.path,
            release_ref: state
                .release_ref
                .unwrap_or_else(|| LEGACY_INTEGRATION_REF.into()),
            release_oid: state
                .release_oid
                .unwrap_or_else(|| state.staging_oid.clone()),
            staging_ref: state.staging_ref,
            staging_oid: state.staging_oid,
            release_lag: state.release_lag,
            release_state: state.release_state,
            queue_revision: state.queue_revision,
            event_sequence: state.event_sequence,
            engine_epoch: state.engine_epoch,
            execution_state: state.execution_state,
            block_reasons: state.block_reasons,
            active_configuration_digest: state.active_configuration_digest,
            active_window: state.active_window,
            active_window_floor: state.active_window_floor,
            active_window_ceiling: state.active_window_ceiling,
            remote_enabled: state.remote_enabled,
            release_block_reasons: state.release_block_reasons,
            promotion_pause: state.promotion_pause,
        }
    }
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
    /// Canonical queue order from admission or the latest explicit manual
    /// reorder. Automatic authorization priority never rewrites this value.
    #[serde(default)]
    pub admission_sequence: Option<u64>,
    pub enqueue_sequence: u64,
    pub source_oid: GitOid,
    pub source_ref: String,
    pub metadata: SourceMetadata,
    pub state: QueueItemState,
    pub terminal_reason: Option<String>,
    pub remote_state: RemoteState,
    pub cleanup_state: CleanupState,
    #[serde(default)]
    pub cleanup_policy: CleanupPolicy,
    pub dependencies: Vec<QueueItemId>,
    #[serde(default)]
    pub retry_of_item_id: Option<QueueItemId>,
    /// Promotion authority is deliberately separate from admission to the
    /// speculative queue. Legacy persisted items default to authorized.
    #[serde(default = "default_promotion_authorized")]
    pub promotion_authorized: bool,
    #[serde(default)]
    pub promotion_authorized_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub promotion_authorized_by: Option<CommandId>,
    pub current_generation_id: Option<ValidationGenerationId>,
    pub buildset_id: Option<BuildsetId>,
    pub certificate_id: Option<CertificateId>,
    /// Approved with `tg approve --release-fix`: the candidate fixes a red release, so the
    /// `max_release_lag` pause never holds it back.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub release_fix: bool,
}

const fn default_promotion_authorized() -> bool {
    true
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
    #[serde(default)]
    pub diagnostics: Vec<StepDiagnostic>,
    #[serde(default)]
    pub reused_from_attempt_id: Option<StepAttemptId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StepDiagnostic {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub failure_kind: Option<DiagnosticFailureKind>,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub repair: Option<RepairCommand>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiagnosticFailureKind {
    Validation,
    Infrastructure,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RepairCommand {
    Argv { argv: Vec<String> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrozenStep {
    pub id: StepId,
    pub name: String,
    pub command: FrozenCommand,
    #[serde(default, skip_serializing_if = "FrozenStepStage::is_gate")]
    pub stage: FrozenStepStage,
    pub working_directory: String,
    pub needs: Vec<StepId>,
    pub soft_needs: Vec<StepId>,
    pub voting: bool,
    pub final_step: bool,
    #[serde(default)]
    pub reuse_on_retry: bool,
    pub timeout_ns: u64,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub rss_limit_bytes: Option<u64>,
    pub semaphores: Vec<String>,
}

/// The validation stage a frozen step was configured in. Gate-stage steps are
/// omitted from serialized frozen steps, which keeps buildsets frozen before
/// stages existed byte-identical.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FrozenStepStage {
    #[default]
    Gate,
    Release,
}

impl FrozenStepStage {
    pub const fn is_gate(&self) -> bool {
        matches!(self, Self::Gate)
    }
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
    pub fn validates_frozen_inputs(
        &self,
        item: &QueueItem,
        generation: &ValidationGeneration,
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
            && self.engine_epoch == generation.engine_epoch
            && self.configuration_digest == config_digest
            && self.step_graph_digest == step_graph_digest
            && self.engine_epoch == engine_epoch
            && self.checkout_verified
    }

    pub fn validates(
        &self,
        item: &QueueItem,
        generation: &ValidationGeneration,
        observed_master: &GitOid,
        config_digest: &str,
        step_graph_digest: &str,
        engine_epoch: u64,
    ) -> bool {
        self.validates_frozen_inputs(
            item,
            generation,
            config_digest,
            step_graph_digest,
            engine_epoch,
        ) && self.expected_parent_oid == *observed_master
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository_state_json() -> serde_json::Value {
        serde_json::json!({
            "id": RepositoryId::new(),
            "name": "repository",
            "path": "/tmp/repository",
            "staging_ref": "refs/heads/staging",
            "staging_oid": GitOid::from_hex(&"a".repeat(40)).unwrap(),
            "release_ref": "refs/heads/release",
            "release_oid": GitOid::from_hex(&"b".repeat(40)).unwrap(),
            "release_lag": { "commits": 1, "since": null },
            "release_state": "pending",
            "queue_revision": 3,
            "event_sequence": 4,
            "engine_epoch": 1,
            "execution_state": "active",
            "block_reasons": [],
            "active_configuration_digest": "digest",
            "active_window": 20,
            "active_window_floor": 3,
            "active_window_ceiling": 20,
            "remote_enabled": false
        })
    }

    #[test]
    fn repository_state_round_trips_both_tollgate_refs() {
        let value = repository_state_json();
        let state = serde_json::from_value::<RepositoryState>(value.clone()).unwrap();
        assert_eq!(state.staging_ref, "refs/heads/staging");
        assert_eq!(state.release_oid.to_hex(), "b".repeat(40));
        assert_eq!(state.release_state, ReleaseState::Pending);
        assert!(!state.opt_out_equivalence_holds());
        // State persisted before release blocks and promotion pauses existed decodes with
        // neither and encodes both explicitly.
        assert!(state.release_block_reasons.is_empty());
        assert_eq!(state.promotion_pause, None);
        let mut expected = value.clone();
        expected["release_block_reasons"] = serde_json::json!([]);
        expected["promotion_pause"] = serde_json::Value::Null;
        assert_eq!(serde_json::to_value(&state).unwrap(), expected);
        let held = RepositoryState {
            release_block_reasons: vec![BlockReason {
                code: "push-blocked".into(),
                message: "message".into(),
                recovery_action: "action".into(),
            }],
            ..state
        };
        let round_trip =
            serde_json::from_value::<RepositoryState>(serde_json::to_value(&held).unwrap())
                .unwrap();
        assert_eq!(round_trip, held);
    }

    #[test]
    fn state_persisted_before_staging_decodes_with_release_at_the_integration_oid() {
        let mut value = repository_state_json();
        let object = value.as_object_mut().unwrap();
        for field in [
            "staging_ref",
            "staging_oid",
            "release_ref",
            "release_oid",
            "release_lag",
            "release_state",
        ] {
            object.remove(field);
        }
        let integration = GitOid::from_hex(&"c".repeat(40)).unwrap();
        object.insert("integration_ref".into(), LEGACY_INTEGRATION_REF.into());
        object.insert(
            "master_oid".into(),
            serde_json::to_value(&integration).unwrap(),
        );

        let state = serde_json::from_value::<RepositoryState>(value).unwrap();
        assert_eq!(state.staging_ref, LEGACY_INTEGRATION_REF);
        assert_eq!(state.release_ref, LEGACY_INTEGRATION_REF);
        assert_eq!(state.staging_oid, integration);
        assert_eq!(state.release_oid, integration);
        assert!(state.opt_out_equivalence_holds());
        let encoded = serde_json::to_value(&state).unwrap();
        assert!(encoded.get("integration_ref").is_none());
        assert!(encoded.get("master_oid").is_none());
    }

    #[test]
    fn setting_staging_and_release_restores_opt_out_equivalence() {
        let mut state = serde_json::from_value::<RepositoryState>(repository_state_json()).unwrap();
        let promoted = GitOid::from_hex(&"d".repeat(40)).unwrap();
        state.set_staging_and_release(promoted.clone());
        assert_eq!(state.staging_oid, promoted);
        assert_eq!(state.release_oid, promoted);
        assert_eq!(state.release_lag, ReleaseLag::default());
        assert_eq!(state.release_state, ReleaseState::Green);
        assert!(state.opt_out_equivalence_holds());
    }

    #[test]
    fn release_projection_follows_lag_and_the_latest_release_verdict() {
        let mut state = serde_json::from_value::<RepositoryState>(repository_state_json()).unwrap();
        let started = OffsetDateTime::UNIX_EPOCH;
        state.release_lag = ReleaseLag::default();
        state.refresh_release_projection(2, false, started);
        assert_eq!(state.release_lag.commits, 2);
        assert_eq!(state.release_lag.since, Some(started));
        assert_eq!(state.release_state, ReleaseState::Pending);
        assert!(state.release_projection_consistent());

        let later = started + time::Duration::hours(1);
        state.refresh_release_projection(3, true, later);
        assert_eq!(state.release_lag.commits, 3);
        assert_eq!(
            state.release_lag.since,
            Some(started),
            "lag keeps the moment staging first advanced past release"
        );
        assert_eq!(state.release_state, ReleaseState::Failing);
        assert!(state.release_projection_consistent());

        state.release_oid = state.staging_oid.clone();
        state.refresh_release_projection(0, true, later);
        assert!(state.opt_out_equivalence_holds());
        assert!(state.release_projection_consistent());

        let mut inconsistent =
            serde_json::from_value::<RepositoryState>(repository_state_json()).unwrap();
        assert!(
            !inconsistent.release_projection_consistent(),
            "lag without a start time"
        );
        inconsistent.release_lag.since = Some(started);
        assert!(inconsistent.release_projection_consistent());
        inconsistent.release_state = ReleaseState::Green;
        assert!(!inconsistent.release_projection_consistent());
    }

    #[test]
    fn frozen_steps_record_release_stage_and_keep_gate_steps_unchanged() {
        let mut step = FrozenStep {
            id: StepId::new(),
            name: "ci".into(),
            command: FrozenCommand::Argv {
                argv: vec!["ci".into()],
            },
            stage: FrozenStepStage::Gate,
            working_directory: ".".into(),
            needs: Vec::new(),
            soft_needs: Vec::new(),
            voting: true,
            final_step: false,
            reuse_on_retry: false,
            timeout_ns: 1,
            cpu_tokens: 0,
            memory_bytes: 0,
            rss_limit_bytes: None,
            semaphores: Vec::new(),
        };
        let gate = serde_json::to_value(&step).unwrap();
        assert!(gate.get("stage").is_none());
        assert_eq!(serde_json::from_value::<FrozenStep>(gate).unwrap(), step);

        step.stage = FrozenStepStage::Release;
        let release = serde_json::to_value(&step).unwrap();
        assert_eq!(release["stage"], "release");
        assert_eq!(serde_json::from_value::<FrozenStep>(release).unwrap(), step);
    }
}
