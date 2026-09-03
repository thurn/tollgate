use serde::{Deserialize, Serialize};

use crate::DomainError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryExecutionState {
    Active,
    Paused,
    ConfigurationPending,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueItemState {
    Constructing,
    Queued,
    Preparing,
    Running,
    Ready,
    Promoting,
    PromotedLocalPushPending,
    Promoted,
    ExternallyIntegrated,
    Failed,
    MergeConflict,
    DependencyFailed,
    Canceled,
    Superseded,
    InfrastructureExhausted,
    CheckPassed,
    CheckFailed,
}

impl QueueItemState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Constructing => "constructing",
            Self::Queued => "queued",
            Self::Preparing => "preparing",
            Self::Running => "running",
            Self::Ready => "ready",
            Self::Promoting => "promoting",
            Self::PromotedLocalPushPending => "promoted-local-push-pending",
            Self::Promoted => "promoted",
            Self::ExternallyIntegrated => "externally-integrated",
            Self::Failed => "failed",
            Self::MergeConflict => "merge-conflict",
            Self::DependencyFailed => "dependency-failed",
            Self::Canceled => "canceled",
            Self::Superseded => "superseded",
            Self::InfrastructureExhausted => "infrastructure-exhausted",
            Self::CheckPassed => "check-passed",
            Self::CheckFailed => "check-failed",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Promoted
                | Self::ExternallyIntegrated
                | Self::Failed
                | Self::MergeConflict
                | Self::DependencyFailed
                | Self::Canceled
                | Self::Superseded
                | Self::InfrastructureExhausted
                | Self::CheckPassed
                | Self::CheckFailed
        )
    }

    pub fn transition(self, event: ItemEvent) -> Result<Self, DomainError> {
        use ItemEvent as E;
        use QueueItemState as S;
        let next = match (self, event) {
            (S::Constructing, E::GenerationPrepared) => S::Queued,
            (S::Constructing, E::MergeConflict) => S::MergeConflict,
            (S::Queued, E::PreparationStarted) => S::Preparing,
            (S::Preparing, E::WorkerStarted) => S::Running,
            (S::Preparing | S::Running, E::InfrastructureRetry) => S::Queued,
            (S::Queued | S::Preparing | S::Running, E::InfrastructureExhausted) => {
                S::InfrastructureExhausted
            }
            (S::Running, E::BuildPassed) => S::Ready,
            (S::Running, E::VotingFailed) => S::Failed,
            (S::Running, E::IndependentCheckPassed) => S::CheckPassed,
            (S::Running, E::IndependentCheckFailed) => S::CheckFailed,
            (S::Ready, E::PromotionStarted) => S::Promoting,
            (S::Promoting, E::PromotionDeferred) => S::Ready,
            (S::Promoting, E::PromotedWithoutPush) => S::Promoted,
            (S::Promoting, E::PromotedWithPush) => S::PromotedLocalPushPending,
            (S::PromotedLocalPushPending, E::PushCompleted | E::PushAbandoned) => S::Promoted,
            (
                S::Constructing | S::Queued | S::Preparing | S::Running | S::Ready,
                E::InputsChanged,
            ) => S::Constructing,
            (S::Constructing | S::Queued | S::Preparing | S::Running | S::Ready, E::Canceled) => {
                S::Canceled
            }
            (S::Constructing | S::Queued | S::Preparing | S::Running | S::Ready, E::Superseded) => {
                S::Superseded
            }
            (
                S::Constructing | S::Queued | S::Preparing | S::Running | S::Ready,
                E::DependencyLost,
            ) => S::DependencyFailed,
            (
                S::Constructing | S::Queued | S::Preparing | S::Running | S::Ready,
                E::ExternallyIntegrated,
            ) => S::ExternallyIntegrated,
            (from, event) => {
                return Err(DomainError::InvalidItemTransition {
                    from,
                    event: format!("{event:?}"),
                });
            }
        };
        Ok(next)
    }
}

impl std::fmt::Display for QueueItemState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod display_tests {
    use super::QueueItemState;

    #[test]
    fn item_states_use_their_wire_names_for_display() {
        assert_eq!(QueueItemState::MergeConflict.to_string(), "merge-conflict");
        assert_eq!(
            QueueItemState::PromotedLocalPushPending.to_string(),
            "promoted-local-push-pending"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemEvent {
    GenerationPrepared,
    MergeConflict,
    PreparationStarted,
    WorkerStarted,
    InfrastructureRetry,
    InfrastructureExhausted,
    BuildPassed,
    VotingFailed,
    InputsChanged,
    Canceled,
    Superseded,
    DependencyLost,
    ExternallyIntegrated,
    PromotionStarted,
    PromotionDeferred,
    PromotedWithoutPush,
    PromotedWithPush,
    PushCompleted,
    PushAbandoned,
    IndependentCheckPassed,
    IndependentCheckFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BuildsetState {
    Pending,
    Preparing,
    Running,
    Passed,
    PassedWithWarnings,
    Failed,
    Interrupted,
    Canceled,
    Invalidated,
    InfrastructureExhausted,
}

impl BuildsetState {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Preparing | Self::Running)
    }

    pub fn transition(self, event: BuildsetEvent) -> Result<Self, DomainError> {
        use BuildsetEvent as E;
        use BuildsetState as S;
        let next = match (self, event) {
            (S::Pending, E::PreparationStarted) => S::Preparing,
            (S::Preparing, E::WorkerStarted) => S::Running,
            (S::Pending | S::Preparing, E::Canceled) => S::Canceled,
            (S::Preparing | S::Running, E::Invalidated) => S::Invalidated,
            (S::Preparing | S::Running, E::Interrupted) => S::Interrupted,
            (S::Preparing | S::Running, E::InfrastructureExhausted) => S::InfrastructureExhausted,
            (S::Running, E::Passed) => S::Passed,
            (S::Running, E::PassedWithWarnings) => S::PassedWithWarnings,
            (S::Running, E::Failed) => S::Failed,
            (S::Running, E::Canceled) => S::Canceled,
            (S::Passed | S::PassedWithWarnings, E::Invalidated) => S::Invalidated,
            (from, event) => {
                return Err(DomainError::InvalidBuildsetTransition {
                    from,
                    event: format!("{event:?}"),
                });
            }
        };
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildsetEvent {
    PreparationStarted,
    WorkerStarted,
    Passed,
    PassedWithWarnings,
    Failed,
    Canceled,
    Invalidated,
    Interrupted,
    InfrastructureExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteState {
    Disabled,
    PreflightPending,
    Ready,
    Pushing,
    PushBlocked,
    Synchronized,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CleanupState {
    NotEligible,
    Pending,
    Running,
    Completed,
    NeedsAttention,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CleanupPolicy {
    #[default]
    Automatic,
    RetainWorktree,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_item_path_is_explicit() {
        let state = QueueItemState::Constructing
            .transition(ItemEvent::GenerationPrepared)
            .unwrap()
            .transition(ItemEvent::PreparationStarted)
            .unwrap()
            .transition(ItemEvent::WorkerStarted)
            .unwrap()
            .transition(ItemEvent::BuildPassed)
            .unwrap()
            .transition(ItemEvent::PromotionStarted)
            .unwrap()
            .transition(ItemEvent::PromotedWithoutPush)
            .unwrap();
        assert_eq!(state, QueueItemState::Promoted);
    }

    #[test]
    fn terminal_states_never_reopen() {
        for state in [
            QueueItemState::Promoted,
            QueueItemState::Failed,
            QueueItemState::Canceled,
            QueueItemState::MergeConflict,
        ] {
            assert!(state.transition(ItemEvent::InputsChanged).is_err());
        }
    }
}
