use thiserror::Error;

use crate::{BuildsetState, QueueItemId, QueueItemState, RepositoryExecutionState};

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DomainError {
    #[error("invalid object id length: expected {expected} bytes/characters, got {actual}")]
    InvalidOidLength { expected: usize, actual: usize },
    #[error("invalid hexadecimal object id: {0}")]
    InvalidOid(String),
    #[error("queue revision conflict: expected {expected}, current revision is {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("queue item {0} was not found")]
    ItemNotFound(QueueItemId),
    #[error("source object is already active as queue item {0}")]
    DuplicateSource(QueueItemId),
    #[error("hard dependency cycle or invalid queue order")]
    DependencyOrder,
    #[error("transition from queue item state {from:?} on {event} is not allowed")]
    InvalidItemTransition { from: QueueItemState, event: String },
    #[error("transition from buildset state {from:?} on {event} is not allowed")]
    InvalidBuildsetTransition { from: BuildsetState, event: String },
    #[error("repository is {0:?}; this operation requires an active repository")]
    RepositoryUnavailable(RepositoryExecutionState),
    #[error("promotion requires the current queue head")]
    NotQueueHead,
    #[error("promotion certificate does not match current validation inputs: {0}")]
    InvalidCertificate(String),
    #[error("promotion parent does not equal the observed master object")]
    PromotionParentMismatch,
    #[error("invalid domain input: {0}")]
    InvalidInput(String),
}
