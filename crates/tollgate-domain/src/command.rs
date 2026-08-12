use serde::{Deserialize, Serialize};

use crate::{ClientInstanceId, CommandId, QueueItemId, RepositoryId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandEnvelope<T> {
    pub client_instance_id: ClientInstanceId,
    pub command_id: CommandId,
    pub repository_id: RepositoryId,
    pub expected_revision: Option<u64>,
    pub command: T,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RepositoryCommand {
    SubmitCandidate { revision: String },
    Approve { revision: String },
    AuthorizeCandidate { item_id: QueueItemId },
    Cancel { item_id: QueueItemId },
    Retry { item_id: QueueItemId, cold: bool },
    Reorder { ordered_item_ids: Vec<QueueItemId> },
    Pause,
    Resume,
    ApplyConfiguration { candidate_digest: String },
    ReloadEnvironment,
    RetryPush,
    Reconcile { action: String },
}
