use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{CommandId, EventId, RepositoryId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Actor {
    App,
    Cli,
    Ui,
    Recovery,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: EventId,
    pub repository_id: RepositoryId,
    pub sequence: u64,
    pub actor: Actor,
    pub command_id: Option<CommandId>,
    pub kind: String,
    pub payload: serde_json::Value,
    pub created_at: OffsetDateTime,
}
