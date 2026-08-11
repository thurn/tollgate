use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{DomainError, GitOid, QueueItem, QueueItemId};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DependentQueue {
    pub revision: u64,
    pub items: Vec<QueueItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueMutation {
    pub old_revision: u64,
    pub new_revision: u64,
    pub invalidated: Vec<QueueItemId>,
    pub removed: Vec<QueueItemId>,
}

impl DependentQueue {
    pub fn active_items(&self) -> impl Iterator<Item = &QueueItem> {
        self.items.iter().filter(|item| !item.state.is_terminal())
    }

    pub fn enqueue(
        &mut self,
        item: QueueItem,
        expected_revision: u64,
    ) -> Result<QueueMutation, DomainError> {
        self.guard_revision(expected_revision)?;
        if let Some(existing) = self
            .active_items()
            .find(|existing| existing.source_oid == item.source_oid)
        {
            return Err(DomainError::DuplicateSource(existing.id));
        }
        if !item.dependencies.iter().all(|dependency| {
            self.active_items()
                .any(|candidate| candidate.id == *dependency)
        }) {
            return Err(DomainError::DependencyOrder);
        }
        let old_revision = self.revision;
        self.items.push(item);
        self.revision += 1;
        Ok(QueueMutation {
            old_revision,
            new_revision: self.revision,
            invalidated: Vec::new(),
            removed: Vec::new(),
        })
    }

    pub fn cancel(
        &mut self,
        id: QueueItemId,
        expected_revision: u64,
    ) -> Result<QueueMutation, DomainError> {
        self.guard_revision(expected_revision)?;
        let active = self.active_positions();
        let removed_pos = *active.get(&id).ok_or(DomainError::ItemNotFound(id))?;

        let mut removed = HashSet::from([id]);
        loop {
            let prior_len = removed.len();
            for item in self.active_items() {
                if item.dependencies.iter().any(|dep| removed.contains(dep)) {
                    removed.insert(item.id);
                }
            }
            if removed.len() == prior_len {
                break;
            }
        }

        let invalidated = self
            .active_items()
            .enumerate()
            .filter(|(position, item)| *position > removed_pos && !removed.contains(&item.id))
            .map(|(_, item)| item.id)
            .collect::<Vec<_>>();

        let old_revision = self.revision;
        self.revision += 1;
        Ok(QueueMutation {
            old_revision,
            new_revision: self.revision,
            invalidated,
            removed: self
                .items
                .iter()
                .filter(|item| removed.contains(&item.id))
                .map(|item| item.id)
                .collect(),
        })
    }

    pub fn reorder(
        &mut self,
        ordered: &[QueueItemId],
        expected_revision: u64,
    ) -> Result<QueueMutation, DomainError> {
        self.guard_revision(expected_revision)?;
        let active_ids = self.active_items().map(|item| item.id).collect::<Vec<_>>();
        if active_ids.len() != ordered.len()
            || active_ids.iter().collect::<HashSet<_>>() != ordered.iter().collect::<HashSet<_>>()
        {
            return Err(DomainError::InvalidInput(
                "reorder must include every active queue item exactly once".into(),
            ));
        }

        let new_positions = ordered
            .iter()
            .enumerate()
            .map(|(position, id)| (*id, position))
            .collect::<HashMap<_, _>>();
        for item in self.active_items() {
            if item
                .dependencies
                .iter()
                .any(|dep| new_positions[dep] >= new_positions[&item.id])
            {
                return Err(DomainError::DependencyOrder);
            }
        }

        let old_positions = self.active_positions();
        let first_changed = ordered
            .iter()
            .enumerate()
            .find(|(position, id)| old_positions[id] != *position)
            .map(|(position, _)| position);
        let invalidated = first_changed
            .map(|start| ordered[start..].to_vec())
            .unwrap_or_default();

        let by_id = self
            .items
            .drain(..)
            .map(|item| (item.id, item))
            .collect::<HashMap<_, _>>();
        let mut by_id = by_id;
        let mut reordered = Vec::with_capacity(by_id.len());
        for id in ordered {
            if let Some(item) = by_id.remove(id) {
                reordered.push(item);
            }
        }
        reordered.extend(by_id.into_values());
        self.items = reordered;

        let old_revision = self.revision;
        self.revision += 1;
        Ok(QueueMutation {
            old_revision,
            new_revision: self.revision,
            invalidated,
            removed: Vec::new(),
        })
    }

    pub fn affected_prefix_after_removal(&self, source_oid: &GitOid) -> Vec<QueueItemId> {
        let Some(position) = self
            .active_items()
            .position(|item| &item.source_oid == source_oid)
        else {
            return Vec::new();
        };
        self.active_items()
            .skip(position + 1)
            .map(|item| item.id)
            .collect()
    }

    fn active_positions(&self) -> HashMap<QueueItemId, usize> {
        self.active_items()
            .enumerate()
            .map(|(position, item)| (item.id, position))
            .collect()
    }

    fn guard_revision(&self, expected: u64) -> Result<(), DomainError> {
        if self.revision != expected {
            return Err(DomainError::RevisionConflict {
                expected,
                actual: self.revision,
            });
        }
        Ok(())
    }
}
