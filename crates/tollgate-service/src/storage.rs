use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tollgate_domain::{RepositoryId, SlotId};

pub const TARGET_BYTES: u64 = 60 * 1024 * 1024 * 1024;
pub const HIGH_WATER_BYTES: u64 = 70 * 1024 * 1024 * 1024;
pub const HARD_LIMIT_BYTES: u64 = 80 * 1024 * 1024 * 1024;
pub const MINIMUM_FREE_BYTES: u64 = 20 * 1024 * 1024 * 1024;
pub const SEED_LIMIT_BYTES: u64 = 20 * 1024 * 1024 * 1024;
pub const MAX_WARM_SLOTS: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageEntryView {
    pub repository_id: Option<RepositoryId>,
    pub slot_id: Option<SlotId>,
    pub class: String,
    pub path: PathBuf,
    pub charged_bytes: u64,
    pub reclaimable: bool,
    pub protected_reason: Option<String>,
    pub last_accessed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageView {
    pub charged_bytes: u64,
    pub reclaimable_bytes: u64,
    pub target_bytes: u64,
    pub high_water_bytes: u64,
    pub hard_limit_bytes: u64,
    pub minimum_free_bytes: u64,
    pub seed_limit_bytes: u64,
    pub entries: Vec<StorageEntryView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoragePruneResult {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub reclaimed_bytes: u64,
    pub removed_slots: Vec<SlotId>,
    pub removed_orphan_roots: Vec<PathBuf>,
    pub removed_recovery_roots: Vec<PathBuf>,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct SlotStorage {
    pub repository_id: RepositoryId,
    pub slot_id: SlotId,
    pub path: PathBuf,
    pub state: String,
    pub health: String,
    pub repository_active: bool,
    pub last_accessed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub struct SeedStorage {
    pub repository_id: RepositoryId,
    pub id: String,
    pub path: PathBuf,
    pub state: String,
    pub protected: bool,
}

pub fn inspect(
    cache_root: &Path,
    registered: &HashSet<RepositoryId>,
    slots: &[SlotStorage],
    seeds: &[SeedStorage],
) -> io::Result<StorageView> {
    let mut entries = Vec::new();
    let excess = excess_warm_slots(slots);
    for slot in slots {
        entries.push(StorageEntryView {
            repository_id: Some(slot.repository_id),
            slot_id: Some(slot.slot_id),
            class: "slot".into(),
            charged_bytes: tree_charged_size(&slot.path)?,
            reclaimable: excess.contains(&slot.slot_id),
            protected_reason: (slot.state != "idle").then(|| format!("slot is {}", slot.state)),
            path: slot.path.clone(),
            last_accessed_at: slot.last_accessed_at,
        });
    }
    for seed in seeds {
        entries.push(StorageEntryView {
            repository_id: Some(seed.repository_id),
            slot_id: None,
            class: "seed".into(),
            charged_bytes: tree_charged_size(&seed.path)?,
            reclaimable: !seed.protected,
            protected_reason: seed
                .protected
                .then(|| "latest compatible published seed".into()),
            path: seed.path.clone(),
            last_accessed_at: None,
        });
    }
    if cache_root.is_dir() {
        for child in fs::read_dir(cache_root)? {
            let child = child?;
            let path = child.path();
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                continue;
            }
            let name = child.file_name().to_string_lossy().into_owned();
            let repository_id = name.parse::<RepositoryId>().ok();
            if repository_id.is_some_and(|id| registered.contains(&id)) {
                continue;
            }
            let orphan = repository_id.is_some();
            let prune_quarantine = name.starts_with(".pruned-");
            entries.push(StorageEntryView {
                repository_id,
                slot_id: None,
                class: if orphan {
                    "orphan-repository".into()
                } else if prune_quarantine {
                    "prune-quarantine".into()
                } else if name.starts_with("recovery-") {
                    "recovery".into()
                } else {
                    "unknown".into()
                },
                charged_bytes: tree_charged_size(&path)?,
                reclaimable: orphan || prune_quarantine,
                protected_reason: (!orphan && !prune_quarantine)
                    .then(|| "ownership requires review".into()),
                path,
                last_accessed_at: None,
            });
        }
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let charged_bytes = tree_charged_size(cache_root)?;
    let reclaimable_bytes = entries
        .iter()
        .filter(|entry| entry.reclaimable)
        .map(|entry| entry.charged_bytes)
        .sum();
    Ok(StorageView {
        charged_bytes,
        reclaimable_bytes,
        target_bytes: TARGET_BYTES,
        high_water_bytes: HIGH_WATER_BYTES,
        hard_limit_bytes: HARD_LIMIT_BYTES,
        minimum_free_bytes: MINIMUM_FREE_BYTES,
        seed_limit_bytes: SEED_LIMIT_BYTES,
        entries,
    })
}

pub fn excess_warm_slots(slots: &[SlotStorage]) -> HashSet<SlotId> {
    let mut idle = slots
        .iter()
        .filter(|slot| slot.state == "idle" && slot.health == "healthy" && !slot.repository_active)
        .collect::<Vec<_>>();
    idle.sort_by(|left, right| {
        right
            .last_accessed_at
            .cmp(&left.last_accessed_at)
            .then_with(|| left.slot_id.to_string().cmp(&right.slot_id.to_string()))
    });
    let mut repositories = HashMap::new();
    let mut retained = HashSet::new();
    for slot in idle {
        if retained.len() == MAX_WARM_SLOTS {
            break;
        }
        if repositories.insert(slot.repository_id, ()).is_none() {
            retained.insert(slot.slot_id);
        }
    }
    slots
        .iter()
        .filter(|slot| {
            slot.state == "idle" && !slot.repository_active && !retained.contains(&slot.slot_id)
        })
        .map(|slot| slot.slot_id)
        .collect()
}

pub fn tree_charged_size(path: &Path) -> io::Result<u64> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(0);
    };
    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("storage path is a symbolic link: {}", path.display()),
        ));
    }
    tree_charged_size_entry(path, &metadata)
}

fn tree_charged_size_entry(path: &Path, metadata: &fs::Metadata) -> io::Result<u64> {
    if metadata.file_type().is_symlink() || metadata.is_file() {
        return Ok(charged_size(metadata));
    }
    let mut total = charged_size(metadata);
    for child in fs::read_dir(path)? {
        let child = child?;
        let metadata = fs::symlink_metadata(child.path())?;
        total = total.saturating_add(tree_charged_size_entry(&child.path(), &metadata)?);
    }
    Ok(total)
}

#[cfg(unix)]
fn charged_size(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn charged_size(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(repository_id: RepositoryId, age: i64) -> SlotStorage {
        SlotStorage {
            repository_id,
            slot_id: SlotId::new(),
            path: PathBuf::new(),
            state: "idle".into(),
            health: "healthy".into(),
            repository_active: false,
            last_accessed_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(age)),
        }
    }

    #[test]
    fn warm_pool_keeps_one_recent_slot_per_repository_and_four_globally() {
        let first = RepositoryId::new();
        let second = RepositoryId::new();
        let mut slots = vec![slot(first, 1), slot(first, 2), slot(second, 3)];
        for age in 4..8 {
            slots.push(slot(RepositoryId::new(), age));
        }
        let excess = excess_warm_slots(&slots);
        assert_eq!(slots.len() - excess.len(), MAX_WARM_SLOTS);
        assert!(excess.contains(&slots[0].slot_id));
        for slot in &slots[3..] {
            assert!(!excess.contains(&slot.slot_id));
        }
    }

    #[test]
    fn inspection_refuses_symbolic_link_roots() {
        let temporary = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(temporary.path(), temporary.path().join("link")).unwrap();
            assert!(tree_charged_size(&temporary.path().join("link")).is_err());
            tree_charged_size(temporary.path()).unwrap();
        }
    }

    #[test]
    fn inspection_distinguishes_owned_orphans_and_recovery_data() {
        let temporary = tempfile::tempdir().unwrap();
        let registered_id = RepositoryId::new();
        let orphan_id = RepositoryId::new();
        for name in [
            registered_id.to_string(),
            orphan_id.to_string(),
            "recovery-interrupted".into(),
            ".pruned-interrupted".into(),
        ] {
            let directory = temporary.path().join(name);
            fs::create_dir(&directory).unwrap();
            fs::write(directory.join("data"), b"payload").unwrap();
        }
        let view = inspect(temporary.path(), &HashSet::from([registered_id]), &[], &[]).unwrap();
        assert!(view.entries.iter().any(|entry| {
            entry.class == "orphan-repository"
                && entry.repository_id == Some(orphan_id)
                && entry.reclaimable
        }));
        assert!(
            view.entries
                .iter()
                .any(|entry| { entry.class == "prune-quarantine" && entry.reclaimable })
        );
        assert!(
            view.entries
                .iter()
                .any(|entry| { entry.class == "recovery" && !entry.reclaimable })
        );
        assert!(
            !view
                .entries
                .iter()
                .any(|entry| { entry.repository_id == Some(registered_id) })
        );
    }
}
