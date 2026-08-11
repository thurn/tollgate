use std::{
    collections::{HashMap, HashSet},
    ffi::CString,
    fs::{self, File},
    io::{Read, Write},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CloneError {
    #[error("clone source and destination are on different volumes")]
    CrossVolume,
    #[error("APFS force clone failed for {path}: {source}")]
    CloneFailed {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("unsafe cache entry {path}: {reason}")]
    UnsafeEntry { path: PathBuf, reason: String },
    #[error("cache source changed while it was being captured: {0}")]
    SourceChanged(PathBuf),
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest serialization error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("force cloning is supported only on macOS APFS")]
    UnsupportedPlatform,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CloneManifest {
    pub version: u16,
    pub source_device: u64,
    pub logical_size: u64,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub relative_path: PathBuf,
    pub kind: EntryKind,
    pub mode: u32,
    pub size: u64,
    pub source_inode: u64,
    pub structural_hash: Option<String>,
    pub symlink_target: Option<PathBuf>,
    pub clone_succeeded: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    Directory,
    File,
    Symlink,
    Hardlink,
}

pub fn force_clone_tree(source: &Path, destination: &Path) -> Result<CloneManifest, CloneError> {
    if destination.exists() {
        return Err(CloneError::DestinationExists(destination.into()));
    }
    let source = source.canonicalize()?;
    let source_meta = fs::symlink_metadata(&source)?;
    if !source_meta.is_dir() {
        return Err(CloneError::UnsafeEntry {
            path: source.clone(),
            reason: "source root is not a directory".into(),
        });
    }
    let destination_parent = destination
        .parent()
        .ok_or_else(|| CloneError::UnsafeEntry {
            path: destination.into(),
            reason: "destination has no parent".into(),
        })?;
    fs::create_dir_all(destination_parent)?;
    if fs::metadata(destination_parent)?.dev() != source_meta.dev() {
        return Err(CloneError::CrossVolume);
    }
    fs::create_dir(destination)?;
    let mut state = CloneState {
        source_root: &source,
        destination_root: destination,
        source_device: source_meta.dev(),
        logical_size: 0,
        entries: Vec::new(),
        hardlinks: HashMap::new(),
    };
    if let Err(error) = state.clone_directory(Path::new("")) {
        let _ = fs::remove_dir_all(destination);
        return Err(error);
    }
    let after = fs::symlink_metadata(&source)?;
    if after.dev() != source_meta.dev()
        || after.ino() != source_meta.ino()
        || after.mtime() != source_meta.mtime()
        || after.mtime_nsec() != source_meta.mtime_nsec()
    {
        let _ = fs::remove_dir_all(destination);
        return Err(CloneError::SourceChanged(source));
    }
    sync_tree(destination)?;
    Ok(CloneManifest {
        version: 1,
        source_device: source_meta.dev(),
        logical_size: state.logical_size,
        entries: state.entries,
    })
}

struct CloneState<'a> {
    source_root: &'a Path,
    destination_root: &'a Path,
    source_device: u64,
    logical_size: u64,
    entries: Vec<ManifestEntry>,
    hardlinks: HashMap<(u64, u64), PathBuf>,
}

impl CloneState<'_> {
    fn clone_directory(&mut self, relative: &Path) -> Result<(), CloneError> {
        let source_directory = self.source_root.join(relative);
        let before = fs::symlink_metadata(&source_directory)?;
        if before.dev() != self.source_device {
            return Err(CloneError::UnsafeEntry {
                path: relative.into(),
                reason: "mount/device boundary".into(),
            });
        }
        let mut children = fs::read_dir(&source_directory)?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let child_relative = relative.join(child.file_name());
            validate_relative(&child_relative)?;
            let source_path = self.source_root.join(&child_relative);
            let destination_path = self.destination_root.join(&child_relative);
            let metadata_before = fs::symlink_metadata(&source_path)?;
            if metadata_before.dev() != self.source_device {
                return Err(CloneError::UnsafeEntry {
                    path: child_relative,
                    reason: "mount/device boundary".into(),
                });
            }
            if metadata_before.uid() != nix::unistd::Uid::current().as_raw() {
                return Err(CloneError::UnsafeEntry {
                    path: child_relative,
                    reason: "entry is not owned by the current user".into(),
                });
            }
            let file_type = metadata_before.file_type();
            if file_type.is_dir() {
                fs::create_dir(&destination_path)?;
                self.entries.push(entry(
                    &child_relative,
                    &source_path,
                    EntryKind::Directory,
                    &metadata_before,
                    None,
                    false,
                )?);
                self.clone_directory(&child_relative)?;
                fs::set_permissions(
                    &destination_path,
                    fs::Permissions::from_mode(metadata_before.mode()),
                )?;
            } else if file_type.is_file() {
                self.logical_size = self.logical_size.saturating_add(metadata_before.len());
                let identity = (metadata_before.dev(), metadata_before.ino());
                if metadata_before.nlink() > 1 && self.hardlinks.contains_key(&identity) {
                    fs::hard_link(
                        self.destination_root.join(&self.hardlinks[&identity]),
                        &destination_path,
                    )?;
                    self.entries.push(entry(
                        &child_relative,
                        &source_path,
                        EntryKind::Hardlink,
                        &metadata_before,
                        None,
                        true,
                    )?);
                } else {
                    force_clone_file(&source_path, &destination_path)?;
                    self.hardlinks.insert(identity, child_relative.clone());
                    self.entries.push(entry(
                        &child_relative,
                        &source_path,
                        EntryKind::File,
                        &metadata_before,
                        None,
                        true,
                    )?);
                }
            } else if file_type.is_symlink() {
                let target = fs::read_link(&source_path)?;
                validate_symlink(relative, &target).map_err(|reason| CloneError::UnsafeEntry {
                    path: child_relative.clone(),
                    reason,
                })?;
                std::os::unix::fs::symlink(&target, &destination_path)?;
                self.entries.push(entry(
                    &child_relative,
                    &source_path,
                    EntryKind::Symlink,
                    &metadata_before,
                    Some(target),
                    false,
                )?);
            } else {
                return Err(CloneError::UnsafeEntry {
                    path: child_relative,
                    reason: "devices, sockets, and FIFOs are forbidden".into(),
                });
            }
            let metadata_after = fs::symlink_metadata(&source_path)?;
            if metadata_after.dev() != metadata_before.dev()
                || metadata_after.ino() != metadata_before.ino()
                || metadata_after.mode() != metadata_before.mode()
                || metadata_after.len() != metadata_before.len()
                || metadata_after.mtime() != metadata_before.mtime()
                || metadata_after.mtime_nsec() != metadata_before.mtime_nsec()
            {
                return Err(CloneError::SourceChanged(child_relative));
            }
        }
        let after = fs::symlink_metadata(&source_directory)?;
        if after.ino() != before.ino()
            || after.mtime() != before.mtime()
            || after.mtime_nsec() != before.mtime_nsec()
        {
            return Err(CloneError::SourceChanged(relative.into()));
        }
        Ok(())
    }
}

fn entry(
    relative_path: &Path,
    source_path: &Path,
    kind: EntryKind,
    metadata: &fs::Metadata,
    symlink_target: Option<PathBuf>,
    clone_succeeded: bool,
) -> Result<ManifestEntry, CloneError> {
    let structural_hash = if matches!(kind, EntryKind::File | EntryKind::Hardlink) {
        Some(hash_file(source_path)?)
    } else {
        None
    };
    Ok(ManifestEntry {
        relative_path: relative_path.into(),
        kind,
        mode: metadata.mode(),
        size: metadata.len(),
        source_inode: metadata.ino(),
        structural_hash,
        symlink_target,
        clone_succeeded,
    })
}

pub fn verify_clone_tree(source: &Path, manifest: &CloneManifest) -> Result<(), CloneError> {
    let source = source.canonicalize()?;
    let root = fs::symlink_metadata(&source)?;
    if !root.is_dir() || root.dev() != manifest.source_device {
        return Err(CloneError::UnsafeEntry {
            path: source,
            reason: "seed root identity differs from its manifest".into(),
        });
    }
    let expected = manifest
        .entries
        .iter()
        .map(|entry| entry.relative_path.clone())
        .collect::<HashSet<_>>();
    let mut observed = HashSet::new();
    let mut directories = vec![PathBuf::new()];
    while let Some(relative) = directories.pop() {
        let mut children = fs::read_dir(source.join(&relative))?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let child_relative = relative.join(child.file_name());
            validate_relative(&child_relative)?;
            let metadata = fs::symlink_metadata(source.join(&child_relative))?;
            if metadata.dev() != root.dev() {
                return Err(CloneError::UnsafeEntry {
                    path: child_relative,
                    reason: "mount/device boundary".into(),
                });
            }
            if metadata.file_type().is_dir() {
                directories.push(child_relative.clone());
            } else if !metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
                return Err(CloneError::UnsafeEntry {
                    path: child_relative,
                    reason: "devices, sockets, and FIFOs are forbidden".into(),
                });
            }
            observed.insert(child_relative);
        }
    }
    if observed != expected {
        return Err(CloneError::SourceChanged(source));
    }
    let mut hardlinks = HashMap::<u64, u64>::new();
    for entry in &manifest.entries {
        let path = source.join(&entry.relative_path);
        let metadata = fs::symlink_metadata(&path)?;
        let kind_matches = match entry.kind {
            EntryKind::Directory => metadata.file_type().is_dir(),
            EntryKind::File | EntryKind::Hardlink => metadata.file_type().is_file(),
            EntryKind::Symlink => metadata.file_type().is_symlink(),
        };
        if !kind_matches || metadata.mode() != entry.mode || metadata.len() != entry.size {
            return Err(CloneError::SourceChanged(entry.relative_path.clone()));
        }
        match entry.kind {
            EntryKind::File => {
                hardlinks.insert(entry.source_inode, metadata.ino());
                if entry.structural_hash.as_deref() != Some(hash_file(&path)?.as_str()) {
                    return Err(CloneError::SourceChanged(entry.relative_path.clone()));
                }
            }
            EntryKind::Hardlink => {
                if hardlinks.get(&entry.source_inode).copied() != Some(metadata.ino())
                    || entry.structural_hash.as_deref() != Some(hash_file(&path)?.as_str())
                {
                    return Err(CloneError::SourceChanged(entry.relative_path.clone()));
                }
            }
            EntryKind::Symlink => {
                if fs::read_link(&path).ok().as_ref() != entry.symlink_target.as_ref() {
                    return Err(CloneError::SourceChanged(entry.relative_path.clone()));
                }
            }
            EntryKind::Directory => {}
        }
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String, CloneError> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(target_os = "macos")]
pub fn force_clone_file(source: &Path, destination: &Path) -> Result<(), CloneError> {
    if destination.exists() {
        return Err(CloneError::DestinationExists(destination.into()));
    }
    let source_c =
        CString::new(source.as_os_str().as_bytes()).map_err(|_| CloneError::UnsafeEntry {
            path: source.into(),
            reason: "NUL in source path".into(),
        })?;
    let destination_c =
        CString::new(destination.as_os_str().as_bytes()).map_err(|_| CloneError::UnsafeEntry {
            path: destination.into(),
            reason: "NUL in destination path".into(),
        })?;
    // clonefile is the force-clone primitive: unlike copyfile/cp, it cannot silently copy bytes.
    // Paths were canonicalized beneath verified roots, both volumes were compared above, and
    // CLONE_NOFOLLOW keeps the syscall from following the final source symlink.
    let result =
        unsafe { libc::clonefile(source_c.as_ptr(), destination_c.as_ptr(), 0x0001 | 0x0004) };
    if result != 0 {
        return Err(CloneError::CloneFailed {
            path: source.into(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn force_clone_file(_: &Path, _: &Path) -> Result<(), CloneError> {
    Err(CloneError::UnsupportedPlatform)
}

pub fn publish_seed(
    staging_parent: &Path,
    generation_path: &Path,
    source: &Path,
) -> Result<CloneManifest, CloneError> {
    if generation_path.exists() {
        return Err(CloneError::DestinationExists(generation_path.into()));
    }
    fs::create_dir_all(staging_parent)?;
    let staging = staging_parent.join(format!(
        ".staging-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let manifest = force_clone_tree(source, &staging)?;
    let manifest_path = staging.join(".tollgate-seed-manifest.json");
    let mut file = File::options()
        .create_new(true)
        .write(true)
        .open(&manifest_path)?;
    serde_json::to_writer_pretty(&mut file, &manifest)?;
    file.flush()?;
    file.sync_all()?;
    File::open(&staging)?.sync_all()?;
    fs::rename(&staging, generation_path)?;
    File::open(generation_path.parent().unwrap_or(staging_parent))?.sync_all()?;
    Ok(manifest)
}

fn validate_relative(path: &Path) -> Result<(), CloneError> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CloneError::UnsafeEntry {
            path: path.into(),
            reason: "path is not a normalized relative path".into(),
        });
    }
    Ok(())
}

fn validate_symlink(parent: &Path, target: &Path) -> Result<(), String> {
    if target.is_absolute() {
        return Err("absolute symlink target".into());
    }
    let combined = parent.join(target);
    let mut depth = 0i32;
    for component in combined.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err("symlink escapes selected cache root".into());
                }
            }
            _ => return Err("unsupported symlink component".into()),
        }
    }
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), std::io::Error> {
    for child in fs::read_dir(root)? {
        let child = child?;
        if child.file_type()?.is_dir() {
            sync_tree(&child.path())?;
        }
    }
    File::open(root)?.sync_all()
}

use std::os::unix::fs::PermissionsExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escaping_symlink_is_rejected() {
        assert!(validate_symlink(Path::new("cache"), Path::new("../../secret")).is_err());
        assert!(validate_symlink(Path::new("cache"), Path::new("../shared/ok")).is_ok());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn clone_tree_proves_regular_file_clones_and_preserves_hardlinks() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let destination = temporary.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("a"), b"cache").unwrap();
        fs::hard_link(source.join("a"), source.join("b")).unwrap();
        let manifest = force_clone_tree(&source, &destination).unwrap();
        assert!(
            manifest
                .entries
                .iter()
                .filter(|entry| matches!(entry.kind, EntryKind::File | EntryKind::Hardlink))
                .all(|entry| entry.clone_succeeded)
        );
        let a = fs::metadata(destination.join("a")).unwrap();
        let b = fs::metadata(destination.join("b")).unwrap();
        assert_eq!(a.ino(), b.ino());
    }
}
