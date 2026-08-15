#![forbid(unsafe_code)]

use std::{
    ffi::OsStr,
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    process::Stdio,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{fs, io::AsyncReadExt, process::Command};
use tollgate_domain::{GitOid, ObjectFormat, QueueItemId};

const EMPTY_HOOKS_DIRECTORY: &str = "/dev/null";
pub const INTEGRATION_BRANCH: &str = "release";
pub const INTEGRATION_REF: &str = "refs/heads/release";
pub const USER_BRANCH: &str = "master";
pub const USER_BRANCH_REF: &str = "refs/heads/master";

#[derive(Debug, Error)]
pub enum GitError {
    #[error("Git command failed ({command}): {stderr}")]
    Command { command: String, stderr: String },
    #[error("Git command produced invalid output: {0}")]
    InvalidOutput(String),
    #[error("repository is dirty: {0}")]
    DirtyWorktree(String),
    #[error("Tollgate integration branch `release` is checked out in {0}")]
    IntegrationCheckedOut(String),
    #[error("unsupported Git object format `{0}`")]
    UnsupportedObjectFormat(String),
    #[error("source commit must have exactly one parent")]
    InvalidSourceShape,
    #[error("Git could not rebase the requested commit")]
    Unmergeable,
    #[error(
        "cannot synthesize source {source_oid}: merge conflicts in {conflicting_paths:?} (source base {source_parent_oid}; current queue prefix {prefix_oid}); rebase onto the latest release (or the displayed queue prefix when an earlier candidate is involved), resolve the listed paths, regenerate derived files, and resubmit"
    )]
    SyntheticConflict {
        source_oid: GitOid,
        source_parent_oid: GitOid,
        prefix_oid: GitOid,
        conflicting_paths: Vec<String>,
    },
    #[error(
        "cannot synthesize source {source_oid}: the patch is empty (source base {source_parent_oid}; current queue prefix {prefix_oid}); its changes are already present, so there is nothing new to validate"
    )]
    SyntheticEmpty {
        source_oid: GitOid,
        source_parent_oid: GitOid,
        prefix_oid: GitOid,
    },
    #[error("malformed raw commit: {0}")]
    MalformedCommit(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("object id error: {0}")]
    Oid(#[from] tollgate_domain::DomainError),
}

impl GitError {
    pub fn is_synthetic_rejection(&self) -> bool {
        matches!(
            self,
            Self::SyntheticConflict { .. } | Self::SyntheticEmpty { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitSemanticsProfile {
    pub executable: PathBuf,
    pub version: String,
    pub object_format: ObjectFormat,
    pub hooks_path: String,
    pub merge_strategy: String,
    pub digest: String,
}

#[derive(Clone, Debug)]
pub struct GitRepository {
    pub worktree_root: PathBuf,
    pub common_dir: PathBuf,
    pub profile: GitSemanticsProfile,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalProbe {
    pub source_oid: GitOid,
    pub parent_oid: GitOid,
    pub subject: String,
    pub author_name: String,
    pub author_email: String,
    pub branch: Option<String>,
    pub message_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticCommit {
    pub source_oid: GitOid,
    pub oid: GitOid,
    pub parent_oid: GitOid,
    pub tree_oid: GitOid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum UserMasterSyncOutcome {
    UpdatedCheckout {
        path: PathBuf,
    },
    UpdatedRef,
    AlreadyCurrent {
        path: Option<PathBuf>,
    },
    NeedsAttention {
        path: Option<PathBuf>,
        reason: String,
    },
}

impl GitRepository {
    pub async fn discover(path: impl AsRef<Path>) -> Result<Self, GitError> {
        let path = path.as_ref();
        let executable = resolve_git().await?;
        let worktree_root = canonical_git_path(
            run_raw(
                &executable,
                path,
                ["rev-parse", "--path-format=absolute", "--show-toplevel"],
            )
            .await?,
        )?;
        let common_dir = canonical_git_path(
            run_raw(
                &executable,
                path,
                ["rev-parse", "--path-format=absolute", "--git-common-dir"],
            )
            .await?,
        )?;
        let version = text(run_raw(&executable, path, ["version"]).await?)?;
        let object_format_text =
            text(run_raw(&executable, path, ["rev-parse", "--show-object-format"]).await?)?;
        let object_format = match object_format_text.trim() {
            "sha1" => ObjectFormat::Sha1,
            "sha256" => ObjectFormat::Sha256,
            value => return Err(GitError::UnsupportedObjectFormat(value.into())),
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(executable.as_os_str().as_encoded_bytes());
        hasher.update(version.as_bytes());
        hasher.update(object_format_text.as_bytes());
        hasher.update(b"ort:no-commit:hooks-disabled:v1");
        let profile = GitSemanticsProfile {
            executable,
            version: version.trim().into(),
            object_format,
            hooks_path: EMPTY_HOOKS_DIRECTORY.into(),
            merge_strategy: "ort/no-commit/v1".into(),
            digest: hasher.finalize().to_hex().to_string(),
        };
        Ok(Self {
            worktree_root,
            common_dir,
            profile,
        })
    }

    pub async fn integration_oid(&self) -> Result<GitOid, GitError> {
        self.resolve_oid(INTEGRATION_REF).await
    }

    pub async fn tree_oid(&self, commit: &GitOid) -> Result<GitOid, GitError> {
        let output = self
            .git(["show", "-s", "--format=%T", &commit.to_hex()])
            .await?;
        GitOid::from_hex(text(output)?.trim()).map_err(Into::into)
    }

    pub async fn commit_parent_oid(&self, commit: &GitOid) -> Result<GitOid, GitError> {
        let output = self
            .git(["show", "-s", "--format=%P", &commit.to_hex()])
            .await?;
        let parents = text(output)?;
        let parents = parents.split_whitespace().collect::<Vec<_>>();
        if parents.len() != 1 {
            return Err(GitError::InvalidSourceShape);
        }
        GitOid::from_hex(parents[0]).map_err(Into::into)
    }

    pub async fn mirror_tree_oid(
        &self,
        mirror: &Path,
        commit: &GitOid,
    ) -> Result<GitOid, GitError> {
        let output = run_raw(
            &self.profile.executable,
            mirror,
            ["show", "-s", "--format=%T", &commit.to_hex()],
        )
        .await?;
        GitOid::from_hex(text(output)?.trim()).map_err(Into::into)
    }

    pub async fn resolve_oid(&self, revision: &str) -> Result<GitOid, GitError> {
        let output = self
            .git(["rev-parse", "--verify", &format!("{revision}^{{commit}}")])
            .await?;
        GitOid::from_hex(text(output)?.trim()).map_err(Into::into)
    }

    pub async fn optional_ref_oid(&self, reference: &str) -> Result<Option<GitOid>, GitError> {
        let exists = internal_command(&self.profile.executable, &self.worktree_root)
            .args(["show-ref", "--verify", "--quiet", reference])
            .output()
            .await?;
        if !exists.status.success() {
            if exists.status.code() == Some(1) && exists.stderr.is_empty() {
                return Ok(None);
            }
            return Err(GitError::Command {
                command: format!("git show-ref --verify --quiet {reference}"),
                stderr: String::from_utf8_lossy(&exists.stderr).trim().into(),
            });
        }
        let output = internal_command(&self.profile.executable, &self.worktree_root)
            .args(["show-ref", "--verify", "--hash", reference])
            .output()
            .await?;
        if output.status.success() {
            return GitOid::from_hex(text(output.stdout)?.trim())
                .map(Some)
                .map_err(Into::into);
        }
        Err(GitError::Command {
            command: format!("git show-ref --verify --hash {reference}"),
            stderr: String::from_utf8_lossy(&output.stderr).trim().into(),
        })
    }

    pub async fn ensure_integration_not_checked_out(&self) -> Result<(), GitError> {
        let bytes = self.git(["worktree", "list", "--porcelain", "-z"]).await?;
        let fields = bytes.split(|byte| *byte == 0).collect::<Vec<_>>();
        let mut current_path = None;
        for field in fields {
            if let Some(path) = field.strip_prefix(b"worktree ") {
                current_path = Some(String::from_utf8_lossy(path).into_owned());
            } else if field == b"branch refs/heads/release" {
                return Err(GitError::IntegrationCheckedOut(
                    current_path.unwrap_or_default(),
                ));
            }
        }
        Ok(())
    }

    pub async fn worktree_for_branch(&self, branch_ref: &str) -> Result<Option<PathBuf>, GitError> {
        let bytes = self.git(["worktree", "list", "--porcelain", "-z"]).await?;
        let mut current_path = None;
        let mut matches = Vec::new();
        for field in bytes.split(|byte| *byte == 0) {
            if field.is_empty() {
                current_path = None;
            } else if let Some(path) = field.strip_prefix(b"worktree ") {
                current_path = Some(PathBuf::from(String::from_utf8_lossy(path).into_owned()));
            } else if field
                .strip_prefix(b"branch ")
                .is_some_and(|branch| branch == branch_ref.as_bytes())
            {
                matches.push(current_path.clone().ok_or_else(|| {
                    GitError::InvalidOutput("worktree branch record omitted its path".into())
                })?);
            }
        }
        match matches.as_slice() {
            [] => Ok(None),
            [path] => Ok(Some(path.clone())),
            _ => Err(GitError::InvalidOutput(format!(
                "branch `{branch_ref}` is checked out in multiple worktrees"
            ))),
        }
    }

    pub async fn ensure_clean(&self) -> Result<(), GitError> {
        let checks = [
            (
                ["diff-index", "--quiet", "HEAD", "--"].as_slice(),
                "staged changes",
            ),
            (
                ["diff-files", "--quiet", "--"].as_slice(),
                "tracked modifications",
            ),
        ];
        for (args, label) in checks {
            let status = self.git_status(args.iter().copied()).await?;
            if !status.success() {
                return Err(GitError::DirtyWorktree(label.into()));
            }
        }
        let untracked = self
            .git(["ls-files", "--others", "--exclude-standard", "-z"])
            .await?;
        if !untracked.is_empty() {
            return Err(GitError::DirtyWorktree(
                "non-ignored untracked files".into(),
            ));
        }
        Ok(())
    }

    pub async fn probe_approval(&self, revision: &str) -> Result<ApprovalProbe, GitError> {
        self.probe_revision(revision, true).await
    }

    pub async fn probe_check(&self, revision: &str) -> Result<ApprovalProbe, GitError> {
        self.probe_revision(revision, false).await
    }

    async fn probe_revision(
        &self,
        revision: &str,
        reject_integrated: bool,
    ) -> Result<ApprovalProbe, GitError> {
        self.ensure_clean().await?;
        self.ensure_integration_not_checked_out().await?;
        let source_oid = self.resolve_oid(revision).await?;
        let integration = self.integration_oid().await?;
        if reject_integrated && self.is_ancestor(&source_oid, &integration).await? {
            return Err(GitError::InvalidOutput(
                "source is already an ancestor of release".into(),
            ));
        }
        let parents = text(
            self.git(["show", "-s", "--format=%P", &source_oid.to_hex()])
                .await?,
        )?;
        let parent_values = parents.split_whitespace().collect::<Vec<_>>();
        if parent_values.len() > 1 || (reject_integrated && parent_values.len() != 1) {
            return Err(GitError::InvalidSourceShape);
        }
        let parent_oid = if let Some(parent) = parent_values.first() {
            GitOid::from_hex(parent)?
        } else {
            source_oid.clone()
        };
        let metadata = self
            .git([
                "show",
                "-s",
                "--format=%s%x00%an%x00%ae",
                &source_oid.to_hex(),
            ])
            .await?;
        let fields = metadata.split(|byte| *byte == 0).collect::<Vec<_>>();
        if fields.len() < 3 {
            return Err(GitError::InvalidOutput("commit metadata frame".into()));
        }
        let branch_output = self.git(["branch", "--show-current"]).await?;
        let branch = text(branch_output)?.trim().to_owned();
        let raw_commit = self
            .git(["cat-file", "commit", &source_oid.to_hex()])
            .await?;
        let message = raw_commit
            .windows(2)
            .position(|window| window == b"\n\n")
            .map(|index| &raw_commit[index + 2..])
            .ok_or_else(|| {
                GitError::MalformedCommit("commit message separator is missing".into())
            })?;
        Ok(ApprovalProbe {
            source_oid,
            parent_oid,
            subject: String::from_utf8_lossy(fields[0]).into_owned(),
            author_name: String::from_utf8_lossy(fields[1]).into_owned(),
            author_email: String::from_utf8_lossy(fields[2]).trim().into(),
            branch: (!branch.is_empty()).then_some(branch),
            message_hash: blake3::hash(message).to_hex().to_string(),
        })
    }

    pub async fn unmerged_first_parent_ancestors(
        &self,
        parent: &GitOid,
        master: &GitOid,
    ) -> Result<Vec<GitOid>, GitError> {
        if self.is_ancestor(parent, master).await? {
            return Ok(Vec::new());
        }
        let output = self
            .git([
                "rev-list",
                "--first-parent",
                &parent.to_hex(),
                &format!("^{}", master.to_hex()),
            ])
            .await?;
        text(output)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(GitOid::from_hex)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub async fn changed_paths(&self, source: &GitOid) -> Result<Vec<String>, GitError> {
        let output = self
            .git([
                "diff-tree",
                "--no-commit-id",
                "--name-status",
                "-r",
                "-z",
                "--find-renames",
                "--find-copies",
                &source.to_hex(),
            ])
            .await?;
        let fields = output
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty())
            .collect::<Vec<_>>();
        let mut paths = Vec::new();
        let mut index = 0;
        while index < fields.len() {
            let status = String::from_utf8_lossy(fields[index]);
            index += 1;
            let count = if status.starts_with('R') || status.starts_with('C') {
                2
            } else {
                1
            };
            for _ in 0..count {
                let field = fields.get(index).ok_or_else(|| {
                    GitError::InvalidOutput("truncated NUL-delimited diff-tree output".into())
                })?;
                let path = std::str::from_utf8(field)
                    .map_err(|_| GitError::InvalidOutput("changed path is not UTF-8".into()))?;
                paths.push(path.to_owned());
                index += 1;
            }
        }
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    pub async fn ignored_directories(&self, worktree: &Path) -> Result<Vec<PathBuf>, GitError> {
        let output = run_raw(
            &self.profile.executable,
            worktree,
            [
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
                "-z",
            ],
        )
        .await?;
        let mut directories = output
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty())
            .filter_map(|field| std::str::from_utf8(field).ok())
            .map(|path| PathBuf::from(path.trim_end_matches('/')))
            .filter(|path| {
                !path.as_os_str().is_empty()
                    && !path.is_absolute()
                    && path
                        .components()
                        .all(|component| matches!(component, std::path::Component::Normal(_)))
                    && worktree.join(path).is_dir()
            })
            .collect::<Vec<_>>();
        directories.sort();
        directories.dedup();
        let mut top_level = Vec::new();
        for directory in directories {
            if !top_level
                .iter()
                .any(|parent: &PathBuf| directory.starts_with(parent))
            {
                top_level.push(directory);
            }
        }
        Ok(top_level)
    }

    pub async fn create_source_ref(
        &self,
        item_id: QueueItemId,
        source_oid: &GitOid,
    ) -> Result<String, GitError> {
        let name = format!("refs/tollgate/sources/{item_id}");
        let zero = "0".repeat(self.profile.object_format.byte_len() * 2);
        self.git(["update-ref", &name, &source_oid.to_hex(), &zero])
            .await?;
        let observed = self.resolve_oid(&name).await?;
        if observed != *source_oid {
            return Err(GitError::InvalidOutput(
                "source retention ref mismatch".into(),
            ));
        }
        Ok(name)
    }

    pub async fn delete_source_ref(
        &self,
        reference: &str,
        expected_source: &GitOid,
    ) -> Result<(), GitError> {
        self.git(["update-ref", "-d", reference, &expected_source.to_hex()])
            .await?;
        if self.optional_ref_oid(reference).await?.is_some() {
            return Err(GitError::InvalidOutput(
                "source retention ref still exists after asserted deletion".into(),
            ));
        }
        Ok(())
    }

    pub async fn cleanup_linked_source_worktree(
        &self,
        worktree: &Path,
        branch: &str,
        expected_source: &GitOid,
    ) -> Result<bool, GitError> {
        let worktree = std::fs::canonicalize(worktree)?;
        if worktree == self.worktree_root || matches!(branch, USER_BRANCH | INTEGRATION_BRANCH) {
            return Ok(false);
        }
        let discovered = Self::discover(&worktree).await?;
        if discovered.common_dir != self.common_dir {
            return Err(GitError::InvalidOutput(
                "cleanup worktree belongs to a different repository".into(),
            ));
        }
        discovered.ensure_clean().await?;
        if discovered.resolve_oid("HEAD").await? != *expected_source {
            return Err(GitError::InvalidOutput(
                "cleanup worktree moved after approval".into(),
            ));
        }
        let branch_ref = format!("refs/heads/{branch}");
        if self.resolve_oid(&branch_ref).await? != *expected_source {
            return Err(GitError::InvalidOutput(
                "cleanup branch moved after approval".into(),
            ));
        }
        self.git([
            "worktree",
            "remove",
            "--force",
            worktree.to_string_lossy().as_ref(),
        ])
        .await?;
        self.git(["update-ref", "-d", &branch_ref, &expected_source.to_hex()])
            .await?;
        Ok(true)
    }

    pub async fn current_branch(&self) -> Result<Option<String>, GitError> {
        let branch = text(self.git(["branch", "--show-current"]).await?)?
            .trim()
            .to_owned();
        Ok((!branch.is_empty()).then_some(branch))
    }

    pub async fn initialize_integration_ref_from_master(&self) -> Result<GitOid, GitError> {
        self.ensure_integration_not_checked_out().await?;
        let master = self.resolve_oid(USER_BRANCH_REF).await?;
        match self.optional_ref_oid(INTEGRATION_REF).await? {
            Some(release) if release != master => Err(GitError::InvalidOutput(format!(
                "local `release` already exists at {}, but `master` is {}; Tollgate will not overwrite it",
                release.short(),
                master.short()
            ))),
            Some(release) => Ok(release),
            None => {
                self.git(["update-ref", INTEGRATION_REF, &master.to_hex(), ""])
                    .await?;
                let release = self.integration_oid().await?;
                if release != master {
                    return Err(GitError::InvalidOutput(
                        "release initialization did not preserve the exact master OID".into(),
                    ));
                }
                Ok(release)
            }
        }
    }

    pub async fn migrate_integration_ref_from_master(&self) -> Result<GitOid, GitError> {
        self.ensure_integration_not_checked_out().await?;
        let master = self.resolve_oid(USER_BRANCH_REF).await?;
        if let Some(release) = self.optional_ref_oid(INTEGRATION_REF).await? {
            if release != master {
                return Err(GitError::InvalidOutput(format!(
                    "cannot migrate Tollgate authority: existing `release` {} differs from legacy `master` {}",
                    release.short(),
                    master.short()
                )));
            }
            return Ok(release);
        }
        self.git(["update-ref", INTEGRATION_REF, &master.to_hex(), ""])
            .await?;
        self.integration_oid().await
    }

    pub async fn create_feature_worktree(
        &self,
        branch: &str,
        destination: &Path,
    ) -> Result<GitOid, GitError> {
        if destination.exists() {
            return Err(GitError::InvalidOutput(format!(
                "worktree destination already exists: {}",
                destination.display()
            )));
        }
        let valid = self
            .git_status(["check-ref-format", "--branch", branch])
            .await?;
        if !valid.success() || matches!(branch, USER_BRANCH | INTEGRATION_BRANCH) {
            return Err(GitError::InvalidOutput(
                "feature branch name is invalid or reserved".into(),
            ));
        }
        let master = self.integration_oid().await?;
        self.git([
            "worktree",
            "add",
            "-b",
            branch,
            destination.to_string_lossy().as_ref(),
            &master.to_hex(),
        ])
        .await?;
        let created = Self::discover(destination).await?;
        if created.common_dir != self.common_dir || created.resolve_oid("HEAD").await? != master {
            return Err(GitError::InvalidOutput(
                "created worktree identity did not match the gated release".into(),
            ));
        }
        Ok(master)
    }

    pub async fn update_one_commit_feature(&self) -> Result<(GitOid, GitOid), GitError> {
        self.ensure_clean().await?;
        let branch = self.current_branch().await?.ok_or_else(|| {
            GitError::InvalidOutput("feature update requires a checked-out branch".into())
        })?;
        if matches!(branch.as_str(), USER_BRANCH | INTEGRATION_BRANCH) {
            return Err(GitError::InvalidOutput(
                "feature update cannot operate on master or release".into(),
            ));
        }
        let old = self.resolve_oid("HEAD").await?;
        let old_parent = self.commit_parent_oid(&old).await?;
        let master = self.integration_oid().await?;
        if old_parent == master {
            return Ok((old.clone(), old));
        }
        let unique = text(
            self.git([
                "rev-list",
                "--count",
                &format!("{}..{}", master.to_hex(), old.to_hex()),
            ])
            .await?,
        )?;
        if unique.trim() != "1" {
            return Err(GitError::InvalidOutput(
                "feature update requires exactly one unique source commit".into(),
            ));
        }
        let output = internal_command(&self.profile.executable, &self.worktree_root)
            .args([
                "rebase",
                "--onto",
                &master.to_hex(),
                &old_parent.to_hex(),
                &branch,
            ])
            .output()
            .await?;
        if !output.status.success() {
            let _ = internal_command(&self.profile.executable, &self.worktree_root)
                .args(["rebase", "--abort"])
                .status()
                .await;
            return Err(GitError::Unmergeable);
        }
        let new = self.resolve_oid("HEAD").await?;
        if self.commit_parent_oid(&new).await? != master {
            return Err(GitError::InvalidOutput(
                "updated feature does not have current release as its exact parent".into(),
            ));
        }
        Ok((old, new))
    }

    pub async fn initialize_mirror(&self, mirror: &Path) -> Result<(), GitError> {
        if !mirror.exists() {
            fs::create_dir_all(
                mirror
                    .parent()
                    .ok_or_else(|| GitError::InvalidOutput("mirror parent".into()))?,
            )
            .await?;
            run_raw(
                &self.profile.executable,
                mirror.parent().unwrap(),
                ["init", "--bare", mirror.to_string_lossy().as_ref()],
            )
            .await?;
        }
        let source = self.common_dir.to_string_lossy();
        run_raw(
            &self.profile.executable,
            mirror,
            [
                "fetch",
                "--no-tags",
                source.as_ref(),
                "+refs/heads/release:refs/tollgate/release",
                "+refs/tollgate/sources/*:refs/tollgate/sources/*",
            ],
        )
        .await?;
        Ok(())
    }

    pub async fn construct_prefix(
        &self,
        mirror: &Path,
        builder: &Path,
        base: &GitOid,
        sources: &[GitOid],
    ) -> Result<Vec<SyntheticCommit>, GitError> {
        if builder.exists() {
            let _ = run_raw(
                &self.profile.executable,
                mirror,
                [
                    "worktree",
                    "remove",
                    "--force",
                    builder.to_string_lossy().as_ref(),
                ],
            )
            .await;
        }
        run_raw(
            &self.profile.executable,
            mirror,
            [
                "worktree",
                "add",
                "--detach",
                builder.to_string_lossy().as_ref(),
                &base.to_hex(),
            ],
        )
        .await?;
        let mut parent = base.clone();
        let mut result = Vec::with_capacity(sources.len());
        for source in sources {
            let source_parent = self.commit_parent_oid(source).await?;
            let apply = internal_command(&self.profile.executable, builder)
                .args([
                    "cherry-pick",
                    "--no-commit",
                    "--strategy=ort",
                    &source.to_hex(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await?;
            if !apply.status.success() {
                let mut conflicting_paths = nul_delimited_strings(
                    run_internal(
                        &self.profile.executable,
                        builder,
                        ["diff", "--name-only", "--diff-filter=U", "-z"],
                    )
                    .await?,
                );
                conflicting_paths.sort();
                conflicting_paths.dedup();
                let attempted_tree = if conflicting_paths.is_empty() {
                    Some(GitOid::from_hex(
                        text(
                            run_internal(&self.profile.executable, builder, ["write-tree"]).await?,
                        )?
                        .trim(),
                    )?)
                } else {
                    None
                };
                let parent_tree = self.mirror_tree_oid(mirror, &parent).await?;
                let _ = internal_command(&self.profile.executable, builder)
                    .args(["reset", "--hard", &parent.to_hex()])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
                if !conflicting_paths.is_empty() {
                    return Err(GitError::SyntheticConflict {
                        source_oid: source.clone(),
                        source_parent_oid: source_parent,
                        prefix_oid: parent,
                        conflicting_paths,
                    });
                }
                if attempted_tree.as_ref() == Some(&parent_tree) {
                    return Err(GitError::SyntheticEmpty {
                        source_oid: source.clone(),
                        source_parent_oid: source_parent,
                        prefix_oid: parent,
                    });
                }
                return Err(GitError::Command {
                    command: format!(
                        "git cherry-pick --no-commit --strategy=ort {}",
                        source.to_hex()
                    ),
                    stderr: String::from_utf8_lossy(&apply.stderr).trim().into(),
                });
            }
            let tree = GitOid::from_hex(
                text(run_raw(&self.profile.executable, builder, ["write-tree"]).await?)?.trim(),
            )?;
            let parent_tree = GitOid::from_hex(
                text(
                    run_raw(
                        &self.profile.executable,
                        builder,
                        ["show", "-s", "--format=%T", &parent.to_hex()],
                    )
                    .await?,
                )?
                .trim(),
            )?;
            if tree == parent_tree {
                return Err(GitError::SyntheticEmpty {
                    source_oid: source.clone(),
                    source_parent_oid: source_parent,
                    prefix_oid: parent,
                });
            }
            let raw_source = run_raw(
                &self.profile.executable,
                mirror,
                ["cat-file", "commit", &source.to_hex()],
            )
            .await?;
            let parsed = RawCommit::parse(&raw_source)?;
            // Reuse the source object itself when it is already the exact prospective
            // commit. Besides avoiding needless object churn, this preserves signed
            // commit bytes. Signatures are deliberately stripped only when a parent
            // or tree rewrite makes the original signature invalid.
            let tree_hex = tree.to_hex();
            let parent_hex = parent.to_hex();
            let oid =
                if parsed.tree == tree_hex.as_bytes() && parsed.parent == parent_hex.as_bytes() {
                    source.clone()
                } else {
                    let raw_synthetic = parsed.rewrite(&tree_hex, &parent_hex);
                    let oid_bytes =
                        hash_object(&self.profile.executable, mirror, &raw_synthetic).await?;
                    GitOid::from_hex(text(oid_bytes)?.trim())?
                };
            run_raw(
                &self.profile.executable,
                builder,
                ["reset", "--hard", &oid.to_hex()],
            )
            .await?;
            result.push(SyntheticCommit {
                source_oid: source.clone(),
                oid: oid.clone(),
                parent_oid: parent.clone(),
                tree_oid: tree,
            });
            parent = oid;
        }
        Ok(result)
    }

    pub async fn retain_tested_object(
        &self,
        mirror: &Path,
        buildset_ref: &str,
        oid: &GitOid,
    ) -> Result<(), GitError> {
        self.retain_mirror_object(mirror, &format!("refs/tollgate/tested/{buildset_ref}"), oid)
            .await
    }

    pub async fn retain_projected_object(
        &self,
        mirror: &Path,
        generation_ref: &str,
        oid: &GitOid,
    ) -> Result<(), GitError> {
        self.retain_mirror_object(
            mirror,
            &format!("refs/tollgate/projected/{generation_ref}"),
            oid,
        )
        .await
    }

    async fn retain_mirror_object(
        &self,
        mirror: &Path,
        destination: &str,
        oid: &GitOid,
    ) -> Result<(), GitError> {
        if let Ok(existing) = self.resolve_oid(destination).await {
            return if existing == *oid {
                Ok(())
            } else {
                Err(GitError::InvalidOutput(format!(
                    "retained-object ref {destination} already records a different object"
                )))
            };
        }
        let incoming = format!("refs/tollgate/incoming/{}", uuid::Uuid::now_v7());
        let source = mirror.to_string_lossy();
        self.git([
            "fetch",
            source.as_ref(),
            &format!("{}:{incoming}", oid.to_hex()),
        ])
        .await?;
        let zero = "0".repeat(self.profile.object_format.byte_len() * 2);
        let update = self
            .git(["update-ref", destination, &oid.to_hex(), &zero])
            .await;
        let _ = self.git(["update-ref", "-d", &incoming]).await;
        if let Err(error) = update {
            if self.resolve_oid(destination).await.ok().as_ref() == Some(oid) {
                return Ok(());
            }
            return Err(error);
        }
        if self.resolve_oid(destination).await? != *oid {
            return Err(GitError::InvalidOutput(
                "tested object retention mismatch".into(),
            ));
        }
        Ok(())
    }

    pub async fn provision_slot(
        &self,
        mirror: &Path,
        slot: &Path,
        oid: &GitOid,
    ) -> Result<(), GitError> {
        if slot.exists() {
            let reset = run_raw(
                &self.profile.executable,
                slot,
                ["reset", "--hard", &oid.to_hex()],
            )
            .await;
            if reset.is_ok() {
                run_raw(&self.profile.executable, slot, ["clean", "-d", "-f"]).await?;
                return Ok(());
            }
            let _ = run_raw(
                &self.profile.executable,
                mirror,
                [
                    "worktree",
                    "remove",
                    "--force",
                    slot.to_string_lossy().as_ref(),
                ],
            )
            .await;
        }
        if let Some(parent) = slot.parent() {
            fs::create_dir_all(parent).await?;
        }
        run_raw(
            &self.profile.executable,
            mirror,
            [
                "worktree",
                "add",
                "--detach",
                "--lock",
                slot.to_string_lossy().as_ref(),
                &oid.to_hex(),
            ],
        )
        .await?;
        run_raw(&self.profile.executable, slot, ["clean", "-d", "-f"]).await?;
        Ok(())
    }

    pub async fn remove_slot(&self, mirror: &Path, slot: &Path) -> Result<(), GitError> {
        let _ = run_raw(
            &self.profile.executable,
            mirror,
            ["worktree", "unlock", slot.to_string_lossy().as_ref()],
        )
        .await;
        run_raw(
            &self.profile.executable,
            mirror,
            [
                "worktree",
                "remove",
                "--force",
                slot.to_string_lossy().as_ref(),
            ],
        )
        .await?;
        run_raw(
            &self.profile.executable,
            mirror,
            ["worktree", "prune", "--expire", "now"],
        )
        .await?;
        Ok(())
    }

    pub async fn worktree_patch(
        &self,
        slot: &Path,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, GitError> {
        run_raw(
            &self.profile.executable,
            slot,
            ["add", "--intent-to-add", "--all"],
        )
        .await?;
        let mut patch = tempfile::tempfile()?;
        let mut command = Command::new(&self.profile.executable);
        command
            .current_dir(slot)
            .args(["diff", "--binary", "--full-index", "--no-ext-diff", "HEAD"])
            .stdout(Stdio::from(patch.try_clone()?))
            .stderr(Stdio::piped());
        let output = command.spawn()?.wait_with_output().await?;
        if !output.status.success() {
            return Err(GitError::Command {
                command: "git diff --binary --full-index --no-ext-diff HEAD".into(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().into(),
            });
        }
        let length = patch.metadata()?.len();
        if length > maximum_bytes {
            return Err(GitError::InvalidOutput(format!(
                "repair patch is {length} bytes, exceeding the {maximum_bytes}-byte limit"
            )));
        }
        patch.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::with_capacity(length as usize);
        tokio::fs::File::from_std(patch)
            .read_to_end(&mut bytes)
            .await?;
        Ok(bytes)
    }

    pub async fn quarantine_slot(
        &self,
        mirror: &Path,
        slot: &Path,
        quarantine: &Path,
    ) -> Result<(), GitError> {
        let metadata = std::fs::symlink_metadata(slot)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() || quarantine.exists() {
            return Err(GitError::InvalidOutput(
                "slot quarantine requires an existing real directory and absent destination".into(),
            ));
        }
        if let Some(parent) = quarantine.parent() {
            fs::create_dir_all(parent).await?;
        }
        let _ = run_raw(
            &self.profile.executable,
            mirror,
            ["worktree", "unlock", slot.to_string_lossy().as_ref()],
        )
        .await;
        fs::rename(slot, quarantine).await?;
        run_raw(
            &self.profile.executable,
            mirror,
            ["worktree", "prune", "--expire", "now"],
        )
        .await?;
        if slot.exists() || !quarantine.exists() {
            return Err(GitError::InvalidOutput(
                "slot quarantine path identities did not settle as expected".into(),
            ));
        }
        Ok(())
    }

    pub async fn compare_and_swap_integration(
        &self,
        expected_old: &GitOid,
        tested_new: &GitOid,
    ) -> Result<(), GitError> {
        self.ensure_integration_not_checked_out().await?;
        self.git([
            "update-ref",
            INTEGRATION_REF,
            &tested_new.to_hex(),
            &expected_old.to_hex(),
        ])
        .await?;
        if self.integration_oid().await? != *tested_new {
            return Err(GitError::InvalidOutput(
                "release CAS result mismatch".into(),
            ));
        }
        Ok(())
    }

    pub async fn sync_user_master(
        &self,
        tested_new: &GitOid,
        replace_source: Option<&GitOid>,
        remote_tracking: Option<(&str, &str, &GitOid)>,
    ) -> Result<UserMasterSyncOutcome, GitError> {
        if let Some((remote, branch, remote_oid)) = remote_tracking {
            let tracking_ref = format!("refs/remotes/{remote}/{branch}");
            if !self
                .git_status(["check-ref-format", &tracking_ref])
                .await?
                .success()
            {
                return Ok(UserMasterSyncOutcome::NeedsAttention {
                    path: None,
                    reason: format!("remote-tracking ref `{tracking_ref}` is invalid"),
                });
            }
            let current_tracking = self.optional_ref_oid(&tracking_ref).await?;
            if current_tracking.as_ref() != Some(remote_oid) {
                let expected = current_tracking
                    .as_ref()
                    .map(GitOid::to_hex)
                    .unwrap_or_else(|| "0".repeat(self.profile.object_format.byte_len() * 2));
                self.git(["update-ref", &tracking_ref, &remote_oid.to_hex(), &expected])
                    .await?;
            }
        }
        let path = self.worktree_for_branch(USER_BRANCH_REF).await?;
        let Some(current) = self.optional_ref_oid(USER_BRANCH_REF).await? else {
            return Ok(UserMasterSyncOutcome::NeedsAttention {
                path,
                reason: "local `master` does not exist".into(),
            });
        };

        if let Some(path) = path.as_ref() {
            let checkout = Self::discover(path).await?;
            if checkout.common_dir != self.common_dir {
                return Err(GitError::InvalidOutput(
                    "master worktree belongs to a different repository".into(),
                ));
            }
            match checkout.ensure_clean().await {
                Ok(()) => {}
                Err(GitError::DirtyWorktree(reason)) => {
                    return Ok(UserMasterSyncOutcome::NeedsAttention {
                        path: Some(path.clone()),
                        reason: format!("master worktree is dirty: {reason}"),
                    });
                }
                Err(error) => return Err(error),
            }
            if checkout.current_branch().await?.as_deref() != Some(USER_BRANCH)
                || checkout.resolve_oid("HEAD").await? != current
            {
                return Ok(UserMasterSyncOutcome::NeedsAttention {
                    path: Some(path.clone()),
                    reason: "master worktree HEAD does not match the local master ref".into(),
                });
            }
        }

        if current == *tested_new {
            return Ok(UserMasterSyncOutcome::AlreadyCurrent { path });
        }
        if replace_source == Some(&current) {
            if let Some(path) = path {
                run_internal(
                    &self.profile.executable,
                    &path,
                    ["reset", "--keep", &tested_new.to_hex()],
                )
                .await?;
                let checkout = Self::discover(&path).await?;
                if checkout.resolve_oid("HEAD").await? != *tested_new
                    || self.resolve_oid(USER_BRANCH_REF).await? != *tested_new
                {
                    return Err(GitError::InvalidOutput(
                        "projected master checkout result mismatch".into(),
                    ));
                }
                checkout.ensure_clean().await?;
                return Ok(UserMasterSyncOutcome::UpdatedCheckout { path });
            }
            self.git([
                "update-ref",
                USER_BRANCH_REF,
                &tested_new.to_hex(),
                &current.to_hex(),
            ])
            .await?;
            if self.resolve_oid(USER_BRANCH_REF).await? != *tested_new {
                return Err(GitError::InvalidOutput(
                    "projected master ref result mismatch".into(),
                ));
            }
            return Ok(UserMasterSyncOutcome::UpdatedRef);
        }
        if !self.is_ancestor(&current, tested_new).await? {
            return Ok(UserMasterSyncOutcome::NeedsAttention {
                path,
                reason: format!(
                    "local master {} cannot fast-forward to certified release {}",
                    current.short(),
                    tested_new.short()
                ),
            });
        }

        if let Some(path) = path {
            run_internal(
                &self.profile.executable,
                &path,
                ["merge", "--ff-only", "--no-edit", &tested_new.to_hex()],
            )
            .await?;
            let checkout = Self::discover(&path).await?;
            if checkout.resolve_oid("HEAD").await? != *tested_new
                || self.resolve_oid(USER_BRANCH_REF).await? != *tested_new
            {
                return Err(GitError::InvalidOutput(
                    "checked-out master fast-forward result mismatch".into(),
                ));
            }
            checkout.ensure_clean().await?;
            return Ok(UserMasterSyncOutcome::UpdatedCheckout { path });
        }

        self.git([
            "update-ref",
            USER_BRANCH_REF,
            &tested_new.to_hex(),
            &current.to_hex(),
        ])
        .await?;
        if self.resolve_oid(USER_BRANCH_REF).await? != *tested_new {
            return Err(GitError::InvalidOutput(
                "user master CAS result mismatch".into(),
            ));
        }
        Ok(UserMasterSyncOutcome::UpdatedRef)
    }

    pub async fn observe_remote_ref(
        &self,
        remote: &str,
        branch: &str,
    ) -> Result<Option<GitOid>, GitError> {
        let reference = format!("refs/heads/{branch}");
        let output = Command::new(&self.profile.executable)
            .current_dir(&self.worktree_root)
            .args(["ls-remote", "--exit-code", "--refs", remote, &reference])
            .output()
            .await?;
        if output.status.success() {
            let stdout = text(output.stdout)?;
            let mut lines = stdout.lines();
            let line = lines
                .next()
                .ok_or_else(|| GitError::InvalidOutput("empty remote observation".into()))?;
            if lines.next().is_some() {
                return Err(GitError::InvalidOutput(
                    "remote observation returned multiple exact-ref matches".into(),
                ));
            }
            let (oid, observed_ref) = line
                .split_once(char::is_whitespace)
                .ok_or_else(|| GitError::InvalidOutput("malformed remote observation".into()))?;
            if observed_ref.trim() != reference {
                return Err(GitError::InvalidOutput(
                    "remote observation returned the wrong ref".into(),
                ));
            }
            return GitOid::from_hex(oid.trim()).map(Some).map_err(Into::into);
        }
        if output.status.code() == Some(2) && output.stdout.is_empty() && output.stderr.is_empty() {
            return Ok(None);
        }
        Err(GitError::Command {
            command: format!("git ls-remote --exit-code --refs {remote} {reference}"),
            stderr: String::from_utf8_lossy(&output.stderr).trim().into(),
        })
    }

    pub async fn fetch_remote_ref(
        &self,
        remote: &str,
        branch: &str,
        observation_ref: &str,
    ) -> Result<Option<GitOid>, GitError> {
        let observed = self.observe_remote_ref(remote, branch).await?;
        let Some(oid) = observed else {
            if let Some(existing) = self.optional_ref_oid(observation_ref).await? {
                self.git(["update-ref", "-d", observation_ref, &existing.to_hex()])
                    .await?;
            }
            return Ok(None);
        };
        self.git([
            "fetch",
            "--no-tags",
            remote,
            &format!("+refs/heads/{branch}:{observation_ref}"),
        ])
        .await?;
        let fetched = self
            .optional_ref_oid(observation_ref)
            .await?
            .ok_or_else(|| GitError::InvalidOutput("fetched observation ref is absent".into()))?;
        if fetched != oid {
            return Err(GitError::InvalidOutput(
                "remote changed while its exact ref was being fetched".into(),
            ));
        }
        Ok(Some(oid))
    }

    pub async fn remote_url(&self, remote: &str, push: bool) -> Result<String, GitError> {
        let output = if push {
            self.git(["remote", "get-url", "--push", remote]).await?
        } else {
            self.git(["remote", "get-url", remote]).await?
        };
        let url = text(output)?.trim().to_owned();
        if url.is_empty() || url.lines().count() != 1 {
            return Err(GitError::InvalidOutput(format!(
                "remote {remote} resolved to an invalid URL"
            )));
        }
        Ok(url)
    }

    pub async fn push_with_lease(
        &self,
        remote: &str,
        branch: &str,
        expected: Option<&GitOid>,
        new_oid: &GitOid,
    ) -> Result<(), GitError> {
        let reference = format!("refs/heads/{branch}");
        let expected = expected.map(GitOid::to_hex).unwrap_or_default();
        let output = Command::new(&self.profile.executable)
            .current_dir(&self.worktree_root)
            .args([
                "push",
                "--porcelain",
                &format!("--force-with-lease={reference}:{expected}"),
                remote,
                &format!("{}:{reference}", new_oid.to_hex()),
            ])
            .output()
            .await?;
        if !output.status.success() {
            return Err(GitError::Command {
                command: format!("git push --force-with-lease {remote} {reference}"),
                stderr: String::from_utf8_lossy(&output.stderr).trim().into(),
            });
        }
        let observed = self.observe_remote_ref(remote, branch).await?;
        if observed.as_ref() != Some(new_oid) {
            return Err(GitError::InvalidOutput(
                "remote did not contain the exact pushed object after transport success".into(),
            ));
        }
        Ok(())
    }

    pub async fn first_parent_commits_between(
        &self,
        older: &GitOid,
        newer: &GitOid,
    ) -> Result<Vec<GitOid>, GitError> {
        let output = self
            .git([
                "rev-list",
                "--first-parent",
                "--reverse",
                &format!("{}..{}", older.to_hex(), newer.to_hex()),
            ])
            .await?;
        text(output)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(GitOid::from_hex)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub async fn merge_base_oid(&self, left: &GitOid, right: &GitOid) -> Result<GitOid, GitError> {
        let output = self
            .git(["merge-base", &left.to_hex(), &right.to_hex()])
            .await?;
        GitOid::from_hex(text(output)?.trim()).map_err(Into::into)
    }

    pub async fn rebase_user_master_onto_release(
        &self,
        expected_master: &GitOid,
    ) -> Result<GitOid, GitError> {
        self.ensure_clean().await?;
        if self.current_branch().await?.as_deref() != Some(USER_BRANCH)
            || self.resolve_oid("HEAD").await? != *expected_master
            || self.resolve_oid(USER_BRANCH_REF).await? != *expected_master
        {
            return Err(GitError::InvalidOutput(
                "master moved after push preflight".into(),
            ));
        }
        let release = self.integration_oid().await?;
        if release == *expected_master || self.is_ancestor(&release, expected_master).await? {
            return Ok(expected_master.clone());
        }
        let base = self.merge_base_oid(&release, expected_master).await?;
        let commits = self
            .first_parent_commits_between(&base, expected_master)
            .await?;
        let mut expected_parent = base.clone();
        for commit in &commits {
            let parent = self.commit_parent_oid(commit).await?;
            if parent != expected_parent {
                return Err(GitError::InvalidOutput(format!(
                    "master commit {} does not directly follow {}; only a linear, merge-free master range can be rebased",
                    commit.short(),
                    expected_parent.short()
                )));
            }
            expected_parent = commit.clone();
        }
        let operation = if commits.is_empty() {
            internal_command(&self.profile.executable, &self.worktree_root)
                .args(["merge", "--ff-only", &release.to_hex()])
                .output()
                .await?
        } else {
            internal_command(&self.profile.executable, &self.worktree_root)
                .args([
                    "rebase",
                    "--onto",
                    &release.to_hex(),
                    &base.to_hex(),
                    USER_BRANCH,
                ])
                .output()
                .await?
        };
        if !operation.status.success() {
            if !commits.is_empty() {
                let _ = internal_command(&self.profile.executable, &self.worktree_root)
                    .args(["rebase", "--abort"])
                    .status()
                    .await;
            }
            return Err(GitError::Unmergeable);
        }
        let new_master = self.resolve_oid(USER_BRANCH_REF).await?;
        if self.resolve_oid("HEAD").await? != new_master
            || !self.is_ancestor(&release, &new_master).await?
        {
            return Err(GitError::InvalidOutput(
                "rebased master did not descend from certified release".into(),
            ));
        }
        self.ensure_clean().await?;
        Ok(new_master)
    }

    pub async fn unmerged_user_master_commits(&self) -> Result<Vec<GitOid>, GitError> {
        let release = self.integration_oid().await?;
        let master = self.resolve_oid(USER_BRANCH_REF).await?;
        if release == master {
            return Ok(Vec::new());
        }
        if !self.is_ancestor(&release, &master).await? {
            return Err(GitError::InvalidOutput(format!(
                "local master {} is not a descendant of certified release {}; reconcile the branches before pushing master",
                master.short(),
                release.short()
            )));
        }
        let commits = self.first_parent_commits_between(&release, &master).await?;
        let mut expected_parent = release;
        for commit in &commits {
            let parent = self.commit_parent_oid(commit).await?;
            if parent != expected_parent {
                return Err(GitError::InvalidOutput(format!(
                    "master commit {} does not directly follow {}; only a linear, merge-free master range can be submitted",
                    commit.short(),
                    expected_parent.short()
                )));
            }
            expected_parent = commit.clone();
        }
        if expected_parent != master {
            return Err(GitError::InvalidOutput(
                "master range did not resolve to the local master tip".into(),
            ));
        }
        Ok(commits)
    }

    pub async fn is_ancestor(
        &self,
        ancestor: &GitOid,
        descendant: &GitOid,
    ) -> Result<bool, GitError> {
        let status = self
            .git_status([
                "merge-base",
                "--is-ancestor",
                &ancestor.to_hex(),
                &descendant.to_hex(),
            ])
            .await?;
        Ok(status.success())
    }

    async fn git<I, S>(&self, args: I) -> Result<Vec<u8>, GitError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        run_internal(&self.profile.executable, &self.worktree_root, args).await
    }

    async fn git_status<I, S>(&self, args: I) -> Result<std::process::ExitStatus, GitError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Ok(
            internal_command(&self.profile.executable, &self.worktree_root)
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?,
        )
    }
}

#[derive(Debug)]
struct RawCommit<'a> {
    tree: &'a [u8],
    parent: &'a [u8],
    author: &'a [u8],
    committer: &'a [u8],
    encoding: Option<&'a [u8]>,
    message: &'a [u8],
}

impl<'a> RawCommit<'a> {
    fn parse(raw: &'a [u8]) -> Result<Self, GitError> {
        let separator = raw
            .windows(2)
            .position(|window| window == b"\n\n")
            .ok_or_else(|| GitError::MalformedCommit("missing header separator".into()))?;
        let (headers, message_with_separator) = raw.split_at(separator);
        let message = &message_with_separator[2..];
        let lines = continuation_lines(headers)?;
        let mut tree = None;
        let mut parent = None;
        let mut author = None;
        let mut committer = None;
        let mut encoding = None;
        let mut gpg_signature_seen = false;
        let mut sha256_signature_seen = false;
        for (name, value) in lines {
            match name {
                b"tree" if tree.replace(value).is_none() => {}
                b"parent" if parent.replace(value).is_none() => {}
                b"author" if author.replace(value).is_none() => {}
                b"committer" if committer.replace(value).is_none() => {}
                b"encoding" if encoding.replace(value).is_none() => {}
                b"gpgsig" if !gpg_signature_seen => gpg_signature_seen = true,
                b"gpgsig-sha256" if !sha256_signature_seen => sha256_signature_seen = true,
                _ => {
                    return Err(GitError::MalformedCommit(format!(
                        "duplicate or unknown header `{}`",
                        String::from_utf8_lossy(name)
                    )));
                }
            }
        }
        Ok(Self {
            tree: tree.ok_or(GitError::InvalidSourceShape)?,
            parent: parent.ok_or(GitError::InvalidSourceShape)?,
            author: author.ok_or_else(|| GitError::MalformedCommit("missing author".into()))?,
            committer: committer
                .ok_or_else(|| GitError::MalformedCommit("missing committer".into()))?,
            encoding,
            message,
        })
    }

    fn rewrite(&self, tree: &str, parent: &str) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(format!("tree {tree}\nparent {parent}\nauthor ").as_bytes());
        output.extend_from_slice(self.author);
        output.extend_from_slice(b"\ncommitter ");
        output.extend_from_slice(self.committer);
        if let Some(encoding) = self.encoding {
            output.extend_from_slice(b"\nencoding ");
            output.extend_from_slice(encoding);
        }
        output.extend_from_slice(b"\n\n");
        output.extend_from_slice(self.message);
        output
    }
}

type RawHeader<'a> = (&'a [u8], &'a [u8]);

fn continuation_lines(headers: &[u8]) -> Result<Vec<RawHeader<'_>>, GitError> {
    let mut result = Vec::new();
    let mut start = 0;
    while start < headers.len() {
        let end = headers[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|offset| start + offset)
            .unwrap_or(headers.len());
        let line = &headers[start..end];
        if line.first() == Some(&b' ') {
            if result.is_empty() {
                return Err(GitError::MalformedCommit("orphan continuation line".into()));
            }
        } else {
            let space = line
                .iter()
                .position(|byte| *byte == b' ')
                .ok_or_else(|| GitError::MalformedCommit("header without value".into()))?;
            result.push((&line[..space], &line[space + 1..]));
        }
        start = end.saturating_add(1);
    }
    Ok(result)
}

async fn resolve_git() -> Result<PathBuf, GitError> {
    let output = Command::new("/usr/bin/which").arg("git").output().await?;
    if !output.status.success() {
        return Err(GitError::InvalidOutput("Git executable not found".into()));
    }
    Ok(PathBuf::from(text(output.stdout)?.trim()))
}

async fn hash_object(
    executable: &Path,
    repository: &Path,
    bytes: &[u8],
) -> Result<Vec<u8>, GitError> {
    let mut child = internal_command(executable, repository)
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    use tokio::io::AsyncWriteExt;
    child.stdin.take().unwrap().write_all(bytes).await?;
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(GitError::Command {
            command: "git hash-object".into(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(output.stdout)
}

fn internal_command(executable: &Path, repository: &Path) -> Command {
    let mut command = Command::new(executable);
    command.current_dir(repository).args([
        "-c",
        &format!("core.hooksPath={EMPTY_HOOKS_DIRECTORY}"),
        "-c",
        "commit.gpgSign=false",
        "-c",
        "rerere.enabled=false",
    ]);
    command
}

async fn run_internal<I, S>(
    executable: &Path,
    repository: &Path,
    args: I,
) -> Result<Vec<u8>, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = internal_command(executable, repository);
    command.args(args);
    run_command(command).await
}

async fn run_raw<I, S>(executable: &Path, directory: &Path, args: I) -> Result<Vec<u8>, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(executable);
    command.current_dir(directory).args(args);
    run_command(command).await
}

async fn run_command(mut command: Command) -> Result<Vec<u8>, GitError> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let display = format!("{command:?}");
    let output = command.output().await?;
    if !output.status.success() {
        return Err(GitError::Command {
            command: display,
            stderr: String::from_utf8_lossy(&output.stderr).trim().into(),
        });
    }
    Ok(output.stdout)
}

fn text(bytes: Vec<u8>) -> Result<String, GitError> {
    String::from_utf8(bytes).map_err(|error| GitError::InvalidOutput(error.to_string()))
}

fn nul_delimited_strings(bytes: Vec<u8>) -> Vec<String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .map(|field| String::from_utf8_lossy(field).into_owned())
        .collect()
}

fn canonical_git_path(bytes: Vec<u8>) -> Result<PathBuf, GitError> {
    let path = PathBuf::from(text(bytes)?.trim());
    std::fs::canonicalize(&path).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;

    #[test]
    fn raw_commit_rewrite_strips_signature_and_preserves_message_bytes() {
        let raw = b"tree 111\nparent 222\nauthor A <a@b> 1 +0000\ncommitter C <c@d> 2 +0000\ngpgsig sig\n continuation\n\nmessage\xff\n";
        let parsed = RawCommit::parse(raw).unwrap();
        let rewritten = parsed.rewrite("aaa", "bbb");
        assert!(!rewritten.windows(6).any(|value| value == b"gpgsig"));
        assert!(rewritten.ends_with(b"message\xff\n"));
    }

    fn git(directory: &Path, args: &[&str]) -> String {
        let output = StdCommand::new("git")
            .current_dir(directory)
            .args(args)
            .env("GIT_AUTHOR_NAME", "Tollgate Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Tollgate Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().into()
    }

    #[tokio::test]
    async fn initializes_release_without_moving_the_checked_out_master_branch() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        let master = adapter.resolve_oid(USER_BRANCH_REF).await.unwrap();
        let release = adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();

        assert_eq!(release, master);
        assert_eq!(adapter.integration_oid().await.unwrap(), master);
        assert_eq!(
            adapter.current_branch().await.unwrap().as_deref(),
            Some(USER_BRANCH)
        );
        adapter.ensure_integration_not_checked_out().await.unwrap();
    }

    #[tokio::test]
    async fn lists_unmerged_master_commits_oldest_first() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        std::fs::write(repository.join("one.txt"), "one\n").unwrap();
        git(&repository, &["add", "one.txt"]);
        git(&repository, &["commit", "-m", "one"]);
        let one = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();
        std::fs::write(repository.join("two.txt"), "two\n").unwrap();
        git(&repository, &["add", "two.txt"]);
        git(&repository, &["commit", "-m", "two"]);
        let two = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();

        assert_eq!(
            adapter.unmerged_user_master_commits().await.unwrap(),
            vec![one, two]
        );
    }

    #[tokio::test]
    async fn rejects_a_master_range_containing_a_merge_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        git(&repository, &["checkout", "-b", "side"]);
        std::fs::write(repository.join("side.txt"), "side\n").unwrap();
        git(&repository, &["add", "side.txt"]);
        git(&repository, &["commit", "-m", "side"]);
        git(&repository, &["checkout", USER_BRANCH]);
        std::fs::write(repository.join("master.txt"), "master\n").unwrap();
        git(&repository, &["add", "master.txt"]);
        git(&repository, &["commit", "-m", "master"]);
        git(
            &repository,
            &["merge", "--no-ff", "side", "-m", "merge side"],
        );

        let error = adapter.unmerged_user_master_commits().await.unwrap_err();
        assert!(matches!(error, GitError::InvalidSourceShape));
    }

    #[tokio::test]
    async fn rejects_master_that_diverged_from_release() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        git(&repository, &["checkout", "--detach", INTEGRATION_BRANCH]);
        std::fs::write(repository.join("release.txt"), "release\n").unwrap();
        git(&repository, &["add", "release.txt"]);
        git(&repository, &["commit", "-m", "move release"]);
        git(&repository, &["branch", "-f", INTEGRATION_BRANCH, "HEAD"]);
        git(&repository, &["checkout", USER_BRANCH]);
        std::fs::write(repository.join("master.txt"), "master\n").unwrap();
        git(&repository, &["add", "master.txt"]);
        git(&repository, &["commit", "-m", "move master"]);

        let error = adapter.unmerged_user_master_commits().await.unwrap_err();
        assert!(error.to_string().contains("is not a descendant"));
    }

    #[tokio::test]
    async fn rebases_diverged_master_commits_onto_release() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        std::fs::write(repository.join("master.txt"), "master\n").unwrap();
        git(&repository, &["add", "master.txt"]);
        git(&repository, &["commit", "-m", "master"]);
        let old_master = adapter.resolve_oid(USER_BRANCH_REF).await.unwrap();
        let base = adapter.commit_parent_oid(&old_master).await.unwrap();
        git(&repository, &["checkout", "--detach", &base.to_hex()]);
        std::fs::write(repository.join("release.txt"), "release\n").unwrap();
        git(&repository, &["add", "release.txt"]);
        git(&repository, &["commit", "-m", "release"]);
        git(&repository, &["branch", "-f", INTEGRATION_BRANCH, "HEAD"]);
        let release = adapter.integration_oid().await.unwrap();
        git(&repository, &["checkout", USER_BRANCH]);

        let new_master = adapter
            .rebase_user_master_onto_release(&old_master)
            .await
            .unwrap();

        assert_ne!(new_master, old_master);
        assert_eq!(
            adapter.commit_parent_oid(&new_master).await.unwrap(),
            release
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("master.txt")).unwrap(),
            "master\n"
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("release.txt")).unwrap(),
            "release\n"
        );
    }

    #[tokio::test]
    async fn projects_an_unchanged_master_source_to_its_tested_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("base.txt"), "base\n").unwrap();
        git(&repository, &["add", "base.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = git(&repository, &["rev-parse", "HEAD"]);
        std::fs::write(repository.join("source.txt"), "source\n").unwrap();
        git(&repository, &["add", "source.txt"]);
        git(&repository, &["commit", "-m", "source"]);
        let source = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();

        let tested_worktree = temporary.path().join("tested");
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "tested",
                tested_worktree.to_str().unwrap(),
                &base,
            ],
        );
        std::fs::write(tested_worktree.join("release.txt"), "release\n").unwrap();
        std::fs::write(tested_worktree.join("source.txt"), "source\n").unwrap();
        git(&tested_worktree, &["add", "release.txt", "source.txt"]);
        git(&tested_worktree, &["commit", "-m", "tested"]);
        let tested = GitOid::from_hex(&git(&tested_worktree, &["rev-parse", "HEAD"])).unwrap();

        let adapter = GitRepository::discover(&tested_worktree).await.unwrap();
        let outcome = adapter
            .sync_user_master(&tested, Some(&source), None)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            UserMasterSyncOutcome::UpdatedCheckout {
                path: std::fs::canonicalize(&repository).unwrap()
            }
        );
        assert_eq!(adapter.resolve_oid(USER_BRANCH_REF).await.unwrap(), tested);
        assert_eq!(
            std::fs::read_to_string(repository.join("release.txt")).unwrap(),
            "release\n"
        );
    }

    #[tokio::test]
    async fn synchronizes_checked_out_master_and_its_files_by_fast_forward() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);

        let feature = temporary.path().join("feature");
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
            ],
        );
        std::fs::write(feature.join("file.txt"), "promoted\n").unwrap();
        git(&feature, &["commit", "-am", "promoted"]);
        let promoted = GitOid::from_hex(&git(&feature, &["rev-parse", "HEAD"])).unwrap();

        let adapter = GitRepository::discover(&feature).await.unwrap();
        let base = adapter.resolve_oid(USER_BRANCH_REF).await.unwrap();
        git(
            &feature,
            &["update-ref", "refs/remotes/origin/master", &base.to_hex()],
        );
        let outcome = adapter
            .sync_user_master(&promoted, None, Some(("origin", "master", &promoted)))
            .await
            .unwrap();

        assert_eq!(
            outcome,
            UserMasterSyncOutcome::UpdatedCheckout {
                path: std::fs::canonicalize(&repository).unwrap()
            }
        );
        assert_eq!(
            std::fs::read_to_string(repository.join("file.txt")).unwrap(),
            "promoted\n"
        );
        assert_eq!(
            adapter.resolve_oid(USER_BRANCH_REF).await.unwrap(),
            promoted
        );
        assert_eq!(
            adapter
                .resolve_oid("refs/remotes/origin/master")
                .await
                .unwrap(),
            promoted
        );
    }

    #[tokio::test]
    async fn leaves_a_dirty_checked_out_master_untouched() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let master = git(&repository, &["rev-parse", "HEAD"]);
        let master_oid = GitOid::from_hex(&master).unwrap();

        let feature = temporary.path().join("feature");
        git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature.to_str().unwrap(),
            ],
        );
        std::fs::write(feature.join("file.txt"), "promoted\n").unwrap();
        git(&feature, &["commit", "-am", "promoted"]);
        let promoted = GitOid::from_hex(&git(&feature, &["rev-parse", "HEAD"])).unwrap();
        std::fs::write(repository.join("file.txt"), "local edit\n").unwrap();

        let adapter = GitRepository::discover(&feature).await.unwrap();
        let outcome = adapter
            .sync_user_master(&promoted, Some(&master_oid), None)
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            UserMasterSyncOutcome::NeedsAttention { reason, .. }
                if reason.contains("dirty")
        ));
        assert_eq!(git(&repository, &["rev-parse", USER_BRANCH_REF]), master);
        assert_eq!(
            std::fs::read_to_string(repository.join("file.txt")).unwrap(),
            "local edit\n"
        );
    }

    #[tokio::test]
    async fn synchronizes_an_unchecked_out_master_with_an_exact_ref_cas() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(&repository, &["checkout", "--detach"]);
        std::fs::write(repository.join("file.txt"), "promoted\n").unwrap();
        git(&repository, &["commit", "-am", "promoted"]);
        let promoted = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();

        let adapter = GitRepository::discover(&repository).await.unwrap();
        let outcome = adapter
            .sync_user_master(&promoted, None, None)
            .await
            .unwrap();

        assert_eq!(outcome, UserMasterSyncOutcome::UpdatedRef);
        assert_eq!(
            adapter.resolve_oid(USER_BRANCH_REF).await.unwrap(),
            promoted
        );
        assert_eq!(git(&repository, &["rev-parse", "HEAD"]), promoted.to_hex());
    }

    #[tokio::test]
    async fn refuses_to_overwrite_an_existing_divergent_release_branch() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        git(&repository, &["branch", INTEGRATION_BRANCH]);
        std::fs::write(repository.join("file.txt"), "master moved\n").unwrap();
        git(&repository, &["commit", "-am", "move master"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        let error = adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap_err();

        assert!(error.to_string().contains("will not overwrite"));
        assert_ne!(
            adapter.integration_oid().await.unwrap(),
            adapter.resolve_oid(USER_BRANCH_REF).await.unwrap()
        );
    }

    #[tokio::test]
    async fn constructs_a_shared_synthetic_prefix_with_preserved_parent_chain() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = git(&repository, &["rev-parse", "HEAD"]);
        git(&repository, &["checkout", "-b", "feature-a"]);
        std::fs::write(repository.join("a.txt"), "a\n").unwrap();
        git(&repository, &["add", "a.txt"]);
        git(&repository, &["commit", "-m", "add a"]);
        let source_a = git(&repository, &["rev-parse", "HEAD"]);
        git(&repository, &["checkout", "-b", "feature-b"]);
        std::fs::write(repository.join("b.txt"), "b\n").unwrap();
        git(&repository, &["add", "b.txt"]);
        git(&repository, &["commit", "-m", "add b"]);
        let source_b = git(&repository, &["rev-parse", "HEAD"]);
        git(&repository, &["checkout", "--detach", "HEAD"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        let base_oid = GitOid::from_hex(&base).unwrap();
        let a_oid = GitOid::from_hex(&source_a).unwrap();
        let b_oid = GitOid::from_hex(&source_b).unwrap();
        adapter
            .create_source_ref(QueueItemId::new(), &a_oid)
            .await
            .unwrap();
        adapter
            .create_source_ref(QueueItemId::new(), &b_oid)
            .await
            .unwrap();
        let mirror = temporary.path().join("mirror.git");
        adapter.initialize_mirror(&mirror).await.unwrap();
        let chain = adapter
            .construct_prefix(
                &mirror,
                &temporary.path().join("builder"),
                &base_oid,
                &[a_oid.clone(), b_oid.clone()],
            )
            .await
            .unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].parent_oid, base_oid);
        assert_eq!(chain[1].parent_oid, chain[0].oid);
        assert_eq!(chain[0].oid, a_oid);
        assert_eq!(chain[1].oid, b_oid);
        assert_ne!(chain[0].oid, chain[1].oid);
    }

    #[tokio::test]
    async fn reports_conflicting_paths_and_source_base_when_prefix_synthesis_fails() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("messages.json"), "base\n").unwrap();
        git(&repository, &["add", "messages.json"]);
        git(&repository, &["commit", "-m", "base"]);
        let source_base = git(&repository, &["rev-parse", "HEAD"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        git(&repository, &["checkout", "-b", "feature"]);
        std::fs::write(repository.join("messages.json"), "feature\n").unwrap();
        git(&repository, &["commit", "-am", "update generated messages"]);
        let source = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();
        adapter
            .create_source_ref(QueueItemId::new(), &source)
            .await
            .unwrap();

        git(&repository, &["checkout", USER_BRANCH]);
        std::fs::write(repository.join("messages.json"), "release\n").unwrap();
        git(&repository, &["commit", "-am", "advance release"]);
        git(&repository, &["branch", "-f", INTEGRATION_BRANCH, "HEAD"]);
        let prefix = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();
        let mirror = temporary.path().join("mirror.git");
        adapter.initialize_mirror(&mirror).await.unwrap();

        let error = adapter
            .construct_prefix(
                &mirror,
                &temporary.path().join("builder"),
                &prefix,
                std::slice::from_ref(&source),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            &error,
            GitError::SyntheticConflict {
                source_oid,
                source_parent_oid,
                prefix_oid,
                conflicting_paths,
            } if source_oid == &source
                && source_parent_oid.to_hex() == source_base
                && prefix_oid == &prefix
                && conflicting_paths == &["messages.json"]
        ));
        let message = error.to_string();
        assert!(message.contains("messages.json"));
        assert!(message.contains(&source_base));
        assert!(message.contains(&prefix.to_hex()));
        assert!(message.contains("rebase onto the latest release"));
    }

    #[tokio::test]
    async fn distinguishes_an_empty_synthetic_patch_from_a_conflict() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", USER_BRANCH]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();
        git(
            &repository,
            &["commit", "--allow-empty", "-m", "empty source"],
        );
        let source = GitOid::from_hex(&git(&repository, &["rev-parse", "HEAD"])).unwrap();

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        adapter
            .create_source_ref(QueueItemId::new(), &source)
            .await
            .unwrap();
        let mirror = temporary.path().join("mirror.git");
        adapter.initialize_mirror(&mirror).await.unwrap();

        let error = adapter
            .construct_prefix(
                &mirror,
                &temporary.path().join("builder"),
                &source,
                std::slice::from_ref(&source),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            GitError::SyntheticEmpty {
                source_oid,
                source_parent_oid,
                prefix_oid,
            } if source_oid == source && source_parent_oid == base && prefix_oid == source
        ));
    }

    #[tokio::test]
    async fn tested_object_ref_never_overwrites_contradictory_owned_evidence() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        std::fs::write(repository.join("file.txt"), "base\n").unwrap();
        git(&repository, &["add", "file.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let base = git(&repository, &["rev-parse", "HEAD"]);
        std::fs::write(repository.join("file.txt"), "next\n").unwrap();
        git(&repository, &["commit", "-am", "next"]);
        let next = git(&repository, &["rev-parse", "HEAD"]);
        git(&repository, &["checkout", "--detach", "HEAD"]);

        let adapter = GitRepository::discover(&repository).await.unwrap();
        adapter
            .initialize_integration_ref_from_master()
            .await
            .unwrap();
        let mirror = temporary.path().join("mirror.git");
        adapter.initialize_mirror(&mirror).await.unwrap();
        git(
            &repository,
            &["update-ref", "refs/tollgate/tested/buildset", &base],
        );
        let error = adapter
            .retain_tested_object(&mirror, "buildset", &GitOid::from_hex(&next).unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("different object"));
        assert_eq!(
            git(&repository, &["rev-parse", "refs/tollgate/tested/buildset"]),
            base
        );
    }
}
