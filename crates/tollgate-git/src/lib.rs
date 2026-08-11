#![forbid(unsafe_code)]

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Stdio,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{fs, process::Command};
use tollgate_domain::{GitOid, ObjectFormat, QueueItemId};

const EMPTY_HOOKS_DIRECTORY: &str = "/dev/null";

#[derive(Debug, Error)]
pub enum GitError {
    #[error("Git command failed ({command}): {stderr}")]
    Command { command: String, stderr: String },
    #[error("Git command produced invalid output: {0}")]
    InvalidOutput(String),
    #[error("repository is dirty: {0}")]
    DirtyWorktree(String),
    #[error("master is checked out in {0}")]
    MasterCheckedOut(String),
    #[error("unsupported Git object format `{0}`")]
    UnsupportedObjectFormat(String),
    #[error("source commit must have exactly one parent")]
    InvalidSourceShape,
    #[error("synthetic commit is empty or conflicts with its prefix")]
    Unmergeable,
    #[error("malformed raw commit: {0}")]
    MalformedCommit(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("object id error: {0}")]
    Oid(#[from] tollgate_domain::DomainError),
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

    pub async fn master_oid(&self) -> Result<GitOid, GitError> {
        self.resolve_oid("refs/heads/master").await
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

    pub async fn ensure_master_not_checked_out(&self) -> Result<(), GitError> {
        let bytes = self.git(["worktree", "list", "--porcelain", "-z"]).await?;
        let fields = bytes.split(|byte| *byte == 0).collect::<Vec<_>>();
        let mut current_path = None;
        for field in fields {
            if let Some(path) = field.strip_prefix(b"worktree ") {
                current_path = Some(String::from_utf8_lossy(path).into_owned());
            } else if field == b"branch refs/heads/master" {
                return Err(GitError::MasterCheckedOut(current_path.unwrap_or_default()));
            }
        }
        Ok(())
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
        self.ensure_master_not_checked_out().await?;
        let source_oid = self.resolve_oid(revision).await?;
        let master = self.master_oid().await?;
        if reject_integrated && self.is_ancestor(&source_oid, &master).await? {
            return Err(GitError::InvalidOutput(
                "source is already an ancestor of master".into(),
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
        if worktree == self.worktree_root || branch == "master" {
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

    pub async fn detach_current_master_if_clean(&self) -> Result<bool, GitError> {
        if self.current_branch().await?.as_deref() != Some("master") {
            return Ok(false);
        }
        self.ensure_clean().await?;
        let master = self.master_oid().await?;
        self.git(["switch", "--detach", &master.to_hex()]).await?;
        if self.resolve_oid("HEAD").await? != master || self.current_branch().await?.is_some() {
            return Err(GitError::InvalidOutput(
                "clean master worktree did not detach at the exact same OID".into(),
            ));
        }
        Ok(true)
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
        if !valid.success() || branch == "master" {
            return Err(GitError::InvalidOutput(
                "feature branch name is invalid or reserved".into(),
            ));
        }
        let master = self.master_oid().await?;
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
                "created worktree identity did not match the gated master".into(),
            ));
        }
        Ok(master)
    }

    pub async fn update_one_commit_feature(&self) -> Result<(GitOid, GitOid), GitError> {
        self.ensure_clean().await?;
        let branch = self.current_branch().await?.ok_or_else(|| {
            GitError::InvalidOutput("feature update requires a checked-out branch".into())
        })?;
        if branch == "master" {
            return Err(GitError::InvalidOutput(
                "feature update cannot operate on master".into(),
            ));
        }
        let old = self.resolve_oid("HEAD").await?;
        let old_parent = self.commit_parent_oid(&old).await?;
        let master = self.master_oid().await?;
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
                "updated feature does not have current master as its exact parent".into(),
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
                "+refs/heads/master:refs/tollgate/master",
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
                let _ = internal_command(&self.profile.executable, builder)
                    .args(["cherry-pick", "--abort"])
                    .status()
                    .await;
                return Err(GitError::Unmergeable);
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
                return Err(GitError::Unmergeable);
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
        let destination = format!("refs/tollgate/tested/{buildset_ref}");
        if let Ok(existing) = self.resolve_oid(&destination).await {
            return if existing == *oid {
                Ok(())
            } else {
                Err(GitError::InvalidOutput(format!(
                    "tested-object ref {destination} already records a different object"
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
            .git(["update-ref", &destination, &oid.to_hex(), &zero])
            .await;
        let _ = self.git(["update-ref", "-d", &incoming]).await;
        if let Err(error) = update {
            if self.resolve_oid(&destination).await.ok().as_ref() == Some(oid) {
                return Ok(());
            }
            return Err(error);
        }
        if self.resolve_oid(&destination).await? != *oid {
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

    pub async fn compare_and_swap_master(
        &self,
        expected_old: &GitOid,
        tested_new: &GitOid,
    ) -> Result<(), GitError> {
        self.ensure_master_not_checked_out().await?;
        self.git([
            "update-ref",
            "refs/heads/master",
            &tested_new.to_hex(),
            &expected_old.to_hex(),
        ])
        .await?;
        if self.master_oid().await? != *tested_new {
            return Err(GitError::InvalidOutput("master CAS result mismatch".into()));
        }
        Ok(())
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
