#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::{Component, Path},
};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_TIMEOUT_NS: u64 = 7 * 24 * 60 * 60 * 1_000_000_000;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("configuration could not be parsed: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("unsupported configuration version {0}; expected version 1")]
    UnsupportedVersion(u16),
    #[error("invalid step `{step}`: {message}")]
    InvalidStep { step: String, message: String },
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error("matcher pattern `{pattern}` is invalid: {message}")]
    InvalidMatcher { pattern: String, message: String },
    #[error("canonical configuration serialization failed: {0}")]
    Canonicalization(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    pub version: u16,
    #[serde(default)]
    pub sync_user_master: SyncUserMaster,
    #[serde(default)]
    pub runner: Option<Vec<String>>,
    #[serde(default)]
    pub allow_no_job: bool,
    #[serde(default)]
    pub allow_concurrent_roots: bool,
    #[serde(default)]
    pub step: Vec<StepFile>,
    #[serde(default)]
    pub resources: ResourceFile,
    #[serde(default)]
    pub remote: RemoteFile,
    #[serde(default)]
    pub cache: CacheFile,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceFile {
    #[serde(default = "default_max_buildsets")]
    pub max_buildsets: u16,
    #[serde(default)]
    pub cpu_tokens: u16,
    #[serde(default)]
    pub memory_bytes: u64,
    #[serde(default = "default_repository_concurrency")]
    pub repository_concurrency: u16,
    #[serde(default = "default_scheduler_weight")]
    pub scheduler_weight: u16,
    #[serde(default = "default_volume_warning_bytes")]
    pub volume_warning_bytes: u64,
    #[serde(default = "default_volume_critical_bytes")]
    pub volume_critical_bytes: u64,
    #[serde(default = "default_volume_emergency_bytes")]
    pub volume_emergency_bytes: u64,
    #[serde(default = "default_release_concurrency")]
    pub release_concurrency: u16,
    #[serde(default)]
    pub max_release_lag: u32,
}

impl Default for ResourceFile {
    fn default() -> Self {
        Self {
            max_buildsets: default_max_buildsets(),
            cpu_tokens: 0,
            memory_bytes: 0,
            repository_concurrency: default_repository_concurrency(),
            scheduler_weight: default_scheduler_weight(),
            volume_warning_bytes: default_volume_warning_bytes(),
            volume_critical_bytes: default_volume_critical_bytes(),
            volume_emergency_bytes: default_volume_emergency_bytes(),
            release_concurrency: default_release_concurrency(),
            max_release_lag: 0,
        }
    }
}

/// Which Tollgate-owned ref user-owned local `master` follows after promotion.
///
/// The configuration file accepts `true` (an alias for `"staging"`), `false`,
/// `"staging"`, or `"release"`. Canonical bytes omit the `"staging"` default
/// and encode `false` as a boolean, so configurations written before staged
/// release keep their digests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyncUserMaster {
    #[default]
    Staging,
    Release,
    Disabled,
}

impl SyncUserMaster {
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    const fn is_staging(&self) -> bool {
        matches!(self, Self::Staging)
    }
}

impl Serialize for SyncUserMaster {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Staging => serializer.serialize_str("staging"),
            Self::Release => serializer.serialize_str("release"),
            Self::Disabled => serializer.serialize_bool(false),
        }
    }
}

impl<'de> Deserialize<'de> for SyncUserMaster {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = SyncUserMaster;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("true, false, \"staging\", or \"release\"")
            }

            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(if value {
                    SyncUserMaster::Staging
                } else {
                    SyncUserMaster::Disabled
                })
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                match value {
                    "staging" => Ok(SyncUserMaster::Staging),
                    "release" => Ok(SyncUserMaster::Release),
                    _ => Err(E::invalid_value(serde::de::Unexpected::Str(value), &self)),
                }
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

/// The validation stage a step belongs to. Gate-stage steps certify a
/// promotion; release-stage steps certify a release advance.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum StepStage {
    #[default]
    Gate,
    Release,
}

impl StepStage {
    pub const fn is_gate(&self) -> bool {
        matches!(self, Self::Gate)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gate => "gate",
            Self::Release => "release",
        }
    }
}

const fn default_max_buildsets() -> u16 {
    4
}
const fn default_repository_concurrency() -> u16 {
    2
}
const fn default_scheduler_weight() -> u16 {
    1
}
const fn default_volume_warning_bytes() -> u64 {
    15 * 1024 * 1024 * 1024
}
const fn default_volume_critical_bytes() -> u64 {
    10 * 1024 * 1024 * 1024
}
const fn default_volume_emergency_bytes() -> u64 {
    512 * 1024 * 1024
}
const fn default_release_concurrency() -> u16 {
    1
}

fn is_default_release_concurrency(value: &u16) -> bool {
    *value == default_release_concurrency()
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteFile {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_remote_name")]
    pub name: String,
    #[serde(default = "default_remote_branch")]
    pub branch: String,
}

impl Default for RemoteFile {
    fn default() -> Self {
        Self {
            enabled: false,
            name: default_remote_name(),
            branch: default_remote_branch(),
        }
    }
}

fn default_remote_name() -> String {
    "origin".into()
}
fn default_remote_branch() -> String {
    "master".into()
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFile {
    #[serde(default)]
    pub epoch: u64,
    #[serde(default)]
    pub paths: Vec<CachePathFile>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CachePathFile {
    pub path: String,
    #[serde(default)]
    pub policy: CachePolicy,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CachePolicy {
    Preserve,
    #[default]
    Clone,
    Shared,
    Discard,
    Sensitive,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepFile {
    pub name: String,
    pub run: Option<String>,
    pub argv: Option<Vec<String>>,
    #[serde(default)]
    pub stage: StepStage,
    #[serde(default = "root_directory")]
    pub working_directory: String,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(default)]
    pub soft_needs: Vec<String>,
    #[serde(default = "default_true")]
    pub voting: bool,
    #[serde(default, rename = "final")]
    pub final_step: bool,
    #[serde(default)]
    pub reuse_on_retry: bool,
    #[serde(default = "default_timeout")]
    pub timeout: String,
    #[serde(default)]
    pub cpu_tokens: u16,
    #[serde(default)]
    pub memory_bytes: u64,
    pub rss_limit_bytes: Option<u64>,
    #[serde(default)]
    pub semaphores: Vec<String>,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub include_mode: MatchMode,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub exclude_mode: MatchMode,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub remove_environment: Vec<String>,
    #[serde(default)]
    pub artifact: Vec<ArtifactFile>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    #[default]
    Any,
    All,
}

fn root_directory() -> String {
    ".".into()
}
fn default_timeout() -> String {
    "60m".into()
}
const fn default_true() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactFile {
    pub name: String,
    pub patterns: Vec<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default = "default_retention_days")]
    pub retention_days: u16,
}

const fn default_retention_days() -> u16 {
    30
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveConfig {
    pub version: u16,
    #[serde(default)]
    pub sync_user_master: SyncUserMaster,
    pub runner: Vec<String>,
    pub allow_no_job: bool,
    pub allow_concurrent_roots: bool,
    pub steps: Vec<EffectiveStep>,
    pub resources: EffectiveResources,
    pub remote: EffectiveRemote,
    pub cache: EffectiveCache,
    pub digest: String,
    /// Digest of the gate-stage step graph. Gate validation generations freeze
    /// it, so release-stage steps never contribute to it.
    pub step_graph_digest: String,
    /// Digest of the steps a release run executes: every release-stage step
    /// plus the gate-stage steps they transitively need. Absent when the
    /// configuration has no release-stage steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_step_graph_digest: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveResources {
    pub max_buildsets: u16,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub repository_concurrency: u16,
    pub scheduler_weight: u16,
    pub volume_warning_bytes: u64,
    pub volume_critical_bytes: u64,
    pub volume_emergency_bytes: u64,
    #[serde(
        default = "default_release_concurrency",
        skip_serializing_if = "is_default_release_concurrency"
    )]
    pub release_concurrency: u16,
    /// Unreleased staging commits tolerated after a failed release run before
    /// promotion pauses. Zero means unlimited.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub max_release_lag: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveRemote {
    pub enabled: bool,
    pub name: String,
    pub branch: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveCache {
    pub epoch: u64,
    pub paths: Vec<EffectiveCachePath>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveCachePath {
    pub path: String,
    pub policy: CachePolicy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveStep {
    pub name: String,
    pub command: EffectiveCommand,
    #[serde(default, skip_serializing_if = "StepStage::is_gate")]
    pub stage: StepStage,
    pub working_directory: String,
    pub needs: Vec<String>,
    pub soft_needs: Vec<String>,
    pub voting: bool,
    pub final_step: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub reuse_on_retry: bool,
    pub timeout_ns: u64,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub rss_limit_bytes: Option<u64>,
    pub semaphores: Vec<String>,
    pub include: Vec<String>,
    #[serde(default, skip_serializing_if = "match_mode_is_any")]
    pub include_mode: MatchMode,
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "match_mode_is_any")]
    pub exclude_mode: MatchMode,
    pub environment: BTreeMap<String, String>,
    pub remove_environment: Vec<String>,
    pub artifacts: Vec<EffectiveArtifact>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EffectiveCommand {
    Shell { script: String },
    Argv { argv: Vec<String> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveArtifact {
    pub name: String,
    pub patterns: Vec<String>,
    pub required: bool,
    pub retention_days: u16,
}

impl EffectiveConfig {
    pub fn parse(input: &str) -> Result<Self, ConfigError> {
        let raw: ConfigFile = toml::from_str(input)?;
        Self::from_file(raw)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ConfigError> {
        #[derive(Serialize)]
        struct Canonical<'a> {
            version: u16,
            #[serde(skip_serializing_if = "SyncUserMaster::is_staging")]
            sync_user_master: SyncUserMaster,
            runner: &'a [String],
            allow_no_job: bool,
            allow_concurrent_roots: bool,
            steps: &'a [EffectiveStep],
            resources: &'a EffectiveResources,
            remote: &'a EffectiveRemote,
            cache: &'a EffectiveCache,
        }
        Ok(serde_json::to_vec(&Canonical {
            version: self.version,
            sync_user_master: self.sync_user_master,
            runner: &self.runner,
            allow_no_job: self.allow_no_job,
            allow_concurrent_roots: self.allow_concurrent_roots,
            steps: &self.steps,
            resources: &self.resources,
            remote: &self.remote,
            cache: &self.cache,
        })?)
    }

    pub fn restore_canonical(
        bytes: &[u8],
        digest: String,
        step_graph_digest: String,
    ) -> Result<Self, ConfigError> {
        #[derive(Deserialize)]
        struct Canonical {
            version: u16,
            #[serde(default)]
            sync_user_master: SyncUserMaster,
            runner: Vec<String>,
            allow_no_job: bool,
            allow_concurrent_roots: bool,
            steps: Vec<EffectiveStep>,
            resources: EffectiveResources,
            remote: EffectiveRemote,
            cache: EffectiveCache,
        }
        let value: Canonical = serde_json::from_slice(bytes)?;
        let release_step_graph_digest = release_graph_digest(&value.steps)?;
        Ok(Self {
            version: value.version,
            sync_user_master: value.sync_user_master,
            runner: value.runner,
            allow_no_job: value.allow_no_job,
            allow_concurrent_roots: value.allow_concurrent_roots,
            steps: value.steps,
            resources: value.resources,
            remote: value.remote,
            cache: value.cache,
            digest,
            step_graph_digest,
            release_step_graph_digest,
        })
    }

    /// Steps of one stage, in declaration order.
    pub fn stage_steps(&self, stage: StepStage) -> impl Iterator<Item = &EffectiveStep> {
        self.steps.iter().filter(move |step| step.stage == stage)
    }

    /// Steps a release run executes, in declaration order: every
    /// release-stage step plus the gate-stage steps they transitively need.
    /// Empty when the configuration has no release-stage steps.
    pub fn release_run_steps(&self) -> Vec<&EffectiveStep> {
        release_run_steps(&self.steps)
    }

    pub fn applicable_steps(
        &self,
        changed_paths: &[String],
    ) -> Result<Vec<&EffectiveStep>, ConfigError> {
        self.steps
            .iter()
            .filter_map(|step| match step.is_applicable(changed_paths) {
                Ok(true) => Some(Ok(step)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    /// Whether the configuration declares a release stage. Without one, the
    /// repository is opt-out: every promotion moves `staging` and `release`
    /// together and no release run is ever queued.
    pub fn has_release_stage(&self) -> bool {
        self.release_step_graph_digest.is_some()
    }

    /// Steps one run of `stage` executes, in declaration order. A gate run
    /// executes the gate-stage steps; a release run executes
    /// [`Self::release_run_steps`].
    pub fn run_steps(&self, stage: StepStage) -> Vec<&EffectiveStep> {
        match stage {
            StepStage::Gate => self.stage_steps(StepStage::Gate).collect(),
            StepStage::Release => self.release_run_steps(),
        }
    }

    /// Steps one run of `stage` executes for `changed_paths`, in declaration
    /// order. A gate run selects each gate-stage step by its own path filters.
    /// A release run starts from the release-stage steps its path filters
    /// select and adds only the run steps those transitively need (through
    /// `needs` or `soft_needs`) whose own path filters also select them, so a
    /// release run that selects no release-stage step selects nothing and
    /// never runs a prerequisite on its own.
    pub fn selected_run_steps(
        &self,
        stage: StepStage,
        changed_paths: &[String],
    ) -> Result<Vec<&EffectiveStep>, ConfigError> {
        let mut selected = Vec::new();
        for step in self.run_steps(stage) {
            if step.is_applicable(changed_paths)? {
                selected.push(step);
            }
        }
        Ok(match stage {
            StepStage::Gate => selected,
            StepStage::Release => dependency_closure(&selected, |step| !step.stage.is_gate()),
        })
    }

    /// Whether `other` applies the same gate policy: it differs from this
    /// configuration at most in release-only policy, meaning release-stage
    /// steps, `release_concurrency`, and `max_release_lag`. Such an edit
    /// never affects gate validation, so it leaves gate generations, their
    /// buildsets, and their certificates intact. Adding the first or removing
    /// the last release-stage step changes how a promotion moves the refs, so
    /// it always changes the gate policy.
    pub fn gate_policy_matches(&self, other: &Self) -> bool {
        let gate_policy = |config: &Self| {
            let mut policy = config.clone();
            policy.steps.retain(|step| step.stage.is_gate());
            policy.resources.release_concurrency = default_release_concurrency();
            policy.resources.max_release_lag = 0;
            policy.digest.clear();
            policy.release_step_graph_digest = None;
            policy
        };
        self.has_release_stage() == other.has_release_stage()
            && gate_policy(self) == gate_policy(other)
    }

    /// The release-stage step graph digest a release run freezes, or the
    /// gate-stage digest for a gate run.
    pub fn run_step_graph_digest(&self, stage: StepStage) -> Option<&str> {
        match stage {
            StepStage::Gate => Some(&self.step_graph_digest),
            StepStage::Release => self.release_step_graph_digest.as_deref(),
        }
    }

    /// Parse and validate every stage rule.
    fn from_file(raw: ConfigFile) -> Result<Self, ConfigError> {
        if raw.version != 1 {
            return Err(ConfigError::UnsupportedVersion(raw.version));
        }
        let runner = raw
            .runner
            .unwrap_or_else(|| vec!["/bin/sh".into(), "-c".into()]);
        if runner.is_empty()
            || runner
                .iter()
                .any(|value| value.is_empty() || value.contains('\0'))
        {
            return Err(ConfigError::Invalid(
                "runner must contain nonempty NUL-free arguments".into(),
            ));
        }
        if raw.step.is_empty() && !raw.allow_no_job {
            return Err(ConfigError::Invalid(
                "at least one step is required unless allow_no_job is true".into(),
            ));
        }
        let any_explicit_edges = raw
            .step
            .iter()
            .any(|step| !step.needs.is_empty() || !step.soft_needs.is_empty());
        if !any_explicit_edges
            && let Some(pair) = raw
                .step
                .windows(2)
                .find(|pair| !pair[0].stage.is_gate() && pair[1].stage.is_gate())
        {
            return Err(ConfigError::InvalidStep {
                step: pair[1].name.clone(),
                message: format!(
                    "the implicit declaration-order chain would make this gate-stage step \
                     depend on release-stage step `{}`; declare gate-stage steps first or \
                     declare explicit needs",
                    pair[0].name
                ),
            });
        }
        let mut steps: Vec<EffectiveStep> = Vec::with_capacity(raw.step.len());
        for (index, mut step) in raw.step.into_iter().enumerate() {
            let implicit_needs = if !any_explicit_edges && index > 0 {
                vec![steps[index - 1].name.clone()]
            } else {
                std::mem::take(&mut step.needs)
            };
            steps.push(normalize_step(step, implicit_needs)?);
        }
        validate_graph(&steps)?;
        if !raw.allow_no_job && !steps.iter().any(|step| step.voting && step.stage.is_gate()) {
            return Err(ConfigError::Invalid(
                "a gate configuration requires at least one voting step".into(),
            ));
        }

        let resources = EffectiveResources {
            max_buildsets: if raw.resources.max_buildsets == 0 {
                default_max_buildsets()
            } else {
                raw.resources.max_buildsets
            },
            cpu_tokens: raw.resources.cpu_tokens,
            memory_bytes: raw.resources.memory_bytes,
            repository_concurrency: if raw.resources.repository_concurrency == 0 {
                default_repository_concurrency()
            } else {
                raw.resources.repository_concurrency
            },
            scheduler_weight: if raw.resources.scheduler_weight == 0 {
                default_scheduler_weight()
            } else {
                raw.resources.scheduler_weight
            },
            volume_warning_bytes: if raw.resources.volume_warning_bytes == 0 {
                default_volume_warning_bytes()
            } else {
                raw.resources.volume_warning_bytes
            },
            volume_critical_bytes: if raw.resources.volume_critical_bytes == 0 {
                default_volume_critical_bytes()
            } else {
                raw.resources.volume_critical_bytes
            },
            volume_emergency_bytes: if raw.resources.volume_emergency_bytes == 0 {
                default_volume_emergency_bytes()
            } else {
                raw.resources.volume_emergency_bytes
            },
            release_concurrency: raw.resources.release_concurrency,
            max_release_lag: raw.resources.max_release_lag,
        };
        validate_resources(&steps, &resources)?;

        let cache = EffectiveCache {
            epoch: raw.cache.epoch,
            paths: raw
                .cache
                .paths
                .into_iter()
                .map(|entry| {
                    Ok(EffectiveCachePath {
                        path: normalize_relative(&entry.path)?,
                        policy: entry.policy,
                    })
                })
                .collect::<Result<_, ConfigError>>()?,
        };
        let remote = EffectiveRemote {
            enabled: raw.remote.enabled,
            name: raw.remote.name,
            branch: raw.remote.branch,
        };
        let step_graph_digest = graph_digest(steps.iter().filter(|step| step.stage.is_gate()))?;
        let release_step_graph_digest = release_graph_digest(&steps)?;
        let mut config = Self {
            version: 1,
            sync_user_master: raw.sync_user_master,
            runner,
            allow_no_job: raw.allow_no_job,
            allow_concurrent_roots: raw.allow_concurrent_roots,
            steps,
            resources,
            remote,
            cache,
            digest: String::new(),
            step_graph_digest,
            release_step_graph_digest,
        };
        config.digest = blake3::hash(&config.canonical_bytes()?)
            .to_hex()
            .to_string();
        Ok(config)
    }
}

impl EffectiveStep {
    pub fn is_applicable(&self, changed_paths: &[String]) -> Result<bool, ConfigError> {
        if self.include.is_empty() && self.exclude.is_empty() {
            return Ok(true);
        }
        let includes = build_matcher(&self.include)?;
        let excludes = build_matcher(&self.exclude)?;
        let selected = self.include.is_empty()
            || self
                .include_mode
                .matches(changed_paths, |path| includes.is_match(path));
        let excluded = !self.exclude.is_empty()
            && self
                .exclude_mode
                .matches(changed_paths, |path| excludes.is_match(path));
        Ok(selected && !excluded)
    }
}

impl MatchMode {
    fn matches(self, changed_paths: &[String], matches: impl Fn(&str) -> bool) -> bool {
        !changed_paths.is_empty()
            && match self {
                Self::Any => changed_paths.iter().any(|path| matches(path)),
                Self::All => changed_paths.iter().all(|path| matches(path)),
            }
    }
}

fn match_mode_is_any(mode: &MatchMode) -> bool {
    *mode == MatchMode::Any
}

fn normalize_step(step: StepFile, needs: Vec<String>) -> Result<EffectiveStep, ConfigError> {
    validate_name(&step.name).map_err(|message| ConfigError::InvalidStep {
        step: step.name.clone(),
        message,
    })?;
    let command = match (step.run, step.argv) {
        (Some(script), None) if !script.is_empty() && !script.contains('\0') => {
            EffectiveCommand::Shell { script }
        }
        (None, Some(argv))
            if !argv.is_empty()
                && !argv[0].is_empty()
                && argv.iter().all(|arg| !arg.contains('\0')) =>
        {
            EffectiveCommand::Argv { argv }
        }
        _ => {
            return Err(ConfigError::InvalidStep {
                step: step.name,
                message: "exactly one nonempty `run` or `argv` is required".into(),
            });
        }
    };
    let timeout_ns = parse_duration(&step.timeout).map_err(|message| ConfigError::InvalidStep {
        step: step.name.clone(),
        message,
    })?;
    if !(1_000_000_000..=MAX_TIMEOUT_NS).contains(&timeout_ns) {
        return Err(ConfigError::InvalidStep {
            step: step.name,
            message: "timeout must be between one second and seven days".into(),
        });
    }
    let overlap = needs.iter().find(|name| step.soft_needs.contains(name));
    if let Some(name) = overlap {
        return Err(ConfigError::InvalidStep {
            step: step.name,
            message: format!("dependency `{name}` occurs in both needs and soft_needs"),
        });
    }
    validate_environment(&step.name, &step.environment, &step.remove_environment)?;
    for pattern in step.include.iter().chain(&step.exclude) {
        validate_pattern(pattern)?;
    }
    let artifacts = step
        .artifact
        .into_iter()
        .map(|artifact| {
            validate_name(&artifact.name).map_err(|message| ConfigError::InvalidStep {
                step: step.name.clone(),
                message: format!("artifact `{}`: {message}", artifact.name),
            })?;
            if artifact.patterns.is_empty() {
                return Err(ConfigError::InvalidStep {
                    step: step.name.clone(),
                    message: format!("artifact `{}` needs at least one pattern", artifact.name),
                });
            }
            if artifact.retention_days == 0 {
                return Err(ConfigError::InvalidStep {
                    step: step.name.clone(),
                    message: format!(
                        "artifact `{}` retention_days must be at least one",
                        artifact.name
                    ),
                });
            }
            for pattern in &artifact.patterns {
                artifact_pattern(pattern, Some("configuration-validation"))?;
            }
            Ok(EffectiveArtifact {
                name: artifact.name,
                patterns: artifact.patterns,
                required: artifact.required,
                retention_days: artifact.retention_days,
            })
        })
        .collect::<Result<Vec<_>, ConfigError>>()?;
    let mut artifact_names = std::collections::HashSet::new();
    if let Some(duplicate) = artifacts
        .iter()
        .find(|artifact| !artifact_names.insert(artifact.name.clone()))
    {
        return Err(ConfigError::InvalidStep {
            step: step.name.clone(),
            message: format!(
                "artifact name `{}` is declared more than once",
                duplicate.name
            ),
        });
    }
    if step.reuse_on_retry && (step.final_step || !artifacts.is_empty()) {
        return Err(ConfigError::InvalidStep {
            step: step.name.clone(),
            message: "retry reuse requires an ordinary step with no retained artifacts".into(),
        });
    }
    Ok(EffectiveStep {
        name: step.name,
        command,
        stage: step.stage,
        working_directory: normalize_relative(&step.working_directory)?,
        needs,
        soft_needs: sorted_unique(step.soft_needs)?,
        voting: step.voting,
        final_step: step.final_step,
        reuse_on_retry: step.reuse_on_retry,
        timeout_ns,
        cpu_tokens: step.cpu_tokens,
        memory_bytes: step.memory_bytes,
        rss_limit_bytes: step.rss_limit_bytes,
        semaphores: sorted_names(step.semaphores)?,
        include: step.include,
        include_mode: step.include_mode,
        exclude: step.exclude,
        exclude_mode: step.exclude_mode,
        environment: step.environment,
        remove_environment: sorted_unique(step.remove_environment)?,
        artifacts,
    })
}

fn validate_graph(steps: &[EffectiveStep]) -> Result<(), ConfigError> {
    let by_name = steps
        .iter()
        .map(|step| (step.name.as_str(), step))
        .collect::<HashMap<_, _>>();
    let mut indegrees = steps
        .iter()
        .map(|step| (step.name.as_str(), 0usize))
        .collect::<HashMap<_, _>>();
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    for step in steps {
        for dependency in step.needs.iter().chain(&step.soft_needs) {
            let Some(prerequisite) = by_name.get(dependency.as_str()) else {
                return Err(ConfigError::InvalidStep {
                    step: step.name.clone(),
                    message: format!("unknown dependency `{dependency}`"),
                });
            };
            if dependency == &step.name {
                return Err(ConfigError::InvalidStep {
                    step: step.name.clone(),
                    message: "a step cannot depend on itself".into(),
                });
            }
            if step.stage.is_gate() && !prerequisite.stage.is_gate() {
                return Err(ConfigError::InvalidStep {
                    step: step.name.clone(),
                    message: format!(
                        "a gate-stage step cannot depend on release-stage step `{dependency}`"
                    ),
                });
            }
            if prerequisite.final_step && !step.final_step {
                return Err(ConfigError::InvalidStep {
                    step: step.name.clone(),
                    message: "a non-final step cannot depend on a final step".into(),
                });
            }
            *indegrees.get_mut(step.name.as_str()).unwrap() += 1;
            outgoing.entry(dependency).or_default().push(&step.name);
        }
    }
    let mut ready = indegrees
        .iter()
        .filter_map(|(name, count)| (*count == 0).then_some(*name))
        .collect::<VecDeque<_>>();
    let mut visited = 0;
    while let Some(name) = ready.pop_front() {
        visited += 1;
        for dependent in outgoing.get(name).into_iter().flatten() {
            let degree = indegrees.get_mut(dependent).unwrap();
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(dependent);
            }
        }
    }
    if visited != steps.len() {
        return Err(ConfigError::Invalid(
            "step dependency graph contains a cycle".into(),
        ));
    }
    Ok(())
}

fn validate_resources(
    steps: &[EffectiveStep],
    resources: &EffectiveResources,
) -> Result<(), ConfigError> {
    if !(1..=64).contains(&resources.max_buildsets) {
        return Err(ConfigError::Invalid(
            "max_buildsets must be between 1 and 64".into(),
        ));
    }
    if resources.repository_concurrency == 0
        || resources.repository_concurrency > resources.max_buildsets
    {
        return Err(ConfigError::Invalid(
            "repository_concurrency must be between 1 and max_buildsets".into(),
        ));
    }
    if resources.release_concurrency == 0 || resources.release_concurrency > resources.max_buildsets
    {
        return Err(ConfigError::Invalid(
            "release_concurrency must be between 1 and max_buildsets".into(),
        ));
    }
    if !(1..=100).contains(&resources.scheduler_weight) {
        return Err(ConfigError::Invalid(
            "scheduler_weight must be between 1 and 100".into(),
        ));
    }
    if resources.volume_warning_bytes <= resources.volume_critical_bytes
        || resources.volume_critical_bytes < 1024 * 1024 * 1024
        || resources.volume_emergency_bytes == 0
        || resources.volume_emergency_bytes > resources.volume_critical_bytes
    {
        return Err(ConfigError::Invalid(
            "volume thresholds require warning > critical >= 1 GiB and 0 < emergency <= critical"
                .into(),
        ));
    }
    for step in steps {
        if step
            .rss_limit_bytes
            .is_some_and(|limit| limit < 1024 * 1024)
        {
            return Err(ConfigError::InvalidStep {
                step: step.name.clone(),
                message: "rss_limit_bytes must be at least 1 MiB when configured".into(),
            });
        }
        if step.cpu_tokens > 0
            && (resources.cpu_tokens == 0 || step.cpu_tokens > resources.cpu_tokens)
        {
            return Err(ConfigError::InvalidStep {
                step: step.name.clone(),
                message: "CPU reservation requires and must fit the configured CPU pool".into(),
            });
        }
        if step.memory_bytes > 0
            && (resources.memory_bytes == 0 || step.memory_bytes > resources.memory_bytes)
        {
            return Err(ConfigError::InvalidStep {
                step: step.name.clone(),
                message: "memory reservation requires and must fit the configured memory pool"
                    .into(),
            });
        }
    }
    Ok(())
}

fn validate_environment(
    name: &str,
    additions: &BTreeMap<String, String>,
    removals: &[String],
) -> Result<(), ConfigError> {
    let mut seen = BTreeSet::new();
    for key in additions.keys().chain(removals) {
        if !valid_environment_name(key) {
            return Err(ConfigError::InvalidStep {
                step: name.into(),
                message: format!("invalid environment variable name `{key}`"),
            });
        }
        if additions.contains_key(key) && removals.contains(key) {
            return Err(ConfigError::InvalidStep {
                step: name.into(),
                message: format!("environment variable `{key}` is both added and removed"),
            });
        }
        if !seen.insert(key) && removals.iter().filter(|value| *value == key).count() > 1 {
            return Err(ConfigError::InvalidStep {
                step: name.into(),
                message: format!("environment variable `{key}` is removed more than once"),
            });
        }
    }
    if additions.values().any(|value| value.contains('\0')) {
        return Err(ConfigError::InvalidStep {
            step: name.into(),
            message: "environment values must be NUL-free".into(),
        });
    }
    Ok(())
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|ch| matches!(ch, '_' | 'A'..='Z' | 'a'..='z' | '0'..='9'))
}

fn validate_name(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    if name.len() > 64
        || !matches!(chars.next(), Some('A'..='Z' | 'a'..='z' | '0'..='9'))
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err("name must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}".into());
    }
    Ok(())
}

fn normalize_relative(value: &str) -> Result<String, ConfigError> {
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(ConfigError::Invalid(format!(
            "path `{value}` must be relative"
        )));
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => parts.push(value.to_string_lossy().into_owned()),
            _ => {
                return Err(ConfigError::Invalid(format!(
                    "path `{value}` contains parent or platform components"
                )));
            }
        }
    }
    Ok(if parts.is_empty() {
        ".".into()
    } else {
        parts.join("/")
    })
}

fn parse_duration(value: &str) -> Result<u64, String> {
    let split = value
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "duration needs a unit (s, m, h, or d)".to_string())?;
    let number = value[..split]
        .parse::<u64>()
        .map_err(|_| "duration value is invalid".to_string())?;
    let multiplier = match &value[split..] {
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 60 * 60 * 1_000_000_000,
        "d" => 24 * 60 * 60 * 1_000_000_000,
        _ => return Err("duration unit must be s, m, h, or d".into()),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "duration overflows".into())
}

/// Resolve the artifact-only buildset token from runner-owned execution context.
pub fn artifact_pattern(pattern: &str, buildset_id: Option<&str>) -> Result<String, ConfigError> {
    const TOKEN: &str = "{{buildset_id}}";
    let resolved = if pattern.contains(TOKEN) {
        let id = buildset_id.ok_or_else(|| ConfigError::InvalidMatcher {
            pattern: pattern.into(),
            message: "artifact pattern requires trusted buildset identity".into(),
        })?;
        assert!(
            !id.is_empty()
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        );
        pattern.replace(TOKEN, id)
    } else {
        pattern.to_owned()
    };
    validate_pattern(&resolved)?;
    Ok(resolved)
}

fn validate_pattern(pattern: &str) -> Result<(), ConfigError> {
    if pattern.starts_with('/')
        || pattern.split('/').any(|component| component == "..")
        || pattern.contains('\\')
    {
        return Err(ConfigError::InvalidMatcher {
            pattern: pattern.into(),
            message: "patterns must be repository-relative `/`-separated paths".into(),
        });
    }
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
        .map_err(|error| ConfigError::InvalidMatcher {
            pattern: pattern.into(),
            message: error.to_string(),
        })?;
    Ok(())
}

fn build_matcher(patterns: &[String]) -> Result<GlobSet, ConfigError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        validate_pattern(pattern)?;
        builder.add(
            GlobBuilder::new(pattern)
                .literal_separator(true)
                .backslash_escape(false)
                .build()
                .unwrap(),
        );
    }
    builder
        .build()
        .map_err(|error| ConfigError::Invalid(error.to_string()))
}

fn sorted_unique(values: Vec<String>) -> Result<Vec<String>, ConfigError> {
    let set = values.iter().collect::<BTreeSet<_>>();
    if set.len() != values.len() {
        return Err(ConfigError::Invalid(
            "set-like arrays may not contain duplicates".into(),
        ));
    }
    Ok(set.into_iter().cloned().collect())
}

fn sorted_names(values: Vec<String>) -> Result<Vec<String>, ConfigError> {
    for value in &values {
        validate_name(value).map_err(ConfigError::Invalid)?;
    }
    sorted_unique(values)
}

/// Hash a step graph as the JSON array of its steps. Gate-stage steps omit
/// `stage`, so a configuration without release-stage steps hashes exactly as
/// its whole step list did before stages existed.
fn graph_digest<'a>(steps: impl Iterator<Item = &'a EffectiveStep>) -> Result<String, ConfigError> {
    Ok(
        blake3::hash(&serde_json::to_vec(&steps.collect::<Vec<_>>())?)
            .to_hex()
            .to_string(),
    )
}

fn release_graph_digest(steps: &[EffectiveStep]) -> Result<Option<String>, ConfigError> {
    let run = release_run_steps(steps);
    if run.is_empty() {
        return Ok(None);
    }
    graph_digest(run.into_iter()).map(Some)
}

fn release_run_steps(steps: &[EffectiveStep]) -> Vec<&EffectiveStep> {
    let all = steps.iter().collect::<Vec<_>>();
    dependency_closure(&all, |step| !step.stage.is_gate())
}

/// The steps of `steps` matching `root`, plus every step of `steps` they
/// transitively need through `needs` or `soft_needs`, in declaration order. A
/// dependency outside `steps` is not followed.
fn dependency_closure<'a>(
    steps: &[&'a EffectiveStep],
    root: impl Fn(&EffectiveStep) -> bool,
) -> Vec<&'a EffectiveStep> {
    let by_name = steps
        .iter()
        .map(|step| (step.name.as_str(), *step))
        .collect::<HashMap<_, _>>();
    let mut selected = BTreeSet::new();
    let mut pending = steps
        .iter()
        .filter(|step| root(step))
        .map(|step| step.name.as_str())
        .collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        if !selected.insert(name) {
            continue;
        }
        if let Some(step) = by_name.get(name) {
            pending.extend(
                step.needs
                    .iter()
                    .chain(&step.soft_needs)
                    .map(String::as_str),
            );
        }
    }
    steps
        .iter()
        .copied()
        .filter(|step| selected.contains(step.name.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_configuration_and_expands_defaults() {
        let config =
            EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\n")
                .unwrap();
        assert_eq!(config.runner, ["/bin/sh", "-c"]);
        assert_eq!(config.sync_user_master, SyncUserMaster::Staging);
        assert!(
            serde_json::from_slice::<serde_json::Value>(&config.canonical_bytes().unwrap())
                .unwrap()
                .get("sync_user_master")
                .is_none()
        );
        assert_eq!(config.steps[0].timeout_ns, 60 * 60 * 1_000_000_000);
        assert_eq!(config.steps[0].working_directory, ".");
        assert_eq!(config.digest.len(), 64);
    }

    #[test]
    fn user_master_synchronization_is_explicitly_opt_out() {
        let config = EffectiveConfig::parse(
            "version = 1\nsync_user_master = false\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\n",
        )
        .unwrap();

        assert_eq!(config.sync_user_master, SyncUserMaster::Disabled);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&config.canonical_bytes().unwrap())
                .unwrap()
                .get("sync_user_master"),
            Some(&serde_json::Value::Bool(false))
        );
    }

    #[test]
    fn legacy_canonical_configuration_defaults_user_master_sync_on() {
        let config =
            EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\n")
                .unwrap();
        let mut canonical: serde_json::Value =
            serde_json::from_slice(&config.canonical_bytes().unwrap()).unwrap();
        canonical
            .as_object_mut()
            .unwrap()
            .remove("sync_user_master");

        let restored = EffectiveConfig::restore_canonical(
            &serde_json::to_vec(&canonical).unwrap(),
            "legacy-digest".into(),
            config.step_graph_digest,
        )
        .unwrap();

        assert_eq!(restored.sync_user_master, SyncUserMaster::Staging);
    }

    #[test]
    fn declaration_order_is_an_implicit_chain() {
        let config = EffectiveConfig::parse("version = 1\n[[step]]\nname = \"build\"\nrun = \"build\"\n[[step]]\nname = \"test\"\nrun = \"test\"\n").unwrap();
        assert_eq!(config.steps[1].needs, ["build"]);
    }

    #[test]
    fn unknown_fields_and_cycles_are_rejected() {
        assert!(
            EffectiveConfig::parse(
                "version = 1\nwat = true\n[[step]]\nname = \"ci\"\nrun = \"ci\"\n"
            )
            .is_err()
        );
        assert!(EffectiveConfig::parse("version = 1\n[[step]]\nname = \"a\"\nrun = \"a\"\nneeds = [\"b\"]\n[[step]]\nname = \"b\"\nrun = \"b\"\nneeds = [\"a\"]\n").is_err());
    }

    #[test]
    fn all_path_modes_safely_partition_prose_and_mixed_changes() {
        let config = EffectiveConfig::parse(
            "version = 1\n[[step]]\nname = \"prose\"\nrun = \"prose\"\ninclude = [\"docs/**\"]\ninclude_mode = \"all\"\nneeds = []\n[[step]]\nname = \"full\"\nrun = \"full\"\nexclude = [\"docs/**\"]\nexclude_mode = \"all\"\nneeds = []\n",
        )
        .unwrap();

        let names = |paths: &[&str]| {
            config
                .applicable_steps(&paths.iter().map(|path| (*path).into()).collect::<Vec<_>>())
                .unwrap()
                .into_iter()
                .map(|step| step.name.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&["docs/one.md", "docs/nested/two.md"]), ["prose"]);
        assert_eq!(names(&["docs/one.md", "src/main.rs"]), ["full"]);
        assert_eq!(names(&["src/main.rs"]), ["full"]);
        assert_eq!(names(&[]), ["full"]);
    }

    #[test]
    fn canonical_digest_is_stable_across_map_order() {
        let a = EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\n[step.environment]\nB = \"2\"\nA = \"1\"\n").unwrap();
        let b = EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\n[step.environment]\nA = \"1\"\nB = \"2\"\n").unwrap();
        assert_eq!(a.digest, b.digest);
    }

    #[test]
    fn artifact_names_are_unique_and_retention_is_nonzero() {
        assert!(EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\n[[step.artifact]]\nname = \"report\"\npatterns = [\"out\"]\n[[step.artifact]]\nname = \"report\"\npatterns = [\"other\"]\n").is_err());
        assert!(EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\n[[step.artifact]]\nname = \"report\"\npatterns = [\"out\"]\nretention_days = 0\n").is_err());
    }

    #[test]
    fn retry_reuse_is_explicit_and_excludes_artifacts_and_finalizers() {
        let defaulted =
            EffectiveConfig::parse("version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\n").unwrap();
        let explicit_false = EffectiveConfig::parse(
            "version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\nreuse_on_retry = false\n",
        )
        .unwrap();
        assert_eq!(defaulted.digest, explicit_false.digest);
        let ordinary = EffectiveConfig::parse(
            "version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\nreuse_on_retry = true\n",
        )
        .unwrap();
        assert!(ordinary.steps[0].reuse_on_retry);
        assert_ne!(defaulted.digest, ordinary.digest);
        assert!(
            EffectiveConfig::parse(
                "version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\nreuse_on_retry = true\nfinal = true\n"
            )
            .is_err()
        );
        assert!(
            EffectiveConfig::parse(
                "version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\nreuse_on_retry = true\n[[step.artifact]]\nname = \"report\"\npatterns = [\"out\"]\n"
            )
            .is_err()
        );
    }

    const LEGACY_SINGLE: &str = "version = 1\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\n";
    const LEGACY_CHAIN: &str = "version = 1\nsync_user_master = false\n[resources]\nrepository_concurrency = 1\n[[step]]\nname = \"build\"\nrun = \"build\"\n[[step]]\nname = \"test\"\nargv = [\"cargo\", \"test\"]\nvoting = false\n[[step]]\nname = \"lint\"\nrun = \"lint\"\nneeds = []\n";

    fn staged(input: &str) -> Result<EffectiveConfig, ConfigError> {
        EffectiveConfig::parse(input)
    }

    fn staged_pair(gate: &str, release: &str) -> EffectiveConfig {
        staged(&format!(
            "version = 1\n[[step]]\nname = \"setup\"\nrun = \"setup\"\n[[step]]\nname = \"fast\"\nrun = \"{gate}\"\nneeds = [\"setup\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"{release}\"\nneeds = [\"setup\"]\n"
        ))
        .unwrap()
    }

    #[test]
    fn configurations_without_stages_keep_their_pre_stage_digests() {
        let single = EffectiveConfig::parse(LEGACY_SINGLE).unwrap();
        assert_eq!(
            single.digest,
            "0dc5c49a2823952c1b9cd6f14cc48d6cebe06764a2c9b3caa2d4d01cb7fdaf9e"
        );
        assert_eq!(
            single.step_graph_digest,
            "45ad637f7d3252e4a5d66498dc10f9d3d4f6c309c748419a936d0b7cc211646a"
        );
        assert_eq!(single.release_step_graph_digest, None);
        let chain = EffectiveConfig::parse(LEGACY_CHAIN).unwrap();
        assert_eq!(
            chain.digest,
            "9d509518b6feb090fc896a940f05515784c458493a7767540547516478253006"
        );
        assert_eq!(
            chain.step_graph_digest,
            "fa61fb5aabb4d9b2b25d941edc1e82de3848b9fc69b54fc9e5c011107a4b2f70"
        );

        let explicit_defaults = EffectiveConfig::parse(
            "version = 1\nsync_user_master = true\n[resources]\nrelease_concurrency = 1\nmax_release_lag = 0\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\nstage = \"gate\"\n",
        )
        .unwrap();
        assert_eq!(explicit_defaults.digest, single.digest);
        assert_eq!(
            explicit_defaults.step_graph_digest,
            single.step_graph_digest
        );
    }

    #[test]
    fn sync_user_master_accepts_booleans_and_ref_names() {
        let parse = |value: &str| {
            EffectiveConfig::parse(&format!(
                "version = 1\nsync_user_master = {value}\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\n"
            ))
        };
        let defaulted = EffectiveConfig::parse(LEGACY_SINGLE).unwrap();
        assert_eq!(defaulted.sync_user_master, SyncUserMaster::Staging);
        for (value, expected) in [
            ("true", SyncUserMaster::Staging),
            ("\"staging\"", SyncUserMaster::Staging),
            ("\"release\"", SyncUserMaster::Release),
            ("false", SyncUserMaster::Disabled),
        ] {
            let config = parse(value).unwrap();
            assert_eq!(config.sync_user_master, expected, "{value}");
            assert_eq!(
                config.sync_user_master.is_enabled(),
                expected != SyncUserMaster::Disabled
            );
            assert_eq!(
                config.digest == defaulted.digest,
                expected == SyncUserMaster::Staging,
                "{value}"
            );
            let restored = EffectiveConfig::restore_canonical(
                &config.canonical_bytes().unwrap(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
            )
            .unwrap();
            assert_eq!(restored, config, "{value}");
        }
        let release: serde_json::Value =
            serde_json::from_slice(&parse("\"release\"").unwrap().canonical_bytes().unwrap())
                .unwrap();
        assert_eq!(release["sync_user_master"], "release");
        assert!(parse("\"master\"").is_err());
        assert!(parse("1").is_err());
    }

    #[test]
    fn release_resources_default_validate_and_contribute_when_set() {
        let defaulted = EffectiveConfig::parse(LEGACY_SINGLE).unwrap();
        assert_eq!(defaulted.resources.release_concurrency, 1);
        assert_eq!(defaulted.resources.max_release_lag, 0);
        let with = |resources: &str| {
            EffectiveConfig::parse(&format!(
                "version = 1\n[resources]\n{resources}\n[[step]]\nname = \"ci\"\nrun = \"./ci\"\n"
            ))
        };
        let concurrent = with("release_concurrency = 2").unwrap();
        assert_eq!(concurrent.resources.release_concurrency, 2);
        assert_ne!(concurrent.digest, defaulted.digest);
        let lagged = with("max_release_lag = 5").unwrap();
        assert_eq!(lagged.resources.max_release_lag, 5);
        assert_ne!(lagged.digest, defaulted.digest);
        for config in [&concurrent, &lagged] {
            let restored = EffectiveConfig::restore_canonical(
                &config.canonical_bytes().unwrap(),
                config.digest.clone(),
                config.step_graph_digest.clone(),
            )
            .unwrap();
            assert_eq!(&restored, config);
        }
        assert!(with("release_concurrency = 0").is_err());
        assert!(with("max_buildsets = 2\nrelease_concurrency = 3").is_err());
        assert!(with("max_release_lag = -1").is_err());
    }

    #[test]
    fn release_stage_steps_are_accepted_and_select_the_release_run() {
        let input = "version = 1\n[[step]]\nname = \"fast\"\nrun = \"fast\"\n[[step]]\nname = \"lint\"\nrun = \"lint\"\nneeds = []\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nneeds = [\"fast\"]\n";
        let config = EffectiveConfig::parse(input).unwrap();
        assert!(config.has_release_stage());
        assert_eq!(config.steps[2].stage, StepStage::Release);
        let names = |steps: Vec<&EffectiveStep>| {
            steps
                .into_iter()
                .map(|step| step.name.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(config.run_steps(StepStage::Gate)), ["fast", "lint"]);
        assert_eq!(
            names(config.run_steps(StepStage::Release)),
            ["fast", "full"]
        );
        assert_eq!(
            config.run_step_graph_digest(StepStage::Release),
            config.release_step_graph_digest.as_deref()
        );
        assert_eq!(
            config.run_step_graph_digest(StepStage::Gate),
            Some(config.step_graph_digest.as_str())
        );

        let opt_out = EffectiveConfig::parse(LEGACY_SINGLE).unwrap();
        assert!(!opt_out.has_release_stage());
        assert!(opt_out.run_steps(StepStage::Release).is_empty());
        assert_eq!(opt_out.run_step_graph_digest(StepStage::Release), None);
        assert!(
            EffectiveConfig::parse(
                "version = 1\n[[step]]\nname = \"ci\"\nrun = \"ci\"\nstage = \"nightly\"\n"
            )
            .is_err()
        );
    }

    #[test]
    fn gate_steps_cannot_depend_on_release_steps() {
        for edge in ["needs", "soft_needs"] {
            let input = format!(
                "version = 1\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\n[[step]]\nname = \"fast\"\nrun = \"fast\"\n{edge} = [\"full\"]\n"
            );
            let error = staged(&input).unwrap_err();
            assert!(
                matches!(&error, ConfigError::InvalidStep { step, .. } if step == "fast"),
                "{edge}: {error}"
            );
            assert!(EffectiveConfig::parse(&input).is_err());
        }
        assert!(
            staged(
                "version = 1\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\n[[step]]\nname = \"fast\"\nrun = \"fast\"\n"
            )
            .is_err(),
            "an implicit chain from a release step into a gate step is rejected"
        );
        let release_needs_gate = staged(
            "version = 1\n[[step]]\nname = \"fast\"\nrun = \"fast\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nsoft_needs = [\"fast\"]\n",
        )
        .unwrap();
        assert_eq!(release_needs_gate.steps[1].soft_needs, ["fast"]);
    }

    #[test]
    fn the_voting_step_requirement_applies_to_the_gate_stage() {
        let error = staged(
            "version = 1\n[[step]]\nname = \"fast\"\nrun = \"fast\"\nvoting = false\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\n",
        )
        .unwrap_err();
        assert!(matches!(error, ConfigError::Invalid(_)), "{error}");
        assert!(
            staged(
                "version = 1\n[[step]]\nname = \"fast\"\nrun = \"fast\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nvoting = false\n"
            )
            .is_ok()
        );
        assert!(
            staged(
                "version = 1\nallow_no_job = true\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\n"
            )
            .is_ok()
        );
    }

    #[test]
    fn stage_digests_are_independent() {
        let base = staged_pair("fast", "full");
        let release_digest = base.release_step_graph_digest.clone().unwrap();

        let release_edit = staged_pair("fast", "full --slow");
        assert_eq!(release_edit.step_graph_digest, base.step_graph_digest);
        assert_ne!(
            release_edit.release_step_graph_digest.as_deref(),
            Some(release_digest.as_str())
        );

        let gate_edit = staged_pair("fast --quick", "full");
        assert_ne!(gate_edit.step_graph_digest, base.step_graph_digest);
        assert_eq!(
            gate_edit.release_step_graph_digest.as_deref(),
            Some(release_digest.as_str()),
            "a gate step outside the release run does not affect the release digest"
        );

        let without_release = staged(
            "version = 1\n[[step]]\nname = \"setup\"\nrun = \"setup\"\n[[step]]\nname = \"fast\"\nrun = \"fast\"\nneeds = [\"setup\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n",
        )
        .unwrap();
        assert_eq!(base.step_graph_digest, without_release.step_graph_digest);
        assert_eq!(without_release.release_step_graph_digest, None);
    }

    #[test]
    fn release_runs_include_their_transitive_gate_prerequisites() {
        let config = staged(
            "version = 1\n[[step]]\nname = \"install\"\nrun = \"install\"\n[[step]]\nname = \"build\"\nrun = \"build\"\nneeds = [\"install\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nsoft_needs = [\"build\"]\n[[step]]\nname = \"e2e\"\nstage = \"release\"\nrun = \"e2e\"\nneeds = [\"full\"]\n",
        )
        .unwrap();
        let names = |steps: Vec<&EffectiveStep>| {
            steps
                .into_iter()
                .map(|step| step.name.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(config.release_run_steps()),
            ["install", "build", "full", "e2e"]
        );
        assert_eq!(
            names(config.stage_steps(StepStage::Gate).collect()),
            ["install", "build", "lint"]
        );

        let install_edit = staged(
            &"version = 1\n[[step]]\nname = \"install\"\nrun = \"install\"\n[[step]]\nname = \"build\"\nrun = \"build\"\nneeds = [\"install\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nsoft_needs = [\"build\"]\n[[step]]\nname = \"e2e\"\nstage = \"release\"\nrun = \"e2e\"\nneeds = [\"full\"]\n"
                .replacen("run = \"install\"", "run = \"install --frozen\"", 1),
        )
        .unwrap();
        assert_ne!(
            install_edit.release_step_graph_digest, config.release_step_graph_digest,
            "a gate step the release run reruns is part of the release digest"
        );

        let restored = EffectiveConfig::restore_canonical(
            &config.canonical_bytes().unwrap(),
            config.digest.clone(),
            config.step_graph_digest.clone(),
        )
        .unwrap();
        assert_eq!(restored, config);
    }

    #[test]
    fn release_runs_select_path_filtered_release_steps_and_only_their_prerequisites() {
        let config = staged(
            "version = 1\n[[step]]\nname = \"setup\"\nrun = \"setup\"\n[[step]]\nname = \"build\"\nrun = \"build\"\nneeds = [\"setup\"]\n[[step]]\nname = \"fast\"\nrun = \"fast\"\nneeds = [\"setup\"]\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nneeds = [\"setup\"]\ninclude = [\"src/**\"]\n[[step]]\nname = \"docs\"\nstage = \"release\"\nrun = \"docs\"\nsoft_needs = [\"build\"]\ninclude = [\"docs/**\"]\n",
        )
        .unwrap();
        let selected = |stage: StepStage, paths: &[&str]| {
            config
                .selected_run_steps(
                    stage,
                    &paths
                        .iter()
                        .map(|path| (*path).to_owned())
                        .collect::<Vec<_>>(),
                )
                .unwrap()
                .into_iter()
                .map(|step| step.name.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            selected(StepStage::Release, &["README.md"]),
            Vec::<&str>::new(),
            "no selected release step selects no prerequisite either"
        );
        assert_eq!(
            selected(StepStage::Release, &["src/lib.rs"]),
            ["setup", "full"]
        );
        assert_eq!(
            selected(StepStage::Release, &["docs/guide.md"]),
            ["setup", "build", "docs"],
            "soft needs are followed transitively"
        );
        assert_eq!(
            selected(StepStage::Release, &["src/lib.rs", "docs/guide.md"]),
            ["setup", "build", "full", "docs"]
        );
        assert_eq!(
            selected(StepStage::Gate, &["README.md"]),
            ["setup", "build", "fast"],
            "gate runs select each gate step by its own filters"
        );
    }

    #[test]
    fn release_only_edits_keep_the_gate_policy() {
        let base = staged_pair("fast", "full");
        assert!(base.gate_policy_matches(&base));
        assert!(base.gate_policy_matches(&staged_pair("fast", "full --slow")));
        let with_release_resources = staged(
            "version = 1\n[resources]\nrelease_concurrency = 2\nmax_release_lag = 5\n[[step]]\nname = \"setup\"\nrun = \"setup\"\n[[step]]\nname = \"fast\"\nrun = \"fast\"\nneeds = [\"setup\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nneeds = [\"setup\"]\n",
        )
        .unwrap();
        assert_ne!(with_release_resources.digest, base.digest);
        assert!(base.gate_policy_matches(&with_release_resources));

        assert!(!base.gate_policy_matches(&staged_pair("fast --quick", "full")));
        let gate_resources = staged(
            "version = 1\n[resources]\nrepository_concurrency = 3\n[[step]]\nname = \"setup\"\nrun = \"setup\"\n[[step]]\nname = \"fast\"\nrun = \"fast\"\nneeds = [\"setup\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n[[step]]\nname = \"full\"\nstage = \"release\"\nrun = \"full\"\nneeds = [\"setup\"]\n",
        )
        .unwrap();
        assert!(!base.gate_policy_matches(&gate_resources));
        let opt_out = staged(
            "version = 1\n[[step]]\nname = \"setup\"\nrun = \"setup\"\n[[step]]\nname = \"fast\"\nrun = \"fast\"\nneeds = [\"setup\"]\n[[step]]\nname = \"lint\"\nrun = \"lint\"\n",
        )
        .unwrap();
        assert_eq!(opt_out.step_graph_digest, base.step_graph_digest);
        assert!(
            !base.gate_policy_matches(&opt_out) && !opt_out.gate_policy_matches(&base),
            "opting in or out changes how promotions move the refs"
        );
    }

    #[test]
    fn schema_declares_the_staged_release_keys() {
        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../../schemas/config-v1.schema.json")).unwrap();
        let sync = &schema["properties"]["sync_user_master"];
        assert_eq!(sync["default"], "staging");
        let accepted = sync["oneOf"].as_array().unwrap();
        assert!(accepted.iter().any(|entry| entry["type"] == "boolean"));
        assert!(
            accepted
                .iter()
                .any(|entry| { entry["enum"] == serde_json::json!(["staging", "release"]) })
        );
        let resources = &schema["properties"]["resources"]["properties"];
        assert_eq!(
            resources["release_concurrency"]["default"],
            u64::from(default_release_concurrency())
        );
        assert_eq!(resources["release_concurrency"]["minimum"], 1);
        assert_eq!(resources["max_release_lag"]["default"], 0);
        assert_eq!(resources["max_release_lag"]["minimum"], 0);
        let stage = &schema["$defs"]["step"]["properties"]["stage"];
        assert_eq!(stage["enum"], serde_json::json!(["gate", "release"]));
        assert_eq!(stage["default"], StepStage::default().as_str());
    }
}
