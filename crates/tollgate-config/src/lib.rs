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

#[derive(Clone, Debug, Default, Deserialize)]
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
    pub exclude: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub remove_environment: Vec<String>,
    #[serde(default)]
    pub artifact: Vec<ArtifactFile>,
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
    pub runner: Vec<String>,
    pub allow_no_job: bool,
    pub allow_concurrent_roots: bool,
    pub steps: Vec<EffectiveStep>,
    pub resources: EffectiveResources,
    pub remote: EffectiveRemote,
    pub cache: EffectiveCache,
    pub digest: String,
    pub step_graph_digest: String,
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
    pub working_directory: String,
    pub needs: Vec<String>,
    pub soft_needs: Vec<String>,
    pub voting: bool,
    pub final_step: bool,
    pub timeout_ns: u64,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub rss_limit_bytes: Option<u64>,
    pub semaphores: Vec<String>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
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
            runner: Vec<String>,
            allow_no_job: bool,
            allow_concurrent_roots: bool,
            steps: Vec<EffectiveStep>,
            resources: EffectiveResources,
            remote: EffectiveRemote,
            cache: EffectiveCache,
        }
        let value: Canonical = serde_json::from_slice(bytes)?;
        Ok(Self {
            version: value.version,
            runner: value.runner,
            allow_no_job: value.allow_no_job,
            allow_concurrent_roots: value.allow_concurrent_roots,
            steps: value.steps,
            resources: value.resources,
            remote: value.remote,
            cache: value.cache,
            digest,
            step_graph_digest,
        })
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
        if !raw.allow_no_job && !steps.iter().any(|step| step.voting) {
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
        let step_graph_digest = graph_digest(&steps)?;
        let mut config = Self {
            version: 1,
            runner,
            allow_no_job: raw.allow_no_job,
            allow_concurrent_roots: raw.allow_concurrent_roots,
            steps,
            resources,
            remote,
            cache,
            digest: String::new(),
            step_graph_digest,
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
        let selected =
            self.include.is_empty() || changed_paths.iter().any(|path| includes.is_match(path));
        Ok(selected && !changed_paths.iter().any(|path| excludes.is_match(path)))
    }
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
                validate_pattern(pattern)?;
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
    Ok(EffectiveStep {
        name: step.name,
        command,
        working_directory: normalize_relative(&step.working_directory)?,
        needs,
        soft_needs: sorted_unique(step.soft_needs)?,
        voting: step.voting,
        final_step: step.final_step,
        timeout_ns,
        cpu_tokens: step.cpu_tokens,
        memory_bytes: step.memory_bytes,
        rss_limit_bytes: step.rss_limit_bytes,
        semaphores: sorted_names(step.semaphores)?,
        include: step.include,
        exclude: step.exclude,
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

fn graph_digest(steps: &[EffectiveStep]) -> Result<String, ConfigError> {
    Ok(blake3::hash(&serde_json::to_vec(steps)?)
        .to_hex()
        .to_string())
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
        assert_eq!(config.steps[0].timeout_ns, 60 * 60 * 1_000_000_000);
        assert_eq!(config.steps[0].working_directory, ".");
        assert_eq!(config.digest.len(), 64);
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
}
