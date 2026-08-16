#![deny(unsafe_op_in_unsafe_fn)]

pub mod apfs;

#[cfg(target_os = "macos")]
use std::os::unix::fs::OpenOptionsExt;

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use blake3::Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom},
    process::Command,
    sync::mpsc,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;
use tollgate_config::{EffectiveCommand, EffectiveConfig, EffectiveStep};
use tollgate_domain::{GitOid, RepairCommand, StepAttemptId, StepDiagnostic, StepId};
use tollgate_scheduler::{GlobalScheduler, StepResources};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("could not start command: {0}")]
    Spawn(#[from] std::io::Error),
    #[error("log serialization failed: {0}")]
    LogFrame(#[from] serde_json::Error),
    #[error("command supervision was interrupted: {0}")]
    Interrupted(String),
    #[error("workspace verification failed: {0}")]
    WorkspaceDirty(String),
    #[error("login-shell environment bootstrap failed: {0}")]
    Environment(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogFrame {
    pub stream: LogStream,
    pub stream_offset: u64,
    pub broker_sequence: u64,
    pub monotonic_ns: u64,
    pub wall_time: OffsetDateTime,
    pub payload_len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealedLog {
    pub stdout_end: u64,
    pub stderr_end: u64,
    pub broker_sequence_end: u64,
    pub hash: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RenderedLogFrame {
    pub frame: LogFrame,
    pub text: String,
    pub invalid_utf8: bool,
}

pub async fn read_durable_log(
    path: impl AsRef<Path>,
    start_sequence: u64,
    limit: usize,
) -> Result<Vec<RenderedLogFrame>, RunnerError> {
    const MAX_LOG_PAYLOAD: u32 = 1024 * 1024;
    let path = path.as_ref();
    let mut file = File::open(path).await?;
    let index_path = log_index_path(path);
    if start_sequence > 0 && tokio::fs::try_exists(&index_path).await? {
        let mut index = File::open(&index_path).await?;
        let index_len = index.metadata().await?.len();
        if index_len % 16 != 0 {
            return Err(RunnerError::Interrupted(
                "log seek index is malformed".into(),
            ));
        }
        let record_count = index_len / 16;
        if start_sequence <= record_count {
            index
                .seek(SeekFrom::Start((start_sequence - 1) * 16))
                .await?;
            let mut record = [0u8; 16];
            index.read_exact(&mut record).await?;
            let sequence = u64::from_be_bytes(record[..8].try_into().expect("eight bytes"));
            let offset = u64::from_be_bytes(record[8..].try_into().expect("eight bytes"));
            if sequence != start_sequence || offset > file.metadata().await?.len() {
                return Err(RunnerError::Interrupted(
                    "log seek index failed validation".into(),
                ));
            }
            file.seek(SeekFrom::Start(offset)).await?;
        } else {
            return Ok(Vec::new());
        }
    }
    let mut result = Vec::new();
    loop {
        let mut length = [0u8; 4];
        match file.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
        let header_len = u32::from_be_bytes(length) as usize;
        if header_len > 64 * 1024 {
            return Err(RunnerError::Interrupted(
                "log frame header exceeds 64 KiB".into(),
            ));
        }
        let mut header = vec![0u8; header_len];
        file.read_exact(&mut header).await?;
        let frame: LogFrame = serde_json::from_slice(&header)?;
        if frame.payload_len > MAX_LOG_PAYLOAD {
            return Err(RunnerError::Interrupted(
                "log frame payload exceeds 1 MiB".into(),
            ));
        }
        let mut payload = vec![0u8; frame.payload_len as usize];
        file.read_exact(&mut payload).await?;
        if frame.broker_sequence >= start_sequence && result.len() < limit {
            let invalid_utf8 = std::str::from_utf8(&payload).is_err();
            result.push(RenderedLogFrame {
                frame,
                text: String::from_utf8_lossy(&payload).into_owned(),
                invalid_utf8,
            });
        }
        if result.len() >= limit {
            break;
        }
    }
    Ok(result)
}

pub async fn durable_log_tail_start(
    path: impl AsRef<Path>,
    frame_count: u64,
) -> Result<u64, RunnerError> {
    let index_path = log_index_path(path.as_ref());
    let length = match tokio::fs::metadata(&index_path).await {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    if length % 16 != 0 {
        return Err(RunnerError::Interrupted(
            "log seek index is malformed".into(),
        ));
    }
    let total_frames = length / 16;
    Ok(if total_frames == 0 {
        0
    } else {
        total_frames
            .saturating_sub(frame_count.saturating_sub(1))
            .max(1)
    })
}

pub async fn verify_durable_log(
    path: impl AsRef<Path>,
    expected_hash: &str,
    expected_stdout_end: u64,
    expected_stderr_end: u64,
) -> Result<bool, RunnerError> {
    const MAX_LOG_PAYLOAD: u32 = 1024 * 1024;
    let mut file = File::open(path).await?;
    let mut hasher = Hasher::new();
    let mut stdout_end = 0u64;
    let mut stderr_end = 0u64;
    loop {
        let mut length = [0u8; 4];
        if file.read(&mut length[..1]).await? == 0 {
            break;
        }
        file.read_exact(&mut length[1..]).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                RunnerError::Interrupted("truncated log frame length".into())
            } else {
                error.into()
            }
        })?;
        hasher.update(&length);
        let header_len = u32::from_be_bytes(length) as usize;
        if header_len > 64 * 1024 {
            return Err(RunnerError::Interrupted(
                "log frame header exceeds 64 KiB".into(),
            ));
        }
        let mut header = vec![0u8; header_len];
        file.read_exact(&mut header).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                RunnerError::Interrupted("truncated log frame header".into())
            } else {
                error.into()
            }
        })?;
        hasher.update(&header);
        let frame: LogFrame = serde_json::from_slice(&header)?;
        if frame.payload_len > MAX_LOG_PAYLOAD {
            return Err(RunnerError::Interrupted(
                "log frame payload exceeds 1 MiB".into(),
            ));
        }
        let mut remaining = frame.payload_len as usize;
        let mut payload = [0u8; 32 * 1024];
        while remaining > 0 {
            let chunk_len = remaining.min(payload.len());
            file.read_exact(&mut payload[..chunk_len])
                .await
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        RunnerError::Interrupted("truncated log frame payload".into())
                    } else {
                        error.into()
                    }
                })?;
            hasher.update(&payload[..chunk_len]);
            remaining -= chunk_len;
        }
        let stream_end = frame
            .stream_offset
            .checked_add(u64::from(frame.payload_len))
            .ok_or_else(|| RunnerError::Interrupted("log stream offset overflow".into()))?;
        match frame.stream {
            LogStream::Stdout => {
                stdout_end = stdout_end.max(stream_end);
            }
            LogStream::Stderr => {
                stderr_end = stderr_end.max(stream_end);
            }
        }
    }
    Ok(hasher.finalize().to_hex().as_str() == expected_hash
        && stdout_end == expected_stdout_end
        && stderr_end == expected_stderr_end)
}

pub struct DurableLogWriter {
    file: File,
    index: File,
    path: PathBuf,
    file_offset: u64,
    stdout_offset: u64,
    stderr_offset: u64,
    broker_sequence: u64,
    started: Instant,
    hasher: Hasher,
}

impl DurableLogWriter {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self, RunnerError> {
        if let Some(parent) = path.as_ref().parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path.as_ref())
            .await?;
        let index = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(log_index_path(path.as_ref()))
            .await?;
        Ok(Self {
            file,
            index,
            path: path.as_ref().to_owned(),
            file_offset: 0,
            stdout_offset: 0,
            stderr_offset: 0,
            broker_sequence: 0,
            started: Instant::now(),
            hasher: Hasher::new(),
        })
    }

    pub async fn append(
        &mut self,
        stream: LogStream,
        payload: &[u8],
    ) -> Result<LogFrame, RunnerError> {
        let offset = match stream {
            LogStream::Stdout => self.stdout_offset,
            LogStream::Stderr => self.stderr_offset,
        };
        self.broker_sequence += 1;
        let frame = LogFrame {
            stream,
            stream_offset: offset,
            broker_sequence: self.broker_sequence,
            monotonic_ns: self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            wall_time: OffsetDateTime::now_utc(),
            payload_len: payload.len() as u32,
        };
        let header = serde_json::to_vec(&frame)?;
        self.index
            .write_all(&frame.broker_sequence.to_be_bytes())
            .await?;
        self.index
            .write_all(&self.file_offset.to_be_bytes())
            .await?;
        self.index.flush().await?;
        self.file
            .write_all(&(header.len() as u32).to_be_bytes())
            .await?;
        self.file.write_all(&header).await?;
        self.file.write_all(payload).await?;
        self.file.flush().await?;
        self.file_offset = self
            .file_offset
            .saturating_add(4 + header.len() as u64 + payload.len() as u64);
        self.hasher.update(&(header.len() as u32).to_be_bytes());
        self.hasher.update(&header);
        self.hasher.update(payload);
        match stream {
            LogStream::Stdout => self.stdout_offset += payload.len() as u64,
            LogStream::Stderr => self.stderr_offset += payload.len() as u64,
        }
        Ok(frame)
    }

    pub async fn seal(self) -> Result<SealedLog, RunnerError> {
        self.file.sync_all().await?;
        self.index.sync_all().await?;
        Ok(SealedLog {
            stdout_end: self.stdout_offset,
            stderr_end: self.stderr_offset,
            broker_sequence_end: self.broker_sequence,
            hash: self.hasher.finalize().to_hex().to_string(),
            path: self.path,
        })
    }
}

fn log_index_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.idx", path.display()))
}

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    pub cwd: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub read_only_context: BTreeMap<String, String>,
    pub runner: Vec<String>,
    pub log_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StepResultClass {
    Success,
    ExitFailure,
    Timeout,
    Canceled,
    Interrupted,
    SpawnFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepResult {
    pub attempt_id: StepAttemptId,
    pub class: StepResultClass,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub elapsed_ms: u64,
    pub log: SealedLog,
    #[serde(default)]
    pub diagnostics: Vec<StepDiagnostic>,
}

fn externally_terminated(
    command: &EffectiveCommand,
    exit_code: Option<i32>,
    signal: Option<i32>,
) -> bool {
    #[cfg(unix)]
    {
        const EXTERNAL_CONTROL_SIGNALS: [i32; 4] = [1, 2, 9, 15];
        signal.is_some_and(|value| EXTERNAL_CONTROL_SIGNALS.contains(&value))
            || matches!(command, EffectiveCommand::Shell { .. })
                && exit_code
                    .and_then(|value| value.checked_sub(128))
                    .is_some_and(|value| EXTERNAL_CONTROL_SIGNALS.contains(&value))
    }
    #[cfg(not(unix))]
    {
        let _ = (command, exit_code, signal);
        false
    }
}

fn containment_diagnostics(message: Option<String>) -> Vec<StepDiagnostic> {
    message
        .into_iter()
        .map(|message| StepDiagnostic {
            code: "tollgate.containment-escape".into(),
            message,
            paths: Vec::new(),
            repair: None,
        })
        .collect()
}

pub async fn run_step(
    step: &EffectiveStep,
    mut context: ExecutionContext,
    cancellation: CancellationToken,
) -> Result<StepResult, RunnerError> {
    let diagnostics_path =
        PathBuf::from(format!("{}.diagnostics.jsonl", context.log_path.display()));
    match tokio::fs::remove_file(&diagnostics_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    context.read_only_context.insert(
        "TOLLGATE_DIAGNOSTICS_FILE".into(),
        diagnostics_path.to_string_lossy().into_owned(),
    );
    let mut result = run_step_inner(step, context, cancellation).await?;
    match read_step_diagnostics(&diagnostics_path).await {
        Ok(diagnostics) => result.diagnostics.extend(diagnostics),
        Err(error) => {
            result.class = StepResultClass::ExitFailure;
            result.diagnostics.push(StepDiagnostic {
                code: "tollgate.diagnostics-invalid".into(),
                message: error,
                paths: Vec::new(),
                repair: None,
            });
        }
    }
    Ok(result)
}

async fn run_step_inner(
    step: &EffectiveStep,
    context: ExecutionContext,
    cancellation: CancellationToken,
) -> Result<StepResult, RunnerError> {
    if let Some(worker) = locate_worker() {
        return run_step_via_worker(worker, step, context, cancellation).await;
    }
    // Cargo's dependency test executables are not app lifetimes and do not have a
    // bundled sidecar. The direct path exists solely to keep crate and service
    // fixtures hermetic; a normal app execution fails closed without its worker.
    if std::env::current_exe()
        .ok()
        .is_some_and(|path| path.components().any(|part| part.as_os_str() == "deps"))
    {
        return run_step_direct(step, context, cancellation).await;
    }
    Err(RunnerError::Interrupted(
        "tollgate-worker sidecar is unavailable".into(),
    ))
}

async fn read_step_diagnostics(path: &Path) -> Result<Vec<StepDiagnostic>, String> {
    const MAX_BYTES: u64 = 1024 * 1024;
    const MAX_DIAGNOSTICS: usize = 128;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "macos")]
    options.custom_flags(nix::libc::O_NOFOLLOW);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("could not open diagnostic output safely: {error}")),
    };
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not inspect diagnostic output: {error}"))?;
    if !metadata.is_file() {
        return Err("diagnostic output must be a regular file".into());
    }
    if metadata.len() > MAX_BYTES {
        return Err(format!("diagnostic output exceeds {MAX_BYTES} bytes"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::from_std(file)
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| format!("could not read diagnostic output: {error}"))?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(format!("diagnostic output exceeds {MAX_BYTES} bytes"));
    }
    let contents = String::from_utf8(bytes)
        .map_err(|error| format!("diagnostic output is not UTF-8: {error}"))?;
    let mut diagnostics = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        if diagnostics.len() == MAX_DIAGNOSTICS {
            return Err(format!(
                "diagnostic output exceeds {MAX_DIAGNOSTICS} records"
            ));
        }
        let diagnostic: StepDiagnostic = serde_json::from_str(line)
            .map_err(|error| format!("diagnostic record {} is malformed: {error}", index + 1))?;
        validate_step_diagnostic(&diagnostic)
            .map_err(|error| format!("diagnostic record {} is invalid: {error}", index + 1))?;
        diagnostics.push(diagnostic);
    }
    Ok(diagnostics)
}

fn validate_step_diagnostic(diagnostic: &StepDiagnostic) -> Result<(), String> {
    if diagnostic.code.is_empty()
        || diagnostic.code.len() > 128
        || !diagnostic.code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
        })
    {
        return Err("code must be 1-128 lowercase ASCII letters, digits, '.', '-' or '_'".into());
    }
    if diagnostic.message.is_empty() || diagnostic.message.len() > 4096 {
        return Err("message must be 1-4096 bytes".into());
    }
    if diagnostic.paths.len() > 128 {
        return Err("paths exceeds 128 entries".into());
    }
    for path in &diagnostic.paths {
        let path = Path::new(path);
        if path.as_os_str().is_empty()
            || path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err("paths must be non-empty repository-relative paths without '..'".into());
        }
    }
    if let Some(RepairCommand::Argv { argv }) = &diagnostic.repair
        && (argv.is_empty() || argv.len() > 64 || argv.iter().any(|argument| argument.len() > 4096))
    {
        return Err("repair argv must contain 1-64 bounded arguments".into());
    }
    Ok(())
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerInput {
    Spec {
        nonce: String,
        cwd: PathBuf,
        argv: Vec<String>,
        environment: BTreeMap<String, String>,
        timeout_ms: u64,
        termination_grace_ms: u64,
        rss_limit_bytes: Option<u64>,
    },
    Start {
        nonce: String,
    },
    Terminate {
        nonce: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerOutput {
    Registered {
        nonce: String,
        worker_pid: u32,
    },
    Started {
        nonce: String,
        child_pid: u32,
        process_group_id: u32,
    },
    Log {
        nonce: String,
        stream: String,
        payload_base64: String,
    },
    Terminal {
        nonce: String,
        exit_code: Option<i32>,
        signal: Option<i32>,
        timed_out: bool,
        canceled: bool,
        pipes_eof: bool,
        process_group_reaped: bool,
        rss_exceeded: bool,
        containment_escaped: bool,
        #[serde(default)]
        containment_diagnostic: Option<String>,
    },
}

struct ProcessGroupGuard(Option<u32>);

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.0.and_then(|value| i32::try_from(value).ok()) {
            use nix::{
                sys::signal::{Signal, killpg},
                unistd::Pid,
            };
            let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }
}

async fn run_step_via_worker(
    worker: PathBuf,
    step: &EffectiveStep,
    context: ExecutionContext,
    cancellation: CancellationToken,
) -> Result<StepResult, RunnerError> {
    let attempt_id = StepAttemptId::new();
    let nonce = Uuid::now_v7().to_string();
    let argv = match &step.command {
        EffectiveCommand::Shell { script } => context
            .runner
            .iter()
            .cloned()
            .chain(std::iter::once(script.clone()))
            .collect::<Vec<_>>(),
        EffectiveCommand::Argv { argv } => argv.clone(),
    };
    let mut environment = context.environment;
    environment.extend(step.environment.clone());
    for name in &step.remove_environment {
        environment.remove(name);
    }
    environment.extend(context.read_only_context);
    let started = Instant::now();
    let mut log = DurableLogWriter::create(&context.log_path).await?;
    let mut process = Command::new(worker)
        .args(["--nonce", &nonce])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut input = process
        .stdin
        .take()
        .ok_or_else(|| RunnerError::Interrupted("worker control pipe missing".into()))?;
    let output = process
        .stdout
        .take()
        .ok_or_else(|| RunnerError::Interrupted("worker output pipe missing".into()))?;
    let mut output = BufReader::new(output).lines();
    let mut worker_stderr = process
        .stderr
        .take()
        .ok_or_else(|| RunnerError::Interrupted("worker diagnostic pipe missing".into()))?;
    send_worker_input(
        &mut input,
        &WorkerInput::Spec {
            nonce: nonce.clone(),
            cwd: context.cwd.join(&step.working_directory),
            argv,
            environment,
            timeout_ms: Duration::from_nanos(step.timeout_ns).as_millis() as u64,
            termination_grace_ms: 10_000,
            rss_limit_bytes: step.rss_limit_bytes,
        },
    )
    .await?;
    let registered = next_worker_output(&mut output).await?;
    match registered {
        WorkerOutput::Registered {
            nonce: value,
            worker_pid,
        } if value == nonce && Some(worker_pid) == process.id() => {}
        _ => {
            return Err(RunnerError::Interrupted(
                "worker registration identity mismatch".into(),
            ));
        }
    }
    send_worker_input(
        &mut input,
        &WorkerInput::Start {
            nonce: nonce.clone(),
        },
    )
    .await?;

    let mut terminate_sent = false;
    let mut supervised_group = None;
    let mut group_guard = ProcessGroupGuard(None);
    let terminal = loop {
        tokio::select! {
            _ = cancellation.cancelled(), if !terminate_sent => {
                send_worker_input(&mut input, &WorkerInput::Terminate { nonce: nonce.clone() }).await?;
                terminate_sent = true;
            }
            frame = next_worker_output(&mut output) => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        if let Some(process_group) = supervised_group {
                            terminate_external_group(process_group).await;
                        }
                        let _ = process.kill().await;
                        return Err(error);
                    }
                };
                match frame {
                    WorkerOutput::Started { nonce: value, child_pid, process_group_id }
                        if value == nonce && child_pid > 0 && process_group_id == child_pid => {
                            supervised_group = Some(process_group_id);
                            group_guard.0 = Some(process_group_id);
                        }
                    WorkerOutput::Log { nonce: value, stream, payload_base64 } if value == nonce => {
                        let payload = STANDARD.decode(payload_base64).map_err(|error| RunnerError::Interrupted(format!("worker log payload is malformed: {error}")))?;
                        let stream = match stream.as_str() {
                            "stdout" => LogStream::Stdout,
                            "stderr" => LogStream::Stderr,
                            _ => return Err(RunnerError::Interrupted("worker reported an unknown log stream".into())),
                        };
                        log.append(stream, &payload).await?;
                    }
                    terminal @ WorkerOutput::Terminal { .. } => {
                        if !matches!(&terminal, WorkerOutput::Terminal { nonce: value, .. } if value == &nonce) {
                            return Err(RunnerError::Interrupted("worker terminal nonce mismatch".into()));
                        }
                        break terminal;
                    }
                    _ => return Err(RunnerError::Interrupted("worker lifetime frame failed validation".into())),
                }
            }
        }
    };
    drop(input);
    let status = process.wait().await?;
    let mut diagnostics = Vec::new();
    worker_stderr.read_to_end(&mut diagnostics).await?;
    if !status.success() {
        return Err(RunnerError::Interrupted(format!(
            "worker exited unsuccessfully: {}",
            String::from_utf8_lossy(&diagnostics).trim()
        )));
    }
    let WorkerOutput::Terminal {
        exit_code,
        signal,
        timed_out,
        canceled,
        pipes_eof,
        process_group_reaped,
        rss_exceeded,
        containment_escaped,
        containment_diagnostic,
        ..
    } = terminal
    else {
        unreachable!()
    };
    if !pipes_eof || !process_group_reaped {
        return Err(RunnerError::Interrupted(
            "worker could not prove pipe EOF and process-group reaping".into(),
        ));
    }
    group_guard.0 = None;
    let class = if rss_exceeded || containment_escaped {
        StepResultClass::ExitFailure
    } else if timed_out {
        StepResultClass::Timeout
    } else if canceled {
        StepResultClass::Canceled
    } else if exit_code == Some(0) && signal.is_none() {
        StepResultClass::Success
    } else if externally_terminated(&step.command, exit_code, signal) {
        StepResultClass::Interrupted
    } else {
        StepResultClass::ExitFailure
    };
    let diagnostics = containment_diagnostics(containment_diagnostic);
    Ok(StepResult {
        attempt_id,
        class,
        exit_code,
        signal,
        elapsed_ms: started.elapsed().as_millis() as u64,
        log: log.seal().await?,
        diagnostics,
    })
}

async fn terminate_external_group(process_group: u32) {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(process_group) {
        use nix::{
            sys::signal::{Signal, killpg},
            unistd::Pid,
        };
        let pid = Pid::from_raw(pid);
        let _ = killpg(pid, Signal::SIGTERM);
        tokio::time::sleep(Duration::from_millis(250)).await;
        let _ = killpg(pid, Signal::SIGKILL);
    }
}

async fn send_worker_input(
    input: &mut tokio::process::ChildStdin,
    value: &WorkerInput,
) -> Result<(), RunnerError> {
    input.write_all(&serde_json::to_vec(value)?).await?;
    input.write_all(b"\n").await?;
    input.flush().await?;
    Ok(())
}

async fn next_worker_output(
    output: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
) -> Result<WorkerOutput, RunnerError> {
    let line = output
        .next_line()
        .await?
        .ok_or_else(|| RunnerError::Interrupted("worker lifetime channel closed".into()))?;
    Ok(serde_json::from_str(&line)?)
}

fn locate_worker() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("TOLLGATE_WORKER") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    let executable = std::env::current_exe().ok()?;
    let directory = executable.parent()?;
    [
        directory.join("tollgate-worker"),
        directory.parent()?.join("tollgate-worker"),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

async fn run_step_direct(
    step: &EffectiveStep,
    context: ExecutionContext,
    cancellation: CancellationToken,
) -> Result<StepResult, RunnerError> {
    let attempt_id = StepAttemptId::new();
    let mut command = match &step.command {
        EffectiveCommand::Shell { script } => {
            let mut command = Command::new(&context.runner[0]);
            command.args(&context.runner[1..]).arg(script);
            command
        }
        EffectiveCommand::Argv { argv } => {
            let mut command = Command::new(&argv[0]);
            command.args(&argv[1..]);
            command
        }
    };
    command.current_dir(context.cwd.join(&step.working_directory));
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
        .env_clear()
        .envs(context.environment)
        .envs(&step.environment);
    for name in &step.remove_environment {
        command.env_remove(name);
    }
    command.envs(context.read_only_context);
    #[cfg(unix)]
    command.process_group(0);

    let started = Instant::now();
    let mut log = DurableLogWriter::create(&context.log_path).await?;
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            log.append(
                LogStream::Stderr,
                format!("Tollgate could not spawn step: {error}\n").as_bytes(),
            )
            .await?;
            return Ok(StepResult {
                attempt_id,
                class: StepResultClass::SpawnFailure,
                exit_code: None,
                signal: None,
                elapsed_ms: started.elapsed().as_millis() as u64,
                log: log.seal().await?,
                diagnostics: Vec::new(),
            });
        }
    };
    let child_id = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| RunnerError::Interrupted("stdout pipe missing".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| RunnerError::Interrupted("stderr pipe missing".into()))?;
    let (sender, mut receiver) = mpsc::channel::<(LogStream, Vec<u8>)>(64);
    let stdout_task = tokio::spawn(read_pipe(stdout, LogStream::Stdout, sender.clone()));
    let stderr_task = tokio::spawn(read_pipe(stderr, LogStream::Stderr, sender));
    let log_task = tokio::spawn(async move {
        while let Some((stream, bytes)) = receiver.recv().await {
            log.append(stream, &bytes).await?;
        }
        log.seal().await
    });
    let command_timeout = Duration::from_nanos(step.timeout_ns);

    enum Termination {
        Exited(std::process::ExitStatus),
        TimedOut,
        Canceled,
    }
    let termination = tokio::select! {
        status = child.wait() => Termination::Exited(status?),
        _ = tokio::time::sleep(command_timeout) => { terminate_group(child_id, &mut child).await; Termination::TimedOut },
        _ = cancellation.cancelled() => { terminate_group(child_id, &mut child).await; Termination::Canceled },
    };
    drop(child);
    stdout_task
        .await
        .map_err(|error| RunnerError::Interrupted(error.to_string()))??;
    stderr_task
        .await
        .map_err(|error| RunnerError::Interrupted(error.to_string()))??;
    let log = log_task
        .await
        .map_err(|error| RunnerError::Interrupted(error.to_string()))??;
    let (class, exit_code, signal) = match termination {
        Termination::TimedOut => (StepResultClass::Timeout, None, None),
        Termination::Canceled => (StepResultClass::Canceled, None, None),
        Termination::Exited(status) => {
            let exit_code = status.code();
            #[cfg(unix)]
            let signal = {
                use std::os::unix::process::ExitStatusExt;
                status.signal()
            };
            #[cfg(not(unix))]
            let signal = None;
            let class = if status.success() {
                StepResultClass::Success
            } else if externally_terminated(&step.command, exit_code, signal) {
                StepResultClass::Interrupted
            } else {
                StepResultClass::ExitFailure
            };
            (class, exit_code, signal)
        }
    };
    Ok(StepResult {
        attempt_id,
        class,
        exit_code,
        signal,
        elapsed_ms: started.elapsed().as_millis() as u64,
        log,
        diagnostics: Vec::new(),
    })
}

async fn read_pipe<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    stream: LogStream,
    sender: mpsc::Sender<(LogStream, Vec<u8>)>,
) -> Result<(), RunnerError> {
    let mut buffer = vec![0u8; 32 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        if sender
            .send((stream, buffer[..read].to_vec()))
            .await
            .is_err()
        {
            return Err(RunnerError::Interrupted("durable log broker closed".into()));
        }
    }
    Ok(())
}

async fn terminate_group(child_id: Option<u32>, child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child_id.and_then(|value| i32::try_from(value).ok()) {
        use nix::{
            sys::signal::{Signal, killpg},
            unistd::Pid,
        };
        let _ = killpg(Pid::from_raw(pid), Signal::SIGTERM);
        if timeout(Duration::from_secs(10), child.wait()).await.is_ok() {
            return;
        }
        let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
        let _ = timeout(Duration::from_secs(5), child.wait()).await;
        return;
    }
    let _ = child.kill().await;
}

#[derive(Clone, Debug)]
pub struct BuildsetExecution {
    pub tested_oid: GitOid,
    pub slot_root: PathBuf,
    pub log_directory: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub context: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildsetResult {
    pub passed: bool,
    pub passed_with_warnings: bool,
    pub steps: Vec<(String, StepResult)>,
    pub skipped: Vec<String>,
    pub workspace_verified: bool,
    pub workspace_verification_error: Option<String>,
}

pub async fn run_buildset(
    config: &EffectiveConfig,
    execution: BuildsetExecution,
    changed_paths: &[String],
    cancellation: CancellationToken,
) -> Result<BuildsetResult, RunnerError> {
    run_buildset_scheduled(config, execution, changed_paths, cancellation, None).await
}

pub async fn run_buildset_scheduled(
    config: &EffectiveConfig,
    execution: BuildsetExecution,
    changed_paths: &[String],
    cancellation: CancellationToken,
    scheduler: Option<Arc<GlobalScheduler>>,
) -> Result<BuildsetResult, RunnerError> {
    let applicable = config
        .applicable_steps(changed_paths)
        .map_err(|error| RunnerError::Interrupted(error.to_string()))?;
    let applicable_names = applicable
        .iter()
        .map(|step| step.name.clone())
        .collect::<std::collections::HashSet<_>>();
    let mut results = HashMap::new();
    let mut ordered = Vec::new();
    let mut skipped = config
        .steps
        .iter()
        .filter(|step| !applicable_names.contains(step.name.as_str()))
        .map(|step| step.name.clone())
        .collect::<Vec<_>>();
    for step in &applicable {
        if step
            .needs
            .iter()
            .any(|need| !applicable_names.contains(need.as_str()))
        {
            return Err(RunnerError::Interrupted(format!(
                "applicable step `{}` has a skipped hard dependency",
                step.name
            )));
        }
    }
    let mut pending = applicable;
    while !pending.is_empty() {
        let ordinary_pending = pending.iter().any(|step| !step.final_step);
        let mut runnable = pending
            .iter()
            .enumerate()
            .filter_map(|(index, step)| {
                (step.final_step != ordinary_pending
                    && step.needs.iter().chain(&step.soft_needs).all(|need| {
                        !applicable_names.contains(need.as_str()) || results.contains_key(need)
                    }))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        if runnable.is_empty() {
            return Err(RunnerError::Interrupted(
                "step DAG made no scheduling progress".into(),
            ));
        }
        let failed = runnable
            .iter()
            .copied()
            .filter(|index| {
                let step = &pending[*index];
                !step.final_step
                    && step.needs.iter().chain(&step.soft_needs).any(|need| {
                        results
                            .get(need)
                            .is_some_and(|class| *class != StepResultClass::Success)
                    })
            })
            .collect::<Vec<_>>();
        if !failed.is_empty() {
            for index in failed.into_iter().rev() {
                let step = pending.remove(index);
                skipped.push(step.name.clone());
                results.insert(step.name.clone(), StepResultClass::Interrupted);
            }
            continue;
        }
        if !config.allow_concurrent_roots {
            runnable.truncate(1);
        }
        let mut steps = Vec::with_capacity(runnable.len());
        for index in runnable.into_iter().rev() {
            steps.push(pending.remove(index));
        }
        steps.reverse();
        let executions = steps.into_iter().map(|step| {
            let scheduler = scheduler.clone();
            let cancellation = cancellation.child_token();
            let context = ExecutionContext {
                cwd: execution.slot_root.clone(),
                environment: execution.environment.clone(),
                read_only_context: execution.context.clone(),
                runner: config.runner.clone(),
                log_path: execution.log_directory.join(format!("{}.tlog", step.name)),
            };
            async move {
                let _resources = if let Some(scheduler) = scheduler {
                    Some(
                        scheduler
                            .acquire_step(
                                StepId::new(),
                                StepResources {
                                    cpu_tokens: step.cpu_tokens,
                                    memory_bytes: step.memory_bytes,
                                    semaphores: step
                                        .semaphores
                                        .iter()
                                        .map(|name| (name.clone(), 1))
                                        .collect(),
                                },
                                &cancellation,
                            )
                            .await
                            .map_err(|error| RunnerError::Interrupted(error.to_string()))?,
                    )
                } else {
                    None
                };
                let name = step.name.clone();
                let result = run_step(step, context, cancellation).await?;
                if result.class == StepResultClass::Interrupted {
                    return Err(RunnerError::Interrupted(format!(
                        "step `{name}` was externally terminated (exit {:?}, signal {:?})",
                        result.exit_code, result.signal
                    )));
                }
                Ok::<_, RunnerError>((name, result))
            }
        });
        for completed in futures::future::join_all(executions).await {
            let (name, result) = completed?;
            results.insert(name.clone(), result.class.clone());
            ordered.push((name, result));
        }
    }
    for step in config
        .steps
        .iter()
        .filter(|step| applicable_names.contains(step.name.as_str()))
    {
        if verify_required_artifacts(step, &execution.slot_root).is_err() {
            results.insert(step.name.clone(), StepResultClass::ExitFailure);
            if let Some((_, result)) = ordered.iter_mut().find(|(name, _)| name == &step.name) {
                result.class = StepResultClass::ExitFailure;
            }
        }
    }
    let applicable_voting = config
        .steps
        .iter()
        .any(|step| step.voting && applicable_names.contains(step.name.as_str()));
    let voting_failed = (!config.allow_no_job && !applicable_voting)
        || config
            .steps
            .iter()
            .filter(|step| step.voting && applicable_names.contains(step.name.as_str()))
            .any(|step| {
                results
                    .get(&step.name)
                    .is_none_or(|class| *class != StepResultClass::Success)
            });
    let warning = config
        .steps
        .iter()
        .filter(|step| !step.voting && applicable_names.contains(step.name.as_str()))
        .any(|step| {
            results
                .get(&step.name)
                .is_some_and(|class| *class != StepResultClass::Success)
        });
    let workspace_verification_error = if !voting_failed {
        verify_workspace(&execution.slot_root, &execution.tested_oid)
            .await
            .err()
            .map(|error| error.to_string())
    } else {
        None
    };
    let workspace_verified = !voting_failed && workspace_verification_error.is_none();
    Ok(BuildsetResult {
        passed: !voting_failed && workspace_verified,
        passed_with_warnings: !voting_failed && workspace_verified && warning,
        steps: ordered,
        skipped,
        workspace_verified,
        workspace_verification_error,
    })
}

fn verify_required_artifacts(step: &EffectiveStep, root: &Path) -> Result<(), RunnerError> {
    for artifact in step.artifacts.iter().filter(|artifact| artifact.required) {
        let mut builder = globset::GlobSetBuilder::new();
        for pattern in &artifact.patterns {
            builder.add(
                globset::Glob::new(pattern)
                    .map_err(|error| RunnerError::Interrupted(error.to_string()))?,
            );
        }
        let matcher = builder
            .build()
            .map_err(|error| RunnerError::Interrupted(error.to_string()))?;
        if !tree_contains_match(root, root, &matcher)? {
            return Err(RunnerError::WorkspaceDirty(format!(
                "required artifact `{}` matched no retained path",
                artifact.name
            )));
        }
    }
    Ok(())
}

fn tree_contains_match(
    root: &Path,
    directory: &Path,
    matcher: &globset::GlobSet,
) -> Result<bool, RunnerError> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|error| RunnerError::Interrupted(error.to_string()))?;
        if matcher.is_match(relative) {
            if file_type.is_symlink() {
                return Err(RunnerError::WorkspaceDirty(format!(
                    "artifact path `{}` is a symbolic link",
                    relative.display()
                )));
            }
            if file_type.is_file() {
                return Ok(true);
            }
        }
        if file_type.is_dir() && tree_contains_match(root, &path, matcher)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn verify_workspace(root: &Path, tested_oid: &GitOid) -> Result<(), RunnerError> {
    async fn git(root: &Path, args: &[&str]) -> Result<std::process::Output, RunnerError> {
        Ok(Command::new("git")
            .current_dir(root)
            .args(args)
            .output()
            .await?)
    }
    let head = git(root, &["rev-parse", "HEAD"]).await?;
    if !head.status.success() || String::from_utf8_lossy(&head.stdout).trim() != tested_oid.to_hex()
    {
        return Err(RunnerError::WorkspaceDirty("HEAD moved".into()));
    }
    for (args, reason) in [
        (
            &["diff-index", "--cached", "--quiet", "HEAD", "--"][..],
            "index differs from HEAD",
        ),
        (
            &["diff-files", "--quiet", "--"][..],
            "tracked worktree differs from index",
        ),
    ] {
        if !git(root, args).await?.status.success() {
            return Err(RunnerError::WorkspaceDirty(reason.into()));
        }
    }
    for marker in ["MERGE_HEAD", "CHERRY_PICK_HEAD", "REBASE_HEAD"] {
        if git(root, &["rev-parse", "--verify", "--quiet", marker])
            .await?
            .status
            .success()
        {
            return Err(RunnerError::WorkspaceDirty(format!(
                "Git operation in progress: {marker}"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentSnapshot {
    pub id: String,
    pub variables: Arc<BTreeMap<String, String>>,
    pub fingerprint: String,
}

impl EnvironmentSnapshot {
    pub fn capture() -> Self {
        let variables = std::env::vars().collect::<BTreeMap<_, _>>();
        Self::from_variables(variables)
    }

    pub async fn capture_login_shell() -> Result<Self, RunnerError> {
        const START: &[u8] = b"\x1eTOLLGATE_ENV_V1\0";
        const END: &[u8] = b"\x1eTOLLGATE_ENV_END\0";
        let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
            .map_err(|error| RunnerError::Environment(error.to_string()))?
            .ok_or_else(|| RunnerError::Environment("current user record is unavailable".into()))?;
        let shell_name = user
            .shell
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let mode = if matches!(shell_name, "zsh" | "bash" | "sh" | "ksh") {
            "-ilc"
        } else {
            "-lc"
        };
        let script =
            "printf '\\036TOLLGATE_ENV_V1\\0'; /usr/bin/env -0; printf '\\036TOLLGATE_ENV_END\\0'";
        let output = timeout(
            Duration::from_secs(15),
            Command::new(&user.shell)
                .arg(mode)
                .arg(script)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        )
        .await
        .map_err(|_| RunnerError::Environment("login shell timed out".into()))??;
        if !output.status.success() {
            return Err(RunnerError::Environment(
                String::from_utf8_lossy(&output.stderr).trim().into(),
            ));
        }
        let start = find_bytes(&output.stdout, START)
            .map(|index| index + START.len())
            .ok_or_else(|| RunnerError::Environment("environment start frame missing".into()))?;
        let end = find_bytes(&output.stdout[start..], END)
            .map(|index| start + index)
            .ok_or_else(|| RunnerError::Environment("environment end frame missing".into()))?;
        let mut variables = BTreeMap::new();
        for field in output.stdout[start..end]
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty())
        {
            let separator = field
                .iter()
                .position(|byte| *byte == b'=')
                .ok_or_else(|| RunnerError::Environment("environment entry omitted '='".into()))?;
            let name = std::str::from_utf8(&field[..separator])
                .map_err(|error| RunnerError::Environment(error.to_string()))?;
            let value = std::str::from_utf8(&field[separator + 1..])
                .map_err(|error| RunnerError::Environment(error.to_string()))?;
            variables.insert(name.to_owned(), value.to_owned());
        }
        if !variables.contains_key("PATH") {
            return Err(RunnerError::Environment(
                "login shell did not provide PATH".into(),
            ));
        }
        Ok(Self::from_variables(variables))
    }

    fn from_variables(variables: BTreeMap<String, String>) -> Self {
        let id = uuid_like_id();
        let mut hasher = Hasher::new();
        hasher.update(id.as_bytes());
        for (name, value) in &variables {
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(value.as_bytes());
            hasher.update(&[0xff]);
        }
        Self {
            id,
            variables: Arc::new(variables),
            fingerprint: hasher.finalize().to_hex().to_string(),
        }
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn uuid_like_id() -> String {
    format!("env-{}", OffsetDateTime::now_utc().unix_timestamp_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tollgate_config::EffectiveConfig;

    #[tokio::test]
    async fn captures_stdout_and_stderr_without_utf8_assumptions() {
        let temporary = tempfile::tempdir().unwrap();
        let config = EffectiveConfig::parse(
            "version=1\n[[step]]\nname=\"ci\"\nrun=\"printf out; printf err >&2\"\n",
        )
        .unwrap();
        let context = ExecutionContext {
            cwd: temporary.path().into(),
            environment: std::env::vars().collect(),
            read_only_context: BTreeMap::new(),
            runner: config.runner.clone(),
            log_path: temporary.path().join("step.tlog"),
        };
        let result = run_step(&config.steps[0], context, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.class, StepResultClass::Success);
        assert_eq!(result.log.stdout_end, 3);
        assert_eq!(result.log.stderr_end, 3);
        assert_ne!(result.log.hash, "");
    }

    #[tokio::test]
    async fn successful_voting_step_with_large_stderr_reports_checkout_attestation_failure() {
        let temporary = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["init", "-q"])
            .status()
            .unwrap();
        std::fs::write(temporary.path().join("tracked"), "original\n").unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["add", "tracked"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .args(["commit", "-qm", "base"])
            .status()
            .unwrap();
        let oid = std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let config = EffectiveConfig::parse(
            r#"version=1
[[step]]
name="review"
run='''
awk 'BEGIN { for (i = 0; i < 409600; i++) printf "x" }' >&2
printf 'restored asset\n' > tracked
'''
"#,
        )
        .unwrap();

        let result = run_buildset(
            &config,
            BuildsetExecution {
                tested_oid: GitOid::from_hex(String::from_utf8_lossy(&oid.stdout).trim()).unwrap(),
                slot_root: temporary.path().into(),
                log_directory: temporary.path().join("logs"),
                environment: std::env::vars().collect(),
                context: BTreeMap::new(),
            },
            &[],
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(!result.passed);
        assert!(!result.workspace_verified);
        assert_eq!(result.steps.len(), 1);
        assert_eq!(result.steps[0].1.class, StepResultClass::Success);
        assert!(result.steps[0].1.log.stderr_end >= 400_000);
        assert_eq!(
            result.workspace_verification_error.as_deref(),
            Some("workspace verification failed: tracked worktree differs from index")
        );
    }

    #[tokio::test]
    async fn captures_bounded_structured_diagnostics_without_scraping_logs() {
        let temporary = tempfile::tempdir().unwrap();
        let config = EffectiveConfig::parse(
            r#"version=1
[[step]]
name="ci"
run='''
printf '%s\n' '{"code":"generated-output-drift","message":"Generated report is stale","paths":["reports/current.csv"],"repair":{"kind":"argv","argv":["tool","generate"]}}' > "$TOLLGATE_DIAGNOSTICS_FILE"
exit 1
'''
"#,
        )
        .unwrap();
        let context = ExecutionContext {
            cwd: temporary.path().into(),
            environment: std::env::vars().collect(),
            read_only_context: BTreeMap::new(),
            runner: config.runner.clone(),
            log_path: temporary.path().join("diagnostic.tlog"),
        };
        let result = run_step(&config.steps[0], context, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.class, StepResultClass::ExitFailure);
        assert_eq!(result.diagnostics.len(), 1);
        assert_eq!(result.diagnostics[0].code, "generated-output-drift");
        assert_eq!(result.diagnostics[0].paths, vec!["reports/current.csv"]);
        assert_eq!(
            result.diagnostics[0].repair,
            Some(RepairCommand::Argv {
                argv: vec!["tool".into(), "generate".into()]
            })
        );
    }

    #[tokio::test]
    async fn malformed_diagnostic_contract_fails_a_successful_step() {
        let temporary = tempfile::tempdir().unwrap();
        let config = EffectiveConfig::parse(
            r#"version=1
[[step]]
name="ci"
run='''
printf '%s\n' '{"code":"BAD CODE","message":"unsafe","paths":["../escape"]}' > "$TOLLGATE_DIAGNOSTICS_FILE"
'''
"#,
        )
        .unwrap();
        let context = ExecutionContext {
            cwd: temporary.path().into(),
            environment: std::env::vars().collect(),
            read_only_context: BTreeMap::new(),
            runner: config.runner.clone(),
            log_path: temporary.path().join("invalid-diagnostic.tlog"),
        };
        let result = run_step(&config.steps[0], context, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.class, StepResultClass::ExitFailure);
        assert_eq!(result.diagnostics[0].code, "tollgate.diagnostics-invalid");
    }

    #[tokio::test]
    async fn durable_log_index_seeks_and_streaming_verification_matches_the_seal() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("indexed.tlog");
        let mut writer = DurableLogWriter::create(&path).await.unwrap();
        writer.append(LogStream::Stdout, b"one").await.unwrap();
        writer.append(LogStream::Stderr, b"two").await.unwrap();
        writer.append(LogStream::Stdout, b"three").await.unwrap();
        let seal = writer.seal().await.unwrap();

        let tail = read_durable_log(&path, 2, 10).await.unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].frame.broker_sequence, 2);
        assert_eq!(tail[0].text, "two");
        assert_eq!(tail[1].text, "three");
        assert!(
            verify_durable_log(&path, &seal.hash, seal.stdout_end, seal.stderr_end)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn timeout_is_a_conclusive_classification() {
        let temporary = tempfile::tempdir().unwrap();
        let config = EffectiveConfig::parse(
            "version=1\n[[step]]\nname=\"ci\"\nrun=\"sleep 2\"\ntimeout=\"1s\"\n",
        )
        .unwrap();
        let context = ExecutionContext {
            cwd: temporary.path().into(),
            environment: std::env::vars().collect(),
            read_only_context: BTreeMap::new(),
            runner: config.runner.clone(),
            log_path: temporary.path().join("timeout.tlog"),
        };
        let result = run_step(&config.steps[0], context, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.class, StepResultClass::Timeout);
    }

    #[cfg(unix)]
    #[test]
    fn shell_wrapped_external_control_signals_are_interruptions() {
        let config =
            EffectiveConfig::parse("version=1\n[[step]]\nname=\"ci\"\nrun=\"true\"\n").unwrap();
        let command = &config.steps[0].command;

        assert!(externally_terminated(command, Some(143), None));
        assert!(externally_terminated(command, Some(137), None));
        assert!(externally_terminated(command, None, Some(15)));
        assert!(!externally_terminated(command, Some(1), None));
        assert!(!externally_terminated(command, Some(139), None));
    }

    #[test]
    fn containment_failure_has_structured_diagnostic_evidence() {
        let diagnostics = containment_diagnostics(Some(
            "Tollgate rejected an unsupported session escape by descendant PID 42.".into(),
        ));

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "tollgate.containment-escape");
        assert!(diagnostics[0].message.contains("PID 42"));
        assert!(diagnostics[0].paths.is_empty());
        assert!(diagnostics[0].repair.is_none());
    }

    #[tokio::test]
    async fn forward_dependencies_run_before_their_dependents() {
        let temporary = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["init", "-q"])
            .status()
            .unwrap();
        std::fs::write(temporary.path().join("marker"), "base").unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["add", "."])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .args(["commit", "-qm", "base"])
            .status()
            .unwrap();
        let oid = std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let config = EffectiveConfig::parse("version=1\n[[step]]\nname=\"gate\"\nrun=\"test -f ready\"\nneeds=[\"setup\"]\n[[step]]\nname=\"setup\"\nrun=\"touch ready\"\n").unwrap();
        let result = run_buildset(
            &config,
            BuildsetExecution {
                tested_oid: GitOid::from_hex(String::from_utf8_lossy(&oid.stdout).trim()).unwrap(),
                slot_root: temporary.path().into(),
                log_directory: temporary.path().join("logs"),
                environment: std::env::vars().collect(),
                context: BTreeMap::new(),
            },
            &[],
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(result.passed);
        assert_eq!(
            result
                .steps
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["setup", "gate"]
        );
    }

    #[tokio::test]
    async fn explicitly_unconnected_roots_can_run_concurrently() {
        let temporary = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["init", "-q"])
            .status()
            .unwrap();
        std::fs::write(temporary.path().join("marker"), "base").unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["add", "."])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .args(["commit", "-qm", "base"])
            .status()
            .unwrap();
        let oid = std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let config = EffectiveConfig::parse(
            r#"version=1
allow_concurrent_roots=true
[[step]]
name="a"
run="touch a.started; for n in 1 2 3 4 5 6 7 8 9 10; do test -f b.started && exit 0; sleep .05; done; exit 1"
voting=false
[[step]]
name="b"
run="touch b.started; for n in 1 2 3 4 5 6 7 8 9 10; do test -f a.started && exit 0; sleep .05; done; exit 1"
voting=false
[[step]]
name="gate"
run="true"
needs=["a","b"]
"#,
        )
        .unwrap();
        let result = run_buildset(
            &config,
            BuildsetExecution {
                tested_oid: GitOid::from_hex(String::from_utf8_lossy(&oid.stdout).trim()).unwrap(),
                slot_root: temporary.path().into(),
                log_directory: temporary.path().join("logs"),
                environment: std::env::vars().collect(),
                context: BTreeMap::new(),
            },
            &[],
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(result.passed);
        assert!(
            result
                .steps
                .iter()
                .all(|(_, step)| step.class == StepResultClass::Success)
        );
    }

    #[tokio::test]
    async fn missing_required_artifact_fails_the_buildset() {
        let temporary = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["init", "-q"])
            .status()
            .unwrap();
        std::fs::write(temporary.path().join("marker"), "base").unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["add", "."])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temporary.path())
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .args(["commit", "-qm", "base"])
            .status()
            .unwrap();
        let oid = std::process::Command::new("git")
            .current_dir(temporary.path())
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let config = EffectiveConfig::parse("version=1\n[[step]]\nname=\"gate\"\nrun=\"true\"\n[[step.artifact]]\nname=\"report\"\npatterns=[\"reports/*.xml\"]\nrequired=true\n").unwrap();
        let result = run_buildset(
            &config,
            BuildsetExecution {
                tested_oid: GitOid::from_hex(String::from_utf8_lossy(&oid.stdout).trim()).unwrap(),
                slot_root: temporary.path().into(),
                log_directory: temporary.path().join("logs"),
                environment: std::env::vars().collect(),
                context: BTreeMap::new(),
            },
            &[],
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!result.passed);
    }

    #[tokio::test]
    async fn filtered_voting_steps_do_not_authorize_a_no_job_pass() {
        let temporary = tempfile::tempdir().unwrap();
        let config = EffectiveConfig::parse(
            "version=1\n[[step]]\nname=\"gate\"\nrun=\"true\"\ninclude=[\"src/**\"]\n",
        )
        .unwrap();
        let result = run_buildset(
            &config,
            BuildsetExecution {
                tested_oid: GitOid::from_hex("0000000000000000000000000000000000000000").unwrap(),
                slot_root: temporary.path().into(),
                log_directory: temporary.path().join("logs"),
                environment: std::env::vars().collect(),
                context: BTreeMap::new(),
            },
            &["docs/readme.md".into()],
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!result.passed);
    }

    #[tokio::test]
    async fn finalizer_cannot_remove_a_required_retained_artifact() {
        let temporary = tempfile::tempdir().unwrap();
        let config = EffectiveConfig::parse(
            "version=1\n[[step]]\nname=\"gate\"\nrun=\"touch report\"\n[[step.artifact]]\nname=\"report\"\npatterns=[\"report\"]\nrequired=true\n[[step]]\nname=\"cleanup\"\nrun=\"rm report\"\nfinal=true\n",
        )
        .unwrap();
        let result = run_buildset(
            &config,
            BuildsetExecution {
                tested_oid: GitOid::from_hex("0000000000000000000000000000000000000000").unwrap(),
                slot_root: temporary.path().into(),
                log_directory: temporary.path().join("logs"),
                environment: std::env::vars().collect(),
                context: BTreeMap::new(),
            },
            &[],
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!result.passed);
    }

    #[cfg(unix)]
    #[test]
    fn required_artifact_rejects_symbolic_links() {
        let temporary = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("/etc/passwd", temporary.path().join("report")).unwrap();
        let config = EffectiveConfig::parse("version=1\n[[step]]\nname=\"gate\"\nrun=\"true\"\n[[step.artifact]]\nname=\"report\"\npatterns=[\"report\"]\nrequired=true\n").unwrap();
        assert!(verify_required_artifacts(&config.steps[0], temporary.path()).is_err());
    }
}
