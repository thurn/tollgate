#![forbid(unsafe_code)]

#[cfg(target_os = "macos")]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::OpenOptions as StdOpenOptions,
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(about = "Ephemeral Tollgate command supervisor")]
struct Args {
    #[arg(long)]
    nonce: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Input {
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

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Output {
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
        stream: &'static str,
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
        containment_diagnostic: Option<String>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let stdin = tokio::io::stdin();
    let mut input = BufReader::new(stdin).lines();
    let mut output = tokio::io::stdout();
    let spec_line = input
        .next_line()
        .await?
        .ok_or_else(|| anyhow!("lifetime channel closed before spec"))?;
    let Input::Spec {
        nonce,
        cwd,
        argv,
        environment,
        timeout_ms,
        termination_grace_ms,
        rss_limit_bytes,
    } = serde_json::from_str(&spec_line)?
    else {
        return Err(anyhow!("first worker frame must be spec"));
    };
    if nonce != args.nonce || argv.is_empty() {
        return Err(anyhow!("worker nonce mismatch or empty argv"));
    }
    send(
        &mut output,
        &Output::Registered {
            nonce: nonce.clone(),
            worker_pid: std::process::id(),
        },
    )
    .await?;
    let start_line = input
        .next_line()
        .await?
        .ok_or_else(|| anyhow!("lifetime channel closed before start gate"))?;
    match serde_json::from_str::<Input>(&start_line)? {
        Input::Start { nonce: start_nonce } if start_nonce == nonce => {}
        _ => return Err(anyhow!("invalid start gate frame")),
    }

    let output_root = std::env::temp_dir().join(format!(
        "tollgate-worker-output-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir(&output_root)?;
    let mut stdout_path = output_root.join("stdout.fifo");
    let mut stderr_path = output_root.join("stderr.fifo");
    #[cfg(unix)]
    {
        use nix::{sys::stat::Mode, unistd::mkfifo};
        mkfifo(&stdout_path, Mode::S_IRUSR | Mode::S_IWUSR)?;
        mkfifo(&stderr_path, Mode::S_IRUSR | Mode::S_IWUSR)?;
    }
    stdout_path = std::fs::canonicalize(stdout_path)?;
    stderr_path = std::fs::canonicalize(stderr_path)?;
    let open_reader = |path: &PathBuf| {
        let mut options = StdOpenOptions::new();
        options.read(true);
        #[cfg(target_os = "macos")]
        options.custom_flags(nix::libc::O_NONBLOCK);
        options.open(path)
    };
    let stdout_reader = open_reader(&stdout_path)?;
    let stderr_reader = open_reader(&stderr_path)?;
    let stdout_writer = StdOpenOptions::new().write(true).open(&stdout_path)?;
    let stderr_writer = StdOpenOptions::new().write(true).open(&stderr_path)?;

    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .current_dir(cwd)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::from(stderr_writer))
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().context("spawn supervised command")?;
    let child_pid = child
        .id()
        .ok_or_else(|| anyhow!("spawned command has no process ID"))?;
    if let Err(error) = send(
        &mut output,
        &Output::Started {
            nonce: nonce.clone(),
            child_pid,
            process_group_id: child_pid,
        },
    )
    .await
    {
        terminate(child_pid, &mut child, 0).await;
        let _ = child.wait().await;
        return Err(error.context("lifetime channel closed after child spawn"));
    }
    let (sender, mut receiver) = mpsc::channel(64);
    let output_done = CancellationToken::new();
    let stdout_task = tokio::spawn(read_output(
        tokio::fs::File::from_std(stdout_reader),
        "stdout",
        sender.clone(),
        output_done.clone(),
    ));
    let stderr_task = tokio::spawn(read_output(
        tokio::fs::File::from_std(stderr_reader),
        "stderr",
        sender,
        output_done.clone(),
    ));
    let deadline = tokio::time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(deadline);
    let mut timed_out = false;
    let mut canceled = false;
    let mut rss_exceeded = false;
    let mut containment_escaped = false;
    let mut containment_diagnostic = None;
    let mut observed_descendants = HashSet::new();
    let mut owned_process_groups = HashSet::new();
    let mut root_identity = None;
    let rss_interval = tokio::time::interval(Duration::from_millis(100));
    tokio::pin!(rss_interval);
    let containment_interval = tokio::time::interval(Duration::from_millis(20));
    tokio::pin!(containment_interval);
    let status = loop {
        tokio::select! {
            status = child.wait() => break status?,
            _ = &mut deadline => { timed_out = true; terminate(child_pid, &mut child, termination_grace_ms).await; break child.wait().await?; },
            control = input.next_line() => {
                match control? {
                    None => { canceled = true; terminate(child_pid, &mut child, termination_grace_ms).await; break child.wait().await?; }
                    Some(line) => if matches!(serde_json::from_str::<Input>(&line), Ok(Input::Terminate { nonce: value }) if value == nonce) { canceled = true; terminate(child_pid, &mut child, termination_grace_ms).await; break child.wait().await?; },
                }
            }
            _ = rss_interval.tick(), if rss_limit_bytes.is_some() => {
                let limit = rss_limit_bytes.expect("RSS polling is enabled only with a limit");
                if root_identity.is_none() {
                    root_identity = process_identity(child_pid).await;
                }
                let rss_bytes = match root_identity.as_ref() {
                    Some(root) => process_tree_rss_bytes(root, &observed_descendants).await,
                    None => process_group_rss_bytes(child_pid).await,
                };
                if rss_bytes.is_some_and(|rss| rss > limit) {
                    rss_exceeded = true;
                    let diagnostic = format!("Tollgate terminated this step because its process tree exceeded the configured RSS hard limit of {limit} bytes.\n");
                    if send(&mut output, &Output::Log { nonce: nonce.clone(), stream: "stderr", payload_base64: STANDARD.encode(diagnostic) }).await.is_err() {
                        terminate(child_pid, &mut child, 0).await;
                        let _ = child.wait().await;
                        return Err(anyhow!("lifetime channel closed while reporting RSS limit"));
                    }
                    terminate(child_pid, &mut child, termination_grace_ms).await;
                    break child.wait().await?;
                }
            }
            _ = containment_interval.tick() => {
                if let Some(processes) = process_table().await {
                    if root_identity.is_none() {
                        root_identity = processes
                            .iter()
                            .find(|process| process.pid == child_pid)
                            .cloned();
                    }
                    let Some(root) = root_identity.as_ref() else {
                        continue;
                    };
                    let descendants =
                        descendants_of(&root.instance(), &processes, &observed_descendants);
                    observed_descendants
                        .extend(descendants.iter().map(ProcessIdentity::instance));
                    record_owned_process_groups(root, &descendants, &mut owned_process_groups);
                    if let Some((escaped, diagnostic)) = descendants.iter().find_map(|process| {
                        containment_violation(root, child_pid, process, &owned_process_groups)
                            .map(|diagnostic| (process, diagnostic))
                    })
                    {
                        containment_escaped = true;
                        terminate_descendant(root, escaped, &owned_process_groups);
                        containment_diagnostic = Some(diagnostic.clone());
                        let diagnostic = format!("{diagnostic}\n");
                        let _ = send(&mut output, &Output::Log { nonce: nonce.clone(), stream: "stderr", payload_base64: STANDARD.encode(diagnostic) }).await;
                        terminate(child_pid, &mut child, termination_grace_ms).await;
                        break child.wait().await?;
                    }
                }
            }
            frame = receiver.recv() => if let Some((stream, bytes)) = frame
                && let Err(error) = send(&mut output, &Output::Log { nonce: nonce.clone(), stream, payload_base64: STANDARD.encode(bytes) }).await
            {
                terminate(child_pid, &mut child, termination_grace_ms).await;
                let _ = child.wait().await;
                return Err(error.context("lifetime channel closed while streaming output"));
            }
        }
    };
    if !group_is_empty(child_pid) {
        terminate(child_pid, &mut child, termination_grace_ms).await;
    }
    if let Some(root) = root_identity.as_ref()
        && let Some(processes) = process_table().await
    {
        let descendants = descendants_of(&root.instance(), &processes, &observed_descendants);
        record_owned_process_groups(root, &descendants, &mut owned_process_groups);
        for process in descendants
            .iter()
            .filter(|process| process.process_group != child_pid)
        {
            let violation = containment_violation(root, child_pid, process, &owned_process_groups);
            terminate_descendant(root, process, &owned_process_groups);
            if !timed_out
                && !canceled
                && let Some(diagnostic) = violation
            {
                containment_escaped = true;
                containment_diagnostic.get_or_insert(diagnostic);
            }
        }
    }
    let ordinary_drain_deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    while tokio::time::Instant::now() < ordinary_drain_deadline
        && (!stdout_task.is_finished() || !stderr_task.is_finished())
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    #[cfg(target_os = "macos")]
    if !stdout_task.is_finished() || !stderr_task.is_finished() {
        let pipe_paths = [stdout_path.clone(), stderr_path.clone()];
        let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while tokio::time::Instant::now() < cleanup_deadline {
            let escaped = terminate_named_pipe_holders(&pipe_paths).await;
            if escaped && !timed_out && !canceled {
                containment_escaped = true;
                containment_diagnostic.get_or_insert_with(|| {
                    "Tollgate rejected a supervised descendant that outlived the root command and retained its output pipe.".into()
                });
            }
            if !escaped {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    output_done.cancel();
    loop {
        tokio::select! {
            frame = receiver.recv() => match frame {
                Some((stream, bytes)) => if let Err(error) = send(
                    &mut output,
                    &Output::Log {
                        nonce: nonce.clone(),
                        stream,
                        payload_base64: STANDARD.encode(bytes),
                    },
                ).await {
                    terminate(child_pid, &mut child, 0).await;
                    let _ = child.wait().await;
                    return Err(error.context("lifetime channel closed while draining output"));
                },
                None => break,
            },
            _ = containment_interval.tick() => {
                if let Some(processes) = process_table().await {
                    if root_identity.is_none() {
                        root_identity = processes
                            .iter()
                            .find(|process| process.pid == child_pid)
                            .cloned();
                    }
                    let Some(root) = root_identity.as_ref() else {
                        continue;
                    };
                    let descendants =
                        descendants_of(&root.instance(), &processes, &observed_descendants);
                    observed_descendants
                        .extend(descendants.iter().map(ProcessIdentity::instance));
                    record_owned_process_groups(root, &descendants, &mut owned_process_groups);
                    for descendant in descendants.iter().filter(|process| process.process_group != child_pid) {
                        let violation = containment_violation(
                            root,
                            child_pid,
                            descendant,
                            &owned_process_groups,
                        );
                        terminate_descendant(root, descendant, &owned_process_groups);
                        if !timed_out && !canceled
                            && let Some(diagnostic) = violation
                        {
                            containment_escaped = true;
                            containment_diagnostic.get_or_insert(diagnostic);
                        }
                    }
                }
            }
        }
    }
    let pipes_eof = stdout_task.await.is_ok_and(|value| value.is_ok())
        && stderr_task.await.is_ok_and(|value| value.is_ok());
    let _ = std::fs::remove_file(&stdout_path);
    let _ = std::fs::remove_file(&stderr_path);
    let _ = std::fs::remove_dir(&output_root);
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    let process_group_reaped = group_is_empty(child_pid)
        && match root_identity.as_ref() {
            Some(root) => {
                wait_for_supervised_descendants_to_exit(root, &observed_descendants).await
            }
            None => true,
        };
    send(
        &mut output,
        &Output::Terminal {
            nonce,
            exit_code: status.code(),
            signal,
            timed_out,
            canceled,
            pipes_eof,
            process_group_reaped,
            rss_exceeded,
            containment_escaped,
            containment_diagnostic,
        },
    )
    .await?;
    Ok(())
}

#[cfg(target_os = "macos")]
async fn terminate_named_pipe_holders(paths: &[PathBuf]) -> bool {
    let mut command = Command::new("/usr/sbin/lsof");
    command
        .args(["-n", "-P", "-Fpn"])
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    let Ok(output) = command.output().await else {
        return false;
    };
    let mut current_pid = None;
    let mut holders = HashSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(pid) = line.strip_prefix('p').and_then(|value| value.parse().ok()) {
            current_pid = Some(pid);
        } else if let Some(name) = line.strip_prefix('n')
            && paths.iter().any(|path| path.as_os_str() == name)
            && current_pid != Some(std::process::id())
            && let Some(pid) = current_pid
        {
            holders.insert(pid);
        }
    }
    let processes = process_table().await.unwrap_or_default();
    let escaped = !holders.is_empty();
    for pid in holders {
        let process_group = processes
            .iter()
            .find(|process| process.pid == pid)
            .map_or(pid, |process| process.process_group);
        terminate_process_group(pid, process_group);
    }
    escaped
}

#[derive(Clone, Debug)]
struct ProcessIdentity {
    pid: u32,
    parent: u32,
    process_group: u32,
    session_id: u32,
    rss_bytes: u64,
    started_at: String,
}

impl ProcessIdentity {
    fn instance(&self) -> ProcessInstance {
        ProcessInstance {
            pid: self.pid,
            started_at: self.started_at.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProcessInstance {
    pid: u32,
    started_at: String,
}

async fn process_table() -> Option<Vec<ProcessIdentity>> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("/bin/ps")
            .args(["-axo", "pid=,ppid=,pgid=,rss=,lstart="])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        Some(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| {
                    let mut fields = line.split_whitespace();
                    let pid = fields.next()?.parse().ok()?;
                    let parent = fields.next()?.parse().ok()?;
                    let process_group = fields.next()?.parse().ok()?;
                    let rss_bytes = fields.next()?.parse::<u64>().ok()?.saturating_mul(1024);
                    let session_id = session_id(pid)?;
                    let started_at = fields.collect::<Vec<_>>().join(" ");
                    if started_at.is_empty() {
                        return None;
                    }
                    Some(ProcessIdentity {
                        pid,
                        parent,
                        process_group,
                        session_id,
                        rss_bytes,
                        started_at,
                    })
                })
                .collect(),
        )
    }
    #[cfg(not(target_os = "macos"))]
    None
}

fn session_id(pid: u32) -> Option<u32> {
    #[cfg(unix)]
    {
        use nix::unistd::{Pid, getsid};
        let pid = i32::try_from(pid).ok()?;
        u32::try_from(getsid(Some(Pid::from_raw(pid))).ok()?.as_raw()).ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        None
    }
}

async fn process_identity(pid: u32) -> Option<ProcessIdentity> {
    process_table()
        .await?
        .into_iter()
        .find(|process| process.pid == pid)
}

fn descendants_of(
    root: &ProcessInstance,
    processes: &[ProcessIdentity],
    previously_observed: &HashSet<ProcessInstance>,
) -> Vec<ProcessIdentity> {
    let mut identities = previously_observed.clone();
    identities.insert(root.clone());
    let current_instances = processes
        .iter()
        .map(|process| (process.pid, process.instance()))
        .collect::<HashMap<_, _>>();
    loop {
        let before = identities.len();
        for process in processes {
            if current_instances
                .get(&process.parent)
                .is_some_and(|parent| identities.contains(parent))
            {
                identities.insert(process.instance());
            }
        }
        if identities.len() == before {
            break;
        }
    }
    processes
        .iter()
        .filter(|process| {
            let instance = process.instance();
            &instance != root && identities.contains(&instance)
        })
        .cloned()
        .collect()
}

fn record_owned_process_groups(
    root: &ProcessIdentity,
    descendants: &[ProcessIdentity],
    owned_process_groups: &mut HashSet<u32>,
) {
    owned_process_groups.retain(|process_group| {
        descendants
            .iter()
            .any(|process| process.process_group == *process_group)
    });
    owned_process_groups.extend(
        descendants
            .iter()
            .filter(|process| {
                process.session_id == root.session_id && process.pid == process.process_group
            })
            .map(|process| process.process_group),
    );
}

fn containment_violation(
    root: &ProcessIdentity,
    root_process_group: u32,
    descendant: &ProcessIdentity,
    owned_process_groups: &HashSet<u32>,
) -> Option<String> {
    if descendant.session_id != root.session_id {
        return Some(format!(
            "Tollgate rejected an unsupported session escape by descendant PID {}.",
            descendant.pid
        ));
    }
    if descendant.process_group != root_process_group
        && !owned_process_groups.contains(&descendant.process_group)
    {
        return Some(format!(
            "Tollgate rejected descendant PID {} joining unrelated process group {}.",
            descendant.pid, descendant.process_group
        ));
    }
    None
}

fn terminate_descendant(
    root: &ProcessIdentity,
    descendant: &ProcessIdentity,
    owned_process_groups: &HashSet<u32>,
) {
    if descendant.session_id != root.session_id
        || owned_process_groups.contains(&descendant.process_group)
    {
        terminate_process_group(descendant.pid, descendant.process_group);
    } else {
        terminate_process(descendant.pid);
    }
}

fn terminate_process(pid: u32) {
    #[cfg(unix)]
    {
        use nix::{
            sys::signal::{Signal, kill},
            unistd::Pid,
        };
        if let Ok(pid) = i32::try_from(pid) {
            let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }
}

fn terminate_process_group(pid: u32, process_group: u32) {
    #[cfg(unix)]
    {
        use nix::{
            sys::signal::{Signal, killpg},
            unistd::Pid,
        };
        terminate_process(pid);
        if let Ok(process_group) = i32::try_from(process_group) {
            let _ = killpg(Pid::from_raw(process_group), Signal::SIGKILL);
        }
    }
}

async fn wait_for_supervised_descendants_to_exit(
    root: &ProcessIdentity,
    observed_descendants: &HashSet<ProcessInstance>,
) -> bool {
    #[cfg(target_os = "macos")]
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while tokio::time::Instant::now() < deadline {
            if process_table().await.is_some_and(|processes| {
                descendants_of(&root.instance(), &processes, observed_descendants).is_empty()
            }) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (root, observed_descendants);
        true
    }
}

async fn process_group_rss_bytes(process_group: u32) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("/bin/ps")
            .args(["-o", "rss=", "-g", &process_group.to_string()])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .try_fold(0u64, |total, line| {
                line.trim()
                    .parse::<u64>()
                    .ok()
                    .map(|kib| total.saturating_add(kib.saturating_mul(1024)))
            })
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = process_group;
        None
    }
}

async fn process_tree_rss_bytes(
    root: &ProcessIdentity,
    previously_observed: &HashSet<ProcessInstance>,
) -> Option<u64> {
    let processes = process_table().await?;
    let current_root = processes
        .iter()
        .find(|process| process.instance() == root.instance())?;
    let descendants = descendants_of(&root.instance(), &processes, previously_observed);
    Some(
        descendants
            .iter()
            .fold(current_root.rss_bytes, |total, process| {
                total.saturating_add(process.rss_bytes)
            }),
    )
}

async fn read_output(
    mut reader: tokio::fs::File,
    stream: &'static str,
    sender: mpsc::Sender<(&'static str, Vec<u8>)>,
    done: CancellationToken,
) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; 32 * 1024];
    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if done.is_cancelled() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        sender
            .send((stream, buffer[..read].to_vec()))
            .await
            .map_err(|_| anyhow!("worker output broker closed"))?;
    }
    Ok(())
}

async fn send(output: &mut tokio::io::Stdout, value: &Output) -> anyhow::Result<()> {
    output.write_all(&serde_json::to_vec(value)?).await?;
    output.write_all(b"\n").await?;
    output.flush().await?;
    Ok(())
}

async fn terminate(process_group: u32, child: &mut tokio::process::Child, grace_ms: u64) {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(process_group) {
        use nix::{
            sys::signal::{Signal, killpg},
            unistd::Pid,
        };
        let _ = killpg(Pid::from_raw(pid), Signal::SIGTERM);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(grace_ms);
        while tokio::time::Instant::now() < deadline {
            if child.try_wait().ok().flatten().is_some() {
                break;
            }
            if group_is_empty(process_group) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        let kill_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while tokio::time::Instant::now() < kill_deadline && !group_is_empty(process_group) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        return;
    }
    let _ = child.kill().await;
}

fn group_is_empty(process_group: u32) -> bool {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(process_group) {
        use nix::{errno::Errno, sys::signal::killpg, unistd::Pid};
        return matches!(killpg(Pid::from_raw(pid), None), Err(Errno::ESRCH));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process(
        pid: u32,
        parent: u32,
        process_group: u32,
        session_id: u32,
        started_at: &str,
    ) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            parent,
            process_group,
            session_id,
            rss_bytes: 1,
            started_at: started_at.into(),
        }
    }

    #[test]
    fn descendant_tracking_ignores_a_reused_observed_pid() {
        let root = process(10, 1, 10, 1, "root");
        let old_child = process(20, 10, 10, 1, "old-child");
        let observed = HashSet::from([old_child.instance()]);
        let reused_pid = process(20, 1, 20, 2, "unrelated-new-process");

        assert!(descendants_of(&root.instance(), &[root, reused_pid], &observed).is_empty());
    }

    #[test]
    fn descendant_tracking_retains_a_reparented_process_instance() {
        let root = process(10, 1, 10, 1, "root");
        let child = process(20, 10, 10, 1, "child");
        let descendants = descendants_of(
            &root.instance(),
            &[root.clone(), child.clone()],
            &HashSet::new(),
        );
        let observed = descendants
            .iter()
            .map(ProcessIdentity::instance)
            .collect::<HashSet<_>>();
        let escaped = process(20, 1, 20, 1, "child");

        let descendants = descendants_of(&root.instance(), &[escaped], &observed);
        assert_eq!(descendants.len(), 1);
        assert_eq!(descendants[0].pid, 20);
        assert_eq!(descendants[0].process_group, 20);
    }

    #[test]
    fn process_tree_rss_includes_subordinate_process_groups() {
        let root = process(10, 1, 10, 1, "root");
        let mut subgroup = process(20, 10, 20, 1, "subgroup");
        subgroup.rss_bytes = 7;
        let mut member = process(21, 20, 20, 1, "member");
        member.rss_bytes = 11;
        let descendants =
            descendants_of(&root.instance(), &[root, subgroup, member], &HashSet::new());

        assert_eq!(
            descendants
                .iter()
                .map(|process| process.rss_bytes)
                .sum::<u64>(),
            18
        );
    }

    #[test]
    fn only_same_session_descendant_group_leaders_claim_owned_groups() {
        let root = process(10, 1, 10, 1, "root");
        let subgroup = process(20, 10, 20, 1, "subgroup");
        let subgroup_member = process(21, 20, 20, 1, "member");
        let session_escape = process(30, 10, 30, 30, "session-escape");
        let mut owned = HashSet::new();

        record_owned_process_groups(
            &root,
            &[subgroup, subgroup_member, session_escape],
            &mut owned,
        );

        assert_eq!(owned, HashSet::from([20]));

        record_owned_process_groups(&root, &[], &mut owned);
        assert!(owned.is_empty());
    }
}
