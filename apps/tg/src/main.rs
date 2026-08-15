use std::{path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, anyhow};
use clap::{Args, Parser, Subcommand};
use directories::ProjectDirs;
use futures::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio_util::codec::Framed;
use tollgate_domain::{BuildsetId, CommandId, QueueItemId, RepositoryId};
use tollgate_git::{GitRepository, USER_BRANCH, USER_BRANCH_REF};
use tollgate_ipc::{
    Frame, FrameCodec, FrameKind, Handshake, HandshakeAck, IpcCommand, IpcResponse,
    MAX_CONTROL_PAYLOAD, MAX_LOG_PAYLOAD, PROTOCOL_VERSION, StructuredError, verify_peer_uid,
};
use tollgate_service::{AppSnapshot, DiagnoseResult, QueueItemView, RepositorySnapshot};
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "tg",
    version,
    about = "Tollgate local dependent gate",
    max_term_width = 100
)]
struct Cli {
    #[arg(long, global = true, help = "Return stable versioned JSON")]
    json: bool,
    #[arg(
        long,
        global = true,
        help = "Do not launch the Tollgate app when unavailable"
    )]
    no_launch: bool,
    #[arg(
        long,
        global = true,
        value_name = "ID",
        help = "Select a registered repository"
    )]
    repository: Option<RepositoryId>,
    #[command(subcommand)]
    command: TopCommand,
}

#[derive(Subcommand)]
enum TopCommand {
    Init(InitArgs),
    #[command(subcommand)]
    Repo(RepoCommand),
    Candidate(RevisionArgs),
    Approve(RevisionArgs),
    PushMaster(PushMasterArgs),
    Queue,
    Status {
        id: Option<QueueItemId>,
    },
    Wait {
        id: QueueItemId,
    },
    Logs {
        id: QueueItemId,
        #[arg(long, help = "Read an exact retained buildset attempt")]
        buildset: Option<BuildsetId>,
        #[arg(long)]
        step: Option<String>,
        #[arg(long)]
        follow: bool,
    },
    Cancel {
        id: QueueItemId,
    },
    Retry {
        id: QueueItemId,
        #[arg(long)]
        cold: bool,
    },
    Reorder {
        ids: Vec<QueueItemId>,
    },
    Check(RevisionArgs),
    Diagnose(DiagnoseArgs),
    Pause,
    Resume,
    Pull,
    Push,
    Reconcile,
    Update,
    #[command(subcommand)]
    Worktree(WorktreeCommand),
    #[command(subcommand)]
    Env(EnvCommand),
    #[command(subcommand)]
    Config(ConfigCommand),
    #[command(subcommand)]
    Slot(SlotCommand),
    #[command(subcommand)]
    Cache(CacheCommand),
    #[command(subcommand)]
    Artifact(ArtifactCommand),
    History,
    Doctor,
}

#[derive(Args)]
struct InitArgs {
    #[arg(default_value = ".")]
    path: PathBuf,
    #[arg(long, value_name = "COMMAND")]
    run: Option<String>,
    #[arg(long)]
    no_bootstrap: bool,
    /// Legacy no-op retained for command-line compatibility.
    #[arg(long, hide = true)]
    detach_master: bool,
}

#[derive(Args)]
struct RevisionArgs {
    #[arg(
        default_value = "HEAD",
        help = "Git revision; `tg approve` also accepts an existing candidate ID"
    )]
    revision: String,
    #[arg(long, help = "Wait for validation or promotion to finish")]
    wait: bool,
}

#[derive(Args)]
struct DiagnoseArgs {
    id: QueueItemId,
    #[arg(long, help = "Use retained evidence without scheduling clean replays")]
    no_replay: bool,
    #[arg(
        long,
        help = "Execute a structured repair in a disposable clone and retain a verified patch"
    )]
    verify_repair: bool,
}

#[derive(Args)]
struct PushMasterArgs {
    #[arg(long, help = "Wait for validation and remote synchronization")]
    wait: bool,
    #[arg(
        long,
        conflicts_with = "wait",
        help = "Report the latest master push without scheduling work"
    )]
    status: bool,
}

#[derive(Subcommand)]
enum RepoCommand {
    Add {
        path: PathBuf,
        #[arg(long, hide = true)]
        detach_master: bool,
    },
    Remove {
        id: RepositoryId,
    },
    List,
}
#[derive(Subcommand)]
enum WorktreeCommand {
    Create { name: String },
    Remove { path: PathBuf },
}
#[derive(Subcommand)]
enum EnvCommand {
    Reload,
    Show,
}
#[derive(Subcommand)]
enum ConfigCommand {
    Validate,
    Explain,
    Regenerate,
    Apply,
}
#[derive(Subcommand)]
enum SlotCommand {
    List,
    Reset { id: String },
}
#[derive(Subcommand)]
enum CacheCommand {
    Status,
    Snapshot,
    Purge {
        #[arg(long)]
        all_slots: bool,
    },
}
#[derive(Subcommand)]
enum ArtifactCommand {
    List,
    Pin { id: String },
    Unpin { id: String },
    Prune { id: String },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let json = cli.json;
    match run(cli).await {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            if json {
                if let Some(error) = error.downcast_ref::<IpcRequestError>() {
                    eprintln!("{}", serde_json::json!({"ok": false, "error": error.0}));
                } else {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "ok": false,
                            "error": {
                                "code": "cli-error",
                                "message": format!("{error:#}"),
                                "retryable": false
                            }
                        })
                    );
                }
            } else {
                eprintln!("tg: {error:#}");
            }
            ExitCode::from(classify_exit(&error))
        }
    }
}

#[derive(Debug)]
struct IpcRequestError(StructuredError);

impl std::fmt::Display for IpcRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl std::error::Error for IpcRequestError {}

async fn run(cli: Cli) -> anyhow::Result<u8> {
    let mut client = IpcClient::connect(cli.no_launch).await?;
    match cli.command {
        TopCommand::Init(args) => {
            let path = std::fs::canonicalize(&args.path)
                .with_context(|| format!("cannot open {}", args.path.display()))?;
            let response = client
                .request(IpcCommand::Initialize {
                    path: path.to_string_lossy().into_owned(),
                    run: args.run,
                    bootstrap: !args.no_bootstrap,
                    detach_master: args.detach_master,
                })
                .await?;
            print_value(response, cli.json, |value| {
                let repository: RepositorySnapshot = serde_json::from_value(value.clone())?;
                println!(
                    "Initialized {}\n  repository  {}\n  release     {}\n  config      {}",
                    repository.state.name,
                    repository.state.id,
                    repository.state.master_oid.short(),
                    &repository.configuration.digest[..12]
                );
                if repository.state.execution_state
                    == tollgate_domain::RepositoryExecutionState::Blocked
                {
                    println!(
                        "\nGate needs attention: {}",
                        repository
                            .state
                            .block_reasons
                            .first()
                            .map(|reason| reason.message.as_str())
                            .unwrap_or("blocked")
                    );
                }
                Ok(())
            })?;
        }
        TopCommand::Repo(RepoCommand::Add {
            path,
            detach_master,
        }) => {
            let path = std::fs::canonicalize(path)?;
            let response = client
                .request(IpcCommand::Initialize {
                    path: path.to_string_lossy().into_owned(),
                    run: None,
                    bootstrap: true,
                    detach_master,
                })
                .await?;
            print_value(response, cli.json, |_| {
                println!("Repository registered.");
                Ok(())
            })?;
        }
        TopCommand::Repo(RepoCommand::List) => {
            let snapshot = client.snapshot().await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&snapshot.repositories)?);
            } else if snapshot.repositories.is_empty() {
                println!("No registered repositories.");
            } else {
                for repository in snapshot.repositories {
                    println!(
                        "{:<20} {}  {}",
                        repository.state.name, repository.state.id, repository.state.path
                    );
                }
            }
        }
        TopCommand::Repo(RepoCommand::Remove { id }) => {
            client
                .request(IpcCommand::RemoveRepository {
                    repository_id: id,
                    command_id: CommandId::new(),
                })
                .await?;
            if cli.json {
                println!("{{\"status\":\"removed\",\"repository_id\":\"{id}\"}}");
            } else {
                println!("Removed repository {id} from Tollgate. Repository data was preserved.");
            }
        }
        TopCommand::Approve(args) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let candidate_id = args.revision.parse::<QueueItemId>().ok();
            let response = if let Some(item_id) = candidate_id {
                client
                    .request(IpcCommand::AuthorizeCandidate {
                        repository_id: repository.state.id,
                        item_id,
                        expected_revision: repository.state.queue_revision,
                        command_id: CommandId::new(),
                    })
                    .await?
            } else {
                client
                    .request(IpcCommand::Approve {
                        repository_id: repository.state.id,
                        revision: args.revision,
                        worktree_path: Some(
                            std::env::current_dir()?.to_string_lossy().into_owned(),
                        ),
                        purpose: None,
                        command_id: CommandId::new(),
                    })
                    .await?
            };
            print_value(response.clone(), cli.json, |value| {
                println!(
                    "Approved {}{}\n  authority  {}\n  source  {}\n  tested  {}\n  queue revision {}",
                    value["item_id"].as_str().unwrap_or("?"),
                    if value["evidence_reused"].as_bool() == Some(true) {
                        " (completed validation reused)"
                    } else {
                        ""
                    },
                    match value["authorized_item_ids"].as_array() {
                        Some(items) if items.len() > 1 => {
                            format!("{} candidates (including dependencies)", items.len())
                        }
                        _ => "1 candidate".into(),
                    },
                    oid_value(&value["source_oid"]),
                    oid_value(&value["tested_oid"]),
                    value["queue_revision"]
                );
                if let Some(items) = value["restarted_item_ids"].as_array()
                    && !items.is_empty()
                {
                    println!(
                        "  priority  moved ahead of unapproved work; {} affected validation{} restarted",
                        items.len(),
                        if items.len() == 1 { "" } else { "s" }
                    );
                }
                Ok(())
            })?;
            if args.wait {
                return wait_for_item(
                    &mut client,
                    repository.state.id,
                    response["item_id"]
                        .as_str()
                        .ok_or_else(|| anyhow!("approval response omitted item ID"))?
                        .parse()?,
                    cli.json,
                )
                .await;
            }
        }
        TopCommand::Candidate(args) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let response = client
                .request(IpcCommand::Candidate {
                    repository_id: repository.state.id,
                    revision: args.revision,
                    worktree_path: Some(std::env::current_dir()?.to_string_lossy().into_owned()),
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(response.clone(), cli.json, |value| {
                println!(
                    "Candidate {} submitted without promotion authority\n  source  {}\n  tested  {}\n  queue revision {}",
                    value["item_id"].as_str().unwrap_or("?"),
                    oid_value(&value["source_oid"]),
                    oid_value(&value["tested_oid"]),
                    value["queue_revision"]
                );
                Ok(())
            })?;
            if args.wait {
                return wait_for_candidate(
                    &mut client,
                    repository.state.id,
                    response["item_id"]
                        .as_str()
                        .ok_or_else(|| anyhow!("candidate response omitted item ID"))?
                        .parse()?,
                    cli.json,
                )
                .await;
            }
        }
        TopCommand::PushMaster(args) => {
            return push_master(
                &mut client,
                cli.repository,
                cli.json,
                args.wait,
                args.status,
            )
            .await;
        }
        TopCommand::Check(args) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let response = client
                .request(IpcCommand::Check {
                    repository_id: repository.state.id,
                    revision: args.revision,
                    worktree_path: Some(std::env::current_dir()?.to_string_lossy().into_owned()),
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(response.clone(), cli.json, |value| {
                println!(
                    "Independent check started {}\n  source  {}\n  tested  {}",
                    value["item_id"].as_str().unwrap_or("?"),
                    oid_value(&value["source_oid"]),
                    oid_value(&value["tested_oid"]),
                );
                Ok(())
            })?;
            if args.wait {
                return wait_for_item(
                    &mut client,
                    repository.state.id,
                    response["item_id"]
                        .as_str()
                        .ok_or_else(|| anyhow!("check response omitted run ID"))?
                        .parse()?,
                    cli.json,
                )
                .await;
            }
        }
        TopCommand::Diagnose(args) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let mut diagnosis: DiagnoseResult = serde_json::from_value(
                client
                    .request(IpcCommand::Diagnose {
                        repository_id: repository.state.id,
                        item_id: args.id,
                        replay: !args.no_replay,
                        verify_repair: args.verify_repair,
                        command_id: CommandId::new(),
                    })
                    .await?,
            )?;
            if !diagnosis.replay_item_ids.is_empty() {
                let repair_artifact = diagnosis.repair_artifact.clone();
                if !cli.json {
                    println!(
                        "Running clean diagnostic matrix for base and candidate (candidate twice)…"
                    );
                }
                wait_for_terminal_checks(
                    &mut client,
                    repository.state.id,
                    &diagnosis.replay_item_ids,
                )
                .await?;
                diagnosis = serde_json::from_value(
                    client
                        .request(IpcCommand::Diagnose {
                            repository_id: repository.state.id,
                            item_id: args.id,
                            replay: false,
                            verify_repair: false,
                            command_id: CommandId::new(),
                        })
                        .await?,
                )?;
                diagnosis.repair_artifact = repair_artifact;
            }
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&diagnosis)?);
            } else {
                print_diagnosis(&diagnosis);
            }
        }
        TopCommand::Queue => {
            let repository = select_repository(&mut client, cli.repository).await?;
            print_queue(&repository, cli.json)?;
        }
        TopCommand::Status { id } => {
            let repository = select_repository(&mut client, cli.repository).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&repository)?);
            } else {
                print_status(&repository, id);
            }
        }
        TopCommand::Wait { id } => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let validation_only = repository
                .queue
                .iter()
                .find(|view| view.item.id == id)
                .is_some_and(|view| !view.item.promotion_authorized);
            return if validation_only {
                wait_for_candidate(&mut client, repository.state.id, id, cli.json).await
            } else {
                wait_for_item(&mut client, repository.state.id, id, cli.json).await
            };
        }
        TopCommand::Logs {
            id,
            buildset,
            step,
            follow,
        } => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let mut sequence = 0;
            loop {
                let value = match client
                    .request(IpcCommand::Logs {
                        repository_id: repository.state.id,
                        item_id: id,
                        buildset_id: buildset,
                        step: step.clone(),
                        start_sequence: sequence,
                    })
                    .await
                {
                    Ok(value) => value,
                    Err(error) if follow => {
                        if !cli.json {
                            eprintln!(
                                "\nLog connection interrupted ({error}); resuming from sequence {sequence}…"
                            );
                        }
                        reconnect_wait_client(&mut client).await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let frames = value.as_array().cloned().unwrap_or_default();
                for frame in &frames {
                    if cli.json {
                        println!("{}", serde_json::to_string(frame)?);
                    } else {
                        print!("{}", frame["text"].as_str().unwrap_or(""));
                    }
                    sequence = sequence.max(
                        frame["frame"]["broker_sequence"]
                            .as_u64()
                            .unwrap_or(sequence)
                            + 1,
                    );
                }
                if !follow {
                    break;
                }
                let snapshot = match client.snapshot().await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        if !cli.json {
                            eprintln!(
                                "\nLog status connection interrupted ({error}); reconnecting…"
                            );
                        }
                        reconnect_wait_client(&mut client).await?;
                        continue;
                    }
                };
                let active = snapshot
                    .repositories
                    .iter()
                    .flat_map(|repository| repository.queue.iter().chain(&repository.checks))
                    .find(|view| view.item.id == id)
                    .is_some_and(|view| {
                        matches!(
                            view.item.state,
                            tollgate_domain::QueueItemState::Running
                                | tollgate_domain::QueueItemState::Preparing
                        )
                    });
                if !active && frames.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
        TopCommand::Cancel { id } => {
            let repository = select_repository(&mut client, cli.repository).await?;
            client
                .request(IpcCommand::Cancel {
                    repository_id: repository.state.id,
                    item_id: id,
                    expected_revision: repository.state.queue_revision,
                    command_id: CommandId::new(),
                })
                .await?;
            if cli.json {
                println!("{{\"status\":\"accepted\",\"item_id\":\"{id}\"}}");
            } else {
                println!(
                    "Canceled {id}; affected descendants will receive new validation generations."
                );
            }
        }
        TopCommand::Retry { id, cold } => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::Retry {
                    repository_id: repository.state.id,
                    item_id: id,
                    cold,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "Retried {id} as {} at the queue tail{}.",
                    value["item_id"].as_str().unwrap_or("?"),
                    if cold { " with a cold slot" } else { "" }
                );
                Ok(())
            })?;
        }
        TopCommand::Reorder { ids } => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::Reorder {
                    repository_id: repository.state.id,
                    selected_ids: ids,
                    expected_revision: repository.state.queue_revision,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                let restarted = value["restarted_item_ids"].as_array().map_or(0, Vec::len);
                println!(
                    "Queue reordered at revision {}. {restarted} affected item{} received a new validation generation.",
                    value["queue_revision"],
                    if restarted == 1 { "" } else { "s" }
                );
                Ok(())
            })?;
        }
        command @ (TopCommand::Pause | TopCommand::Resume) => {
            let paused = matches!(command, TopCommand::Pause);
            let repository = select_repository(&mut client, cli.repository).await?;
            let command_id = CommandId::new();
            client
                .request(if paused {
                    IpcCommand::Pause {
                        repository_id: repository.state.id,
                        command_id,
                    }
                } else {
                    IpcCommand::Resume {
                        repository_id: repository.state.id,
                        command_id,
                    }
                })
                .await?;
            if cli.json {
                println!(
                    "{{\"execution_state\":\"{}\"}}",
                    if paused { "paused" } else { "active" }
                );
            } else {
                println!("Gate {}.", if paused { "paused" } else { "resumed" });
            }
        }
        command @ (TopCommand::Pull | TopCommand::Push) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(if matches!(command, TopCommand::Pull) {
                    IpcCommand::Pull {
                        repository_id: repository.state.id,
                        command_id: CommandId::new(),
                    }
                } else {
                    IpcCommand::Push {
                        repository_id: repository.state.id,
                        command_id: CommandId::new(),
                    }
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}\n  local   {}\n  remote  {}\n  action  {}",
                    value["message"]
                        .as_str()
                        .unwrap_or("Remote operation completed."),
                    oid_value(&value["local_master"]),
                    if value["remote_master"].is_null() {
                        "absent".into()
                    } else {
                        oid_value(&value["remote_master"])
                    },
                    value["action"].as_str().unwrap_or("unknown")
                );
                Ok(())
            })?;
        }
        TopCommand::Reconcile => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::Reconcile {
                    repository_id: repository.state.id,
                    expected_observed_master: None,
                    expected_queue_revision: None,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}\n  adopted release {}\n  queue revision  {}",
                    value["message"]
                        .as_str()
                        .unwrap_or("Repository reconciled."),
                    oid_value(&value["local_master"]),
                    value["queue_revision"]
                );
                Ok(())
            })?;
        }
        TopCommand::Update => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::Update {
                    repository_id: repository.state.id,
                    worktree_path: std::env::current_dir()?.to_string_lossy().into_owned(),
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}\n  old  {}\n  new  {}",
                    value["message"].as_str().unwrap_or("Feature updated."),
                    oid_value(&value["old_oid"]),
                    oid_value(&value["new_oid"])
                );
                Ok(())
            })?;
        }
        TopCommand::Worktree(WorktreeCommand::Create { name }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::WorktreeCreate {
                    repository_id: repository.state.id,
                    branch: name,
                    destination: None,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}\n  path    {}\n  branch  {}",
                    value["message"].as_str().unwrap_or("Worktree created."),
                    value["path"].as_str().unwrap_or("?"),
                    value["branch"].as_str().unwrap_or("?")
                );
                Ok(())
            })?;
        }
        TopCommand::Worktree(WorktreeCommand::Remove { path }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let path = std::fs::canonicalize(path)?;
            let value = client
                .request(IpcCommand::WorktreeRemove {
                    repository_id: repository.state.id,
                    path: path.to_string_lossy().into_owned(),
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}\n  removed  {}",
                    value["message"].as_str().unwrap_or("Worktree removed."),
                    value["path"].as_str().unwrap_or("?")
                );
                Ok(())
            })?;
        }
        TopCommand::Env(EnvCommand::Reload) => {
            let value = client
                .request(IpcCommand::ReloadEnvironment {
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "Environment reloaded for future buildsets.\n  snapshot     {}\n  fingerprint  {}",
                    value["snapshot_id"].as_str().unwrap_or("?"),
                    value["fingerprint"].as_str().unwrap_or("?")
                );
                Ok(())
            })?;
        }
        TopCommand::Env(EnvCommand::Show) => {
            let snapshot = client.snapshot().await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&snapshot.environment)?);
            } else {
                println!(
                    "Shell environment\n  snapshot     {}\n  fingerprint  {}\n  variables    {}\n  PATH         {}",
                    snapshot.environment.snapshot_id,
                    snapshot.environment.fingerprint,
                    snapshot.environment.variable_count,
                    snapshot.environment.path
                );
            }
        }
        TopCommand::Config(ConfigCommand::Validate | ConfigCommand::Explain) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let candidate = client
                .request(IpcCommand::ValidateConfiguration {
                    repository_id: repository.state.id,
                })
                .await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&candidate)?);
            } else {
                println!(
                    "Configuration valid\n  digest  {}\n  graph   {}\n  runner  {}",
                    candidate["digest"].as_str().unwrap_or("?"),
                    candidate["step_graph_digest"].as_str().unwrap_or("?"),
                    candidate["runner"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|value| value.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                );
                for step in candidate["steps"].as_array().into_iter().flatten() {
                    println!(
                        "  step    {:<18} voting={} timeout={}m",
                        step["name"].as_str().unwrap_or("?"),
                        step["voting"].as_bool().unwrap_or(false),
                        step["timeout_ns"].as_u64().unwrap_or(0) / 60_000_000_000
                    );
                }
            }
        }
        TopCommand::Config(ConfigCommand::Apply) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::ApplyConfiguration {
                    repository_id: repository.state.id,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "Configuration applied.\n  digest  {}\n  queue revision  {}",
                    value["configuration"]["digest"].as_str().unwrap_or("?"),
                    value["state"]["queue_revision"].as_u64().unwrap_or(0)
                );
                Ok(())
            })?;
        }
        TopCommand::Config(ConfigCommand::Regenerate) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::RegenerateConfiguration {
                    repository_id: repository.state.id,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "Configuration regenerated and awaits explicit apply.\n  digest  {}\n  runner  {}",
                    value["digest"].as_str().unwrap_or("?"),
                    value["runner"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|value| value.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                );
                Ok(())
            })?;
        }
        TopCommand::Slot(SlotCommand::List) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&repository.slots)?);
            } else {
                for slot in repository.slots {
                    println!(
                        "{}  {:<8}  {}",
                        slot.id,
                        slot.state,
                        slot.checkout_oid
                            .map(|oid| oid.short())
                            .unwrap_or_else(|| "—".into())
                    );
                }
            }
        }
        TopCommand::Slot(SlotCommand::Reset { id }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let slot_id = id.parse()?;
            let value = client
                .request(IpcCommand::ResetSlot {
                    repository_id: repository.state.id,
                    slot_id,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "Slot {} was quarantined and recreated cold.\n  checkout  {}\n  health    {}",
                    value["id"].as_str().unwrap_or("?"),
                    oid_value(&value["checkout_oid"]),
                    value["health"].as_str().unwrap_or("?")
                );
                Ok(())
            })?;
        }
        TopCommand::Cache(CacheCommand::Status) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = serde_json::json!({"slots": repository.slots.len(), "seeds": repository.seeds, "cache_policy": "preserve-ignored", "config_digest": repository.configuration.digest});
            print_value(value, cli.json, |value| {
                println!(
                    "Cache\n  persistent slots  {}\n  policy            preserve ignored files\n  config digest     {}",
                    value["slots"],
                    value["config_digest"].as_str().unwrap_or("?")
                );
                Ok(())
            })?;
        }
        TopCommand::Cache(CacheCommand::Snapshot) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::SnapshotCache {
                    repository_id: repository.state.id,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}\n  logical bytes  {}",
                    value["message"].as_str().unwrap_or("Cache seed published."),
                    value["logical_bytes"]
                );
                Ok(())
            })?;
        }
        TopCommand::Cache(CacheCommand::Purge { all_slots }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::PurgeCache {
                    repository_id: repository.state.id,
                    all_slots,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!("{}", value["message"].as_str().unwrap_or("Cache purged."));
                Ok(())
            })?;
        }
        TopCommand::Artifact(ArtifactCommand::List) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&repository.artifacts)?);
            } else if repository.artifacts.is_empty() {
                println!("No retained artifacts.");
            } else {
                for artifact in repository.artifacts {
                    println!(
                        "{}  {:<8}  {:>10}  {}",
                        artifact.artifact_id,
                        artifact.retention_state,
                        artifact.size,
                        artifact.source_path
                    );
                }
            }
        }
        TopCommand::Artifact(ArtifactCommand::Pin { id }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::ArtifactPin {
                    repository_id: repository.state.id,
                    artifact_id: id,
                    pinned: true,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}",
                    value["message"]
                        .as_str()
                        .unwrap_or("Artifact retention updated.")
                );
                Ok(())
            })?;
        }
        TopCommand::Artifact(ArtifactCommand::Unpin { id }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::ArtifactPin {
                    repository_id: repository.state.id,
                    artifact_id: id,
                    pinned: false,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}",
                    value["message"]
                        .as_str()
                        .unwrap_or("Artifact retention updated.")
                );
                Ok(())
            })?;
        }
        TopCommand::Artifact(ArtifactCommand::Prune { id }) => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let value = client
                .request(IpcCommand::ArtifactPrune {
                    repository_id: repository.state.id,
                    artifact_id: id,
                    command_id: CommandId::new(),
                })
                .await?;
            print_value(value, cli.json, |value| {
                println!(
                    "{}",
                    value["message"].as_str().unwrap_or("Artifact pruned.")
                );
                Ok(())
            })?;
        }
        TopCommand::History => {
            let repository = select_repository(&mut client, cli.repository).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&repository.history)?);
            } else {
                for event in repository.history.iter().rev().take(100) {
                    println!(
                        "#{:<6} {:<28} {:?}",
                        event.sequence, event.kind, event.actor
                    );
                }
            }
        }
        TopCommand::Doctor => {
            let repository = select_repository(&mut client, cli.repository).await?;
            let report = client
                .request(IpcCommand::Doctor {
                    repository_id: repository.state.id,
                })
                .await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("Tollgate doctor · {}", repository.state.name);
                for check in report["checks"].as_array().into_iter().flatten() {
                    let healthy = check["status"].as_str() == Some("healthy");
                    println!(
                        "  {} {:<28} {}",
                        if healthy { "✓" } else { "!" },
                        check["name"].as_str().unwrap_or("check"),
                        check["detail"].as_str().unwrap_or("")
                    );
                    if !healthy && let Some(action) = check["recovery_action"].as_str() {
                        println!("      recovery: {action}");
                    }
                }
            }
        }
    }
    Ok(0)
}

struct IpcClient {
    framed: Framed<UnixStream, FrameCodec>,
    no_launch: bool,
    client_instance_id: tollgate_domain::ClientInstanceId,
}

impl IpcClient {
    async fn connect(no_launch: bool) -> anyhow::Result<Self> {
        Self::connect_with_instance(no_launch, tollgate_domain::ClientInstanceId::new()).await
    }

    async fn connect_with_instance(
        no_launch: bool,
        client_instance_id: tollgate_domain::ClientInstanceId,
    ) -> anyhow::Result<Self> {
        let path = socket_path()?;
        let stream = match UnixStream::connect(&path).await {
            Ok(stream) => stream,
            Err(first_error) if !no_launch => {
                let status = tokio::process::Command::new("/usr/bin/open")
                    .args(["-gj", "-a", "Tollgate"])
                    .status()
                    .await
                    .context("could not launch Tollgate")?;
                if !status.success() {
                    return Err(anyhow!(
                        "Tollgate launch failed; initial IPC error: {first_error}"
                    ));
                }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                loop {
                    match UnixStream::connect(&path).await {
                        Ok(stream) => break stream,
                        Err(_error) if tokio::time::Instant::now() < deadline => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(error) => {
                            return Err(anyhow!("Tollgate did not become available: {error}"));
                        }
                    }
                }
            }
            Err(error) => {
                return Err(anyhow!(
                    "Tollgate app is unavailable (--no-launch): {error}"
                ));
            }
        };
        verify_peer_uid(&stream).context("Tollgate app peer identity check failed")?;
        let mut framed = Framed::new(stream, FrameCodec);
        let correlation = Uuid::now_v7();
        let handshake = Handshake {
            client_instance_id,
            client_version: env!("CARGO_PKG_VERSION").into(),
            protocol_min: PROTOCOL_VERSION,
            protocol_max: PROTOCOL_VERSION,
            schema_version: 1,
            max_control_payload: MAX_CONTROL_PAYLOAD as u32,
            max_log_payload: MAX_LOG_PAYLOAD as u32,
            supported_frame_kinds: vec![
                FrameKind::Handshake,
                FrameKind::HandshakeAck,
                FrameKind::Request,
                FrameKind::Response,
                FrameKind::Event,
                FrameKind::Log,
                FrameKind::Gap,
            ],
        };
        framed
            .send(Frame::control(
                FrameKind::Handshake,
                correlation,
                &handshake,
            )?)
            .await?;
        let ack = framed
            .next()
            .await
            .ok_or_else(|| anyhow!("app closed during handshake"))??;
        if ack.kind != FrameKind::HandshakeAck || ack.correlation_id != correlation {
            return Err(anyhow!("invalid app handshake response"));
        }
        let _: HandshakeAck = ack.decode_json()?;
        Ok(Self {
            framed,
            no_launch,
            client_instance_id,
        })
    }

    async fn request(&mut self, command: IpcCommand) -> anyhow::Result<serde_json::Value> {
        loop {
            let correlation = Uuid::now_v7();
            if let Err(error) = self
                .framed
                .send(Frame::control(FrameKind::Request, correlation, &command)?)
                .await
            {
                self.reconnect_once()
                    .await
                    .with_context(|| format!("request send failed before reconnect: {error}"))?;
                continue;
            }
            loop {
                let frame = match self.framed.next().await {
                    Some(Ok(frame)) => frame,
                    Some(Err(error)) => {
                        self.reconnect_once().await.with_context(|| {
                            format!("response stream failed before reconnect: {error}")
                        })?;
                        break;
                    }
                    None => {
                        self.reconnect_once()
                            .await
                            .with_context(|| "app closed before responding; reconnect failed")?;
                        break;
                    }
                };
                if frame.kind == FrameKind::Event {
                    continue;
                }
                if frame.kind != FrameKind::Response || frame.correlation_id != correlation {
                    return Err(anyhow!("mismatched IPC response"));
                }
                let response: IpcResponse = frame.decode_json()?;
                return if response.ok {
                    Ok(response.result.unwrap_or(serde_json::Value::Null))
                } else {
                    Err(anyhow!(IpcRequestError(response.error.unwrap_or_else(
                        || {
                            StructuredError {
                                code: "unknown-service-error".into(),
                                message: "unknown service error".into(),
                                retryable: false,
                                details: None,
                            }
                        }
                    ))))
                };
            }
        }
    }

    async fn reconnect_once(&mut self) -> anyhow::Result<()> {
        *self = Self::connect_with_instance(self.no_launch, self.client_instance_id).await?;
        Ok(())
    }

    async fn snapshot(&mut self) -> anyhow::Result<AppSnapshot> {
        Ok(serde_json::from_value(
            self.request(IpcCommand::Snapshot).await?,
        )?)
    }
}

async fn push_master(
    client: &mut IpcClient,
    selected: Option<RepositoryId>,
    json: bool,
    wait: bool,
    status: bool,
) -> anyhow::Result<u8> {
    let mut repository = select_repository(client, selected).await?;
    if status {
        print_push_master_status(&repository, json)?;
        return Ok(0);
    }
    if !repository.configuration.remote_enabled {
        return Err(anyhow!(
            "remote push is disabled for this repository; enable [remote] before using `tg push-master`"
        ));
    }

    let git = GitRepository::discover(&repository.state.path).await?;
    let master_worktree = git
        .worktree_for_branch(USER_BRANCH_REF)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "local master is not checked out; check it out in a worktree before using `tg push-master`"
            )
        })?;
    let master_git = GitRepository::discover(&master_worktree).await?;
    if master_git.common_dir != git.common_dir
        || master_git.current_branch().await?.as_deref() != Some(USER_BRANCH)
    {
        return Err(anyhow!(
            "the discovered master worktree does not belong to the selected repository"
        ));
    }
    master_git.ensure_clean().await?;
    let release_oid = git.integration_oid().await?;
    let mut master_oid = git.resolve_oid(USER_BRANCH_REF).await?;
    if master_git.resolve_oid("HEAD").await? != master_oid {
        return Err(anyhow!(
            "master worktree HEAD does not match the local master ref"
        ));
    }

    let mut rebased_from = None;
    if !git.is_ancestor(&release_oid, &master_oid).await? {
        let merge_base = git.merge_base_oid(&release_oid, &master_oid).await?;
        let stale_sources = git
            .first_parent_commits_between(&merge_base, &master_oid)
            .await?;
        if let Some(active) = repository
            .queue
            .iter()
            .find(|view| stale_sources.contains(&view.item.source_oid))
        {
            return Err(anyhow!(
                "master commit {} is still active as candidate {}; wait for it to finish before rebasing master",
                active.item.source_oid.short(),
                active.item.id
            ));
        }
        let old_master = master_oid.clone();
        master_oid = master_git
            .rebase_user_master_onto_release(&old_master)
            .await?;
        if master_oid != old_master {
            rebased_from = Some(old_master);
        }
    }

    let source_oids = git.unmerged_user_master_commits().await?;
    if source_oids.is_empty() {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "up-to-date",
                    "release_oid": release_oid,
                    "master_oid": master_oid,
                    "rebased_from": rebased_from,
                    "items": [],
                }))?
            );
        } else if let Some(old_master) = rebased_from {
            println!(
                "Updated local master {} to certified release {}.",
                old_master.short(),
                master_oid.short()
            );
        } else {
            println!("Local master has no commits beyond certified release.");
        }
        return Ok(0);
    }

    let mut scheduled = Vec::with_capacity(source_oids.len());
    for source_oid in source_oids {
        let existing = repository
            .queue
            .iter()
            .find(|view| view.item.source_oid == source_oid)
            .map(|view| (view.item.id, view.item.promotion_authorized));
        let (item_id, action) = match existing {
            Some((item_id, true)) => (item_id, "already-authorized"),
            Some((item_id, false)) => {
                let response = client
                    .request(IpcCommand::AuthorizeCandidate {
                        repository_id: repository.state.id,
                        item_id,
                        expected_revision: repository.state.queue_revision,
                        command_id: CommandId::new(),
                    })
                    .await?;
                let response_item_id = response["item_id"]
                    .as_str()
                    .ok_or_else(|| anyhow!("authorization response omitted item ID"))?
                    .parse::<QueueItemId>()?;
                (response_item_id, "authorized-candidate")
            }
            None => {
                let response = client
                    .request(IpcCommand::Approve {
                        repository_id: repository.state.id,
                        revision: source_oid.to_hex(),
                        worktree_path: Some(master_worktree.to_string_lossy().into_owned()),
                        purpose: Some("push-master".into()),
                        command_id: CommandId::new(),
                    })
                    .await?;
                let item_id = response["item_id"]
                    .as_str()
                    .ok_or_else(|| anyhow!("approval response omitted item ID"))?
                    .parse::<QueueItemId>()?;
                (item_id, "submitted")
            }
        };
        scheduled.push(serde_json::json!({
            "item_id": item_id,
            "source_oid": source_oid,
            "action": action,
        }));
        repository = select_repository(client, Some(repository.state.id)).await?;
    }

    let tail_id = scheduled
        .last()
        .and_then(|item| item["item_id"].as_str())
        .ok_or_else(|| anyhow!("master submission did not produce a tail item"))?
        .parse::<QueueItemId>()?;
    let result = serde_json::json!({
        "status": "scheduled",
        "release_oid": release_oid,
        "master_oid": master_oid,
        "rebased_from": rebased_from,
        "items": scheduled,
        "tail_item_id": tail_id,
        "queue_revision": repository.state.queue_revision,
    });
    print_value(result, json, |value| {
        let count = value["items"].as_array().map_or(0, Vec::len);
        println!(
            "Scheduled {count} master commit{} for certified push\n  release  {}\n  master   {}\n  tail     {}",
            if count == 1 { "" } else { "s" },
            oid_value(&value["release_oid"]),
            oid_value(&value["master_oid"]),
            tail_id,
        );
        if value["rebased_from"].is_object() {
            println!("  rebased  stale local master onto certified release");
        }
        if wait {
            println!("Waiting for Tollgate to certify, push, and synchronize local master.");
        } else {
            println!(
                "Tollgate will keep a clean, unchanged local master projected onto newly certified history while validation runs."
            );
        }
        Ok(())
    })?;
    if wait {
        return wait_for_item(client, repository.state.id, tail_id, json).await;
    }
    Ok(0)
}

fn print_push_master_status(repository: &RepositorySnapshot, json: bool) -> anyhow::Result<()> {
    let Some(view) = repository.master_push.as_ref() else {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "none",
                    "repository_id": repository.state.id,
                    "repository_name": repository.state.name,
                }))?
            );
        } else {
            println!(
                "No master push has been recorded for {}.",
                repository.state.name
            );
        }
        return Ok(());
    };

    let status = master_push_status(view);
    let failed_steps = master_push_failed_steps(view);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": status,
                "repository_id": repository.state.id,
                "repository_name": repository.state.name,
                "item_id": view.item.id,
                "state": view.item.state,
                "remote_state": view.item.remote_state,
                "source_oid": view.item.source_oid,
                "tested_oid": view.generation.as_ref().map(|generation| &generation.tested_oid),
                "subject": view.item.metadata.subject,
                "terminal_reason": view.item.terminal_reason,
                "failed_steps": failed_steps,
            }))?
        );
        return Ok(());
    }

    println!("{}", master_push_status_heading(status));
    println!("  candidate  {}", view.item.id);
    println!("  source     {}", view.item.source_oid.short());
    println!(
        "  tested     {}",
        view.generation
            .as_ref()
            .map(|generation| generation.tested_oid.short())
            .unwrap_or_else(|| "—".into())
    );
    println!("  state      {:?}", view.item.state);
    if let Some(reason) = &view.item.terminal_reason {
        println!("  reason     {reason}");
    }
    for step in &failed_steps {
        println!(
            "  failed     {} ({})",
            step["name"].as_str().unwrap_or("unknown"),
            step["result"].as_str().unwrap_or("failed")
        );
    }
    if status == "failed" {
        if let Some(step) = failed_steps.first().and_then(|step| step["name"].as_str()) {
            println!("  inspect    tg logs {} --step {step}", view.item.id);
        } else {
            println!("  inspect    tg status {}", view.item.id);
        }
    }
    Ok(())
}

fn master_push_status(view: &QueueItemView) -> &'static str {
    use tollgate_domain::{QueueItemState as State, RemoteState};
    if view.item.remote_state == RemoteState::PushBlocked {
        return "failed";
    }
    match view.item.state {
        State::Promoted | State::ExternallyIntegrated => "succeeded",
        State::Failed
        | State::MergeConflict
        | State::DependencyFailed
        | State::InfrastructureExhausted => "failed",
        State::Canceled | State::Superseded => "canceled",
        State::Ready => "ready",
        _ => "running",
    }
}

fn master_push_status_heading(status: &str) -> &'static str {
    match status {
        "succeeded" => "Master push succeeded",
        "failed" => "Master push failed",
        "canceled" => "Master push was canceled",
        "ready" => "Master push passed validation and is awaiting promotion",
        _ => "Master push is running",
    }
}

fn master_push_failed_steps(view: &QueueItemView) -> Vec<serde_json::Value> {
    if let Some(attribution) = &view.failure_attribution {
        return attribution
            .steps
            .iter()
            .map(|step| {
                serde_json::json!({
                    "name": step.name,
                    "result": step.candidate_result,
                    "origin": step.origin,
                })
            })
            .collect();
    }
    view.buildset
        .iter()
        .flat_map(|buildset| &buildset.step_results)
        .filter(|step| !matches!(step.result_class.as_str(), "success" | "skipped"))
        .map(|step| serde_json::json!({"name": step.name, "result": step.result_class}))
        .collect()
}

async fn select_repository(
    client: &mut IpcClient,
    selected: Option<RepositoryId>,
) -> anyhow::Result<RepositorySnapshot> {
    let snapshot = client.snapshot().await?;
    if let Some(id) = selected {
        return snapshot
            .repositories
            .into_iter()
            .find(|repository| repository.state.id == id)
            .ok_or_else(|| anyhow!("repository {id} is not registered"));
    }
    if snapshot.repositories.len() == 1 {
        return Ok(snapshot.repositories.into_iter().next().unwrap());
    }
    let current = std::env::current_dir()
        .ok()
        .and_then(|cwd| std::fs::canonicalize(cwd).ok());
    if let Some(repository) = snapshot.repositories.iter().find(|repository| {
        current
            .as_ref()
            .is_some_and(|cwd| cwd.starts_with(&repository.state.path))
    }) {
        return Ok(repository.clone());
    }
    Err(anyhow!(
        "select a repository with --repository <ID> ({} registered)",
        snapshot.repositories.len()
    ))
}

async fn wait_for_item(
    client: &mut IpcClient,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    json: bool,
) -> anyhow::Result<u8> {
    wait_for_item_until(client, repository_id, item_id, json, false).await
}

async fn wait_for_candidate(
    client: &mut IpcClient,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    json: bool,
) -> anyhow::Result<u8> {
    wait_for_item_until(client, repository_id, item_id, json, true).await
}

async fn wait_for_item_until(
    client: &mut IpcClient,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    json: bool,
    validation_only: bool,
) -> anyhow::Result<u8> {
    loop {
        let snapshot = match client.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if !json {
                    eprintln!("\nConnection interrupted ({error}); waiting for Tollgate…");
                }
                reconnect_wait_client(client).await?;
                continue;
            }
        };
        if let Some(view) = snapshot
            .repositories
            .iter()
            .flat_map(|repository| repository.queue.iter().chain(&repository.checks))
            .find(|view| view.item.id == item_id)
        {
            if json {
                println!("{}", serde_json::to_string(view)?);
            } else {
                eprint!(
                    "\r{}  {:<30}",
                    view.item.state_as_display(),
                    view.item.metadata.subject
                );
            }
            if view.item.state.is_terminal()
                || (validation_only && view.item.state == tollgate_domain::QueueItemState::Ready)
            {
                if !json {
                    println!();
                }
                return Ok(exit_for_item(&view.item));
            }
            if let Some(repository) = snapshot
                .repositories
                .iter()
                .find(|repository| repository.state.id == repository_id)
                && repository.state.execution_state
                    == tollgate_domain::RepositoryExecutionState::Blocked
            {
                if !json {
                    println!("\nRepository blocked before this item could complete.");
                }
                return Ok(4);
            }
        } else {
            let value = match client
                .request(IpcCommand::ItemStatus {
                    repository_id,
                    item_id,
                })
                .await
            {
                Ok(value) => value,
                Err(first_error) => {
                    reconnect_wait_client(client).await?;
                    client
                        .request(IpcCommand::ItemStatus {
                            repository_id,
                            item_id,
                        })
                        .await
                        .with_context(|| {
                            format!("item status failed after reconnect: {first_error}")
                        })?
                }
            };
            let item: tollgate_domain::QueueItem = serde_json::from_value(value)?;
            if json {
                println!("{}", serde_json::to_string(&item)?);
            } else {
                eprint!(
                    "\r{}  {:<30}",
                    item.state_as_display(),
                    item.metadata.subject
                );
            }
            if item.state.is_terminal()
                || (validation_only && item.state == tollgate_domain::QueueItemState::Ready)
            {
                if !json {
                    println!();
                }
                return Ok(exit_for_item(&item));
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn reconnect_wait_client(client: &mut IpcClient) -> anyhow::Result<()> {
    let no_launch = client.no_launch;
    let client_instance_id = client.client_instance_id;
    let mut delay = Duration::from_millis(100);
    loop {
        match IpcClient::connect_with_instance(no_launch, client_instance_id).await {
            Ok(reconnected) => {
                *client = reconnected;
                return Ok(());
            }
            Err(error) if no_launch => {
                return Err(error).context(
                    "Tollgate became unavailable and --no-launch forbids relaunch/reconnect",
                );
            }
            Err(_) => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
    }
}

fn exit_for_item(item: &tollgate_domain::QueueItem) -> u8 {
    use tollgate_domain::QueueItemState as State;
    if item.remote_state == tollgate_domain::RemoteState::PushBlocked
        || item.cleanup_state == tollgate_domain::CleanupState::NeedsAttention
    {
        return 4;
    }
    match item.state {
        State::Promoted | State::ExternallyIntegrated | State::CheckPassed => 0,
        State::InfrastructureExhausted => 5,
        State::Canceled | State::Superseded | State::DependencyFailed => 3,
        State::Failed | State::MergeConflict | State::CheckFailed => 1,
        _ => 0,
    }
}

trait QueueItemDisplay {
    fn state_as_display(&self) -> String;
}
impl QueueItemDisplay for tollgate_domain::QueueItem {
    fn state_as_display(&self) -> String {
        format!("{:?}", self.state).to_lowercase()
    }
}

fn print_queue(repository: &RepositorySnapshot, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&repository.queue)?);
        return Ok(());
    }
    println!(
        "{} · queue revision {} · release {}",
        repository.state.name,
        repository.state.queue_revision,
        repository.state.master_oid.short()
    );
    if repository.queue.is_empty() {
        println!("Gate is clear.");
    }
    for (index, view) in repository.queue.iter().enumerate() {
        println!(
            "{:>2}  {:<11}  {:<10}  {}  {:<10}  {}",
            index + 1,
            format!("{:?}", view.item.state).to_lowercase(),
            if view.item.promotion_authorized {
                "authorized"
            } else {
                "candidate"
            },
            view.item.source_oid.short(),
            view.item.metadata.branch.as_deref().unwrap_or("detached"),
            view.item.metadata.subject
        );
        if let Some(generation) = &view.generation {
            println!(
                "    tested {}  parent {}  generation {}",
                generation.tested_oid.short(),
                generation.expected_parent_oid.short(),
                &generation.identity_digest[..10]
            );
        }
    }
    Ok(())
}

fn print_status(repository: &RepositorySnapshot, id: Option<QueueItemId>) {
    if let Some(id) = id {
        if let Some(view) = repository
            .queue
            .iter()
            .chain(&repository.checks)
            .chain(&repository.history_items)
            .find(|view| view.item.id == id)
        {
            println!(
                "{}\n  state       {:?}\n  authority   {}\n  source      {}\n  tested      {}\n  generation  {}\n  evidence    {}",
                view.item.metadata.subject,
                view.item.state,
                if view.item.promotion_authorized {
                    "promotion authorized"
                } else {
                    "validation only"
                },
                view.item.source_oid.short(),
                view.generation
                    .as_ref()
                    .map(|value| value.tested_oid.short())
                    .unwrap_or_else(|| "—".into()),
                view.generation
                    .as_ref()
                    .map(|value| &value.identity_digest[..10])
                    .unwrap_or("—"),
                if view.certificate.is_some() {
                    "promotion-grade certificate ready"
                } else {
                    "not complete"
                }
            );
            if let Some(attribution) = &view.failure_attribution {
                println!(
                    "  failure     {:?}\n  base        {}\n  environment {}",
                    attribution.origin,
                    attribution.base_oid.short(),
                    &attribution.environment_fingerprint[..12]
                );
                for step in &attribution.steps {
                    println!(
                        "    {}: {:?} (candidate {}, base {})",
                        step.name,
                        step.origin,
                        step.candidate_result,
                        step.baseline_result
                            .as_deref()
                            .unwrap_or("no comparable run")
                    );
                    for diagnostic in &step.diagnostics {
                        println!("      [{}] {}", diagnostic.code, diagnostic.message);
                        if let Some(tollgate_domain::RepairCommand::Argv { argv }) =
                            &diagnostic.repair
                        {
                            println!("      suggested repair: {}", argv.join(" "));
                        }
                    }
                }
            }
        } else {
            println!("Queue item {id} was not found in the recent snapshot.");
        }
        return;
    }
    println!(
        "{}\n  execution  {:?}\n  release    {}\n  queue      {} items · revision {}\n  runs       {} active · {} waiting\n  config     {}",
        repository.state.name,
        repository.state.execution_state,
        repository.state.master_oid.short(),
        repository.queue.len(),
        repository.state.queue_revision,
        repository.resources.active_runs,
        repository.resources.queued_runs,
        &repository.configuration.digest[..12]
    );
}

async fn wait_for_terminal_checks(
    client: &mut IpcClient,
    repository_id: RepositoryId,
    item_ids: &[QueueItemId],
) -> anyhow::Result<()> {
    loop {
        let mut complete = true;
        for item_id in item_ids {
            let command = || IpcCommand::ItemStatus {
                repository_id,
                item_id: *item_id,
            };
            let value = match client.request(command()).await {
                Ok(value) => value,
                Err(first_error) => {
                    reconnect_wait_client(client).await?;
                    client.request(command()).await.with_context(|| {
                        format!("diagnostic replay status failed after reconnect: {first_error}")
                    })?
                }
            };
            let item: tollgate_domain::QueueItem = serde_json::from_value(value)?;
            complete &= item.state.is_terminal();
        }
        if complete {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn print_diagnosis(diagnosis: &DiagnoseResult) {
    let Some(attribution) = &diagnosis.attribution else {
        println!("No voting-step failure evidence is available for this item.");
        return;
    };
    println!(
        "Failure origin: {:?}\n  candidate   {}\n  base        {}\n  config      {}\n  environment {}",
        attribution.origin,
        attribution.candidate_tested_oid.short(),
        attribution.base_oid.short(),
        &attribution.configuration_digest[..12],
        &attribution.environment_fingerprint[..12],
    );
    for step in &attribution.steps {
        println!(
            "  {}: {:?} (candidate {}, base {})",
            step.name,
            step.origin,
            step.candidate_result,
            step.baseline_result
                .as_deref()
                .unwrap_or("no comparable run")
        );
        for diagnostic in &step.diagnostics {
            println!("    [{}] {}", diagnostic.code, diagnostic.message);
            if !diagnostic.paths.is_empty() {
                println!("    paths: {}", diagnostic.paths.join(", "));
            }
            if let Some(tollgate_domain::RepairCommand::Argv { argv }) = &diagnostic.repair {
                println!("    suggested repair: {}", argv.join(" "));
            }
        }
    }
    if let Some(artifact) = &diagnosis.repair_artifact {
        println!(
            "Verified repair patch:\n  path  {}\n  hash  {}\n  bytes {}",
            artifact.path, artifact.blake3, artifact.byte_length
        );
        println!("The candidate source was not modified; submit the patch as a new candidate.");
    }
}

fn print_value(
    value: serde_json::Value,
    json: bool,
    human: impl FnOnce(&serde_json::Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
        Ok(())
    } else {
        human(&value)
    }
}
fn socket_path() -> anyhow::Result<PathBuf> {
    Ok(ProjectDirs::from("dev", "Tollgate", "Tollgate")
        .ok_or_else(|| anyhow!("application support directory unavailable"))?
        .data_dir()
        .join("tollgate.sock"))
}
fn short_oid(value: &str) -> String {
    value.chars().take(10).collect()
}
fn oid_value(value: &serde_json::Value) -> String {
    short_oid(value["bytes"].as_str().unwrap_or("?"))
}
fn classify_exit(error: &anyhow::Error) -> u8 {
    if let Some(error) = error.downcast_ref::<IpcRequestError>()
        && matches!(
            error.0.code.as_str(),
            "stale-queue-prefix" | "unknown-source-ancestor" | "revision-conflict"
        )
    {
        return 4;
    }
    let message = error.to_string();
    if message.contains("configuration")
        || message.contains("argument")
        || message.contains("not found")
        || message.contains("unknown")
    {
        2
    } else if message.contains("blocked")
        || message.contains("conflict")
        || message.contains("reconcile")
    {
        4
    } else {
        5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reorder_command_has_no_promote_alias() {
        let id = "019ffe40-a60d-7722-a369-2635222d1203";
        let parsed = Cli::try_parse_from(["tg", "reorder", id]).unwrap();
        let TopCommand::Reorder { ids } = parsed.command else {
            panic!("reorder did not parse as the queue reorder command");
        };
        assert_eq!(ids, vec![id.parse().unwrap()]);
        assert!(Cli::try_parse_from(["tg", "promote", id]).is_err());
    }

    #[test]
    fn push_master_wait_flag_parses() {
        let parsed = Cli::try_parse_from(["tg", "push-master", "--wait"]).unwrap();
        let TopCommand::PushMaster(args) = parsed.command else {
            panic!("push-master did not parse as the master submission command");
        };
        assert!(args.wait);
        assert!(!args.status);
    }

    #[test]
    fn push_master_status_flag_is_read_only_and_conflicts_with_wait() {
        let parsed = Cli::try_parse_from(["tg", "push-master", "--status"]).unwrap();
        let TopCommand::PushMaster(args) = parsed.command else {
            panic!("push-master did not parse as the master submission command");
        };
        assert!(args.status);
        assert!(!args.wait);
        assert!(Cli::try_parse_from(["tg", "push-master", "--status", "--wait"]).is_err());
    }

    #[test]
    fn diagnose_replays_by_default_and_accepts_explicit_repair_verification() {
        let id = "019ffe40-a60d-7722-a369-2635222d1203";
        let parsed = Cli::try_parse_from(["tg", "diagnose", id, "--verify-repair"]).unwrap();
        let TopCommand::Diagnose(args) = parsed.command else {
            panic!("diagnose did not parse");
        };
        assert_eq!(args.id, id.parse().unwrap());
        assert!(!args.no_replay);
        assert!(args.verify_repair);
    }
}
