use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use directories::ProjectDirs;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, Manager};
use tauri_plugin_notification::NotificationExt;
use tauri_plugin_opener::OpenerExt;
use tokio_util::codec::Framed;
use tollgate_domain::{BuildsetId, CleanupPolicy, CommandId, QueueItemId, RepositoryId, SlotId};
use tollgate_ipc::{
    Frame, FrameCodec, FrameKind, Handshake, HandshakeAck, IpcCommand, IpcResponse,
    MAX_CONTROL_PAYLOAD, MAX_LOG_PAYLOAD, PROTOCOL_VERSION, StructuredError,
    acquire_user_authority_lock, bind_user_socket, verify_peer_uid,
};
use tollgate_service::{
    AppSnapshot, ApproveResult, CandidateAuthorizationResult, DoctorReport, EnvironmentView,
    QueueReorderResult, RemoteSyncResult, RepositorySnapshot, ServiceError, TollgateService,
    WorktreeOperationResult,
};

type Service = Arc<TollgateService>;

const STRUCTURED_SERVICE_ERROR_PREFIX: &str = "tollgate-structured-error:";

struct QuitCoordinator {
    confirmed: AtomicBool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct NotificationPreferences {
    quiet_mode: bool,
    muted_repositories: HashSet<RepositoryId>,
}

struct NotificationPreferencesState {
    path: PathBuf,
    preferences: tokio::sync::RwLock<NotificationPreferences>,
}

#[derive(Serialize)]
struct CliInstallStatus {
    bundled_available: bool,
    installed: bool,
    destination: String,
    directory_on_path: bool,
}

#[tauri::command]
async fn snapshot(service: tauri::State<'_, Service>) -> Result<AppSnapshot, String> {
    service.snapshot().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn initialize_repository(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    path: String,
    run: Option<String>,
    detach_master: bool,
) -> Result<RepositorySnapshot, String> {
    let result = service
        .initialize_repository_with_policy(path, run, true, detach_master)
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn approve(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    revision: String,
    worktree_path: Option<String>,
) -> Result<ApproveResult, String> {
    let result = service
        .approve_from(repository_id, revision, worktree_path, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn submit_candidate(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    revision: String,
    worktree_path: Option<String>,
) -> Result<ApproveResult, String> {
    let result = service
        .submit_candidate_from(repository_id, revision, worktree_path, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn authorize_candidate(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    expected_revision: u64,
) -> Result<CandidateAuthorizationResult, String> {
    let result = service
        .authorize_candidate(repository_id, item_id, expected_revision, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn check(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    revision: String,
    worktree_path: Option<String>,
) -> Result<ApproveResult, String> {
    let result = service
        .check_from(repository_id, revision, worktree_path, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn retry_item(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    cold: bool,
) -> Result<ApproveResult, String> {
    let result = service
        .retry(repository_id, item_id, cold, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn cancel_item(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    expected_revision: u64,
) -> Result<(), String> {
    service
        .cancel_command(repository_id, item_id, expected_revision, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(())
}

#[tauri::command]
async fn reorder_queue(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    selected_ids: Vec<QueueItemId>,
    expected_revision: u64,
) -> Result<QueueReorderResult, String> {
    let result = service
        .reorder_queue(
            repository_id,
            selected_ids,
            expected_revision,
            CommandId::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn pull(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<RemoteSyncResult, String> {
    let result = service
        .pull(repository_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn push(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<RemoteSyncResult, String> {
    let result = service
        .push(repository_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn reconcile(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    expected_observed_master: Option<tollgate_domain::GitOid>,
    expected_queue_revision: Option<u64>,
) -> Result<RemoteSyncResult, String> {
    let result = service
        .reconcile_expected(
            repository_id,
            expected_observed_master,
            expected_queue_revision,
            CommandId::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn update_feature(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    worktree_path: String,
) -> Result<WorktreeOperationResult, String> {
    let result = service
        .update_feature_worktree(repository_id, worktree_path, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn create_worktree(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    branch: String,
    destination: Option<String>,
) -> Result<WorktreeOperationResult, String> {
    let result = service
        .create_worktree(repository_id, branch, destination, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn remove_worktree(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    path: String,
) -> Result<WorktreeOperationResult, String> {
    let result = service
        .remove_worktree(repository_id, path, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn set_paused(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    paused: bool,
) -> Result<(), String> {
    service
        .set_paused_command(repository_id, paused, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(())
}

#[tauri::command]
async fn reload_environment(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
) -> Result<EnvironmentView, String> {
    let result = service
        .reload_environment()
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn apply_configuration(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<RepositorySnapshot, String> {
    let result = service
        .apply_configuration(repository_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    Ok(result)
}

#[tauri::command]
async fn regenerate_configuration(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<serde_json::Value, String> {
    let result = service
        .regenerate_configuration(repository_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn reset_slot(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    slot_id: SlotId,
) -> Result<serde_json::Value, String> {
    let result = service
        .reset_slot(repository_id, slot_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn snapshot_cache(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<serde_json::Value, String> {
    let result = service
        .snapshot_cache(repository_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn purge_cache(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    all_slots: bool,
) -> Result<serde_json::Value, String> {
    let result = service
        .purge_cache(repository_id, all_slots, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn set_artifact_pinned(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    artifact_id: String,
    pinned: bool,
) -> Result<serde_json::Value, String> {
    let result = service
        .set_artifact_pinned(repository_id, artifact_id, pinned, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn prune_artifact(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    artifact_id: String,
) -> Result<serde_json::Value, String> {
    let result = service
        .prune_artifact(repository_id, artifact_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn remove_repository(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<serde_json::Value, String> {
    let result = service
        .unregister_repository_command(repository_id, CommandId::new())
        .await
        .map_err(|error| error.to_string())?;
    let _ = app.emit("tollgate://snapshot-changed", ());
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn logs(
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    buildset_id: Option<BuildsetId>,
    step: Option<String>,
    start_sequence: u64,
    tail: Option<bool>,
) -> Result<serde_json::Value, String> {
    let start_sequence = if tail.unwrap_or(false) {
        service
            .log_tail_sequence(repository_id, item_id, buildset_id, step.clone(), 2_000)
            .await
            .map_err(|error| error.to_string())?
    } else {
        start_sequence
    };
    let frames = service
        .logs(
            repository_id,
            item_id,
            buildset_id,
            step,
            start_sequence,
            2_000,
        )
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_value(frames).map_err(|error| error.to_string())
}

#[tauri::command]
async fn history_items(
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    offset: usize,
    limit: usize,
) -> Result<serde_json::Value, String> {
    let page = service
        .history_items_page(repository_id, offset, limit)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_value(page).map_err(|error| error.to_string())
}

#[tauri::command]
async fn item_details(
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    item_id: QueueItemId,
) -> Result<serde_json::Value, String> {
    let item = service
        .item_details(repository_id, item_id)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_value(item).map_err(|error| error.to_string())
}

#[tauri::command]
async fn open_raw_log(
    app: tauri::AppHandle,
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
    item_id: QueueItemId,
    buildset_id: Option<BuildsetId>,
    step: Option<String>,
) -> Result<(), String> {
    let path = service
        .raw_log_path(repository_id, item_id, buildset_id, step)
        .await
        .map_err(|error| error.to_string())?;
    app.opener()
        .open_path(path.to_string_lossy().into_owned(), None::<String>)
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn doctor(
    service: tauri::State<'_, Service>,
    repository_id: RepositoryId,
) -> Result<DoctorReport, String> {
    service
        .doctor(repository_id)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn confirm_quit(app: tauri::AppHandle, quit: tauri::State<'_, Arc<QuitCoordinator>>) {
    quit.confirmed.store(true, Ordering::Release);
    app.exit(0);
}

#[tauri::command]
fn cli_install_status() -> CliInstallStatus {
    let destination = cli_destination();
    let bundled = bundled_cli();
    let installed =
        destination
            .as_ref()
            .zip(bundled.as_ref())
            .is_some_and(|(destination, bundled)| {
                std::fs::canonicalize(destination).ok() == std::fs::canonicalize(bundled).ok()
            });
    let directory_on_path = destination
        .as_ref()
        .and_then(|path| path.parent())
        .is_some_and(|directory| {
            std::env::var_os("PATH").is_some_and(|path| {
                std::env::split_paths(&path).any(|candidate| candidate == directory)
            })
        });
    CliInstallStatus {
        bundled_available: bundled.is_some(),
        installed,
        destination: destination
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        directory_on_path,
    }
}

#[tauri::command]
fn install_cli() -> Result<CliInstallStatus, String> {
    #[cfg(unix)]
    {
        let bundled = bundled_cli().ok_or("the bundled tg executable is unavailable")?;
        let destination = cli_destination().ok_or("the home directory is unavailable")?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        if destination.exists() || std::fs::symlink_metadata(&destination).is_ok() {
            if std::fs::canonicalize(&destination).ok() == std::fs::canonicalize(&bundled).ok() {
                return Ok(cli_install_status());
            }
            let repairable_tollgate_link = std::fs::symlink_metadata(&destination)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
                && std::fs::read_link(&destination).is_ok_and(|target| {
                    target.is_absolute()
                        && target
                            .components()
                            .rev()
                            .take(4)
                            .map(|part| part.as_os_str().to_string_lossy().into_owned())
                            .collect::<Vec<_>>()
                            == ["tg", "MacOS", "Contents", "Tollgate.app"]
                });
            if repairable_tollgate_link {
                std::fs::remove_file(&destination).map_err(|error| error.to_string())?;
                std::os::unix::fs::symlink(&bundled, &destination)
                    .map_err(|error| error.to_string())?;
                return Ok(cli_install_status());
            }
            return Err(format!(
                "{} already exists and was not created by Tollgate",
                destination.display()
            ));
        }
        std::os::unix::fs::symlink(&bundled, &destination).map_err(|error| error.to_string())?;
        Ok(cli_install_status())
    }
    #[cfg(not(unix))]
    Err("CLI installation is supported only on macOS".into())
}

#[tauri::command]
async fn notification_preferences(
    preferences: tauri::State<'_, Arc<NotificationPreferencesState>>,
) -> Result<NotificationPreferences, String> {
    Ok(preferences.preferences.read().await.clone())
}

#[tauri::command]
async fn set_notification_preferences(
    preferences: tauri::State<'_, Arc<NotificationPreferencesState>>,
    quiet_mode: bool,
    muted_repositories: HashSet<RepositoryId>,
) -> Result<NotificationPreferences, String> {
    let value = NotificationPreferences {
        quiet_mode,
        muted_repositories,
    };
    let parent = preferences
        .path
        .parent()
        .ok_or("notification preferences have no parent")?;
    let temporary = parent.join(format!(
        ".notification-preferences-{}.json.tmp",
        uuid::Uuid::now_v7()
    ));
    let bytes = serde_json::to_vec_pretty(&value).map_err(|error| error.to_string())?;
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|error| error.to_string())?;
    use tokio::io::AsyncWriteExt;
    file.write_all(&bytes)
        .await
        .map_err(|error| error.to_string())?;
    file.sync_all().await.map_err(|error| error.to_string())?;
    tokio::fs::rename(&temporary, &preferences.path)
        .await
        .map_err(|error| error.to_string())?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    *preferences.preferences.write().await = value.clone();
    Ok(value)
}

pub fn run() {
    let support_root = ProjectDirs::from("dev", "Tollgate", "Tollgate")
        .expect("application support directory")
        .data_dir()
        .to_owned();
    let _authority = acquire_user_authority_lock(&support_root.join("app-authority.lock"))
        .expect("another Tollgate app authority is already active");
    let service = tauri::async_runtime::block_on(TollgateService::open_default())
        .expect("Tollgate service initialization failed");
    let notification_path = support_root.join("notification-preferences.json");
    let initial_notification_preferences = if notification_path.exists() {
        match std::fs::read(&notification_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        {
            Some(preferences) => preferences,
            None => {
                let preserved = notification_path
                    .with_extension(format!("corrupt-{}-json", uuid::Uuid::now_v7()));
                let _ = std::fs::rename(&notification_path, &preserved);
                eprintln!(
                    "Preserved malformed notification preferences at {}",
                    preserved.display()
                );
                NotificationPreferences::default()
            }
        }
    } else {
        NotificationPreferences::default()
    };
    let notification_state = Arc::new(NotificationPreferencesState {
        path: notification_path,
        preferences: tokio::sync::RwLock::new(initial_notification_preferences),
    });
    let quit = Arc::new(QuitCoordinator {
        confirmed: AtomicBool::new(false),
    });
    let app = tauri::Builder::default()
        .manage(service)
        .manage(quit.clone())
        .manage(notification_state)
        .setup(|app| {
            let service = app.state::<Service>().inner().clone();
            let notifications = service.clone();
            let notification_preferences = app
                .state::<Arc<NotificationPreferencesState>>()
                .inner()
                .clone();
            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                monitor_failure_notifications(notifications, app_handle, notification_preferences)
                    .await;
            });
            let path = ProjectDirs::from("dev", "Tollgate", "Tollgate")
                .expect("application support directory")
                .data_dir()
                .join("tollgate.sock");
            tauri::async_runtime::spawn(async move {
                if let Err(error) = serve_ipc(service, path).await {
                    eprintln!("Tollgate IPC server stopped: {error}");
                }
            });
            Ok(())
        })
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            snapshot,
            initialize_repository,
            approve,
            submit_candidate,
            authorize_candidate,
            check,
            retry_item,
            cancel_item,
            reorder_queue,
            pull,
            push,
            reconcile,
            update_feature,
            create_worktree,
            remove_worktree,
            set_paused,
            reload_environment,
            apply_configuration,
            regenerate_configuration,
            reset_slot,
            snapshot_cache,
            purge_cache,
            set_artifact_pinned,
            prune_artifact,
            remove_repository,
            logs,
            history_items,
            item_details,
            open_raw_log,
            doctor,
            confirm_quit,
            cli_install_status,
            install_cli,
            notification_preferences,
            set_notification_preferences
        ])
        .on_window_event(|window, event| {
            if window.label() == "main"
                && let tauri::WindowEvent::CloseRequested { api, .. } = event
            {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building Tollgate");
    let shutting_down = Arc::new(AtomicBool::new(false));
    app.run(move |app, event| {
        if let tauri::RunEvent::ExitRequested { api, .. } = event
            && !shutting_down.swap(true, Ordering::AcqRel)
        {
            api.prevent_exit();
            let service = app.state::<Service>().inner().clone();
            let app = app.clone();
            let quit = quit.clone();
            let shutting_down = shutting_down.clone();
            tauri::async_runtime::spawn(async move {
                let has_active_work = service.snapshot().await.is_ok_and(|snapshot| {
                    snapshot.repositories.iter().any(|repository| {
                        repository.resources.active_runs > 0
                            || repository.queue.iter().any(|item| {
                                matches!(
                                    item.item.state,
                                    tollgate_domain::QueueItemState::Preparing
                                        | tollgate_domain::QueueItemState::Running
                                )
                            })
                    })
                });
                if has_active_work && !quit.confirmed.load(Ordering::Acquire) {
                    let _ = app.emit("tollgate://quit-confirmation-required", ());
                    if let Some(window) = app.get_webview_window("main") {
                        let _ = window.show();
                        let _ = window.set_focus();
                    }
                    shutting_down.store(false, Ordering::Release);
                    return;
                }
                if let Err(error) = service.shutdown().await {
                    eprintln!("Tollgate orderly shutdown failed: {error}");
                }
                app.exit(0);
            });
        }
    });
}

async fn monitor_failure_notifications(
    service: Service,
    app: tauri::AppHandle,
    preferences: Arc<NotificationPreferencesState>,
) {
    let mut cursors = HashMap::new();
    let mut blocked = HashSet::new();
    if let Ok(snapshot) = service.snapshot().await {
        for repository in snapshot.repositories {
            cursors.insert(repository.state.id, repository.state.event_sequence);
            for reason in repository.state.block_reasons {
                blocked.insert((repository.state.id, reason.code));
            }
        }
    }
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let Ok(snapshot) = service.snapshot().await else {
            continue;
        };
        let snapshot_preferences = preferences.preferences.read().await.clone();
        for repository in snapshot.repositories {
            let notifications_enabled = !snapshot_preferences.quiet_mode
                && !snapshot_preferences
                    .muted_repositories
                    .contains(&repository.state.id);
            let cursor = cursors.entry(repository.state.id).or_insert(0);
            let starting_cursor = *cursor;
            let mut observed_cursor = starting_cursor;
            for event in repository
                .history
                .iter()
                .filter(|event| event.sequence > starting_cursor)
            {
                let state = event
                    .payload
                    .get("state")
                    .and_then(serde_json::Value::as_str);
                let remote = event
                    .payload
                    .get("remote_state")
                    .and_then(serde_json::Value::as_str);
                let reason = event
                    .payload
                    .get("terminal_reason")
                    .and_then(serde_json::Value::as_str);
                let user_master_sync_attention = event.kind == "user-master.sync-needs-attention";
                let failure = matches!(
                    state,
                    Some("failed" | "check-failed" | "infrastructure-exhausted")
                ) || remote == Some("push-blocked")
                    || reason == Some("baseline-failing")
                    || user_master_sync_attention;
                if failure && notifications_enabled {
                    let subject = event
                        .payload
                        .get("metadata")
                        .and_then(|metadata| metadata.get("subject"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("A Tollgate validation needs attention");
                    let body = reason.unwrap_or_else(|| {
                        if user_master_sync_attention {
                            event
                                .payload
                                .get("reason")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("Certified promotion succeeded, but local master still needs a manual fast-forward")
                        } else if remote == Some("push-blocked") {
                            "Remote push failed"
                        } else if state == Some("infrastructure-exhausted") {
                            "Infrastructure retries were exhausted"
                        } else {
                            "Validation failed"
                        }
                    });
                    let _ = app
                        .notification()
                        .builder()
                        .title(format!("{} · Tollgate", repository.state.name))
                        .body(format!("{subject}: {body}"))
                        .show();
                }
                observed_cursor = observed_cursor.max(event.sequence);
            }
            *cursor = observed_cursor;
            for reason in &repository.state.block_reasons {
                if blocked.insert((repository.state.id, reason.code.clone()))
                    && notifications_enabled
                {
                    let _ = app
                        .notification()
                        .builder()
                        .title(format!("{} is blocked", repository.state.name))
                        .body(&reason.message)
                        .show();
                }
            }
        }
    }
}

fn bundled_cli() -> Option<std::path::PathBuf> {
    let executable = std::env::current_exe().ok()?;
    let directory = executable.parent()?;
    (directory.file_name().and_then(|name| name.to_str()) == Some("MacOS"))
        .then(|| directory.join("tg"))
        .filter(|path| path.is_file())
}

fn cli_destination() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .map(|home| home.join(".local/bin/tg"))
}

async fn serve_ipc(service: Service, path: std::path::PathBuf) -> Result<(), String> {
    let listener = bind_user_socket(&path)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        if let Err(error) = verify_peer_uid(&stream) {
            eprintln!("Rejected IPC peer: {error}");
            continue;
        }
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_ipc_connection(service, stream).await {
                eprintln!("IPC connection closed: {error}");
            }
        });
    }
}

async fn handle_ipc_connection(
    service: Service,
    stream: tokio::net::UnixStream,
) -> Result<(), String> {
    let mut framed = Framed::new(stream, FrameCodec);
    let first = framed
        .next()
        .await
        .ok_or("client closed before handshake")?
        .map_err(|error| error.to_string())?;
    if first.kind != FrameKind::Handshake {
        return Err("first IPC frame must be a handshake".into());
    }
    let handshake: Handshake = first.decode_json().map_err(|error| error.to_string())?;
    if handshake.protocol_min > PROTOCOL_VERSION || handshake.protocol_max < PROTOCOL_VERSION {
        return Err("no mutually supported protocol version".into());
    }
    let ack = HandshakeAck {
        app_version: env!("CARGO_PKG_VERSION").into(),
        selected_protocol: PROTOCOL_VERSION,
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
            FrameKind::Ping,
            FrameKind::Pong,
        ],
    };
    framed
        .send(
            Frame::control(FrameKind::HandshakeAck, first.correlation_id, &ack)
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    while let Some(frame) = framed.next().await {
        let frame = frame.map_err(|error| error.to_string())?;
        if frame.kind == FrameKind::Ping {
            framed
                .send(Frame {
                    kind: FrameKind::Pong,
                    ..frame
                })
                .await
                .map_err(|error| error.to_string())?;
            continue;
        }
        if frame.kind != FrameKind::Request {
            return Err("unexpected non-request frame".into());
        }
        let command = frame
            .decode_json::<IpcCommand>()
            .map_err(|error| error.to_string())?;
        let response = execute_ipc_command(&service, command).await;
        framed
            .send(
                Frame::control(FrameKind::Response, frame.correlation_id, &response)
                    .map_err(|error| error.to_string())?,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn execute_ipc_command(service: &Service, command: IpcCommand) -> IpcResponse {
    let result: Result<serde_json::Value, String> = async {
        match command {
            IpcCommand::Snapshot => serde_json::to_value(
                service
                    .snapshot()
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ItemStatus {
                repository_id,
                item_id,
            } => serde_json::to_value(
                service
                    .item_status(repository_id, item_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ItemDetails {
                repository_id,
                item_id,
            } => serde_json::to_value(
                service
                    .item_details_by_id(repository_id, item_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ItemWaitStatus {
                repository_id,
                item_id,
            } => serde_json::to_value(
                service
                    .item_wait_status(repository_id, item_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Initialize {
                path,
                run,
                bootstrap,
                detach_master,
            } => serde_json::to_value(
                service
                    .initialize_repository_with_policy(path, run, bootstrap, detach_master)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Approve {
                repository_id,
                revision,
                worktree_path,
                purpose,
                retain_worktree,
                command_id,
            } => serde_json::to_value(
                service
                    .approve_from_with_cleanup_policy(
                        repository_id,
                        revision,
                        worktree_path,
                        purpose,
                        if retain_worktree {
                            CleanupPolicy::RetainWorktree
                        } else {
                            CleanupPolicy::Automatic
                        },
                        command_id,
                    )
                    .await
                    .map_err(encode_candidate_submission_error)?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Candidate {
                repository_id,
                revision,
                worktree_path,
                retain_worktree,
                command_id,
            } => serde_json::to_value(
                service
                    .submit_candidate_from_with_cleanup_policy(
                        repository_id,
                        revision,
                        worktree_path,
                        if retain_worktree {
                            CleanupPolicy::RetainWorktree
                        } else {
                            CleanupPolicy::Automatic
                        },
                        command_id,
                    )
                    .await
                    .map_err(encode_candidate_submission_error)?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::AuthorizeCandidate {
                repository_id,
                item_id,
                expected_revision,
                command_id,
            } => serde_json::to_value(
                service
                    .authorize_candidate(repository_id, item_id, expected_revision, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Check {
                repository_id,
                revision,
                worktree_path,
                command_id,
            } => serde_json::to_value(
                service
                    .check_from(repository_id, revision, worktree_path, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Diagnose {
                repository_id,
                item_id,
                replay,
                verify_repair,
                command_id,
            } => serde_json::to_value(
                service
                    .diagnose_failure(repository_id, item_id, replay, verify_repair, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Cancel {
                repository_id,
                item_id,
                expected_revision,
                command_id,
            } => {
                let result = service
                    .cancel_command(repository_id, item_id, expected_revision, command_id)
                    .await
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(result).map_err(|error| error.to_string())
            }
            IpcCommand::Retry {
                repository_id,
                item_id,
                cold,
                command_id,
            } => serde_json::to_value(
                service
                    .retry(repository_id, item_id, cold, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Reorder {
                repository_id,
                selected_ids,
                expected_revision,
                command_id,
            } => serde_json::to_value(
                service
                    .reorder_queue(repository_id, selected_ids, expected_revision, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Pull {
                repository_id,
                command_id,
            } => serde_json::to_value(
                service
                    .pull(repository_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Push {
                repository_id,
                command_id,
            } => serde_json::to_value(
                service
                    .push(repository_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Reconcile {
                repository_id,
                expected_observed_master,
                expected_queue_revision,
                command_id,
            } => serde_json::to_value(
                service
                    .reconcile_expected(
                        repository_id,
                        expected_observed_master,
                        expected_queue_revision,
                        command_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Update {
                repository_id,
                worktree_path,
                command_id,
            } => serde_json::to_value(
                service
                    .update_feature_worktree(repository_id, worktree_path, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::WorktreeCreate {
                repository_id,
                branch,
                destination,
                command_id,
            } => serde_json::to_value(
                service
                    .create_worktree(repository_id, branch, destination, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::WorktreeRemove {
                repository_id,
                path,
                command_id,
            } => serde_json::to_value(
                service
                    .remove_worktree(repository_id, path, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::RemoveRepository {
                repository_id,
                command_id,
            } => serde_json::to_value(
                service
                    .unregister_repository_command(repository_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ApplyConfiguration {
                repository_id,
                command_id,
            } => serde_json::to_value(
                service
                    .apply_configuration(repository_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ValidateConfiguration { repository_id } => serde_json::to_value(
                service
                    .validate_configuration(repository_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::RegenerateConfiguration {
                repository_id,
                command_id,
            } => serde_json::to_value(
                service
                    .regenerate_configuration(repository_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ResetSlot {
                repository_id,
                slot_id,
                command_id,
            } => serde_json::to_value(
                service
                    .reset_slot(repository_id, slot_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::SnapshotCache {
                repository_id,
                command_id,
            } => serde_json::to_value(
                service
                    .snapshot_cache(repository_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::PurgeCache {
                repository_id,
                all_slots,
                command_id,
            } => serde_json::to_value(
                service
                    .purge_cache(repository_id, all_slots, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::StorageStatus => serde_json::to_value(
                service
                    .storage_status()
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::StoragePrune { force, command_id } => serde_json::to_value(
                service
                    .prune_storage(force, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ArtifactPin {
                repository_id,
                artifact_id,
                pinned,
                command_id,
            } => serde_json::to_value(
                service
                    .set_artifact_pinned(repository_id, artifact_id, pinned, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::ArtifactPrune {
                repository_id,
                artifact_id,
                command_id,
            } => serde_json::to_value(
                service
                    .prune_artifact(repository_id, artifact_id, command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Logs {
                repository_id,
                item_id,
                buildset_id,
                step,
                start_sequence,
            } => serde_json::to_value(
                service
                    .logs(
                        repository_id,
                        item_id,
                        buildset_id,
                        step,
                        start_sequence,
                        10_000,
                    )
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Doctor { repository_id } => serde_json::to_value(
                service
                    .doctor(repository_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
            IpcCommand::Pause {
                repository_id,
                command_id,
            } => {
                let result = service
                    .set_paused_command(repository_id, true, command_id)
                    .await
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(result).map_err(|error| error.to_string())
            }
            IpcCommand::Resume {
                repository_id,
                command_id,
            } => {
                let result = service
                    .set_paused_command(repository_id, false, command_id)
                    .await
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(result).map_err(|error| error.to_string())
            }
            IpcCommand::ReloadEnvironment { command_id } => serde_json::to_value(
                service
                    .reload_environment_command(command_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
        }
    }
    .await;
    match result {
        Ok(value) => IpcResponse {
            ok: true,
            result: Some(value),
            error: None,
        },
        Err(message) => {
            let error = message
                .strip_prefix(STRUCTURED_SERVICE_ERROR_PREFIX)
                .and_then(|encoded| serde_json::from_str(encoded).ok())
                .unwrap_or_else(|| StructuredError {
                    code: classify_service_error(&message).into(),
                    message,
                    retryable: false,
                    details: None,
                });
            IpcResponse {
                ok: false,
                result: None,
                error: Some(error),
            }
        }
    }
}

fn encode_candidate_submission_error(error: ServiceError) -> String {
    let message = error.to_string();
    let structured = match error {
        ServiceError::StaleQueuePrefix {
            source_parent_oid,
            release_oid,
            queue_revision,
            current_prefix_oid,
        } => Some(StructuredError {
            code: "stale-queue-prefix".into(),
            message: message.clone(),
            retryable: true,
            details: Some(serde_json::json!({
                "source_parent_oid": source_parent_oid,
                "release_oid": release_oid,
                "queue_revision": queue_revision,
                "current_prefix_oid": current_prefix_oid,
                "retry": "Rebase the single task commit onto release_oid only, never current_prefix_oid; resolve and regenerate, then resubmit."
            })),
        }),
        ServiceError::UnknownSourceAncestor {
            ancestor,
            release_oid,
            queue_revision,
            current_prefix_oid,
        } => Some(StructuredError {
            code: "unknown-source-ancestor".into(),
            message: message.clone(),
            retryable: true,
            details: Some(serde_json::json!({
                "ancestor_oid": ancestor,
                "release_oid": release_oid,
                "queue_revision": queue_revision,
                "current_prefix_oid": current_prefix_oid,
                "retry": "Rebase the single task commit onto release_oid only, never current_prefix_oid, then resubmit."
            })),
        }),
        ServiceError::UnpromotedSourceAncestor {
            ancestor,
            release_oid,
        } => Some(StructuredError {
            code: "unpromoted-source-ancestor".into(),
            message: message.clone(),
            retryable: true,
            details: Some(serde_json::json!({
                "ancestor_oid": ancestor,
                "release_oid": release_oid,
                "retry": "Rebase the single task commit onto release_oid only, then resubmit."
            })),
        }),
        _ => None,
    };
    structured
        .and_then(|error| serde_json::to_string(&error).ok())
        .map(|encoded| format!("{STRUCTURED_SERVICE_ERROR_PREFIX}{encoded}"))
        .unwrap_or(message)
}

fn classify_service_error(message: &str) -> &'static str {
    if message.contains("revision conflict") {
        "revision-conflict"
    } else if message.contains("configuration") {
        "configuration-invalid"
    } else if message.contains("not registered") || message.contains("not found") {
        "not-found"
    } else if message.contains("blocked") || message.contains("unavailable") {
        "repository-blocked"
    } else {
        "service-error"
    }
}

#[cfg(test)]
mod ipc_error_tests {
    use super::*;
    use tollgate_domain::GitOid;

    #[test]
    fn stale_candidate_error_preserves_retry_context() {
        let release_oid = GitOid::from_hex("1111111111111111111111111111111111111111").unwrap();
        let source_parent_oid =
            GitOid::from_hex("2222222222222222222222222222222222222222").unwrap();
        let current_prefix_oid =
            GitOid::from_hex("3333333333333333333333333333333333333333").unwrap();
        let encoded = encode_candidate_submission_error(ServiceError::StaleQueuePrefix {
            source_parent_oid: source_parent_oid.clone(),
            release_oid: release_oid.clone(),
            queue_revision: 42,
            current_prefix_oid: current_prefix_oid.clone(),
        });
        let error: StructuredError = serde_json::from_str(
            encoded
                .strip_prefix(STRUCTURED_SERVICE_ERROR_PREFIX)
                .expect("stale errors must cross IPC as structured errors"),
        )
        .unwrap();
        assert_eq!(error.code, "stale-queue-prefix");
        assert!(error.retryable);
        assert_eq!(error.details.as_ref().unwrap()["queue_revision"], 42);
        assert_eq!(
            error.details.as_ref().unwrap()["release_oid"],
            serde_json::to_value(release_oid).unwrap()
        );
        assert_eq!(
            error.details.as_ref().unwrap()["source_parent_oid"],
            serde_json::to_value(source_parent_oid).unwrap()
        );
        assert_eq!(
            error.details.as_ref().unwrap()["current_prefix_oid"],
            serde_json::to_value(current_prefix_oid).unwrap()
        );
        assert!(
            error.details.as_ref().unwrap()["retry"]
                .as_str()
                .unwrap()
                .contains("release_oid only")
        );
    }

    #[test]
    fn unpromoted_source_error_never_exposes_the_speculative_prefix_as_a_base() {
        let release_oid = GitOid::from_hex("1111111111111111111111111111111111111111").unwrap();
        let ancestor = GitOid::from_hex("2222222222222222222222222222222222222222").unwrap();
        let encoded = encode_candidate_submission_error(ServiceError::UnpromotedSourceAncestor {
            ancestor: ancestor.clone(),
            release_oid: release_oid.clone(),
        });
        let error: StructuredError = serde_json::from_str(
            encoded
                .strip_prefix(STRUCTURED_SERVICE_ERROR_PREFIX)
                .expect("unpromoted ancestry must cross IPC as a structured error"),
        )
        .unwrap();
        assert_eq!(error.code, "unpromoted-source-ancestor");
        assert!(error.retryable);
        assert_eq!(
            error.details.as_ref().unwrap()["release_oid"],
            serde_json::to_value(release_oid).unwrap()
        );
        assert_eq!(
            error.details.as_ref().unwrap()["ancestor_oid"],
            serde_json::to_value(ancestor).unwrap()
        );
        assert!(
            !error.details.as_ref().unwrap()["retry"]
                .as_str()
                .unwrap()
                .contains("prefix")
        );
    }
}
