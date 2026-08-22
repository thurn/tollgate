#![forbid(unsafe_code)]

use std::{fs::File, io, path::Path};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Decoder, Encoder};
use tollgate_domain::{BuildsetId, CommandId, GitOid, QueueItemId, RepositoryId, SlotId};
use uuid::Uuid;

pub const MAGIC: [u8; 4] = *b"TGL1";
pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_CONTROL_PAYLOAD: usize = 8 * 1024 * 1024;
pub const MAX_LOG_PAYLOAD: usize = 1024 * 1024;
const HEADER_SIZE: usize = 28;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FrameKind {
    Handshake = 1,
    HandshakeAck = 2,
    Request = 3,
    Response = 4,
    Event = 5,
    Log = 6,
    Gap = 7,
    Ping = 8,
    Pong = 9,
}

impl TryFrom<u8> for FrameKind {
    type Error = ProtocolError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Handshake),
            2 => Ok(Self::HandshakeAck),
            3 => Ok(Self::Request),
            4 => Ok(Self::Response),
            5 => Ok(Self::Event),
            6 => Ok(Self::Log),
            7 => Ok(Self::Gap),
            8 => Ok(Self::Ping),
            9 => Ok(Self::Pong),
            _ => Err(ProtocolError::UnknownFrameKind(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub version: u16,
    pub kind: FrameKind,
    pub flags: u8,
    pub correlation_id: Uuid,
    pub payload: Bytes,
}

impl Frame {
    pub fn control<T: Serialize>(
        kind: FrameKind,
        correlation_id: Uuid,
        value: &T,
    ) -> Result<Self, ProtocolError> {
        let payload = serde_json::to_vec(value)?;
        if payload.len() > MAX_CONTROL_PAYLOAD {
            return Err(ProtocolError::OversizedPayload {
                declared: payload.len(),
                maximum: MAX_CONTROL_PAYLOAD,
            });
        }
        Ok(Self {
            version: PROTOCOL_VERSION,
            kind,
            flags: 0,
            correlation_id,
            payload: payload.into(),
        })
    }

    pub fn decode_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, ProtocolError> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid frame magic")]
    InvalidMagic,
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown mandatory frame kind {0}")]
    UnknownFrameKind(u8),
    #[error("payload length {declared} exceeds maximum {maximum}")]
    OversizedPayload { declared: usize, maximum: usize },
    #[error("zero payload is not valid for this frame kind")]
    EmptyPayload,
    #[error("control JSON is malformed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("peer user ID {actual} does not match current user {expected}")]
    PeerUid { expected: u32, actual: u32 },
}

#[derive(Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if source.len() < HEADER_SIZE {
            return Ok(None);
        }
        if source[..4] != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }
        let version = u16::from_be_bytes([source[4], source[5]]);
        if version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }
        let kind = FrameKind::try_from(source[6])?;
        let flags = source[7];
        let correlation_id = Uuid::from_slice(&source[8..24])
            .map_err(|_| ProtocolError::InvalidMagic)
            .expect("16 byte UUID frame");
        let declared =
            u32::from_be_bytes([source[24], source[25], source[26], source[27]]) as usize;
        let maximum = if kind == FrameKind::Log {
            MAX_LOG_PAYLOAD
        } else {
            MAX_CONTROL_PAYLOAD
        };
        if declared > maximum {
            return Err(ProtocolError::OversizedPayload { declared, maximum });
        }
        if declared == 0 && !matches!(kind, FrameKind::Ping | FrameKind::Pong) {
            return Err(ProtocolError::EmptyPayload);
        }
        if source.len() < HEADER_SIZE + declared {
            source.reserve(HEADER_SIZE + declared - source.len());
            return Ok(None);
        }
        source.advance(HEADER_SIZE);
        let payload = source.split_to(declared).freeze();
        Ok(Some(Frame {
            version,
            kind,
            flags,
            correlation_id,
            payload,
        }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = ProtocolError;
    fn encode(&mut self, frame: Frame, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let maximum = if frame.kind == FrameKind::Log {
            MAX_LOG_PAYLOAD
        } else {
            MAX_CONTROL_PAYLOAD
        };
        if frame.payload.len() > maximum {
            return Err(ProtocolError::OversizedPayload {
                declared: frame.payload.len(),
                maximum,
            });
        }
        if frame.payload.is_empty() && !matches!(frame.kind, FrameKind::Ping | FrameKind::Pong) {
            return Err(ProtocolError::EmptyPayload);
        }
        destination.reserve(HEADER_SIZE + frame.payload.len());
        destination.put_slice(&MAGIC);
        destination.put_u16(frame.version);
        destination.put_u8(frame.kind as u8);
        destination.put_u8(frame.flags);
        destination.put_slice(frame.correlation_id.as_bytes());
        destination.put_u32(frame.payload.len() as u32);
        destination.put_slice(&frame.payload);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Handshake {
    #[serde(default)]
    pub client_instance_id: tollgate_domain::ClientInstanceId,
    pub client_version: String,
    pub protocol_min: u16,
    pub protocol_max: u16,
    pub schema_version: u16,
    pub max_control_payload: u32,
    pub max_log_payload: u32,
    pub supported_frame_kinds: Vec<FrameKind>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HandshakeAck {
    pub app_version: String,
    pub selected_protocol: u16,
    pub schema_version: u16,
    pub max_control_payload: u32,
    pub max_log_payload: u32,
    pub supported_frame_kinds: Vec<FrameKind>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum IpcCommand {
    Snapshot,
    ItemStatus {
        repository_id: RepositoryId,
        item_id: QueueItemId,
    },
    ItemDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        repository_id: Option<RepositoryId>,
        item_id: QueueItemId,
    },
    ItemWaitStatus {
        repository_id: RepositoryId,
        item_id: QueueItemId,
    },
    Initialize {
        path: String,
        run: Option<String>,
        #[serde(default = "default_true")]
        bootstrap: bool,
        #[serde(default)]
        detach_master: bool,
    },
    Approve {
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        #[serde(default)]
        purpose: Option<String>,
        #[serde(default)]
        retain_worktree: bool,
        command_id: CommandId,
    },
    Candidate {
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        #[serde(default)]
        retain_worktree: bool,
        command_id: CommandId,
    },
    AuthorizeCandidate {
        repository_id: RepositoryId,
        item_id: QueueItemId,
        expected_revision: u64,
        command_id: CommandId,
    },
    Check {
        repository_id: RepositoryId,
        revision: String,
        worktree_path: Option<String>,
        command_id: CommandId,
    },
    Diagnose {
        repository_id: RepositoryId,
        item_id: QueueItemId,
        #[serde(default)]
        replay: bool,
        #[serde(default)]
        verify_repair: bool,
        command_id: CommandId,
    },
    Cancel {
        repository_id: RepositoryId,
        item_id: QueueItemId,
        expected_revision: u64,
        command_id: CommandId,
    },
    Retry {
        repository_id: RepositoryId,
        item_id: QueueItemId,
        cold: bool,
        command_id: CommandId,
    },
    Reorder {
        repository_id: RepositoryId,
        selected_ids: Vec<QueueItemId>,
        expected_revision: u64,
        command_id: CommandId,
    },
    Pull {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    Push {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    Reconcile {
        repository_id: RepositoryId,
        #[serde(default)]
        expected_observed_master: Option<GitOid>,
        #[serde(default)]
        expected_queue_revision: Option<u64>,
        command_id: CommandId,
    },
    Update {
        repository_id: RepositoryId,
        worktree_path: String,
        command_id: CommandId,
    },
    WorktreeCreate {
        repository_id: RepositoryId,
        branch: String,
        destination: Option<String>,
        #[serde(default)]
        warm: bool,
        command_id: CommandId,
    },
    WorktreeRemove {
        repository_id: RepositoryId,
        path: String,
        command_id: CommandId,
    },
    RemoveRepository {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    ApplyConfiguration {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    ValidateConfiguration {
        repository_id: RepositoryId,
    },
    RegenerateConfiguration {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    ResetSlot {
        repository_id: RepositoryId,
        slot_id: SlotId,
        command_id: CommandId,
    },
    SnapshotCache {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    PurgeCache {
        repository_id: RepositoryId,
        all_slots: bool,
        command_id: CommandId,
    },
    ArtifactPin {
        repository_id: RepositoryId,
        artifact_id: String,
        pinned: bool,
        command_id: CommandId,
    },
    ArtifactPrune {
        repository_id: RepositoryId,
        artifact_id: String,
        command_id: CommandId,
    },
    Logs {
        repository_id: RepositoryId,
        item_id: QueueItemId,
        #[serde(default)]
        buildset_id: Option<BuildsetId>,
        step: Option<String>,
        start_sequence: u64,
    },
    Doctor {
        repository_id: RepositoryId,
    },
    Pause {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    Resume {
        repository_id: RepositoryId,
        command_id: CommandId,
    },
    ReloadEnvironment {
        command_id: CommandId,
    },
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IpcResponse {
    pub ok: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<StructuredError>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StructuredError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

pub struct UserSocketListener {
    listener: UnixListener,
    #[cfg(unix)]
    _authority_lock: nix::fcntl::Flock<File>,
}

#[cfg(unix)]
pub struct UserAuthorityLock(nix::fcntl::Flock<File>);

#[cfg(unix)]
pub fn acquire_user_authority_lock(path: &Path) -> Result<UserAuthorityLock, ProtocolError> {
    use nix::fcntl::{Flock, FlockArg};
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let lock = Flock::lock(lock_file, FlockArg::LockExclusiveNonblock).map_err(|(_, error)| {
        io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("another Tollgate authority owns the app lock: {error}"),
        )
    })?;
    Ok(UserAuthorityLock(lock))
}

impl UserSocketListener {
    pub async fn accept(&self) -> io::Result<(UnixStream, tokio::net::unix::SocketAddr)> {
        self.listener.accept().await
    }
}

pub async fn bind_user_socket(path: &Path) -> Result<UserSocketListener, ProtocolError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    #[cfg(unix)]
    let authority_lock = acquire_user_authority_lock(&path.with_extension("lock"))?.0;
    if path.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{FileTypeExt, MetadataExt};
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.file_type().is_socket()
                || metadata.uid() != nix::unistd::Uid::current().as_raw()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to replace an unowned or non-socket IPC path",
                )
                .into());
            }
        }
        if UnixStream::connect(path).await.is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "another Tollgate authority is already listening",
            )
            .into());
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(UserSocketListener {
        listener,
        #[cfg(unix)]
        _authority_lock: authority_lock,
    })
}

pub fn verify_peer_uid(stream: &UnixStream) -> Result<(), ProtocolError> {
    let expected = nix::unistd::Uid::current().as_raw();
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    let actual = nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::LocalPeerCred)
        .map_err(io::Error::other)?
        .uid();
    #[cfg(target_os = "linux")]
    let actual = nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
        .map_err(io::Error::other)?
        .uid();
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
    let actual = expected;
    if actual != expected {
        return Err(ProtocolError::PeerUid { expected, actual });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_frames_resume_without_skipping_bytes() {
        let original = Frame::control(
            FrameKind::Request,
            Uuid::now_v7(),
            &serde_json::json!({"hello":"world"}),
        )
        .unwrap();
        let mut encoded = BytesMut::new();
        FrameCodec.encode(original.clone(), &mut encoded).unwrap();
        let second = encoded.split_off(10);
        let mut codec = FrameCodec;
        assert!(codec.decode(&mut encoded).unwrap().is_none());
        encoded.extend_from_slice(&second);
        assert_eq!(codec.decode(&mut encoded).unwrap().unwrap(), original);
    }

    #[test]
    fn oversized_declaration_fails_before_payload_allocation() {
        let mut bytes = BytesMut::new();
        bytes.put_slice(&MAGIC);
        bytes.put_u16(PROTOCOL_VERSION);
        bytes.put_u8(FrameKind::Log as u8);
        bytes.put_u8(0);
        bytes.put_slice(Uuid::nil().as_bytes());
        bytes.put_u32((MAX_LOG_PAYLOAD + 1) as u32);
        assert!(matches!(
            FrameCodec.decode(&mut bytes),
            Err(ProtocolError::OversizedPayload { .. })
        ));
    }

    #[test]
    fn item_details_can_resolve_a_globally_unique_item_without_a_repository_snapshot() {
        let item_id = QueueItemId::new();
        let value = serde_json::to_value(IpcCommand::ItemDetails {
            repository_id: None,
            item_id,
        })
        .unwrap();
        assert_eq!(value["command"], "item-details");
        assert_eq!(value["item_id"], item_id.to_string());
        assert!(value.get("repository_id").is_none());
        assert_eq!(
            serde_json::from_value::<IpcCommand>(value).unwrap(),
            IpcCommand::ItemDetails {
                repository_id: None,
                item_id,
            }
        );
    }

    #[tokio::test]
    async fn live_authority_socket_cannot_be_replaced() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("tollgate.sock");
        let first = bind_user_socket(&path).await.unwrap();
        let error = bind_user_socket(&path).await.err().unwrap();
        assert!(error.to_string().contains("authority"));
        drop(first);
        bind_user_socket(&path).await.unwrap();
    }
}
