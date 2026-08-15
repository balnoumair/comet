//! zeron-engine — the local desktop backend: sessions, documents, journals,
//! repositories, terminals, diffs, and the localhost RPC server.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
pub use zeron_proto::{EngineInfo, HarnessId, WorkspaceScope};
use zeron_rpc::{RpcError, RpcReply, RpcService, methods};
use zeron_sync::DocsStore;

pub mod agent_accounts;
pub mod diff_sync;
pub mod doc_host;
pub mod instance_lock;
pub mod profile;
pub mod registry;
pub mod repos;
pub mod rpc;
pub mod run_journal;
pub mod sessions;
pub mod spaces;
pub mod terminals;
pub mod titles;
pub mod uploads;
pub mod workspace_host;

pub use agent_accounts::{AgentAccounts, AgentAccountsConfig};
pub use diff_sync::{
    CheckoutDiffSync, DiffFileTextPair, DiffSnapshot, TurnSnapshot, capture_commit_diff,
    capture_diff, capture_diff_against, capture_turn_diff, merge_base, read_diff_file_text,
    snapshot_tree, working_diff_base,
};
pub use doc_host::{ChatDocHandle, DocHost, DocHostConfig};
pub use instance_lock::InstanceLock;
pub use profile::EngineProfile;
pub use registry::{HarnessDescriptor, HarnessRegistry, default_registry};
pub use repos::{CheckoutIdentity, Repos, worktree_branch_from_title};
pub use rpc::EngineRpc;
pub use run_journal::{JournalError, RunJournal};
pub use sessions::{JournaledEvent, SessionsEngine, SteerOutcome};
pub use spaces::SpacesSync;
pub use terminals::Terminals;
pub use titles::TitleGenerator;
pub use uploads::{AttachmentChunk, Uploads};
pub use workspace_host::{WORKSPACE_DOC_ID, WorkspaceHost, WorkspaceHostConfig};

pub(crate) const LEGACY_UNKNOWN_DEVICE_NAME: &str = "unknown-device";

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("doc: {0}")]
    Doc(#[from] zeron_doc::DocError),
    #[error("journal: {0}")]
    Journal(#[from] run_journal::JournalError),
    #[error("store: {0}")]
    Store(#[from] zeron_sync::StoreError),
    #[error("harness: {0}")]
    Harness(#[from] zeron_harness::HarnessError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub ipc_port: u16,
    pub default_harness: HarnessId,
}

pub struct EngineCore {
    pub sessions: SessionsEngine,
    pub doc_host: DocHost,
    pub workspace: WorkspaceHost,
    pub registry: Arc<HarnessRegistry>,
    pub repos: Repos,
    pub terminals: Terminals,
    pub diff_sync: CheckoutDiffSync,
    pub spaces_sync: SpacesSync,
    pub uploads: Uploads,
    pub agent_accounts: AgentAccounts,
    pub device_id: String,
    workspace_scope: WorkspaceScope,
    _instance_lock: InstanceLock,
}

impl EngineCore {
    pub fn assemble(
        data_dir: &Path,
        registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
    ) -> Result<Self, EngineError> {
        let profile = EngineProfile::local(data_dir)?;
        Self::assemble_with_profile(profile, registry, default_harness)
    }

    pub fn assemble_with_profile(
        profile: EngineProfile,
        registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
    ) -> Result<Self, EngineError> {
        let lock = InstanceLock::acquire(profile.device_root())?;
        Self::assemble_with_profile_locked(profile, registry, default_harness, lock)
    }

    pub fn assemble_with_profile_locked(
        profile: EngineProfile,
        registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
        lock: InstanceLock,
    ) -> Result<Self, EngineError> {
        let data_dir = profile.device_root();
        std::fs::create_dir_all(data_dir)?;
        let device_id = load_or_create_device_id(data_dir)?;
        registry.load_prefs(data_dir);

        let store = Arc::new(DocsStore::open(profile.store_root())?);
        let journal = Arc::new(RunJournal::open(profile.store_root().join("journals"))?);
        let sessions = SessionsEngine::new(device_id.clone(), journal, registry.clone());
        let doc_host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: device_id.clone(),
                default_harness,
            },
        );
        let workspace = WorkspaceHost::open(
            store,
            WorkspaceHostConfig {
                device_id: device_id.clone(),
                device_name: local_device_name(&device_id),
                platform: std::env::consts::OS.to_string(),
            },
        )?;
        doc_host.set_workspace(workspace.clone());
        doc_host.set_sessions(sessions.clone());
        sessions.set_doc_host(doc_host.clone());
        match sessions.recover_stale() {
            Ok(0) => {}
            Ok(recovered) => tracing::info!(recovered, "stale sessions recovered on boot"),
            Err(err) => tracing::error!(error = %err, "stale-session recovery failed"),
        }

        let repos = Repos::new(data_dir, &device_id);
        let terminals = Terminals::new();
        let uploads = Uploads::from_root(profile.uploads_root());
        let agent_accounts = AgentAccounts::new(AgentAccountsConfig::detect(data_dir));
        sessions.set_titles(TitleGenerator::new(
            workspace.clone(),
            registry.clone(),
            repos.clone(),
        ));
        let diff_sync = CheckoutDiffSync::start(repos.clone(), workspace.clone(), &device_id);
        let turn_diff = diff_sync.clone();
        sessions.set_turn_listener(Arc::new(move |chat_id, cwd| {
            turn_diff.note_turn_start(chat_id, cwd);
        }));
        let spaces_sync = SpacesSync::start(repos.clone(), workspace.clone(), &device_id);

        Ok(Self {
            sessions,
            doc_host,
            workspace,
            registry,
            repos,
            terminals,
            diff_sync,
            spaces_sync,
            uploads,
            agent_accounts,
            device_id,
            workspace_scope: WorkspaceScope::Local,
            _instance_lock: lock,
        })
    }

    pub fn workspace_scope(&self) -> WorkspaceScope {
        self.workspace_scope
    }

    pub fn rpc_service(&self) -> Arc<EngineRpc> {
        Arc::new(EngineRpc::new(
            self.sessions.clone(),
            self.doc_host.clone(),
            self.workspace.clone(),
            self.registry.clone(),
            self.repos.clone(),
            self.terminals.clone(),
            self.diff_sync.clone(),
            self.uploads.clone(),
            self.agent_accounts.clone(),
            WorkspaceScope::Local,
        ))
    }

    pub async fn shutdown(&self) {
        self.sessions.shutdown().await;
        self.terminals.shutdown();
        self.agent_accounts.shutdown();
        self.diff_sync.shutdown().await;
        self.spaces_sync.shutdown().await;
        self.doc_host.shutdown_workers().await;
        self.doc_host.flush_all();
        self.workspace.shutdown();
        self.sessions.clear_doc_host();
    }
}

pub struct Engine {
    pub config: EngineConfig,
}

pub struct EngineRuntime {
    core: EngineCore,
}

struct HeadlessRpc {
    inner: Arc<dyn RpcService>,
    stop_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait]
impl RpcService for HeadlessRpc {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        if method != methods::STOP_ENGINE {
            return self.inner.handle(method, params).await;
        }
        let stop_tx = self.stop_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let _ = stop_tx.send(());
        });
        RpcReply::value(&serde_json::json!({ "ok": true }))
    }
}

impl EngineRuntime {
    pub fn core(&self) -> &EngineCore {
        &self.core
    }

    pub fn workspace_scope(&self) -> WorkspaceScope {
        self.core.workspace_scope()
    }

    pub async fn shutdown(&self) {
        self.core.shutdown().await;
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self { config }
    }

    pub fn engine_info(config: &EngineConfig) -> Result<EngineInfo, EngineError> {
        std::fs::create_dir_all(&config.data_dir)?;
        Ok(EngineInfo {
            device_id: load_or_create_device_id(&config.data_dir)?,
            workspace_scope: WorkspaceScope::Local,
        })
    }

    pub async fn assemble_runtime(config: &EngineConfig) -> anyhow::Result<EngineRuntime> {
        let profile = EngineProfile::local(&config.data_dir)?;
        let core = EngineCore::assemble_with_profile(
            profile,
            Arc::new(default_registry()),
            config.default_harness,
        )?;
        tracing::info!(device_id = %core.device_id, "local engine core assembled");
        Ok(EngineRuntime { core })
    }

    pub async fn assemble_runtime_with_lock(
        config: &EngineConfig,
        lock: InstanceLock,
    ) -> anyhow::Result<EngineRuntime> {
        let profile = EngineProfile::local(&config.data_dir)?;
        let core = EngineCore::assemble_with_profile_locked(
            profile,
            Arc::new(default_registry()),
            config.default_harness,
            lock,
        )?;
        tracing::info!(device_id = %core.device_id, "local engine core assembled");
        Ok(EngineRuntime { core })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let config = self.config;
        tracing::info!(data_dir = %config.data_dir.display(), "local engine starting");
        std::fs::create_dir_all(&config.data_dir)?;

        let runtime = Self::assemble_runtime(&config).await?;
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::unbounded_channel();
        let service: Arc<dyn RpcService> = Arc::new(HeadlessRpc {
            inner: runtime.core().rpc_service(),
            stop_tx,
        });
        let server = serve_ipc(config.ipc_port, service).await?;
        tokio::select! {
            result = shutdown_signal() => result?,
            requested = stop_rx.recv() => {
                if requested.is_some() {
                    tracing::info!("headless shutdown requested over IPC");
                }
            }
        }
        server.abort();
        runtime.shutdown().await;
        Ok(())
    }
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

pub async fn serve_ipc(
    port: u16,
    service: Arc<dyn zeron_rpc::RpcService>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!(port, "IPC server listening");
    Ok(tokio::spawn(zeron_rpc::serve_ws_listener(
        listener, service,
    )))
}

fn local_device_name(device_id: &str) -> String {
    select_local_device_name(
        [
            std::env::var("ZERON_DEVICE_NAME").ok(),
            native_friendly_device_name(),
            std::env::var("HOSTNAME").ok(),
            gethostname::gethostname().into_string().ok(),
            std::fs::read_to_string("/etc/hostname").ok(),
        ],
        device_id,
        std::env::consts::OS,
    )
}

fn select_local_device_name(
    candidates: impl IntoIterator<Item = Option<String>>,
    device_id: &str,
    platform: &str,
) -> String {
    candidates
        .into_iter()
        .flatten()
        .map(|name| name.trim().to_string())
        .find(|name| !name.is_empty())
        .unwrap_or_else(|| {
            let platform = match platform {
                "macos" => "macOS",
                "windows" => "Windows",
                "linux" => "Linux",
                _ => "Local",
            };
            let short_id: String = device_id.chars().take(8).collect();
            format!("{platform} device {short_id}")
        })
}

#[cfg(target_os = "macos")]
fn native_friendly_device_name() -> Option<String> {
    let output = std::process::Command::new("/usr/sbin/scutil")
        .args(["--get", "ComputerName"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(not(target_os = "macos"))]
fn native_friendly_device_name() -> Option<String> {
    #[cfg(target_os = "windows")]
    return std::env::var("COMPUTERNAME").ok();

    #[cfg(not(target_os = "windows"))]
    None
}

#[cfg(test)]
mod device_name_tests {
    use super::select_local_device_name;

    fn name(candidates: &[Option<&str>], device_id: &str, platform: &str) -> String {
        select_local_device_name(
            candidates
                .iter()
                .map(|candidate| candidate.map(str::to_string)),
            device_id,
            platform,
        )
    }

    #[test]
    fn explicit_override_wins_and_is_trimmed() {
        assert_eq!(
            name(
                &[Some("  Studio Mac  "), Some("system-host")],
                "17bc0aa2-rest",
                "macos"
            ),
            "Studio Mac"
        );
    }

    #[test]
    fn native_friendly_name_wins_over_hostnames() {
        assert_eq!(
            name(
                &[
                    None,
                    Some("MacBook Pro de Jose"),
                    None,
                    Some("MacBook-Pro.local")
                ],
                "17bc0aa2-rest",
                "macos"
            ),
            "MacBook Pro de Jose"
        );
    }

    #[test]
    fn windows_computer_name_is_used_when_present() {
        assert_eq!(
            name(
                &[None, Some("DESKTOP-123"), Some("shell-host")],
                "17bc0aa2-rest",
                "windows"
            ),
            "DESKTOP-123"
        );
    }

    #[test]
    fn blank_candidates_are_ignored() {
        assert_eq!(
            name(
                &[Some("  "), None, Some("\n"), Some("linux-box")],
                "17bc0aa2-rest",
                "linux"
            ),
            "linux-box"
        );
    }

    #[test]
    fn final_fallback_is_platform_specific_and_distinct() {
        assert_eq!(
            name(&[None, Some(" ")], "17bc0aa2-rest", "linux"),
            "Linux device 17bc0aa2"
        );
    }
}

fn load_or_create_device_id(data_dir: &Path) -> Result<String, EngineError> {
    std::fs::create_dir_all(data_dir)?;
    let _identity_lock = DeviceIdentityLock::acquire(data_dir)?;
    let path = data_dir.join("device-id");
    let recovering_empty = match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => return Ok(id.trim().to_string()),
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => return Err(err.into()),
    };

    let id = new_id();
    let temp_path = data_dir.join(format!(
        ".device-id.tmp-{}-{}",
        std::process::id(),
        new_id()
    ));
    let write_result = (|| -> Result<(), EngineError> {
        let mut temp = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        temp.write_all(id.as_bytes())?;
        temp.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }

    let publish_result = if recovering_empty {
        match std::fs::read_to_string(&path) {
            Ok(existing) if !existing.trim().is_empty() => {
                let _ = std::fs::remove_file(&temp_path);
                return Ok(existing.trim().to_string());
            }
            Ok(_) => replace_empty_device_id(&temp_path, &path),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::hard_link(&temp_path, &path)
            }
            Err(err) => Err(err),
        }
    } else {
        std::fs::hard_link(&temp_path, &path)
    };
    let _ = std::fs::remove_file(&temp_path);
    match publish_result {
        Ok(()) => Ok(id),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let winner = std::fs::read_to_string(&path)?;
            if winner.trim().is_empty() {
                Err(EngineError::Other(format!(
                    "invalid device identity {}: file is empty",
                    path.display()
                )))
            } else {
                Ok(winner.trim().to_string())
            }
        }
        Err(err) => Err(err.into()),
    }
}

struct DeviceIdentityLock {
    _file: std::fs::File,
}

impl DeviceIdentityLock {
    fn acquire(data_dir: &Path) -> Result<Self, EngineError> {
        let path = data_dir.join("device-id.lock");
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
            let mut retries = 200;
            let file = loop {
                match options.open(&path) {
                    Ok(file) => break file,
                    Err(err)
                        if err.kind() == std::io::ErrorKind::PermissionDenied && retries > 0 =>
                    {
                        retries -= 1;
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(err) => return Err(err.into()),
                }
            };
            return Ok(Self { _file: file });
        }

        #[cfg(not(windows))]
        let file = options.open(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            loop {
                if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                    break;
                }
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EINTR) {
                    return Err(err.into());
                }
            }
        }
        #[cfg(not(windows))]
        Ok(Self { _file: file })
    }
}

fn replace_empty_device_id(temp_path: &Path, path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::rename(temp_path, path)
    }
    #[cfg(not(unix))]
    {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        std::fs::hard_link(temp_path, path)
    }
}
