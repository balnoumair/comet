//! Local workspace registry and its watch channels.
//!
//! The registry remains useful in local mode: it indexes spaces, chats,
//! devices, and live session status in one persisted snapshot. It is no longer
//! a network document and has no presence, room, or remote-device behavior.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use chrono::Utc;
use tokio::sync::watch;

use zeron_doc::{DeletedSpace, REGISTRY_DOC_ID, RegistryDoc, WorkspaceDoc};
use zeron_proto::{Chat, ChatConfig, Device, Session, Space};
use zeron_sync::DocsStore;

use crate::EngineError;

pub const WORKSPACE_DOC_ID: &str = "workspace2";
const LEGACY_WORKSPACE_DOC_ID: &str = "workspace";

#[derive(Debug, Clone)]
pub struct WorkspaceHostConfig {
    pub device_id: String,
    pub device_name: String,
    pub platform: String,
}

struct WorkspaceHostInner {
    store: Arc<DocsStore>,
    config: WorkspaceHostConfig,
    reg: Mutex<RegistryDoc>,
    chats_tx: watch::Sender<Vec<Chat>>,
    devices_tx: watch::Sender<Vec<Device>>,
    sessions_tx: watch::Sender<Vec<Session>>,
    spaces_tx: watch::Sender<Vec<Space>>,
    changed_tx: watch::Sender<u64>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct WorkspaceHost {
    inner: Arc<WorkspaceHostInner>,
}

impl WorkspaceHost {
    pub fn open(store: Arc<DocsStore>, config: WorkspaceHostConfig) -> Result<Self, EngineError> {
        let mut doc = match store.load_snapshot(REGISTRY_DOC_ID)? {
            Some(bytes) => RegistryDoc::from_bytes(&bytes, &config.device_id)
                .map_err(|e| EngineError::Other(format!("registry snapshot load failed: {e}")))?,
            None => {
                let mut doc = RegistryDoc::new(&config.device_id);
                if let Some(bytes) = store.load_snapshot(WORKSPACE_DOC_ID)? {
                    let raw = loro::LoroDoc::new();
                    if raw.import(&bytes).is_ok() {
                        if let Ok(state) = WorkspaceDoc::from_doc(raw).read_all() {
                            let _ = doc.seed_from_workspace(&state);
                        }
                    }
                }
                doc
            }
        };

        store.delete_snapshot(LEGACY_WORKSPACE_DOC_ID).ok();
        let now = Utc::now();
        let existing = doc
            .read_devices()?
            .into_iter()
            .find(|device| device.id == config.device_id);
        doc.upsert_device(&Device {
            id: config.device_id.clone(),
            name: device_name_on_boot(
                existing.as_ref().map(|device| device.name.as_str()),
                &config.device_name,
            ),
            platform: config.platform.clone(),
            last_seen_at: Some(now),
            created_at: existing.and_then(|device| device.created_at).or(Some(now)),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
        })?;

        let state = doc.read_all()?;
        let (chats_tx, _) = watch::channel(state.chats);
        let (devices_tx, _) = watch::channel(state.devices);
        let (sessions_tx, _) = watch::channel(state.sessions);
        let (spaces_tx, _) = watch::channel(state.spaces);
        let (changed_tx, changed_rx) = watch::channel(0u64);
        let host = Self {
            inner: Arc::new(WorkspaceHostInner {
                store,
                config,
                reg: Mutex::new(doc),
                chats_tx,
                devices_tx,
                sessions_tx,
                spaces_tx,
                changed_tx,
            }),
        };
        host.inner.save_snapshot();
        tokio::spawn(workspace_task(Arc::downgrade(&host.inner), changed_rx));
        Ok(host)
    }

    pub fn device_id(&self) -> &str {
        &self.inner.config.device_id
    }

    /// Local persistence is always available once the host has opened.
    pub fn connected(&self) -> bool {
        true
    }

    fn mutate<R>(&self, f: impl FnOnce(&mut RegistryDoc) -> R) -> R {
        let result = f(&mut lock(&self.inner.reg));
        self.inner.bump_changed();
        result
    }

    fn read<R>(&self, f: impl FnOnce(&RegistryDoc) -> R) -> R {
        f(&lock(&self.inner.reg))
    }

    pub fn chat(&self, chat_id: &str) -> Result<Option<Chat>, EngineError> {
        Ok(self.read(|doc| doc.chat(chat_id))?)
    }

    pub fn space(&self, space_id: &str) -> Result<Option<Space>, EngineError> {
        Ok(self.read(|doc| doc.space(space_id))?)
    }

    pub fn read_chats(&self) -> Result<Vec<Chat>, EngineError> {
        Ok(self.read(|doc| doc.read_chats())?)
    }

    pub fn read_devices(&self) -> Result<Vec<Device>, EngineError> {
        Ok(self.read(|doc| doc.read_devices())?)
    }

    pub fn read_sessions(&self) -> Result<Vec<Session>, EngineError> {
        Ok(self.read(|doc| doc.read_sessions())?)
    }

    pub fn watch_chats(&self) -> watch::Receiver<Vec<Chat>> {
        self.inner.chats_tx.subscribe()
    }

    pub fn watch_devices(&self) -> watch::Receiver<Vec<Device>> {
        self.inner.devices_tx.subscribe()
    }

    pub fn watch_session_rows(&self) -> watch::Receiver<Vec<Session>> {
        self.inner.sessions_tx.subscribe()
    }

    pub fn watch_spaces(&self) -> watch::Receiver<Vec<Space>> {
        self.inner.spaces_tx.subscribe()
    }

    pub fn merged_sessions_watch(
        &self,
        mut local: watch::Receiver<Vec<Session>>,
    ) -> watch::Receiver<Vec<Session>> {
        let mut rows = self.watch_session_rows();
        let device_id = self.inner.config.device_id.clone();
        let (tx, rx) = watch::channel(merge_sessions(&device_id, &rows.borrow(), &local.borrow()));
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = rows.changed() => if changed.is_err() { break },
                    changed = local.changed() => if changed.is_err() { break },
                }
                let merged = merge_sessions(
                    &device_id,
                    &rows.borrow_and_update(),
                    &local.borrow_and_update(),
                );
                if tx.send(merged).is_err() {
                    break;
                }
            }
        });
        rx
    }

    pub fn is_host(&self, chat_id: &str) -> bool {
        self.read(|doc| doc.chat(chat_id))
            .ok()
            .flatten()
            .is_none_or(|chat| chat.device_id == self.device_id())
    }

    pub fn claim_chat(&self, chat_id: &str, cwd: Option<&str>) -> Result<(), EngineError> {
        if self.chat(chat_id)?.is_some() {
            return Ok(());
        }
        let space_id = cwd.map(|path| self.space_for_path(path)).transpose()?;
        self.mutate(|doc| doc.claim_chat(chat_id, cwd, space_id.as_deref(), Utc::now()));
        Ok(())
    }

    fn space_for_path(&self, path: &str) -> Result<String, EngineError> {
        let spaces = self.read_spaces()?;
        if let Some(space) = spaces
            .iter()
            .find(|space| space.device_id == self.device_id() && space.path == path)
        {
            return Ok(space.id.clone());
        }
        let root = linked_worktree_root(std::path::Path::new(path));
        if let Some(root) = root.as_deref()
            && let Some(space) = spaces
                .iter()
                .find(|space| space.device_id == self.device_id() && space.path == root)
        {
            return Ok(space.id.clone());
        }
        let space = Space {
            id: crate::new_id(),
            device_id: self.device_id().to_string(),
            path: root.unwrap_or_else(|| path.to_string()),
            name: None,
            git_detected: false,
            git_checked_at: None,
            checkout_id: None,
            created_at: Utc::now(),
        };
        let id = space.id.clone();
        self.mutate(|doc| doc.upsert_space(&space))?;
        Ok(id)
    }

    pub fn chat_config(&self, chat_id: &str) -> Option<ChatConfig> {
        self.chat(chat_id)
            .ok()
            .flatten()
            .and_then(|chat| chat.config)
    }

    pub fn note_message(&self, chat_id: &str, text: &str) {
        let preview: String = text.chars().take(120).collect();
        if let Err(err) = self.claim_chat(chat_id, None).and_then(|_| {
            self.mutate(|doc| doc.set_chat_last_message(chat_id, &preview, Utc::now()))
                .map(|_| ())
                .map_err(EngineError::from)
        }) {
            tracing::warn!(chat = %chat_id, error = %err, "registry last-message write failed");
        }
    }

    pub fn set_chat_harness_session(&self, chat_id: &str, session_id: &str, cwd: &str) {
        if let Err(err) = self.mutate(|doc| doc.set_chat_harness_session(chat_id, session_id, cwd))
        {
            tracing::warn!(chat = %chat_id, error = %err, "registry harness-session write failed");
        }
    }

    pub fn chat_harness_session(&self, chat_id: &str) -> Option<(String, Option<String>)> {
        self.chat(chat_id).ok().flatten().and_then(|chat| {
            chat.harness_session_id
                .map(|id| (id, chat.harness_session_cwd))
        })
    }

    pub fn record_session(&self, session: &Session) {
        if let Err(err) = self.mutate(|doc| doc.upsert_session(session)) {
            tracing::warn!(chat = %session.chat_id, error = %err, "registry session write failed");
        }
    }

    pub fn create_chat(
        &self,
        chat_id: &str,
        space_id: Option<&str>,
        device_id: Option<&str>,
        config: Option<ChatConfig>,
        cwd: Option<String>,
    ) -> Result<(), EngineError> {
        if self.chat(chat_id)?.is_some() {
            return Ok(());
        }
        let space = space_id.map(|id| self.space(id)).transpose()?.flatten();
        if let Some(space) = &space
            && space.device_id != self.device_id()
        {
            return Err(EngineError::Other(
                "local mode cannot create a remote chat".into(),
            ));
        }
        let host_device = device_id.unwrap_or(self.device_id());
        if host_device != self.device_id() {
            return Err(EngineError::Other("local mode has one device".into()));
        }
        self.mutate(|doc| {
            doc.upsert_chat(&Chat {
                id: chat_id.to_string(),
                device_id: self.device_id().to_string(),
                title: None,
                archived: false,
                cwd: Some(cwd.unwrap_or_else(|| {
                    space
                        .as_ref()
                        .map(|space| space.path.clone())
                        .unwrap_or_else(|| "~".to_string())
                })),
                branch: None,
                checkout_id: None,
                config,
                last_message_preview: None,
                last_message_at: None,
                created_at: Utc::now(),
                harness_session_id: None,
                harness_session_cwd: None,
                space_id: space.as_ref().map(|space| space.id.clone()),
                last_seen_at: None,
            })
        })?;
        Ok(())
    }

    pub fn create_space(
        &self,
        space_id: &str,
        device_id: &str,
        path: &str,
        name: Option<String>,
        git_detected: bool,
    ) -> Result<(), EngineError> {
        if device_id != self.device_id() {
            return Err(EngineError::Other("local mode has one device".into()));
        }
        let spaces = self.read_spaces()?;
        if spaces.iter().any(|space| {
            space.id == space_id || (space.device_id == device_id && space.path == path)
        }) {
            return Ok(());
        }
        self.mutate(|doc| {
            doc.upsert_space(&Space {
                id: space_id.to_string(),
                device_id: device_id.to_string(),
                path: path.to_string(),
                name,
                git_detected,
                git_checked_at: None,
                checkout_id: None,
                created_at: Utc::now(),
            })
        })?;
        Ok(())
    }

    pub fn rename_space(&self, space_id: &str, name: Option<&str>) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.rename_space(space_id, name))?)
    }

    pub fn delete_space(&self, space_id: &str) -> Result<DeletedSpace, EngineError> {
        Ok(self.mutate(|doc| doc.delete_space(space_id))?)
    }

    pub fn mark_chat_seen(
        &self,
        chat_id: &str,
        at: chrono::DateTime<Utc>,
    ) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.set_chat_seen(chat_id, at))?)
    }

    pub fn set_space_git(
        &self,
        space_id: &str,
        detected: bool,
        checkout_id: Option<&str>,
    ) -> Result<bool, EngineError> {
        match self.space(space_id)? {
            Some(space) if space.device_id == self.device_id() => {
                Ok(self
                    .mutate(|doc| doc.set_space_git(space_id, detected, checkout_id, Utc::now()))?)
            }
            Some(_) => Ok(false),
            None => Ok(false),
        }
    }

    pub fn read_spaces(&self) -> Result<Vec<Space>, EngineError> {
        Ok(self.read(|doc| doc.read_spaces())?)
    }

    pub fn rename_chat(&self, chat_id: &str, title: &str) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.rename_chat(chat_id, title))?)
    }

    pub fn set_chat_activity(
        &self,
        chat_id: &str,
        last_message_at: Option<i64>,
        created_at: Option<i64>,
    ) -> Result<bool, EngineError> {
        let Some(mut chat) = self.chat(chat_id)? else {
            return Ok(false);
        };
        if let Some(ms) = last_message_at {
            chat.last_message_at = chrono::DateTime::<Utc>::from_timestamp_millis(ms);
        }
        if let Some(ms) = created_at
            && let Some(at) = chrono::DateTime::<Utc>::from_timestamp_millis(ms)
        {
            chat.created_at = at;
        }
        self.mutate(|doc| doc.upsert_chat(&chat))?;
        Ok(true)
    }

    pub fn set_chat_host(&self, chat_id: &str, device_id: &str) -> Result<bool, EngineError> {
        if device_id != self.device_id() {
            return Err(EngineError::Other("local mode has one device".into()));
        }
        let Some(mut chat) = self.chat(chat_id)? else {
            return Ok(false);
        };
        chat.device_id = device_id.to_string();
        self.mutate(|doc| doc.upsert_chat(&chat))?;
        Ok(true)
    }

    pub fn set_chat_archived(&self, chat_id: &str, archived: bool) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.set_chat_archived(chat_id, archived))?)
    }

    pub fn set_chat_config(&self, chat_id: &str, config: &ChatConfig) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.set_chat_config(chat_id, config))?)
    }

    pub fn delete_chat(&self, chat_id: &str) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.delete_chat(chat_id))?)
    }

    pub fn rename_device(&self, device_id: &str, name: &str) -> Result<bool, EngineError> {
        if device_id != self.device_id() {
            return Ok(false);
        }
        Ok(self.mutate(|doc| doc.rename_device(device_id, name))?)
    }

    pub fn set_chat_branch(&self, chat_id: &str, branch: &str) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.set_chat_branch(chat_id, branch))?)
    }

    pub fn set_chat_cwd(&self, chat_id: &str, cwd: &str) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.set_chat_cwd(chat_id, cwd))?)
    }

    pub fn set_chat_checkout(&self, chat_id: &str, checkout_id: &str) -> Result<bool, EngineError> {
        Ok(self.mutate(|doc| doc.set_chat_checkout(chat_id, checkout_id))?)
    }

    pub fn flush(&self) {
        self.inner.save_snapshot();
    }

    pub fn shutdown(&self) {
        let device_id = self.inner.config.device_id.clone();
        if let Err(err) = self.mutate(|doc| doc.set_device_last_seen(&device_id, Utc::now())) {
            tracing::warn!(error = %err, "device lastSeenAt stamp failed");
        }
        self.inner.save_snapshot();
    }
}

impl WorkspaceHostInner {
    fn bump_changed(&self) {
        self.changed_tx
            .send_modify(|value| *value = value.wrapping_add(1));
    }

    fn publish(&self) {
        match lock(&self.reg).read_all() {
            Ok(state) => {
                self.chats_tx.send_replace(state.chats);
                self.devices_tx.send_replace(state.devices);
                self.sessions_tx.send_replace(state.sessions);
                self.spaces_tx.send_replace(state.spaces);
            }
            Err(err) => tracing::warn!(error = %err, "registry read failed"),
        }
    }

    fn save_snapshot(&self) {
        match lock(&self.reg).to_bytes() {
            Ok(bytes) => {
                if let Err(err) = self.store.save_snapshot(REGISTRY_DOC_ID, &bytes) {
                    tracing::warn!(error = %err, "registry snapshot save failed");
                }
            }
            Err(err) => tracing::warn!(error = %err, "registry snapshot export failed"),
        }
    }
}

async fn workspace_task(weak: Weak<WorkspaceHostInner>, mut changed_rx: watch::Receiver<u64>) {
    let mut save_deadline: Option<tokio::time::Instant> = None;
    loop {
        tokio::select! {
            changed = changed_rx.changed() => {
                if changed.is_err() { break; }
                let Some(inner) = weak.upgrade() else { break };
                inner.publish();
                if save_deadline.is_none() {
                    save_deadline = Some(tokio::time::Instant::now() + std::time::Duration::from_millis(1_000));
                }
            }
            _ = async {
                if let Some(deadline) = save_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                save_deadline = None;
                let Some(inner) = weak.upgrade() else { break };
                inner.save_snapshot();
            }
        }
    }
}

fn linked_worktree_root(path: &std::path::Path) -> Option<String> {
    let gitfile = path.join(".git");
    if !std::fs::metadata(&gitfile).ok()?.is_file() {
        return None;
    }
    let content = std::fs::read_to_string(&gitfile).ok()?;
    let target = content
        .lines()
        .find_map(|line| line.strip_prefix("gitdir:"))?
        .trim();
    let mut target = std::path::PathBuf::from(target);
    if target.is_relative() {
        target = std::fs::canonicalize(path.join(target)).ok()?;
    }
    let worktrees = target.parent()?;
    let dot_git = worktrees.parent()?;
    if worktrees.file_name()? != "worktrees" || dot_git.file_name()? != ".git" {
        return None;
    }
    Some(dot_git.parent()?.to_string_lossy().into_owned())
}

fn merge_sessions(device_id: &str, rows: &[Session], local: &[Session]) -> Vec<Session> {
    let mut merged: std::collections::HashMap<String, Session> = rows
        .iter()
        .filter(|session| session.device_id != device_id)
        .map(|session| (session.chat_id.clone(), session.clone()))
        .collect();
    for session in local {
        merged.insert(session.chat_id.clone(), session.clone());
    }
    let mut list: Vec<Session> = merged.into_values().collect();
    list.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
    list
}

fn device_name_on_boot(existing_name: Option<&str>, detected_name: &str) -> String {
    existing_name
        .filter(|name| {
            let name = name.trim();
            !name.is_empty() && name != crate::LEGACY_UNKNOWN_DEVICE_NAME
        })
        .unwrap_or(detected_name)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::{device_name_on_boot, linked_worktree_root};

    #[test]
    fn boot_repairs_the_legacy_unknown_device_sentinel() {
        assert_eq!(
            device_name_on_boot(Some("unknown-device"), "MacBook Pro"),
            "MacBook Pro"
        );
    }

    #[test]
    fn boot_preserves_a_user_selected_device_name() {
        assert_eq!(
            device_name_on_boot(Some("Work laptop"), "MacBook Pro"),
            "Work laptop"
        );
    }

    #[test]
    fn linked_worktree_resolves_to_the_checkout_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("proj");
        let wt = dir.path().join("clever-ember");
        std::fs::create_dir_all(root.join(".git").join("worktrees").join("clever-ember")).unwrap();
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(
            wt.join(".git"),
            format!(
                "gitdir: {}\n",
                root.join(".git/worktrees/clever-ember").display()
            ),
        )
        .unwrap();
        assert_eq!(
            linked_worktree_root(&wt),
            Some(root.to_string_lossy().into_owned())
        );
    }
}
