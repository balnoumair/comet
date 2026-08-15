//! Local per-chat session documents.
//!
//! A chat document is the durable local transcript and command queue. Changes
//! are published to local watchers, pending commands are drained by the local
//! sessions engine, and snapshots are debounced into SQLite.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use zeron_doc::{
    COMMAND_DEFAULT_TTL_MS, CommandBasedOn, CommandDisposition, DocError, EvaluationContext,
    MessagePart, MessageRole, MessageStatus, SessionCommandEntry, SessionCommandPayload,
    SessionCommandStatus, SessionDoc, SessionMessageEntry, evaluate_command,
    join_continuation_entries,
};
use zeron_proto::{HarnessId, UserInputAnswer, UserInputQuestion};
use zeron_sync::DocsStore;

use crate::sessions::{SessionsEngine, SteerOutcome};
use crate::workspace_host::WorkspaceHost;
use crate::{EngineError, new_id, now_ms};

const SNAPSHOT_DEBOUNCE_MS: u64 = 1_000;
const WARM_DOC_CAP: usize = 12;
const RESIDENT_BYTES_PER_SNAPSHOT_BYTE: usize = 6;
const DOC_RESIDENT_FLOOR_BYTES: usize = 512 * 1024;
const EVICT_MIN_IDLE_MS: i64 = 30_000;

pub struct DocHostConfig {
    pub device_id: String,
    pub default_harness: HarnessId,
}

struct DocHostInner {
    store: Arc<DocsStore>,
    config: DocHostConfig,
    sessions: Mutex<Option<SessionsEngine>>,
    workspace: OnceLock<WorkspaceHost>,
    shutdown: CancellationToken,
    tasks: TaskTracker,
    handles: Mutex<HashMap<String, Arc<ChatDocHandle>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct DocHost {
    inner: Arc<DocHostInner>,
}

pub struct ChatDocHandle {
    chat_id: String,
    device_id: String,
    doc: Arc<SessionDoc>,
    messages_tx: watch::Sender<Vec<SessionMessageEntry>>,
    mirror_dirty: AtomicBool,
    last_access: AtomicI64,
    snapshot_bytes: AtomicUsize,
    _sub: loro::Subscription,
}

impl ChatDocHandle {
    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    pub fn doc(&self) -> &SessionDoc {
        &self.doc
    }

    pub fn doc_arc(&self) -> Arc<SessionDoc> {
        self.doc.clone()
    }

    pub fn watch_messages(&self) -> watch::Receiver<Vec<SessionMessageEntry>> {
        self.touch();
        let rx = self.messages_tx.subscribe();
        if self.mirror_dirty.load(Ordering::Acquire) {
            self.publish_messages();
        }
        rx
    }

    fn touch(&self) {
        self.last_access.store(now_ms(), Ordering::Relaxed);
    }

    pub fn connected(&self) -> bool {
        true
    }

    pub fn write_user_message(
        &self,
        message_id: &str,
        text: &str,
        created_at: i64,
    ) -> Result<(), DocError> {
        if self
            .doc
            .read_entries()?
            .iter()
            .any(|entry| entry.id == message_id)
        {
            return Ok(());
        }
        self.doc.push_message(&SessionMessageEntry {
            id: message_id.to_string(),
            role: MessageRole::User,
            parts: vec![MessagePart::Text {
                id: "t0".into(),
                text: text.to_string(),
            }],
            created_at,
            device_id: self.device_id.clone(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        })
    }

    pub fn mark_abandoned_streams(&self, note: &str) -> Result<Vec<(String, i64)>, DocError> {
        let mut stamped = Vec::new();
        for entry in self.doc.read_entries()? {
            if entry.role == MessageRole::Assistant
                && entry.status == Some(MessageStatus::Streaming)
                && entry.device_id == self.device_id
                && self
                    .doc
                    .set_message_status(&entry.id, MessageStatus::Aborted)?
            {
                let part_id = format!("{}-recovery", entry.id);
                if let Err(err) = self.doc.append_error_part(&entry.id, &part_id, note) {
                    tracing::warn!(chat = %self.chat_id, error = %err, "recovery note append failed");
                }
                stamped.push((entry.id.clone(), entry.created_at));
            }
        }
        if !stamped.is_empty() {
            self.publish_messages();
        }
        Ok(stamped)
    }

    fn publish_messages(&self) {
        self.mirror_dirty.store(false, Ordering::Release);
        match self.doc.read_entries() {
            Ok(entries) => {
                self.messages_tx
                    .send_replace(join_continuation_entries(entries));
            }
            Err(err) => {
                tracing::warn!(chat = %self.chat_id, error = %err, "transcript read failed")
            }
        }
    }

    fn publish_messages_if_watched(&self) {
        if self.messages_tx.receiver_count() == 0 {
            self.mirror_dirty.store(true, Ordering::Release);
            self.messages_tx.send_replace(Vec::new());
        } else {
            self.publish_messages();
        }
    }

    fn resident_estimate(&self) -> usize {
        (self.snapshot_bytes.load(Ordering::Relaxed) * RESIDENT_BYTES_PER_SNAPSHOT_BYTE)
            .max(DOC_RESIDENT_FLOOR_BYTES)
    }
}

impl DocHost {
    pub fn new(store: Arc<DocsStore>, config: DocHostConfig) -> Self {
        Self {
            inner: Arc::new(DocHostInner {
                store,
                config,
                sessions: Mutex::new(None),
                workspace: OnceLock::new(),
                shutdown: CancellationToken::new(),
                tasks: TaskTracker::new(),
                handles: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn spawn_worker(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        let cancel = self.inner.shutdown.clone();
        self.inner.tasks.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = fut => {}
            }
        });
    }

    fn sessions(&self) -> Option<SessionsEngine> {
        lock(&self.inner.sessions).clone()
    }

    pub fn set_sessions(&self, sessions: SessionsEngine) {
        {
            let mut slot = lock(&self.inner.sessions);
            if slot.is_none() {
                *slot = Some(sessions);
            }
        }
        let handles: Vec<_> = lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            let host = self.clone();
            self.spawn_worker(async move { host.drain_commands(&handle).await });
        }
    }

    pub async fn shutdown_workers(&self) {
        self.inner.shutdown.cancel();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        self.flush_all();
        let handles = std::mem::take(&mut *lock(&self.inner.handles));
        drop(handles);
        lock(&self.inner.sessions).take();
    }

    #[doc(hidden)]
    pub fn retirement_probe(&self) -> Box<dyn Fn() -> bool + Send + Sync> {
        let weak = Arc::downgrade(&self.inner);
        Box::new(move || weak.upgrade().is_none())
    }

    pub fn set_workspace(&self, workspace: WorkspaceHost) {
        let _ = self.inner.workspace.set(workspace);
    }

    pub fn workspace(&self) -> Option<&WorkspaceHost> {
        self.inner.workspace.get()
    }

    pub fn device_id(&self) -> &str {
        &self.inner.config.device_id
    }

    pub fn open(&self, chat_id: &str) -> Result<Arc<ChatDocHandle>, EngineError> {
        {
            let handles = lock(&self.inner.handles);
            if let Some(handle) = handles.get(chat_id) {
                handle.touch();
                return Ok(handle.clone());
            }
        }

        let stored = self.inner.store.load_snapshot(chat_id)?;
        let (doc, snapshot_len) = match stored {
            Some(bytes) => {
                let raw = loro::LoroDoc::new();
                raw.import(&bytes)
                    .map_err(|err| EngineError::Other(format!("snapshot import failed: {err}")))?;
                (SessionDoc::from_doc(raw), bytes.len())
            }
            None => (SessionDoc::init(chat_id)?, 0),
        };
        let doc = Arc::new(doc);

        let (changed_tx, changed_rx) = watch::channel(0u64);
        let sub = doc.doc().subscribe_root(Arc::new(move |_diff| {
            changed_tx.send_modify(|value| *value = value.wrapping_add(1));
        }));
        let (messages_tx, _) = watch::channel(Vec::new());
        let handle = Arc::new(ChatDocHandle {
            chat_id: chat_id.to_string(),
            device_id: self.inner.config.device_id.clone(),
            doc,
            messages_tx,
            mirror_dirty: AtomicBool::new(true),
            last_access: AtomicI64::new(now_ms()),
            snapshot_bytes: AtomicUsize::new(snapshot_len),
            _sub: sub,
        });

        {
            let mut handles = lock(&self.inner.handles);
            if let Some(existing) = handles.get(chat_id) {
                return Ok(existing.clone());
            }
            handles.insert(chat_id.to_string(), handle.clone());
        }

        self.spawn_worker(chat_task(self.clone(), Arc::downgrade(&handle), changed_rx));
        self.evict_over_budget();
        Ok(handle)
    }

    fn is_host(&self, chat_id: &str) -> bool {
        self.workspace()
            .is_none_or(|workspace| workspace.is_host(chat_id))
    }

    pub(crate) fn harness_for(&self, chat_id: &str) -> HarnessId {
        self.workspace()
            .and_then(|workspace| workspace.chat_config(chat_id))
            .map(|config| config.harness)
            .unwrap_or(self.inner.config.default_harness)
    }

    pub(crate) fn harness_for_request(
        &self,
        chat_id: &str,
        request: &zeron_proto::RunRequest,
    ) -> HarnessId {
        request.harness.unwrap_or_else(|| self.harness_for(chat_id))
    }

    fn pinned(&self, handle: &Arc<ChatDocHandle>) -> bool {
        if handle.messages_tx.receiver_count() > 0 || Arc::strong_count(&handle.doc) > 1 {
            return true;
        }
        if self.is_host(&handle.chat_id) {
            let is_processed = |id: &str| self.inner.store.is_processed(id).unwrap_or(false);
            return match handle.doc.read_commands() {
                Ok(commands) => commands.iter().any(|command| {
                    command.status == SessionCommandStatus::Pending && !is_processed(&command.id)
                }),
                Err(_) => true,
            };
        }
        false
    }

    fn evict_over_budget(&self) {
        let mut by_age: Vec<_> = lock(&self.inner.handles)
            .values()
            .map(|handle| {
                (
                    handle.last_access.load(Ordering::Relaxed),
                    handle.chat_id.clone(),
                )
            })
            .collect();
        by_age.sort_unstable();

        for (last_access, chat_id) in by_age {
            if now_ms() - last_access < EVICT_MIN_IDLE_MS {
                return;
            }
            let (count, estimate) = {
                let handles = lock(&self.inner.handles);
                (
                    handles.len(),
                    handles
                        .values()
                        .map(|handle| handle.resident_estimate())
                        .sum::<usize>(),
                )
            };
            if count <= WARM_DOC_CAP && estimate <= zeron_doc::DOC_LRU_BYTE_BUDGET {
                return;
            }
            let evicted = {
                let mut handles = lock(&self.inner.handles);
                match handles.get(&chat_id) {
                    Some(handle) if !self.pinned(handle) => handles.remove(&chat_id),
                    _ => None,
                }
            };
            if let Some(handle) = evicted {
                self.save_snapshot(&handle);
                tracing::debug!(chat = %handle.chat_id, "local doc evicted");
            }
        }
    }

    pub fn probe_open_chats(&self) {}

    pub fn purge_chat(&self, chat_id: &str) {
        drop(lock(&self.inner.handles).remove(chat_id));
        if let Err(err) = self.inner.store.delete_snapshot(chat_id) {
            tracing::warn!(chat = %chat_id, error = %err, "snapshot delete failed");
        }
    }

    pub fn queue_command(
        &self,
        chat_id: &str,
        payload: SessionCommandPayload,
    ) -> Result<String, EngineError> {
        let handle = self.open(chat_id)?;
        let id = new_id();
        let now = now_ms();
        let based_on = handle
            .doc
            .read_entries()?
            .last()
            .map(|entry| CommandBasedOn {
                turn_id: Some(entry.id.clone()),
                frontier: None,
            });
        let is_message = matches!(
            payload,
            SessionCommandPayload::Run { .. } | SessionCommandPayload::Steer { .. }
        );
        handle.doc.queue_command(&SessionCommandEntry {
            id: id.clone(),
            payload,
            issued_by: self.inner.config.device_id.clone(),
            issued_at: now,
            based_on,
            expires_at: Some(now + COMMAND_DEFAULT_TTL_MS),
            status: SessionCommandStatus::Pending,
            resolution: None,
        })?;
        if is_message
            && let Some(workspace) = self.workspace()
            && matches!(workspace.chat(chat_id), Ok(Some(chat)) if chat.archived)
            && let Err(err) = workspace.set_chat_archived(chat_id, false)
        {
            tracing::warn!(chat = %chat_id, error = %err, "unarchive on send failed");
        }
        Ok(id)
    }

    pub async fn drain_commands(&self, handle: &Arc<ChatDocHandle>) {
        let Some(sessions) = self.sessions() else {
            return;
        };
        if !self.is_host(&handle.chat_id) {
            return;
        }
        let mut skipped = HashSet::new();
        loop {
            let commands = match handle.doc.read_commands() {
                Ok(commands) => commands,
                Err(err) => {
                    tracing::warn!(chat = %handle.chat_id, error = %err, "command read failed");
                    return;
                }
            };
            let is_processed = |id: &str| self.inner.store.is_processed(id).unwrap_or(false);
            let Some(entry) = commands
                .iter()
                .find(|command| {
                    command.status == SessionCommandStatus::Pending
                        && !skipped.contains(&command.id)
                        && !is_processed(&command.id)
                })
                .cloned()
            else {
                return;
            };
            let messages = handle.doc.read_entries().unwrap_or_default();
            let current_turn_id = messages.last().map(|entry| entry.id.clone());
            let turn_is_past = |turn_id: &str| messages.iter().any(|entry| entry.id == turn_id);
            let disposition = evaluate_command(
                &entry,
                &EvaluationContext {
                    is_processed: &is_processed,
                    now_ms: now_ms(),
                    entries: &commands,
                    current_turn_id: current_turn_id.as_deref(),
                    turn_is_past: &turn_is_past,
                },
            );
            if let Err(err) = self.inner.store.mark_processed(&entry.id) {
                tracing::error!(chat = %handle.chat_id, error = %err, "processed-ledger write failed; halting drain");
                return;
            }
            match disposition {
                CommandDisposition::Skip => {
                    skipped.insert(entry.id.clone());
                }
                CommandDisposition::Expired => {
                    self.resolve_command(handle, &entry.id, SessionCommandStatus::Expired, None);
                }
                CommandDisposition::Superseded => {
                    self.resolve_command(handle, &entry.id, SessionCommandStatus::Superseded, None);
                }
                CommandDisposition::Execute => {
                    let (status, resolution) = match self.execute(&sessions, handle, &entry).await {
                        Ok(outcome) => outcome,
                        Err(err) => (SessionCommandStatus::Rejected, Some(err.to_string())),
                    };
                    self.resolve_command(handle, &entry.id, status, resolution.as_deref());
                }
            }
        }
    }

    fn resolve_command(
        &self,
        handle: &ChatDocHandle,
        command_id: &str,
        status: SessionCommandStatus,
        resolution: Option<&str>,
    ) {
        if let Err(err) = handle
            .doc
            .set_command_status(command_id, status, resolution)
        {
            tracing::warn!(
                chat = %handle.chat_id,
                command = %command_id,
                error = %err,
                "command outcome write failed"
            );
        }
    }

    async fn execute(
        &self,
        sessions: &SessionsEngine,
        handle: &Arc<ChatDocHandle>,
        entry: &SessionCommandEntry,
    ) -> Result<(SessionCommandStatus, Option<String>), EngineError> {
        let chat_id = &handle.chat_id;
        match &entry.payload {
            SessionCommandPayload::Run {
                request,
                message_id,
            } => {
                if let Some(workspace) = self.workspace() {
                    workspace.claim_chat(chat_id, Some(&request.cwd))?;
                }
                let harness = self.harness_for_request(chat_id, request);
                if let Some(workspace) = self.workspace()
                    && workspace.chat_config(chat_id).is_none()
                {
                    let config = zeron_proto::ChatConfig {
                        harness,
                        model: request.model.clone(),
                        reasoning: request.reasoning,
                        model_options: request.model_options.clone(),
                        sandbox: request.sandbox,
                    };
                    if let Err(err) = workspace.set_chat_config(chat_id, &config) {
                        tracing::warn!(chat = %chat_id, error = %err, "run-config backfill failed");
                    }
                }
                sessions
                    .dispatch(chat_id, harness, request.clone(), Some(message_id.clone()))
                    .await?;
                Ok((SessionCommandStatus::Applied, None))
            }
            SessionCommandPayload::Steer { prompt, message_id } => {
                match sessions.steer(chat_id, prompt, message_id.clone()).await? {
                    SteerOutcome::Accepted => Ok((SessionCommandStatus::Applied, None)),
                    SteerOutcome::NotSteerable => {
                        let request = sessions
                            .last_request(chat_id)
                            .or_else(|| self.request_from_chat_row(chat_id, prompt));
                        let Some(mut request) = request else {
                            return Ok((
                                SessionCommandStatus::Rejected,
                                Some("no live run and no prior run config".into()),
                            ));
                        };
                        request.prompt = prompt.clone();
                        request.resume = None;
                        request.attachments = Vec::new();
                        let harness = self.harness_for_request(chat_id, &request);
                        sessions
                            .dispatch(chat_id, harness, request, message_id.clone())
                            .await?;
                        Ok((
                            SessionCommandStatus::Applied,
                            Some("queued as new turn".into()),
                        ))
                    }
                }
            }
            SessionCommandPayload::Interrupt {} => {
                sessions.interrupt(chat_id).await?;
                Ok((SessionCommandStatus::Applied, None))
            }
            SessionCommandPayload::RespondInput {
                request_id,
                answers,
            } => {
                if sessions.respond_input(chat_id, request_id, answers.clone())? {
                    return Ok((SessionCommandStatus::Applied, None));
                }
                let questions = handle.doc.read_entries().ok().and_then(|entries| {
                    entries
                        .iter()
                        .rev()
                        .filter(|entry| entry.status != Some(MessageStatus::Streaming))
                        .find_map(|entry| {
                            entry.parts.iter().find_map(|part| match part {
                                MessagePart::Input {
                                    request_id: id,
                                    questions,
                                    resolved: false,
                                    ..
                                } if id == request_id => Some(questions.clone()),
                                _ => None,
                            })
                        })
                });
                let Some(questions) = questions else {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request".into()),
                    ));
                };
                let request = sessions
                    .last_request(chat_id)
                    .or_else(|| self.request_from_chat_row(chat_id, ""));
                let Some(mut request) = request else {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request and no prior run config".into()),
                    ));
                };
                request.prompt = respond_input_prompt(&questions, answers);
                request.resume = None;
                request.attachments = Vec::new();
                if let Err(err) = handle.doc.resolve_input(request_id) {
                    tracing::warn!(chat = %chat_id, request = %request_id, error = %err, "orphaned input resolve failed");
                }
                let harness = self.harness_for_request(chat_id, &request);
                sessions.dispatch(chat_id, harness, request, None).await?;
                Ok((
                    SessionCommandStatus::Applied,
                    Some("answered as new turn".into()),
                ))
            }
        }
    }

    pub(crate) fn request_from_chat_row(
        &self,
        chat_id: &str,
        prompt: &str,
    ) -> Option<zeron_proto::RunRequest> {
        let workspace = self.workspace()?;
        let chat = match workspace.chat(chat_id) {
            Ok(Some(chat)) => chat,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(chat = %chat_id, error = %err, "workspace chat read failed");
                return None;
            }
        };
        let config = chat.config;
        Some(zeron_proto::RunRequest {
            prompt: prompt.to_string(),
            harness: config.as_ref().map(|config| config.harness),
            model: config.as_ref().and_then(|config| config.model.clone()),
            reasoning: config.as_ref().and_then(|config| config.reasoning),
            model_options: config
                .as_ref()
                .map(|config| config.model_options.clone())
                .unwrap_or_default(),
            cwd: chat.cwd.unwrap_or_default(),
            sandbox: config
                .as_ref()
                .map(|config| config.sandbox)
                .unwrap_or(zeron_proto::SandboxLevel::WorkspaceWrite),
            auto_approve: false,
            attachments: Vec::new(),
            resume: None,
        })
    }

    fn save_snapshot(&self, handle: &ChatDocHandle) {
        match handle.doc.export_snapshot() {
            Ok(bytes) => {
                handle.snapshot_bytes.store(bytes.len(), Ordering::Relaxed);
                if let Err(err) = self.inner.store.save_snapshot(&handle.chat_id, &bytes) {
                    tracing::warn!(chat = %handle.chat_id, error = %err, "snapshot save failed");
                }
            }
            Err(err) => {
                tracing::warn!(chat = %handle.chat_id, error = %err, "snapshot export failed")
            }
        }
    }

    pub fn flush_all(&self) {
        let handles: Vec<_> = lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            self.save_snapshot(&handle);
        }
    }
}

pub fn respond_input_prompt(
    questions: &[UserInputQuestion],
    answers: &[UserInputAnswer],
) -> String {
    let mut lines = vec!["Answering your earlier question:".to_string()];
    for answer in answers {
        let picked = answer.labels.join(", ");
        let question = questions
            .iter()
            .find(|question| question.id == answer.question_id)
            .map(|question| question.question.trim())
            .filter(|question| !question.is_empty());
        match question {
            Some(question) => lines.push(format!("{question} — {picked}")),
            None => lines.push(picked),
        }
    }
    lines.join("\n")
}

async fn chat_task(host: DocHost, weak: Weak<ChatDocHandle>, mut changed_rx: watch::Receiver<u64>) {
    {
        let Some(handle) = weak.upgrade() else { return };
        host.drain_commands(&handle).await;
    }
    let mut save_deadline = None;
    loop {
        let sleep_until = save_deadline.unwrap_or_else(tokio::time::Instant::now);
        tokio::select! {
            changed = changed_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let Some(handle) = weak.upgrade() else { break };
                handle.publish_messages_if_watched();
                host.drain_commands(&handle).await;
                if save_deadline.is_none() {
                    save_deadline = Some(tokio::time::Instant::now()
                        + std::time::Duration::from_millis(SNAPSHOT_DEBOUNCE_MS));
                }
            }
            _ = tokio::time::sleep_until(sleep_until), if save_deadline.is_some() => {
                save_deadline = None;
                let Some(handle) = weak.upgrade() else { break };
                host.save_snapshot(&handle);
                host.evict_over_budget();
            }
        }
    }
}
