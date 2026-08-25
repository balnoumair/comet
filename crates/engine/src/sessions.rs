use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use chrono::Utc;
use futures::StreamExt;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use zeron_doc::{
    DocError, MessagePart, MessageRole, MessageStatus, STREAM_COMMIT_MS, SegmentWriter, SessionDoc,
    fold_event_into_parts, sanitize_tool_call,
};
use zeron_harness::{CancellationToken, Harness, RunControls, SteerMessage};
use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, RunRequest, Session, SessionStatus, UserInputAnswer,
    UserInputQuestion,
};

use crate::doc_host::{ChatDocHandle, DocHost};
use crate::registry::HarnessRegistry;
use crate::run_journal::RunJournal;
use crate::{EngineError, new_id, now_ms};

#[derive(Debug, Clone)]
pub struct JournaledEvent {
    pub seq: u64,
    pub event: AgentEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SteerOutcome {
    Accepted,
    NotSteerable,
}

type PendingInputs = Arc<Mutex<HashMap<String, oneshot::Sender<Vec<UserInputAnswer>>>>>;

#[derive(Debug, Clone)]
struct HarnessSessionRef {
    session_id: String,
    cwd: String,
}

#[derive(Debug, Clone, PartialEq)]
struct RuntimeConfig {
    harness_id: HarnessId,
    model: Option<String>,
    reasoning: Option<zeron_proto::ReasoningLevel>,
    model_options: serde_json::Map<String, serde_json::Value>,
    cwd: String,
    sandbox: zeron_proto::SandboxLevel,
    auto_approve: bool,
}

impl RuntimeConfig {
    fn from_request(harness_id: HarnessId, request: &RunRequest) -> Self {
        Self {
            harness_id,
            model: request.model.clone(),
            reasoning: request.reasoning,
            model_options: request.model_options.clone(),
            cwd: request.cwd.clone(),
            sandbox: request.sandbox,
            auto_approve: request.auto_approve,
        }
    }

    fn can_route(&self, harness_id: HarnessId, request: &RunRequest) -> bool {
        request.attachments.is_empty() && self == &Self::from_request(harness_id, request)
    }
}

struct RunHandle {
    run_id: String,
    steerable: bool,
    runtime_config: RuntimeConfig,
    steer_tx: mpsc::Sender<SteerMessage>,
    interrupt_token: CancellationToken,
    cancel: watch::Sender<bool>,
    engine_tx: mpsc::UnboundedSender<AgentEvent>,
    pending_inputs: PendingInputs,
    routed_steers: Arc<Mutex<std::collections::VecDeque<RoutedSteer>>>,
}

#[derive(Debug, Clone)]
struct RoutedSteer {
    prompt: String,
    message_id: String,
}

struct Inner {
    device_id: String,
    journal: Arc<RunJournal>,
    registry: Arc<HarnessRegistry>,
    doc_host: Mutex<Option<DocHost>>,
    runs: Mutex<HashMap<String, RunHandle>>,
    hubs: Mutex<HashMap<String, broadcast::Sender<JournaledEvent>>>,
    statuses: Mutex<HashMap<String, Session>>,
    sessions_tx: watch::Sender<Vec<Session>>,
    last_requests: Mutex<HashMap<String, RunRequest>>,
    harness_sessions: Mutex<HashMap<String, HarnessSessionRef>>,
    titles: OnceLock<crate::titles::TitleGenerator>,
    turn_listener: OnceLock<TurnListener>,
}

pub type TurnListener = Arc<dyn Fn(&str, &str) + Send + Sync>;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct SessionsEngine {
    inner: Arc<Inner>,
}

impl SessionsEngine {
    pub fn new(
        device_id: String,
        journal: Arc<RunJournal>,
        registry: Arc<HarnessRegistry>,
    ) -> Self {
        let (sessions_tx, _) = watch::channel(Vec::new());
        Self {
            inner: Arc::new(Inner {
                device_id,
                journal,
                registry,
                doc_host: Mutex::new(None),
                runs: Mutex::new(HashMap::new()),
                hubs: Mutex::new(HashMap::new()),
                statuses: Mutex::new(HashMap::new()),
                sessions_tx,
                last_requests: Mutex::new(HashMap::new()),
                harness_sessions: Mutex::new(HashMap::new()),
                titles: OnceLock::new(),
                turn_listener: OnceLock::new(),
            }),
        }
    }

    pub fn set_doc_host(&self, host: DocHost) {
        let mut slot = lock(&self.inner.doc_host);
        if slot.is_none() {
            *slot = Some(host);
        }
    }

    pub fn clear_doc_host(&self) {
        lock(&self.inner.doc_host).take();
    }

    pub fn set_titles(&self, titles: crate::titles::TitleGenerator) {
        let _ = self.inner.titles.set(titles);
    }

    pub fn set_turn_listener(&self, listener: TurnListener) {
        let _ = self.inner.turn_listener.set(listener);
    }

    fn note_turn_start(&self, chat_id: &str, cwd: &str) {
        if let Some(listener) = self.inner.turn_listener.get() {
            listener(chat_id, cwd);
        }
    }

    fn doc_handle(&self, chat_id: &str) -> Result<Arc<ChatDocHandle>, EngineError> {
        let host = self
            .inner
            .doc_host()
            .ok_or_else(|| EngineError::Other("doc host not wired into sessions engine".into()))?;
        host.open(chat_id)
    }

    pub fn watch_sessions(&self) -> watch::Receiver<Vec<Session>> {
        self.inner.sessions_tx.subscribe()
    }

    pub fn session_status(&self, chat_id: &str) -> Option<Session> {
        lock(&self.inner.statuses).get(chat_id).cloned()
    }

    pub fn any_active(&self) -> bool {
        lock(&self.inner.statuses).values().any(|s| {
            matches!(
                s.status,
                zeron_proto::SessionStatus::Working | zeron_proto::SessionStatus::AwaitingInput
            )
        })
    }

    pub fn last_request(&self, chat_id: &str) -> Option<RunRequest> {
        lock(&self.inner.last_requests).get(chat_id).cloned()
    }

    pub fn subscribe(
        &self,
        chat_id: &str,
        after_seq: u64,
    ) -> Result<(Vec<JournaledEvent>, broadcast::Receiver<JournaledEvent>), EngineError> {
        let rx = {
            let mut hubs = lock(&self.inner.hubs);
            hubs.entry(chat_id.to_string())
                .or_insert_with(|| broadcast::channel(1024).0)
                .subscribe()
        };
        let replay = self
            .inner
            .journal
            .replay(chat_id, after_seq)?
            .into_iter()
            .map(|(seq, event)| JournaledEvent { seq, event })
            .collect();
        Ok((replay, rx))
    }

    pub async fn dispatch(
        &self,
        chat_id: &str,
        harness_id: HarnessId,
        request: RunRequest,
        message_id: Option<String>,
    ) -> Result<String, EngineError> {
        self.dispatch_with(chat_id, harness_id, request, message_id, false)
            .await
    }

    fn dispatch_with<'a>(
        &'a self,
        chat_id: &'a str,
        harness_id: HarnessId,
        request: RunRequest,
        message_id: Option<String>,
        startup_retry: bool,
    ) -> futures::future::BoxFuture<'a, Result<String, EngineError>> {
        Box::pin(self.dispatch_inner(chat_id, harness_id, request, message_id, startup_retry))
    }

    async fn dispatch_inner(
        &self,
        chat_id: &str,
        harness_id: HarnessId,
        mut request: RunRequest,
        mut message_id: Option<String>,
        startup_retry: bool,
    ) -> Result<String, EngineError> {
        request.cwd = expand_home(&request.cwd);
        self.note_turn_start(chat_id, &request.cwd);
        let routed = lock(&self.inner.runs).get(chat_id).map(|h| {
            (
                h.run_id.clone(),
                h.steerable,
                h.runtime_config.can_route(harness_id, &request),
                h.steer_tx.clone(),
                h.routed_steers.clone(),
            )
        });
        if let Some((run_id, steerable, same_runtime, steer_tx, ledger)) = routed {
            let message = SteerMessage {
                prompt: request.prompt.clone(),
                message_id: message_id.clone(),
            };
            if steerable && same_runtime && steer_tx.try_send(message).is_ok() {
                let user_id = message_id.clone().unwrap_or_else(new_id);
                lock(&ledger).push_back(RoutedSteer {
                    prompt: request.prompt.clone(),
                    message_id: user_id.clone(),
                });
                let handle = self.doc_handle(chat_id)?;
                handle.write_user_message(&user_id, &request.prompt, now_ms())?;
                if self.is_live(chat_id, &run_id) {
                    self.set_status(chat_id, SessionStatus::Working, false);
                    self.inner.note_message(chat_id, &request.prompt);
                    return Ok(run_id);
                }
                let reclaimed = {
                    let mut ledger = lock(&ledger);
                    let before = ledger.len();
                    ledger.retain(|s| s.message_id != user_id);
                    ledger.len() != before
                };
                if !reclaimed {
                    self.inner.note_message(chat_id, &request.prompt);
                    return Ok(run_id);
                }
                message_id = Some(user_id);
            }
            if !same_runtime {
                tracing::debug!(
                    chat = %chat_id,
                    "restarting live harness to apply changed run configuration"
                );
            }
            self.interrupt(chat_id).await?;
        }

        let harness = self.inner.registry.resolve(harness_id)?;
        let handle = self.doc_handle(chat_id)?;
        let user_id = message_id.unwrap_or_else(new_id);
        handle.write_user_message(&user_id, &request.prompt, now_ms())?;

        let mut resume_injected = false;
        if request.resume.is_none() {
            request.resume = self.inner.resume_for(chat_id, &request.cwd);
            resume_injected = request.resume.is_some();
        }
        lock(&self.inner.last_requests).insert(chat_id.to_string(), request.clone());

        let run_id = new_id();
        let (steer_tx, steer_rx) = mpsc::channel::<SteerMessage>(32);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (engine_tx, engine_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let pending_inputs: PendingInputs = Arc::new(Mutex::new(HashMap::new()));

        let request_input = {
            let pending = pending_inputs.clone();
            let engine_tx = engine_tx.clone();
            Box::new(move |questions: Vec<UserInputQuestion>| {
                let (tx, rx) = oneshot::channel();
                let request_id = new_id();
                lock(&pending).insert(request_id.clone(), tx);
                let _ = engine_tx.send(AgentEvent::InputRequested {
                    request_id,
                    questions,
                });
                rx
            })
        };
        let interrupt_token = CancellationToken::new();
        let controls = RunControls {
            request_input,
            steering: steer_rx,
            interrupt: interrupt_token.clone(),
        };

        lock(&self.inner.runs).insert(
            chat_id.to_string(),
            RunHandle {
                run_id: run_id.clone(),
                steerable: harness.supports_steering(),
                runtime_config: RuntimeConfig::from_request(harness_id, &request),
                steer_tx,
                interrupt_token,
                cancel: cancel_tx,
                engine_tx,
                pending_inputs,
                routed_steers: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            },
        );
        self.set_status(chat_id, SessionStatus::Working, true);
        self.inner.note_message(chat_id, &request.prompt);

        if let Some(titles) = self.inner.titles.get() {
            titles.maybe_generate(chat_id, harness_id, &request.prompt, &request.cwd);
        }

        tokio::spawn(drive_run(
            self.inner.clone(),
            chat_id.to_string(),
            run_id.clone(),
            harness,
            request,
            handle.doc_arc(),
            controls,
            engine_rx,
            cancel_rx,
            RunResumeState {
                user_message_id: user_id,
                resume_injected,
                startup_retry,
            },
        ));
        Ok(run_id)
    }

    pub async fn steer(
        &self,
        chat_id: &str,
        prompt: &str,
        message_id: Option<String>,
    ) -> Result<SteerOutcome, EngineError> {
        let target = lock(&self.inner.runs)
            .get(chat_id)
            .filter(|h| h.steerable)
            .map(|h| {
                (
                    h.run_id.clone(),
                    h.steer_tx.clone(),
                    h.routed_steers.clone(),
                )
            });
        let Some((run_id, steer_tx, ledger)) = target else {
            return Ok(SteerOutcome::NotSteerable);
        };
        let message = SteerMessage {
            prompt: prompt.to_string(),
            message_id: message_id.clone(),
        };
        if steer_tx.try_send(message).is_err() {
            return Ok(SteerOutcome::NotSteerable);
        }
        let user_id = message_id.unwrap_or_else(new_id);
        lock(&ledger).push_back(RoutedSteer {
            prompt: prompt.to_string(),
            message_id: user_id.clone(),
        });
        let handle = self.doc_handle(chat_id)?;
        handle.write_user_message(&user_id, prompt, now_ms())?;
        if let Some(request) = self.last_request(chat_id) {
            self.note_turn_start(chat_id, &request.cwd);
        }
        if self.is_live(chat_id, &run_id) {
            self.set_status(chat_id, SessionStatus::Working, false);
            self.inner.note_message(chat_id, prompt);
            return Ok(SteerOutcome::Accepted);
        }
        let reclaimed = {
            let mut ledger = lock(&ledger);
            let before = ledger.len();
            ledger.retain(|s| s.message_id != user_id);
            ledger.len() != before
        };
        if reclaimed {
            return Ok(SteerOutcome::NotSteerable);
        }
        self.inner.note_message(chat_id, prompt);
        Ok(SteerOutcome::Accepted)
    }

    pub async fn interrupt(&self, chat_id: &str) -> Result<bool, EngineError> {
        let target = lock(&self.inner.runs).get(chat_id).map(|h| {
            (
                h.run_id.clone(),
                h.interrupt_token.clone(),
                h.cancel.clone(),
                h.pending_inputs.clone(),
            )
        });
        let Some((run_id, token, cancel, pending)) = target else {
            return Ok(false);
        };
        let parked: Vec<_> = lock(&pending).drain().map(|(_, tx)| tx).collect();
        for tx in parked {
            let _ = tx.send(Vec::new());
        }
        token.cancel();
        let _ = cancel.send(true);
        for _ in 0..500 {
            if !self.is_live(chat_id, &run_id) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(true)
    }

    pub fn respond_input(
        &self,
        chat_id: &str,
        request_id: &str,
        answers: Vec<UserInputAnswer>,
    ) -> Result<bool, EngineError> {
        let target = lock(&self.inner.runs)
            .get(chat_id)
            .map(|h| (h.pending_inputs.clone(), h.engine_tx.clone()));
        let Some((pending, engine_tx)) = target else {
            return Ok(false);
        };
        let Some(resolver) = lock(&pending).remove(request_id) else {
            return Ok(false);
        };
        let _ = resolver.send(answers);
        let _ = engine_tx.send(AgentEvent::InputResolved {
            request_id: request_id.to_string(),
        });
        Ok(true)
    }

    pub fn recover_stale(&self) -> Result<usize, EngineError> {
        const MAX_AUTO_RESUME: u32 = 3;
        const RESUME_FRESH_MS: i64 = 12 * 60 * 60 * 1000;

        let stale = self.inner.journal.stale_sessions()?;
        let mut recovered = 0usize;
        for chat_id in stale {
            if lock(&self.inner.runs).contains_key(&chat_id) {
                continue;
            }
            let handle = self.doc_handle(&chat_id)?;
            if let Some((session_id, cwd)) = self.inner.journal_harness_session(&chat_id) {
                self.inner
                    .remember_harness_session(&chat_id, &session_id, &cwd);
            }
            let prompt = handle.doc().read_entries().ok().and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|e| e.role == MessageRole::User)
                    .and_then(|e| {
                        e.parts.iter().find_map(|p| match p {
                            MessagePart::Text { text, .. } => Some((e.id.clone(), text.clone())),
                            _ => None,
                        })
                    })
            });
            let attempts = self.inner.journal.resume_attempts(&chat_id);
            let fresh = handle
                .doc()
                .read_entries()
                .ok()
                .and_then(|entries| {
                    entries
                        .iter()
                        .rev()
                        .find(|e| e.status == Some(MessageStatus::Streaming))
                        .map(|e| now_ms() - e.created_at < RESUME_FRESH_MS)
                })
                .unwrap_or(false);
            let will_resume = fresh && prompt.is_some() && attempts < MAX_AUTO_RESUME;

            let note = if will_resume {
                "Run interrupted by engine restart — resuming"
            } else {
                "Run interrupted by engine restart"
            };
            let done = AgentEvent::Done {
                status: DoneStatus::Interrupted,
                result: None,
                error: Some(note.into()),
                session_id: None,
            };
            self.inner.publish(&chat_id, &done);
            let stamped = handle.mark_abandoned_streams(note)?.len();
            self.set_status(&chat_id, SessionStatus::Idle, false);
            tracing::info!(chat = %chat_id, stamped, will_resume, attempts, "recovered stale session journal");
            recovered += 1;

            if !will_resume {
                continue;
            }
            let attempt = self.inner.journal.note_resume_attempt(&chat_id);
            let (user_id, prompt_text) = prompt.expect("gated by will_resume");
            let sessions = self.clone();
            tokio::spawn(async move {
                let Some(host) = sessions.inner.doc_host() else {
                    return;
                };
                let request = sessions
                    .last_request(&chat_id)
                    .or_else(|| host.request_from_chat_row(&chat_id, &prompt_text))
                    .or_else(|| {
                        let (_, cwd) = sessions.inner.journal_harness_session(&chat_id)?;
                        Some(RunRequest {
                            prompt: String::new(),
                            harness: None,
                            model: None,
                            reasoning: None,
                            model_options: Default::default(),
                            cwd,
                            sandbox: zeron_proto::SandboxLevel::WorkspaceWrite,
                            auto_approve: false,
                            attachments: Vec::new(),
                            resume: None,
                        })
                    });
                let Some(mut request) = request else {
                    tracing::warn!(chat = %chat_id, "auto-resume skipped: no run config");
                    return;
                };
                request.prompt = prompt_text;
                request.resume = None;
                request.attachments = Vec::new();
                let harness_id = host.harness_for_request(&chat_id, &request);
                match sessions
                    .dispatch(&chat_id, harness_id, request, Some(user_id))
                    .await
                {
                    Ok(_) => {
                        tracing::info!(chat = %chat_id, attempt, "auto-resumed crashed run")
                    }
                    Err(err) => {
                        tracing::warn!(chat = %chat_id, error = %err, "auto-resume dispatch failed")
                    }
                }
            });
        }
        Ok(recovered)
    }

    pub async fn shutdown(&self) {
        let chats: Vec<String> = lock(&self.inner.runs).keys().cloned().collect();
        for chat_id in chats {
            if let Err(err) = self.interrupt(&chat_id).await {
                tracing::warn!(chat = %chat_id, error = %err, "shutdown interrupt failed");
            }
        }
    }

    fn is_live(&self, chat_id: &str, run_id: &str) -> bool {
        lock(&self.inner.runs)
            .get(chat_id)
            .is_some_and(|h| h.run_id == run_id)
    }

    fn set_status(&self, chat_id: &str, status: SessionStatus, fresh_start: bool) {
        self.inner.set_status(chat_id, status, fresh_start);
    }
}

impl Inner {
    fn publish(&self, chat_id: &str, event: &AgentEvent) -> u64 {
        let seq = match self.journal.append(chat_id, event) {
            Ok(seq) => seq,
            Err(err) => {
                tracing::error!(chat = %chat_id, error = %err, "journal append failed");
                0
            }
        };
        if let Some(hub) = lock(&self.hubs).get(chat_id) {
            let _ = hub.send(JournaledEvent {
                seq,
                event: event.clone(),
            });
        }
        seq
    }

    fn touch_session(&self, chat_id: &str) {
        const TOUCH_THROTTLE_MS: i64 = 10_000;
        let now = Utc::now();
        let session = {
            let mut statuses = lock(&self.statuses);
            let Some(entry) = statuses.get_mut(chat_id) else {
                return;
            };
            let age = now
                .signed_duration_since(entry.updated_at)
                .num_milliseconds();
            if age < TOUCH_THROTTLE_MS {
                return;
            }
            entry.updated_at = now;
            let session = entry.clone();
            let mut list: Vec<Session> = statuses.values().cloned().collect();
            list.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
            self.sessions_tx.send_replace(list);
            session
        };
        if let Some(ws) = self.workspace() {
            ws.record_session(&session);
        }
    }

    fn set_status(&self, chat_id: &str, status: SessionStatus, fresh_start: bool) {
        let now = Utc::now();
        let session = {
            let mut statuses = lock(&self.statuses);
            let entry = statuses
                .entry(chat_id.to_string())
                .or_insert_with(|| Session {
                    chat_id: chat_id.to_string(),
                    device_id: self.device_id.clone(),
                    status,
                    started_at: None,
                    updated_at: now,
                });
            let was_active = matches!(
                entry.status,
                SessionStatus::Working | SessionStatus::AwaitingInput
            );
            entry.status = status;
            entry.updated_at = now;
            match status {
                SessionStatus::Working if fresh_start || !was_active => {
                    entry.started_at = Some(now);
                }
                SessionStatus::Working | SessionStatus::AwaitingInput => {}
                SessionStatus::Idle | SessionStatus::Errored => {
                    entry.started_at = None;
                }
            }
            let session = entry.clone();
            let mut list: Vec<Session> = statuses.values().cloned().collect();
            list.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
            self.sessions_tx.send_replace(list);
            session
        };
        if let Some(ws) = self.workspace() {
            ws.record_session(&session);
        }
    }

    fn doc_host(&self) -> Option<DocHost> {
        lock(&self.doc_host).clone()
    }

    fn workspace(&self) -> Option<crate::workspace_host::WorkspaceHost> {
        self.doc_host().and_then(|host| host.workspace().cloned())
    }

    fn note_message(&self, chat_id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        if let Some(ws) = self.workspace() {
            ws.note_message(chat_id, text);
        }
    }

    fn remember_harness_session(&self, chat_id: &str, session_id: &str, cwd: &str) {
        if session_id.is_empty() {
            return;
        }
        lock(&self.harness_sessions).insert(
            chat_id.to_string(),
            HarnessSessionRef {
                session_id: session_id.to_string(),
                cwd: cwd.to_string(),
            },
        );
        if let Some(ws) = self.workspace() {
            ws.set_chat_harness_session(chat_id, session_id, cwd);
        }
    }

    fn resume_for(&self, chat_id: &str, cwd: &str) -> Option<String> {
        let cwd_ok = |session_cwd: &str| session_cwd.is_empty() || session_cwd == cwd;
        if let Some(known) = lock(&self.harness_sessions).get(chat_id).cloned() {
            return (!known.session_id.is_empty() && cwd_ok(&known.cwd))
                .then_some(known.session_id);
        }
        if let Some(ws) = self.workspace()
            && let Some((session_id, session_cwd)) = ws.chat_harness_session(chat_id)
        {
            return (!session_id.is_empty() && cwd_ok(session_cwd.as_deref().unwrap_or("")))
                .then_some(session_id);
        }
        let (session_id, session_cwd) = self.journal_harness_session(chat_id)?;
        self.remember_harness_session(chat_id, &session_id, &session_cwd);
        cwd_ok(&session_cwd).then_some(session_id)
    }

    fn journal_harness_session(&self, chat_id: &str) -> Option<(String, String)> {
        let events = match self.journal.replay(chat_id, 0) {
            Ok(events) => events,
            Err(err) => {
                tracing::warn!(chat = %chat_id, error = %err, "journal scan for harness session failed");
                return None;
            }
        };
        let mut current_cwd = String::new();
        let mut found: Option<(String, String)> = None;
        for (_, event) in events {
            match event {
                AgentEvent::SessionStarted {
                    session_id, cwd, ..
                } => {
                    current_cwd = cwd;
                    if !session_id.is_empty() {
                        found = Some((session_id, current_cwd.clone()));
                    }
                }
                AgentEvent::Done {
                    session_id: Some(session_id),
                    ..
                } if !session_id.is_empty() => {
                    found = Some((session_id, current_cwd.clone()));
                }
                _ => {}
            }
        }
        found
    }

    fn remove_run(&self, chat_id: &str, run_id: &str) {
        let mut runs = lock(&self.runs);
        if runs.get(chat_id).is_some_and(|h| h.run_id == run_id) {
            runs.remove(chat_id);
        }
    }
}

pub(crate) fn subagent_doc_id(chat_id: &str, tool_use_id: &str) -> String {
    let clean = tool_use_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    let budget = 128usize.saturating_sub(chat_id.len() + "--sub--".len());
    if clean && !tool_use_id.is_empty() && tool_use_id.len() <= budget {
        return format!("{chat_id}--sub--{tool_use_id}");
    }
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(tool_use_id.as_bytes());
    let mut hex = String::with_capacity(16);
    for b in &digest[..8] {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    format!("{chat_id}--sub--{hex}")
}

struct SubagentSink {
    doc_id: String,
    doc: Arc<SessionDoc>,
    entry_id: String,
    started_at: i64,
    entry_index: Option<usize>,
    written: Vec<MessagePart>,
    folded: Vec<MessagePart>,
    dirty: bool,
}

impl SubagentSink {
    fn flush(&mut self, device_id: &str) {
        if !self.dirty || self.folded.is_empty() {
            return;
        }
        let rendered = render_parts(&self.folded);
        let result = match self.entry_index {
            Some(ix) => {
                let mut w = SegmentWriter::resume(&self.doc, ix, std::mem::take(&mut self.written));
                let r = w.sync(&rendered);
                let (ix, written) = w.into_state();
                self.entry_index = Some(ix);
                self.written = written;
                r
            }
            None => {
                match SegmentWriter::begin(&self.doc, &self.entry_id, device_id, self.started_at) {
                    Ok(mut w) => {
                        let r = w.sync(&rendered);
                        let (ix, written) = w.into_state();
                        self.entry_index = Some(ix);
                        self.written = written;
                        r
                    }
                    Err(e) => Err(e),
                }
            }
        };
        if let Err(err) = result {
            tracing::warn!(doc = %self.doc_id, error = %err, "subagent sink flush failed");
        }
        self.dirty = false;
    }

    fn finish(mut self, device_id: &str, status: MessageStatus) -> Option<String> {
        let rendered = render_parts(&self.folded);
        let finished = match self.entry_index {
            Some(ix) => SegmentWriter::resume(&self.doc, ix, std::mem::take(&mut self.written))
                .finish(&rendered, status),
            None if !self.folded.is_empty() => {
                match SegmentWriter::begin(&self.doc, &self.entry_id, device_id, self.started_at) {
                    Ok(w) => w.finish(&rendered, status),
                    Err(e) => Err(e),
                }
            }
            None => Ok(()),
        };
        if let Err(err) = finished {
            tracing::warn!(doc = %self.doc_id, error = %err, "subagent sink finish failed");
        }
        let entries = zeron_doc::join_continuation_entries(self.doc.read_entries().ok()?);
        serde_json::to_string(&entries).ok()
    }
}

fn subagent_chip_update(event: &AgentEvent) -> Option<&'static str> {
    match event {
        AgentEvent::Done { status, .. } => Some(match status {
            DoneStatus::Errored => "failed",
            _ => "done",
        }),
        _ => Some("running"),
    }
}

fn render_parts(parts: &[MessagePart]) -> Vec<MessagePart> {
    parts
        .iter()
        .map(|part| match part {
            MessagePart::Tool {
                id,
                call,
                is_error,
                resolved,
                output,
                diff,
                output_ref,
                output_bytes,
                diff_ref,
                diff_stats,
                subagent_ref,
                subagent_status,
                subagent_tail,
            } => MessagePart::Tool {
                id: id.clone(),
                call: sanitize_tool_call(call),
                is_error: *is_error,
                resolved: *resolved,
                output: output.clone(),
                diff: diff.clone(),
                output_ref: output_ref.clone(),
                output_bytes: *output_bytes,
                diff_ref: diff_ref.clone(),
                diff_stats: diff_stats.clone(),
                subagent_ref: subagent_ref.clone(),
                subagent_status: *subagent_status,
                subagent_tail: subagent_tail.clone(),
            },
            other => other.clone(),
        })
        .collect()
}

fn folded_text(parts: &[MessagePart]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            MessagePart::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn sync_segment<'a>(
    doc: &'a SessionDoc,
    writer: &mut Option<SegmentWriter<'a>>,
    entry_id: &str,
    device_id: &str,
    started_at: i64,
    folded: &[MessagePart],
) -> Result<(), DocError> {
    if folded.is_empty() {
        return Ok(());
    }
    let rendered = render_parts(folded);
    if writer.is_none() {
        *writer = Some(SegmentWriter::begin(doc, entry_id, device_id, started_at)?);
    }
    if let Some(w) = writer.as_mut() {
        w.sync(&rendered)?;
    }
    Ok(())
}

fn finish_segment<'a>(
    doc: &'a SessionDoc,
    writer: Option<SegmentWriter<'a>>,
    entry_id: &str,
    device_id: &str,
    started_at: i64,
    folded: &[MessagePart],
    status: MessageStatus,
) -> Result<(), DocError> {
    let rendered = render_parts(folded);
    match writer {
        Some(w) => w.finish(&rendered, status),
        None if !folded.is_empty() => {
            SegmentWriter::begin(doc, entry_id, device_id, started_at)?.finish(&rendered, status)
        }
        None => Ok(()),
    }
}

fn expand_home(cwd: &str) -> String {
    match cwd.strip_prefix("~") {
        Some("") => crate::repos::home_dir().to_string_lossy().into_owned(),
        Some(rest) if rest.starts_with('/') => crate::repos::home_dir()
            .join(&rest[1..])
            .to_string_lossy()
            .into_owned(),
        _ => cwd.to_string(),
    }
}

struct RunResumeState {
    user_message_id: String,
    resume_injected: bool,
    startup_retry: bool,
}

#[allow(clippy::too_many_arguments)]
async fn drive_run(
    inner: Arc<Inner>,
    chat_id: String,
    run_id: String,
    harness: Arc<dyn Harness>,
    request: RunRequest,
    doc: Arc<SessionDoc>,
    controls: RunControls,
    mut engine_rx: mpsc::UnboundedReceiver<AgentEvent>,
    mut cancel_rx: watch::Receiver<bool>,
    resume_state: RunResumeState,
) {
    let device_id = inner.device_id.clone();
    let harness_id = harness.id();
    let user_prompt = request.prompt.clone();
    let run_cwd = request.cwd.clone();
    let mut retry_request = Some(RunRequest {
        resume: None,
        ..request.clone()
    });
    let mut stream = match harness.run(request, controls).await {
        Ok(stream) => stream,
        Err(err) => {
            let message = err.to_string();
            inner.publish(
                &chat_id,
                &AgentEvent::Error {
                    message: message.clone(),
                },
            );
            inner.publish(
                &chat_id,
                &AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(message),
                    session_id: None,
                },
            );
            inner.remove_run(&chat_id, &run_id);
            inner.set_status(&chat_id, SessionStatus::Errored, false);
            return;
        }
    };

    let doc_ref: &SessionDoc = &doc;
    let mut folded: Vec<MessagePart> = Vec::new();
    let mut seen_tools: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut entry_id = new_id();
    let mut segment_started = now_ms();
    let mut writer: Option<SegmentWriter<'_>> = None;
    let mut dirty = false;
    let mut flush_at = tokio::time::Instant::now();
    let mut interrupt_deadline: Option<tokio::time::Instant> = None;
    let mut interrupted = false;
    let mut saw_session_started = false;
    let mut live_heartbeat = tokio::time::interval(std::time::Duration::from_secs(15));
    live_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    const SESSION_IDLE: std::time::Duration = std::time::Duration::from_secs(30 * 60);
    let mut idle_since: Option<tokio::time::Instant> = None;
    let steerable = harness.supports_steering();
    let deterministic_turn_end = harness.deterministic_turn_end();
    let quiesce_after: Option<std::time::Duration> = match std::env::var("ZERON_TURN_QUIESCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(0) => None,
        Some(ms) => Some(std::time::Duration::from_millis(ms)),
        None if deterministic_turn_end => None,
        None => Some(std::time::Duration::from_secs(120)),
    };
    let mut last_stream_activity = tokio::time::Instant::now();
    let mut subagents: std::collections::HashMap<String, SubagentSink> =
        std::collections::HashMap::new();
    let final_status = loop {
        let event: AgentEvent = tokio::select! {
            biased;
            changed = cancel_rx.changed(), if !interrupted => {
                let _ = changed;
                interrupted = true;
                interrupt_deadline = Some(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(3),
                );
                continue;
            }
            _ = tokio::time::sleep_until(
                interrupt_deadline.unwrap_or_else(tokio::time::Instant::now)
            ), if interrupt_deadline.is_some() => AgentEvent::Done {
                status: DoneStatus::Interrupted,
                result: None,
                error: None,
                session_id: None,
            },
            _ = live_heartbeat.tick() => {
                inner.touch_session(&chat_id);
                continue;
            }
            _ = tokio::time::sleep_until(
                idle_since.map(|at| at + SESSION_IDLE).unwrap_or_else(tokio::time::Instant::now)
            ), if idle_since.is_some() => {
                tracing::info!(chat = %chat_id, "reaping idle persistent session");
                if let Some(token) = lock(&inner.runs)
                    .get(&chat_id)
                    .filter(|h| h.run_id == run_id)
                    .map(|h| h.interrupt_token.clone())
                {
                    token.cancel();
                }
                break SessionStatus::Idle;
            }
            Some(event) = engine_rx.recv() => event,
            next = stream.next() => match next {
                Some(Ok(event)) => event,
                Some(Err(err)) if idle_since.is_some() => {
                    tracing::warn!(chat = %chat_id, error = %err, "parked session child died; ending clean");
                    break SessionStatus::Idle;
                }
                Some(Err(err)) => AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(err.to_string()),
                    session_id: None,
                },
                None if interrupted => AgentEvent::Done {
                    status: DoneStatus::Interrupted,
                    result: None,
                    error: None,
                    session_id: None,
                },
                None if idle_since.is_some() => break SessionStatus::Idle,
                None => AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some("harness stream ended without Done".into()),
                    session_id: None,
                },
            },
            _ = tokio::time::sleep_until(flush_at), if dirty || subagents.values().any(|s| s.dirty) => {
                if dirty {
                    if let Err(err) = sync_segment(
                        doc_ref, &mut writer, &entry_id, &device_id, segment_started, &folded,
                    ) {
                        tracing::warn!(chat = %chat_id, error = %err, "segment sync failed");
                    }
                    dirty = false;
                }
                for sink in subagents.values_mut() {
                    sink.flush(&device_id);
                }
                continue;
            }
            _ = tokio::time::sleep_until(
                last_stream_activity + quiesce_after.unwrap_or_default()
            ), if quiesce_after.is_some()
                && idle_since.is_none()
                && !interrupted
                && steerable
                && !folded.iter().any(|p| match p {
                    MessagePart::Tool { id, resolved: false, .. } => {
                        id != zeron_proto::LIVE_PLAN_TOOL_ID
                    }
                    MessagePart::Input { resolved: false, .. } => true,
                    _ => false,
                }) =>
            {
                tracing::warn!(
                    chat = %chat_id,
                    quiet_ms = quiesce_after.unwrap_or_default().as_millis() as u64,
                    "turn quiesced: stream silent after completed output with no \
                     turn-end; parking (suspected missing harness Done)"
                );
                if !folded.is_empty() || writer.is_some() {
                    if let Err(err) = finish_segment(
                        doc_ref,
                        writer.take(),
                        &entry_id,
                        &device_id,
                        segment_started,
                        &folded,
                        MessageStatus::Complete,
                    ) {
                        tracing::warn!(chat = %chat_id, error = %err, "quiesce segment finish failed");
                    }
                    inner.note_message(&chat_id, &folded_text(&folded));
                }
                folded.clear();
                dirty = false;
                entry_id = new_id();
                segment_started = now_ms();
                idle_since = Some(tokio::time::Instant::now());
                inner.set_status(&chat_id, SessionStatus::Idle, false);
                continue;
            }
        };

        if let AgentEvent::Subagent {
            parent_tool_use_id,
            event: sub_event,
        } = &event
        {
            inner.publish(&chat_id, &event);
            let sub_id = subagent_doc_id(&chat_id, parent_tool_use_id);
            let chip_streaming = folded
                .iter()
                .any(|p| matches!(p, MessagePart::Tool { id, .. } if id == parent_tool_use_id));
            let sink_known = subagents.contains_key(parent_tool_use_id);
            if chip_streaming {
                if !sink_known {
                    for p in folded.iter_mut() {
                        if let MessagePart::Tool {
                            id, subagent_ref, ..
                        } = p
                            && id == parent_tool_use_id
                        {
                            *subagent_ref = Some(sub_id.clone());
                        }
                    }
                }
                zeron_doc::fold_event_into_parts(&mut folded, &event);
                if !dirty {
                    dirty = true;
                    flush_at = tokio::time::Instant::now()
                        + std::time::Duration::from_millis(STREAM_COMMIT_MS);
                }
            }
            let done_only = !sink_known && matches!(sub_event.as_ref(), AgentEvent::Done { .. });
            if !sink_known && !done_only {
                let opened = inner.doc_host().and_then(|host| match host.open(&sub_id) {
                    Ok(handle) => Some(handle.doc_arc()),
                    Err(err) => {
                        tracing::warn!(doc = %sub_id, error = %err, "subagent doc open failed (chip-only)");
                        None
                    }
                });
                if let Some(sub_doc) = opened {
                    subagents.insert(
                        parent_tool_use_id.clone(),
                        SubagentSink {
                            doc_id: sub_id.clone(),
                            doc: sub_doc,
                            entry_id: new_id(),
                            started_at: now_ms(),
                            entry_index: None,
                            written: Vec::new(),
                            folded: Vec::new(),
                            dirty: false,
                        },
                    );
                    if !chip_streaming {
                        let _ = doc_ref.update_subagent_chip(
                            parent_tool_use_id,
                            Some(&sub_id),
                            Some("running"),
                            None,
                        );
                    }
                }
            }
            let done = matches!(sub_event.as_ref(), AgentEvent::Done { .. });
            if let Some(sink) = subagents.get_mut(parent_tool_use_id) {
                zeron_doc::fold_event_into_parts(&mut sink.folded, sub_event);
                sink.dirty = true;
                if !chip_streaming && done {
                    let _ = doc_ref.update_subagent_chip(
                        parent_tool_use_id,
                        None,
                        subagent_chip_update(sub_event),
                        None,
                    );
                }
                if done {
                    let status = match sub_event.as_ref() {
                        AgentEvent::Done {
                            status: DoneStatus::Errored,
                            ..
                        } => MessageStatus::Complete,
                        AgentEvent::Done {
                            status: DoneStatus::Interrupted,
                            ..
                        } => MessageStatus::Aborted,
                        _ => MessageStatus::Complete,
                    };
                    let sink = subagents.remove(parent_tool_use_id).expect("checked");
                    let _ = sink.finish(&device_id, status);
                }
            }
            continue;
        }

        inner.touch_session(&chat_id);
        last_stream_activity = tokio::time::Instant::now();
        if let AgentEvent::InputRequested { request_id, .. } = &event {
            let pending = lock(&inner.runs)
                .get(&chat_id)
                .map(|h| h.pending_inputs.clone());
            let known = pending.is_some_and(|p| lock(&p).contains_key(request_id));
            if !known {
                tracing::warn!(
                    chat = %chat_id,
                    request = %request_id,
                    "dropping harness-emitted InputRequested (unknown id; \
                     the engine input bridge owns this lifecycle)"
                );
                continue;
            }
        }
        if idle_since.is_some() {
            match &event {
                AgentEvent::Steered { .. } | AgentEvent::Done { .. } => {
                    idle_since = None;
                }
                AgentEvent::InputRequested { request_id, .. } => {
                    let resolver = lock(&inner.runs)
                        .get(&chat_id)
                        .and_then(|h| lock(&h.pending_inputs).remove(request_id));
                    if let Some(tx) = resolver {
                        let _ = tx.send(Vec::new());
                    }
                    tracing::debug!(chat = %chat_id, "parked session: post-turn input request auto-declined");
                    continue;
                }
                AgentEvent::InputResolved { .. } => continue,
                _ => {
                    tracing::debug!(chat = %chat_id, "parked session: ignoring late agent event");
                    continue;
                }
            }
        }
        if matches!(&event, AgentEvent::ReasoningDelta { text } if text.is_empty()) {
            continue;
        }

        let in_segment = |folded: &[MessagePart], id: &str| {
            folded
                .iter()
                .any(|p| matches!(p, MessagePart::Tool { id: pid, .. } if pid == id))
        };
        match &event {
            AgentEvent::ToolCall { id, .. } if id == zeron_proto::LIVE_PLAN_TOOL_ID => {}
            AgentEvent::ToolResult { id, .. } if id == zeron_proto::LIVE_PLAN_TOOL_ID => {}
            AgentEvent::ToolCall { id, .. } => {
                if !in_segment(&folded, id) && seen_tools.contains(id) {
                    continue;
                }
                seen_tools.insert(id.clone());
            }
            AgentEvent::ToolResult { id, .. }
                if !in_segment(&folded, id) && seen_tools.contains(id) =>
            {
                continue;
            }
            _ => {}
        }

        if resume_state.resume_injected
            && !resume_state.startup_retry
            && !saw_session_started
            && folded.is_empty()
            && !interrupted
            && matches!(
                &event,
                AgentEvent::Done {
                    status: DoneStatus::Errored,
                    ..
                }
            )
            && let Some(retry) = retry_request.take()
        {
            tracing::warn!(
                chat = %chat_id,
                "run died before session start; retrying once (resume kept)"
            );
            inner.remove_run(&chat_id, &run_id);
            let engine = SessionsEngine {
                inner: inner.clone(),
            };
            let chat = chat_id.clone();
            let message_id = resume_state.user_message_id.clone();
            tokio::spawn(async move {
                if let Err(err) = engine
                    .dispatch_with(&chat, harness_id, retry, Some(message_id), true)
                    .await
                {
                    tracing::error!(chat = %chat, error = %err, "startup-crash retry dispatch failed");
                    engine
                        .inner
                        .set_status(&chat, SessionStatus::Errored, false);
                }
            });
            return;
        }

        if let AgentEvent::Steered {
            next_assistant_message_id,
            ..
        } = &event
        {
            inner.publish(&chat_id, &event);
            if let Err(err) = finish_segment(
                doc_ref,
                writer.take(),
                &entry_id,
                &device_id,
                segment_started,
                &folded,
                MessageStatus::Complete,
            ) {
                tracing::warn!(chat = %chat_id, error = %err, "segment finish failed");
            }
            inner.note_message(&chat_id, &folded_text(&folded));
            folded.clear();
            dirty = false;
            entry_id = next_assistant_message_id.clone().unwrap_or_else(new_id);
            segment_started = now_ms();
            inner.set_status(&chat_id, SessionStatus::Working, true);
            if let Some(h) = lock(&inner.runs)
                .get(&chat_id)
                .filter(|h| h.run_id == run_id)
            {
                lock(&h.routed_steers).pop_front();
            }
            continue;
        }

        match &event {
            AgentEvent::SessionStarted {
                session_id, cwd, ..
            } => {
                saw_session_started = true;
                inner.remember_harness_session(&chat_id, session_id, cwd);
            }
            AgentEvent::Done {
                session_id: Some(session_id),
                ..
            } => {
                inner.remember_harness_session(&chat_id, session_id, &run_cwd);
            }
            AgentEvent::InputRequested { .. } => {
                inner.set_status(&chat_id, SessionStatus::AwaitingInput, false);
            }
            AgentEvent::InputResolved { .. } => {
                inner.set_status(&chat_id, SessionStatus::Working, false);
            }
            _ => {}
        }

        inner.publish(&chat_id, &event);

        let skip_fold = matches!(&event, AgentEvent::SessionStarted { .. }) && !folded.is_empty();
        if !skip_fold {
            fold_event_into_parts(&mut folded, &event);
        }

        if let AgentEvent::Done { status, .. } = &event {
            let pending = lock(&inner.runs)
                .get(&chat_id)
                .filter(|h| h.run_id == run_id)
                .map(|h| h.pending_inputs.clone());
            if let Some(pending) = pending {
                for (_, tx) in lock(&pending).drain() {
                    let _ = tx.send(Vec::new());
                }
            }
            let message_status = match status {
                DoneStatus::Interrupted => MessageStatus::Aborted,
                DoneStatus::Completed | DoneStatus::Errored => MessageStatus::Complete,
            };
            for part in folded.iter_mut() {
                if let MessagePart::Input { resolved, .. } = part {
                    *resolved = true;
                }
            }
            let nothing_streamed = writer.is_none() && folded.is_empty();
            if !nothing_streamed {
                if let Err(err) = finish_segment(
                    doc_ref,
                    writer.take(),
                    &entry_id,
                    &device_id,
                    segment_started,
                    &folded,
                    message_status,
                ) {
                    tracing::warn!(chat = %chat_id, error = %err, "final segment finish failed");
                }
                inner.note_message(&chat_id, &folded_text(&folded));
            }
            if *status == DoneStatus::Completed {
                inner.journal.clear_resume_attempts(&chat_id);
            }
            if *status == DoneStatus::Completed
                && let Some(titles) = inner.titles.get()
            {
                titles.maybe_generate(&chat_id, harness_id, &user_prompt, &run_cwd);
            }
            if *status == DoneStatus::Completed && steerable && !interrupted {
                folded.clear();
                dirty = false;
                entry_id = new_id();
                segment_started = now_ms();
                saw_session_started = true;
                idle_since = Some(tokio::time::Instant::now());
                inner.set_status(&chat_id, SessionStatus::Idle, false);
                continue;
            }
            break match status {
                DoneStatus::Errored => SessionStatus::Errored,
                _ => SessionStatus::Idle,
            };
        }

        if !folded.is_empty() && !dirty {
            dirty = true;
            flush_at =
                tokio::time::Instant::now() + std::time::Duration::from_millis(STREAM_COMMIT_MS);
        }
    };

    for (parent_id, sink) in subagents.drain() {
        let _ = doc_ref.update_subagent_chip(&parent_id, None, Some("failed"), None);
        let _ = sink.finish(&device_id, MessageStatus::Aborted);
    }

    let orphans: Vec<RoutedSteer> = lock(&inner.runs)
        .get(&chat_id)
        .filter(|h| h.run_id == run_id)
        .map(|h| std::mem::take(&mut *lock(&h.routed_steers)).into())
        .unwrap_or_default();
    inner.remove_run(&chat_id, &run_id);
    inner.set_status(&chat_id, final_status, false);
    if !interrupted && !orphans.is_empty() {
        let engine = SessionsEngine {
            inner: inner.clone(),
        };
        let chat = chat_id.clone();
        tokio::spawn(async move {
            for steer in orphans {
                let Some(mut request) = engine.last_request(&chat) else {
                    tracing::warn!(chat = %chat, "orphaned steer lost: no run config to re-dispatch");
                    break;
                };
                request.prompt = steer.prompt.clone();
                request.resume = None;
                request.attachments = Vec::new();
                tracing::info!(chat = %chat, "re-dispatching steer orphaned by a dying run");
                if let Err(err) = engine
                    .dispatch(&chat, harness_id, request, Some(steer.message_id.clone()))
                    .await
                {
                    tracing::warn!(chat = %chat, error = %err, "orphaned steer re-dispatch failed");
                    engine
                        .inner
                        .set_status(&chat, SessionStatus::Errored, false);
                    break;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeConfig, subagent_doc_id};
    use zeron_proto::{HarnessId, RunRequest, SandboxLevel};

    fn request() -> RunRequest {
        RunRequest {
            prompt: "first".into(),
            harness: None,
            model: Some("grok-4.6".into()),
            reasoning: Some(zeron_proto::ReasoningLevel::High),
            model_options: serde_json::Map::new(),
            cwd: "/tmp".into(),
            sandbox: SandboxLevel::WorkspaceWrite,
            auto_approve: true,
            resume: None,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn live_routing_requires_the_same_runtime_configuration() {
        let initial = request();
        let config = RuntimeConfig::from_request(HarnessId::Grok, &initial);

        let mut follow_up = initial.clone();
        follow_up.prompt = "second".into();
        follow_up.resume = Some("session-1".into());
        assert!(config.can_route(HarnessId::Grok, &follow_up));

        follow_up.model = Some("grok-4.5".into());
        assert!(!config.can_route(HarnessId::Grok, &follow_up));
        follow_up.model = initial.model.clone();

        follow_up.reasoning = Some(zeron_proto::ReasoningLevel::Medium);
        assert!(!config.can_route(HarnessId::Grok, &follow_up));
        follow_up.reasoning = initial.reasoning;

        follow_up.attachments.push("/tmp/image.png".into());
        assert!(!config.can_route(HarnessId::Grok, &follow_up));
    }

    #[test]
    fn clean_tool_ids_ride_verbatim_and_fit_id_re() {
        let id = subagent_doc_id("chat-abc123", "toolu_01EjFLnNhCiMR2PBNKcVvkT");
        assert_eq!(id, "chat-abc123--sub--toolu_01EjFLnNhCiMR2PBNKcVvkT");
        assert!(id.len() <= 128);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        );
    }

    #[test]
    fn unclean_or_oversized_ids_hash_deterministically() {
        let dirty = subagent_doc_id("chat", "call/with:odd chars");
        assert!(
            dirty
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
            "{dirty}"
        );
        assert_eq!(dirty, subagent_doc_id("chat", "call/with:odd chars"));
        let long_chat = "c".repeat(100);
        let long = subagent_doc_id(&long_chat, &"t".repeat(64));
        assert!(long.len() <= 128, "{}", long.len());
        assert_ne!(
            subagent_doc_id("chat", "a:b"),
            subagent_doc_id("chat", "a:c")
        );
    }
}
