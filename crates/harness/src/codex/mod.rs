pub(crate) mod catalog;
mod normalize;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tokio::io::AsyncBufReadExt;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SteeringMode,
    UserInputAnswer, UserInputQuestion,
};

use crate::jsonrpc::{Incoming, RpcClient};
use crate::{Harness, HarnessError, RunControls};
use catalog::{REASONING_LEVELS, sandbox_mode, sandbox_policy_value, static_models, to_effort};
use normalize::{
    ChildRoute, Phase, delta_text, item_id, item_type, map_item, notification_thread_id,
    route_child_notification, turn_error_message, turn_id, usage_event, user_message_text,
};

fn resolve_codex_executable() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("CODEX_EXECUTABLE")
        && !p.is_empty()
    {
        return Some(PathBuf::from(p));
    }
    let exe = if cfg!(windows) { "codex.exe" } else { "codex" };
    let mut candidates: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|path| {
            std::env::split_paths(&path)
                .filter(|d| !d.as_os_str().is_empty())
                .map(|d| d.join(exe))
                .collect()
        })
        .unwrap_or_default();
    if let Some(shell_path) = crate::shell_env::login_shell_path() {
        candidates.extend(
            std::env::split_paths(shell_path)
                .filter(|d| !d.as_os_str().is_empty())
                .map(|d| d.join(exe)),
        );
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        candidates.push(home.join(".local").join("bin").join("codex"));
        candidates.push(home.join(".codex").join("bin").join("codex"));
        candidates.push(home.join(".npm-global").join("bin").join("codex"));
    }
    candidates.push(PathBuf::from("/opt/homebrew/bin/codex"));
    candidates.push(PathBuf::from("/usr/local/bin/codex"));
    candidates.extend(
        crate::node_version_manager_bins()
            .into_iter()
            .map(|d| d.join(exe)),
    );
    candidates.into_iter().find(|p| p.exists())
}

pub struct CodexHarness {
    executable: Option<PathBuf>,
    interrupt_grace: Duration,
    kill_grace: Duration,
}

impl Default for CodexHarness {
    fn default() -> Self {
        Self {
            executable: None,
            interrupt_grace: Duration::from_secs(2),
            kill_grace: Duration::from_secs(3),
        }
    }
}

impl CodexHarness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_executable(mut self, path: impl Into<PathBuf>) -> Self {
        self.executable = Some(path.into());
        self
    }

    pub fn with_graces(mut self, interrupt_grace: Duration, kill_grace: Duration) -> Self {
        self.interrupt_grace = interrupt_grace;
        self.kill_grace = kill_grace;
        self
    }

    fn resolve_executable(&self) -> Result<PathBuf, HarnessError> {
        if let Some(p) = &self.executable {
            return Ok(p.clone());
        }
        resolve_codex_executable().ok_or_else(|| {
            HarnessError::NotInstalled(
                "codex (searched PATH, the login shell's PATH, ~/.local/bin, \
                 ~/.codex/bin, ~/.npm-global/bin, /opt/homebrew/bin, /usr/local/bin, \
                 and fnm/nvm/volta/pnpm/bun install dirs; set CODEX_EXECUTABLE to \
                 override)"
                    .into(),
            )
        })
    }
}

#[async_trait]
impl Harness for CodexHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Codex
    }
    fn display_name(&self) -> &str {
        "Codex"
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::StepBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        REASONING_LEVELS
    }
    fn installed(&self) -> bool {
        self.executable.is_some() || resolve_codex_executable().is_some()
    }
    fn deterministic_turn_end(&self) -> bool {
        true
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        self.resolve_executable()?;
        Ok(static_models())
    }

    async fn run(
        &self,
        mut request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let exe = self.resolve_executable()?;
        request.sandbox = zeron_proto::SandboxLevel::DangerFullAccess;
        let mut cmd = Command::new(&exe);
        cmd.arg("app-server");
        crate::compose_child_path(&mut cmd, &exe);
        if !request.cwd.is_empty() {
            cmd.current_dir(&request.cwd);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                HarnessError::NotInstalled(exe.display().to_string())
            } else {
                HarnessError::Io(e)
            }
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HarnessError::Protocol("codex child has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Protocol("codex child has no stdout".into()))?;
        let stderr_tail = crate::StderrTail::default();
        if let Some(stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "zeron_harness::codex", "stderr: {line}");
                    tail.push(&line);
                }
            });
        }

        let (client, incoming) = RpcClient::new(stdin, stdout);
        let (event_tx, event_rx) = mpsc::channel::<Result<AgentEvent, HarnessError>>(256);
        tokio::spawn(run_session(Session {
            child,
            client,
            incoming,
            event_tx,
            controls,
            request,
            interrupt_grace: self.interrupt_grace,
            kill_grace: self.kill_grace,
            stderr_tail,
        }));

        Ok(futures::stream::unfold(event_rx, |mut rx| async move {
            rx.recv().await.map(|ev| (ev, rx))
        })
        .boxed())
    }
}

struct Session {
    child: Child,
    client: RpcClient,
    incoming: mpsc::Receiver<Incoming>,
    event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
    controls: RunControls,
    request: RunRequest,
    interrupt_grace: Duration,
    kill_grace: Duration,
    stderr_tail: crate::StderrTail,
}

#[derive(Default)]
struct TurnRouter {
    active: Option<String>,
    completed: VecDeque<String>,
}

impl TurnRouter {
    fn is_completed(&self, id: &str) -> bool {
        self.completed.iter().any(|c| c == id)
    }

    fn note_started(&mut self, id: String) {
        if id.is_empty() || self.is_completed(&id) {
            return;
        }
        if let Some(prev) = self.active.take()
            && prev != id
        {
            self.remember_completed(prev);
        }
        self.active = Some(id);
    }

    fn note_completed(&mut self, id: &str) {
        if id.is_empty() {
            return;
        }
        self.remember_completed(id.to_owned());
        if self.active.as_deref() == Some(id) {
            self.active = None;
        }
    }

    fn adopt_started(&mut self, id: String) {
        self.active = (!id.is_empty() && !self.is_completed(&id)).then_some(id);
    }

    fn remember_completed(&mut self, id: String) {
        self.completed.push_back(id);
        while self.completed.len() > 32 {
            self.completed.pop_front();
        }
    }
}

fn new_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn rotate(id: &mut String) -> (String, String) {
    let prev = std::mem::replace(id, new_message_id());
    (prev, id.clone())
}

async fn send(tx: &mpsc::Sender<Result<AgentEvent, HarnessError>>, ev: AgentEvent) -> bool {
    tx.send(Ok(ev)).await.is_ok()
}

async fn start_turn(client: &RpcClient, params: Value) -> Result<String, HarnessError> {
    let started = client.request("turn/start", params).await?;
    Ok(started["turn"]["id"].as_str().unwrap_or("").to_owned())
}

async fn run_session(session: Session) {
    let Session {
        mut child,
        client,
        mut incoming,
        event_tx,
        controls,
        request,
        interrupt_grace,
        kill_grace,
        stderr_tail,
    } = session;
    let RunControls {
        request_input,
        mut steering,
        interrupt,
    } = controls;
    let request_input = Arc::new(request_input);

    let approval_policy = "never";
    let effort = to_effort(request.reasoning);
    let service_tier = request
        .model_options
        .get("serviceTier")
        .and_then(Value::as_str)
        .filter(|t| *t != "default")
        .map(str::to_owned);

    let start_params = {
        let mut p = serde_json::Map::new();
        p.insert("cwd".into(), Value::String(request.cwd.clone()));
        p.insert("approvalPolicy".into(), approval_policy.into());
        p.insert("sandbox".into(), sandbox_mode(request.sandbox).into());
        if let Some(model) = &request.model {
            p.insert("model".into(), Value::String(model.clone()));
        }
        if let Some(tier) = &service_tier {
            p.insert("serviceTier".into(), Value::String(tier.clone()));
        }
        p
    };

    let setup = async {
        client
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "zeron-native",
                        "title": "Zeron",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": { "experimentalApi": true },
                }),
            )
            .await?;
        client.notify("initialized", None);

        let thread = if let Some(resume) = &request.resume {
            let mut p = start_params.clone();
            p.insert("threadId".into(), Value::String(resume.clone()));
            match client.request("thread/resume", Value::Object(p)).await {
                Ok(thread) => thread,
                Err(e) => {
                    tracing::debug!(
                        target: "zeron_harness::codex",
                        "thread/resume failed (starting fresh): {e}"
                    );
                    client
                        .request("thread/start", Value::Object(start_params.clone()))
                        .await?
                }
            }
        } else {
            client
                .request("thread/start", Value::Object(start_params.clone()))
                .await?
        };
        let thread_id = thread["thread"]["id"].as_str().unwrap_or("").to_owned();
        Ok::<String, HarnessError>(thread_id)
    };
    let thread_id = tokio::select! {
        res = setup => match res {
            Ok(thread_id) => thread_id,
            Err(e) => {
                let _ = event_tx
                    .send(Ok(AgentEvent::Done {
                        status: DoneStatus::Errored,
                        result: None,
                        error: Some(e.to_string()),
                        session_id: None,
                    }))
                    .await;
                shutdown_child(&mut child, kill_grace).await;
                return;
            }
        },
        _ = interrupt.cancelled() => {
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Interrupted,
                    result: None,
                    error: None,
                    session_id: None,
                }))
                .await;
            shutdown_child(&mut child, kill_grace).await;
            return;
        }
    };

    let turn_params = |text: &str| -> Value {
        let mut p = serde_json::Map::new();
        p.insert("threadId".into(), Value::String(thread_id.clone()));
        p.insert("input".into(), json!([{ "type": "text", "text": text }]));
        p.insert("approvalPolicy".into(), approval_policy.into());
        p.insert(
            "sandboxPolicy".into(),
            sandbox_policy_value(request.sandbox),
        );
        p.insert("summary".into(), "auto".into());
        if let Some(model) = &request.model {
            p.insert("model".into(), Value::String(model.clone()));
        }
        if let Some(effort) = effort {
            p.insert("effort".into(), effort.into());
        }
        if let Some(tier) = &service_tier {
            p.insert("serviceTier".into(), Value::String(tier.clone()));
        }
        Value::Object(p)
    };

    let mut assistant_message_id = new_message_id();
    if !send(
        &event_tx,
        AgentEvent::SessionStarted {
            harness: HarnessId::Codex,
            model: request.model.clone().unwrap_or_default(),
            tools: Vec::new(),
            cwd: request.cwd.clone(),
            session_id: thread_id.clone(),
            assistant_message_id: assistant_message_id.clone(),
        },
    )
    .await
    {
        shutdown_child(&mut child, kill_grace).await;
        return;
    }

    let mut router = TurnRouter::default();
    let mut children: HashMap<String, String> = HashMap::new();
    match start_turn(&client, turn_params(&request.prompt)).await {
        Ok(id) => router.adopt_started(id),
        Err(e) => {
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(e.to_string()),
                    session_id: Some(thread_id.clone()),
                }))
                .await;
            shutdown_child(&mut child, kill_grace).await;
            return;
        }
    }

    let mut streamed_text: HashSet<String> = HashSet::new();
    let mut pending_usage: Option<AgentEvent> = None;
    let mut queued_steers: VecDeque<String> = VecDeque::new();
    let mut steering_open = true;
    let mut interrupted = false;
    let mut interrupt_sent = false;
    let mut done_current = false;
    let mut done_after_interrupt = false;
    let mut escalation: Option<tokio::task::JoinHandle<()>> = None;

    'main: loop {
        tokio::select! {
            inc = incoming.recv() => match inc {
                Some(Incoming::Notification { method, params }) => {
                if let Some(nthread) = notification_thread_id(&method, &params)
                    && !nthread.is_empty()
                    && nthread != thread_id
                {
                    if method == "thread/started"
                        && params
                            .pointer("/thread/source/subAgent/thread_spawn")
                            .is_some()
                    {
                        children
                            .entry(nthread.clone())
                            .or_insert_with(|| nthread.clone());
                    }
                    match route_child_notification(&method) {
                        ChildRoute::Parent => {
                        }
                        ChildRoute::Consumed => continue,
                        ChildRoute::Subagent => {
                            if let Some(parent_call) = children.get(&nthread).cloned() {
                                let events: Vec<AgentEvent> = match method.as_str() {
                                    "turn/completed" => vec![AgentEvent::Done {
                                        status: if turn_error_message(&params).is_some()
                                            || params
                                                .pointer("/turn/status")
                                                .and_then(Value::as_str)
                                                == Some("failed")
                                        {
                                            DoneStatus::Errored
                                        } else {
                                            DoneStatus::Completed
                                        },
                                        result: None,
                                        error: None,
                                        session_id: Some(nthread.clone()),
                                    }],
                                    "turn/failed" => vec![AgentEvent::Done {
                                        status: DoneStatus::Errored,
                                        result: None,
                                        error: turn_error_message(&params),
                                        session_id: Some(nthread.clone()),
                                    }],
                                    "turn/aborted" => vec![AgentEvent::Done {
                                        status: DoneStatus::Interrupted,
                                        result: None,
                                        error: None,
                                        session_id: Some(nthread.clone()),
                                    }],
                                    "item/agentMessage/delta" => delta_text(&params)
                                        .map(|text| AgentEvent::TextDelta { text })
                                        .into_iter()
                                        .collect(),
                                    "item/reasoning/textDelta"
                                    | "item/reasoning/summaryTextDelta" => delta_text(&params)
                                        .map(|text| AgentEvent::ReasoningDelta { text })
                                        .into_iter()
                                        .collect(),
                                    "item/started" | "item/completed" => {
                                        let phase = if method == "item/started" {
                                            Phase::Started
                                        } else {
                                            Phase::Completed
                                        };
                                        let item =
                                            params.get("item").cloned().unwrap_or(Value::Null);
                                        if phase == Phase::Completed
                                            && matches!(
                                                item_type(&item),
                                                "agentMessage" | "agent_message"
                                            )
                                        {
                                            vec![AgentEvent::TextDelta {
                                                text: "\n\n".into(),
                                            }]
                                        } else if matches!(
                                            item_type(&item),
                                            "userMessage" | "user_message"
                                        ) {
                                            if phase == Phase::Completed {
                                                user_message_text(&item)
                                                    .map(|text| AgentEvent::UserMessage { text })
                                                    .into_iter()
                                                    .collect()
                                            } else {
                                                Vec::new()
                                            }
                                        } else {
                                            map_item(phase, &item)
                                        }
                                    }
                                    "error" => vec![AgentEvent::Error {
                                        message: params
                                            .pointer("/error/message")
                                            .and_then(Value::as_str)
                                            .or_else(|| {
                                                params.get("message").and_then(Value::as_str)
                                            })
                                            .unwrap_or("Codex subagent error")
                                            .to_owned(),
                                    }],
                                    "thread/closed" => vec![AgentEvent::Done {
                                        status: DoneStatus::Completed,
                                        result: None,
                                        error: None,
                                        session_id: Some(nthread.clone()),
                                    }],
                                    _ => Vec::new(),
                                };
                                for ev in events {
                                    let wrapped = AgentEvent::Subagent {
                                        parent_tool_use_id: parent_call.clone(),
                                        event: Box::new(ev),
                                    };
                                    if !send(&event_tx, wrapped).await {
                                        break 'main;
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }
                match method.as_str() {
                    "turn/started" => router.note_started(turn_id(&params)),

                    "item/agentMessage/delta" => {
                        streamed_text.insert(item_id(&params));
                        if let Some(text) = delta_text(&params)
                            && !send(&event_tx, AgentEvent::TextDelta { text }).await
                        {
                            break 'main;
                        }
                    }

                    "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
                        if let Some(text) = delta_text(&params)
                            && !send(&event_tx, AgentEvent::ReasoningDelta { text }).await
                        {
                            break 'main;
                        }
                    }

                    "item/started" | "item/completed" => {
                        let phase = if method == "item/started" {
                            Phase::Started
                        } else {
                            Phase::Completed
                        };
                        let item = params.get("item").cloned().unwrap_or(Value::Null);
                        if matches!(
                            item_type(&item),
                            "subAgentActivity" | "sub_agent_activity"
                        ) {
                            let child = item
                                .get("agentThreadId")
                                .and_then(Value::as_str)
                                .unwrap_or("");
                            let path = item
                                .get("agentPath")
                                .and_then(Value::as_str)
                                .unwrap_or("");
                            let call = item.get("id").and_then(Value::as_str).unwrap_or("");
                            if !child.is_empty()
                                && child != thread_id
                                && path != "/root"
                                && path != "/"
                                && !call.is_empty()
                            {
                                children.insert(child.to_owned(), call.to_owned());
                            } else if child == thread_id || path == "/root" || path == "/" {
                                continue;
                            }
                        }
                        if matches!(item_type(&item), "agentMessage" | "agent_message") {
                            if phase == Phase::Completed {
                                let id = item.get("id").and_then(Value::as_str).unwrap_or("");
                                let text = item.get("text").and_then(Value::as_str).unwrap_or("");
                                if !streamed_text.contains(id)
                                    && !text.is_empty()
                                    && !send(&event_tx, AgentEvent::TextDelta { text: text.into() }).await
                                {
                                    break 'main;
                                }
                                if !send(
                                    &event_tx,
                                    AgentEvent::TextDelta {
                                        text: "\n\n".into(),
                                    },
                                )
                                .await
                                {
                                    break 'main;
                                }
                                let (prev, _next) = rotate(&mut assistant_message_id);
                                if !send(
                                    &event_tx,
                                    AgentEvent::AssistantMessageCompleted {
                                        assistant_message_id: prev,
                                    },
                                )
                                .await
                                {
                                    break 'main;
                                }
                            }
                        } else {
                            for ev in map_item(phase, &item) {
                                if !send(&event_tx, ev).await {
                                    break 'main;
                                }
                            }
                        }
                    }

                    "thread/tokenUsage/updated" => {
                        if let Some(usage) = usage_event(&params) {
                            pending_usage = Some(usage);
                        }
                    }

                    "turn/completed" => {
                        let id = turn_id(&params);
                        router.note_completed(&id);
                        streamed_text.clear();
                        if let Some(usage) = pending_usage.take()
                            && !send(&event_tx, usage).await
                        {
                            break 'main;
                        }
                        let error = turn_error_message(&params).or_else(|| {
                            (params
                                .pointer("/turn/status")
                                .and_then(Value::as_str)
                                == Some("failed"))
                            .then(|| "Codex turn failed".to_owned())
                        });
                        let status = if interrupted {
                            DoneStatus::Interrupted
                        } else if error.is_some() {
                            DoneStatus::Errored
                        } else {
                            DoneStatus::Completed
                        };
                        done_current = true;
                        if !send(
                            &event_tx,
                            AgentEvent::Done {
                                status,
                                result: None,
                                error,
                                session_id: Some(thread_id.clone()),
                            },
                        )
                        .await
                        {
                            break 'main;
                        }
                        if interrupted {
                            done_after_interrupt = true;
                            break 'main;
                        }
                        if let Some(text) = queued_steers.pop_front() {
                            if !steer_as_new_turn(
                                &client,
                                turn_params(&text),
                                &mut router,
                                &event_tx,
                                &mut assistant_message_id,
                                &mut done_current,
                            )
                            .await
                            {
                                break 'main;
                            }
                        } else if !steering_open {
                            break 'main;
                        }
                    }

                    "turn/failed" => {
                        router.note_completed(&turn_id(&params));
                        if let Some(usage) = pending_usage.take()
                            && !send(&event_tx, usage).await
                        {
                            break 'main;
                        }
                        done_current = true;
                        if interrupted {
                            done_after_interrupt = true;
                        }
                        let _ = send(
                            &event_tx,
                            AgentEvent::Done {
                                status: if interrupted {
                                    DoneStatus::Interrupted
                                } else {
                                    DoneStatus::Errored
                                },
                                result: None,
                                error: Some(
                                    turn_error_message(&params)
                                        .unwrap_or_else(|| "Codex turn failed".into()),
                                ),
                                session_id: Some(thread_id.clone()),
                            },
                        )
                        .await;
                        break 'main;
                    }

                    "turn/aborted" => {
                        router.note_completed(&turn_id(&params));
                        done_current = true;
                        if interrupted {
                            done_after_interrupt = true;
                        }
                        let _ = send(
                            &event_tx,
                            AgentEvent::Done {
                                status: DoneStatus::Interrupted,
                                result: None,
                                error: None,
                                session_id: Some(thread_id.clone()),
                            },
                        )
                        .await;
                        break 'main;
                    }

                    "error" => {
                        let message = params
                            .pointer("/error/message")
                            .and_then(Value::as_str)
                            .or_else(|| params.get("message").and_then(Value::as_str))
                            .unwrap_or("Codex error")
                            .to_owned();
                        if !send(&event_tx, AgentEvent::Error { message }).await {
                            break 'main;
                        }
                    }

                    _ => {}
                }
                }

                Some(Incoming::Request { id, method, params }) => {
                    handle_server_request(
                        &client,
                        id,
                        &method,
                        &params,
                        request.auto_approve,
                        &request_input,
                    );
                }

                Some(Incoming::Eof) | None => break 'main,
            },

            steer = steering.recv(), if steering_open && !interrupted => match steer {
                Some(msg) => {
                    let text = msg.prompt;
                    if let Some(expected) = router.active.clone() {
                        let steer_params = json!({
                            "threadId": thread_id,
                            "expectedTurnId": expected,
                            "input": [{ "type": "text", "text": text }],
                        });
                        match client.request("turn/steer", steer_params).await {
                            Ok(_) => {
                                let (prev, next) = rotate(&mut assistant_message_id);
                                if !send(
                                    &event_tx,
                                    AgentEvent::Steered {
                                        assistant_message_id: Some(prev),
                                        next_assistant_message_id: Some(next),
                                    },
                                )
                                .await
                                {
                                    break 'main;
                                }
                            }
                            Err(e) => {
                                tracing::debug!(
                                    target: "zeron_harness::codex",
                                    "turn/steer rejected (queued as next turn): {e}"
                                );
                                if router.active.as_deref() == Some(expected.as_str())
                                    && !router.is_completed(&expected)
                                {
                                    queued_steers.push_back(text);
                                } else if !steer_as_new_turn(
                                    &client,
                                    turn_params(&text),
                                    &mut router,
                                    &event_tx,
                                    &mut assistant_message_id,
                                    &mut done_current,
                                )
                                .await
                                {
                                    break 'main;
                                }
                            }
                        }
                    } else if !steer_as_new_turn(
                        &client,
                        turn_params(&text),
                        &mut router,
                        &event_tx,
                        &mut assistant_message_id,
                        &mut done_current,
                    )
                    .await
                    {
                        break 'main;
                    }
                }
                None => {
                    steering_open = false;
                    if router.active.is_none() && queued_steers.is_empty() {
                        break 'main;
                    }
                }
            },

            _ = interrupt.cancelled(), if !interrupt_sent => {
                interrupt_sent = true;
                interrupted = true;
                if let Some(turn) = router.active.clone() {
                    let client = client.clone();
                    let thread = thread_id.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client
                            .request("turn/interrupt", json!({ "threadId": thread, "turnId": turn }))
                            .await
                        {
                            tracing::debug!(
                                target: "zeron_harness::codex",
                                "turn/interrupt failed (escalation will reap): {e}"
                            );
                        }
                    });
                    if let Some(pid) = child.id() {
                        escalation = Some(tokio::spawn(async move {
                            tokio::time::sleep(interrupt_grace).await;
                            send_signal(pid, Signal::Term);
                            tokio::time::sleep(kill_grace).await;
                            send_signal(pid, Signal::Kill);
                        }));
                    }
                } else {
                    break 'main;
                }
            },

            _ = event_tx.closed() => break 'main,
        }
    }

    if !event_tx.is_closed() {
        if interrupted && !done_after_interrupt {
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Interrupted,
                    result: None,
                    error: None,
                    session_id: Some(thread_id.clone()),
                }))
                .await;
        } else if !interrupted && !done_current {
            let status = child.try_wait().ok().flatten();
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(crate::crash_message(
                        "codex app-server",
                        status,
                        &stderr_tail,
                    )),
                    session_id: Some(thread_id.clone()),
                }))
                .await;
        }
    }

    shutdown_child(&mut child, kill_grace).await;
    if let Some(handle) = escalation {
        handle.abort();
    }
}

async fn steer_as_new_turn(
    client: &RpcClient,
    params: Value,
    router: &mut TurnRouter,
    event_tx: &mpsc::Sender<Result<AgentEvent, HarnessError>>,
    assistant_message_id: &mut String,
    done_current: &mut bool,
) -> bool {
    match start_turn(client, params).await {
        Ok(id) => {
            router.adopt_started(id);
            *done_current = false;
            let (prev, next) = rotate(assistant_message_id);
            send(
                event_tx,
                AgentEvent::Steered {
                    assistant_message_id: Some(prev),
                    next_assistant_message_id: Some(next),
                },
            )
            .await
        }
        Err(e) => {
            let _ = send(
                event_tx,
                AgentEvent::Error {
                    message: format!("Steering failed: {e}"),
                },
            )
            .await;
            false
        }
    }
}

type RequestInputFn = Box<
    dyn Fn(Vec<UserInputQuestion>) -> tokio::sync::oneshot::Receiver<Vec<UserInputAnswer>>
        + Send
        + Sync,
>;

fn handle_server_request(
    client: &RpcClient,
    id: Value,
    method: &str,
    params: &Value,
    auto_approve: bool,
    request_input: &Arc<RequestInputFn>,
) {
    if method == "item/tool/requestUserInput" {
        let questions = user_input_questions(params);
        if questions.is_empty() {
            client.respond(&id, json!({ "answers": {} }));
            return;
        }
        let client = client.clone();
        let request_input = Arc::clone(request_input);
        tokio::spawn(async move {
            let asked: Vec<UserInputQuestion> = questions.iter().map(|(_, q)| q.clone()).collect();
            let answers = (request_input)(asked).await.unwrap_or_default();
            let mut by_id = serde_json::Map::new();
            for (wire_id, q) in &questions {
                let labels: Vec<Value> = answers
                    .iter()
                    .find(|a| a.question_id == q.id)
                    .map(|a| a.labels.iter().cloned().map(Value::String).collect())
                    .unwrap_or_default();
                by_id.insert(wire_id.clone(), json!({ "answers": labels }));
            }
            client.respond(&id, json!({ "answers": by_id }));
        });
        return;
    }
    let is_approval = matches!(
        method,
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
    );
    if !is_approval {
        tracing::debug!(
            target: "zeron_harness::codex",
            "unhandled server request: {method}"
        );
        client.respond_error(&id, -32601, &format!("unsupported method: {method}"));
        return;
    }
    if auto_approve {
        client.respond(&id, json!({ "decision": "accept" }));
        return;
    }

    let question = approval_question(method, params);
    let client = client.clone();
    let request_input = Arc::clone(request_input);
    tokio::spawn(async move {
        let answers = (request_input)(vec![question.clone()])
            .await
            .unwrap_or_default();
        let accept = answers.iter().any(|a| {
            a.question_id == question.id && a.labels.iter().any(|l| l.eq_ignore_ascii_case("yes"))
        });
        client.respond(
            &id,
            json!({ "decision": if accept { "accept" } else { "decline" } }),
        );
    });
}

fn user_input_questions(params: &Value) -> Vec<(String, UserInputQuestion)> {
    params
        .get("questions")
        .and_then(Value::as_array)
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(ix, q)| {
            let field = |keys: [&str; 3]| {
                keys.iter()
                    .find_map(|k| q.get(*k).and_then(Value::as_str))
                    .unwrap_or("")
                    .to_owned()
            };
            let wire_id = {
                let id = field(["id", "questionId", "question_id"]);
                if id.is_empty() { format!("q{ix}") } else { id }
            };
            let question = UserInputQuestion {
                id: new_message_id(),
                header: {
                    let h = field(["header", "title", "label"]);
                    if h.is_empty() {
                        "Codex question".into()
                    } else {
                        h
                    }
                },
                question: field(["question", "prompt", "text"]),
                options: q
                    .get("options")
                    .and_then(Value::as_array)
                    .map(|a| a.as_slice())
                    .unwrap_or_default()
                    .iter()
                    .map(|op| match op {
                        Value::String(s) => s.clone(),
                        other => other
                            .get("label")
                            .or_else(|| other.get("value"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .into(),
                    })
                    .collect(),
                multi_select: ["multiSelect", "multi_select"]
                    .iter()
                    .find_map(|k| q.get(*k).and_then(Value::as_bool))
                    .unwrap_or(false),
            };
            (wire_id, question)
        })
        .collect()
}

fn approval_question(method: &str, params: &Value) -> UserInputQuestion {
    let (header, question) = if method.contains("commandExecution") {
        let command = match params.get("command") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Array(parts)) => parts
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
            _ => String::new(),
        };
        (
            "Approve command".to_owned(),
            if command.is_empty() {
                "Codex wants to run a command. Allow it?".to_owned()
            } else {
                format!("Codex wants to run `{command}`. Allow it?")
            },
        )
    } else {
        let paths: Vec<&str> = params
            .get("changes")
            .and_then(Value::as_array)
            .map(|a| a.as_slice())
            .unwrap_or_default()
            .iter()
            .filter_map(|c| c.get("path").and_then(Value::as_str))
            .collect();
        (
            "Approve file change".to_owned(),
            if paths.is_empty() {
                "Codex wants to modify files. Allow it?".to_owned()
            } else {
                format!("Codex wants to modify {}. Allow it?", paths.join(", "))
            },
        )
    };
    UserInputQuestion {
        id: new_message_id(),
        header,
        question,
        options: vec!["Yes".into(), "No".into()],
        multi_select: false,
    }
}

use crate::{Signal, send_signal, shutdown_child};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn approval_questions_are_yes_no() {
        let q = approval_question(
            "item/commandExecution/requestApproval",
            &json!({"itemId": "c1", "command": "rm -rf /tmp/x"}),
        );
        assert_eq!(q.header, "Approve command");
        assert!(q.question.contains("rm -rf /tmp/x"));
        assert_eq!(q.options, vec!["Yes".to_string(), "No".to_string()]);
        assert!(!q.multi_select);

        let q = approval_question(
            "item/fileChange/requestApproval",
            &json!({"changes": [{"path": "/a.rs"}, {"path": "/b.rs"}]}),
        );
        assert_eq!(q.header, "Approve file change");
        assert!(q.question.contains("/a.rs, /b.rs"));

        let q = approval_question(
            "item/commandExecution/requestApproval",
            &json!({"command": ["git", "push", "--force"]}),
        );
        assert!(q.question.contains("git push --force"));
    }

    #[test]
    fn turn_router_never_revives_completed_turns() {
        let mut r = TurnRouter::default();
        r.note_completed("t-1");
        r.adopt_started("t-1".into());
        assert_eq!(r.active, None);
        r.note_started("t-1".into());
        assert_eq!(r.active, None);

        r.note_started("t-2".into());
        assert_eq!(r.active.as_deref(), Some("t-2"));
        r.note_started("t-3".into());
        assert_eq!(r.active.as_deref(), Some("t-3"));
        assert!(r.is_completed("t-2"));
    }
}
