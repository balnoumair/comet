use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ModelOption, ModelOptionChoice, ReasoningLevel,
    RunRequest, SteeringMode, TodoItem, ToolCall,
};

use crate::{Harness, HarnessError, RunControls, Signal, send_signal, shutdown_child};

const CURSOR_SDK_PIN: &str = "@cursor/sdk@1.0.28";
const SHIM_NAME: &str = "zeron-cursor-shim.mjs";
const SHIM_SOURCE: &str = include_str!("shim.mjs");

fn cursor_cli_paths() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        dirs.push(home.join(".local").join("bin").join("cursor-agent"));
        dirs.push(home.join(".cursor").join("bin").join("cursor-agent"));
    }
    dirs.push(PathBuf::from("/opt/homebrew/bin/cursor-agent"));
    dirs.push(PathBuf::from("/usr/local/bin/cursor-agent"));
    dirs
}

pub struct CursorHarness {
    executable: Option<PathBuf>,
    interrupt_grace: Duration,
    kill_grace: Duration,
    models_cache: tokio::sync::OnceCell<Vec<Model>>,
}

impl Default for CursorHarness {
    fn default() -> Self {
        Self {
            executable: None,
            interrupt_grace: Duration::from_secs(2),
            kill_grace: Duration::from_secs(3),
            models_cache: tokio::sync::OnceCell::new(),
        }
    }
}

impl CursorHarness {
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

    async fn discover_models(&self) -> Result<Vec<Model>, HarnessError> {
        let (exe, args) = self.resolve_shim().await?;
        let mut cmd = Command::new(&exe);
        cmd.args(&args);
        crate::compose_child_path(&mut cmd, &exe);
        cmd.arg("models")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let run = async {
            let output = cmd
                .output()
                .await
                .map_err(|e| HarnessError::Protocol(format!("cursor models probe: {e}")))?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let items = stdout
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
                .find(|v| v.get("ev").and_then(Value::as_str) == Some("models"))
                .and_then(|v| v.get("items").cloned())
                .ok_or_else(|| {
                    HarnessError::Protocol("cursor models probe returned no catalog".into())
                })?;
            Ok::<_, HarnessError>(map_model_items(&items))
        };
        tokio::time::timeout(Duration::from_secs(15), run)
            .await
            .map_err(|_| HarnessError::Protocol("cursor models probe timed out".into()))?
    }

    async fn discover_cli_models(&self) -> Result<Vec<Model>, HarnessError> {
        let exe = crate::acp::find_on_paths("cursor-agent", cursor_cli_paths())
            .ok_or_else(|| HarnessError::NotInstalled("cursor-agent".into()))?;
        let mut cmd = Command::new(exe);
        cmd.arg("models")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(15), cmd.output())
            .await
            .map_err(|_| HarnessError::Protocol("cursor CLI models probe timed out".into()))?
            .map_err(|e| HarnessError::Protocol(format!("cursor CLI models probe: {e}")))?;
        let models = parse_cli_models(&String::from_utf8_lossy(&output.stdout));
        if !output.status.success() && models.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(HarnessError::Protocol(format!(
                "cursor CLI models probe failed: {}",
                stderr.trim()
            )));
        }
        if models.is_empty() {
            return Err(HarnessError::Protocol(
                "cursor CLI models probe returned no catalog".into(),
            ));
        }
        Ok(models)
    }

    pub async fn resolve_shim(&self) -> Result<(PathBuf, Vec<String>), HarnessError> {
        if let Some(p) = &self.executable {
            return Ok((p.clone(), Vec::new()));
        }
        if let Some(p) = std::env::var_os("CURSOR_SDK_SHIM_EXECUTABLE")
            && !p.is_empty()
        {
            return Ok((PathBuf::from(p), Vec::new()));
        }
        let pin = crate::adapter_install::NpmPin::parse(CURSOR_SDK_PIN);
        let shim =
            crate::adapter_install::ensure_installed_shim(pin, "Cursor", SHIM_NAME, SHIM_SOURCE)
                .await?;
        crate::adapter_install::launch_for_entry(&shim)
    }
}

pub async fn login_command(store_path: &std::path::Path) -> Result<Command, HarnessError> {
    let (exe, args) = CursorHarness::default().resolve_shim().await?;
    let mut cmd = Command::new(&exe);
    cmd.args(&args);
    crate::compose_child_path(&mut cmd, &exe);
    cmd.arg("login").arg(store_path);
    Ok(cmd)
}

#[async_trait]
impl Harness for CursorHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Cursor
    }
    fn display_name(&self) -> &str {
        "Cursor"
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::TurnBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[]
    }
    fn installed(&self) -> bool {
        self.executable.is_some()
            || crate::acp::find_on_paths("cursor-agent", cursor_cli_paths()).is_some()
    }
    fn deterministic_turn_end(&self) -> bool {
        true
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        if let Some(models) = self.models_cache.get() {
            return Ok(models.clone());
        }

        if self.executable.is_none() {
            match self.discover_cli_models().await {
                Ok(models) if !models.is_empty() => {
                    let _ = self.models_cache.set(models.clone());
                    return Ok(models);
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::debug!(
                        target: "zeron_harness::cursor",
                        error = %err,
                        "cursor CLI model discovery unavailable; trying SDK"
                    );
                }
            }
        }
        match self.discover_models().await {
            Ok(models) if !models.is_empty() => {
                let _ = self.models_cache.set(models.clone());
                Ok(models)
            }
            Ok(_) => Ok(static_models()),
            Err(err) => {
                tracing::debug!(
                    target: "zeron_harness::cursor",
                    error = %err,
                    "cursor SDK model discovery unavailable; using fallback catalog"
                );
                Ok(static_models())
            }
        }
    }

    async fn run(
        &self,
        request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let (exe, args) = self.resolve_shim().await?;
        let mut cmd = Command::new(&exe);
        cmd.args(&args);
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
            .ok_or_else(|| HarnessError::Protocol("cursor shim has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Protocol("cursor shim has no stdout".into()))?;
        let stderr_tail = crate::StderrTail::default();
        if let Some(stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "zeron_harness::cursor", "stderr: {line}");
                    tail.push(&line);
                }
            });
        }

        let (stdin_tx, stdin_rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(stdin_writer(stdin, stdin_rx));
        let first = json!({
            "op": "run",
            "prompt": request.prompt,
            "cwd": request.cwd,
            "model": request.model,
            "modelOptions": request.model_options,
            "resume": request.resume,
        });
        let _ = stdin_tx.send(first.to_string());

        let (event_tx, event_rx) = mpsc::channel::<Result<AgentEvent, HarnessError>>(256);
        tokio::spawn(run_session(Session {
            child,
            stdout_lines: BufReader::new(stdout).lines(),
            stdin_tx,
            event_tx,
            controls,
            request_cwd: request.cwd,
            request_model: request.model.unwrap_or_default(),
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

fn static_models() -> Vec<Model> {
    vec![
        Model {
            id: "auto".into(),
            label: "Auto".into(),
            description: Some("Cursor picks the model per request".into()),
            reasoning_levels: Vec::new(),
            options: Vec::new(),
        },
        Model {
            id: "composer-2.5".into(),
            label: "Composer 2.5".into(),
            description: Some("Cursor's own fast coding model".into()),
            reasoning_levels: Vec::new(),
            options: Vec::new(),
        },
    ]
}

fn parse_cli_models(output: &str) -> Vec<Model> {
    output
        .lines()
        .filter_map(|line| {
            let (id, label) = line.trim().split_once(" - ")?;
            let id = id.trim();
            let label = label.trim();
            if id.is_empty() || label.is_empty() || id.chars().any(char::is_whitespace) {
                return None;
            }
            Some(Model {
                id: id.into(),
                label: label.into(),
                description: None,
                reasoning_levels: Vec::new(),
                options: Vec::new(),
            })
        })
        .collect()
}

fn map_model_items(items: &Value) -> Vec<Model> {
    let str_of = |v: &Value, key: &str| -> Option<String> {
        v.get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    items
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .filter_map(|item| {
            let id = str_of(item, "id")?;
            if id == "default" {
                return None;
            }
            let label = str_of(item, "displayName").unwrap_or_else(|| id.clone());
            let default_variant = item
                .get("variants")
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
                .iter()
                .find(|v| v.get("isDefault").and_then(Value::as_bool) == Some(true))
                .and_then(|v| v.get("params").and_then(Value::as_array).cloned())
                .unwrap_or_default();
            let options: Vec<ModelOption> = item
                .get("parameters")
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
                .iter()
                .filter_map(|p| {
                    let pid = str_of(p, "id")?;
                    let choices: Vec<ModelOptionChoice> = p
                        .get("values")
                        .and_then(Value::as_array)
                        .map(|a| a.as_slice())
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|c| {
                            let cid = str_of(c, "value")?;
                            Some(ModelOptionChoice {
                                label: str_of(c, "displayName").unwrap_or_else(|| cid.clone()),
                                id: cid,
                            })
                        })
                        .collect();
                    if choices.is_empty() {
                        return None;
                    }
                    let default_choice = default_variant
                        .iter()
                        .find(|dv| dv.get("id").and_then(Value::as_str) == Some(pid.as_str()))
                        .and_then(|dv| str_of(dv, "value"))
                        .unwrap_or_else(|| choices[0].id.clone());
                    Some(ModelOption {
                        label: str_of(p, "displayName").unwrap_or_else(|| pid.clone()),
                        id: pid,
                        choices,
                        default_choice,
                    })
                })
                .collect();
            Some(Model {
                id,
                label,
                description: str_of(item, "description"),
                reasoning_levels: Vec::new(),
                options,
            })
        })
        .collect()
}

async fn stdin_writer(mut stdin: ChildStdin, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = rx.recv().await {
        let write = async {
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        };
        if let Err(e) = write.await {
            tracing::debug!(target: "zeron_harness::cursor", "stdin write failed (tolerated): {e}");
            return;
        }
    }
    let _ = stdin.shutdown().await;
}

struct Session {
    child: Child,
    stdout_lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    stdin_tx: mpsc::UnboundedSender<String>,
    event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
    controls: RunControls,
    request_cwd: String,
    request_model: String,
    interrupt_grace: Duration,
    kill_grace: Duration,
    stderr_tail: crate::StderrTail,
}

fn new_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

async fn run_session(session: Session) {
    let Session {
        mut child,
        mut stdout_lines,
        stdin_tx,
        event_tx,
        controls,
        request_cwd,
        request_model,
        interrupt_grace,
        kill_grace,
        stderr_tail,
    } = session;
    let RunControls {
        request_input: _request_input,
        mut steering,
        interrupt,
    } = controls;

    let mut assistant_message_id = new_message_id();
    let mut session_id: Option<String> = None;
    let mut steering_open = true;
    let mut interrupted = false;
    let mut interrupt_sent = false;
    let mut any_done = false;
    let mut done_after_interrupt = false;
    let mut parked = false;
    let mut queued_steers: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut escalation: Option<tokio::task::JoinHandle<()>> = None;

    let send = |ev: AgentEvent| {
        let tx = event_tx.clone();
        async move { tx.send(Ok(ev)).await.is_ok() }
    };

    'main: loop {
        tokio::select! {
            line = stdout_lines.next_line() => match line {
                Ok(Some(line)) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    let Ok(frame) = serde_json::from_str::<Value>(line) else {
                        tracing::debug!(target: "zeron_harness::cursor", "unparseable shim frame (skipped)");
                        continue;
                    };
                    match frame.get("ev").and_then(Value::as_str).unwrap_or("") {
                        "ready" => {
                            let agent_id = frame
                                .get("agentId")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned();
                            session_id = Some(agent_id.clone());
                            let model = frame
                                .get("model")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                                .unwrap_or_else(|| request_model.clone());
                            if !send(AgentEvent::SessionStarted {
                                harness: HarnessId::Cursor,
                                model,
                                tools: Vec::new(),
                                cwd: request_cwd.clone(),
                                session_id: agent_id,
                                assistant_message_id: assistant_message_id.clone(),
                            })
                            .await
                            {
                                break 'main;
                            }
                        }
                        _ => {
                            for ev in map_shim_frame(&frame, interrupted) {
                                let is_done = matches!(ev, AgentEvent::Done { .. });
                                let ev = if let AgentEvent::Done { status, result, error, .. } = ev {
                                    AgentEvent::Done { status, result, error, session_id: session_id.clone() }
                                } else {
                                    ev
                                };
                                if !send(ev).await {
                                    break 'main;
                                }
                                if is_done {
                                    any_done = true;
                                    if interrupted {
                                        done_after_interrupt = true;
                                        break 'main;
                                    }
                                    if let Some(text) = queued_steers.pop_front() {
                                        let prev = std::mem::replace(
                                            &mut assistant_message_id,
                                            new_message_id(),
                                        );
                                        if !send(AgentEvent::Steered {
                                            assistant_message_id: Some(prev),
                                            next_assistant_message_id: Some(
                                                assistant_message_id.clone(),
                                            ),
                                        })
                                        .await
                                        {
                                            break 'main;
                                        }
                                        let _ = stdin_tx.send(
                                            json!({ "op": "user", "prompt": text }).to_string(),
                                        );
                                    } else if !steering_open {
                                        break 'main;
                                    } else {
                                        parked = true;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(None) => break 'main,
                Err(e) => {
                    let _ = event_tx.send(Err(HarnessError::Io(e))).await;
                    break 'main;
                }
            },

            steer = steering.recv(), if steering_open && !interrupted => match steer {
                Some(msg) => {
                    if parked {
                        parked = false;
                        let prev = std::mem::replace(&mut assistant_message_id, new_message_id());
                        if !send(AgentEvent::Steered {
                            assistant_message_id: Some(prev),
                            next_assistant_message_id: Some(assistant_message_id.clone()),
                        })
                        .await
                        {
                            break 'main;
                        }
                        let _ = stdin_tx
                            .send(json!({ "op": "user", "prompt": msg.prompt }).to_string());
                    } else {
                        queued_steers.push_back(msg.prompt);
                    }
                }
                None => {
                    steering_open = false;
                    if parked && queued_steers.is_empty() {
                        break 'main;
                    }
                }
            },

            _ = interrupt.cancelled(), if !interrupt_sent => {
                interrupt_sent = true;
                interrupted = true;
                let _ = stdin_tx.send(json!({ "op": "interrupt" }).to_string());
                if let Some(pid) = child.id() {
                    escalation = Some(tokio::spawn(async move {
                        tokio::time::sleep(interrupt_grace).await;
                        send_signal(pid, Signal::Term);
                        tokio::time::sleep(kill_grace).await;
                        send_signal(pid, Signal::Kill);
                    }));
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
                    session_id: session_id.clone(),
                }))
                .await;
        } else if !interrupted && !any_done {
            let status = tokio::time::timeout(Duration::from_millis(500), child.wait())
                .await
                .ok()
                .and_then(Result::ok);
            tokio::task::yield_now().await;
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(crate::crash_message("cursor shim", status, &stderr_tail)),
                    session_id: session_id.clone(),
                }))
                .await;
        }
    }

    shutdown_child(&mut child, kill_grace).await;
    if let Some(handle) = escalation {
        handle.abort();
    }
}

fn decode_tool(name: &str, args: &Value) -> ToolCall {
    let s = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| args.get(*k).and_then(Value::as_str))
            .unwrap_or("")
            .to_owned()
    };
    let opt = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| args.get(*k).and_then(Value::as_str))
            .map(str::to_owned)
    };
    match name {
        "shell" => ToolCall::Exec {
            command: s(&["command"]),
        },
        "read" => ToolCall::ReadFile {
            path: s(&["path", "filePath", "file_path"]),
        },
        "write" => ToolCall::WriteFile {
            path: s(&["path", "filePath", "file_path"]),
            content: None,
        },
        "edit" => ToolCall::EditFile {
            path: s(&["path", "filePath", "file_path"]),
            old_string: None,
            new_string: None,
        },
        "delete" => ToolCall::Unknown {
            name: format!("Delete: {}", s(&["path", "filePath", "file_path"])),
            input: (!args.is_null()).then(|| args.clone()),
        },
        "grep" => ToolCall::Search {
            pattern: s(&["pattern", "query"]),
            path: opt(&["path", "directory"]),
        },
        "glob" => ToolCall::Glob {
            pattern: s(&["pattern", "globPattern"]),
        },
        "webSearch" => ToolCall::WebSearch {
            query: s(&["query", "search"]),
        },
        "webFetch" => ToolCall::WebFetch {
            url: s(&["url"]),
            prompt: None,
        },
        "updateTodos" => ToolCall::Todo {
            items: args
                .get("todos")
                .or_else(|| args.get("items"))
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
                .iter()
                .map(|t| TodoItem {
                    text: t
                        .get("content")
                        .or_else(|| t.get("text"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    done: t.get("status").and_then(Value::as_str) == Some("completed")
                        || t.get("completed").and_then(Value::as_bool) == Some(true),
                })
                .collect(),
        },
        "mcp" => ToolCall::Mcp {
            server: s(&["server", "serverName"]),
            tool: s(&["tool", "toolName", "name"]),
            input: args.get("args").or(args.get("input")).cloned(),
        },
        "task" => ToolCall::Unknown {
            name: {
                let description = s(&["description", "prompt"]);
                if description.is_empty() {
                    "Agent".to_owned()
                } else {
                    format!("Agent: {description}")
                }
            },
            input: (!args.is_null()).then(|| args.clone()),
        },
        other => ToolCall::Unknown {
            name: other.to_owned(),
            input: (!args.is_null()).then(|| args.clone()),
        },
    }
}

fn map_shim_frame(frame: &Value, interrupted: bool) -> Vec<AgentEvent> {
    let text = || {
        frame
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    let parent = frame
        .get("parent")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let tag = |ev: AgentEvent| -> AgentEvent {
        match &parent {
            Some(parent) => AgentEvent::Subagent {
                parent_tool_use_id: parent.clone(),
                event: Box::new(ev),
            },
            None => ev,
        }
    };
    match frame.get("ev").and_then(Value::as_str).unwrap_or("") {
        "text" => vec![tag(AgentEvent::TextDelta { text: text() })],
        "thinking" => vec![tag(AgentEvent::ReasoningDelta { text: text() })],
        "tool" => {
            let id = frame
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let name = frame.get("name").and_then(Value::as_str).unwrap_or("tool");
            let args = frame.get("args").cloned().unwrap_or(Value::Null);
            match frame.get("phase").and_then(Value::as_str) {
                Some("start") => {
                    let mut events = vec![tag(AgentEvent::ToolCall {
                        id: id.clone(),
                        call: decode_tool(name, &args),
                    })];
                    if name == "task" && parent.is_none() {
                        if let Some(prompt) = args
                            .get("prompt")
                            .or_else(|| args.get("description"))
                            .and_then(Value::as_str)
                            .filter(|p| !p.trim().is_empty())
                        {
                            events.push(AgentEvent::Subagent {
                                parent_tool_use_id: id,
                                event: Box::new(AgentEvent::UserMessage {
                                    text: prompt.to_owned(),
                                }),
                            });
                        }
                    }
                    events
                }
                Some("end") => {
                    let is_error = frame.get("error").and_then(Value::as_bool) == Some(true);
                    let mut events = vec![
                        tag(AgentEvent::ToolCall {
                            id: id.clone(),
                            call: decode_tool(name, &args),
                        }),
                        tag(AgentEvent::ToolResult {
                            id: id.clone(),
                            is_error,
                            output: None,
                            diff: None,
                        }),
                    ];
                    if name == "task" && parent.is_none() {
                        events.push(AgentEvent::Subagent {
                            parent_tool_use_id: id,
                            event: Box::new(AgentEvent::Done {
                                status: if is_error {
                                    DoneStatus::Errored
                                } else {
                                    DoneStatus::Completed
                                },
                                result: None,
                                error: None,
                                session_id: None,
                            }),
                        });
                    }
                    events
                }
                _ => Vec::new(),
            }
        }
        "usage" => vec![AgentEvent::Usage {
            input_tokens: frame.get("input").and_then(Value::as_u64).unwrap_or(0),
            output_tokens: frame.get("output").and_then(Value::as_u64).unwrap_or(0),
        }],
        "turn" => {
            let status = frame.get("status").and_then(Value::as_str).unwrap_or("");
            let error = frame
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let status = if interrupted || status == "cancelled" {
                DoneStatus::Interrupted
            } else if status == "error" {
                DoneStatus::Errored
            } else {
                DoneStatus::Completed
            };
            vec![AgentEvent::Done {
                status,
                result: None,
                error,
                session_id: None,
            }]
        }
        "fatal" => vec![AgentEvent::Done {
            status: DoneStatus::Errored,
            result: None,
            error: Some(
                frame
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("cursor shim failed")
                    .to_owned(),
            ),
            session_id: None,
        }],
        other => {
            tracing::debug!(target: "zeron_harness::cursor", "unknown shim frame (skipped): {other}");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_cursor_tool_vocabulary() {
        assert_eq!(
            decode_tool("shell", &json!({"command": "ls"})),
            ToolCall::Exec {
                command: "ls".into()
            }
        );
        assert_eq!(
            decode_tool("grep", &json!({"pattern": "foo", "path": "/src"})),
            ToolCall::Search {
                pattern: "foo".into(),
                path: Some("/src".into())
            }
        );
        assert!(matches!(
            decode_tool("task", &json!({"description": "map the repo"})),
            ToolCall::Unknown { name, .. } if name == "Agent: map the repo"
        ));
        assert!(matches!(
            decode_tool("somethingNew", &json!({})),
            ToolCall::Unknown { .. }
        ));
    }

    #[test]
    fn task_start_seeds_the_subagent_opening_user_message() {
        let frame: Value = serde_json::from_str(
            r#"{"ev":"tool","phase":"start","id":"call_task_1","name":"task","args":{"description":"probe","prompt":"scan the fold path"}}"#,
        )
        .unwrap();
        let events = map_shim_frame(&frame, false);
        assert!(matches!(
            &events[..],
            [
                AgentEvent::ToolCall { id, .. },
                AgentEvent::Subagent { parent_tool_use_id, event },
            ] if id == "call_task_1"
                && parent_tool_use_id == "call_task_1"
                && matches!(event.as_ref(), AgentEvent::UserMessage { text } if text == "scan the fold path")
        ));
        let nested: Value = serde_json::from_str(
            r#"{"ev":"tool","phase":"start","id":"call_task_2","name":"task","args":{"prompt":"inner"},"parent":"call_task_1"}"#,
        )
        .unwrap();
        assert_eq!(map_shim_frame(&nested, false).len(), 1);
    }

    #[test]
    fn nested_frames_arrive_tagged() {
        let frame: Value =
            serde_json::from_str(r#"{"ev":"text","text":"sub says","parent":"call_task_1"}"#)
                .unwrap();
        assert_eq!(
            map_shim_frame(&frame, false),
            vec![AgentEvent::Subagent {
                parent_tool_use_id: "call_task_1".into(),
                event: Box::new(AgentEvent::TextDelta {
                    text: "sub says".into()
                }),
            }]
        );
    }

    #[test]
    fn parses_cursor_cli_model_catalog_and_ignores_headings() {
        let models = parse_cli_models(
            "Available models\n\nauto - Auto (current, default)\n\ngpt-5.3-codex - Codex 5.3\n",
        );
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "auto");
        assert_eq!(models[0].label, "Auto (current, default)");
        assert_eq!(models[1].id, "gpt-5.3-codex");
        assert_eq!(models[1].label, "Codex 5.3");
    }
}
