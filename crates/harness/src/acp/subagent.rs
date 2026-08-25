use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use zeron_proto::{AgentEvent, DoneStatus, ToolCall};

use crate::HarnessError;

use super::normalize::{OUTPUT_CAP, cap_text, xai_tool_name};

const TAIL_POLL: Duration = Duration::from_millis(250);
const DRAIN_POLLS: u32 = 6;
const DRAIN_POLL: Duration = Duration::from_millis(200);

fn default_sessions_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join(".grok")
        .join("sessions")
}

fn tag(parent: &str, event: AgentEvent) -> AgentEvent {
    AgentEvent::Subagent {
        parent_tool_use_id: parent.to_owned(),
        event: Box::new(event),
    }
}

fn done_status(status: &str) -> DoneStatus {
    match status {
        "failed" | "error" | "errored" => DoneStatus::Errored,
        "cancelled" | "canceled" | "killed" | "stopped" | "interrupted" => DoneStatus::Interrupted,
        _ => DoneStatus::Completed,
    }
}

struct PendingSpawn {
    tool_call_id: String,
    description: String,
}

struct TailFinish {
    status: DoneStatus,
    output: String,
}

struct SpawnedUnbound {
    child_session_id: String,
}

pub(crate) struct SubagentTracker {
    session_id: String,
    event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
    sessions_root: PathBuf,
    pending: VecDeque<PendingSpawn>,
    bound: HashMap<String, String>,
    spawned_unbound: HashMap<String, SpawnedUnbound>,
    tails: HashMap<String, oneshot::Sender<TailFinish>>,
}

impl SubagentTracker {
    pub(crate) fn new(
        session_id: String,
        event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
        sessions_root: Option<PathBuf>,
    ) -> Self {
        Self {
            session_id,
            event_tx,
            sessions_root: sessions_root.unwrap_or_else(default_sessions_root),
            pending: VecDeque::new(),
            bound: HashMap::new(),
            spawned_unbound: HashMap::new(),
            tails: HashMap::new(),
        }
    }

    pub(crate) fn observe(&mut self, update: &Value) {
        match update.get("sessionUpdate").and_then(Value::as_str) {
            Some("tool_call") | Some("tool_call_update") => self.observe_tool_call(update),
            Some("subagent_spawned") => self.observe_spawned(update),
            Some("subagent_finished") => self.observe_finished(update),
            _ => {}
        }
    }

    fn observe_tool_call(&mut self, update: &Value) {
        let id = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if id.is_empty() {
            return;
        }
        if xai_tool_name(update) == Some("spawn_subagent") {
            let description = update
                .get("rawInput")
                .and_then(|r| r.get("description"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            match self.pending.iter_mut().find(|p| p.tool_call_id == id) {
                Some(p) if p.description.is_empty() => p.description = description.to_owned(),
                Some(_) => {}
                None => self.pending.push_back(PendingSpawn {
                    tool_call_id: id.to_owned(),
                    description: description.to_owned(),
                }),
            }
        }
        if update.get("status").and_then(Value::as_str) == Some("completed")
            && self.pending.iter().any(|p| p.tool_call_id == id)
            && let Some(sub_id) = spawn_output_subagent_id(update)
        {
            self.pending.retain(|p| p.tool_call_id != id);
            self.bound.insert(sub_id.clone(), id.to_owned());
            if let Some(spawned) = self.spawned_unbound.remove(&sub_id) {
                self.start_tail(&sub_id, &spawned.child_session_id);
            }
        }
    }

    fn observe_spawned(&mut self, update: &Value) {
        if let Some(parent) = update.get("parent_session_id").and_then(Value::as_str)
            && parent != self.session_id
        {
            return;
        }
        let Some(sub_id) = update
            .get("subagent_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return;
        };
        let child_session_id = update
            .get("child_session_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(sub_id)
            .to_owned();
        if !self.bound.contains_key(sub_id) {
            let description = update
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let ix = self
                .pending
                .iter()
                .position(|p| !description.is_empty() && p.description == description)
                .or(if self.pending.is_empty() {
                    None
                } else {
                    Some(0)
                });
            match ix.and_then(|i| self.pending.remove(i)) {
                Some(p) => {
                    self.bound.insert(sub_id.to_owned(), p.tool_call_id);
                }
                None => {
                    self.spawned_unbound
                        .insert(sub_id.to_owned(), SpawnedUnbound { child_session_id });
                    return;
                }
            }
        }
        self.start_tail(sub_id, &child_session_id);
    }

    fn observe_finished(&mut self, update: &Value) {
        let Some(sub_id) = update.get("subagent_id").and_then(Value::as_str) else {
            return;
        };
        let finish = TailFinish {
            status: done_status(
                update
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("completed"),
            ),
            output: update
                .get("output")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        };
        self.spawned_unbound.remove(sub_id);
        if let Some(tx) = self.tails.remove(sub_id) {
            let _ = tx.send(finish);
        } else if let Some(parent) = self.bound.get(sub_id).cloned() {
            let event_tx = self.event_tx.clone();
            tokio::spawn(async move {
                if !finish.output.is_empty() {
                    let _ = event_tx
                        .send(Ok(tag(
                            &parent,
                            AgentEvent::TextDelta {
                                text: finish.output,
                            },
                        )))
                        .await;
                }
                let _ = event_tx
                    .send(Ok(tag(
                        &parent,
                        AgentEvent::Done {
                            status: finish.status,
                            result: None,
                            error: None,
                            session_id: None,
                        },
                    )))
                    .await;
            });
        }
    }

    fn start_tail(&mut self, sub_id: &str, child_session_id: &str) {
        let Some(parent) = self.bound.get(sub_id).cloned() else {
            return;
        };
        if self.tails.contains_key(sub_id) {
            return;
        }
        let (finished_tx, finished_rx) = oneshot::channel();
        self.tails.insert(sub_id.to_owned(), finished_tx);
        tokio::spawn(tail_task(
            self.event_tx.clone(),
            self.sessions_root.clone(),
            child_session_id.to_owned(),
            parent,
            finished_rx,
        ));
    }
}

fn spawn_output_subagent_id(update: &Value) -> Option<String> {
    let text = update
        .get("rawOutput")
        .and_then(|r| r.get("text"))
        .and_then(Value::as_str)
        .or_else(|| {
            update
                .get("content")?
                .as_array()?
                .iter()
                .find_map(|c| c.get("content")?.get("text")?.as_str())
        })?;
    text.lines().find_map(|line| {
        let id = line.trim().strip_prefix("subagent_id:")?.trim();
        (!id.is_empty()).then(|| id.to_owned())
    })
}

fn locate_history(root: &Path, child_session_id: &str) -> Option<PathBuf> {
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let candidate = entry
            .path()
            .join(child_session_id)
            .join("chat_history.jsonl");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

struct TailReader {
    path: PathBuf,
    offset: u64,
    carry: Vec<u8>,
}

impl TailReader {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            carry: Vec::new(),
        }
    }

    fn read_new(&mut self) -> Vec<Value> {
        use std::io::{Read as _, Seek as _};
        let Ok(mut file) = std::fs::File::open(&self.path) else {
            return Vec::new();
        };
        if file.seek(std::io::SeekFrom::Start(self.offset)).is_err() {
            return Vec::new();
        }
        let mut bytes = Vec::new();
        let Ok(read) = file.read_to_end(&mut bytes) else {
            return Vec::new();
        };
        self.offset += read as u64;
        self.carry.extend_from_slice(&bytes);
        let mut entries = Vec::new();
        while let Some(nl) = self.carry.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.carry.drain(..=nl).collect();
            if let Ok(v) = serde_json::from_slice::<Value>(&line) {
                entries.push(v);
            }
        }
        entries
    }
}

fn entry_events(entry: &Value) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    match entry.get("type").and_then(Value::as_str) {
        Some("reasoning") => {
            let text: Vec<&str> = entry
                .get("summary")
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
                .iter()
                .filter(|s| s.get("type").and_then(Value::as_str) == Some("summary_text"))
                .filter_map(|s| s.get("text").and_then(Value::as_str))
                .filter(|t| !t.is_empty())
                .collect();
            if !text.is_empty() {
                events.push(AgentEvent::ReasoningDelta {
                    text: format!("{}\n\n", text.join("\n")),
                });
            }
        }
        Some("assistant") => {
            if let Some(text) = entry
                .get("content")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
            {
                events.push(AgentEvent::TextDelta {
                    text: format!("{text}\n\n"),
                });
            }
            for tc in entry
                .get("tool_calls")
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
            {
                let id = tc.get("id").and_then(Value::as_str).unwrap_or_default();
                let name = tc.get("name").and_then(Value::as_str).unwrap_or_default();
                if id.is_empty() || name.is_empty() {
                    continue;
                }
                let args = tc
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|a| serde_json::from_str::<Value>(a).ok())
                    .unwrap_or(Value::Null);
                events.push(AgentEvent::ToolCall {
                    id: id.to_owned(),
                    call: grok_tool_call(name, &args),
                });
            }
        }
        Some("tool_result") => {
            if let Some(id) = entry
                .get("tool_call_id")
                .and_then(Value::as_str)
                .filter(|i| !i.is_empty())
            {
                let output = entry
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|t| !t.is_empty())
                    .map(|t| cap_text(t, OUTPUT_CAP));
                events.push(AgentEvent::ToolResult {
                    id: id.to_owned(),
                    is_error: false,
                    output,
                    diff: None,
                });
            }
        }
        Some("user") => {
            if let Some(text) = entry
                .get("content")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|t| !t.is_empty() && !t.starts_with('<'))
            {
                events.push(AgentEvent::UserMessage {
                    text: text.to_owned(),
                });
            }
        }
        _ => {}
    }
    events
}

fn grok_tool_call(name: &str, args: &Value) -> ToolCall {
    let s = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| args.get(*k))
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    match name {
        "run_terminal_command" => ToolCall::Exec {
            command: s(&["command"]).unwrap_or_default(),
        },
        "read_file" => ToolCall::ReadFile {
            path: s(&["target_file", "file_path", "path"]).unwrap_or_default(),
        },
        "write_file" => ToolCall::WriteFile {
            path: s(&["target_file", "file_path", "path"]).unwrap_or_default(),
            content: s(&["contents", "content"]),
        },
        "search_replace" => ToolCall::EditFile {
            path: s(&["file_path", "target_file", "path"]).unwrap_or_default(),
            old_string: s(&["old_string"]),
            new_string: s(&["new_string"]),
        },
        "grep" => ToolCall::Search {
            pattern: s(&["pattern"]).unwrap_or_default(),
            path: s(&["path"]),
        },
        "glob" => ToolCall::Glob {
            pattern: s(&["pattern", "glob_pattern"]).unwrap_or_default(),
        },
        "web_search" => ToolCall::WebSearch {
            query: s(&["query", "search_term"]).unwrap_or_default(),
        },
        "spawn_subagent" => ToolCall::Unknown {
            name: s(&["description"])
                .map(|d| format!("Agent: {d}"))
                .unwrap_or_else(|| "Agent".into()),
            input: (!args.is_null()).then(|| args.clone()),
        },
        _ => ToolCall::Unknown {
            name: name.to_owned(),
            input: (!args.is_null()).then(|| args.clone()),
        },
    }
}

async fn tail_task(
    event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
    root: PathBuf,
    child_session_id: String,
    parent_tool_use_id: String,
    mut finished_rx: oneshot::Receiver<TailFinish>,
) {
    let mut reader: Option<TailReader> = None;
    let mut emitted = false;
    let pump = |reader: &mut Option<TailReader>| -> Vec<AgentEvent> {
        if reader.is_none() {
            *reader = locate_history(&root, &child_session_id).map(TailReader::new);
        }
        reader
            .as_mut()
            .map(|r| r.read_new())
            .unwrap_or_default()
            .iter()
            .flat_map(entry_events)
            .collect()
    };

    let finish = loop {
        for ev in pump(&mut reader) {
            emitted = true;
            if event_tx
                .send(Ok(tag(&parent_tool_use_id, ev)))
                .await
                .is_err()
            {
                return;
            }
        }
        if event_tx.is_closed() {
            return;
        }
        tokio::select! {
            fin = &mut finished_rx => {
                break fin.unwrap_or(TailFinish {
                    status: DoneStatus::Interrupted,
                    output: String::new(),
                });
            }
            _ = tokio::time::sleep(TAIL_POLL) => {}
        }
    };

    for i in 0..DRAIN_POLLS {
        let events = pump(&mut reader);
        for ev in events.iter().cloned() {
            emitted = true;
            if event_tx
                .send(Ok(tag(&parent_tool_use_id, ev)))
                .await
                .is_err()
            {
                return;
            }
        }
        if events.is_empty() && i > 0 {
            break;
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }
    if !emitted && !finish.output.is_empty() {
        if event_tx
            .send(Ok(tag(
                &parent_tool_use_id,
                AgentEvent::TextDelta {
                    text: finish.output,
                },
            )))
            .await
            .is_err()
        {
            return;
        }
    }
    let _ = event_tx
        .send(Ok(tag(
            &parent_tool_use_id,
            AgentEvent::Done {
                status: finish.status,
                result: None,
                error: None,
                session_id: None,
            },
        )))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn spawn_completion_output_yields_the_subagent_id() {
        let update = json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "t1",
            "status": "completed",
            "rawOutput": {"type": "Text", "text": "Subagent started in background.\nsubagent_id: 01a0-abc\ntype: explore"},
        });
        assert_eq!(
            spawn_output_subagent_id(&update).as_deref(),
            Some("01a0-abc")
        );
        let update = json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "t1",
            "status": "completed",
            "content": [{"type": "content", "content": {"type": "text", "text": "subagent_id: xyz"}}],
        });
        assert_eq!(spawn_output_subagent_id(&update).as_deref(), Some("xyz"));
    }

    #[test]
    fn chat_history_entries_map_to_transcript_events() {
        let reasoning = json!({
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "The user wants a probe.\n"}],
            "encrypted_content": "opaque", "status": "completed",
        });
        assert!(matches!(
            entry_events(&reasoning).as_slice(),
            [AgentEvent::ReasoningDelta { text }] if text.starts_with("The user wants a probe.")
        ));

        let assistant = json!({
            "type": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call-1-0",
                "name": "read_file",
                "arguments": "{\"target_file\":\"/w/README.md\",\"limit\":80}",
            }],
            "model_id": "grok-4.6-build",
        });
        assert!(matches!(
            entry_events(&assistant).as_slice(),
            [AgentEvent::ToolCall { id, call: ToolCall::ReadFile { path } }]
                if id == "call-1-0" && path == "/w/README.md"
        ));

        let result = json!({
            "type": "tool_result",
            "tool_call_id": "call-1-0",
            "content": "1→# zeron — Architecture",
        });
        assert!(matches!(
            entry_events(&result).as_slice(),
            [AgentEvent::ToolResult { id, is_error: false, output: Some(o), .. }]
                if id == "call-1-0" && o.contains("Architecture")
        ));

        let closing = json!({"type": "assistant", "content": "finished", "model_id": "m"});
        assert!(matches!(
            entry_events(&closing).as_slice(),
            [AgentEvent::TextDelta { text }] if text == "finished\n\n"
        ));

        assert!(matches!(
            entry_events(&json!({"type": "user", "content": "scan the fold path"})).as_slice(),
            [AgentEvent::UserMessage { text }] if text == "scan the fold path"
        ));
        assert!(entry_events(&json!({"type": "system", "content": "x"})).is_empty());
        assert!(
            entry_events(
                &json!({"type": "user", "content": "<system-reminder>tick</system-reminder>"})
            )
            .is_empty()
        );
    }

    #[test]
    fn grok_tool_names_type_the_common_calls() {
        let call = grok_tool_call("run_terminal_command", &json!({"command": "ls -la"}));
        assert_eq!(
            call,
            ToolCall::Exec {
                command: "ls -la".into()
            }
        );
        let call = grok_tool_call("grep", &json!({"pattern": "0\\.1", "glob": "*.rs"}));
        assert_eq!(
            call,
            ToolCall::Search {
                pattern: "0\\.1".into(),
                path: None
            }
        );
        let call = grok_tool_call(
            "search_replace",
            &json!({"file_path": "/w/a.rs", "old_string": "a", "new_string": "b"}),
        );
        assert_eq!(
            call,
            ToolCall::EditFile {
                path: "/w/a.rs".into(),
                old_string: Some("a".into()),
                new_string: Some("b".into()),
            }
        );
        let call = grok_tool_call("spawn_subagent", &json!({"description": "Scan crates"}));
        assert!(matches!(call, ToolCall::Unknown { name, .. } if name == "Agent: Scan crates"));
        let call = grok_tool_call("mystery_tool", &json!({"x": 1}));
        assert!(
            matches!(call, ToolCall::Unknown { name, input: Some(_) } if name == "mystery_tool")
        );
    }
}
