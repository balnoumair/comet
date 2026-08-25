use serde_json::Value;
use zeron_proto::{AgentEvent, DoneStatus, HarnessId, TodoItem, ToolCall};

use super::wire::{ContentBlock, Frame};

fn assistant_error_text(code: &str) -> String {
    match code {
        "authentication_failed" => "Authentication failed — sign in to Claude again.".into(),
        "oauth_org_not_allowed" => "This organization isn't allowed to use Claude here.".into(),
        "billing_error" => "Billing error — check your Claude plan or payment method.".into(),
        "rate_limit" => "Claude usage limit reached — try again after the limit resets.".into(),
        "overloaded" => "Claude is overloaded right now — try again shortly.".into(),
        "invalid_request" => "The request was rejected as invalid.".into(),
        "model_not_found" => "The selected model isn't available.".into(),
        "server_error" => "Claude had a server error — try again.".into(),
        "max_output_tokens" => "The reply hit the maximum output length.".into(),
        "unknown" => "Claude returned an unspecified error.".into(),
        other => format!("Claude error: {other}"),
    }
}

fn rate_window_label(kind: &str) -> &'static str {
    match kind {
        "five_hour" => "5-hour",
        "seven_day" | "seven_day_overage_included" => "weekly",
        "seven_day_opus" => "weekly (Opus)",
        "seven_day_sonnet" => "weekly (Sonnet)",
        "overage" => "overage",
        _ => "usage",
    }
}

fn result_error_text(subtype: &str) -> &'static str {
    match subtype {
        "error_max_turns" => "The run hit the maximum number of turns.",
        "error_max_budget_usd" => "The run hit its cost budget.",
        "error_max_structured_output_retries" => "The run exhausted its structured-output retries.",
        _ => "The run ended with an error.",
    }
}

fn is_internal_diagnostic(message: &str) -> bool {
    message.contains("[ede_diagnostic]")
}

fn str_field(input: &Value, key: &str) -> String {
    input.get(key).and_then(Value::as_str).unwrap_or("").into()
}

fn opt_str_field(input: &Value, key: &str) -> Option<String> {
    input.get(key).and_then(Value::as_str).map(str::to_owned)
}

pub(crate) fn decode_tool_use(name: &str, input: &Value) -> ToolCall {
    match name {
        "Bash" => ToolCall::Exec {
            command: str_field(input, "command"),
        },
        "Read" => ToolCall::ReadFile {
            path: str_field(input, "file_path"),
        },
        "Write" => ToolCall::WriteFile {
            path: str_field(input, "file_path"),
            content: opt_str_field(input, "content"),
        },
        "Edit" => ToolCall::EditFile {
            path: str_field(input, "file_path"),
            old_string: opt_str_field(input, "old_string"),
            new_string: opt_str_field(input, "new_string"),
        },
        "Grep" => ToolCall::Search {
            pattern: str_field(input, "pattern"),
            path: opt_str_field(input, "path"),
        },
        "Glob" => ToolCall::Glob {
            pattern: str_field(input, "pattern"),
        },
        "WebFetch" => ToolCall::WebFetch {
            url: str_field(input, "url"),
            prompt: opt_str_field(input, "prompt"),
        },
        "WebSearch" => ToolCall::WebSearch {
            query: str_field(input, "query"),
        },
        "TodoWrite" => ToolCall::Todo {
            items: input
                .get("todos")
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
                .iter()
                .map(|t| TodoItem {
                    text: str_field(t, "content"),
                    done: t.get("status").and_then(Value::as_str) == Some("completed"),
                })
                .collect(),
        },
        "Agent" | "Task" => {
            let description = str_field(input, "description");
            ToolCall::Unknown {
                name: if description.is_empty() {
                    "Agent".into()
                } else {
                    format!("Agent: {description}")
                },
                input: (!input.is_null()).then(|| input.clone()),
            }
        }
        _ => match name.strip_prefix("mcp__").and_then(|r| r.split_once("__")) {
            Some((server, tool)) => ToolCall::Mcp {
                server: server.into(),
                tool: tool.into(),
                input: (!input.is_null()).then(|| input.clone()),
            },
            None => ToolCall::Unknown {
                name: name.into(),
                input: (!input.is_null()).then(|| input.clone()),
            },
        },
    }
}

fn new_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn tag(parent: &str, event: AgentEvent) -> AgentEvent {
    AgentEvent::Subagent {
        parent_tool_use_id: parent.to_owned(),
        event: Box::new(event),
    }
}

fn is_synthetic_user_text(text: &str) -> bool {
    let text = text.trim_start();
    text.starts_with("<system-reminder>") || text.starts_with("[Request interrupted")
}

pub(crate) struct Normalizer {
    saw_init: bool,
    assistant_message_id: String,
    pub session_id: Option<String>,
}

impl Normalizer {
    pub fn new() -> Self {
        Self {
            saw_init: false,
            assistant_message_id: new_message_id(),
            session_id: None,
        }
    }

    pub fn rotate_for_steer(&mut self) -> (String, String) {
        let prev = std::mem::replace(&mut self.assistant_message_id, new_message_id());
        (prev, self.assistant_message_id.clone())
    }

    pub fn normalize(&mut self, frame: Frame, interrupted: bool) -> Vec<AgentEvent> {
        match frame {
            Frame::System(f) => {
                if f.subtype == "task_notification" {
                    let Some(parent) = f.tool_use_id.as_deref().filter(|t| !t.is_empty()) else {
                        return Vec::new();
                    };
                    let status = match f.status.as_deref().unwrap_or("") {
                        "completed" | "complete" | "succeeded" | "success" => DoneStatus::Completed,
                        "failed" | "errored" | "error" => DoneStatus::Errored,
                        "killed" | "cancelled" | "canceled" | "stopped" | "interrupted" => {
                            DoneStatus::Interrupted
                        }
                        _ => return Vec::new(),
                    };
                    return vec![tag(
                        parent,
                        AgentEvent::Done {
                            status,
                            result: None,
                            error: None,
                            session_id: None,
                        },
                    )];
                }
                if f.subtype != "init" || self.saw_init {
                    return Vec::new();
                }
                self.saw_init = true;
                self.session_id = Some(f.session_id.clone());
                vec![AgentEvent::SessionStarted {
                    harness: HarnessId::ClaudeCode,
                    model: f.model,
                    tools: f.tools,
                    cwd: f.cwd,
                    session_id: f.session_id,
                    assistant_message_id: self.assistant_message_id.clone(),
                }]
            }

            Frame::StreamEvent(f) => {
                if f.event.kind != "content_block_delta" {
                    return Vec::new();
                }
                if let Some(parent) = &f.parent_tool_use_id {
                    return match f.event.delta.kind.as_str() {
                        "text_delta" => vec![tag(
                            parent,
                            AgentEvent::TextDelta {
                                text: f.event.delta.text,
                            },
                        )],
                        "thinking_delta" if !f.event.delta.thinking.is_empty() => vec![tag(
                            parent,
                            AgentEvent::ReasoningDelta {
                                text: f.event.delta.thinking,
                            },
                        )],
                        _ => Vec::new(),
                    };
                }
                match f.event.delta.kind.as_str() {
                    "text_delta" => vec![AgentEvent::TextDelta {
                        text: f.event.delta.text,
                    }],
                    "thinking_delta" => vec![AgentEvent::ReasoningDelta {
                        text: f.event.delta.thinking,
                    }],
                    "input_json_delta" => vec![AgentEvent::ReasoningDelta {
                        text: String::new(),
                    }],
                    _ => Vec::new(),
                }
            }

            Frame::Assistant(f) => {
                if let Some(parent) = &f.parent_tool_use_id {
                    let mut out: Vec<AgentEvent> = f
                        .message
                        .blocks()
                        .filter_map(|b: ContentBlock| match b.kind.as_str() {
                            "text" if !b.text.is_empty() => Some(tag(
                                parent,
                                AgentEvent::TextDelta {
                                    text: format!("{}\n\n", b.text.trim_end()),
                                },
                            )),
                            "tool_use" => Some(tag(
                                parent,
                                AgentEvent::ToolCall {
                                    id: b.id.clone(),
                                    call: decode_tool_use(&b.name, &b.input),
                                },
                            )),
                            _ => None,
                        })
                        .collect();
                    if let Some(code) = &f.error {
                        out.push(tag(
                            parent,
                            AgentEvent::Error {
                                message: assistant_error_text(code),
                            },
                        ));
                    }
                    return out;
                }
                let mut out: Vec<AgentEvent> = f
                    .message
                    .blocks()
                    .filter(|b: &ContentBlock| b.kind == "tool_use")
                    .flat_map(|b| {
                        let call = AgentEvent::ToolCall {
                            id: b.id.clone(),
                            call: decode_tool_use(&b.name, &b.input),
                        };
                        let opening = matches!(b.name.as_str(), "Agent" | "Task")
                            .then(|| b.input.get("prompt"))
                            .flatten()
                            .and_then(Value::as_str)
                            .filter(|p| !p.trim().is_empty())
                            .map(|prompt| {
                                tag(
                                    &b.id,
                                    AgentEvent::UserMessage {
                                        text: prompt.to_owned(),
                                    },
                                )
                            });
                        std::iter::once(call).chain(opening)
                    })
                    .collect();
                if let Some(code) = &f.error {
                    out.push(AgentEvent::Error {
                        message: assistant_error_text(code),
                    });
                }
                let (prev, _next) = self.rotate_for_steer();
                out.push(AgentEvent::AssistantMessageCompleted {
                    assistant_message_id: prev,
                });
                out
            }

            Frame::User(f) => {
                if let Some(parent) = &f.parent_tool_use_id {
                    let mut out: Vec<AgentEvent> = f
                        .message
                        .blocks()
                        .filter(|b: &ContentBlock| b.kind == "tool_result")
                        .map(|b| {
                            tag(
                                parent,
                                AgentEvent::ToolResult {
                                    id: b.tool_use_id.clone(),
                                    is_error: b.is_error.unwrap_or(false),
                                    output: None,
                                    diff: None,
                                },
                            )
                        })
                        .collect();
                    out.extend(
                        f.message
                            .blocks()
                            .filter(|b: &ContentBlock| {
                                b.kind == "text"
                                    && !b.text.trim().is_empty()
                                    && !is_synthetic_user_text(&b.text)
                            })
                            .map(|b| tag(parent, AgentEvent::UserMessage { text: b.text })),
                    );
                    return out;
                }
                f.message
                    .blocks()
                    .filter(|b: &ContentBlock| b.kind == "tool_result")
                    .map(|b| AgentEvent::ToolResult {
                        id: b.tool_use_id.clone(),
                        is_error: b.is_error.unwrap_or(false),
                        output: None,
                        diff: None,
                    })
                    .collect()
            }

            Frame::RateLimit(f) => {
                if f.rate_limit_info.status != "rejected" {
                    return Vec::new();
                }
                let window =
                    rate_window_label(f.rate_limit_info.rate_limit_type.as_deref().unwrap_or(""));
                vec![AgentEvent::Error {
                    message: format!(
                        "Claude {window} limit reached — the turn was blocked. Try again after it resets."
                    ),
                }]
            }

            Frame::Result(f) => {
                if let Some(id) = &f.session_id {
                    self.session_id = Some(id.clone());
                }
                let usage = AgentEvent::Usage {
                    input_tokens: f.usage.input_tokens,
                    output_tokens: f.usage.output_tokens,
                };
                let done = if f.subtype == "success" {
                    AgentEvent::Done {
                        status: if interrupted {
                            DoneStatus::Interrupted
                        } else {
                            DoneStatus::Completed
                        },
                        result: f.result,
                        error: None,
                        session_id: f.session_id,
                    }
                } else {
                    let (diagnostics, errors): (Vec<String>, Vec<String>) = f
                        .errors
                        .iter()
                        .map(|e| match e {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .partition(|m| is_internal_diagnostic(m));
                    for diagnostic in &diagnostics {
                        tracing::debug!(
                            target: "zeron_harness::claude",
                            "internal CLI diagnostic (not surfaced): {diagnostic}"
                        );
                    }
                    let error = if !errors.is_empty() {
                        Some(errors.join("; "))
                    } else {
                        match f.subtype.as_str() {
                            "error_max_turns"
                            | "error_max_budget_usd"
                            | "error_max_structured_output_retries" => {
                                Some(result_error_text(&f.subtype).to_owned())
                            }
                            _ if !diagnostics.is_empty() => None,
                            _ => Some(result_error_text(&f.subtype).to_owned()),
                        }
                    };
                    AgentEvent::Done {
                        status: if interrupted {
                            DoneStatus::Interrupted
                        } else {
                            DoneStatus::Errored
                        },
                        result: None,
                        error,
                        session_id: f.session_id,
                    }
                };
                vec![usage, done]
            }

            Frame::ControlRequest(_) | Frame::Other => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_typed_tools() {
        assert_eq!(
            decode_tool_use("Bash", &json!({"command": "ls -la"})),
            ToolCall::Exec {
                command: "ls -la".into()
            }
        );
        assert_eq!(
            decode_tool_use(
                "Edit",
                &json!({"file_path": "/a", "old_string": "x", "new_string": "y"})
            ),
            ToolCall::EditFile {
                path: "/a".into(),
                old_string: Some("x".into()),
                new_string: Some("y".into())
            }
        );
        assert_eq!(
            decode_tool_use(
                "TodoWrite",
                &json!({"todos": [{"content": "t", "status": "completed"}]})
            ),
            ToolCall::Todo {
                items: vec![TodoItem {
                    text: "t".into(),
                    done: true
                }]
            }
        );
        assert_eq!(
            decode_tool_use("mcp__linear__search", &json!({"q": "bug"})),
            ToolCall::Mcp {
                server: "linear".into(),
                tool: "search".into(),
                input: Some(json!({"q": "bug"}))
            }
        );
        assert!(matches!(
            decode_tool_use("Mystery", &json!({})),
            ToolCall::Unknown { .. }
        ));
    }

    fn normalize_one(raw: &str) -> Vec<AgentEvent> {
        let frame = crate::claude::wire::parse_frame(raw).expect("frame parses");
        Normalizer::new().normalize(frame, false)
    }

    fn result_done(raw: &str) -> AgentEvent {
        let events = normalize_one(raw);
        assert_eq!(events.len(), 2, "usage + done");
        events.into_iter().nth(1).expect("done event")
    }

    #[test]
    fn stream_deltas_map_to_text_reasoning_and_heartbeats() {
        let ev = normalize_one(
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hmm"}}}"#,
        );
        assert_eq!(ev, vec![AgentEvent::ReasoningDelta { text: "hmm".into() }]);
        let ev = normalize_one(
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"","estimated_tokens":50}}}"#,
        );
        assert_eq!(
            ev,
            vec![AgentEvent::ReasoningDelta {
                text: String::new()
            }]
        );
        let ev = normalize_one(
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{\"file_"}}}"#,
        );
        assert_eq!(
            ev,
            vec![AgentEvent::ReasoningDelta {
                text: String::new()
            }]
        );
        let ev = normalize_one(
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"signature_delta","signature":"abc"}}}"#,
        );
        assert!(ev.is_empty());
    }

    #[test]
    fn subagent_stream_deltas_arrive_tagged() {
        let ev = normalize_one(
            r#"{"type":"stream_event","parent_tool_use_id":"toolu_sub","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"sub text"}}}"#,
        );
        assert_eq!(
            ev,
            vec![AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::TextDelta {
                    text: "sub text".into()
                }),
            }]
        );
        let ev = normalize_one(
            r#"{"type":"stream_event","parent_tool_use_id":"toolu_sub","event":{"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{"}}}"#,
        );
        assert!(ev.is_empty());
    }

    #[test]
    fn subagent_tool_calls_and_results_arrive_tagged_without_boundary_rotation() {
        let mut norm = Normalizer::new();
        let before = norm.assistant_message_id.clone();
        let frame = crate::claude::wire::parse_frame(
            r#"{"type":"assistant","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}"#,
        )
        .expect("parses");
        let ev = norm.normalize(frame, false);
        assert_eq!(
            ev,
            vec![AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::ToolCall {
                    id: "t1".into(),
                    call: ToolCall::Exec {
                        command: "ls".into()
                    },
                }),
            }]
        );
        assert_eq!(norm.assistant_message_id, before);

        let frame = crate::claude::wire::parse_frame(
            r#"{"type":"user","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"tool_result","tool_use_id":"t1","is_error":false}]}}"#,
        )
        .expect("parses");
        let ev = norm.normalize(frame, false);
        assert_eq!(
            ev,
            vec![AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::ToolResult {
                    id: "t1".into(),
                    is_error: false,
                    output: None,
                    diff: None,
                }),
            }]
        );
    }

    #[test]
    fn spawn_prompt_seeds_the_subagent_opening_user_message() {
        let ev = normalize_one(
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_sub","name":"Task","input":{"description":"probe","prompt":"scan the fold path"}}]}}"#,
        );
        assert!(matches!(
            &ev[..],
            [
                AgentEvent::ToolCall { id, .. },
                AgentEvent::Subagent { parent_tool_use_id, event },
                AgentEvent::AssistantMessageCompleted { .. },
            ] if id == "toolu_sub"
                && parent_tool_use_id == "toolu_sub"
                && matches!(event.as_ref(), AgentEvent::UserMessage { text } if text == "scan the fold path")
        ));
        for frame in [
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Task","input":{"description":"probe"}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t2","name":"Bash","input":{"command":"ls","prompt":"red herring"}}]}}"#,
        ] {
            let ev = normalize_one(frame);
            assert!(
                !ev.iter().any(|e| matches!(e, AgentEvent::Subagent { .. })),
                "{frame}: {ev:?}"
            );
        }
    }

    #[test]
    fn the_interruption_marker_is_not_a_steer() {
        for marker in [
            "[Request interrupted by user]",
            "[Request interrupted by user for tool use]",
        ] {
            let frame = format!(
                r#"{{"type":"user","parent_tool_use_id":"toolu_spawn","message":{{"content":[{{"type":"text","text":"{marker}"}}]}}}}"#
            );
            assert!(
                !normalize_one(&frame)
                    .iter()
                    .any(|e| matches!(e, AgentEvent::Subagent { .. })),
                "{marker} leaked as a steer"
            );
        }
        let real = r#"{"type":"user","parent_tool_use_id":"toolu_spawn","message":{"content":[{"type":"text","text":"Keep going."}]}}"#;
        assert!(
            normalize_one(real).iter().any(|e| matches!(
                e,
                AgentEvent::Subagent { parent_tool_use_id, event }
                    if parent_tool_use_id == "toolu_spawn"
                        && matches!(event.as_ref(), AgentEvent::UserMessage { text } if text == "Keep going.")
            )),
            "a real steer must still reach the subagent"
        );
    }

    #[test]
    fn tagged_user_text_becomes_a_subagent_steer() {
        let ev = normalize_one(
            r#"{"type":"user","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"text","text":"Also check the rebuild path."}]}}"#,
        );
        assert_eq!(
            ev,
            vec![AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::UserMessage {
                    text: "Also check the rebuild path.".into(),
                }),
            }]
        );
        for frame in [
            r#"{"type":"user","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"text","text":"<system-reminder>tick</system-reminder>"}]}}"#,
            r#"{"type":"user","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"text","text":"   "}]}}"#,
            r#"{"type":"user","message":{"content":[{"type":"text","text":"typed into the parent"}]}}"#,
        ] {
            assert_eq!(normalize_one(frame), Vec::new(), "frame: {frame}");
        }
        let ev = normalize_one(
            r#"{"type":"user","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"tool_result","tool_use_id":"t1","is_error":false},{"type":"text","text":"Keep going."}]}}"#,
        );
        assert!(matches!(
            &ev[..],
            [
                AgentEvent::Subagent { event: first, .. },
                AgentEvent::Subagent { event: second, .. },
            ] if matches!(first.as_ref(), AgentEvent::ToolResult { .. })
                && matches!(second.as_ref(), AgentEvent::UserMessage { text } if text == "Keep going.")
        ));
    }
    #[test]
    fn task_notification_settles_the_subagent_with_a_tagged_done() {
        let ev = normalize_one(
            r#"{"type":"system","subtype":"task_notification","task_id":"t1","tool_use_id":"toolu_agent","status":"completed","summary":"DONE."}"#,
        );
        assert_eq!(
            ev,
            vec![AgentEvent::Subagent {
                parent_tool_use_id: "toolu_agent".into(),
                event: Box::new(AgentEvent::Done {
                    status: DoneStatus::Completed,
                    result: None,
                    error: None,
                    session_id: None,
                }),
            }]
        );
        let ev = normalize_one(
            r#"{"type":"system","subtype":"task_notification","tool_use_id":"toolu_agent","status":"failed"}"#,
        );
        assert!(matches!(
            &ev[..],
            [AgentEvent::Subagent { event, .. }]
                if matches!(event.as_ref(), AgentEvent::Done { status: DoneStatus::Errored, .. })
        ));
        assert!(normalize_one(
            r#"{"type":"system","subtype":"task_notification","tool_use_id":"toolu_agent","status":"running"}"#,
        )
        .is_empty());
        assert!(
            normalize_one(
                r#"{"type":"system","subtype":"task_notification","status":"completed"}"#,
            )
            .is_empty()
        );
    }

    #[test]
    fn subagent_assistant_text_blocks_emit_tagged_text() {
        let ev = normalize_one(
            r#"{"type":"assistant","parent_tool_use_id":"toolu_sub","message":{"content":[{"type":"text","text":"working on it"},{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}"#,
        );
        assert_eq!(ev.len(), 2);
        assert_eq!(
            ev[0],
            AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::TextDelta {
                    text: "working on it\n\n".into()
                }),
            }
        );
        assert!(matches!(&ev[1], AgentEvent::Subagent { event, .. }
            if matches!(event.as_ref(), AgentEvent::ToolCall { .. })));
    }

    #[test]
    fn wake_turn_init_is_deduped_but_second_result_still_emits_done() {
        let mut norm = Normalizer::new();
        let init = r#"{"type":"system","subtype":"init","model":"m","cwd":"/x","session_id":"s1"}"#;
        let frame = crate::claude::wire::parse_frame(init).unwrap();
        assert_eq!(norm.normalize(frame, false).len(), 1, "first init");
        let frame = crate::claude::wire::parse_frame(init).unwrap();
        assert!(
            norm.normalize(frame, false).is_empty(),
            "wake init deduped — SessionStarted is the fold's run boundary"
        );
        let result = r#"{"type":"result","subtype":"success","session_id":"s1"}"#;
        let frame = crate::claude::wire::parse_frame(result).unwrap();
        let events = norm.normalize(frame, false);
        assert!(
            matches!(
                events.last(),
                Some(AgentEvent::Done {
                    status: DoneStatus::Completed,
                    ..
                })
            ),
            "wake turn settles with its own Done"
        );
    }

    #[test]
    fn ede_diagnostics_never_surface_as_errors() {
        let done = result_done(
            r#"{"type":"result","subtype":"error_during_execution","errors":["[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=null"]}"#,
        );
        match done {
            AgentEvent::Done { status, error, .. } => {
                assert_eq!(status, DoneStatus::Errored);
                assert_eq!(error, None, "diagnostic-only failure surfaces no text");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn real_errors_survive_diagnostic_filtering() {
        let done = result_done(
            r#"{"type":"result","subtype":"error_during_execution","errors":["[ede_diagnostic] turn aborted (x) stop_reason=null","Something real broke"]}"#,
        );
        match done {
            AgentEvent::Done { error, .. } => {
                assert_eq!(error.as_deref(), Some("Something real broke"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn known_failure_subtypes_keep_mapped_wording() {
        let done = result_done(
            r#"{"type":"result","subtype":"error_max_turns","errors":["[ede_diagnostic] turn aborted (max) stop_reason=null"]}"#,
        );
        match done {
            AgentEvent::Done { error, .. } => {
                assert_eq!(
                    error.as_deref(),
                    Some("The run hit the maximum number of turns.")
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
        let done = result_done(r#"{"type":"result","subtype":"error_max_turns","errors":[]}"#);
        match done {
            AgentEvent::Done { error, .. } => {
                assert_eq!(
                    error.as_deref(),
                    Some("The run hit the maximum number of turns.")
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
