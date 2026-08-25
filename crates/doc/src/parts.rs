use serde::{Deserialize, Serialize};

use zeron_proto::{AgentEvent, ToolCall, ToolDiff, UserInputQuestion};

use crate::constants::MSG_INLINE_MAX;

pub const TOOL_OUTPUT_SUMMARY_MAX: usize = 160;

pub fn summarize_tool_output(text: &str) -> Option<String> {
    let kept: Vec<&str> = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("```"))
        .collect();
    let stripped = kept.join("\n");
    let stripped = stripped.trim();
    if stripped.is_empty() {
        return None;
    }
    if stripped.chars().count() <= TOOL_OUTPUT_SUMMARY_MAX {
        return Some(stripped.to_owned());
    }
    let line = stripped
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or(stripped)
        .trim_end();
    let mut chars = 0usize;
    let mut end = line.len();
    for (i, _) in line.char_indices() {
        if chars == TOOL_OUTPUT_SUMMARY_MAX {
            end = i;
            break;
        }
        chars += 1;
    }
    let mut out = line[..end].to_owned();
    out.push('…');
    Some(out)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDiffStat {
    pub path: String,
    pub additions: u64,
    pub deletions: u64,
}

pub fn diff_stat(diff: &ToolDiff) -> ToolDiffStat {
    let (additions, deletions) = match &diff.old_text {
        None => (diff.new_text.lines().count() as u64, 0),
        Some(old) => {
            let text_diff = similar::TextDiff::from_lines(old.as_str(), diff.new_text.as_str());
            let mut additions = 0u64;
            let mut deletions = 0u64;
            for change in text_diff.iter_all_changes() {
                match change.tag() {
                    similar::ChangeTag::Insert => additions += 1,
                    similar::ChangeTag::Delete => deletions += 1,
                    similar::ChangeTag::Equal => {}
                }
            }
            (additions, deletions)
        }
    };
    ToolDiffStat {
        path: diff.path.clone(),
        additions,
        deletions,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageStatus {
    Streaming,
    Complete,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubagentStatus {
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum MessagePart {
    Text {
        id: String,
        text: String,
    },
    #[serde(rename_all = "camelCase")]
    Tool {
        id: String,
        call: ToolCall,
        #[serde(default)]
        is_error: bool,
        #[serde(default)]
        resolved: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff: Option<ToolDiff>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_bytes: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff_stats: Option<Vec<ToolDiffStat>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_status: Option<SubagentStatus>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_tail: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Input {
        id: String,
        request_id: String,
        questions: Vec<UserInputQuestion>,
        #[serde(default)]
        resolved: bool,
    },
    Error {
        id: String,
        message: String,
    },
}

impl MessagePart {
    pub fn id(&self) -> &str {
        match self {
            MessagePart::Text { id, .. }
            | MessagePart::Tool { id, .. }
            | MessagePart::Input { id, .. }
            | MessagePart::Error { id, .. } => id,
        }
    }

    pub fn byte_len(&self) -> usize {
        match self {
            MessagePart::Text { text, .. } => text.len(),
            MessagePart::Tool {
                call,
                output,
                diff,
                diff_stats,
                ..
            } => {
                serde_json::to_vec(call).map_or(0, |v| v.len())
                    + output.as_ref().map_or(0, String::len)
                    + diff
                        .as_ref()
                        .map_or(0, |d| serde_json::to_vec(d).map_or(0, |v| v.len()))
                    + diff_stats
                        .as_ref()
                        .map_or(0, |s| serde_json::to_vec(s).map_or(0, |v| v.len()))
            }
            MessagePart::Input { questions, .. } => {
                serde_json::to_vec(questions).map_or(0, |v| v.len())
            }
            MessagePart::Error { message, .. } => message.len(),
        }
    }
}

pub fn fold_event_into_parts(out: &mut Vec<MessagePart>, event: &AgentEvent) {
    match event {
        AgentEvent::SessionStarted { .. } | AgentEvent::Steered { .. } => {
            out.clear();
        }
        AgentEvent::TextDelta { text } => {
            if let Some(MessagePart::Text { text: tail, .. }) = out.last_mut() {
                tail.push_str(text);
            } else {
                let id = format!("t{}", out.len());
                out.push(MessagePart::Text {
                    id,
                    text: text.clone(),
                });
            }
        }
        AgentEvent::ReasoningDelta { .. } => {}
        AgentEvent::ToolCall { id, call } => {
            if let Some(existing) = out.iter_mut().find_map(|p| match p {
                MessagePart::Tool {
                    id: pid, call: c, ..
                } if pid == id => Some(c),
                _ => None,
            }) {
                *existing = call.clone();
            } else {
                out.push(MessagePart::Tool {
                    id: id.clone(),
                    call: call.clone(),
                    is_error: false,
                    resolved: false,
                    output: None,
                    diff: None,
                    output_ref: None,
                    output_bytes: None,
                    diff_ref: None,
                    diff_stats: None,
                    subagent_ref: None,
                    subagent_status: None,
                    subagent_tail: None,
                });
            }
        }
        AgentEvent::ToolResult {
            id,
            is_error,
            output,
            diff,
        } => {
            for p in out.iter_mut() {
                if let MessagePart::Tool {
                    id: pid,
                    is_error: e,
                    resolved,
                    output: out_slot,
                    diff: diff_slot,
                    output_bytes,
                    diff_stats,
                    ..
                } = p
                    && pid == id
                {
                    *e = *is_error;
                    *resolved = true;
                    let _ = output;
                    *out_slot = None;
                    *output_bytes = None;
                    *diff_slot = None;
                    *diff_stats = diff.as_ref().map(|d| vec![diff_stat(d)]);
                }
            }
        }
        AgentEvent::InputRequested {
            request_id,
            questions,
        } => {
            let id = format!("in-{request_id}");
            if !out.iter().any(|p| p.id() == id) {
                out.push(MessagePart::Input {
                    id,
                    request_id: request_id.clone(),
                    questions: questions.clone(),
                    resolved: false,
                });
            }
        }
        AgentEvent::InputResolved { request_id } => {
            for p in out.iter_mut() {
                if let MessagePart::Input {
                    request_id: rid,
                    resolved,
                    ..
                } = p
                    && rid == request_id
                {
                    *resolved = true;
                }
            }
        }
        AgentEvent::Error { message } => {
            let id = format!("e{}", out.len());
            out.push(MessagePart::Error {
                id,
                message: message.clone(),
            });
        }
        AgentEvent::Done { error, .. } => {
            if let Some(message) = error {
                let id = format!("e{}", out.len());
                out.push(MessagePart::Error {
                    id,
                    message: message.clone(),
                });
            }
        }
        AgentEvent::Subagent {
            parent_tool_use_id,
            event,
        } => {
            let status = match event.as_ref() {
                AgentEvent::Done { status, .. } => Some(match status {
                    zeron_proto::DoneStatus::Errored => SubagentStatus::Failed,
                    _ => SubagentStatus::Done,
                }),
                _ => None,
            };
            for p in out.iter_mut() {
                if let MessagePart::Tool {
                    id,
                    subagent_status,
                    ..
                } = p
                    && id == parent_tool_use_id
                {
                    match status {
                        Some(s) => *subagent_status = Some(s),
                        None if !matches!(
                            subagent_status,
                            Some(SubagentStatus::Done) | Some(SubagentStatus::Failed)
                        ) =>
                        {
                            *subagent_status = Some(SubagentStatus::Running);
                        }
                        None => {}
                    }
                }
            }
        }
        AgentEvent::UserMessage { .. }
        | AgentEvent::AssistantMessageCompleted { .. }
        | AgentEvent::Usage { .. }
        | AgentEvent::AvailableCommands { .. } => {}
    }
}

pub fn apply_sidecar_refs(chat_id: &str, parts: &mut [MessagePart]) {
    for part in parts.iter_mut() {
        if let MessagePart::Tool {
            id,
            resolved: true,
            output_ref,
            output_bytes,
            diff_ref,
            diff_stats,
            ..
        } = part
        {
            if output_ref.is_none() && output_bytes.is_some() {
                *output_ref = Some(format!("{chat_id}/{id}"));
            }
            if diff_ref.is_none() && diff_stats.is_some() {
                *diff_ref = Some(format!("{chat_id}/{id}.diff"));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SidecarPayload {
    pub part_id: String,
    pub output: Option<String>,
    pub diff: Option<ToolDiff>,
}

pub fn sidecar_payload(event: &AgentEvent) -> Option<SidecarPayload> {
    let AgentEvent::ToolResult {
        id, output, diff, ..
    } = event
    else {
        return None;
    };
    let output = output.clone().filter(|o| !o.trim().is_empty());
    if output.is_none() && diff.is_none() {
        return None;
    }
    Some(SidecarPayload {
        part_id: id.clone(),
        output,
        diff: diff.clone(),
    })
}

pub fn sanitize_tool_call(call: &ToolCall) -> ToolCall {
    match call {
        ToolCall::WriteFile { path, .. } => ToolCall::WriteFile {
            path: path.clone(),
            content: None,
        },
        ToolCall::EditFile { path, .. } => ToolCall::EditFile {
            path: path.clone(),
            old_string: None,
            new_string: None,
        },
        ToolCall::WebFetch { url, .. } => ToolCall::WebFetch {
            url: url.clone(),
            prompt: None,
        },
        ToolCall::Mcp { server, tool, .. } => ToolCall::Mcp {
            server: server.clone(),
            tool: tool.clone(),
            input: None,
        },
        ToolCall::Unknown { name, .. } => ToolCall::Unknown {
            name: name.clone(),
            input: None,
        },
        other => other.clone(),
    }
}

pub fn continuation_id(root: &str, index: usize) -> String {
    format!("{root}#c{index}")
}

pub fn split_parts(parts: &[MessagePart]) -> Vec<Vec<MessagePart>> {
    let mut chunks: Vec<Vec<MessagePart>> = vec![Vec::new()];
    let mut current_bytes = 0usize;

    let push_part = |chunks: &mut Vec<Vec<MessagePart>>, current: &mut usize, part: MessagePart| {
        let len = part.byte_len();
        if *current > 0 && *current + len > MSG_INLINE_MAX {
            chunks.push(Vec::new());
            *current = 0;
        }
        *current += len;
        chunks.last_mut().unwrap().push(part);
    };

    for part in parts {
        match part {
            MessagePart::Text { id, text } if text.len() > MSG_INLINE_MAX => {
                let mut start = 0usize;
                let mut piece = 0usize;
                while start < text.len() {
                    let mut end = (start + MSG_INLINE_MAX).min(text.len());
                    while end < text.len() && !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    if end <= start {
                        end = text.len();
                    }
                    let sub = MessagePart::Text {
                        id: if piece == 0 {
                            id.clone()
                        } else {
                            format!("{id}~{piece}")
                        },
                        text: text[start..end].to_string(),
                    };
                    push_part(&mut chunks, &mut current_bytes, sub);
                    start = end;
                    piece += 1;
                }
            }
            other => push_part(&mut chunks, &mut current_bytes, other.clone()),
        }
    }
    chunks
}

pub fn join_continuations(entries: Vec<Vec<MessagePart>>) -> Vec<MessagePart> {
    entries.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_delta(s: &str) -> AgentEvent {
        AgentEvent::TextDelta { text: s.into() }
    }

    #[test]
    fn text_deltas_merge_until_broken_by_tool() {
        let mut parts = Vec::new();
        fold_event_into_parts(&mut parts, &text_delta("Hello "));
        fold_event_into_parts(&mut parts, &text_delta("world"));
        assert_eq!(parts.len(), 1);
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolCall {
                id: "tool-1".into(),
                call: ToolCall::Exec {
                    command: "ls".into(),
                },
            },
        );
        fold_event_into_parts(&mut parts, &text_delta("after"));
        assert_eq!(parts.len(), 3);
        match &parts[2] {
            MessagePart::Text { text, .. } => assert_eq!(text, "after"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn session_started_resets_accumulator() {
        let mut parts = Vec::new();
        fold_event_into_parts(&mut parts, &text_delta("junk"));
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::SessionStarted {
                harness: zeron_proto::HarnessId::Mock,
                model: "m".into(),
                tools: vec![],
                cwd: "/".into(),
                session_id: "s".into(),
                assistant_message_id: "a".into(),
            },
        );
        assert!(parts.is_empty());
    }

    #[test]
    fn tool_call_refresh_is_idempotent() {
        let call = AgentEvent::ToolCall {
            id: "t".into(),
            call: ToolCall::Exec {
                command: "ls".into(),
            },
        };
        let mut once = Vec::new();
        fold_event_into_parts(&mut once, &call);
        let mut twice = once.clone();
        fold_event_into_parts(&mut twice, &call);
        assert_eq!(once, twice);
    }

    #[test]
    fn tool_result_marks_resolution() {
        let mut parts = Vec::new();
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolCall {
                id: "t".into(),
                call: ToolCall::Exec {
                    command: "ls".into(),
                },
            },
        );
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolResult {
                id: "t".into(),
                is_error: true,
                output: None,
                diff: None,
            },
        );
        match &parts[0] {
            MessagePart::Tool {
                is_error, resolved, ..
            } => {
                assert!(*is_error);
                assert!(*resolved);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn sanitize_strips_heavy_inputs_and_is_idempotent() {
        let call = ToolCall::WriteFile {
            path: "/x".into(),
            content: Some("secret".into()),
        };
        let clean = sanitize_tool_call(&call);
        assert_eq!(
            clean,
            ToolCall::WriteFile {
                path: "/x".into(),
                content: None
            }
        );
        assert_eq!(sanitize_tool_call(&clean), clean);
    }

    #[test]
    fn split_and_join_round_trip() {
        let big = "x".repeat(MSG_INLINE_MAX * 2 + 100);
        let parts = vec![
            MessagePart::Text {
                id: "t0".into(),
                text: big.clone(),
            },
            MessagePart::Tool {
                id: "tool-1".into(),
                call: ToolCall::Exec {
                    command: "ls".into(),
                },
                is_error: false,
                resolved: true,
                output: None,
                diff: None,
                output_ref: None,
                output_bytes: None,
                diff_ref: None,
                diff_stats: None,
                subagent_ref: None,
                subagent_status: None,
                subagent_tail: None,
            },
        ];
        let chunks = split_parts(&parts);
        assert!(
            chunks.len() >= 3,
            "expected >=3 chunks, got {}",
            chunks.len()
        );
        for chunk in &chunks {
            let bytes: usize = chunk.iter().map(|p| p.byte_len()).sum();
            assert!(bytes <= MSG_INLINE_MAX, "chunk over cap: {bytes}");
        }
        let joined = join_continuations(chunks);
        let text: String = joined
            .iter()
            .filter_map(|p| match p {
                MessagePart::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, big);
        assert!(matches!(joined.last().unwrap(), MessagePart::Tool { .. }));
    }

    #[test]
    fn continuation_ids_are_deterministic() {
        assert_eq!(continuation_id("m1", 1), "m1#c1");
    }

    #[test]
    fn summarize_inlines_small_outputs_and_marks_big_cuts() {
        assert_eq!(summarize_tool_output(""), None);
        assert_eq!(summarize_tool_output("  \n\t\n"), None);
        assert_eq!(summarize_tool_output("one line"), Some("one line".into()));
        assert_eq!(
            summarize_tool_output("\n\nfirst real\nsecond"),
            Some("first real\nsecond".into())
        );
        assert_eq!(
            summarize_tool_output("only line\n\n  \n"),
            Some("only line".into())
        );
        assert_eq!(
            summarize_tool_output("```console\nreal content\n```"),
            Some("real content".into())
        );
        assert_eq!(summarize_tool_output("```\n```"), None);
        let big = format!("```console\nhead line\n{}\n```", "x".repeat(300));
        assert_eq!(summarize_tool_output(&big), Some("head line…".into()));
        let long = "x".repeat(TOOL_OUTPUT_SUMMARY_MAX + 40);
        let summary = summarize_tool_output(&long).unwrap();
        assert_eq!(summary.chars().count(), TOOL_OUTPUT_SUMMARY_MAX + 1);
        assert!(summary.ends_with('…'));
        let wide = "é".repeat(TOOL_OUTPUT_SUMMARY_MAX + 5);
        let summary = summarize_tool_output(&wide).unwrap();
        assert_eq!(summary.chars().count(), TOOL_OUTPUT_SUMMARY_MAX + 1);
    }

    #[test]
    fn diff_stat_counts_line_changes() {
        let stat = diff_stat(&ToolDiff {
            path: "/w/a.rs".into(),
            old_text: Some("a\nb\nc\n".into()),
            new_text: "a\nB\nc\nd\n".into(),
        });
        assert_eq!(stat.path, "/w/a.rs");
        assert_eq!(stat.additions, 2);
        assert_eq!(stat.deletions, 1);
        let stat = diff_stat(&ToolDiff {
            path: "/w/new.rs".into(),
            old_text: None,
            new_text: "one\ntwo\n".into(),
        });
        assert_eq!((stat.additions, stat.deletions), (2, 0));
    }

    #[test]
    fn fold_strips_output_to_summary_and_diff_to_stats() {
        let mut parts = Vec::new();
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolCall {
                id: "t".into(),
                call: ToolCall::Exec {
                    command: "cargo test".into(),
                },
            },
        );
        let full = "running 42 tests\n".repeat(300);
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolResult {
                id: "t".into(),
                is_error: false,
                output: Some(full.clone()),
                diff: Some(ToolDiff {
                    path: "/w/a.rs".into(),
                    old_text: Some("a\n".into()),
                    new_text: "b\n".into(),
                }),
            },
        );
        match &parts[0] {
            MessagePart::Tool {
                output,
                output_bytes,
                diff,
                diff_stats,
                ..
            } => {
                assert_eq!(output.as_deref(), None);
                assert_eq!(*output_bytes, None);
                assert!(diff.is_none(), "inline diff text must not enter the doc");
                let stats = diff_stats.as_ref().unwrap();
                assert_eq!(stats.len(), 1);
                assert_eq!((stats[0].additions, stats[0].deletions), (1, 1));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn sidecar_refs_stamp_once_and_only_where_content_exists() {
        let mut parts = Vec::new();
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolCall {
                id: "t1".into(),
                call: ToolCall::Exec {
                    command: "ls".into(),
                },
            },
        );
        apply_sidecar_refs("chat-9", &mut parts);
        assert!(matches!(
            &parts[0],
            MessagePart::Tool {
                output_ref: None,
                diff_ref: None,
                ..
            }
        ));
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolResult {
                id: "t1".into(),
                is_error: false,
                output: Some("hello".into()),
                diff: Some(ToolDiff {
                    path: "/w/a".into(),
                    old_text: None,
                    new_text: "x\n".into(),
                }),
            },
        );
        apply_sidecar_refs("chat-9", &mut parts);
        apply_sidecar_refs("chat-9", &mut parts);
        match &parts[0] {
            MessagePart::Tool {
                output_ref,
                diff_ref,
                ..
            } => {
                assert_eq!(output_ref.as_deref(), None);
                assert_eq!(diff_ref.as_deref(), Some("chat-9/t1.diff"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn subagent_events_refresh_the_spawn_chip_in_place() {
        use zeron_proto::DoneStatus;
        let mut parts = Vec::new();
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::ToolCall {
                id: "toolu_sub".into(),
                call: ToolCall::Unknown {
                    name: "Agent".into(),
                    input: None,
                },
            },
        );
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::TextDelta {
                    text: "scanning\nfound 3 issues".into(),
                }),
            },
        );
        match &parts[0] {
            MessagePart::Tool {
                subagent_status,
                subagent_tail,
                ..
            } => {
                assert_eq!(*subagent_status, Some(SubagentStatus::Running));
                assert_eq!(*subagent_tail, None);
            }
            other => panic!("{other:?}"),
        }
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::Done {
                    status: DoneStatus::Completed,
                    result: None,
                    error: None,
                    session_id: None,
                }),
            },
        );
        fold_event_into_parts(
            &mut parts,
            &AgentEvent::Subagent {
                parent_tool_use_id: "toolu_sub".into(),
                event: Box::new(AgentEvent::TextDelta {
                    text: "late flush".into(),
                }),
            },
        );
        match &parts[0] {
            MessagePart::Tool {
                subagent_status, ..
            } => assert_eq!(*subagent_status, Some(SubagentStatus::Done)),
            other => panic!("{other:?}"),
        }
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn sidecar_payload_carries_full_texts() {
        assert_eq!(
            sidecar_payload(&AgentEvent::TextDelta { text: "x".into() }),
            None
        );
        assert_eq!(
            sidecar_payload(&AgentEvent::ToolResult {
                id: "t".into(),
                is_error: false,
                output: Some("   \n".into()),
                diff: None,
            }),
            None,
            "blank output uploads nothing"
        );
        let payload = sidecar_payload(&AgentEvent::ToolResult {
            id: "t".into(),
            is_error: true,
            output: Some("full output".into()),
            diff: None,
        })
        .unwrap();
        assert_eq!(payload.part_id, "t");
        assert_eq!(payload.output.as_deref(), Some("full output"));
    }
}
