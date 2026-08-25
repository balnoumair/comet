use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;

use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SteeringMode,
    UserInputQuestion,
};

use crate::{Harness, HarnessError, RunControls};

pub struct MockHarness {
    pub script: Vec<AgentEvent>,
}

fn question_script() -> Vec<UserInputQuestion> {
    vec![
        UserInputQuestion {
            id: "q-sync".into(),
            header: "Question".into(),
            question: "Which sync strategy should the rewrite use?".into(),
            options: vec![
                "Poll the doc host every 120ms".into(),
                "Event-driven fold with coalesced commits".into(),
                "Hybrid: event-driven with a polling fallback".into(),
            ],
            multi_select: false,
        },
        UserInputQuestion {
            id: "q-gates".into(),
            header: "Question".into(),
            question: "Which suites should gate the merge?".into(),
            options: vec![
                "Unit tests".into(),
                "End-to-end (two-device)".into(),
                "Golden screenshots".into(),
            ],
            multi_select: true,
        },
    ]
}

#[async_trait]
impl Harness for MockHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Mock
    }
    fn display_name(&self) -> &str {
        "Mock"
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::StepBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[ReasoningLevel::Medium]
    }
    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        Ok(vec![
            Model {
                id: "mock-1".into(),
                label: "Mock 1".into(),
                description: None,
                reasoning_levels: vec![ReasoningLevel::Medium],
                options: vec![],
            },
            Model {
                id: "mock-fable-5".into(),
                label: "Fable 5".into(),
                description: None,
                reasoning_levels: vec![
                    ReasoningLevel::Low,
                    ReasoningLevel::Medium,
                    ReasoningLevel::High,
                    ReasoningLevel::XHigh,
                ],
                options: vec![],
            },
        ])
    }
    async fn run(
        &self,
        _request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let delay_ms = std::env::var("ZERON_MOCK_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let delay = std::time::Duration::from_millis(delay_ms);

        let question_mode = std::env::var("ZERON_MOCK_QUESTION")
            .ok()
            .is_some_and(|v| !v.is_empty() && v != "0");
        if question_mode {
            let request_input = controls.request_input;
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<AgentEvent>();
            tokio::spawn(async move {
                let pause = if delay_ms == 0 {
                    std::time::Duration::from_millis(50)
                } else {
                    delay
                };
                tokio::time::sleep(pause).await;
                let _ = tx.send(AgentEvent::TextDelta {
                    text:
                        "Before I wire the reconciliation path I need two decisions from you.\n\n"
                            .into(),
                });
                tokio::time::sleep(pause).await;
                let answers = request_input(question_script()).await.unwrap_or_default();
                let picked: Vec<String> = answers
                    .iter()
                    .flat_map(|a| a.labels.iter().cloned())
                    .collect();
                tokio::time::sleep(pause).await;
                let _ = tx.send(AgentEvent::TextDelta {
                    text: format!(
                        "Locked in: **{}**. Proceeding with the plan.",
                        if picked.is_empty() {
                            "your defaults".to_string()
                        } else {
                            picked.join("**, **")
                        }
                    ),
                });
                let _ = tx.send(AgentEvent::Done {
                    status: DoneStatus::Completed,
                    result: None,
                    error: None,
                    session_id: None,
                });
            });
            let stream = futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|event| (Ok(event), rx))
            });
            return Ok(stream.boxed());
        }

        let repeat = std::env::var("ZERON_MOCK_REPEAT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1);
        let mock_error = std::env::var("ZERON_MOCK_ERROR")
            .ok()
            .is_some_and(|v| !v.is_empty() && v != "0");
        let mock_table = std::env::var("ZERON_MOCK_TABLE")
            .ok()
            .is_some_and(|v| !v.is_empty() && v != "0");
        let done_ix = self
            .script
            .iter()
            .position(|e| matches!(e, AgentEvent::Done { .. }))
            .unwrap_or(self.script.len());
        let (body, tail) = self.script.split_at(done_ix);
        let error_event = mock_error.then(|| AgentEvent::Error {
            message: "Claude usage limit reached — try again after the limit resets.".into(),
        });
        let mock_code = std::env::var("ZERON_MOCK_CODE")
            .ok()
            .is_some_and(|v| !v.is_empty() && v != "0");
        let code_event = mock_code.then(|| AgentEvent::TextDelta {
            text: concat!(
                "\n### Code check\n\n",
                "The `fold_event_into_parts` helper feeds `writer.sync` on a `120ms` cadence:\n\n",
                "```rust\n",
                "// Fold one event into the accumulated parts.\n",
                "pub fn fold(mut acc: Vec<Part>, event: &AgentEvent) -> Vec<Part> {\n",
                "    let label = \"delta\";\n",
                "    if acc.len() > 128 {\n",
                "        acc.truncate(64); // keep the tail hot\n",
                "    }\n",
                "    acc\n",
                "}\n",
                "```\n\n",
                "```ts\n",
                "// Subscribe and fold on the client.\n",
                "const room = await connect(\"wss://mesh.local\", { retries: 3 });\n",
                "export function fold(parts: Part[], event: AgentEvent): Part[] {\n",
                "    return event.kind === \"delta\" ? [...parts, event] : parts;\n",
                "}\n",
                "```\n\n",
            )
            .into(),
        });
        let table_event = mock_table.then(|| AgentEvent::TextDelta {
            text: "\n### Table check\n\n\
                | Column A | Column B | Column C |\n\
                |---|---|---|\n\
                | a1 | b1 | c1 |\n\
                | a2 | b2 | c2 |\n\n\
                And a wide, uneven one:\n\n\
                | Stage | What happens | p95 |\n\
                |:--|:--|--:|\n\
                | Fold | Events fold into parts and diff into the Loro doc on a 120ms coalesced commit cadence, keeping the oplog RLE-merged across devices | 4.2ms |\n\
                | Sync | Session-room fan-out | 18ms |\n\n"
                .into(),
        });
        let mock_mend = std::env::var("ZERON_MOCK_MEND")
            .ok()
            .is_some_and(|v| !v.is_empty() && v != "0");
        let mend_event = mock_mend.then(|| AgentEvent::TextDelta {
            text: concat!(
                "\n### Streaming mend check\n\n",
                "Inline styles hold while text arrives: **bold stays bold**, ",
                "*italic stays italic*, `code stays code`, and ~~this stays struck~~.\n\n",
                "- **Fold** — parts diff into the [Loro doc](https://loro.dev) on a 120ms cadence\n",
                "- **Persist** — commits stay in the local session document\n",
                "- **Paint** — the [display tree](https://github.com/pulldown-cmark/pulldown-cmark) mends hanging markers in the last block only\n\n",
                "Links above never flash their URLs, and closing markers never reflow the paragraph.\n",
            )
            .into(),
        });
        let mock_subagent = std::env::var("ZERON_MOCK_SUBAGENT")
            .ok()
            .is_some_and(|v| !v.is_empty() && v != "0");
        let subagent_events = mock_subagent
            .then(|| {
                let tag = |parent: &str, event: AgentEvent| AgentEvent::Subagent {
                    parent_tool_use_id: parent.into(),
                    event: Box::new(event),
                };
                let spawn = |id: &str, description: &str, prompt: &str| AgentEvent::ToolCall {
                    id: id.into(),
                    call: zeron_proto::ToolCall::Unknown {
                        name: format!("Agent: {description}"),
                        input: Some(serde_json::json!({
                            "description": description,
                            "prompt": prompt,
                        })),
                    },
                };
                let resolve = |id: &str| AgentEvent::ToolResult {
                    id: id.into(),
                    is_error: false,
                    output: None,
                    diff: None,
                };
                let done = AgentEvent::Done {
                    status: DoneStatus::Completed,
                    result: None,
                    error: None,
                    session_id: None,
                };
                vec![
                    AgentEvent::TextDelta {
                        text: "\n### Subagent check\n\nFanning out two scouts before the fold rewrite.\n\n".into(),
                    },
                    spawn(
                        "mock-sub-1",
                        "Audit the fold path",
                        "Read crates/doc and list every call site of fold_event_into_parts, checking each holds the byte cap.",
                    ),
                    spawn(
                        "mock-sub-2",
                        "Verify the commit cadence",
                        "Measure the 120ms coalesced commit cadence under a scripted delta burst.",
                    ),
                    tag(
                        "mock-sub-1",
                        AgentEvent::UserMessage {
                            text: "Read crates/doc and list every call site of fold_event_into_parts, checking each holds the byte cap.".into(),
                        },
                    ),
                    tag(
                        "mock-sub-2",
                        AgentEvent::UserMessage {
                            text: "Measure the 120ms coalesced commit cadence under a scripted delta burst.".into(),
                        },
                    ),
                    tag(
                        "mock-sub-1",
                        AgentEvent::TextDelta {
                            text: "Scanning `crates/doc` for fold call sites.\n\n".into(),
                        },
                    ),
                    tag(
                        "mock-sub-1",
                        AgentEvent::ToolCall {
                            id: "sub1-grep".into(),
                            call: zeron_proto::ToolCall::Exec {
                                command: "grep -rn fold_event_into_parts crates".into(),
                            },
                        },
                    ),
                    tag("mock-sub-1", resolve("sub1-grep")),
                    tag(
                        "mock-sub-1",
                        AgentEvent::TextDelta {
                            text: "Three call sites: the live fold, the rebuild, and the subagent sink — every one applies the byte cap before persisting.".into(),
                        },
                    ),
                    resolve("mock-sub-1"),
                    tag("mock-sub-1", done.clone()),
                    tag(
                        "mock-sub-2",
                        AgentEvent::TextDelta {
                            text: "Driving a 2k-delta burst through the writer.\n\n".into(),
                        },
                    ),
                    tag(
                        "mock-sub-2",
                        AgentEvent::ToolCall {
                            id: "sub2-burst".into(),
                            call: zeron_proto::ToolCall::Exec {
                                command: "cargo test -p zeron-doc cadence_burst -- --nocapture".into(),
                            },
                        },
                    ),
                    resolve("mock-sub-2"),
                    tag("mock-sub-2", resolve("sub2-burst")),
                    tag(
                        "mock-sub-2",
                        AgentEvent::TextDelta {
                            text: "Commits land on the 120ms cadence; no commit carried more than one burst.".into(),
                        },
                    ),
                    tag(
                        "mock-sub-2",
                        AgentEvent::UserMessage {
                            text: "Also verify the cadence holds while a steer lands mid-burst.".into(),
                        },
                    ),
                    tag(
                        "mock-sub-2",
                        AgentEvent::TextDelta {
                            text: "Re-running with a mid-burst steer injected.\n\n".into(),
                        },
                    ),
                    tag(
                        "mock-sub-2",
                        AgentEvent::ToolCall {
                            id: "sub2-steer-burst".into(),
                            call: zeron_proto::ToolCall::Exec {
                                command: "cargo test -p zeron-doc cadence_steer -- --nocapture"
                                    .into(),
                            },
                        },
                    ),
                    tag("mock-sub-2", resolve("sub2-steer-burst")),
                    tag(
                        "mock-sub-2",
                        AgentEvent::TextDelta {
                            text: "Watching the commit log while the burst drains: ".into(),
                        },
                    ),
                    tag("mock-sub-2", AgentEvent::TextDelta { text: "batch 1 clean, ".into() }),
                    tag("mock-sub-2", AgentEvent::TextDelta { text: "batch 2 clean, ".into() }),
                    tag("mock-sub-2", AgentEvent::TextDelta { text: "batch 3 clean, ".into() }),
                    tag("mock-sub-2", AgentEvent::TextDelta { text: "batch 4 clean, ".into() }),
                    tag("mock-sub-2", AgentEvent::TextDelta { text: "batch 5 clean — ".into() }),
                    tag(
                        "mock-sub-2",
                        AgentEvent::TextDelta {
                            text: "every window under 120ms.\n\nSteer landed between commits; the cadence held.".into(),
                        },
                    ),
                    tag("mock-sub-2", done),
                ]
            })
            .into_iter()
            .flatten();
        let code_tool_events = mock_code
            .then(|| {
                [
                    AgentEvent::ToolCall {
                        id: "mock-code-tool".into(),
                        call: zeron_proto::ToolCall::Exec {
                            command: "set -e\nfixture_in_original=0\ngrep -rn \"veil\" crates/engine/src | wc -l".into(),
                        },
                    },
                    AgentEvent::ToolResult {
                        id: "mock-code-tool".into(),
                        is_error: false,
                        output: None,
                        diff: None,
                    },
                ]
            })
            .into_iter()
            .flatten();
        let events: Vec<Result<AgentEvent, HarnessError>> = body
            .iter()
            .cycle()
            .take(body.len() * repeat)
            .cloned()
            .chain(subagent_events)
            .chain(code_tool_events)
            .chain(code_event)
            .chain(table_event)
            .chain(mend_event)
            .chain(error_event)
            .chain(tail.iter().cloned())
            .map(Ok)
            .collect();
        let chunk_chars = std::env::var("ZERON_MOCK_CHARS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0);
        let events: Vec<Result<AgentEvent, HarnessError>> = match chunk_chars {
            None => events,
            Some(n) => events
                .into_iter()
                .flat_map(|event| match event {
                    Ok(AgentEvent::TextDelta { text }) => {
                        let chars: Vec<char> = text.chars().collect();
                        chars
                            .chunks(n)
                            .map(|c| {
                                Ok(AgentEvent::TextDelta {
                                    text: c.iter().collect(),
                                })
                            })
                            .collect::<Vec<_>>()
                    }
                    other => vec![other],
                })
                .collect(),
        };
        if delay_ms == 0 {
            return Ok(futures::stream::iter(events).boxed());
        }
        Ok(futures::stream::iter(events)
            .then(move |event| async move {
                tokio::time::sleep(delay).await;
                event
            })
            .boxed())
    }
}
