use std::sync::{Arc, Once};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::{Mutex, mpsc};

use zeron_doc::{MessagePart, MessageRole, MessageStatus, SessionMessageEntry};
use zeron_engine::{EngineCore, HarnessRegistry, SteerOutcome};
use zeron_harness::{Harness, HarnessError, RunControls};
use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SandboxLevel,
    SessionStatus, SteeringMode, ToolCall,
};

const CHAT: &str = "chat-quiesce";
const QUIESCE_MS: u64 = 300;

fn init_quiesce_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        unsafe { std::env::set_var("ZERON_TURN_QUIESCE_MS", QUIESCE_MS.to_string()) };
    });
}

fn run_request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        harness: None,
        model: None,
        reasoning: None,
        model_options: Default::default(),
        cwd: "/tmp".into(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        attachments: Vec::new(),
        resume: None,
    }
}

fn done(status: DoneStatus) -> AgentEvent {
    AgentEvent::Done {
        status,
        result: None,
        error: None,
        session_id: Some("hs-q".into()),
    }
}

fn session_started() -> AgentEvent {
    AgentEvent::SessionStarted {
        harness: HarnessId::Mock,
        model: "mock-1".into(),
        tools: vec![],
        cwd: "/tmp".into(),
        session_id: "hs-q".into(),
        assistant_message_id: "a-q".into(),
    }
}

fn text(t: &str) -> AgentEvent {
    AgentEvent::TextDelta { text: t.into() }
}

struct FeedHarness {
    main_prompt: String,
    feed: Mutex<Option<mpsc::UnboundedReceiver<AgentEvent>>>,
}

#[async_trait]
impl Harness for FeedHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Mock
    }
    fn display_name(&self) -> &str {
        "Feed"
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
        Ok(vec![])
    }
    async fn run(
        &self,
        request: RunRequest,
        mut controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        if request.prompt != self.main_prompt {
            let events = vec![Ok(done(DoneStatus::Completed))];
            return Ok(futures::stream::iter(events).boxed());
        }
        let mut feed = self
            .feed
            .lock()
            .await
            .take()
            .expect("FeedHarness serves the main dispatch once per test");
        let (tx, rx) = mpsc::channel::<Result<AgentEvent, HarnessError>>(64);
        tokio::spawn(async move {
            let mut steering_open = true;
            loop {
                tokio::select! {
                    biased;
                    steer = controls.steering.recv(), if steering_open => match steer {
                        Some(_) => {
                            let boundary = AgentEvent::Steered {
                                assistant_message_id: None,
                                next_assistant_message_id: None,
                            };
                            if tx.send(Ok(boundary)).await.is_err() {
                                return;
                            }
                        }
                        None => steering_open = false,
                    },
                    event = feed.recv() => match event {
                        Some(event) => {
                            if tx.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                        None => return,
                    },
                }
            }
        });
        Ok(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed())
    }
}

struct Rig {
    core: EngineCore,
    feed: mpsc::UnboundedSender<AgentEvent>,
    _dir: tempfile::TempDir,
}

fn assemble(main_prompt: &str) -> Rig {
    init_quiesce_env();
    let (feed, rx) = mpsc::unbounded_channel();
    let registry = HarnessRegistry::new();
    registry.register(Arc::new(FeedHarness {
        main_prompt: main_prompt.into(),
        feed: Mutex::new(Some(rx)),
    }));
    let dir = tempfile::tempdir().unwrap();
    let core = EngineCore::assemble(dir.path(), Arc::new(registry), HarnessId::Mock)
        .expect("engine core assembles");
    Rig {
        core,
        feed,
        _dir: dir,
    }
}

fn status(core: &EngineCore) -> Option<SessionStatus> {
    core.sessions.session_status(CHAT).map(|s| s.status)
}

fn entries(core: &EngineCore) -> Vec<SessionMessageEntry> {
    core.doc_host
        .open(CHAT)
        .ok()
        .and_then(|h| h.doc().read_entries().ok())
        .unwrap_or_default()
}

fn assistant_texts(core: &EngineCore) -> Vec<(String, Option<MessageStatus>)> {
    entries(core)
        .into_iter()
        .filter(|e| e.role == MessageRole::Assistant)
        .map(|e| {
            let text = e
                .parts
                .iter()
                .filter_map(|p| match p {
                    MessagePart::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            (text, e.status)
        })
        .collect()
}

async fn wait_for<F>(mut predicate: F, what: &str)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn parked_late_output_does_not_reopen_working() {
    let rig = assemble("pull waku and benchmark it");
    rig.core
        .sessions
        .dispatch(
            CHAT,
            HarnessId::Mock,
            run_request("pull waku and benchmark it"),
            None,
        )
        .await
        .expect("dispatch");

    rig.feed.send(session_started()).unwrap();
    rig.feed.send(text("Still going, and healthy.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "park after Done",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1200)).await;
    rig.feed
        .send(text("Build finished successfully. Launching Waku."))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        status(&rig.core),
        Some(SessionStatus::Idle),
        "late output must not reopen a completed persistent turn"
    );
    let texts = assistant_texts(&rig.core);
    assert!(
        !texts
            .iter()
            .any(|(t, _)| t.contains("Build finished successfully")),
        "late post-turn output must not create a transcript entry, got {texts:#?}"
    );

    rig.core.sessions.shutdown().await;
}

#[tokio::test]
async fn missing_turn_end_settles_instead_of_working_forever() {
    let rig = assemble("pull waku and benchmark it");
    rig.core
        .sessions
        .dispatch(
            CHAT,
            HarnessId::Mock,
            run_request("pull waku and benchmark it"),
            None,
        )
        .await
        .expect("dispatch");

    rig.feed.send(session_started()).unwrap();
    rig.feed.send(text("Cloned and building.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "park after Done",
    )
    .await;

    let outcome = rig
        .core
        .sessions
        .steer(CHAT, "what about now", None)
        .await
        .expect("steer");
    assert!(matches!(outcome, SteerOutcome::Accepted));
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Working),
        "steer boundary re-arms Working",
    )
    .await;

    rig.feed.send(text("Done — here are the results.")).unwrap();

    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "watchdog settles the turn whose Done was lost",
    )
    .await;
    let texts = assistant_texts(&rig.core);
    assert!(
        texts.iter().any(|(t, s)| {
            t.contains("here are the results") && *s == Some(MessageStatus::Complete)
        }),
        "the lost-Done turn's answer must still finalize in the doc, got {texts:#?}"
    );

    rig.core.sessions.shutdown().await;
}

#[tokio::test]
async fn open_tool_call_never_quiesces() {
    let rig = assemble("run the slow build");
    rig.core
        .sessions
        .dispatch(
            CHAT,
            HarnessId::Mock,
            run_request("run the slow build"),
            None,
        )
        .await
        .expect("dispatch");

    rig.feed.send(session_started()).unwrap();
    rig.feed.send(text("Kicking off the build.")).unwrap();
    rig.feed
        .send(AgentEvent::ToolCall {
            id: "tool-slow".into(),
            call: ToolCall::Exec {
                command: "cargo build --release".into(),
            },
        })
        .unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Working),
        "run starts Working",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(QUIESCE_MS * 4)).await;
    assert_eq!(
        status(&rig.core),
        Some(SessionStatus::Working),
        "an unresolved tool call must never quiesce"
    );

    rig.feed
        .send(AgentEvent::ToolResult {
            id: "tool-slow".into(),
            is_error: false,
            output: None,
            diff: None,
        })
        .unwrap();
    rig.feed.send(text("Build done.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "clean turn end",
    )
    .await;

    rig.core.sessions.shutdown().await;
}

#[tokio::test]
async fn stale_tool_echo_stays_parked() {
    let rig = assemble("do a thing");
    rig.core
        .sessions
        .dispatch(CHAT, HarnessId::Mock, run_request("do a thing"), None)
        .await
        .expect("dispatch");

    rig.feed.send(session_started()).unwrap();
    rig.feed
        .send(AgentEvent::ToolCall {
            id: "tool-echoed".into(),
            call: ToolCall::Exec {
                command: "sleep 600".into(),
            },
        })
        .unwrap();
    rig.feed
        .send(AgentEvent::ToolResult {
            id: "tool-echoed".into(),
            is_error: false,
            output: None,
            diff: None,
        })
        .unwrap();
    rig.feed
        .send(text("Started it in the background."))
        .unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "park after Done",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1200)).await;
    rig.feed
        .send(AgentEvent::ToolCall {
            id: "tool-echoed".into(),
            call: ToolCall::Exec {
                command: "sleep 600".into(),
            },
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        status(&rig.core),
        Some(SessionStatus::Idle),
        "a stale tool echo must not resume a parked session"
    );

    rig.core.sessions.shutdown().await;
}
