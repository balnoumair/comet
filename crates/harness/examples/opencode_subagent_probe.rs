use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use zeron_harness::{AcpHarness, CancellationToken, Harness, RunControls};
use zeron_proto::{AgentEvent, RunRequest, SandboxLevel, UserInputAnswer};

#[tokio::main]
async fn main() {
    let cwd = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/probe-opencode-viz".into());
    std::fs::create_dir_all(&cwd).unwrap();
    let (_steer_tx, steering) = mpsc::channel(8);
    let controls = RunControls {
        request_input: Box::new(move |questions| {
            let (tx, rx) = oneshot::channel();
            let answers: Vec<UserInputAnswer> = questions
                .iter()
                .map(|q| UserInputAnswer {
                    question_id: q.id.clone(),
                    labels: q.options.first().cloned().into_iter().collect(),
                })
                .collect();
            let _ = tx.send(answers);
            rx
        }),
        steering,
        interrupt: CancellationToken::new(),
    };
    let prompt = std::env::args().nth(2).unwrap_or_else(|| {
        "Use the task tool to launch ONE general subagent with description \
         'Viz probe' and prompt: 'Run `echo viz-probe-ok`, then reply with the \
         word finished.'. Tell me its result."
            .into()
    });
    let request = RunRequest {
        prompt,
        harness: None,
        model: None,
        reasoning: None,
        model_options: serde_json::Map::new(),
        cwd,
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        attachments: Vec::new(),
        resume: None,
    };
    let mut stream = AcpHarness::opencode()
        .run(request, controls)
        .await
        .expect("run starts");
    let mut tagged = 0u32;
    let mut parent_done = false;
    loop {
        let ev = match tokio::time::timeout(std::time::Duration::from_secs(90), stream.next()).await
        {
            Ok(Some(ev)) => ev,
            Ok(None) => break,
            Err(_) => {
                eprintln!("--- timed out waiting (parent_done={parent_done}, tagged={tagged})");
                std::process::exit(2);
            }
        };
        match ev {
            Ok(AgentEvent::Subagent {
                parent_tool_use_id,
                event,
            }) => {
                tagged += 1;
                eprintln!("SUB[{parent_tool_use_id}] {event:?}");
                if matches!(*event, AgentEvent::Done { .. }) && parent_done {
                    eprintln!("--- tagged events total: {tagged}");
                    std::process::exit(0);
                }
            }
            Ok(AgentEvent::Done { status, .. }) => {
                parent_done = true;
                eprintln!("EV Done({status:?}) [parent]");
            }
            Ok(AgentEvent::TextDelta { text }) => eprintln!("TXT {}", text.trim_end()),
            Ok(AgentEvent::ReasoningDelta { .. }) => {}
            Ok(other) => eprintln!("EV {other:?}"),
            Err(e) => eprintln!("ERR {e}"),
        }
    }
    eprintln!("--- stream ended; tagged events total: {tagged}");
}
