mod normalize;
mod subagent;
mod subagent_opencode;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tokio::io::AsyncBufReadExt;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ModelOption, ModelOptionChoice, ReasoningLevel,
    RunRequest, SlashCommand, SteeringMode, UserInputAnswer, UserInputQuestion,
};

use crate::jsonrpc::{Incoming, RpcClient};
use crate::{Harness, HarnessError, RunControls, Signal, send_signal, shutdown_child};
use normalize::{map_update, parse_commands, preferred_allow_option};
use subagent::SubagentTracker;
use subagent_opencode::OpencodeTracker;

const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_MODEL_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_OPENCODE_STARTUP_TIMEOUT: Duration = Duration::from_secs(300);
const OPENCODE_STARTUP_TIMEOUT_ENV: &str = "ZERON_OPENCODE_STARTUP_TIMEOUT_SECS";

fn opencode_startup_timeout() -> Duration {
    std::env::var(OPENCODE_STARTUP_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_OPENCODE_STARTUP_TIMEOUT)
}

struct AcpAgentSpec {
    id: HarnessId,
    display_name: &'static str,
    executable: &'static str,
    env_override: &'static str,
    args: &'static [&'static str],
    npm_package: Option<&'static str>,
    extra_paths: fn() -> Vec<PathBuf>,
    cli_executable: &'static str,
    cli_extra_paths: fn() -> Vec<PathBuf>,
    install_hint: &'static str,
    models: fn() -> Vec<Model>,
    steering_mode: SteeringMode,
    reasoning_levels: &'static [ReasoningLevel],
    prompt_transform: fn(Option<ReasoningLevel>, &str) -> String,
    effort_values: fn(Option<ReasoningLevel>, Option<&str>) -> Vec<&'static str>,
    ladder_extras: &'static [ReasoningLevel],
    prompt_complete_extension: bool,
    prompt_stall: Option<Duration>,
    stall_hint: &'static str,
    http_sidecar: bool,
}

fn identity_transform(_reasoning: Option<ReasoningLevel>, text: &str) -> String {
    text.to_owned()
}

fn free_localhost_port() -> Option<u16> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .ok()
}

pub(crate) fn find_on_paths(exe: &str, extra: Vec<PathBuf>) -> Option<PathBuf> {
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
    candidates.extend(extra);
    candidates.extend(
        crate::node_version_manager_bins()
            .into_iter()
            .map(|d| d.join(exe)),
    );
    candidates.into_iter().find(|p| p.exists())
}

fn default_effort_values(
    reasoning: Option<ReasoningLevel>,
    _model: Option<&str>,
) -> Vec<&'static str> {
    let Some(level) = reasoning else {
        return Vec::new();
    };
    match level {
        ReasoningLevel::Minimal => vec!["minimal", "low"],
        ReasoningLevel::Low => vec!["low", "minimal"],
        ReasoningLevel::Medium => vec!["medium"],
        ReasoningLevel::High => vec!["high"],
        ReasoningLevel::XHigh => vec!["xhigh", "x-high", "high"],
        ReasoningLevel::Max => vec!["max", "xhigh", "high"],
        ReasoningLevel::Ultra | ReasoningLevel::Ultracode | ReasoningLevel::Ultrathink => {
            vec!["ultra", "max", "high"]
        }
    }
}

fn npm_global_paths(exe: &'static str) -> fn() -> Vec<PathBuf> {
    match exe {
        "pi-acp" => || npm_global_bins("pi-acp"),
        _ => || Vec::new(),
    }
}

fn npm_global_bins(exe: &str) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        dirs.push(home.join(".local").join("bin").join(exe));
        dirs.push(home.join(".npm-global").join("bin").join(exe));
    }
    dirs.push(PathBuf::from("/opt/homebrew/bin").join(exe));
    dirs.push(PathBuf::from("/usr/local/bin").join(exe));
    dirs
}

fn grok_install_paths() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        dirs.push(home.join(".local").join("bin").join("grok"));
        dirs.push(home.join(".grok").join("bin").join("grok"));
        dirs.push(home.join(".npm-global").join("bin").join("grok"));
    }
    dirs.push(PathBuf::from("/opt/homebrew/bin/grok"));
    dirs.push(PathBuf::from("/usr/local/bin/grok"));
    dirs
}

fn grok_spec() -> AcpAgentSpec {
    AcpAgentSpec {
        id: HarnessId::Grok,
        display_name: "Grok",
        executable: "grok",
        env_override: "GROK_EXECUTABLE",
        args: &["--no-auto-update", "agent", "--no-leader", "stdio"],
        npm_package: Some("@xai-official/grok@1.0.4"),
        extra_paths: grok_install_paths,
        cli_executable: "grok",
        cli_extra_paths: grok_install_paths,
        install_hint: "grok (searched PATH, the login shell's PATH, ~/.local/bin, \
             ~/.grok/bin, ~/.npm-global/bin, /opt/homebrew/bin, /usr/local/bin, and \
             fnm/nvm/volta/pnpm/bun install dirs; install with \
             `curl -fsSL https://x.ai/cli/install.sh | bash` or \
             `npm install -g @xai-official/grok`; set GROK_EXECUTABLE to override)",
        models: || {
            vec![Model {
                id: "grok-4.5".into(),
                label: "Grok 4.5".into(),
                description: Some("xAI's coding model — 500k context".into()),
                reasoning_levels: vec![
                    ReasoningLevel::Low,
                    ReasoningLevel::Medium,
                    ReasoningLevel::High,
                ],
                options: Vec::new(),
            }]
        },
        steering_mode: SteeringMode::TurnBoundary,
        reasoning_levels: &[
            ReasoningLevel::Low,
            ReasoningLevel::Medium,
            ReasoningLevel::High,
        ],
        prompt_transform: identity_transform,
        effort_values: default_effort_values,
        ladder_extras: &[],
        prompt_complete_extension: true,
        prompt_stall: Some(Duration::from_secs(30)),
        stall_hint: "The agent process is likely wedged — a stale shared leader \
             process or a hung startup check; zeron launches it with --no-leader \
             and --no-auto-update to avoid both.",
        http_sidecar: false,
    }
}

fn hermes_install_paths() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        dirs.push(home.join(".local").join("bin").join("hermes"));
        dirs.push(home.join(".hermes").join("bin").join("hermes"));
    }
    dirs.push(PathBuf::from("/opt/homebrew/bin/hermes"));
    dirs.push(PathBuf::from("/usr/local/bin/hermes"));
    dirs
}

fn hermes_spec() -> AcpAgentSpec {
    AcpAgentSpec {
        id: HarnessId::Hermes,
        display_name: "Hermes",
        executable: "hermes",
        env_override: "HERMES_EXECUTABLE",
        args: &["acp"],
        npm_package: None,
        extra_paths: hermes_install_paths,
        cli_executable: "hermes",
        cli_extra_paths: hermes_install_paths,
        install_hint: "hermes (searched PATH, the login shell's PATH, ~/.local/bin, \
             ~/.hermes/bin, /opt/homebrew/bin, /usr/local/bin, and fnm/nvm/volta/pnpm/bun \
             install dirs; install with \
             `curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash`, then \
             `cd ~/.hermes/hermes-agent && uv pip install -e '.[acp]'` for the ACP \
             server; set HERMES_EXECUTABLE to override)",
        models: || {
            vec![
                Model {
                    id: "hermes-4-405b".into(),
                    label: "Hermes 4 405B".into(),
                    description: Some("Nous Research's hybrid-reasoning flagship".into()),
                    reasoning_levels: Vec::new(),
                    options: Vec::new(),
                },
                Model {
                    id: "hermes-4-70b".into(),
                    label: "Hermes 4 70B".into(),
                    description: Some("Faster Hermes 4 — same post-training, 70B".into()),
                    reasoning_levels: Vec::new(),
                    options: Vec::new(),
                },
            ]
        },
        steering_mode: SteeringMode::TurnBoundary,
        reasoning_levels: &[],
        prompt_transform: identity_transform,
        effort_values: default_effort_values,
        ladder_extras: &[],
        prompt_complete_extension: false,
        prompt_stall: None,
        stall_hint: "The agent process is likely wedged.",
        http_sidecar: false,
    }
}

fn pi_spec() -> AcpAgentSpec {
    AcpAgentSpec {
        id: HarnessId::Pi,
        display_name: "Pi",
        executable: "pi-acp",
        env_override: "PI_ACP_EXECUTABLE",
        args: &[],
        npm_package: Some("pi-acp@0.0.33"),
        extra_paths: npm_global_paths("pi-acp"),
        cli_executable: "pi",
        cli_extra_paths: || npm_global_bins("pi"),
        install_hint: "pi-acp (searched PATH, the login shell's PATH, npm global bins, \
             and fnm/nvm/volta/pnpm/bun install dirs; zeron installs the pinned \
             pi-acp automatically when npm is available — the pi CLI itself is \
             still required, `npm install -g --ignore-scripts \
             @earendil-works/pi-coding-agent`; set PI_ACP_EXECUTABLE to override)",
        models: || {
            vec![Model {
                id: "default".into(),
                label: "pi default".into(),
                description: Some("Runs the model configured in pi (`pi` settings)".into()),
                reasoning_levels: vec![
                    ReasoningLevel::Minimal,
                    ReasoningLevel::Low,
                    ReasoningLevel::Medium,
                    ReasoningLevel::High,
                    ReasoningLevel::XHigh,
                    ReasoningLevel::Max,
                ],
                options: Vec::new(),
            }]
        },
        steering_mode: SteeringMode::TurnBoundary,
        reasoning_levels: &[
            ReasoningLevel::Minimal,
            ReasoningLevel::Low,
            ReasoningLevel::Medium,
            ReasoningLevel::High,
            ReasoningLevel::XHigh,
            ReasoningLevel::Max,
        ],
        prompt_transform: identity_transform,
        effort_values: default_effort_values,
        ladder_extras: &[],
        prompt_complete_extension: false,
        prompt_stall: None,
        stall_hint: "The agent process is likely wedged.",
        http_sidecar: false,
    }
}

fn opencode_install_paths() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        dirs.push(home.join(".opencode").join("bin").join("opencode"));
        dirs.push(home.join(".local").join("bin").join("opencode"));
        dirs.push(home.join(".npm-global").join("bin").join("opencode"));
    }
    dirs.push(PathBuf::from("/opt/homebrew/bin/opencode"));
    dirs.push(PathBuf::from("/usr/local/bin/opencode"));
    dirs
}

fn opencode_spec() -> AcpAgentSpec {
    AcpAgentSpec {
        id: HarnessId::Opencode,
        display_name: "OpenCode",
        executable: "opencode",
        env_override: "OPENCODE_EXECUTABLE",
        args: &["acp"],
        npm_package: None,
        extra_paths: opencode_install_paths,
        cli_executable: "opencode",
        cli_extra_paths: opencode_install_paths,
        install_hint: "opencode (searched PATH, the login shell's PATH, ~/.opencode/bin, \
             ~/.local/bin, ~/.npm-global/bin, /opt/homebrew/bin, /usr/local/bin, and \
             fnm/nvm/volta/pnpm/bun install dirs; install with \
             `curl -fsSL https://opencode.ai/install | bash` or \
             `npm install -g opencode-ai`, then `opencode auth login`; set \
             OPENCODE_EXECUTABLE to override)",
        models: || {
            vec![
                Model {
                    id: "opencode/big-pickle".into(),
                    label: "Big Pickle".into(),
                    description: Some("OpenCode Zen's flagship coding model".into()),
                    reasoning_levels: Vec::new(),
                    options: Vec::new(),
                },
                Model {
                    id: "opencode/deepseek-v4-flash-free".into(),
                    label: "DeepSeek V4 Flash (free)".into(),
                    description: Some("Free tier on OpenCode Zen".into()),
                    reasoning_levels: Vec::new(),
                    options: Vec::new(),
                },
            ]
        },
        steering_mode: SteeringMode::TurnBoundary,
        reasoning_levels: &[
            ReasoningLevel::Low,
            ReasoningLevel::Medium,
            ReasoningLevel::High,
            ReasoningLevel::XHigh,
            ReasoningLevel::Max,
        ],
        prompt_transform: identity_transform,
        effort_values: default_effort_values,
        ladder_extras: &[],
        prompt_complete_extension: false,
        prompt_stall: Some(Duration::from_secs(60)),
        stall_hint: "The model provider is likely unreachable or rejecting \
             requests — opencode retries these silently and never reports the \
             failure. Check the model/provider setup (`opencode auth list`, \
             opencode.json) or the opencode log \
             (~/.local/share/opencode/log).",
        http_sidecar: true,
    }
}

pub fn prewarm_managed_adapters() {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    for spec in [grok_spec(), pi_spec()] {
        let Some(pkg) = spec.npm_package else {
            continue;
        };
        let pin = crate::adapter_install::NpmPin::parse(pkg);
        if find_on_paths(spec.executable, (spec.extra_paths)()).is_some()
            || crate::adapter_install::installed_entry(&pin, spec.executable).is_some()
            || find_on_paths(spec.cli_executable, (spec.cli_extra_paths)()).is_none()
            || crate::adapter_install::find_npm().is_none()
        {
            continue;
        }
        let (bin_name, display_name) = (spec.executable, spec.display_name);
        handle.spawn(async move {
            match crate::adapter_install::ensure_installed(pin, bin_name, display_name).await {
                Ok(entry) => tracing::info!(
                    target: "zeron_harness::adapter_install",
                    adapter = %entry.display(),
                    "prewarmed {display_name} ACP adapter"
                ),
                Err(e) => tracing::warn!(
                    target: "zeron_harness::adapter_install",
                    "prewarm of the {display_name} ACP adapter failed: {e}"
                ),
            }
        });
    }
}

enum Launch {
    Program(PathBuf, Vec<String>),
    Managed {
        pin: crate::adapter_install::NpmPin,
        bin_name: &'static str,
        args: Vec<String>,
    },
}

pub struct AcpHarness {
    spec: AcpAgentSpec,
    executable: Option<PathBuf>,
    sessions_root: Option<PathBuf>,
    interrupt_grace: Duration,
    kill_grace: Duration,
    handshake_timeout: Duration,
    model_discovery_timeout: Duration,
    commands: tokio::sync::OnceCell<Vec<SlashCommand>>,
    models_cache: tokio::sync::OnceCell<Vec<Model>>,
    models_probe: tokio::sync::Mutex<()>,
}

impl AcpHarness {
    fn with_spec(spec: AcpAgentSpec) -> Self {
        Self {
            spec,
            executable: None,
            sessions_root: None,
            interrupt_grace: Duration::from_secs(2),
            kill_grace: Duration::from_secs(3),
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            model_discovery_timeout: DEFAULT_MODEL_DISCOVERY_TIMEOUT,
            commands: tokio::sync::OnceCell::new(),
            models_cache: tokio::sync::OnceCell::new(),
            models_probe: tokio::sync::Mutex::new(()),
        }
    }

    pub fn grok() -> Self {
        Self::with_spec(grok_spec())
    }

    pub fn hermes() -> Self {
        Self::with_spec(hermes_spec())
    }

    pub fn pi() -> Self {
        Self::with_spec(pi_spec())
    }

    pub fn opencode() -> Self {
        let startup_timeout = opencode_startup_timeout();
        let mut harness = Self::with_spec(opencode_spec());
        harness.handshake_timeout = startup_timeout;
        harness.model_discovery_timeout = startup_timeout;
        harness
    }

    pub fn with_executable(mut self, path: impl Into<PathBuf>) -> Self {
        self.executable = Some(path.into());
        self
    }

    #[doc(hidden)]
    pub fn with_sessions_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.sessions_root = Some(root.into());
        self
    }

    pub fn with_graces(mut self, interrupt_grace: Duration, kill_grace: Duration) -> Self {
        self.interrupt_grace = interrupt_grace;
        self.kill_grace = kill_grace;
        self
    }

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    #[doc(hidden)]
    pub fn with_model_discovery_timeout(mut self, timeout: Duration) -> Self {
        self.model_discovery_timeout = timeout;
        self
    }

    #[doc(hidden)]
    pub fn launch_program(&self) -> Result<PathBuf, HarnessError> {
        match self.resolve_launch()? {
            Launch::Program(program, _) => Ok(program),
            Launch::Managed { pin, bin_name, .. } => {
                match crate::adapter_install::installed_entry(&pin, bin_name) {
                    Some(entry) => Ok(entry),
                    None => crate::adapter_install::find_npm()
                        .ok_or_else(|| HarnessError::NotInstalled(self.spec.install_hint.into())),
                }
            }
        }
    }

    fn resolve_launch(&self) -> Result<Launch, HarnessError> {
        let spec_args: Vec<String> = self.spec.args.iter().map(|a| a.to_string()).collect();
        if let Some(p) = &self.executable {
            return Ok(Launch::Program(p.clone(), spec_args));
        }
        if let Some(p) = std::env::var_os(self.spec.env_override)
            && !p.is_empty()
        {
            return Ok(Launch::Program(PathBuf::from(p), spec_args));
        }
        if let Some(found) = find_on_paths(self.spec.executable, (self.spec.extra_paths)()) {
            return Ok(Launch::Program(found, spec_args));
        }
        if let Some(pkg) = self.spec.npm_package {
            let pin = crate::adapter_install::NpmPin::parse(pkg);
            if crate::adapter_install::installed_entry(&pin, self.spec.executable).is_some()
                || crate::adapter_install::find_npm().is_some()
            {
                return Ok(Launch::Managed {
                    pin,
                    bin_name: self.spec.executable,
                    args: spec_args,
                });
            }
        }
        Err(HarnessError::NotInstalled(self.spec.install_hint.into()))
    }

    async fn resolve_program(
        &self,
        block_on_install: bool,
    ) -> Result<(PathBuf, Vec<String>), HarnessError> {
        match self.resolve_launch()? {
            Launch::Program(program, args) => Ok((program, args)),
            Launch::Managed {
                pin,
                bin_name,
                args,
            } => {
                let entry = match crate::adapter_install::installed_entry(&pin, bin_name) {
                    Some(entry) => entry,
                    None if block_on_install => {
                        crate::adapter_install::ensure_installed(
                            pin,
                            bin_name,
                            self.spec.display_name,
                        )
                        .await?
                    }
                    None => {
                        let display_name = self.spec.display_name;
                        tokio::spawn(async move {
                            if let Err(e) = crate::adapter_install::ensure_installed(
                                pin,
                                bin_name,
                                display_name,
                            )
                            .await
                            {
                                tracing::warn!(
                                    target: "zeron_harness::adapter_install",
                                    "background adapter install failed: {e}"
                                );
                            }
                        });
                        return Err(HarnessError::Protocol(format!(
                            "{} adapter is installing in the background",
                            self.spec.display_name
                        )));
                    }
                };
                let (program, mut node_args) = crate::adapter_install::launch_for_entry(&entry)?;
                node_args.extend(args);
                Ok((program, node_args))
            }
        }
    }

    async fn spawn_agent(
        &self,
        cwd: Option<&str>,
        block_on_install: bool,
        extra_args: &[String],
    ) -> Result<(Child, crate::StderrTail), HarnessError> {
        let (exe, args) = self.resolve_program(block_on_install).await?;
        let mut cmd = Command::new(&exe);
        cmd.args(args);
        cmd.args(extra_args);
        crate::compose_child_path(&mut cmd, &exe);
        if let Some(cwd) = cwd.filter(|c| !c.is_empty()) {
            cmd.current_dir(cwd);
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
        let stderr_tail = crate::StderrTail::default();
        if let Some(stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "zeron_harness::acp", "stderr: {line}");
                    tail.push(&line);
                }
            });
        }
        Ok((child, stderr_tail))
    }

    async fn discover_commands(&self) -> Result<Vec<SlashCommand>, HarnessError> {
        let (mut child, _stderr) = self.spawn_agent(None, false, &[]).await?;
        let (client, mut incoming) = match (child.stdin.take(), child.stdout.take()) {
            (Some(stdin), Some(stdout)) => RpcClient::new(stdin, stdout),
            _ => {
                shutdown_child(&mut child, self.kill_grace).await;
                return Err(HarnessError::Protocol("agent child has no stdio".into()));
            }
        };
        let discovery = async {
            let init = client
                .request("initialize", initialize_params(self.spec.id))
                .await?;
            let mut commands = scan_available_commands(&init);
            if commands.is_empty() {
                let cwd = std::env::var("HOME").unwrap_or_else(|_| "/".into());
                let session = client
                    .request("session/new", json!({ "cwd": cwd, "mcpServers": [] }))
                    .await;
                if session.is_ok() {
                    let deadline = tokio::time::sleep(Duration::from_secs(2));
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            inc = incoming.recv() => match inc {
                                Some(Incoming::Notification { method, params })
                                    if method == "session/update" =>
                                {
                                    let update = params.get("update").cloned().unwrap_or(Value::Null);
                                    if update.get("sessionUpdate").and_then(Value::as_str)
                                        == Some("available_commands_update")
                                    {
                                        commands = parse_commands(update.get("availableCommands"));
                                        break;
                                    }
                                }
                                Some(Incoming::Request { id, .. }) => {
                                    client.respond_error(&id, -32601, "unsupported during discovery");
                                }
                                Some(_) => {}
                                None => break,
                            },
                            _ = &mut deadline => break,
                        }
                    }
                }
            }
            Ok::<Vec<SlashCommand>, HarnessError>(commands)
        };
        let result = tokio::time::timeout(Duration::from_secs(10), discovery).await;
        shutdown_child(&mut child, self.kill_grace).await;
        match result {
            Ok(inner) => inner,
            Err(_) => Err(HarnessError::Protocol("command discovery timed out".into())),
        }
    }

    async fn discover_models(&self) -> Result<Vec<Model>, HarnessError> {
        let (mut child, stderr_tail) = self.spawn_agent(None, false, &[]).await?;
        let (client, _incoming) = match (child.stdin.take(), child.stdout.take()) {
            (Some(stdin), Some(stdout)) => RpcClient::new(stdin, stdout),
            _ => {
                shutdown_child(&mut child, self.kill_grace).await;
                return Err(HarnessError::Protocol("agent child has no stdio".into()));
            }
        };
        let discovery = async {
            client
                .request("initialize", initialize_params(self.spec.id))
                .await?;
            let cwd = std::env::var("HOME").unwrap_or_else(|_| "/".into());
            let session = client
                .request("session/new", json!({ "cwd": cwd, "mcpServers": [] }))
                .await?;
            let mut models = models_from_session(&session, &(self.spec.models)());
            for model in &mut models {
                if !model.reasoning_levels.is_empty() {
                    for extra in self.spec.ladder_extras {
                        if !model.reasoning_levels.contains(extra) {
                            model.reasoning_levels.push(*extra);
                        }
                    }
                }
            }
            Ok::<Vec<Model>, HarnessError>(models)
        };
        let result = tokio::time::timeout(self.model_discovery_timeout, discovery).await;
        shutdown_child(&mut child, self.kill_grace).await;
        match result {
            Ok(inner) => inner,
            Err(_) => {
                let mut error = format!(
                    "{} model discovery did not complete within {}s",
                    self.spec.display_name,
                    self.model_discovery_timeout.as_secs()
                );
                if let Some(stderr) = stderr_tail.snapshot() {
                    error.push_str("; stderr: ");
                    error.push_str(&stderr);
                }
                Err(HarnessError::Protocol(error))
            }
        }
    }
}

fn reasoning_from_value(value: &str) -> Option<ReasoningLevel> {
    match norm_id(value).as_str() {
        "minimal" => Some(ReasoningLevel::Minimal),
        "low" => Some(ReasoningLevel::Low),
        "medium" => Some(ReasoningLevel::Medium),
        "high" => Some(ReasoningLevel::High),
        "xhigh" => Some(ReasoningLevel::XHigh),
        "max" => Some(ReasoningLevel::Max),
        "ultra" => Some(ReasoningLevel::Ultra),
        "ultracode" => Some(ReasoningLevel::Ultracode),
        "ultrathink" => Some(ReasoningLevel::Ultrathink),
        _ => None,
    }
}

fn models_from_session(session_response: &Value, catalog: &[Model]) -> Vec<Model> {
    let config_options = session_response
        .get("configOptions")
        .and_then(Value::as_array)
        .map(|a| a.as_slice())
        .unwrap_or_default();

    let ladder: Vec<ReasoningLevel> = config_options
        .iter()
        .find(|o| o.get("category").and_then(Value::as_str) == Some("thought_level"))
        .and_then(|o| o.get("options").and_then(Value::as_array))
        .map(|opts| {
            opts.iter()
                .filter_map(|o| o.get("value").and_then(Value::as_str))
                .filter_map(reasoning_from_value)
                .collect()
        })
        .unwrap_or_default();
    let wire_options: Vec<ModelOption> = config_options
        .iter()
        .filter_map(trait_from_config_option)
        .collect();

    let exact = |id: &str| catalog.iter().find(|m| norm_id(&m.id) == norm_id(id));
    let alias = |id: &str| {
        let norm = norm_id(id);
        (!norm.is_empty() && norm.chars().all(|c| c.is_ascii_alphabetic()))
            .then(|| catalog.iter().find(|m| norm_id(&m.id).contains(&norm)))
            .flatten()
    };
    let build = |id: &str,
                 name: Option<&str>,
                 description: Option<&str>,
                 options: Vec<ModelOption>|
     -> Model {
        let exact = exact(id);
        let aliased = if exact.is_none() { alias(id) } else { None };
        let known = exact.or(aliased);
        Model {
            id: id.to_owned(),
            label: aliased
                .map(|m| m.label.clone())
                .or_else(|| name.map(str::to_owned))
                .or_else(|| known.map(|m| m.label.clone()))
                .unwrap_or_else(|| id.to_owned()),
            description: aliased
                .and_then(|m| m.description.clone())
                .or_else(|| description.map(str::to_owned))
                .or_else(|| known.and_then(|m| m.description.clone())),
            reasoning_levels: match known.filter(|m| !m.reasoning_levels.is_empty()) {
                Some(m) => m.reasoning_levels.clone(),
                None => ladder.clone(),
            },
            options,
        }
    };

    let model_select: Vec<&Value> = config_options
        .iter()
        .find(|o| o.get("category").and_then(Value::as_str) == Some("model"))
        .and_then(|o| o.get("options").and_then(Value::as_array))
        .map(|opts| opts.iter().collect())
        .unwrap_or_default();
    if !model_select.is_empty() {
        let raw_ids: Vec<&str> = model_select
            .iter()
            .filter_map(|o| o.get("value").and_then(Value::as_str))
            .collect();
        let has_real = raw_ids.iter().any(|id| norm_id(id) != "default");
        return model_select
            .iter()
            .filter_map(|o| {
                let id = o.get("value").and_then(Value::as_str)?;
                if has_real && norm_id(id) == "default" {
                    return None;
                }
                let name = o.get("name").and_then(Value::as_str);
                let description = o.get("description").and_then(Value::as_str);
                let mut options = wire_options.clone();
                if let Some(base) = strip_context_hint(id) {
                    if raw_ids.contains(&base) {
                        return None;
                    }
                    let mut window = crate::claude::catalog::context_window();
                    window.default_choice = "1m".into();
                    options.push(window);
                    return Some(build(
                        base,
                        name.map(strip_trailing_parenthetical)
                            .filter(|n| !n.is_empty()),
                        description,
                        options,
                    ));
                }
                if raw_ids
                    .iter()
                    .any(|raw| strip_context_hint(raw) == Some(id))
                {
                    options.push(crate::claude::catalog::context_window());
                }
                Some(build(id, name, description, options))
            })
            .collect();
    }

    session_response
        .get("models")
        .and_then(|m| m.get("availableModels"))
        .and_then(Value::as_array)
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .filter_map(|m| {
            let id = m.get("modelId").and_then(Value::as_str)?;
            Some(build(
                id,
                m.get("name").and_then(Value::as_str),
                m.get("description").and_then(Value::as_str),
                exact(id).map(|k| k.options.clone()).unwrap_or_default(),
            ))
        })
        .collect()
}

fn trait_from_config_option(option: &Value) -> Option<ModelOption> {
    if matches!(
        option.get("category").and_then(Value::as_str),
        Some("mode" | "model" | "thought_level")
    ) {
        return None;
    }
    let id = option.get("id").and_then(Value::as_str)?;
    let label = option.get("name").and_then(Value::as_str).unwrap_or(id);
    match option.get("type").and_then(Value::as_str)? {
        "select" => {
            let choices: Vec<ModelOptionChoice> = option
                .get("options")
                .and_then(Value::as_array)?
                .iter()
                .filter_map(|c| {
                    let id = c.get("value").and_then(Value::as_str)?;
                    Some(ModelOptionChoice {
                        id: id.to_owned(),
                        label: c
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or(id)
                            .to_owned(),
                    })
                })
                .collect();
            let default_choice = option
                .get("currentValue")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| choices.first().map(|c| c.id.clone()))?;
            (choices.len() > 1).then(|| ModelOption {
                id: id.to_owned(),
                label: label.to_owned(),
                choices,
                default_choice,
            })
        }
        "boolean" => Some(ModelOption {
            id: id.to_owned(),
            label: label.to_owned(),
            choices: vec![
                ModelOptionChoice {
                    id: "off".into(),
                    label: "Off".into(),
                },
                ModelOptionChoice {
                    id: "on".into(),
                    label: "On".into(),
                },
            ],
            default_choice: if option.get("currentValue") == Some(&Value::Bool(true)) {
                "on".into()
            } else {
                "off".into()
            },
        }),
        _ => None,
    }
}

#[async_trait]
impl Harness for AcpHarness {
    fn id(&self) -> HarnessId {
        self.spec.id
    }
    fn display_name(&self) -> &str {
        self.spec.display_name
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        self.spec.steering_mode
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        self.spec.reasoning_levels
    }

    fn installed(&self) -> bool {
        if self.executable.is_some() {
            return true;
        }
        if std::env::var_os(self.spec.env_override).is_some_and(|v| !v.is_empty()) {
            return true;
        }
        find_on_paths(self.spec.cli_executable, (self.spec.cli_extra_paths)()).is_some()
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        self.resolve_launch()?;
        if let Some(models) = self.models_cache.get() {
            return Ok(models.clone());
        }
        let _probe = self.models_probe.lock().await;
        if let Some(models) = self.models_cache.get() {
            return Ok(models.clone());
        }
        match self.discover_models().await {
            Ok(models) if !models.is_empty() => {
                let _ = self.models_cache.set(models.clone());
                Ok(self.models_cache.get().cloned().unwrap_or(models))
            }
            Ok(_) => Ok((self.spec.models)()),
            Err(error) if self.spec.id == HarnessId::Opencode => Err(error),
            Err(_) => Ok((self.spec.models)()),
        }
    }

    async fn commands(&self) -> Result<Vec<SlashCommand>, HarnessError> {
        self.commands
            .get_or_try_init(|| self.discover_commands())
            .await
            .cloned()
    }

    async fn run(
        &self,
        request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let sidecar_port = self.spec.http_sidecar.then(free_localhost_port).flatten();
        let extra_args = match sidecar_port {
            Some(port) => vec!["--port".to_owned(), port.to_string()],
            None => Vec::new(),
        };
        let (mut child, stderr_tail) = self
            .spawn_agent(Some(&request.cwd), true, &extra_args)
            .await?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HarnessError::Protocol("agent child has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Protocol("agent child has no stdout".into()))?;
        let (client, incoming) = RpcClient::new(stdin, stdout);
        let (event_tx, event_rx) = mpsc::channel::<Result<AgentEvent, HarnessError>>(256);
        tokio::spawn(run_session(Session {
            child,
            client,
            incoming,
            event_tx,
            controls,
            request,
            harness: self.spec.id,
            agent_name: self.spec.display_name,
            prompt_transform: self.spec.prompt_transform,
            effort_values: self.spec.effort_values,
            prompt_complete_extension: self.spec.prompt_complete_extension,
            prompt_stall: self.spec.prompt_stall,
            stall_hint: self.spec.stall_hint,
            sessions_root: self.sessions_root.clone(),
            sidecar_port,
            interrupt_grace: self.interrupt_grace,
            kill_grace: self.kill_grace,
            handshake_timeout: self.handshake_timeout,
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
    harness: HarnessId,
    agent_name: &'static str,
    prompt_complete_extension: bool,
    prompt_stall: Option<Duration>,
    stall_hint: &'static str,
    sessions_root: Option<PathBuf>,
    sidecar_port: Option<u16>,
    prompt_transform: fn(Option<ReasoningLevel>, &str) -> String,
    effort_values: fn(Option<ReasoningLevel>, Option<&str>) -> Vec<&'static str>,
    interrupt_grace: Duration,
    kill_grace: Duration,
    handshake_timeout: Duration,
    stderr_tail: crate::StderrTail,
}

fn initialize_params(_harness: HarnessId) -> Value {
    let capabilities = json!({
        "fs": { "readTextFile": false, "writeTextFile": false },
        "terminal": false,
    });
    json!({
        "protocolVersion": 1,
        "clientInfo": {
            "name": "zeron",
            "title": "Zeron",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "clientCapabilities": capabilities,
    })
}

fn steering_supported(init: &Value) -> bool {
    init.get("_meta")
        .and_then(|m| m.get("steering"))
        .and_then(|s| s.get("supported"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn scan_available_commands(value: &Value) -> Vec<SlashCommand> {
    fn scan(value: &Value, depth: u8) -> Option<&Value> {
        if depth == 0 {
            return None;
        }
        let obj = value.as_object()?;
        if let Some(cmds) = obj.get("availableCommands").filter(|c| c.is_array()) {
            return Some(cmds);
        }
        obj.values().find_map(|v| scan(v, depth - 1))
    }
    parse_commands(scan(value, 4))
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

fn norm_id(id: &str) -> String {
    id.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn context_hint_1m(id: &str) -> bool {
    id.contains("[1m]") || id.ends_with("-1m")
}

fn strip_context_hint(id: &str) -> Option<&str> {
    id.strip_suffix("[1m]").or_else(|| id.strip_suffix("-1m"))
}

fn strip_trailing_parenthetical(name: &str) -> &str {
    match name.rfind(" (") {
        Some(at) if name.ends_with(')') => name[..at].trim_end(),
        _ => name,
    }
}

fn pick_model_value(requested: &str, available: &[&str], context_1m: bool) -> Option<String> {
    if context_1m {
        for composed in [format!("{requested}[1m]"), format!("{requested}-1m")] {
            if available.contains(&composed.as_str()) {
                return Some(composed);
            }
        }
    }
    if available.contains(&requested) {
        return Some(requested.to_owned());
    }
    let family = ["fable", "opus", "sonnet", "haiku", "gpt"]
        .into_iter()
        .find(|f| norm_id(requested).contains(f))?;
    let candidates: Vec<&&str> = available
        .iter()
        .filter(|v| norm_id(v).contains(family))
        .collect();
    candidates
        .iter()
        .find(|v| context_hint_1m(v) == context_1m)
        .or_else(|| candidates.first())
        .map(|v| (**v).to_owned())
}

fn first_class_model_change(
    session_response: &Value,
    requested: Option<&str>,
) -> Result<Option<String>, HarnessError> {
    let Some(requested) = requested else {
        return Ok(None);
    };
    let has_model_config = session_response
        .get("configOptions")
        .and_then(Value::as_array)
        .is_some_and(|options| {
            options.iter().any(|option| {
                option.get("type").and_then(Value::as_str) == Some("select")
                    && option.get("category").and_then(Value::as_str) == Some("model")
            })
        });
    if has_model_config {
        return Ok(None);
    }

    let Some(models) = session_response.get("models") else {
        return Ok(None);
    };
    let available: Vec<&str> = models
        .get("availableModels")
        .and_then(Value::as_array)
        .map(|models| models.as_slice())
        .unwrap_or_default()
        .iter()
        .filter_map(|model| model.get("modelId").and_then(Value::as_str))
        .collect();
    if available.is_empty() {
        return Ok(None);
    }
    if !available.contains(&requested) {
        return Err(HarnessError::Protocol(format!(
            "agent does not advertise requested model {requested}; available models: {}",
            available.join(", ")
        )));
    }
    if models.get("currentModelId").and_then(Value::as_str) == Some(requested) {
        return Ok(None);
    }
    Ok(Some(requested.to_owned()))
}

fn config_option_sets(
    session_response: &Value,
    model: Option<&str>,
    efforts: &[&'static str],
    model_options: &serde_json::Map<String, Value>,
) -> Vec<(String, Value)> {
    let Some(options) = session_response
        .get("configOptions")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    let context_1m = model_options
        .get("contextWindow")
        .and_then(Value::as_str)
        .is_some_and(|w| w.eq_ignore_ascii_case("1m"));
    let mut sets = Vec::new();
    for option in options {
        let Some(config_id) = option.get("id").and_then(Value::as_str) else {
            continue;
        };
        let kind = option.get("type").and_then(Value::as_str).unwrap_or("");
        let category = option.get("category").and_then(Value::as_str);
        let current = option.get("currentValue");
        let available: Vec<&str> = option
            .get("options")
            .and_then(Value::as_array)
            .map(|a| a.as_slice())
            .unwrap_or_default()
            .iter()
            .filter_map(|o| o.get("value").and_then(Value::as_str))
            .collect();

        let wanted: Option<Value> = match (kind, category) {
            ("select", Some("model")) => model
                .and_then(|m| pick_model_value(m, &available, context_1m))
                .map(Value::String),
            ("select", Some("mode")) => model_options
                .get("mode")
                .and_then(Value::as_str)
                .filter(|c| available.contains(c))
                .map(|c| Value::String(c.to_owned()))
                .or_else(|| {
                    [
                        "bypassPermissions",
                        "bypass_permissions",
                        "yolo",
                        "agent-full-access",
                        "danger-full-access",
                        "full-access",
                    ]
                    .into_iter()
                    .find(|v| available.contains(v))
                    .map(|v| Value::String(v.to_owned()))
                }),
            ("select", Some("thought_level")) => efforts
                .iter()
                .find(|c| available.contains(*c))
                .map(|c| Value::String((*c).to_owned())),
            _ => model_options.iter().find_map(|(opt_id, choice)| {
                if norm_id(opt_id) != norm_id(config_id) || opt_id == "contextWindow" {
                    return None;
                }
                match kind {
                    "select" => choice
                        .as_str()
                        .filter(|c| available.contains(c))
                        .map(|c| Value::String(c.to_owned())),
                    "boolean" => {
                        let on = choice == &Value::Bool(true)
                            || choice
                                .as_str()
                                .is_some_and(|c| c.eq_ignore_ascii_case("on"));
                        Some(Value::Bool(on))
                    }
                    _ => None,
                }
            }),
        };
        if let Some(value) = wanted
            && current != Some(&value)
        {
            let payload = match value {
                Value::Bool(b) => serde_json::json!({ "type": "boolean", "value": b }),
                other => serde_json::json!({ "value": other }),
            };
            sets.push((config_id.to_owned(), payload));
        }
    }
    sets
}

enum SubagentObserver {
    Grok(SubagentTracker),
    Opencode(OpencodeTracker),
}

impl SubagentObserver {
    fn observe(&mut self, update: &Value) {
        match self {
            SubagentObserver::Grok(tracker) => tracker.observe(update),
            SubagentObserver::Opencode(tracker) => tracker.observe(update),
        }
    }
}

fn session_update_events(
    method: &str,
    params: &Value,
    session_id: &str,
    subagents: &mut SubagentObserver,
) -> Vec<AgentEvent> {
    if params.get("sessionId").and_then(Value::as_str) != Some(session_id) {
        return Vec::new();
    }
    let update = params.get("update").unwrap_or(&Value::Null);
    match method {
        "session/update" => {
            subagents.observe(update);
            map_update(update)
        }
        "_x.ai/session_notification" => {
            subagents.observe(update);
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn usage_from_response(res: &Result<Value, HarnessError>) -> Option<AgentEvent> {
    let resp = res.as_ref().ok()?;
    let usage = resp.get("usage").or_else(|| resp.get("_meta"))?;
    let count = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| usage.get(*k))
            .and_then(Value::as_u64)
    };
    let input = count(&["inputTokens", "input_tokens"]);
    let output = count(&["outputTokens", "output_tokens"]);
    (input.is_some() || output.is_some()).then(|| AgentEvent::Usage {
        input_tokens: input.unwrap_or(0),
        output_tokens: output.unwrap_or(0),
    })
}

fn stop_outcome(
    res: &Result<Value, HarnessError>,
    interrupted: bool,
) -> (DoneStatus, Option<String>) {
    if interrupted {
        return (DoneStatus::Interrupted, None);
    }
    match res {
        Ok(resp) => match resp.get("stopReason").and_then(Value::as_str) {
            Some("cancelled") => (DoneStatus::Interrupted, None),
            Some("refusal") => (
                DoneStatus::Errored,
                Some("The agent refused to continue.".to_owned()),
            ),
            _ => (DoneStatus::Completed, None),
        },
        Err(e) => (DoneStatus::Errored, Some(e.to_string())),
    }
}

fn prompt_turn(
    client: RpcClient,
    session_id: String,
    text: String,
    prompt_id: Option<String>,
) -> BoxFuture<'static, Result<Value, HarnessError>> {
    Box::pin(async move {
        let mut params = json!({
            "sessionId": session_id,
            "prompt": [{ "type": "text", "text": text }],
        });
        if let Some(id) = prompt_id {
            params["_meta"] = json!({ "promptId": id, "requestId": id });
        }
        client.request("session/prompt", params).await
    })
}

fn handle_server_request(
    client: &RpcClient,
    id: Value,
    method: &str,
    params: &Value,
) -> Vec<AgentEvent> {
    match method {
        "session/request_permission" => {
            let options: Vec<Value> = params
                .get("options")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            match preferred_allow_option(&options) {
                Some(option_id) => client.respond(
                    &id,
                    json!({ "outcome": { "outcome": "selected", "optionId": option_id } }),
                ),
                None => client.respond(&id, json!({ "outcome": { "outcome": "cancelled" } })),
            }
            Vec::new()
        }
        _ => {
            tracing::debug!(target: "zeron_harness::acp", "unhandled server request: {method}");
            client.respond_error(&id, -32601, &format!("unsupported method: {method}"));
            Vec::new()
        }
    }
}

type RequestInputFn = Box<
    dyn Fn(Vec<UserInputQuestion>) -> tokio::sync::oneshot::Receiver<Vec<UserInputAnswer>>
        + Send
        + Sync,
>;

fn is_user_question(options: &[Value]) -> bool {
    options.iter().any(|option| {
        !matches!(
            option.get("kind").and_then(Value::as_str),
            Some("allow_once" | "allow_always" | "reject_once" | "reject_always")
        )
    })
}

fn handle_server_request_live(
    client: &RpcClient,
    id: Value,
    method: &str,
    params: &Value,
    request_input: &std::sync::Arc<RequestInputFn>,
    open_questions: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> Vec<AgentEvent> {
    if method != "session/request_permission" {
        return handle_server_request(client, id, method, params);
    }
    let options: Vec<Value> = params
        .get("options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !is_user_question(&options) {
        return handle_server_request(client, id, method, params);
    }
    let names: Vec<String> = options
        .iter()
        .map(|o| {
            o.get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    let question = UserInputQuestion {
        id: new_message_id(),
        header: "Agent question".into(),
        question: params
            .get("toolCall")
            .and_then(|t| t.get("title"))
            .and_then(Value::as_str)
            .unwrap_or("The agent needs your input.")
            .to_owned(),
        options: names.clone(),
        multi_select: false,
    };
    let client = client.clone();
    let request_input = std::sync::Arc::clone(request_input);
    let open_questions = std::sync::Arc::clone(open_questions);
    open_questions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    tokio::spawn(async move {
        let answers = (request_input)(vec![question.clone()])
            .await
            .unwrap_or_default();
        let picked = answers
            .iter()
            .find(|a| a.question_id == question.id)
            .and_then(|a| a.labels.first())
            .and_then(|label| {
                options
                    .iter()
                    .find(|o| o.get("name").and_then(Value::as_str) == Some(label.as_str()))
            })
            .and_then(|o| o.get("optionId").and_then(Value::as_str));
        match picked {
            Some(option_id) => client.respond(
                &id,
                json!({ "outcome": { "outcome": "selected", "optionId": option_id } }),
            ),
            None => client.respond(&id, json!({ "outcome": { "outcome": "cancelled" } })),
        }
        open_questions.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    });
    Vec::new()
}

async fn request_draining(
    client: &RpcClient,
    incoming: &mut mpsc::Receiver<Incoming>,
    method: &'static str,
    params: Value,
) -> Result<Value, HarnessError> {
    let mut fut = prompt_like_request(client.clone(), method, params);
    let res = loop {
        tokio::select! {
            res = &mut fut => break res,
            inc = incoming.recv() => match inc {
                Some(Incoming::Request { id, method, params }) => {
                    handle_server_request(client, id, &method, &params);
                }
                Some(_) => {}
                None => {
                    return Err(HarnessError::Protocol(format!(
                        "{method}: agent exited during setup"
                    )));
                }
            },
        }
    };
    while let Ok(inc) = incoming.try_recv() {
        if let Incoming::Request { id, method, params } = inc {
            handle_server_request(client, id, &method, &params);
        }
    }
    res
}

fn prompt_like_request(
    client: RpcClient,
    method: &'static str,
    params: Value,
) -> BoxFuture<'static, Result<Value, HarnessError>> {
    Box::pin(async move { client.request(method, params).await })
}

fn track_turn_signals(
    ev: &AgentEvent,
    content_seen: &mut bool,
    open_tools: &mut std::collections::HashSet<String>,
) {
    match ev {
        AgentEvent::TextDelta { text } if !text.is_empty() => *content_seen = true,
        AgentEvent::ToolCall { id, .. } => {
            *content_seen = true;
            open_tools.insert(id.clone());
        }
        AgentEvent::ToolResult { id, .. } => {
            open_tools.remove(id);
        }
        _ => {}
    }
}

fn steering_call_future(
    client: &RpcClient,
    session_id: &str,
    text: &str,
) -> BoxFuture<'static, Result<Value, HarnessError>> {
    let params = json!({
        "sessionId": session_id,
        "prompt": [{ "type": "text", "text": text }],
        "_meta": { "steering": { "idleBehavior": "promptRequired" } },
    });
    prompt_like_request(client.clone(), "_session/steering", params)
}

async fn run_session(session: Session) {
    let Session {
        mut child,
        client,
        mut incoming,
        event_tx,
        controls,
        request,
        harness,
        agent_name,
        prompt_complete_extension,
        prompt_stall,
        stall_hint,
        sessions_root,
        sidecar_port,
        prompt_transform,
        effort_values,
        interrupt_grace,
        kill_grace,
        handshake_timeout,
        stderr_tail,
    } = session;
    let RunControls {
        request_input,
        mut steering,
        interrupt,
    } = controls;
    let request_input = std::sync::Arc::new(request_input);

    let setup = async {
        let init = client
            .request("initialize", initialize_params(harness))
            .await?;
        let steer_ext = steering_supported(&init);
        let init_commands = scan_available_commands(&init);

        let session_params = json!({ "cwd": request.cwd, "mcpServers": [] });
        let (session_id, session_response) = if let Some(resume) = &request.resume {
            let mut load = session_params.clone();
            load["sessionId"] = Value::String(resume.clone());
            match request_draining(&client, &mut incoming, "session/load", load).await {
                Ok(resp) => (resume.clone(), resp),
                Err(e) => {
                    tracing::debug!(
                        target: "zeron_harness::acp",
                        "session/load failed (starting fresh): {e}"
                    );
                    let new = request_draining(
                        &client,
                        &mut incoming,
                        "session/new",
                        session_params.clone(),
                    )
                    .await?;
                    (
                        new.get("sessionId")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        new,
                    )
                }
            }
        } else {
            let new =
                request_draining(&client, &mut incoming, "session/new", session_params).await?;
            (
                new.get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                new,
            )
        };
        if session_id.is_empty() {
            return Err(HarnessError::Protocol(
                "session/new returned no sessionId".into(),
            ));
        }
        if let Some(model) = first_class_model_change(&session_response, request.model.as_deref())?
        {
            request_draining(
                &client,
                &mut incoming,
                "session/set_model",
                json!({
                    "sessionId": session_id,
                    "modelId": model,
                }),
            )
            .await
            .map_err(|error| {
                HarnessError::Protocol(format!("agent rejected model switch to {model}: {error}"))
            })?;
        }
        let efforts = effort_values(request.reasoning, request.model.as_deref());
        let requested_model = request.model.as_deref();
        let options_snapshot = session_response;
        for (config_id, payload) in config_option_sets(
            &options_snapshot,
            requested_model,
            &efforts,
            &request.model_options,
        ) {
            let mut params = serde_json::Map::new();
            params.insert("sessionId".into(), session_id.clone().into());
            params.insert("configId".into(), config_id.clone().into());
            if let Some(payload) = payload.as_object() {
                for (k, v) in payload {
                    params.insert(k.clone(), v.clone());
                }
            }
            if let Err(e) = request_draining(
                &client,
                &mut incoming,
                "session/set_config_option",
                Value::Object(params),
            )
            .await
            {
                tracing::debug!(
                    target: "zeron_harness::acp",
                    "session/set_config_option {config_id}={payload} rejected (agent default runs): {e}"
                );
            }
        }
        Ok::<(String, bool, Vec<SlashCommand>), HarnessError>((
            session_id,
            steer_ext,
            init_commands,
        ))
    };
    let (session_id, steer_ext, init_commands) = tokio::select! {
        res = tokio::time::timeout(handshake_timeout, setup) => {
            let res = res.unwrap_or_else(|_| {
                Err(HarnessError::Protocol(format!(
                    "{agent_name} did not complete the ACP handshake within {}s \
                     (the agent may be waiting for a login — try running it once \
                     in a terminal)",
                    handshake_timeout.as_secs()
                )))
            });
            match res {
                Ok(v) => v,
                Err(e) => {
                    let error = match child.try_wait() {
                        Ok(Some(status)) => {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            format!(
                                "{e}; {}",
                                crate::crash_message(agent_name, Some(status), &stderr_tail)
                            )
                        }
                        _ => match stderr_tail.snapshot() {
                            Some(tail) => format!("{e}; stderr: {tail}"),
                            None => e.to_string(),
                        },
                    };
                    tracing::warn!(target: "zeron_harness::acp", %error, "agent setup failed");
                    let _ = event_tx
                        .send(Ok(AgentEvent::Done {
                            status: DoneStatus::Errored,
                            result: None,
                            error: Some(error),
                            session_id: None,
                        }))
                        .await;
                    shutdown_child(&mut child, kill_grace).await;
                    return;
                }
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

    let mut assistant_message_id = new_message_id();
    if !send(
        &event_tx,
        AgentEvent::SessionStarted {
            harness,
            model: request.model.clone().unwrap_or_default(),
            tools: Vec::new(),
            cwd: request.cwd.clone(),
            session_id: session_id.clone(),
            assistant_message_id: assistant_message_id.clone(),
        },
    )
    .await
    {
        shutdown_child(&mut child, kill_grace).await;
        return;
    }
    if !init_commands.is_empty()
        && !send(
            &event_tx,
            AgentEvent::AvailableCommands {
                commands: init_commands,
            },
        )
        .await
    {
        shutdown_child(&mut child, kill_grace).await;
        return;
    }

    let mut subagents = if harness == HarnessId::Opencode {
        SubagentObserver::Opencode(OpencodeTracker::new(
            session_id.clone(),
            event_tx.clone(),
            sidecar_port.map(|p| format!("http://127.0.0.1:{p}")),
        ))
    } else {
        SubagentObserver::Grok(SubagentTracker::new(
            session_id.clone(),
            event_tx.clone(),
            sessions_root,
        ))
    };

    let mut prompt_seq: u64 = 0;
    let mut current_prompt_id: Option<String> = None;
    let mut completed_prompts: VecDeque<String> = VecDeque::new();
    let prompt_stall: Option<Duration> = match std::env::var("ZERON_ACP_PROMPT_STALL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(0) => None,
        Some(ms) => Some(Duration::from_millis(ms)),
        None => prompt_stall,
    };
    let mut prompt_stall_deadline: Option<tokio::time::Instant> =
        prompt_stall.map(|d| tokio::time::Instant::now() + d);
    let mut turn: Option<BoxFuture<'static, Result<Value, HarnessError>>> = Some({
        prompt_seq += 1;
        current_prompt_id = prompt_complete_extension.then(|| format!("zeron-p{prompt_seq}"));
        prompt_turn(
            client.clone(),
            session_id.clone(),
            prompt_transform(request.reasoning, &request.prompt),
            current_prompt_id.clone(),
        )
    });
    let mut queued_steers: VecDeque<String> = VecDeque::new();
    let mut steering_call: Option<(String, BoxFuture<'static, Result<Value, HarnessError>>)> = None;
    let mut steer_backlog: VecDeque<String> = VecDeque::new();
    let mut steering_open = true;
    let mut interrupted = false;
    let mut interrupt_sent = false;
    let mut done_current = false;
    let mut done_after_interrupt = false;
    let mut escalation: Option<tokio::task::JoinHandle<()>> = None;
    const STARVE_GRACE: Duration = Duration::from_secs(2);
    let mut starve_deadline: Option<tokio::time::Instant> = None;
    let quiet_settle: Option<Duration> = match std::env::var("ZERON_ACP_QUIET_SETTLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(0) => None,
        Some(ms) => Some(Duration::from_millis(ms)),
        None => Some(Duration::from_secs(30)),
    };
    let mut last_update_at = tokio::time::Instant::now();
    let mut turn_content_seen = false;
    let mut steered_this_turn = false;
    let mut open_tools: std::collections::HashSet<String> = std::collections::HashSet::new();
    let open_questions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    const BUSY_RECENT: Duration = Duration::from_secs(3);
    const CANCEL_FLUSH: Duration = Duration::from_secs(2);
    let mut cancel_flush_deadline: Option<tokio::time::Instant> = None;

    'main: loop {
        tokio::select! {
            res = async { turn.as_mut().expect("guarded by if").await }, if turn.is_some() => {
                turn = None;
                starve_deadline = None;
                prompt_stall_deadline = None;
                if let Some(id) = current_prompt_id.take() {
                    completed_prompts.push_back(id);
                    while completed_prompts.len() > 32 {
                        completed_prompts.pop_front();
                    }
                }
                if let Some((text, mut fut)) = steering_call.take() {
                    let outcome = match tokio::time::timeout(
                        Duration::from_millis(1000),
                        &mut fut,
                    )
                    .await
                    {
                        Ok(Ok(resp)) => resp
                            .get("outcome")
                            .and_then(Value::as_str)
                            .unwrap_or("injected")
                            .to_owned(),
                        Ok(Err(_)) | Err(_) => "promptRequired".to_owned(),
                    };
                    if interrupted {
                    } else if outcome != "promptRequired" {
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
                    } else {
                        queued_steers.push_back(text);
                    }
                    while let Some(next_text) = steer_backlog.pop_front() {
                        queued_steers.push_back(next_text);
                    }
                }
                let mut consumer_gone = false;
                while let Ok(inc) = incoming.try_recv() {
                    match inc {
                        Incoming::Notification { method, params } => {
                            let events =
                                session_update_events(&method, &params, &session_id, &mut subagents);
                            for ev in events {
                                if !send(&event_tx, ev).await {
                                    consumer_gone = true;
                                    break;
                                }
                            }
                        }
                        Incoming::Request { id, method, params } => {
                            for ev in handle_server_request_live(
                                &client,
                                id,
                                &method,
                                &params,
                                &request_input,
                                &open_questions,
                            ) {
                                if !send(&event_tx, ev).await {
                                    consumer_gone = true;
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                    if consumer_gone {
                        break;
                    }
                }
                if consumer_gone {
                    break 'main;
                }
                let (prev, _next) = rotate(&mut assistant_message_id);
                if !send(
                    &event_tx,
                    AgentEvent::AssistantMessageCompleted { assistant_message_id: prev },
                )
                .await
                {
                    break 'main;
                }
                if let Some(usage) = usage_from_response(&res)
                    && !send(&event_tx, usage).await
                {
                    break 'main;
                }
                let (status, error) = stop_outcome(&res, interrupted);
                done_current = true;
                if interrupted {
                    done_after_interrupt = true;
                }
                if !send(
                    &event_tx,
                    AgentEvent::Done {
                        status,
                        result: None,
                        error,
                        session_id: Some(session_id.clone()),
                    },
                )
                .await
                {
                    break 'main;
                }
                if interrupted || res.is_err() {
                    break 'main;
                }
                if let Some(text) = queued_steers.pop_front() {
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
                    done_current = false;
                    turn_content_seen = false;
                    steered_this_turn = false;
                    open_tools.clear();
                    last_update_at = tokio::time::Instant::now();
                    prompt_seq += 1;
                    current_prompt_id =
                        prompt_complete_extension.then(|| format!("zeron-p{prompt_seq}"));
                    prompt_stall_deadline =
                        prompt_stall.map(|d| tokio::time::Instant::now() + d);
                    turn = Some(prompt_turn(
                        client.clone(),
                        session_id.clone(),
                        text,
                        current_prompt_id.clone(),
                    ));
                } else if !steering_open {
                    break 'main;
                }
            },

            inc = incoming.recv() => match inc {
                Some(Incoming::Notification { method, params }) => {
                    last_update_at = tokio::time::Instant::now();
                    let boilerplate = method == "session/update"
                        && matches!(
                            params
                                .get("update")
                                .and_then(|u| u.get("sessionUpdate"))
                                .and_then(Value::as_str),
                            Some("available_commands_update")
                                | Some("config_option_update")
                                | Some("current_mode_update")
                        );
                    if !boilerplate {
                        prompt_stall_deadline = None;
                    }
                    if prompt_complete_extension
                        && method == "_x.ai/session/prompt_complete"
                        && !interrupted
                        && turn.is_some()
                        && params.get("sessionId").and_then(Value::as_str)
                            == Some(session_id.as_str())
                    {
                        let pid = params
                            .get("promptId")
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                        let stale = pid.as_deref().is_some_and(|p| {
                            completed_prompts.iter().any(|c| c == p)
                        }) || (pid.is_some() && pid != current_prompt_id);
                        if !stale {
                            let stop = params
                                .get("stopReason")
                                .and_then(Value::as_str)
                                .unwrap_or("end_turn")
                                .to_owned();
                            if let Some(old) = turn.take() {
                                tokio::spawn(async move {
                                    let _ = old.await;
                                });
                            }
                            turn = Some(Box::pin(async move {
                                Ok(json!({ "stopReason": stop }))
                            }));
                        }
                    }
                    let events =
                        session_update_events(&method, &params, &session_id, &mut subagents);
                    for ev in events {
                        track_turn_signals(&ev, &mut turn_content_seen, &mut open_tools);
                        if !send(&event_tx, ev).await {
                            break 'main;
                        }
                    }
                }
                Some(Incoming::Request { id, method, params }) => {
                    prompt_stall_deadline = None;
                    for ev in handle_server_request_live(
                        &client,
                        id,
                        &method,
                        &params,
                        &request_input,
                        &open_questions,
                    ) {
                        if !send(&event_tx, ev).await {
                            break 'main;
                        }
                    }
                }
                Some(Incoming::Eof) | None => {
                    if let Some(mut fut) = turn.take()
                        && let Ok(res @ Ok(_)) =
                            tokio::time::timeout(Duration::from_millis(50), &mut fut).await
                    {
                        let (prev, _next) = rotate(&mut assistant_message_id);
                        let _ = send(
                            &event_tx,
                            AgentEvent::AssistantMessageCompleted { assistant_message_id: prev },
                        )
                        .await;
                        if let Some(usage) = usage_from_response(&res) {
                            let _ = send(&event_tx, usage).await;
                        }
                        let (status, error) = stop_outcome(&res, interrupted);
                        done_current = true;
                        if interrupted {
                            done_after_interrupt = true;
                        }
                        let _ = send(
                            &event_tx,
                            AgentEvent::Done {
                                status,
                                result: None,
                                error,
                                session_id: Some(session_id.clone()),
                            },
                        )
                        .await;
                    }
                    break 'main;
                }
            },

            res = async { steering_call.as_mut().expect("guarded by if").1.as_mut().await },
                if steering_call.is_some() =>
            {
                let (text, _) = steering_call.take().expect("guarded by if");
                let outcome = match &res {
                    Ok(resp) => resp
                        .get("outcome")
                        .and_then(Value::as_str)
                        .unwrap_or("injected")
                        .to_owned(),
                    Err(e) => {
                        tracing::debug!(
                            target: "zeron_harness::acp",
                            "_session/steering failed (redelivering): {e}"
                        );
                        "promptRequired".to_owned()
                    }
                };
                if interrupted {
                } else if outcome != "promptRequired" {
                    if turn.is_some() {
                        steered_this_turn = true;
                        starve_deadline = None;
                        let mut consumer_gone = false;
                        while let Ok(inc) = incoming.try_recv() {
                            match inc {
                                Incoming::Notification { method, params } => {
                                    let events =
                                        session_update_events(&method, &params, &session_id, &mut subagents);
                                    for ev in events {
                                        if !send(&event_tx, ev).await {
                                            consumer_gone = true;
                                            break;
                                        }
                                    }
                                }
                                Incoming::Request { id, method, params } => {
                                    for ev in handle_server_request_live(
                                        &client,
                                        id,
                                        &method,
                                        &params,
                                        &request_input,
                                        &open_questions,
                                    ) {
                                        if !send(&event_tx, ev).await {
                                            consumer_gone = true;
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            }
                            if consumer_gone {
                                break;
                            }
                        }
                        if consumer_gone {
                            break 'main;
                        }
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
                } else if turn.is_some() {
                    if res
                        .as_ref()
                        .ok()
                        .and_then(|r| r.get("reason"))
                        .and_then(Value::as_str)
                        == Some("noRunningTurn")
                    {
                        tracing::warn!(
                            target: "zeron_harness::acp",
                            "steering answered noRunningTurn with a prompt \
                             outstanding; arming starved-turn recovery"
                        );
                        starve_deadline =
                            Some(tokio::time::Instant::now() + STARVE_GRACE);
                    }
                    queued_steers.push_back(text);
                } else {
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
                    done_current = false;
                    turn_content_seen = false;
                    steered_this_turn = false;
                    open_tools.clear();
                    last_update_at = tokio::time::Instant::now();
                    prompt_seq += 1;
                    current_prompt_id =
                        prompt_complete_extension.then(|| format!("zeron-p{prompt_seq}"));
                    prompt_stall_deadline =
                        prompt_stall.map(|d| tokio::time::Instant::now() + d);
                    turn = Some(prompt_turn(
                        client.clone(),
                        session_id.clone(),
                        text,
                        current_prompt_id.clone(),
                    ));
                }
                while let Some(next_text) = steer_backlog.pop_front() {
                    if turn.is_some() && !interrupted {
                        let fut = steering_call_future(&client, &session_id, &next_text);
                        steering_call = Some((next_text, fut));
                        break;
                    }
                    queued_steers.push_back(next_text);
                }
            },

            _ = tokio::time::sleep_until(
                cancel_flush_deadline.unwrap_or_else(tokio::time::Instant::now)
            ), if cancel_flush_deadline.is_some() && !interrupted => {
                cancel_flush_deadline = None;
                if turn.is_none()
                    && let Some(text) = queued_steers.pop_front()
                {
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
                    done_current = false;
                    turn_content_seen = false;
                    steered_this_turn = false;
                    open_tools.clear();
                    last_update_at = tokio::time::Instant::now();
                    prompt_seq += 1;
                    current_prompt_id =
                        prompt_complete_extension.then(|| format!("zeron-p{prompt_seq}"));
                    prompt_stall_deadline =
                        prompt_stall.map(|d| tokio::time::Instant::now() + d);
                    turn = Some(prompt_turn(
                        client.clone(),
                        session_id.clone(),
                        text,
                        current_prompt_id.clone(),
                    ));
                } else if turn.is_none() && !steering_open {
                    break 'main;
                }
            },

            _ = tokio::time::sleep_until(
                last_update_at + quiet_settle.unwrap_or_default()
            ), if quiet_settle.is_some()
                && starve_deadline.is_none()
                && turn.is_some()
                && !interrupted
                && turn_content_seen
                && open_tools.is_empty()
                && open_questions.load(std::sync::atomic::Ordering::SeqCst) == 0 =>
            {
                tracing::warn!(
                    target: "zeron_harness::acp",
                    quiet_ms = quiet_settle.unwrap_or_default().as_millis() as u64,
                    "turn quiet past the settle window with completed output; \
                     treating the prompt response as dropped"
                );
                starve_deadline = Some(tokio::time::Instant::now());
            },

            _ = tokio::time::sleep_until(
                starve_deadline.unwrap_or_else(tokio::time::Instant::now)
            ), if starve_deadline.is_some() && turn.is_some() && !interrupted => {
                starve_deadline = None;
                tracing::warn!(
                    target: "zeron_harness::acp",
                    "prompt response missing past turn-end evidence; settling \
                     the dead turn (and promoting any queued steer)"
                );
                turn = None;
                let (prev, _next) = rotate(&mut assistant_message_id);
                if !send(
                    &event_tx,
                    AgentEvent::AssistantMessageCompleted { assistant_message_id: prev },
                )
                .await
                {
                    break 'main;
                }
                done_current = true;
                if !send(
                    &event_tx,
                    AgentEvent::Done {
                        status: DoneStatus::Completed,
                        result: None,
                        error: None,
                        session_id: Some(session_id.clone()),
                    },
                )
                .await
                {
                    break 'main;
                }
                if let Some(text) = queued_steers.pop_front() {
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
                    done_current = false;
                    turn_content_seen = false;
                    steered_this_turn = false;
                    open_tools.clear();
                    last_update_at = tokio::time::Instant::now();
                    prompt_seq += 1;
                    current_prompt_id =
                        prompt_complete_extension.then(|| format!("zeron-p{prompt_seq}"));
                    prompt_stall_deadline =
                        prompt_stall.map(|d| tokio::time::Instant::now() + d);
                    turn = Some(prompt_turn(
                        client.clone(),
                        session_id.clone(),
                        text,
                        current_prompt_id.clone(),
                    ));
                } else if !steering_open {
                    break 'main;
                }
            },

            steer = steering.recv(), if steering_open && !interrupted => match steer {
                Some(msg) => {
                    let text = prompt_transform(request.reasoning, &msg.prompt);
                    if turn.is_none() && cancel_flush_deadline.is_some() {
                        queued_steers.push_back(text);
                    } else if turn.is_none()
                        && (!open_tools.is_empty()
                            || last_update_at.elapsed() < BUSY_RECENT)
                    {
                        tracing::info!(
                            target: "zeron_harness::acp",
                            "steer into a self-continuing session; cancelling \
                             the unowned turn before prompting"
                        );
                        client.notify(
                            "session/cancel",
                            Some(json!({ "sessionId": session_id })),
                        );
                        queued_steers.push_back(text);
                        cancel_flush_deadline =
                            Some(tokio::time::Instant::now() + CANCEL_FLUSH);
                    } else if turn.is_none() {
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
                        done_current = false;
                        turn_content_seen = false;
                        steered_this_turn = false;
                        open_tools.clear();
                        last_update_at = tokio::time::Instant::now();
                        prompt_seq += 1;
                    current_prompt_id =
                        prompt_complete_extension.then(|| format!("zeron-p{prompt_seq}"));
                    prompt_stall_deadline =
                        prompt_stall.map(|d| tokio::time::Instant::now() + d);
                    turn = Some(prompt_turn(
                        client.clone(),
                        session_id.clone(),
                        text,
                        current_prompt_id.clone(),
                    ));
                    } else if steer_ext {
                        if steering_call.is_some() {
                            steer_backlog.push_back(text);
                        } else {
                            let fut = steering_call_future(&client, &session_id, &text);
                            steering_call = Some((text, fut));
                        }
                    } else {
                        queued_steers.push_back(text);
                    }
                }
                None => {
                    steering_open = false;
                    if turn.is_none() && queued_steers.is_empty() {
                        break 'main;
                    }
                }
            },

            _ = interrupt.cancelled(), if !interrupt_sent => {
                interrupt_sent = true;
                interrupted = true;
                if turn.is_some() {
                    client.notify("session/cancel", Some(json!({ "sessionId": session_id })));
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

            _ = tokio::time::sleep_until(
                prompt_stall_deadline.unwrap_or_else(tokio::time::Instant::now),
            ), if prompt_stall_deadline.is_some() && turn.is_some() && !interrupted => {
                prompt_stall_deadline = None;
                let _ = send(
                    &event_tx,
                    AgentEvent::Error {
                        message: format!(
                            "{agent_name} did not respond to the prompt at all \
                             (no wire activity for {}s). {}",
                            prompt_stall.map(|d| d.as_secs()).unwrap_or(0),
                            stall_hint,
                        ),
                    },
                )
                .await;
                done_current = true;
                let _ = send(
                    &event_tx,
                    AgentEvent::Done {
                        status: DoneStatus::Errored,
                        result: None,
                        error: Some(format!(
                            "{agent_name} is unresponsive — the run was closed."
                        )),
                        session_id: Some(session_id.clone()),
                    },
                )
                .await;
                break 'main;
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
                    session_id: Some(session_id.clone()),
                }))
                .await;
        } else if !interrupted && !done_current {
            let status = child.try_wait().ok().flatten();
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(crate::crash_message(agent_name, status, &stderr_tail)),
                    session_id: Some(session_id.clone()),
                }))
                .await;
        }
    }

    if let Some(handle) = escalation {
        handle.abort();
    }
    shutdown_child(&mut child, kill_grace).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opencode_model_probe_and_chat_share_one_startup_budget() {
        let harness = AcpHarness::opencode();
        assert_eq!(
            harness.model_discovery_timeout, harness.handshake_timeout,
            "model discovery must not fail before the same OpenCode process would be considered hung"
        );
        if std::env::var_os(OPENCODE_STARTUP_TIMEOUT_ENV).is_none() {
            assert_eq!(harness.handshake_timeout, DEFAULT_OPENCODE_STARTUP_TIMEOUT);
        }
    }

    #[test]
    fn steering_capability_reads_initialize_meta() {
        assert!(steering_supported(&json!({
            "protocolVersion": 1,
            "_meta": { "steering": { "supported": true } },
        })));
        assert!(!steering_supported(&json!({ "protocolVersion": 1 })));
        assert!(!steering_supported(&json!({
            "_meta": { "steering": { "supported": false } },
        })));
    }

    #[test]
    fn config_option_sets_map_model_effort_and_model_options() {
        let response = json!({
            "sessionId": "s-1",
            "configOptions": [
                {
                    "id": "model",
                    "name": "Model",
                    "category": "model",
                    "type": "select",
                    "currentValue": "claude-sonnet-5",
                    "options": [
                        { "value": "claude-sonnet-5", "name": "Sonnet 5" },
                        { "value": "claude-opus-5", "name": "Opus 5" },
                        { "value": "claude-opus-5[1m]", "name": "Opus 5 (1M)" },
                    ],
                },
                {
                    "id": "effort",
                    "name": "Reasoning effort",
                    "category": "thought_level",
                    "type": "select",
                    "currentValue": "high",
                    "options": [
                        { "value": "low", "name": "Low" },
                        { "value": "medium", "name": "Medium" },
                        { "value": "high", "name": "High" },
                        { "value": "max", "name": "Max" },
                    ],
                },
                {
                    "id": "fast_mode",
                    "name": "Fast mode",
                    "category": "model_config",
                    "type": "boolean",
                    "currentValue": false,
                },
            ],
        });
        let no_opts = serde_json::Map::new();
        assert_eq!(
            config_option_sets(&response, Some("claude-opus-5"), &["medium"], &no_opts),
            vec![
                ("model".to_owned(), json!({ "value": "claude-opus-5" })),
                ("effort".to_owned(), json!({ "value": "medium" })),
            ]
        );
        assert_eq!(
            config_option_sets(&response, None, &["xhigh", "max"], &no_opts),
            vec![("effort".to_owned(), json!({ "value": "max" }))]
        );
        let mut opts = serde_json::Map::new();
        opts.insert("contextWindow".into(), json!("1m"));
        opts.insert("fastMode".into(), json!("on"));
        assert_eq!(
            config_option_sets(&response, Some("claude-opus-5"), &["high"], &opts),
            vec![
                ("model".to_owned(), json!({ "value": "claude-opus-5[1m]" })),
                (
                    "fast_mode".to_owned(),
                    json!({ "type": "boolean", "value": true })
                ),
            ]
        );
        assert_eq!(
            config_option_sets(&response, Some("claude-sonnet-5"), &["high"], &no_opts),
            Vec::new()
        );
        assert_eq!(
            config_option_sets(&response, Some("gpt-5.6-sol"), &[], &no_opts),
            Vec::new()
        );
        assert_eq!(
            config_option_sets(&json!({"sessionId": "s"}), Some("x"), &["high"], &no_opts),
            Vec::new()
        );
    }

    #[test]
    fn first_class_models_use_session_set_model_without_config_option() {
        let response = json!({
            "models": {
                "currentModelId": "grok-4.6",
                "availableModels": [
                    { "modelId": "grok-4.6", "name": "Grok 4.6" },
                    { "modelId": "grok-4.5", "name": "Grok 4.5" },
                ],
            },
        });
        assert_eq!(
            first_class_model_change(&response, Some("grok-4.5")).unwrap(),
            Some("grok-4.5".into())
        );
        assert_eq!(
            first_class_model_change(&response, Some("grok-4.6")).unwrap(),
            None
        );
        assert!(first_class_model_change(&response, Some("unknown")).is_err());
    }

    #[test]
    fn model_config_option_takes_precedence_over_legacy_models_state() {
        let response = json!({
            "models": {
                "currentModelId": "gpt-5.6-sol low",
                "availableModels": [
                    { "modelId": "gpt-5.6-sol low", "name": "GPT-5.6-Sol (low)" },
                ],
            },
            "configOptions": [{
                "id": "model",
                "category": "model",
                "type": "select",
                "currentValue": "gpt-5.6-sol",
                "options": [
                    { "value": "gpt-5.6-sol", "name": "GPT-5.6-Sol" },
                    { "value": "gpt-5.6-terra", "name": "GPT-5.6-Terra" },
                ],
            }],
        });
        assert_eq!(
            first_class_model_change(&response, Some("gpt-5.6-terra")).unwrap(),
            None
        );
    }

    #[test]
    fn models_prefer_the_model_config_option_over_legacy_available_models() {
        let response = json!({
            "sessionId": "s-1",
            "models": {
                "currentModelId": "gpt-5.6-sol low",
                "availableModels": [
                    { "modelId": "gpt-5.6-sol low", "name": "GPT-5.6-Sol (low)" },
                    { "modelId": "gpt-5.6-sol medium", "name": "GPT-5.6-Sol (medium)" },
                    { "modelId": "gpt-5.6-terra low", "name": "GPT-5.6-Terra (low)" },
                ],
            },
            "configOptions": [
                {
                    "id": "mode",
                    "name": "Mode",
                    "category": "mode",
                    "type": "select",
                    "currentValue": "agent",
                    "options": [
                        { "value": "read-only", "name": "Read Only" },
                        { "value": "agent", "name": "Agent" },
                        { "value": "agent-full-access", "name": "Agent (full access)" },
                    ],
                },
                {
                    "id": "model",
                    "name": "Model",
                    "category": "model",
                    "type": "select",
                    "currentValue": "gpt-5.6-sol",
                    "options": [
                        { "value": "gpt-5.6-sol", "name": "GPT-5.6-Sol", "description": "Frontier" },
                        { "value": "gpt-5.6-terra", "name": "GPT-5.6-Terra" },
                    ],
                },
                {
                    "id": "reasoning_effort",
                    "name": "Reasoning effort",
                    "category": "thought_level",
                    "type": "select",
                    "currentValue": "medium",
                    "options": [
                        { "value": "low", "name": "Low" },
                        { "value": "medium", "name": "Medium" },
                        { "value": "high", "name": "High" },
                    ],
                },
                {
                    "id": "fast-mode",
                    "name": "Fast mode",
                    "category": "model_config",
                    "type": "select",
                    "currentValue": "off",
                    "options": [
                        { "value": "off", "name": "Off" },
                        { "value": "on", "name": "On" },
                    ],
                },
            ],
        });
        let models = models_from_session(&response, &crate::codex::catalog::static_models());
        assert_eq!(
            models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["gpt-5.6-sol", "gpt-5.6-terra"]
        );
        assert_eq!(models[0].label, "GPT-5.6-Sol");
        assert_eq!(models[0].description.as_deref(), Some("Frontier"));
        assert!(models[0].reasoning_levels.contains(&ReasoningLevel::Ultra));
        assert_eq!(
            models[0]
                .options
                .iter()
                .map(|o| o.id.as_str())
                .collect::<Vec<_>>(),
            vec!["fast-mode"]
        );
        assert_eq!(models[0].options[0].default_choice, "off");
    }

    #[test]
    fn model_1m_variants_collapse_into_a_context_window_trait() {
        let response = json!({
            "sessionId": "s-1",
            "configOptions": [
                {
                    "id": "model",
                    "category": "model",
                    "type": "select",
                    "currentValue": "claude-sonnet-5",
                    "options": [
                        { "value": "claude-sonnet-5", "name": "Sonnet 5" },
                        { "value": "claude-sonnet-5[1m]", "name": "Sonnet 5 (1M)" },
                        { "value": "claude-opus-4-6", "name": "Opus 4.6" },
                        { "value": "claude-opus-4-6-1m", "name": "Opus 4.6 (1M)" },
                        { "value": "claude-haiku-4-5", "name": "Haiku 4.5" },
                    ],
                },
            ],
        });
        let models = models_from_session(&response, &[]);
        assert_eq!(
            models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["claude-sonnet-5", "claude-opus-4-6", "claude-haiku-4-5"]
        );
        assert!(models[0].options.iter().any(|o| o.id == "contextWindow"));
        assert!(models[1].options.iter().any(|o| o.id == "contextWindow"));
        assert!(models[2].options.is_empty());
    }

    #[test]
    fn default_alias_drops_and_orphan_1m_variants_fold_to_their_base() {
        let response = json!({
            "sessionId": "s-1",
            "configOptions": [{
                "id": "model",
                "category": "model",
                "type": "select",
                "currentValue": "claude-fable-5[1m]",
                "options": [
                    { "value": "default", "name": "Default (recommended)" },
                    { "value": "opus[1m]", "name": "Opus (1M context)" },
                    { "value": "claude-fable-5[1m]", "name": "Fable 5" },
                    { "value": "sonnet", "name": "Sonnet" },
                    { "value": "haiku", "name": "Haiku" },
                ],
            }],
        });
        let models = models_from_session(&response, &[]);
        assert_eq!(
            models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["opus", "claude-fable-5", "sonnet", "haiku"]
        );
        assert_eq!(models[0].label, "Opus");
        let window = models[0].options.iter().find(|o| o.id == "contextWindow");
        assert_eq!(window.map(|o| o.default_choice.as_str()), Some("1m"));
        assert!(
            models[1]
                .options
                .iter()
                .any(|o| o.id == "contextWindow" && o.default_choice == "1m")
        );
        assert!(models[2].options.is_empty());
        assert!(models[3].options.is_empty());
    }

    #[test]
    fn claude_aliases_enrich_from_the_curated_catalog() {
        let response = json!({
            "sessionId": "s-1",
            "configOptions": [{
                "id": "model",
                "category": "model",
                "type": "select",
                "currentValue": "default",
                "options": [
                    { "value": "default", "name": "Default (recommended)" },
                    { "value": "opus[1m]", "name": "Opus (1M context)" },
                    { "value": "fable", "name": "Fable" },
                    { "value": "sonnet", "name": "Sonnet" },
                    { "value": "haiku", "name": "Haiku" },
                ],
            }],
        });
        let models = models_from_session(&response, &crate::claude::catalog::static_models());
        assert_eq!(
            models.iter().map(|m| m.label.as_str()).collect::<Vec<_>>(),
            vec!["Opus 5", "Fable 5", "Sonnet 5", "Haiku 4.5"]
        );
        assert!(
            models[1]
                .reasoning_levels
                .contains(&ReasoningLevel::Ultracode)
        );
        assert!(models[3].reasoning_levels.is_empty());
        let foreign = json!({
            "sessionId": "s-1",
            "configOptions": [{
                "id": "model", "category": "model", "type": "select",
                "options": [{ "value": "claude-opus-9-mini", "name": "Opus 9 Mini" }],
            }],
        });
        let models = models_from_session(&foreign, &crate::claude::catalog::static_models());
        assert_eq!(models[0].label, "Opus 9 Mini");
    }

    #[test]
    fn models_fall_back_to_legacy_state_with_catalog_options() {
        let response = json!({
            "sessionId": "s-1",
            "models": {
                "availableModels": [
                    { "modelId": "gpt-5.6-sol", "name": "GPT-5.6-Sol" },
                    { "modelId": "gpt-x", "name": "GPT-X" },
                ],
            },
        });
        let models = models_from_session(&response, &crate::codex::catalog::static_models());
        assert_eq!(models.len(), 2);
        assert!(models[0].options.iter().any(|o| o.id == "serviceTier"));
        assert!(models[1].options.is_empty());
    }

    #[test]
    fn codex_exec_approval_options_are_not_a_question() {
        let options = vec![
            json!({ "optionId": "allow_once", "name": "Allow Once", "kind": "allow_once" }),
            json!({ "optionId": "allow_always", "name": "Allow for Session", "kind": "allow_always" }),
            json!({ "optionId": "allow_prefix", "name": "Allow Commands Starting With `cargo test`", "kind": "allow_always" }),
            json!({ "optionId": "reject", "name": "Reject", "kind": "reject_once" }),
        ];
        assert!(!is_user_question(&options));
        let question = vec![
            json!({ "optionId": "a", "name": "Blue" }),
            json!({ "optionId": "b", "name": "Green" }),
        ];
        assert!(is_user_question(&question));
        let mixed = vec![
            json!({ "optionId": "a", "name": "Proceed", "kind": "allow_once" }),
            json!({ "optionId": "b", "name": "Другое", "kind": "other" }),
        ];
        assert!(is_user_question(&mixed));
    }

    #[test]
    fn mode_config_option_prefers_a_no_prompt_mode_per_adapter_naming() {
        let codex = json!({
            "sessionId": "s-1",
            "configOptions": [{
                "id": "mode",
                "category": "mode",
                "type": "select",
                "currentValue": "agent",
                "options": [
                    { "value": "read-only" },
                    { "value": "agent" },
                    { "value": "agent-full-access" },
                ],
            }],
        });
        let no_opts = serde_json::Map::new();
        assert_eq!(
            config_option_sets(&codex, None, &[], &no_opts),
            vec![("mode".to_owned(), json!({ "value": "agent-full-access" }))]
        );
    }

    #[test]
    fn command_scan_finds_nested_advertisements() {
        let init = json!({
            "protocolVersion": 1,
            "agentCapabilities": {
                "_meta": {
                    "availableCommands": [
                        { "name": "compact", "description": "Compact the session" },
                    ],
                },
            },
        });
        let commands = scan_available_commands(&init);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].name, "compact");
        assert!(scan_available_commands(&json!({ "protocolVersion": 1 })).is_empty());
    }
}
