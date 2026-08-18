# Harness integration (2026-08)

The harness crate gives the engine one `Harness` interface while keeping each
agent's native transport where it is strongest. The shared contract normalizes
agent output into `AgentEvent`, exposes model/command discovery, bridges user
questions, and provides steering and cancellation controls.

## Driver split

| Agent | Driver | Transport | Steering |
|---|---|---|---|
| Claude Code | native `ClaudeHarness` | `claude` stream-json over stdio | CLI step boundary |
| Codex | native `CodexHarness` | `codex app-server`, JSON-RPC over stdio | `turn/steer`, queued fallback |
| Cursor | native `CursorHarness` | pinned `@cursor/sdk` through a Node JSONL shim | turn boundary |
| Grok Build | `AcpHarness` | ACP v1 over stdio | ACP extension or turn boundary |
| Hermes | `AcpHarness` | `hermes acp`, ACP v1 over stdio | turn boundary |
| Pi | `AcpHarness` | community `pi-acp` adapter | turn boundary |

Claude, Codex, and Cursor use native drivers rather than ACP adapters. Their
native wires expose better terminal-turn semantics and preserve capabilities
that the adapter layer either hid or represented ambiguously. ACP remains for
agents that are built around ACP or do not yet have a native driver. The
protocol details for that shared surface are in [acp.md](acp.md).

## Shared runtime contract

Every driver:

- resolves an installed CLI or a managed, pinned adapter/shim;
- composes a GUI-safe child `PATH` through Node version-manager locations;
- reads framed child output and maps it to the common `AgentEvent` stream;
- keeps a bounded stderr tail for actionable crash errors;
- bridges permission/question requests through `RunControls::request_input`;
- accepts queued steering through `RunControls::steering`; and
- cancels through the agent protocol first, then escalates from SIGTERM to
  SIGKILL when the child does not exit.

The engine owns session persistence and resume continuity. Harness-native
session identifiers are recorded against the chat, while the durable chat
document and run journal remain the source of truth across engine restarts.

## Native drivers

### Claude Code

`ClaudeHarness` speaks the CLI's stream-json protocol directly. JSONL frames
are normalized into text, reasoning, tool, usage, question, and terminal events.
Permission requests use Claude's stdio control channel; questions are routed to
the input panel. The CLI's `result` frame ends a turn, and
`parent_tool_use_id` tags background subagent events so they remain separate
from the parent transcript. Model discovery uses the driver's catalog and
short-lived CLI probes.

### Codex

`CodexHarness` starts `codex app-server` and speaks JSON-RPC 2.0. It maps
`thread/start`/`thread/resume`, `turn/start`, `turn/steer`, and
`turn/interrupt`, together with item lifecycle and token-usage notifications.
Child app-server threads are routed as tagged subagent events. Model and
reasoning options come from the Codex catalog and the app-server discovery
surface. The driver is validated against the experimental API used by
codex-cli 0.146.1, so the wire should be rechecked when that CLI changes.

### Cursor

`CursorHarness` runs the pinned `@cursor/sdk` through the managed
`zeron-cursor-shim.mjs` Node process. The SDK provides the agent runtime and
full nested subagent transcript; the shim translates its JSONL frames into
Rust events. SDK credentials are separate from `cursor-agent login`.

For model discovery, the harness first uses the authenticated
`cursor-agent models` catalog, then tries SDK discovery, and finally falls
back to the minimal Auto/Composer list. The CLI fallback matters because the
Cursor CLI and SDK can have separate credential stores.

## ACP driver

`AcpHarness` is retained for Grok, Hermes, and Pi. It performs the ACP v1
handshake, creates or loads a session, maps `session/update` notifications,
discovers model/config options, and uses the prompt response's stop reason for
turn completion. Permission requests are auto-accepted for unattended runs;
question-shaped requests use the engine input bridge. Agents without a
mid-turn steering extension queue steering for the next prompt.

## Model discovery and completion

Model discovery is intentionally short-lived and cached only after a
non-empty catalog succeeds. Native drivers use their own catalogs or CLI
surfaces; ACP drivers probe the agent's advertised model/config options. The
picker normalizes aliases and keeps model options separate from reasoning
levels.

Native drivers report terminal completion from the agent's own terminal frame.
ACP drivers use the protocol stop reason, with the engine watchdog retained as
a backstop for adapter-mediated agents. A run must always finish with one
common `Done` event, including interruption and protocol failure paths.
