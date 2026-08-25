# Architecture

Comet is a single-machine coding-agent backend, shipped as a Rust library
workspace. It owns sessions, transcripts, workspace state, repositories,
terminals, uploads, and the agent harness adapters, and exposes all of it over
a typed localhost RPC control plane.

There is no UI and no binary here. A host process embeds the engine or connects
to a local daemon over that RPC protocol. Nothing in this repo assumes what the
host is, or that a host renders anything at all.

No account, cloud worker, sync service, or network connection is required.

## Runtime

```text
host process ── local RPC ── engine ── harness child process
                               ├────── SQLite snapshots + command ledger
                               ├────── workspace registry
                               ├────── repositories and checkout diffs
                               ├────── terminals and run journals
                               └────── local attachments and agent accounts
```

- `crates/engine` owns sessions, transcripts, the workspace registry, local
  persistence, repositories, terminals, uploads, and harness execution.
- `crates/rpc` is the typed control plane, over an in-memory transport or a
  localhost WebSocket.
- `crates/harness` holds the agent harness adapters — ACP, Claude, Codex,
  OpenCode, Cursor — each a child process the engine launches and drives.
- `crates/proto` holds the wire types, plus the derivations that must not
  diverge between the engine and its consumers.
- `crates/doc` holds the CRDT (loro) document model for workspace and chat
  state.
- `crates/sync` is limited to SQLite-backed local snapshots and the
  processed-command ledger. It contains no network transport.

## Storage

The default data directory is `~/.zeron` (or `ZERON_DATA_DIR`). A stable local
profile ID is stored in `local-profile.json`; workspace and chat documents are
stored in `profiles/local/docs.sqlite3`. Uploads stay under the same local
profile. The engine always reports `WorkspaceScope::Local`.

Snapshots are saved locally after document changes. Commands are claimed in the
local ledger before execution, so a restart cannot execute the same command
twice.

## Process boundaries

A host may embed the engine in-process or connect to a separately running local
daemon. The daemon owns a data-directory lock and serves only localhost IPC.
Agent harnesses are child processes launched by the local engine; they are
never remote workers.

## What belongs here

Backend rules — anything the engine or a harness must enforce for itself. A
rule that only decides what a screen shows belongs to the host, even when it is
pure and testable.

The dividing case is session staleness: `SESSION_STALE_MS` and
`effective_indicator` live in `crates/proto` because the engine heartbeats
against that window and the harnesses reason about it, not because something
draws a status dot. Sort orders, grouping, relative-time formatting, and
tool-chip text went to the host for the opposite reason.

## Not in this repo

No UI, design system, packaging, or application binary.

No Cloudflare Worker or Durable Objects edge service, WorkOS account and
organization flows, remote device relays, multi-device chat or registry rooms,
iOS sync peer, marketing site, or cloud deployment workflows. Some
account-related RPC method names survive as inert protocol identifiers; the
engine implements no operation behind them.
