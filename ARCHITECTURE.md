# Local architecture

Comet is the backend half of Zeron, a single-machine coding-agent runtime.
The desktop UI and the headless daemon live in the
[zeron](https://github.com/balnoumair/zeron) repo; both embed or connect to
the engine in this repo over the same localhost RPC protocol. Neither
requires an account or a hosted service.

## Runtime

```text
desktop UI ─┐                     (hosts live in the zeron repo)
            ├─ local RPC ── engine ── harness process
headless ───┘                  ├────── SQLite snapshots + command ledger
                               ├────── workspace registry
                               ├────── repositories and checkout diffs
                               ├────── terminals and run journals
                               └────── local attachments and agent accounts
```

- `crates/engine` owns sessions, transcripts, the workspace registry, local
  persistence, repositories, terminals, uploads, and harness execution.
- `crates/rpc` is the typed control plane over in-memory transport or localhost
  WebSocket IPC.
- `crates/harness` holds the agent harness adapters (child processes launched
  by the engine).
- `crates/proto` and `crates/doc` hold the shared protocol types and the CRDT
  document model.
- `crates/sync` is intentionally limited to SQLite-backed local snapshots and
  the processed-command ledger. It contains no network transport.

## Storage

The default data directory is `~/.zeron` (or `ZERON_DATA_DIR`). A stable local
profile ID is stored in `local-profile.json`; workspace and chat documents are
stored in `profiles/local/docs.sqlite3`. Uploads remain under the same local
profile. The engine always reports `WorkspaceScope::Local`.

Snapshots are saved locally after document changes. Commands are claimed in
the local ledger before execution so a restart cannot execute the same command
twice.

## Process boundaries

A host may embed the engine or connect to a separately running local daemon.
The daemon owns a data-directory lock and serves only localhost IPC. Agent
harnesses are child processes launched by the local engine; they are not
remote workers.

## Repo boundaries

The UI, the `zeron` binary (headed + headless), and packaging moved to the
[zeron](https://github.com/balnoumair/zeron) repo; the GPUI design system and
syntax highlighting live in
[onyx-ui](https://github.com/balnoumair/onyx-ui). This repo keeps only the
backend crates so upstream intake stays clean.

## Removed from this fork

This local-only fork does not contain the Cloudflare Worker/Durable Objects
edge service, WorkOS account and organization flows, remote device relays,
multi-device chat or registry rooms, the iOS sync peer, the marketing site,
redirect deployment, or cloud/update deployment workflows.
