# Local architecture

Zeron is a single-machine coding-agent runtime in this fork. The desktop UI
and headless daemon use the same Rust engine and the same localhost RPC
protocol; neither requires an account or a hosted service.

## Runtime

```text
desktop UI ─┐
            ├─ local RPC ── engine ── harness process
headless ───┘                  ├────── SQLite snapshots + command ledger
                               ├────── workspace registry
                               ├────── repositories and checkout diffs
                               ├────── terminals and run journals
                               └────── local attachments and agent accounts
```

- `apps/zeron` provides the local CLI and daemon lifecycle.
- `crates/engine` owns sessions, transcripts, the workspace registry, local
  persistence, repositories, terminals, uploads, and harness execution.
- `crates/rpc` is the typed control plane over in-memory transport or localhost
  WebSocket IPC.
- `crates/ui` is a desktop viewport over that control plane.
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

The UI may embed the engine or connect to a separately running local daemon.
The daemon owns a data-directory lock and serves only localhost IPC. Agent
harnesses are child processes launched by the local engine; they are not
remote workers.

## Removed from this fork

This local-only branch does not contain the Cloudflare Worker/Durable Objects
edge service, WorkOS account and organization flows, remote device relays,
multi-device chat or registry rooms, the iOS sync peer, the marketing site,
redirect deployment, or cloud/update deployment workflows.
