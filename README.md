# Comet

Backend crates for a single-machine coding-agent runtime: sessions,
transcripts, workspace state, repositories, terminals, uploads, and the agent
harness adapters, behind one typed localhost RPC control plane.

This repo contains no UI and no binary. It is a library workspace consumed as a
set of path or git dependencies by whatever host drives it.

Everything stays on the machine that runs the engine. No account, cloud worker,
sync service, or network connection is required.

## Crates

| Crate | Role |
| --- | --- |
| `zeron-proto` | Shared wire types and the derivations consumers must agree on |
| `zeron-doc` | CRDT documents (loro) for workspace and chat state |
| `zeron-sync` | SQLite-backed local snapshots + processed-command ledger |
| `zeron-harness` | Agent harness adapters (ACP, Claude, Codex, OpenCode, Cursor) |
| `zeron-engine` | Sessions, transcripts, workspace registry, repositories, terminals, uploads, harness execution |
| `zeron-rpc` | Typed control plane over in-memory transport or localhost WebSocket IPC |

## Consuming

Path form, with comet checked out next to the consumer:

```toml
zeron-engine = { path = "../comet/crates/engine" }
```

Git form, for CI or standalone clones:

```toml
zeron-engine = { git = "https://github.com/balnoumair/comet", rev = "<pin>" }
```

The engine stores its data under `~/.zeron` by default (`ZERON_DATA_DIR` to
override) and exposes its control plane on localhost only.

## Develop

```bash
cargo check --workspace
cargo test --workspace
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the runtime layout and for the rule
on what belongs in this repo.

Licensed under the [MIT License](LICENSE).
