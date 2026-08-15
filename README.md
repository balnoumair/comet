# Zeron

Run coding agents locally with a desktop UI or a headless daemon. Sessions,
workspace metadata, transcripts, attachments, terminals, and agent-account
settings stay on the machine that runs Zeron.

## Run locally

```bash
cargo run -p zeron -- status
cargo run -p zeron -- daemon start
```

The engine stores its data under `~/.zeron` by default and exposes its local
control plane on localhost. No account, cloud worker, sync service, or network
connection is required to create or continue sessions.

Useful commands:

```bash
zeron status
zeron headless
zeron daemon start|stop|restart|status
```

On macOS, the desktop UI can be built from the workspace with `cargo run -p
zeron-ui`. The same local engine is used in-process or through the localhost
daemon.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the local runtime layout.

Licensed under the [MIT License](LICENSE).
