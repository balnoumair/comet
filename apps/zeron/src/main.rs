//! zeron — headed by default; `zeron headless` runs the engine alone. Both start
//! local-only without credentials. `zeron login` and `zeron logout` select the
//! profile used by the next engine start without mutating a live runtime.

mod daemon;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "zeron", about = "Local controller for coding agents")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the local engine without a UI.
    Headless,
    /// Show local workspace and engine configuration.
    Status,
    /// Manage `zeron headless` as a background service (launchd / systemd --user).
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Install, enable, and start the service (captures ZERON_* env).
    Install,
    /// Stop and remove the service.
    Uninstall,
    /// Start the installed service.
    Start,
    /// Stop the service.
    Stop,
    /// Restart the service.
    Restart,
    /// Show the service manager's view of the daemon.
    Status,
}

/// mimalloc: system malloc (macOS libmalloc especially) never returns the
/// streaming churn's high-water pages, so transient allocation became
/// permanent RSS (docs/memory-plan.md §1).
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Long-running modes log at info, one-shot CLI commands at warn (RUST_LOG
    // overrides either).
    // loro's internal block-encode diagnostics log at info and flood
    // journald on every snapshot export — enough to fill a disk on a
    // long-running headless host. Quiet them by default (RUST_LOG still
    // overrides the whole filter).
    let long_running = matches!(&cli.command, None | Some(Command::Headless));
    let default_filter = if long_running {
        "info,loro_internal=warn,loro=warn"
    } else {
        "warn"
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| default_filter.into());
    // Long-running modes mirror stdout logging to {data_dir}/logs — a headed
    // app launched from Finder has no visible stdout, which left every local
    // wedge report ("stale until restart") with zero diagnostics even though
    // the engine logs the exact failure line. One file per launch, previous
    // launch kept as `.old`.
    let log_file = if long_running {
        let mode = if cli.command.is_some() {
            "headless"
        } else {
            "headed"
        };
        open_log_file(mode)
    } else {
        None
    };
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        let registry = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer());
        match log_file {
            Some(file) => registry
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_ansi(false)
                        .with_writer(std::sync::Arc::new(file)),
                )
                .init(),
            None => registry.init(),
        }
    }

    match cli.command {
        Some(Command::Headless) => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(async {
                let engine = zeron_engine::Engine::new(engine_config_from_env());
                engine.run().await
            })
        }
        Some(Command::Status) => {
            let config = engine_config_from_env();
            println!("Mode:     local only");
            println!("Data:     {}", config.data_dir.display());
            println!("IPC:      127.0.0.1:{}", config.ipc_port);
            println!("Harness:  {:?}", config.default_harness);
            Ok(())
        }
        Some(Command::Daemon { command }) => match command {
            DaemonCommand::Install => daemon::install(&engine_config_from_env().data_dir),
            DaemonCommand::Uninstall => daemon::uninstall(),
            DaemonCommand::Start => daemon::start(),
            DaemonCommand::Stop => daemon::stop(),
            DaemonCommand::Restart => daemon::restart(),
            DaemonCommand::Status => daemon::status(),
        },
        None => {
            // Headed: the UI probes ZERON_IPC_PORT and connects to a running
            // daemon, or embeds the engine in-process (ARCHITECTURE §1).
            zeron_ui::run_app(zeron_ui::UiConfig {
                data_dir: std::env::var_os("ZERON_DATA_DIR")
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(dirs_data_dir),
                ipc_port: std::env::var("ZERON_IPC_PORT")
                    .ok()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(27654),
                default_harness: zeron_ui::HarnessId::ClaudeCode,
            });
            Ok(())
        }
    }
}

/// Resolve the local engine configuration shared by `headless` and `status`.
fn engine_config_from_env() -> zeron_engine::EngineConfig {
    zeron_engine::EngineConfig {
        data_dir: std::env::var_os("ZERON_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(dirs_data_dir),
        ipc_port: std::env::var("ZERON_IPC_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(27654),
        default_harness: harness_from_env(),
    }
}

/// `ZERON_HARNESS` (kebab-case id) picks the default harness for chats without a
/// config row — `mock` powers the e2e smoke; default `claude-code`.
fn harness_from_env() -> zeron_engine::HarnessId {
    match std::env::var("ZERON_HARNESS").as_deref().map(str::trim) {
        Ok("mock") => zeron_engine::HarnessId::Mock,
        Ok("codex") => zeron_engine::HarnessId::Codex,
        Ok("cursor") => zeron_engine::HarnessId::Cursor,
        Ok("grok") => zeron_engine::HarnessId::Grok,
        Ok("hermes") => zeron_engine::HarnessId::Hermes,
        Ok("pi") => zeron_engine::HarnessId::Pi,
        _ => zeron_engine::HarnessId::ClaudeCode,
    }
}

fn dirs_data_dir() -> std::path::PathBuf {
    let home = std::path::PathBuf::from(std::env::var_os("HOME").expect("HOME not set"));
    let dir = home.join(".zeron");
    // One-shot 0.2.0 migration: adopt the pre-rename data dir (local profile,
    // device identity, prefs) instead of starting fresh.
    if !dir.exists() {
        let old = home.join(".comet-native");
        if old.exists() && std::fs::rename(&old, &dir).is_ok() {
            eprintln!("migrated data dir {} -> {}", old.display(), dir.display());
        }
    }
    dir
}

/// `{data_dir}/logs/zeron-{mode}.log`, previous launch preserved as `.old`.
/// Headed and headless are separate files so an embedded-engine app and a
/// daemon on the same machine never interleave writes.
///
/// The returned file holds an exclusive `flock` for the process lifetime:
/// rotate-on-launch is only safe when nothing is still WRITING the current
/// file. On 2026-08-04 a dev build launched twice next to the running
/// installed app — the first rename put the daemon's live log at `.old`, the
/// second unlinked it entirely, and the daemon spent the rest of the incident
/// logging to an orphaned inode (an entire day of sync diagnostics gone at
/// the exact moment they were needed). A launch that finds the canonical file
/// locked logs to `zeron-{mode}.{pid}.log` instead; the next lock-holding
/// launch sweeps pid-suffixed files older than a week.
fn open_log_file(mode: &str) -> Option<std::fs::File> {
    let dir = std::env::var_os("ZERON_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(dirs_data_dir)
        .join("logs");
    open_log_file_in(&dir, mode)
}

/// Dir-parameterized body of [`open_log_file`] (unit-testable without env).
fn open_log_file_in(dir: &std::path::Path, mode: &str) -> Option<std::fs::File> {
    std::fs::create_dir_all(dir).ok()?;
    let path = dir.join(format!("zeron-{mode}.log"));
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // Probe the CURRENT inode for a live writer before touching it.
        let preexisting = path.exists();
        let existing = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .ok()?;
        let rc = unsafe { libc::flock(existing.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            // A live process owns the canonical log — leave it alone.
            return std::fs::File::create(
                dir.join(format!("zeron-{mode}.{}.log", std::process::id())),
            )
            .ok();
        }
        // No live writer: rotate, create fresh, and lock it as ours. (The
        // probe's flock dies with `existing`; a first-ever launch has nothing
        // to rotate — the probe itself created the empty file.)
        drop(existing);
        if preexisting {
            let _ = std::fs::rename(&path, dir.join(format!("zeron-{mode}.log.old")));
        }
        let file = std::fs::File::create(&path).ok()?;
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        sweep_stale_pid_logs(dir, mode);
        Some(file)
    }
    #[cfg(not(unix))]
    {
        let _ = std::fs::rename(&path, dir.join(format!("zeron-{mode}.log.old")));
        std::fs::File::create(&path).ok()
    }
}

#[cfg(all(test, unix))]
mod log_file_tests {
    use super::open_log_file_in;

    #[test]
    fn second_launch_never_rotates_a_live_processes_log() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        // First launch owns the canonical file and keeps writing.
        let first = open_log_file_in(dir, "headed").expect("first log");
        assert!(dir.join("zeron-headed.log").is_file());
        // Second launch while the first is alive: canonical file untouched,
        // pid-suffixed overflow file instead (the 2026-08-04 clobber).
        let second = open_log_file_in(dir, "headed").expect("second log");
        let pid_path = dir.join(format!("zeron-headed.{}.log", std::process::id()));
        assert!(pid_path.is_file(), "expected pid-suffixed overflow log");
        assert!(
            !dir.join("zeron-headed.log.old").exists(),
            "live canonical log must not be rotated away"
        );
        drop(second);
        // After the owner exits, a fresh launch rotates normally.
        drop(first);
        let third = open_log_file_in(dir, "headed").expect("third log");
        assert!(
            dir.join("zeron-headed.log.old").is_file(),
            "rotation resumes"
        );
        drop(third);
    }
}

/// Delete `zeron-{mode}.{pid}.log` overflow files older than a week — they
/// only exist when a second instance raced a live one for the canonical log.
#[cfg(unix)]
fn sweep_stale_pid_logs(dir: &std::path::Path, mode: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let prefix = format!("zeron-{mode}.");
    let week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(middle) = name
            .strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix(".log"))
        else {
            continue;
        };
        if !middle.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > week);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
