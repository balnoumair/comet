use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use zeron_proto::{Chat, CheckoutDiff, DiffFileSummary};

use crate::EngineError;
use crate::repos::{CheckoutIdentity, Repos};
use crate::workspace_host::WorkspaceHost;

pub const MAX_PATCH_BYTES: usize = 3 * 1024 * 1024;
pub const MAX_DIFF_SOURCE_BYTES: usize = 2 * 1024 * 1024;
const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
const REPAIR_INTERVAL: Duration = Duration::from_secs(120);
const MAX_WATCH_DIRS: usize = 8_000;
const EMPTY_TREE_SHA: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

#[derive(Debug, Clone)]
pub struct DiffSnapshot {
    pub branch: String,
    pub head_sha: Option<String>,
    pub patch: String,
    pub files: Vec<DiffFileSummary>,
    pub additions: u32,
    pub deletions: u32,
    pub truncated: bool,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffFileTextPair {
    pub old_text: Option<String>,
    pub new_text: Option<String>,
    pub old_content_hash: Option<String>,
    pub new_content_hash: Option<String>,
    pub binary: bool,
    pub truncated: bool,
}

struct CheckoutEntry {
    identity: CheckoutIdentity,
    chats: Mutex<Vec<Chat>>,
    checksum: Mutex<Option<String>>,
    orphaned_since: Mutex<Option<std::time::Instant>>,
    kick_tx: mpsc::UnboundedSender<()>,
    watchers: Mutex<Vec<notify::RecommendedWatcher>>,
}

#[derive(Debug, Clone)]
pub struct TurnSnapshot {
    pub root: PathBuf,
    pub tree: String,
    pub at: chrono::DateTime<chrono::Utc>,
}

struct DiffSyncInner {
    repos: Repos,
    workspace: WorkspaceHost,
    device_id: String,
    entries: Mutex<HashMap<String, Arc<CheckoutEntry>>>,
    reconcile_gate: tokio::sync::Mutex<()>,
    identities: Mutex<HashMap<String, CheckoutIdentity>>,
    orphan_grace: Duration,
    diffs_tx: watch::Sender<Vec<CheckoutDiff>>,
    turn_trees: Mutex<HashMap<String, TurnSnapshot>>,
    cancel: CancellationToken,
    supervisor: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct CheckoutDiffSync {
    inner: Arc<DiffSyncInner>,
}

impl CheckoutDiffSync {
    pub fn start(repos: Repos, workspace: WorkspaceHost, device_id: &str) -> Self {
        Self::start_with_orphan_grace(repos, workspace, device_id, REPAIR_INTERVAL)
    }

    #[doc(hidden)]
    pub fn start_with_orphan_grace(
        repos: Repos,
        workspace: WorkspaceHost,
        device_id: &str,
        orphan_grace: Duration,
    ) -> Self {
        let (diffs_tx, _) = watch::channel(Vec::new());
        let sync = Self {
            inner: Arc::new(DiffSyncInner {
                repos,
                workspace: workspace.clone(),
                device_id: device_id.to_string(),
                entries: Mutex::new(HashMap::new()),
                reconcile_gate: tokio::sync::Mutex::new(()),
                identities: Mutex::new(HashMap::new()),
                orphan_grace,
                diffs_tx,
                turn_trees: Mutex::new(HashMap::new()),
                cancel: CancellationToken::new(),
                supervisor: Mutex::new(None),
            }),
        };
        let task = tokio::spawn(diff_sync_task(
            Arc::downgrade(&sync.inner),
            workspace.watch_chats(),
            sync.inner.cancel.clone(),
        ));
        *lock(&sync.inner.supervisor) = Some(task);
        sync
    }

    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        let task = lock(&self.inner.supervisor).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    pub fn watch_diffs(&self) -> watch::Receiver<Vec<CheckoutDiff>> {
        self.inner.diffs_tx.subscribe()
    }

    pub async fn reconcile_now(&self) {
        let chats = self.inner.workspace.watch_chats().borrow().clone();
        reconcile(&self.inner, chats, false).await;
    }

    #[doc(hidden)]
    pub async fn repair_now(&self) {
        let chats = self.inner.workspace.watch_chats().borrow().clone();
        reconcile(&self.inner, chats, true).await;
        self.sync_all();
    }

    pub fn sync_all(&self) {
        for entry in lock(&self.inner.entries).values() {
            let _ = entry.kick_tx.send(());
        }
    }

    pub fn note_turn_start(&self, chat_id: &str, cwd: &str) {
        let inner = Arc::downgrade(&self.inner);
        let chat_id = chat_id.to_string();
        let cwd = PathBuf::from(cwd);
        tokio::spawn(async move {
            let Some(inner) = inner.upgrade() else { return };
            let identity = match inner.repos.checkout_identity(&cwd).await {
                Ok(identity) => identity,
                Err(_) => return,
            };
            match snapshot_tree(&identity.root).await {
                Ok(tree) => {
                    lock(&inner.turn_trees).insert(
                        chat_id,
                        TurnSnapshot {
                            root: identity.root,
                            tree,
                            at: chrono::Utc::now(),
                        },
                    );
                }
                Err(err) => {
                    tracing::debug!(chat = %chat_id, error = %err,
                        "diff-sync: turn snapshot failed");
                }
            }
        });
    }

    pub fn turn_snapshot(&self, chat_id: &str) -> Option<TurnSnapshot> {
        lock(&self.inner.turn_trees).get(chat_id).cloned()
    }
}

async fn resolve_identity(
    inner: &Arc<DiffSyncInner>,
    cwd: &str,
    fresh: bool,
) -> Option<CheckoutIdentity> {
    if !fresh && let Some(identity) = lock(&inner.identities).get(cwd).cloned() {
        return Some(identity);
    }
    match inner.repos.checkout_identity(Path::new(cwd)).await {
        Ok(identity) => {
            lock(&inner.identities).insert(cwd.to_string(), identity.clone());
            Some(identity)
        }
        Err(err) => {
            if !Path::new(cwd).exists() {
                lock(&inner.identities).remove(cwd);
                tracing::debug!(cwd = %cwd, error = %err, "diff-sync: checkout gone");
                return None;
            }
            let cached = lock(&inner.identities).get(cwd).cloned();
            match &cached {
                Some(_) => tracing::debug!(cwd = %cwd, error = %err,
                    "diff-sync: identity resolve failed; keeping memoized identity"),
                None => tracing::debug!(cwd = %cwd, error = %err, "diff-sync: not a checkout"),
            }
            cached
        }
    }
}

async fn reconcile(inner: &Arc<DiffSyncInner>, chats: Vec<Chat>, fresh: bool) {
    let _gate = inner.reconcile_gate.lock().await;
    let mut groups: HashMap<String, (CheckoutIdentity, Vec<Chat>)> = HashMap::new();
    let mut resolved: HashMap<String, Option<CheckoutIdentity>> = HashMap::new();
    for chat in chats {
        if chat.device_id != inner.device_id {
            continue;
        }
        let Some(cwd) = chat.cwd.clone() else {
            continue;
        };
        let identity = match resolved.get(&cwd) {
            Some(identity) => identity.clone(),
            None => {
                let identity = resolve_identity(inner, &cwd, fresh).await;
                resolved.insert(cwd.clone(), identity.clone());
                identity
            }
        };
        let Some(identity) = identity else {
            continue;
        };
        if chat.checkout_id.as_deref() != Some(identity.id.as_str())
            && let Err(err) = inner.workspace.set_chat_checkout(&chat.id, &identity.id)
        {
            tracing::debug!(chat = %chat.id, error = %err, "diff-sync: checkoutId write failed");
        }
        groups
            .entry(identity.id.clone())
            .or_insert_with(|| (identity, Vec::new()))
            .1
            .push(chat);
    }

    let removed: Vec<String> = {
        let now = std::time::Instant::now();
        let mut entries = lock(&inner.entries);
        let mut removed = Vec::new();
        for (id, entry) in entries.iter() {
            if groups.contains_key(id) {
                *lock(&entry.orphaned_since) = None;
                continue;
            }
            let mut orphaned = lock(&entry.orphaned_since);
            match *orphaned {
                None => *orphaned = Some(now),
                Some(since) if now.duration_since(since) >= inner.orphan_grace => {
                    removed.push(id.clone());
                }
                Some(_) => {}
            }
        }
        for id in &removed {
            entries.remove(id);
        }
        removed
    };
    if !removed.is_empty() {
        publish_watch(inner);
    }

    for (checkout_id, (identity, chats)) in groups {
        let existing = lock(&inner.entries).get(&checkout_id).cloned();
        match existing {
            Some(entry) => {
                let has_new = {
                    let mut held = lock(&entry.chats);
                    let previous: HashSet<String> = held.iter().map(|c| c.id.clone()).collect();
                    let has_new = chats.iter().any(|c| !previous.contains(&c.id));
                    *held = chats;
                    has_new
                };
                if has_new {
                    let _ = entry.kick_tx.send(());
                }
            }
            None => add_entry(inner, identity, chats),
        }
    }
}

fn exceeds_watch_budget(root: &Path) -> bool {
    let mut queue = std::collections::VecDeque::from([root.to_path_buf()]);
    let mut seen = 0usize;
    while let Some(dir) = queue.pop_front() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                seen += 1;
                if seen > MAX_WATCH_DIRS {
                    return true;
                }
                queue.push_back(entry.path());
            }
        }
    }
    false
}

fn watch_targets(identity: &CheckoutIdentity) -> Vec<PathBuf> {
    let mut targets = Vec::new();
    let root_fits = !exceeds_watch_budget(&identity.root);
    if root_fits {
        targets.push(identity.root.clone());
    } else {
        tracing::info!(path = %identity.root.display(),
            "diff-sync: tree too large to watch live; watching the git dir, edits ride the repair tick");
    }
    let git_covered = root_fits && identity.git_dir.starts_with(&identity.root);
    if !git_covered && !exceeds_watch_budget(&identity.git_dir) {
        targets.push(identity.git_dir.clone());
    }
    targets
}

fn add_entry(inner: &Arc<DiffSyncInner>, identity: CheckoutIdentity, chats: Vec<Chat>) {
    let (kick_tx, kick_rx) = mpsc::unbounded_channel();
    let entry = Arc::new(CheckoutEntry {
        identity,
        chats: Mutex::new(chats),
        checksum: Mutex::new(None),
        orphaned_since: Mutex::new(None),
        kick_tx: kick_tx.clone(),
        watchers: Mutex::new(Vec::new()),
    });
    lock(&inner.entries).insert(entry.identity.id.clone(), entry.clone());
    tokio::spawn(entry_task(
        Arc::downgrade(inner),
        Arc::downgrade(&entry),
        kick_rx,
        inner.cancel.clone(),
    ));
    let _ = kick_tx.send(());

    let weak = Arc::downgrade(&entry);
    tokio::task::spawn_blocking(move || {
        let Some(entry) = weak.upgrade() else {
            return;
        };
        let watchers = build_watchers(&entry.identity, &kick_tx);
        *lock(&entry.watchers) = watchers;
        let _ = kick_tx.send(());
    });
}

fn build_watchers(
    identity: &CheckoutIdentity,
    kick_tx: &mpsc::UnboundedSender<()>,
) -> Vec<notify::RecommendedWatcher> {
    let mut watchers = Vec::new();
    for target in watch_targets(identity) {
        let tx = kick_tx.clone();
        let watcher =
            notify::recommended_watcher(move |event: Result<notify::Event, notify::Error>| {
                if event.is_ok() {
                    let _ = tx.send(());
                }
            });
        match watcher {
            Ok(mut watcher) => {
                use notify::Watcher as _;
                match watcher.watch(&target, notify::RecursiveMode::Recursive) {
                    Ok(()) => watchers.push(watcher),
                    Err(err) => {
                        tracing::debug!(path = %target.display(), error = %err, "diff-sync: watch failed")
                    }
                }
            }
            Err(err) => tracing::debug!(error = %err, "diff-sync: watcher create failed"),
        }
    }
    watchers
}

async fn entry_task(
    inner: Weak<DiffSyncInner>,
    entry: Weak<CheckoutEntry>,
    mut kick_rx: mpsc::UnboundedReceiver<()>,
    cancel: CancellationToken,
) {
    while kick_rx.recv().await.is_some() {
        loop {
            match tokio::time::timeout(WATCH_DEBOUNCE, kick_rx.recv()).await {
                Ok(Some(())) => continue,
                Ok(None) => return,
                Err(_) => break,
            }
        }
        let (Some(inner), Some(entry)) = (inner.upgrade(), entry.upgrade()) else {
            return;
        };
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = sync_entry(&inner, &entry) => {}
        }
    }
}

async fn sync_entry(inner: &Arc<DiffSyncInner>, entry: &Arc<CheckoutEntry>) {
    let snapshot = match capture_diff(&inner.repos, &entry.identity.root).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::debug!(checkout = %entry.identity.root.display(), error = %err,
                "diff-sync: capture failed");
            return;
        }
    };

    if let Ok(branch_to_write) = inner.repos.current_branch(&entry.identity.root).await
        && !branch_to_write.is_empty()
    {
        let chats = lock(&entry.chats).clone();
        for chat in &chats {
            if chat.branch.as_deref() != Some(branch_to_write.as_str())
                && let Err(err) = inner.workspace.set_chat_branch(&chat.id, &branch_to_write)
            {
                tracing::debug!(chat = %chat.id, error = %err, "diff-sync: branch write failed");
            }
        }
    }

    if lock(&entry.checksum).as_deref() == Some(snapshot.checksum.as_str()) {
        return;
    }
    *lock(&entry.checksum) = Some(snapshot.checksum.clone());

    let diff = CheckoutDiff {
        checkout_id: entry.identity.id.clone(),
        device_id: inner.device_id.clone(),
        cwd: entry.identity.root.to_string_lossy().to_string(),
        patch: snapshot.patch.clone(),
        files: snapshot.files.clone(),
        additions: snapshot.additions,
        deletions: snapshot.deletions,
        truncated: snapshot.truncated,
        checksum: snapshot.checksum.clone(),
        updated_at: chrono::Utc::now(),
    };
    {
        let entries = lock(&inner.entries);
        if !entries.contains_key(&entry.identity.id) {
            return;
        }
    }
    publish_watch_with(inner, Some(diff));
}

fn publish_watch_with(inner: &Arc<DiffSyncInner>, updated: Option<CheckoutDiff>) {
    let live: HashSet<String> = lock(&inner.entries).keys().cloned().collect();
    inner.diffs_tx.send_modify(|diffs| {
        diffs.retain(|d| live.contains(&d.checkout_id));
        if let Some(updated) = updated {
            match diffs
                .iter_mut()
                .find(|d| d.checkout_id == updated.checkout_id)
            {
                Some(slot) => *slot = updated,
                None => diffs.push(updated),
            }
        }
        diffs.sort_by(|a, b| a.checkout_id.cmp(&b.checkout_id));
    });
}

fn publish_watch(inner: &Arc<DiffSyncInner>) {
    publish_watch_with(inner, None);
}

async fn diff_sync_task(
    inner: Weak<DiffSyncInner>,
    mut chats_rx: watch::Receiver<Vec<Chat>>,
    cancel: CancellationToken,
) {
    let mut repair = tokio::time::interval(REPAIR_INTERVAL);
    repair.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    repair.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            changed = chats_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let Some(inner) = inner.upgrade() else { break };
                let chats = chats_rx.borrow_and_update().clone();
                reconcile(&inner, chats, false).await;
            }
            _ = repair.tick() => {
                let Some(inner) = inner.upgrade() else { break };
                let chats = chats_rx.borrow().clone();
                reconcile(&inner, chats, true).await;
                for entry in lock(&inner.entries).values() {
                    let _ = entry.kick_tx.send(());
                }
            }
        }
    }
}

struct Capture {
    stdout: Vec<u8>,
    truncated: bool,
}

async fn capture_git(cwd: &Path, args: &[&str], max_bytes: usize) -> Result<Capture, EngineError> {
    let mut cmd = tokio::process::Command::new("git");
    cmd.arg("-C").arg(cwd).args(args);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| EngineError::Other(format!("git spawn failed: {e}")))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| EngineError::Other("git stdout unavailable".into()))?;
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 64 * 1024];
    let mut truncated = false;
    loop {
        let n = stdout
            .read(&mut buf)
            .await
            .map_err(|e| EngineError::Other(format!("git read failed: {e}")))?;
        if n == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(out.len());
        if n > remaining {
            out.extend_from_slice(&buf[..remaining]);
            truncated = true;
            let _ = child.start_kill();
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    let output = child
        .wait_with_output()
        .await
        .map_err(|e| EngineError::Other(format!("git wait failed: {e}")))?;
    if !output.status.success() && !truncated {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        return Err(EngineError::Other(if message.is_empty() {
            format!("git exited {}", output.status)
        } else {
            format!("git: {message}")
        }));
    }
    Ok(Capture {
        stdout: out,
        truncated,
    })
}

fn split_z(value: &[u8]) -> Vec<String> {
    value
        .split(|b| *b == 0)
        .filter(|part| !part.is_empty())
        .map(|part| String::from_utf8_lossy(part).to_string())
        .collect()
}

fn parse_name_status(value: &[u8]) -> Vec<DiffFileSummary> {
    let fields = split_z(value);
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < fields.len() {
        let raw = fields[i].clone();
        i += 1;
        let code = raw.chars().next().unwrap_or('M');
        let Some(first) = fields.get(i).cloned() else {
            break;
        };
        i += 1;
        let renamed = code == 'R' || code == 'C';
        let second = if renamed {
            let s = fields.get(i).cloned();
            i += 1;
            s
        } else {
            None
        };
        let status = match code {
            'A' => "added",
            'D' => "deleted",
            'R' => "renamed",
            'C' => "copied",
            'U' => "unmerged",
            _ => "modified",
        };
        out.push(DiffFileSummary {
            path: second.clone().unwrap_or_else(|| first.clone()),
            old_path: second.is_some().then_some(first),
            status: status.to_string(),
            additions: 0,
            deletions: 0,
            binary: false,
        });
    }
    out
}

fn apply_numstat(files: &mut [DiffFileSummary], value: &[u8]) {
    let records: Vec<String> = value
        .split(|b| *b == 0)
        .map(|part| String::from_utf8_lossy(part).to_string())
        .collect();
    let mut i = 0usize;
    while i < records.len() {
        let record = &records[i];
        if record.is_empty() {
            i += 1;
            continue;
        }
        let mut parts = record.splitn(3, '\t');
        let adds = parts.next().unwrap_or_default().to_string();
        let dels = parts.next().unwrap_or_default().to_string();
        let inline_path = parts.next().unwrap_or_default().to_string();
        let path = if inline_path.is_empty() {
            let new_path = records.get(i + 2).cloned().unwrap_or_default();
            i += 2;
            new_path
        } else {
            inline_path
        };
        i += 1;
        if let Some(file) = files.iter_mut().find(|f| f.path == path) {
            file.additions = adds.parse().unwrap_or(0);
            file.deletions = dels.parse().unwrap_or(0);
            file.binary = adds == "-" || dels == "-";
        }
    }
}

fn quote_patch_path(path: &str) -> String {
    if path
        .chars()
        .any(|c| c.is_whitespace() || c == '"' || c == '\\')
    {
        serde_json::to_string(path).unwrap_or_else(|_| format!("\"{path}\""))
    } else {
        path.to_string()
    }
}

fn untracked_patch(path: &str, content: &str) -> String {
    let mut lines: Vec<&str> = content.split('\n').collect();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    let body: String = lines
        .iter()
        .map(|line| format!("+{line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let a = quote_patch_path(&format!("a/{path}"));
    let b = quote_patch_path(&format!("b/{path}"));
    format!(
        "diff --git {a} {b}\nnew file mode 100644\n--- /dev/null\n+++ {b}\n@@ -0,0 +1,{} @@\n{body}\n",
        lines.len()
    )
}

fn validate_diff_path(path: &str) -> Result<&Path, EngineError> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(EngineError::Other("diff path escapes checkout".into()));
    }
    Ok(path)
}

fn decode_diff_source(
    bytes: Vec<u8>,
) -> Result<(Option<String>, Option<String>, bool), EngineError> {
    if bytes.contains(&0) {
        return Ok((None, None, true));
    }
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => return Ok((None, None, true)),
    };
    let hash = crate::repos::hex(&Sha256::digest(text.as_bytes()));
    Ok((Some(text), Some(hash), false))
}

async fn read_worktree_source(root: &Path, path: &Path) -> Result<Capture, EngineError> {
    let canonical_root = tokio::fs::canonicalize(root)
        .await
        .map_err(|error| EngineError::Other(format!("canonical checkout: {error}")))?;
    let full = root.join(path);
    let metadata = tokio::fs::symlink_metadata(&full)
        .await
        .map_err(|error| EngineError::Other(format!("read diff file metadata: {error}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EngineError::Other(
            "diff source is not a regular checkout file".into(),
        ));
    }
    let canonical = tokio::fs::canonicalize(&full)
        .await
        .map_err(|error| EngineError::Other(format!("canonical diff file: {error}")))?;
    if !canonical.starts_with(&canonical_root) {
        return Err(EngineError::Other("diff source escapes checkout".into()));
    }
    if metadata.len() > MAX_DIFF_SOURCE_BYTES as u64 {
        return Ok(Capture {
            stdout: Vec::new(),
            truncated: true,
        });
    }
    let stdout = tokio::fs::read(&canonical)
        .await
        .map_err(|error| EngineError::Other(format!("read diff file: {error}")))?;
    Ok(Capture {
        stdout,
        truncated: false,
    })
}

async fn read_git_source(root: &Path, revision: &str, path: &Path) -> Result<Capture, EngineError> {
    let spec = format!("{revision}:{}", path.to_string_lossy());
    capture_git(root, &["cat-file", "blob", &spec], MAX_DIFF_SOURCE_BYTES).await
}

pub async fn read_diff_file_text(
    root: &Path,
    base: &str,
    file: &DiffFileSummary,
) -> Result<DiffFileTextPair, EngineError> {
    read_diff_file_text_at(root, base, None, file).await
}

pub(crate) async fn read_diff_file_text_at(
    root: &Path,
    base: &str,
    target: Option<&str>,
    file: &DiffFileSummary,
) -> Result<DiffFileTextPair, EngineError> {
    let new_path = validate_diff_path(&file.path)?;
    let old_path = validate_diff_path(file.old_path.as_deref().unwrap_or(&file.path))?;

    let old = if file.status == "added" {
        None
    } else {
        Some(read_git_source(root, base, old_path).await?)
    };
    let new = if file.status == "deleted" {
        None
    } else if let Some(target) = target {
        Some(read_git_source(root, target, new_path).await?)
    } else {
        Some(read_worktree_source(root, new_path).await?)
    };
    let truncated = old.as_ref().is_some_and(|source| source.truncated)
        || new.as_ref().is_some_and(|source| source.truncated);
    if truncated {
        return Ok(DiffFileTextPair {
            old_text: None,
            new_text: None,
            old_content_hash: None,
            new_content_hash: None,
            binary: false,
            truncated: true,
        });
    }
    let (old_text, old_content_hash, old_binary) = match old {
        Some(source) => decode_diff_source(source.stdout)?,
        None => (None, None, false),
    };
    let (new_text, new_content_hash, new_binary) = match new {
        Some(source) => decode_diff_source(source.stdout)?,
        None => (None, None, false),
    };
    let binary = old_binary || new_binary || file.binary;
    Ok(DiffFileTextPair {
        old_text: (!binary).then_some(old_text).flatten(),
        new_text: (!binary).then_some(new_text).flatten(),
        old_content_hash: (!binary).then_some(old_content_hash).flatten(),
        new_content_hash: (!binary).then_some(new_content_hash).flatten(),
        binary,
        truncated: false,
    })
}

pub(crate) async fn commit_diff_base(root: &Path, sha: &str) -> String {
    let parent_spec = format!("{sha}^");
    let parent = capture_git(root, &["rev-parse", "--verify", &parent_spec], 256)
        .await
        .map(|capture| String::from_utf8_lossy(&capture.stdout).trim().to_string())
        .unwrap_or_default();
    if parent.is_empty() {
        EMPTY_TREE_SHA.to_string()
    } else {
        parent
    }
}

pub async fn working_diff_base(root: &Path) -> Result<String, EngineError> {
    let head = capture_git(root, &["rev-parse", "--verify", "HEAD"], 256)
        .await
        .map(|capture| String::from_utf8_lossy(&capture.stdout).trim().to_string())
        .unwrap_or_default();
    Ok(if head.is_empty() {
        EMPTY_TREE_SHA.into()
    } else {
        head
    })
}

pub async fn capture_diff(repos: &Repos, root: &Path) -> Result<DiffSnapshot, EngineError> {
    capture_diff_against(repos, root, None).await
}

pub async fn capture_diff_against(
    repos: &Repos,
    root: &Path,
    base_override: Option<&str>,
) -> Result<DiffSnapshot, EngineError> {
    let head = capture_git(root, &["rev-parse", "--verify", "HEAD"], 256)
        .await
        .map(|c| String::from_utf8_lossy(&c.stdout).trim().to_string())
        .unwrap_or_default();
    let base: &str = match base_override {
        Some(base) => base,
        None if head.is_empty() => EMPTY_TREE_SHA,
        None => &head,
    };
    let branch = repos
        .current_branch(root)
        .await
        .unwrap_or_else(|_| "HEAD".into());

    let names = capture_git(
        root,
        &["diff", "--name-status", "-z", "--find-renames", base, "--"],
        2 * 1024 * 1024,
    )
    .await?;
    let nums = capture_git(
        root,
        &["diff", "--numstat", "-z", "--find-renames", base, "--"],
        2 * 1024 * 1024,
    )
    .await?;
    let tracked = capture_git(
        root,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--find-renames",
            "--unified=3",
            base,
            "--",
        ],
        MAX_PATCH_BYTES,
    )
    .await?;
    let status = capture_git(
        root,
        &["--no-optional-locks", "status", "--porcelain", "-z"],
        2 * 1024 * 1024,
    )
    .await?;

    let mut files = parse_name_status(&names.stdout);
    apply_numstat(&mut files, &nums.stdout);
    let mut patch = String::from_utf8_lossy(&tracked.stdout).to_string();
    let mut truncated = tracked.truncated || names.truncated || nums.truncated || status.truncated;

    if tracked.truncated {
        let boundary = patch.rfind('\n').unwrap_or(0);
        patch.truncate(boundary);
        patch.push_str("\n# Zeron diff truncated\n");
    }

    let mut untracked: Vec<String> = Vec::new();
    let records = split_z(&status.stdout);
    let mut i = 0usize;
    while i < records.len() {
        let record = &records[i];
        i += 1;
        if record.len() < 3 {
            continue;
        }
        let (code, path) = record.split_at(2);
        if code.starts_with('R') || code.starts_with('C') {
            i += 1;
        }
        if code == "??" {
            untracked.push(path.trim_start().to_string());
        }
    }
    untracked.sort();

    for path in untracked {
        let full = root.join(&path);
        let binary;
        let mut additions = 0u32;
        let size = tokio::fs::metadata(&full)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if size > MAX_PATCH_BYTES as u64 {
            binary = true;
            truncated = true;
        } else {
            match tokio::fs::read(&full).await {
                Ok(bytes) => {
                    binary = bytes.contains(&0);
                    if !binary {
                        let text = String::from_utf8_lossy(&bytes).to_string();
                        additions = if text.is_empty() {
                            0
                        } else {
                            (text.split('\n').count() - usize::from(text.ends_with('\n'))) as u32
                        };
                        let addition = untracked_patch(&path, &text);
                        if patch.len() + addition.len() <= MAX_PATCH_BYTES {
                            if !patch.is_empty() && !patch.ends_with('\n') {
                                patch.push('\n');
                            }
                            patch.push_str(&addition);
                        } else {
                            truncated = true;
                        }
                    }
                }
                Err(_) => continue,
            }
        }
        files.push(DiffFileSummary {
            path,
            old_path: None,
            status: "added".to_string(),
            additions,
            deletions: 0,
            binary,
        });
    }

    let additions: u32 = files.iter().map(|f| f.additions).sum();
    let deletions: u32 = files.iter().map(|f| f.deletions).sum();
    let files_json = serde_json::to_string(&files)
        .map_err(|e| EngineError::Other(format!("diff files serialize: {e}")))?;
    let mut hasher = Sha256::new();
    hasher.update(branch.as_bytes());
    hasher.update([0u8]);
    hasher.update(head.as_bytes());
    hasher.update([0u8]);
    hasher.update(patch.as_bytes());
    hasher.update([0u8]);
    hasher.update(files_json.as_bytes());
    hasher.update(if truncated { b"1" } else { b"0" });
    let checksum = crate::repos::hex(&hasher.finalize());

    Ok(DiffSnapshot {
        branch,
        head_sha: (!head.is_empty()).then_some(head),
        patch,
        files,
        additions,
        deletions,
        truncated,
        checksum,
    })
}

pub async fn capture_commit_diff(
    repos: &Repos,
    root: &Path,
    sha: &str,
) -> Result<DiffSnapshot, EngineError> {
    let base = commit_diff_base(root, sha).await;
    let branch = repos
        .current_branch(root)
        .await
        .unwrap_or_else(|_| "HEAD".into());
    let names = capture_git(
        root,
        &[
            "diff",
            "--name-status",
            "-z",
            "--find-renames",
            &base,
            sha,
            "--",
        ],
        2 * 1024 * 1024,
    )
    .await?;
    let nums = capture_git(
        root,
        &[
            "diff",
            "--numstat",
            "-z",
            "--find-renames",
            &base,
            sha,
            "--",
        ],
        2 * 1024 * 1024,
    )
    .await?;
    let tracked = capture_git(
        root,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--find-renames",
            "--unified=3",
            &base,
            sha,
            "--",
        ],
        MAX_PATCH_BYTES,
    )
    .await?;
    let mut files = parse_name_status(&names.stdout);
    apply_numstat(&mut files, &nums.stdout);
    let mut patch = String::from_utf8_lossy(&tracked.stdout).to_string();
    let truncated = tracked.truncated || names.truncated || nums.truncated;
    if tracked.truncated {
        let boundary = patch.rfind('\n').unwrap_or(0);
        patch.truncate(boundary);
        patch.push_str("\n# Comet diff truncated\n");
    }
    let additions: u32 = files.iter().map(|f| f.additions).sum();
    let deletions: u32 = files.iter().map(|f| f.deletions).sum();
    let files_json = serde_json::to_string(&files)
        .map_err(|e| EngineError::Other(format!("diff files serialize: {e}")))?;
    let mut hasher = Sha256::new();
    hasher.update(branch.as_bytes());
    hasher.update([0u8]);
    hasher.update(sha.as_bytes());
    hasher.update([0u8]);
    hasher.update(patch.as_bytes());
    hasher.update([0u8]);
    hasher.update(files_json.as_bytes());
    hasher.update(if truncated { b"1" } else { b"0" });
    let checksum = crate::repos::hex(&hasher.finalize());
    Ok(DiffSnapshot {
        branch,
        head_sha: Some(sha.to_string()),
        patch,
        files,
        additions,
        deletions,
        truncated,
        checksum,
    })
}

pub async fn merge_base(root: &Path, base_ref: &str) -> Result<String, EngineError> {
    let capture = capture_git(root, &["merge-base", base_ref, "HEAD"], 256).await?;
    let sha = String::from_utf8_lossy(&capture.stdout).trim().to_string();
    if sha.is_empty() {
        return Err(EngineError::Other(format!("no merge base with {base_ref}")));
    }
    Ok(sha)
}

pub async fn snapshot_tree(root: &Path) -> Result<String, EngineError> {
    let index = std::env::temp_dir().join(format!(
        "zeron-turn-index-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
    ));
    let run = |args: &[&str]| {
        let mut cmd = tokio::process::Command::new("git");
        cmd.arg("-C").arg(root).args(args);
        cmd.env("GIT_INDEX_FILE", &index);
        cmd.stdin(std::process::Stdio::null());
        cmd.output()
    };
    let added = run(&["add", "-A", "--ignore-errors", "."])
        .await
        .map_err(|e| EngineError::Other(format!("git add failed: {e}")))?;
    if !added.status.success() {
        let _ = tokio::fs::remove_file(&index).await;
        return Err(EngineError::Other(format!(
            "git add: {}",
            String::from_utf8_lossy(&added.stderr).trim()
        )));
    }
    let written = run(&["write-tree"])
        .await
        .map_err(|e| EngineError::Other(format!("git write-tree failed: {e}")));
    let _ = tokio::fs::remove_file(&index).await;
    let written = written?;
    if !written.status.success() {
        return Err(EngineError::Other(format!(
            "git write-tree: {}",
            String::from_utf8_lossy(&written.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&written.stdout).trim().to_string())
}

pub async fn capture_turn_diff(
    repos: &Repos,
    root: &Path,
    turn_tree: &str,
) -> Result<DiffSnapshot, EngineError> {
    let current = snapshot_tree(root).await?;
    let head = capture_git(root, &["rev-parse", "--verify", "HEAD"], 256)
        .await
        .map(|c| String::from_utf8_lossy(&c.stdout).trim().to_string())
        .unwrap_or_default();
    let branch = repos
        .current_branch(root)
        .await
        .unwrap_or_else(|_| "HEAD".into());

    let names = capture_git(
        root,
        &[
            "diff",
            "--name-status",
            "-z",
            "--find-renames",
            turn_tree,
            &current,
            "--",
        ],
        2 * 1024 * 1024,
    )
    .await?;
    let nums = capture_git(
        root,
        &[
            "diff",
            "--numstat",
            "-z",
            "--find-renames",
            turn_tree,
            &current,
            "--",
        ],
        2 * 1024 * 1024,
    )
    .await?;
    let tracked = capture_git(
        root,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--find-renames",
            "--unified=3",
            turn_tree,
            &current,
            "--",
        ],
        MAX_PATCH_BYTES,
    )
    .await?;

    let mut files = parse_name_status(&names.stdout);
    apply_numstat(&mut files, &nums.stdout);
    let mut patch = String::from_utf8_lossy(&tracked.stdout).to_string();
    let truncated = tracked.truncated || names.truncated || nums.truncated;
    if tracked.truncated {
        let boundary = patch.rfind('\n').unwrap_or(0);
        patch.truncate(boundary);
        patch.push_str("\n# Zeron diff truncated\n");
    }

    let additions: u32 = files.iter().map(|f| f.additions).sum();
    let deletions: u32 = files.iter().map(|f| f.deletions).sum();
    let files_json = serde_json::to_string(&files)
        .map_err(|e| EngineError::Other(format!("diff files serialize: {e}")))?;
    let mut hasher = Sha256::new();
    hasher.update(branch.as_bytes());
    hasher.update([0u8]);
    hasher.update(head.as_bytes());
    hasher.update([0u8]);
    hasher.update(patch.as_bytes());
    hasher.update([0u8]);
    hasher.update(files_json.as_bytes());
    hasher.update(if truncated { b"1" } else { b"0" });
    let checksum = crate::repos::hex(&hasher.finalize());

    Ok(DiffSnapshot {
        branch,
        head_sha: (!head.is_empty()).then_some(head),
        patch,
        files,
        additions,
        deletions,
        truncated,
        checksum,
    })
}

#[cfg(test)]
mod watch_budget_tests {
    use super::{CheckoutIdentity, MAX_WATCH_DIRS, exceeds_watch_budget, watch_targets};

    #[test]
    fn small_tree_is_watchable() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("src/a/b")).unwrap();
        std::fs::create_dir_all(root.join("src/c")).unwrap();
        std::fs::write(root.join("src/a/f.txt"), "x").unwrap();
        assert!(!exceeds_watch_budget(root));
    }

    #[test]
    fn budget_is_exceeded_and_probe_stays_bounded() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        for i in 0..(MAX_WATCH_DIRS + 50) {
            std::fs::create_dir(root.join(format!("d{i}"))).unwrap();
        }
        assert!(exceeds_watch_budget(root));
    }

    fn identity(root: &std::path::Path, git_dir: &std::path::Path) -> CheckoutIdentity {
        CheckoutIdentity {
            id: "test".into(),
            root: root.to_path_buf(),
            git_dir: git_dir.to_path_buf(),
        }
    }

    #[test]
    fn small_checkout_watches_root_only() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".git/refs")).unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        assert_eq!(
            watch_targets(&identity(root, &root.join(".git"))),
            vec![root.to_path_buf()]
        );
    }

    #[test]
    fn linked_worktree_watches_root_and_git_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wt");
        let git_dir = tmp.path().join("main/.git/worktrees/wt");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(&git_dir).unwrap();
        assert_eq!(
            watch_targets(&identity(&root, &git_dir)),
            vec![root.clone(), git_dir]
        );
    }

    #[test]
    fn over_budget_root_falls_back_to_the_git_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".git/refs")).unwrap();
        for i in 0..(MAX_WATCH_DIRS + 50) {
            std::fs::create_dir(root.join(format!("d{i}"))).unwrap();
        }
        assert_eq!(
            watch_targets(&identity(root, &root.join(".git"))),
            vec![root.join(".git")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_dir_is_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("real/inner")).unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join("real/inner/loop")).unwrap();
        assert!(!exceeds_watch_budget(root));
    }
}
