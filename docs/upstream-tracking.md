# Upstream tracking

The fork selectively follows `zeronsh/comet`. Two markers matter and they are
not the same thing:

- **Applied checkpoint — `086db2e` (2026-08-21).** The last upstream commit
  whose selected changes were actually cherry-picked into this fork.
- **Surveyed head — `2ebe6ed` (2026-08-25, upstream `v0.2.29`).** The last
  upstream commit read through and triaged. Nothing between the applied
  checkpoint and here has been taken yet.

Neither is an app release version.

Since `d6e89c8` ("Make comet a backend-only library workspace") this repo is a
library workspace: `crates/{proto,doc,sync,harness,engine,rpc}`. Upstream work
in `crates/ui`, `crates/theme`, `apps/*`, `edge/*`, `dist/*`, and packaging is
**permanently out of scope**, not deferred. Triage only needs to look at the
six backend crates.

`crates/proto` diverged further on 2026-08-25: `proto/src/motion.rs` and
`proto/src/view.rs` were removed. Loader motion was already duplicated in
`onyx-ui` (`crates/onyx-ui/src/motion_math.rs`), and the viewport derivations
moved to `zeron-ui` (`crates/zeron-ui/src/view.rs`). Only the session-staleness
rule stayed, folded into `proto/src/entities.rs` as `SESSION_STALE_MS`,
`Indicator`, `effective_indicator`, and `display_status` — the engine and the
harnesses enforce that window themselves. **Upstream commits touching
`proto/src/view.rs` or `proto/src/motion.rs` are now out of scope unless they
change the staleness rule.**

To review only changes added after the applied checkpoint:

```bash
git fetch upstream
git log --oneline 086db2e..upstream/main
git diff --stat 086db2e..upstream/main -- crates/proto crates/doc crates/sync \
  crates/harness crates/engine crates/rpc
```

After the next upstream review, move both markers and record the date and PR
that consumed the selected changes. Intentional exclusions remain outside the
fork.

## 2026-08-25 survey (triage only — nothing applied)

Upstream head `2ebe6ed` (`v0.2.29`). 138 commits since `086db2e`; 122 files
changed. Of those, 14 non-merge commits touch the six backend crates and are
the entire candidate set:

**opencode: ACP path replaced by a native driver** — the largest item, and
interdependent:

- `aa9f8bf` reasoning becomes a first-class transcript part (`doc/parts.rs`,
  `doc/schema.rs`, `doc/transcript_delta.rs`); also deletes
  `harness/src/acp/subagent_opencode.rs`, which this fork still carries
- `bf63444` native HTTP/SSE driver replaces the ACP path — new
  `harness/src/opencode/` (~2.4k lines), new `harness/tests/opencode.rs`,
  drops `harness/tests/acp.rs` and `acp_stall.rs`, touches
  `engine/src/registry.rs` and `proto/src/agent.rs`
- `0b421cc` bound every HTTP call (a boot-window request parks forever)
- `a401432` gate the first prompt on a live event subscription
- `705470c` rustfmt the new driver module
- `4fd3557` picker offers only connected providers' models

Note this supersedes the fork's own opencode ACP integration taken on
2026-08-19 (`0213cac`, `296813f`, `7c6c123`, `54b2d45`). Taking it is a
replacement, not a merge.

**doc / registry:**

- `5306be2` chat rows survive configs from newer peers (`doc/workspace.rs`)
- `1fc6843` registry cursor can no longer jump over unapplied rows —
  `doc/src/registry.rs` applies; the `crates/sync/src/registry.rs` hunk does
  not, that file is not in this fork (`crates/sync` is `lib.rs` + `store.rs`)

**engine:**

- `054c17e` conversation-aware sidebar views — despite the UI-sounding title,
  the backend half is real: `doc/registry.rs`, `doc/workspace.rs`,
  `engine/{change_requests,diff_sync,doc_host,repos,rpc,workspace_host}.rs`,
  `proto/src/entities.rs`
- `6da4e9f` follow-up review fixes to `engine/{doc_host,sessions}.rs`
- `da659ff` formatting for the same series
- `2119bf0` test: diff-sync no longer rewrites `chat.branch`

**harness (small):**

- `da56a6f` mock harness emits thinking parts (`harness/src/mock.rs`)

**Does not apply — cloud/sync surface removed here:**

- `28eb39b` orphan sweep waits for server truth; HTTP ack retries. Lands
  mostly in `crates/sync/src/registry.rs` and `crates/sync/tests/`, which this
  fork does not have. The `engine/{spaces,workspace_host}.rs` hunks would need
  to be lifted out by hand if the orphan-grace behaviour is wanted.

**Now permanently out of scope** (previously listed as deferred): the
right-pane resize series, the transcript spawn-chip series, and every other
`crates/ui` item in the 2026-08-21 notes below. They live in the `zeron` repo
now. The engine/harness subagent-binding fixes flagged inside the spawn-chip
chain (`18987da`, `569793e`) are the only part still worth extracting.

## 2026-08-21 intake notes

> Historical. The `crates/ui` deferrals below are now out of scope for
> this repo; see the 2026-08-25 survey above.

Selected upstream commits applied (cherry-picks, adapted where the fork
diverges):

- `76b49f0` registry: gate default agent enablement on the installed probe
- `aacb621` ui: handle empty agent catalogs safely
- `8528e3b` installed-only harness toggles — resolved directly to the FINAL
  semantics of `a8ef0aa` (offered = enabled AND installed, empty stays empty),
  which the fork's picker already half-carried
- `dc6b8e5` accounts: Codex free-tier quota window is a month
- `bca16a3` accounts: no double Codex auth tab (BROWSER no-op shim; adapted to
  the fork's `wire_login_child` helper)
- `74f4abe` diff-sync: reconcile gate + orphan grace (edge relay param dropped;
  churn-test fixtures adapted to local-only signatures)
- `447b689` composer: full-width @-mention panel, indexed file search
- `89aa28d` + `46de808` changes pane: side-by-side diff + no-newline pairing
- `889b78e` terminal rendering after sidebar reopen
- `f6911c3` decorated ranges at soft wraps
- `3536a37` selection edge scrolling + terminal scrollbar
- `e8f9e03` attachment thumbnails (corners clip, sending indicator) — the
  relay transfer-percent ring and queued-flow alias seeding do not exist
  locally; the indicator is the indeterminate spinner, and upstream's gpui
  pin bump was NOT taken (the fork manages its own pin)
- `eda27e8` ACP Grok model switching (RunRequest has no `worktree` on this
  fork; the routing config drops that field)
- `fde9b4b` harden OpenCode model discovery
- `f5fb9b9` preserve live MCP OAuth when switching Claude accounts
- `181d667` the interruption marker is not a steer (adapted: the fork's
  tagged-user branch now also forwards genuine text steers, filtered through
  the pre-existing `is_synthetic_user_text`)

Intentionally excluded (this intake):

- All `apps/ios/*` and `edge/*` commits, `0bd6a6b` (cloud pull/push
  hardening), `7754391`/`08e53cd` (chat2/sync client — `crates/sync/chat_client`
  is removed here), `446ffbf`/`abacb45` (cloud delivery retry chain),
  `c3d2981` (relay transfer progress), and version bumps.
- The right-pane resize series (`b1214b6`, `c4e8cd4`, `5d20db8`, `79d05d5`,
  `2bd9eb3`, `0c84d0d`, `89cfeef`, `0079972`, `2761213`, `be01c65`, `a2db751`)
  and `a00aa61` (composer idle redraw, depends on that plumbing): upstream
  redesigned the same panel-resize area the fork's own takeover work already
  covers — reconciling the two implementations is a product decision deferred
  to its own pass.
- The transcript spawn-chip series (`7a05159`, `6530b88`, `5019dc1`,
  `18987da`, `569793e`, `cb2f30d`): interdependent chain over ~850 lines of
  transcript.rs drift (relay-percent context the fork excludes); deferred to
  a dedicated intake. Note `18987da`/`569793e` carry engine/harness subagent
  binding fixes worth taking with it.

## 2026-08-19 intake notes

Selected upstream commits applied:

- `0213cac` opencode ACP harness integration
- `296813f` opencode effort picker model-variant support
- `7c6c123` opencode provider-failure surfacing
- `54b2d45` subagent transcript prompt seeding + opencode/grok user forwarding
- `1f94405` codex child user-message steer handling
- `c53ecd1` New-worktree fallback safety + queued-send UX

Intentionally excluded (this intake):

- `f4383e3` (default branch via `gh`) because `crates/engine/src/source_control.rs`
  is removed on this fork and the commit does not apply cleanly.
- `061e6ec` and `48ff777` were attempted, but reverted because they require a
  larger `doc_host`/sync/proto dependency chain not yet present in this fork.
