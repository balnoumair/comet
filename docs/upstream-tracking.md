# Upstream tracking

The fork selectively follows `zeronsh/comet`. The last upstream commit reviewed
for this intake was `086db2e` on 2026-08-21. This is a review checkpoint, not
an app release version.

To review only changes added after this checkpoint:

```bash
git fetch upstream
git log --oneline 086db2e..upstream/main
git diff --stat 086db2e..upstream/main
```

After the next upstream review, replace `086db2e` above with the new reviewed
`upstream/main` commit and record the date and PR that consumed the selected
changes. Intentional exclusions remain outside the fork.

## 2026-08-21 intake notes

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
