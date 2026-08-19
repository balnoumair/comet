# Upstream tracking

The fork selectively follows `zeronsh/comet`. The last upstream commit reviewed
for this intake was `35f2b7a` on 2026-08-19. This is a review checkpoint, not
an app release version.

To review only changes added after this checkpoint:

```bash
git fetch upstream
git log --oneline 35f2b7a..upstream/main
git diff --stat 35f2b7a..upstream/main
```

After the next upstream review, replace `35f2b7a` above with the new reviewed
`upstream/main` commit and record the date and PR that consumed the selected
changes. Intentional exclusions remain outside the fork.

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
