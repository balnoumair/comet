# Upstream tracking

The fork selectively follows `zeronsh/comet`. The last upstream commit reviewed
for PR #4 was `a6db86d` on 2026-08-18. This is a review checkpoint, not an app
release version.

To review only changes added after this checkpoint:

```bash
git fetch upstream
git log --oneline a6db86d..upstream/main
git diff --stat a6db86d..upstream/main
```

After the next upstream review, replace `a6db86d` above with the new reviewed
`upstream/main` commit and record the date and PR that consumed the selected
changes. Intentional exclusions remain outside the fork.
