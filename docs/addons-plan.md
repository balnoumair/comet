# Add-on architecture plan

Status: design only — nothing implemented yet. Decided in conversation 2026-08-17.

Goal: keep a super-simple base app (what comet is today) and build every extra
feature as a clearly separated add-on, removable without trace, while staying
able to selectively cherry-pick upstream changes into the base.

## Upstream tracking

The fork follows `zeronsh/comet` selectively. The last upstream commit reviewed
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

## Topology

```
comet (this repo — one cargo workspace)
│
├─ upstream strata — keep pristine, cherry-pick upstream freely
│   ├─ crates/proto, doc, sync      never touch
│   ├─ crates/harness               never touch (ACP agent adapters — upstream
│   │                                churn here benefits the fork the most)
│   └─ crates/engine, rpc           rarely touch; only if an add-on truly
│                                    cannot shell out to git/gh
│
├─ ours
│   ├─ crates/ui                    base UI + ONE seam: surface registry,
│   │                                Route::Addon(id), nav entries
│   ├─ crates/addons/todo           thin adapter (~50 lines) over todo repo
│   ├─ crates/addons/gh             thin adapter (~100–300 lines): mounts
│   │                                gh-ui views, maps theme, injects paths —
│   │                                covers BOTH the PR-tab and
│   │                                worktree-extended ideas
│   └─ apps/zeron                   composition root: builds the registry,
│                                    one registration line per add-on
│
shared infrastructure repos
│
├─ balnoumair/zed (to fork)         gpui fork: our patches ONLY (EdgeFade,
│                                    blur, memory/alpha fixes). Rebases onto
│                                    zed upstream; never holds app code
│
└─ comet-kit (new repo)             design system: theme types, primitives
                                     (buttons, list rows, frost wrappers,
                                     motion), structure-heavy widgets copied
                                     piecemeal from gpui-component
                                     (Apache-2.0, attributed). Depends only
                                     on the gpui fork
│
external add-on repos (one per add-on — substance lives here, not in comet)
│
├─ gh-tools (existing repo — Electron → Rust rewrite)
│   ├─ gh-core                      logic: gh/git subprocess, poller, recents,
│   │                                settings. NO UI deps, NO zeron deps
│   ├─ gh-ui                        gpui views (PR list/detail, diffs,
│   │                                worktree detail) built on comet-kit —
│   │                                host-agnostic: no window ownership,
│   │                                theme via kit types, paths injected
│   └─ apps/standalone              menubar/tray personality + gh-ui
│
└─ todo (new repo)                  todo-core + todo-ui, same discipline
```

## Repo inventory

| repo | contains | depends on |
|---|---|---|
| `balnoumair/zed` | gpui fork: our patches only | rebases onto zed upstream |
| `comet-kit` | design system: theme, primitives, copied-in widgets | gpui fork (pinned rev) |
| `comet` | base app + seam + thin adapters | kit + gpui fork |
| `gh-tools` | gh-core + gh-ui + standalone | kit + gpui fork |
| `todo` (etc.) | per-add-on core + ui | kit + gpui fork |

Dependency arrows point one way: comet → external core/ui crates by git rev.
External repos never know comet exists and NEVER import comet crates
(`zeron-ui` etc.) — they depend only on the gpui fork + comet-kit + their own
code. One rev chain: all repos pin the same gpui fork rev and the same kit
rev; bumps flow top-down (fork → kit → everyone). That shared pin is what
lets external gpui views mount inside comet without a rewrite.

## Division of labor

- **Comet decides** where an add-on appears (main tab, right-pane surface,
  titlebar button, shortcut, contextual), when it is visible, and how big its
  rectangle is. Placement is chosen per-view in the adapter, so moving an
  add-on later is an adapter tweak, not a gh-tools change.
- **The add-on decides** everything inside its rectangle.

## Decision log

1. **Every add-on's substance lives in its own external repo** (the gh-tools
   pattern, generalized — decided 2026-08-17). Comet keeps only thin adapter
   crates in `crates/addons/` plus registration lines in `apps/zeron`.
   Removal = delete the adapter + one workspace line + one registration line;
   the external repo lives on regardless. This keeps comet's diff vs upstream
   tiny and lets any add-on grow a standalone binary later.
2. **Add-ons are full vertical slices** (UI + their own backend), but the
   backend lives in the add-on's core crate: shell out to `gh`/`git`, own
   storage files under the data dir (never the engine's `docs.sqlite3`).
   Extending the engine requires proving it cannot be done add-on-side.
3. **Comet positions add-ons; add-ons own their rectangle** (see above).
4. **Hard boundary rule:** external add-on crates depend only on the gpui
   fork (same pinned rev) + comet-kit + their own code — never on `zeron-ui`
   or any comet crate (cargo would see two copies of `zeron-ui`, git vs path,
   with incompatible types; comet-kit avoids this precisely because it lives
   in its own repo and everyone, comet included, consumes it by git rev).
   Anything that must import comet types is adapter code and lives in
   `crates/addons/`. External UI crates stay host-agnostic: no window
   ownership, theme via kit types configured by the host, paths injected.
   Accepted costs: (a) the edit→bump-rev loop during add-on development —
   mitigate with a temporary local `[patch]` while iterating; (b) gpui
   lockstep multiplies across N repos on every gpui bump.
5. **Keep the gpui fork for now** — dropping it would not remove rev-pinning
   (gpui has no stable releases; the comet↔gh-ui lockstep exists with
   upstream gpui too). The fork carries real fixes (atlas leak/evict, GPU
   memory bounds, Porter-Duff alpha, wrap rules, macOS 26 blur) and the glass
   design language (BackdropBlur/frost in ~10 UI files, EdgeFade). Revisit as
   its own project; first step then: audit which patches upstream absorbed.
6. **Upstream intake = selective cherry-picks** after analysis, never blanket
   merges. The pristine strata are what keep those picks clean. (Upstream
   remote is not configured yet — `origin` only.)
7. **Own the gpui fork** (decided 2026-08-17): fork `wingleeio/zed` →
   `balnoumair/zed`, branch `comet-pin` at the pinned SHA so it stays
   reachable, repoint the three git URLs in Cargo.toml (same rev, zero code
   change). Removes the third-party availability risk and is the
   prerequisite for any future gpui work; gh-tools and all add-on repos pin
   our URL from day one. Sync workflow is reactive: only when a comet
   cherry-pick bumps the gpui rev — and if the feature is visual-only and
   unwanted, skip the pick and the bump together. Not executed yet.
8. **comet-kit: a shared design-system repo** (decided 2026-08-17). Solves
   the primitive-duplication problem the boundary rule created: gh-ui and
   todo-ui would otherwise each rebuild comet-style primitives. The kit owns
   the theme types and shared components; every host (comet, standalone,
   add-ons) gets one consistent design language. Relationship to
   longbridge/gpui-component (Apache-2.0): **quarry, not base** — never a
   dependency (it floats on zed upstream main, carries its own theme global,
   and would mean maintaining a third fork); instead copy individual
   structure-heavy widgets (table, virtualized list, inputs) into the kit,
   restyled to our theme, with attribution. Growth model: seeded with what
   the first consumer needs, grown on demand; comet's `crates/ui` migrates
   to it lazily — internal duplication in comet is temporary and harmless,
   no deadline. **Import discipline: consumers import only comet-kit** (the
   kit re-exports whatever it uses underneath) — this single rule is what
   keeps the Path B migration (below) cheap if ever wanted.
9. **Path B evaluated and parked** (2026-08-17): the alternative stack —
   upstream `zed-industries/zed` gpui (no patch fork) + gpui-component as a
   real dependency under comet-kit as a thin brand layer. Confirmed viable:
   gpui-component's theming is deep (JSON schema, ~200 tokens, per-state
   colors, fonts/radii/shadows) + per-instance `Styled` overrides + the
   per-component copy-and-own escape hatch. Cost inventory if taken:
   window glass gone (accepted), frosted menus → opaque cards (frost has a
   built-in pass-through fallback), edge fades fakeable with gradients once
   opaque, image cover-crop corners + wrap-punctuation + `evict`/GPU-memory
   fixes need an upstream-absorption audit; harness adapter maintenance
   remains ours either way once upstream-comet tracking stops. Decision:
   **stay on Plan A** (own patched fork, decision #7) — both paths converge
   on comet-kit, so migrating to Path B later is a contained project, not a
   restructuring.

## Build order

- **Stage 0 — the seam.** `AddonRegistry` built in `apps/zeron`, passed to the
  shell. `Route::Addon(id)` + registry-driven nav/surface rendering in
  `crates/ui`. Nav persistence is already string-based (`"settings/agents"`),
  so `"addon/<id>"` serializes with no schema change. Few hundred lines, paid
  once, shared by every future add-on.
- **Pre-stage — own the gpui fork** (decision #7): fork, `comet-pin` branch,
  repoint Cargo.toml URLs. Independent of everything else; cheapest first
  move.
- **Stage 1 — todo add-on + seed comet-kit.** New `todo` repo (todo-core +
  todo-ui) + adapter in comet; comet-kit created with just the theme types
  and the primitives todo-ui needs. Smallest possible slice, zero engine
  dependency; proves the seam, the external-repo pattern (git dep, `[patch]`
  workflow), and the kit end to end before gh-tools rides on any of it.
- **Stage 2 — gh-core** in the gh-tools repo (~1.3k lines of Rust ported from
  the Electron main-process services) — needed for the standalone regardless
  of comet.
- **Stage 3 — gh-ui + standalone binary** (the React → gpui rewrite; ~3.7k
  lines of TSX, shrinking substantially since tray/IPC/window plumbing
  disappears). Built on comet-kit; grow the kit here — copy table/list
  widgets from gpui-component as needed rather than building from scratch.
- **Stage 4 — comet adapter**: mount gh-ui views, choose placements.
- **Further seams** (right-pane surfaces, titlebar buttons, settings pages)
  only when a concrete add-on wants them — rule of two: extract a seam when
  the second consumer appears, not the first.

## Open questions

- **Placement of each gh view** (main tab vs right pane vs button) — decide at
  Stage 4.
- ~~A separate repo for the add-ons?~~ **Resolved** — decision log #1/#4:
  substance in external repos, adapters in comet.
- **Extract comet's git UI (changes pane etc.) toward an add-on / gh-ui?**
  Scope settled 2026-08-17: **UI only — engine-level git stays put.**
  Rationale: git is woven into the session lifecycle in upstream strata
  (`CheckoutIdentity` binds every chat to a worktree; composer ref picker;
  harness runs execute in checkouts), and deletions in upstream files are the
  most cherry-pick-hostile change. The UI side (`crates/ui/src/changes.rs`
  and friends) is ours already, so reshaping or replacing it is a cheap
  ui-crate refactor — decide once gh-ui exists and can be compared.
- **Fork vs mainline gpui** — parked (decision log #5).
- **Whether any worktree feature eventually justifies an engine seam** —
  decide when it hurts.
