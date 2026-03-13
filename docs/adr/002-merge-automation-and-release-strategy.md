# ADR-002: Merge Automation and Release Strategy

**Status:** Proposed
**Date:** 2026-03-13
**Author:** @architect (vfs-s3-bot)

## Context

The vfs-s3 project receives regular dependency updates from Dependabot and will receive PRs
from the bot developer agent. Currently all PRs require manual merge. We need automation for:

1. Auto-merging approved PRs (especially Dependabot) after CI passes
2. Consistent release versioning tied to the new `17.0` branch
3. Automated release creation after merges

The main branch has been renamed from `branch-17.x.x` to `17.0`.

## Decision

### 1. Merge Automation: GitHub Native Auto-Merge

We use **GitHub's built-in auto-merge** feature instead of external bots (Bulldozer, Mergify).

**Rationale:**
- No external GitHub App to install, host, or maintain
- Works natively with branch protection rules
- Supported by `gh pr merge --auto` from CLI and GitHub Actions
- Free for all repository types

**Setup:**
- Enable "Allow auto-merge" in repository settings
- Configure branch protection on `17.0`:
  - Require status checks to pass (CI build)
  - Require at least 1 approving review (from @abashev)
  - Restrict who can push: @abashev and @vfs-s3-bot

**For Dependabot PRs specifically:**
- A GitHub Actions workflow auto-approves Dependabot PRs for patch/minor updates
- The workflow enables auto-merge via `gh pr merge --auto --squash`
- Branch protection ensures CI must pass before the merge happens

### 2. Versioning Scheme

Format: `17.0.N` where:
- `17` = target JDK version (major)
- `0` = feature track (minor) — increments to `17.1` for significant changes
- `N` = auto-incrementing release counter (patch)

The version is derived from the latest git tag on the branch. If the last tag is `17.0.3`,
the next release is `17.0.4`.

### 3. Release Automation

After a PR is merged to `17.0`:
- GitHub Actions workflow computes the next version from the latest tag
- Creates a git tag `17.0.N`
- Creates a GitHub Release with auto-generated release notes
- Triggers only for Dependabot merges initially; bot PRs and manual PRs create releases
  only when @abashev adds a `release` label

### 4. Dependabot Pipeline (Full Cycle)

```
Dependabot opens PR
        │
        v
GitHub Actions: auto-approve (patch/minor only)
        │
        v
GitHub Actions: enable auto-merge (--squash)
        │
        v
CI runs: build + unit tests (+ integration tests after #189)
        │
        v
Branch protection: all checks pass → auto-merge
        │
        v
Post-merge workflow: compute next tag, create release
```

## Consequences

- Dependabot PRs merge fully automatically for patch/minor updates
- Major dependency updates still require manual review
- Every merged Dependabot PR produces a release tag and GitHub Release
- No external services or apps needed — everything is GitHub-native
- Bot developer PRs still require @abashev approval before merge

## Alternatives Considered

1. **Palantir Bulldozer** — Rejected: requires installing a separate GitHub App,
   either self-hosted or via Palantir's hosted service. Adds operational complexity
   for a feature GitHub now provides natively.

2. **Mergify** — Rejected: commercial SaaS with free tier limitations. More powerful
   than needed for this project. Native auto-merge covers our use cases.

3. **GitHub Merge Queue** — Considered but not needed yet. Merge queues help when
   many PRs merge concurrently and need serialization. Our volume doesn't warrant it.
   Can be added later if needed.

4. **Manual releases** — Rejected: too easy to forget, creates inconsistent release cadence.
