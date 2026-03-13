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

### 1. Merge Automation: Palantir Bulldozer

We use **[Bulldozer](https://github.com/palantir/bulldozer)** — a GitHub App by Palantir
that automatically merges PRs when all required checks pass and required reviews are provided.

**Rationale:**
- **Self-hosted** — runs as a container, can be deployed inside a company network
- **Event-driven** — merges within seconds of all preconditions being met
- **Branch updates** — automatically updates PR branches before merging (eliminates
  "out-of-date" problem without requiring "Require branches to be up to date" in ruleset)
- **Configurable per-repo** — via `.bulldozer.yml` at repo root or shared across an org
- **Actively maintained** — latest release June 2025

**Configuration (`.bulldozer.yml`):**

```yaml
version: 1
merge:
  trigger:
    labels: ["merge when ready"]
  method: merge
  delete_after_merge: true
update:
  trigger:
    labels: ["merge when ready"]
```

**Setup:**
- Deploy Bulldozer as a GitHub App (container image from `palantir/bulldozer`)
- Install the app on the repository
- Add `.bulldozer.yml` to repo root on `17.0` branch
- In branch protection ruleset: remove "Require branches to be up to date before merging"
  (Bulldozer's `update` config handles this)
- Label PRs with `merge when ready` to opt in to auto-merge

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
GitHub Actions: add "merge when ready" label
        │
        v
CI runs: build + unit tests (+ integration tests after #189)
        │
        v
Bulldozer: all checks pass + approved → update branch → merge
        │
        v
Post-merge workflow: compute next tag, create release
```

## Consequences

- Dependabot PRs merge fully automatically for patch/minor updates
- Major dependency updates still require manual review
- Every merged Dependabot PR produces a release tag and GitHub Release
- Bulldozer can be deployed in any environment (cloud, on-prem, company network)
- Bot developer PRs still require @abashev approval before merge
- "Require branches to be up to date" can be removed from ruleset — Bulldozer handles it

## Alternatives Considered

1. **GitHub Native Auto-Merge** — Works but limited: no merge queue, no automatic branch
   updates. GitHub's merge queue is only available for organization-owned repos or Enterprise.
   For personal repos, "Require branches to be up to date" creates friction with multiple
   concurrent PRs.

2. **Mergify** — Commercial SaaS with free tier for open source only. Not suitable for
   internal company use without a paid plan.

3. **GitHub Merge Queue** — Not available for personal repositories. Only for org-owned
   repos or GitHub Enterprise.

4. **Manual releases** — Rejected: too easy to forget, creates inconsistent release cadence.
