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

GitHub's merge queue is only available for organization-owned repos or GitHub Enterprise,
so we need an alternative for this personal repository.

## Decision

### 1. Merge Automation: GitHub Native Auto-Merge + Branch Update Action

We use two GitHub-native mechanisms together:

1. **GitHub auto-merge** — built-in feature that merges a PR automatically once all
   required checks pass and required reviews are provided
2. **[adRise/update-pr-branch](https://github.com/adRise/update-pr-branch)** — GitHub
   Action that automatically updates outdated PR branches when new commits land on `17.0`

Together these form a lightweight merge queue without any external services.

**Rationale:**
- **No external apps** — everything runs as GitHub Actions, portable to any GitHub instance
  (personal, org, enterprise, on-prem)
- **Native integration** — auto-merge works with branch protection rules out of the box
- **Automatic branch updates** — the update action keeps PR branches current, so
  "Require branches to be up to date" works without manual "Update branch" clicks
- **Zero operational overhead** — no containers to deploy, no apps to install

**How it works:**

```
PR gets approved + auto-merge enabled
        │
        v
Another PR merges into 17.0
        │
        v
update-pr-branch action triggers (on push to 17.0)
        │
        v
Finds approved PRs with auto-merge → updates their branch
        │
        v
CI re-runs on updated branch
        │
        v
All checks pass → GitHub auto-merge completes the merge
```

**Workflow (`.github/workflows/update-pr-branch.yml`):**

```yaml
name: Update PR branches

on:
  push:
    branches: [17.0]
  schedule:
    # Run every night at 3:00 AM UTC to catch any stuck PRs
    - cron: '0 3 * * *'

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: adRise/update-pr-branch@v0.10
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          base: '17.0'
          required_approval_count: 1
          require_passed_checks: true
```

**Triggers:**
- **On push to `17.0`** — immediately updates the next approved PR after each merge
- **Nightly at 3:00 AM UTC** — catches any PRs stuck due to race conditions or transient failures

### 2. Versioning Scheme

Format: `17.X.N` where:
- `17` = target JDK version (major)
- `X` = feature track (minor) — even numbers (0, 2, 4...) are **unstable** (testing new features),
  odd numbers (1, 3, 5...) are **stable** (production-ready)
- `N` = auto-incrementing release counter (patch)

Current branch `17.0` is the unstable development track. When features are validated,
a stable `17.1` branch will be created. Next unstable cycle will be `17.2`, next stable `17.3`, etc.

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
GitHub Actions: enable auto-merge (gh pr merge --auto --merge)
        │
        v
CI runs: build + unit tests (+ integration tests after #189)
        │
        v
If 17.0 is ahead → update-pr-branch action updates the PR branch
        │
        v
CI re-runs → all checks pass → GitHub auto-merge completes
        │
        v
Post-merge workflow: compute next tag, create release
```

## Consequences

- Dependabot PRs merge fully automatically for patch/minor updates
- Major dependency updates still require manual review
- Every merged Dependabot PR produces a release tag and GitHub Release
- No external services or apps needed — everything is GitHub Actions
- Works on any GitHub instance (personal, org, enterprise, on-prem)
- Bot developer PRs still require @abashev approval before merge
- "Require branches to be up to date" stays enabled — the update action handles it

## Alternatives Considered

1. **Palantir Bulldozer** — Self-hosted GitHub App with merge queue. More powerful but
   requires deploying and maintaining a container. Overkill for current PR volume.

2. **Mergify** — Commercial SaaS with free tier for open source only. Not suitable for
   internal company use without a paid plan.

3. **GitHub Merge Queue** — Not available for personal repositories. Only for org-owned
   repos or GitHub Enterprise.

4. **Manual releases** — Rejected: too easy to forget, creates inconsistent release cadence.
