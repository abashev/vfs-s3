# Multi-Agent Development System for vfs-s3

## Overview

This document describes the multi-agent AI-native development workflow for the `vfs-s3` project.
The system uses Claude Code GitHub Actions to create a team of specialized AI agents that
communicate through GitHub issues and pull requests, with the project owner as the final approver.

## Agent Roles

### 1. Architect (`@architect`)

**Trigger phrase:** `@architect`

**Responsibilities:**
- Review architectural decisions and propose alternatives
- Design APIs and module boundaries (especially for the multi-module split)
- Create Architecture Decision Records (ADRs) in `docs/adr/`
- Comment on issues with design guidance before implementation starts
- Validate that PRs follow the agreed architecture

**When to invoke:**
- New feature issues that need design discussion
- Questions about module boundaries or API design
- Reviewing PRs that change public APIs or module structure

**Model:** `claude-opus-4-6` (complex reasoning needed)

### 2. Developer (`@developer`)

**Trigger phrase:** `@developer`

**Responsibilities:**
- Implement features and fixes based on issue descriptions
- Create pull requests with working code
- Follow coding standards from `CLAUDE.md` (Java 17, Palantir format)
- Respond to review comments with code changes
- Write unit tests alongside implementation

**When to invoke:**
- Issues labeled `ready-for-dev` (architect has approved the design)
- PR review comments requesting code changes
- Bug reports with clear reproduction steps

**Model:** `claude-sonnet-4-6` (good balance of speed and quality)

### 3. Reviewer (`@reviewer`)

**Trigger phrase:** `@reviewer`

**Responsibilities:**
- Review pull requests for code quality, correctness, and security
- Check adherence to Java 17 idioms and Palantir code style
- Verify test coverage meets the 80% threshold
- Suggest improvements as PR review comments
- Approve PRs or request changes

**When to invoke:**
- Automatically on every new PR and push to PR
- Manually via `@reviewer` in PR comments

**Model:** `claude-sonnet-4-6`

## Communication Protocol

### Issue Lifecycle

```
                    +-----------+
                    |  Created  |  (human or bot opens issue)
                    +-----+-----+
                          |
                          v
                 +--------+--------+
                 | @architect      |  Design discussion
                 | reviews design  |  (comments on issue)
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Label:          |  Human approves design
                 | ready-for-dev   |
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | @developer      |  Creates PR with
                 | implements      |  implementation
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | @reviewer       |  Automatic code review
                 | reviews PR      |  on PR open
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Human owner     |  Final approval
                 | approves & merge|  and merge
                 +-----------------+
```

### Label System

| Label | Meaning | Who Sets It |
|-------|---------|-------------|
| `needs-design` | Issue needs architectural review | Human / Triage |
| `ready-for-dev` | Design approved, ready for implementation | Human (after architect review) |
| `in-progress` | Developer is working on it | Developer agent |
| `needs-review` | PR ready for code review | Developer agent |
| `changes-requested` | Reviewer found issues | Reviewer agent |
| `approved` | Review passed | Reviewer agent |
| `owner-approved` | Owner gave final approval | Human |

### Comment Conventions

Agents use structured comments for clarity:

```markdown
## Architect Review

**Decision:** Proceed with Option B
**Rationale:** Better separation of concerns for the multi-module split
**Impact:** Changes to `S3FileProvider` public API
**Action items:**
- [ ] Define new interface in `core` module
- [ ] Move S3-specific logic to `s3-provider` module

---
*This is an automated review by @architect. Tag @abashev for questions.*
```

### PR Description Template (Developer)

```markdown
## Summary
Brief description of what this PR does.

## Related Issue
Closes #123

## Design Reference
Based on @architect's review in #123 (comment link)

## Changes
- What was added/changed/removed

## Testing
- [ ] Unit tests added
- [ ] Integration tests updated
- [ ] Coverage >= 80%

---
*This PR was created by @developer. Tag @reviewer for code review.*
```

## GitHub Actions Setup

### Prerequisites

1. Install the Claude GitHub App: https://github.com/apps/claude
2. Add `ANTHROPIC_API_KEY` to repository secrets
3. Copy workflow files from `.github/workflows/`

### Workflow Files

The system uses **four separate workflow files** so each agent has independent
configuration and trigger conditions:

| File | Agent | Triggers On |
|------|-------|------------|
| `claude-architect.yml` | Architect | `@architect` in issues/PRs |
| `claude-developer.yml` | Developer | `@developer` in issues/PRs, `ready-for-dev` label |
| `claude-reviewer.yml` | Reviewer | New PRs, `@reviewer` in PR comments |
| `claude-triage.yml` | Triage | New issues (auto-label) |

### Security

- Fork PRs do NOT have access to secrets (prevents API credit abuse)
- Each agent has minimal required permissions
- Human approval is required before merging any PR
- The `ANTHROPIC_API_KEY` should have spending limits configured

## CLAUDE.md Role Instructions

Each agent reads the shared `CLAUDE.md` at the repo root, which contains build instructions
and code style rules. Role-specific behavior is injected via the `--append-system-prompt`
parameter in each workflow file.

### Shared Context (CLAUDE.md)

All agents share:
- Build commands (`mise exec -- mvn ...`)
- Java 17 style rules
- Palantir format requirements
- Project structure knowledge

### Role-Specific Prompts

Role-specific instructions are embedded in each workflow's `claude_args` parameter
using `--append-system-prompt`. This keeps role definitions versioned alongside the
workflow files.

## Cost Management

### Token Budget Estimates

| Agent | Model | Avg Tokens/Run | Frequency |
|-------|-------|----------------|-----------|
| Architect | opus-4-6 | ~50K | 2-3x/week |
| Developer | sonnet-4-6 | ~100K | 5-10x/week |
| Reviewer | sonnet-4-6 | ~30K | 5-10x/week |
| Triage | haiku-4-5 | ~5K | per new issue |

### Cost Controls

- Set `--max-turns` to prevent runaway loops
- Use GitHub Actions concurrency controls
- Configure API spending limits in Anthropic console
- Reviewer uses Sonnet (cheaper) since it does not need to write complex code

## Getting Started

### Step 1: Install GitHub App

```bash
# In Claude Code terminal
/install-github-app
```

### Step 2: Add API Key

Go to **Settings > Secrets and variables > Actions** and add `ANTHROPIC_API_KEY`.

### Step 3: Copy Workflow Files

The workflow files are already in `.github/workflows/`. Push them to `branch-17.x.x`.

### Step 4: Create Labels

Create the labels listed in the Label System section above.

### Step 5: Test

1. Create a test issue with `@architect please review the design for adding MinIO support`
2. Verify the architect agent responds
3. Add the `ready-for-dev` label
4. Comment `@developer please implement this`
5. Verify the developer creates a PR
6. Check that the reviewer automatically reviews the PR

## Evolution

This system is designed to grow:

1. **Phase 1 (Current):** Basic agent roles with human gating
2. **Phase 2:** Agents can assign labels (architect marks `ready-for-dev`)
3. **Phase 3:** Agents reference each other's work (developer reads architect's ADR)
4. **Phase 4:** Agent Teams (native Claude Code feature) for parallel work
