# Multi-Agent Development System for vfs-s3

## Overview

This document describes the multi-agent AI-native development workflow for the `vfs-s3` project.
The system uses **Cowork skills** to create a team of specialized AI agents that are invoked by
**@mentioning the bot account** (`@vfs-s3-bot`) in GitHub issue or PR comments.

A scheduled task periodically checks the bot's notification inbox. When a mention from the
project owner (@abashev) is detected, Cowork launches the appropriate skill to handle the request.

> **Language policy:** All postings to GitHub (issue comments, PR descriptions, review comments)
> must be written in **US English**.

> **Authorization:** The bot only responds to @mentions from **@abashev**. Mentions from any
> other user are ignored — this prevents unauthorized triggering via external issue comments.

## Agent Roles

### 1. Architect

**Cowork skill:** `.skills/vfs-architect/`

**Trigger:** `@vfs-s3-bot please prepare design doc` (in an issue comment)

**Responsibilities:**
- Review architectural decisions and propose alternatives
- Design APIs and module boundaries (especially for the multi-module split)
- Create Architecture Decision Records (ADRs) in `docs/adr/`
- Provide design guidance before implementation starts
- Validate that proposed changes follow the agreed architecture

**Model preference:** Opus (complex reasoning needed)

### 2. Developer

**Cowork skill:** `.skills/vfs-developer/`

**Trigger:** `@vfs-s3-bot please proceed with development` (in an issue comment)

**Responsibilities:**
- Implement features and fixes based on issue descriptions
- Create branches and pull requests with working code
- Follow coding standards from `CLAUDE.md` (Java 17, Palantir format)
- Write unit tests alongside implementation
- Check for architect guidance before implementing non-trivial changes

**Model preference:** Sonnet (good balance of speed and quality)

**Build environment:** Uses `mise exec -- mvn <args>` for local builds. On CI, Maven is
available directly via `actions/setup-java`.

### 3. Reviewer

**Cowork skill:** `.skills/vfs-reviewer/`

**Trigger:** `@vfs-s3-bot please review` (in a PR comment)

**Responsibilities:**
- Review external pull requests (from other contributors) for code quality, correctness, and security
- Check adherence to Java 17 idioms and Palantir code style
- Verify test coverage and testing approach
- Flag potential credential leaks and SSRF vulnerabilities (S3-specific)
- Suggest improvements

Note: For bot-created PRs, the internal review loop is built into the developer skill (Phase 3).
The reviewer is intended for PRs from external contributors.

**Model preference:** Sonnet

## Invocation Model

### How It Works

```
@abashev writes a comment on GitHub:
  "@vfs-s3-bot please prepare design doc"
                    │
                    v
  Scheduled task checks bot inbox via:
    gh api /notifications?participating=true
                    │
                    v
  Filters for reason: "mention", author: "abashev"
                    │
                    v
  Parses the command and determines the skill:
    "design doc"   → vfs-architect
    "development"  → vfs-developer
    "review"       → vfs-reviewer
                    │
                    v
  Launches the appropriate Cowork skill
  with the issue/PR number as context
                    │
                    v
  Skill reads the issue, analyzes code, and
  posts results back as a GitHub comment
                    │
                    v
  Marks the notification as read:
    gh api /notifications/threads/<id> --method PATCH
```

### Command Reference

| Comment in GitHub | Skill Invoked | Context |
|-------------------|---------------|---------|
| `@vfs-s3-bot please prepare design doc` | Architect | Issue |
| `@vfs-s3-bot please proceed with development` | Developer | Issue |
| `@vfs-s3-bot please review` | Reviewer | External PR |

Commands are flexible — the scheduled task matches keywords like "design", "develop"/"implement",
and "review" to determine the appropriate skill. Only @abashev's mentions are processed.

## Communication Protocol

### Issue Lifecycle

```
                    +-----------+
                    |  Created  |  (@abashev opens issue)
                    +-----+-----+
                          |
                          v
                 +--------+--------+
                 | @abashev writes |  "@vfs-s3-bot please
                 | issue comment   |   prepare design doc"
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Bot posts       |  Architect review
                 | design review   |  as issue comment
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | @abashev writes |  "@vfs-s3-bot please
                 | issue comment   |   proceed with development"
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Bot implements  |  Worktree, code,
                 | the change      |  build, test
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Internal review |  Bot self-reviews diff,
                 | loop (automatic)|  fixes issues, re-commits
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Bot pushes      |  PR created on GitHub
                 | branch + PR     |
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | @abashev       |  Final approval
                 | reviews + merge |  and merge
                 +-----------------+

External PRs (from other contributors):

                 +--------+--------+
                 | External PR     |  Someone opens a PR
                 | is created      |
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | @abashev writes |  "@vfs-s3-bot please review"
                 | PR comment      |
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Bot posts       |  Code review comment
                 | review          |  on the PR
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | @abashev       |  Final approval
                 | reviews + merge |  and merge
                 +-----------------+
```

### Label System

| Label | Meaning | Who Sets It |
|-------|---------|-------------|
| `bug` | Bug report | Human |
| `enhancement` | Feature request | Human |
| `question` | Needs clarification | Human |
| `documentation` | Documentation improvement | Human |
| `needs-design` | Issue needs architectural review | Human |
| `ready-for-dev` | Design approved, ready for implementation | Human (after architect review) |
| `good-first-issue` | Simple enough for a quick fix | Human |

### Comment Conventions

Agent reviews use structured comments for clarity:

```markdown
## Architect Review

**Decision:** Proceed with Option B
**Rationale:** Better separation of concerns for the multi-module split
**Impact:** Changes to `S3FileProvider` public API
**Action items:**
- [ ] Define new interface in `core` module
- [ ] Move S3-specific logic to `s3-provider` module

---
*Review by @vfs-s3-bot (architect). Final approval: @abashev*
```

### PR Description Template (Developer)

```markdown
## Summary
Brief description of what this PR does.

## Related Issue
Closes #123

## Design Reference
Based on architect review in #123 (comment link)

## Changes
- What was added/changed/removed

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests updated (if applicable)
- [ ] `mvn test` passes
- [ ] `mvn verify` passes (if integration tests changed)

---
*PR created by @vfs-s3-bot (developer). Tag @abashev for review.*
```

## Setup

### Prerequisites

1. **Claude Max subscription** with Cowork mode enabled
2. The project repository cloned locally
3. Cowork skills installed in `.skills/` directory
4. A dedicated GitHub bot account (`@vfs-s3-bot`) with Collaborator access

### Bot Account (`@vfs-s3-bot`)

1. Create a GitHub account for the bot
2. Grant it Collaborator access to `abashev/vfs-s3`
3. Create a fine-grained PAT with minimal scope (see Authentication below)
4. Save the PAT to `.cowork/github-bot-token`

### Skill Files

| Directory | Agent | Purpose |
|-----------|-------|---------|
| `.skills/vfs-architect/` | Architect | Architectural review, API design, ADRs |
| `.skills/vfs-developer/` | Developer | Feature implementation, bug fixes |
| `.skills/vfs-reviewer/` | Reviewer | Code review, security checks |

### How to Use

1. **Open an issue** on GitHub describing what you need
2. **Write a comment** mentioning the bot: `@vfs-s3-bot please prepare design doc`
3. **The scheduled task** picks up the mention and launches the appropriate skill
4. **The bot posts** its analysis or implementation as a GitHub comment or PR

Alternatively, you can invoke skills directly in Cowork by describing what you need.

### Typical Workflow Example

```
In GitHub issue #185:

@abashev:  "@vfs-s3-bot please prepare design doc"
           → Scheduled task detects the mention
           → Architect skill analyzes the issue and codebase
           → Bot posts a design review as an issue comment

@abashev:  (reads the review, approves) "@vfs-s3-bot please proceed with development"
           → Developer skill creates a worktree + branch (issue-185)
           → Implements the change, builds, tests, commits
           → Runs internal review loop: self-reviews, fixes, re-commits
           → Pushes the branch and creates a PR

In the PR:

@abashev:  (reviews the code) "@vfs-s3-bot please review"
           → Reviewer skill analyzes the diff
           → Bot posts a code review comment on the PR

@abashev:  Approves and merges the PR
```

### Bot Account and Authentication

All agent interactions with GitHub use the `@vfs-s3-bot` account authenticated via `gh` CLI
with a fine-grained Personal Access Token (PAT).

**PAT scope — principle of least privilege:**
- `repo:contents` — read/write (push branches)
- `repo:pull_requests` — read/write (create PRs, post review comments)
- `repo:issues` — read/write (read issues, post comments, add labels)
- `repo:metadata` — read (required by GitHub)
- `notifications` — read (check bot inbox for @mentions)
- **Do NOT grant:** `repo:admin`, `repo:actions`, `repo:secrets`, `org:*`

The token is stored in `.cowork/github-bot-token` (gitignored) and loaded by `source .cowork/setup.sh`
at the start of each session.

### Branch Protection

Enable branch protection on `branch-17.x.x` in GitHub repository settings:
- Require pull request reviews before merging (at least 1 review from @abashev)
- Require status checks to pass before merging (CI build)
- Do not allow bypassing the above settings
- Restrict who can push to the branch: only @abashev and the bot account

This ensures that even if an agent misbehaves, it cannot merge or force-push to the main branch.

### Security

- Bot only responds to @mentions from @abashev — all other mentions are ignored
- Bot account uses a fine-grained PAT with minimal scope (see above)
- Human reviews every output before merging
- Credential leak prevention is built into the reviewer skill
- Branch protection prevents unauthorized merges to `branch-17.x.x`

## CLAUDE.md Shared Context

All skills read the shared `CLAUDE.md` at the repo root, which contains:
- Build commands (`mise exec -- mvn ...` for local, `mvn` for CI)
- Java 17 style rules and language feature requirements
- Palantir format requirements (4-space indent, 120 char line)
- Project structure and roadmap context
- Multi-agent workflow rules

## Roadmap Context

When working on issues, agents should be aware of the current roadmap:
- Migrate to Java 17, Gradle, and Palantir code style
- Update dependencies and set up local testing with LocalStack and MinIO
- Split into multi-module: native filesystem, Spring integration, Commons VFS adapter
- Make the project AI-native (this agent system)
- Shade AWS SDK to avoid version conflicts

## Evolution

This system is designed to grow:

1. **Phase 1 (Current):** Cowork skills with @mention invocation via `gh` CLI
2. **Phase 2:** Fully automated scheduled dispatch (bot inbox polling)
3. **Phase 3:** Agent Teams (native Claude feature) for parallel work across issues
