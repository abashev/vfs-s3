# Multi-Agent Development System for vfs-s3

## Overview

This document describes the multi-agent AI-native development workflow for the `vfs-s3` project.
The system uses **Cowork skills** to create a team of specialized AI agents that communicate
through GitHub issues and pull requests, with the project owner (@abashev) as the final approver.

All agents operate through a Claude Max subscription via the Cowork desktop app. The project
owner invokes the appropriate skill manually when needed, reviews the output, and posts it
to GitHub. This keeps the human in the loop at every step while leveraging AI for analysis
and implementation work.

> **Language policy:** All postings to GitHub (issue comments, PR descriptions, review comments)
> must be written in **US English**.

## Agent Roles

### 1. Architect (`@architect`)

**Cowork skill:** `.skills/vfs-architect/`

**Responsibilities:**
- Review architectural decisions and propose alternatives
- Design APIs and module boundaries (especially for the multi-module split)
- Create Architecture Decision Records (ADRs) in `docs/adr/`
- Provide design guidance before implementation starts
- Validate that proposed changes follow the agreed architecture

**When to invoke:**
- New feature issues that need design discussion
- Questions about module boundaries or API design
- Before implementing PRs that change public APIs or module structure

**Model preference:** Opus (complex reasoning needed)

### 2. Developer (`@developer`)

**Cowork skill:** `.skills/vfs-developer/`

**Responsibilities:**
- Implement features and fixes based on issue descriptions
- Create branches and pull requests with working code
- Follow coding standards from `CLAUDE.md` (Java 17, Palantir format)
- Write unit tests alongside implementation
- Check for architect guidance before implementing non-trivial changes

**When to invoke:**
- After the architect review is posted and the owner approves the design —
  just tell Cowork: *"implement issue #179 following the architect's plan"*
- Bug reports with clear reproduction steps — no architect review needed
- Small improvements and refactoring tasks

**Model preference:** Sonnet (good balance of speed and quality)

**Build environment:** Uses `mise exec -- mvn <args>` for local builds. On CI, Maven is
available directly via `actions/setup-java`.

### 3. Reviewer (`@reviewer`)

**Cowork skill:** `.skills/vfs-reviewer/`

**Responsibilities:**
- Review pull requests for code quality, correctness, and security
- Check adherence to Java 17 idioms and Palantir code style
- Verify test coverage and testing approach
- Flag potential credential leaks and SSRF vulnerabilities (S3-specific)
- Suggest improvements

**When to invoke:**
- Before merging any PR
- When code changes need a second pair of eyes

**Model preference:** Sonnet

### 4. Triage

**Cowork skill:** `.skills/vfs-triage/`

**Responsibilities:**
- Categorize new issues and suggest labels
- Connect issues to the project roadmap
- Identify duplicates and related issues

**When to invoke:**
- When new issues are created
- Periodic cleanup of the issue backlog

**Model preference:** Haiku (fast, lightweight categorization)

## Communication Protocol

### Issue Lifecycle

```
                    +-----------+
                    |  Created  |  (human opens issue)
                    +-----+-----+
                          |
                          v
                 +--------+--------+
                 | Owner invokes   |  Design discussion
                 | architect skill |  (review posted as comment)
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Owner approves  |  Label: ready-for-dev
                 | the design      |
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Owner invokes   |  Creates branch + PR
                 | developer skill |  with implementation
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Owner invokes   |  Code review posted
                 | reviewer skill  |  as PR comment
                 +--------+--------+
                          |
                          v
                 +--------+--------+
                 | Owner reviews   |  Final approval
                 | and merges PR   |  and merge
                 +-----------------+
```

### Label System

| Label | Meaning | Who Sets It |
|-------|---------|-------------|
| `bug` | Bug report | Triage / Human |
| `enhancement` | Feature request | Triage / Human |
| `question` | Needs clarification | Triage / Human |
| `documentation` | Documentation improvement | Triage / Human |
| `needs-design` | Issue needs architectural review | Human / Triage |
| `ready-for-dev` | Design approved, ready for implementation | Human (after architect review) |
| `good-first-issue` | Simple enough for a quick fix | Triage / Human |

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
*Review by @architect. Final approval: @abashev*
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
- [ ] Unit tests added/updated
- [ ] Integration tests updated (if applicable)
- [ ] `mvn test` passes
- [ ] `mvn verify` passes (if integration tests changed)

---
*PR created by @developer. Tag @reviewer for code review.*
```

## Setup

### Prerequisites

1. **Claude Max subscription** with Cowork mode enabled
2. The project repository cloned locally
3. Cowork skills installed in `.skills/` directory

### Skill Files

| Directory | Agent | Purpose |
|-----------|-------|---------|
| `.skills/vfs-architect/` | Architect | Architectural review, API design, ADRs |
| `.skills/vfs-developer/` | Developer | Feature implementation, bug fixes |
| `.skills/vfs-reviewer/` | Reviewer | Code review, security checks |
| `.skills/vfs-triage/` | Triage | Issue categorization, labeling |

### How to Use

1. **Open Cowork** and select the project folder
2. **Describe what you need** in natural language — Cowork will use the appropriate skill
3. **Review the output** — the skill will analyze the codebase and produce a structured review or implementation
4. **Post to GitHub** — copy the output as a comment, or let the skill post via the browser

### Typical Workflow Example

Here is a concrete example of the full cycle for issue #179:

```
You:  "Review the architecture for issue #179 — splitting unit and integration tests"
      → Cowork uses the architect skill, analyzes the codebase, produces a review
      → You approve and post the review as a comment on the issue

You:  "Implement issue #179 following the architect's plan"
      → Cowork uses the developer skill, reads the architect's comment,
        renames test files, adds maven-failsafe-plugin, creates a branch and PR
      → You review the PR locally, push, and open it on GitHub

You:  "Review the PR for issue #179"
      → Cowork uses the reviewer skill, checks code quality and security
      → You post the review as a PR comment

You:  Approve and merge the PR
```

The key principle: **you drive the process** by telling Cowork what to do next.
Each step produces output that you review before it goes to GitHub.

### Security

- All actions go through the project owner — no direct API access to GitHub
- No API keys needed (uses Claude Max subscription)
- Human reviews every output before it reaches GitHub
- Credential leak prevention is built into the reviewer skill

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

1. **Phase 1 (Current):** Cowork skills with human-in-the-loop for every action
2. **Phase 2:** Skills can directly interact with GitHub via browser automation
3. **Phase 3:** Scheduled skills for periodic triage and review
4. **Phase 4:** Agent Teams (native Claude feature) for parallel work across issues
