# AI-Assisted Development Workflow for vfs-s3

## Overview

This document describes how AI assistants are used on the `vfs-s3` project. Claude Code is the
sole engine for this workflow.

The current lifecycle is formalized by
[ADR-005](adr/005-single-session-agent-lifecycle.md), which replaces the earlier
label/routine-triggered, Codex-dispatched model from
[ADR-004](adr/004-multi-agent-dispatch.md) (itself an update to
[ADR-001](adr/001-multi-agent-development.md)) with **one continuous session per issue**.

> **Language policy:** All postings to GitHub (issue comments, PR descriptions, review comments)
> must be written in **US English**.

> **Authorization:** Only @abashev decides when AI-generated work is accepted, pushed, reviewed,
> or merged.

## Lifecycle

A human starts one Claude Code session per issue ("let's work on issue #N") and stays in that
same conversation through the first three phases:

1. **Spec** — `/spec` (spec-driven-development skill). Clarifying questions, then a spec the
   human approves in-session.
2. **Plan** — `/plan` (planning-and-task-breakdown skill). Task breakdown with a single human
   approval checkpoint.
3. **Build** — `/build auto` (incremental-implementation + test-driven-development skills, via
   the vfs-developer persona). Runs every task's RED → GREEN → regression → build → commit loop
   without stopping between tasks. Stops only for an unfixable failure
   (debugging-and-error-recovery), spec ambiguity, or a high-risk/irreversible change
   (doubt-driven-development, explicit sign-off required).
4. **Review** — a deliberately **separate, fresh-context session** (the vfs-reviewer persona) —
   the one place a new session is correct, not a compromise: an unbiased second look is the point.
5. **Merge** — human, unchanged: @abashev only.

## Roles

AI sessions can be used in three practical roles. The persona prompts live in `.skills/` and are
invoked within an interactive Claude Code session (see Lifecycle above) — not dispatched by
external automation.

### Architect

**Skill:** `.skills/vfs-architect/`

Use this mode before non-trivial implementation work.

Responsibilities:
- Review architectural decisions and propose alternatives
- Design APIs and module boundaries
- Create or update Architecture Decision Records in `docs/adr/`
- Identify compatibility and migration risks before development starts

### Developer

**Skill:** `.skills/vfs-developer/`

Use this mode for focused implementation work.

Responsibilities:
- Implement features and bug fixes from GitHub issues or direct maintainer requests
- Keep changes scoped to one feature or fix
- Add or update tests for changed behavior
- Run the relevant Gradle checks before asking for review
- Prepare a PR with a clear summary and testing notes

### Reviewer

**Skill:** `.skills/vfs-reviewer/`

Use this mode for PR review, always as a separate, fresh-context session from the one that
built the change — see Lifecycle above.

Responsibilities:
- Prioritize correctness, regressions, security, and missing tests
- Check Java 17 idioms and Palantir Java Format compatibility
- Flag S3-specific risks such as credential leakage, unexpected public access, and SSRF-like URL handling
- Keep review findings concrete and tied to file/line references

## Development Rules

- Target branch: `17.0`
- Do not merge PRs directly; wait for review and approval
- Keep PRs focused
- Reference the related issue number in every PR when one exists
- Use `mise exec -- ./gradlew ...` for local Gradle commands
- Use Java 17 language features where they improve clarity
- Format with Palantir Java Format
- Use `--no-optional-locks` for read-only git commands such as `git status` and `git diff`

## Build And Test Commands

Local development:

```sh
mise exec -- ./gradlew compile
mise exec -- ./gradlew test
mise exec -- ./gradlew integrationTest
mise exec -- ./gradlew check
```

CI:

```sh
./gradlew compile
./gradlew test
./gradlew check integrationTest
```

Integration tests are split from unit tests:

- Unit tests: `./gradlew test`
- Integration tests: `./gradlew integrationTest`

Remote integration tests may require AWS credentials and external S3-compatible resources.

## GitHub Workflow

1. Open or select an issue.
2. Start a Claude Code session and say something like "let's work on issue #N" — this replaces
   per-phase trigger phrases; starting the session is the one gesture needed to kick off Spec →
   Plan → Build (see Lifecycle above).
3. Create a focused branch from `17.0`.
4. Run relevant tests.
5. Push the branch and open a PR.
6. Start a **new**, separate session for review; wait for review and approval before merge.

Suggested branch names:

```text
feature/issue-N-short-description
feature/short-description
```

## Human Gates

Per ADR-005, there are three checkpoints:

1. **Spec approval** — in-session, before planning starts.
2. **Plan approval** — the single checkpoint before the build phase runs unattended through
   every task.
3. **Merge** — unchanged, @abashev-only.

Stops during the build phase for risk, ambiguity, or failure are the build loop's own safety
valve, not a phase transition a human has to dispatch.

## PR Description Template

```markdown
## Summary
Brief description of what this PR does.

## Related Issue
Closes #123

## Changes
- What was added, changed, or removed

## Testing
- [ ] `mise exec -- ./gradlew test`
- [ ] `mise exec -- ./gradlew integrationTest` (if integration behavior changed)
- [ ] Other relevant checks
```

## Security

- Do not commit tokens, credentials, bucket names containing sensitive data, or local tool state
- Prefer environment variables or the host credential store for GitHub and AWS credentials
- Review changes touching S3 URLs, object permissions, ACLs, presigned URLs, and bucket deletion carefully
- Keep GitHub Actions pinned to commit SHAs

## Roadmap Context

When working on issues, keep the current roadmap in mind:

- Migrate to Java 17, Gradle, and Palantir code style
- Update dependencies and set up local testing with LocalStack and MinIO
- Split into multi-module: native filesystem, Spring integration, Commons VFS adapter
- Make the project AI-native with clear assistant-readable instructions
- Shade AWS SDK to avoid version conflicts
