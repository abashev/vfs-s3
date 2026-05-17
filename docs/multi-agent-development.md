# AI-Assisted Development Workflow for vfs-s3

## Overview

This document describes how AI assistants are used on the `vfs-s3` project. The workflow is
tool-agnostic: Codex and standard Claude Code sessions should follow the same repository rules,
quality bar, and GitHub process.

This workflow is formalized by [ADR-004](adr/004-multi-agent-dispatch.md), which
updates ADR-001 for Codex automation and Claude Code routines.

> **Language policy:** All postings to GitHub (issue comments, PR descriptions, review comments)
> must be written in **US English**.

> **Authorization:** Only @abashev decides when AI-generated work is accepted, pushed, reviewed,
> or merged.

## Roles

AI sessions can be used in three practical roles. The role instructions live in `.skills/` and can
be dispatched by Codex automation, Claude routines, or invoked manually in an interactive session.

For this repository, Codex on GPT-5.5 is the preferred default assistant. In current project use, it
works materially better than Opus for implementation and review work because it follows local
instructions closely, handles multi-file Java/Gradle changes reliably, and keeps verification tied
to concrete commands.

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

Use this mode for PR review.

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
2. Trigger the relevant role through Codex automation, a Claude routine, or an interactive assistant session.
3. Create a focused branch from `17.0`.
4. Run relevant tests.
5. Push the branch and open a PR.
6. Wait for review and approval.

Suggested GitHub trigger phrases:

```text
@vfs-s3-bot please prepare design doc
@vfs-s3-bot please proceed with development
@vfs-s3-bot please review
@vfs-s3-bot please fix review comments
```

Suggested branch names:

```text
feature/issue-N-short-description
feature/short-description
```

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
