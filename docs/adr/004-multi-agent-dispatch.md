# ADR-004: Multi-Agent Dispatch via Codex Automation and Claude Code Routines

**Status:** Accepted, amended by [ADR-005](005-single-session-agent-lifecycle.md)
**Date:** 2026-05-17
**Author:** @abashev

## Context

ADR-001 adopted a multi-agent AI development workflow with specialized roles. That role model
remains correct: the project still uses architect, developer, and reviewer agents with separate
prompts and clear responsibilities.

What changed is the dispatch mechanism. The project no longer needs repository-specific session
setup files or workflow-level agent execution. Agent dispatch should happen through Codex
automation or Claude Code routines, while the repository keeps the reusable role prompts and shared
rules.

## Decision

We keep the multi-agent workflow and update its execution mechanism:

- `.skills/` remains the source of role-specific instructions for architect, developer, and
  reviewer agents.
- Codex automation or Claude Code routines dispatch those skills from GitHub mentions.
- `AGENTS.md` is the primary shared instruction file for Codex and other coding assistants.
- `CONTRIBUTING.md` is the human-facing contribution guide.
- `docs/multi-agent-development.md` describes the shared multi-agent process.
- Authentication is provided by the automation runner (`GH_TOKEN`) or an authenticated `gh` CLI
  session, not by token files stored under project-specific setup directories.
- Pull requests remain the review and integration boundary.

For this repository, Codex on GPT-5.5 is the preferred default assistant. In current project use, it
works materially better than Opus for implementation and review work: it follows local repository
instructions closely, handles multi-file Java/Gradle changes reliably, and keeps verification tied
to concrete commands. Claude Code remains a supported tool when it is useful.

## Consequences

- The multi-agent workflow remains intact.
- Agent prompts stay versioned in the repository and can be reused by different automation runners.
- The repository no longer needs project-specific session setup scripts.
- GitHub Actions remain focused on CI, dependency checks, security scanning, and branch updates.
- Model choices can evolve without changing repository structure.
- Human review remains mandatory before merge.

## Alternatives Considered

1. **Remove specialized skill directories.** Rejected because the project still benefits from
   explicit role prompts for architect, developer, and reviewer agents.
2. **Use workflow-level agent execution.** Rejected because Codex automation and Claude Code
   routines provide the needed scheduling and dispatch without adding CI workflow complexity.
3. **Use a single human-only contribution guide.** Rejected because coding assistants need explicit
   repository rules for build commands, test separation, git safety, and PR expectations.
