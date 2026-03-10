# ADR-001: Multi-Agent AI Development Workflow

**Status:** Accepted
**Date:** 2026-03-10
**Author:** @abashev

## Context

The vfs-s3 project aims to become "AI-native" as part of the 17.x.x roadmap. We need a system
that allows AI agents to contribute to the project while maintaining quality and human oversight.

## Decision

We adopt a multi-agent workflow using Claude Code GitHub Actions with four specialized roles:
Architect, Developer, Reviewer, and Triage. Each role has its own GitHub Actions workflow file,
trigger phrase, and system prompt. Agents communicate through GitHub issues and PR comments.

### Key design choices:

1. **Separate workflows per role** rather than a single workflow with mode switching.
   This keeps each agent's permissions, model choice, and prompt isolated.

2. **Human gating at two checkpoints:** design approval (label `ready-for-dev`) and
   merge approval (only @abashev merges).

3. **Structured comment format** so agents and humans can quickly parse agent output.

4. **Opus for architecture, Sonnet for implementation/review, Haiku for triage.**
   Matches cost to complexity.

## Consequences

- Every issue and PR will receive faster initial feedback
- Cost scales with activity (~$50-150/month estimated for moderate activity)
- Human review remains mandatory before merge
- Agents cannot escalate their own permissions
- Fork PRs are excluded from agent processing (security)

## Alternatives Considered

1. **Single agent with all roles** - Rejected: too much prompt context, hard to tune
2. **External orchestrator (e.g., Ruflo)** - Rejected: adds complexity, GitHub Actions is sufficient
3. **Claude Code Agent Teams** - Considered for Phase 4 when the feature is stable
