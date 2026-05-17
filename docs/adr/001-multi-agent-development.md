# ADR-001: Multi-Agent AI Development Workflow

**Status:** Accepted, amended by [ADR-004](004-multi-agent-dispatch.md)
**Date:** 2026-03-10
**Author:** @abashev

## Context

The vfs-s3 project aims to become "AI-native" as part of the 17.0 roadmap. We need a system
that allows AI agents to contribute to the project while maintaining quality and human oversight.

## Decision

We adopt a multi-agent workflow with four specialized roles: Architect, Developer, Reviewer, and
Triage. Each role has its own trigger phrase and system prompt. Agents communicate through GitHub
issues and PR comments.

### Key design choices:

1. **Separate role prompts** rather than a single prompt with mode switching.
   This keeps each agent's permissions, model choice, and prompt isolated.

2. **Human gating at two checkpoints:** design approval (label `ready-for-dev`) and
   merge approval (only @abashev merges).

3. **Structured comment format** so agents and humans can quickly parse agent output.

4. **Codex on GPT-5.5 for architecture, implementation, and review by default.**
   Lower-cost models may be used for simple triage when appropriate.

## Consequences

- Every issue and PR will receive faster initial feedback
- Cost scales with activity (~$50-150/month estimated for moderate activity)
- Human review remains mandatory before merge
- Agents cannot escalate their own permissions
- Fork PRs are excluded from agent processing (security)

## Alternatives Considered

1. **Single agent with all roles** - Rejected: too much prompt context, hard to tune
2. **External orchestrator (e.g., Ruflo)** - Rejected: adds complexity; Codex automation or Claude Code routines are sufficient
3. **Claude Code Agent Teams** - Considered for Phase 4 when the feature is stable
