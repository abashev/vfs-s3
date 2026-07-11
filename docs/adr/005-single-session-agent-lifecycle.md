# ADR-005: Single-Session Lifecycle Replacing Cross-Session Agent Dispatch

**Status:** Accepted
**Date:** 2026-07-04
**Author:** @abashev

## Context

The original problem: ADR-004's comment trigger phrases (`@vfs-s3-bot please prepare design doc`,
`please proceed with development`, `please review`, `please fix review comments`) require a human
to type the next phrase after every phase completes. Agents cannot move the project forward between
phases on their own.

An earlier draft of this ADR proposed fixing this by binding GitHub label transitions to Claude Code
Routines with native GitHub triggers, removing the need for comment phrases. In practice this did
not hold up: routines are expensive to fire often, and their behavior in practice has been
unreliable. Codex has also been dropped as an engine for this workflow — Claude Code is now the
sole engine, which removes one of the two dispatch mechanisms ADR-004 relied on in the first place.

Investigating why cross-session dispatch underperforms surfaced a structural reason, not just a
tuning problem: every routine/label-triggered invocation starts a **fresh session** that has to
reconstruct state from written artifacts (spec, diff, comments) rather than continuing a
conversation. This matches the "sequential orchestrator that paraphrases" anti-pattern documented in
`addyosmani/agent-skills`' `references/orchestration-patterns.md`: it "loses nuance... skips the
human checkpoints that catch wrong-direction work early... doubles token cost via paraphrasing
turns." The expense and quality problems are inherent to splitting a lifecycle into independently
triggered sessions, not an artifact of routines specifically.

That same project's `/build auto` command demonstrates the alternative: collapse the
implementation phase into one continuous session with a single human approval gate (the plan),
running every task's RED→GREEN→regression→build→commit loop without stopping between tasks, while
still halting on unfixable failures, spec ambiguity, or high-risk/irreversible changes. There is no
cross-session hand-off inside that phase at all.

## Decision

Replace the cross-session, label/routine-triggered dispatch model with **one continuous session per
issue** that moves through phases in-context. Replace the monolithic `.skills/vfs-architect` /
`vfs-developer` / `vfs-reviewer` role files with a persona/skill separation adapted from
`addyosmani/agent-skills`: skills hold the reusable technique (the "how"), personas stay thin and
carry only vfs-s3-specific context (roadmap, `mise exec -- ./gradlew ...` commands, Java 17 /
Palantir style, S3-specific risk checklist).

### Lifecycle

1. **Start.** A human starts one session per issue ("let's work on issue #N"). This replaces every
   per-phase trigger phrase — starting work is one natural gesture, not per-phase toil.
2. **Spec.** `spec-driven-development` skill, invoked in-session. The agent asks clarifying
   questions; the human approves the spec in the same conversation.
3. **Plan.** `planning-and-task-breakdown` skill, in-session. Produces a task breakdown; single
   human approval checkpoint.
4. **Build.** `incremental-implementation` + `test-driven-development`, run autonomously
   (`/build auto`-style) through every task without stopping between them. Stops only for:
   an unfixable test/build failure (→ `debugging-and-error-recovery`), spec ambiguity, or a
   high-risk/irreversible change — auth, migrations, deletions, anything not `git revert`-able
   (→ `doubt-driven-development`, explicit sign-off required).
5. **Review.** A deliberately **fresh, separate context** — the one place a new session is correct,
   not a compromise: an unbiased second look is the point. The `vfs-reviewer` persona, extended with
   the existing S3-specific checks (credential leakage, unexpected public ACLs, SSRF-like URL
   handling), reviews the finished PR independently.
6. **Merge.** Human, unchanged — @abashev only.

### Dropped from the design

- GitHub label events, Claude Code Routines, and any external dispatcher for the design→implement
  transition. No GitHub App install, no per-repo routine configuration.
- Codex as an engine for this workflow.

### Human gates (revised from ADR-001/ADR-004)

1. Spec approval — now happens in-session, before planning starts.
2. Plan approval — the single checkpoint before the build phase runs unattended through every task.
3. Merge — unchanged, @abashev-only.

Stops during the build phase for risk, ambiguity, or failure are the build loop's own safety valve,
not a phase transition a human dispatches — they don't reintroduce the toil this ADR removes.

## Consequences

- No cross-session hand-off across spec → plan → build: token cost and fidelity loss from
  paraphrased context is eliminated for that stretch, since it is one continuous conversation.
- Review stays a separate invocation deliberately, not incidentally.
- Removes the dependency on Claude Code Routines (research-preview, unreliable/costly in practice
  per direct trial) and on Codex automation entirely.
- `.skills/vfs-architect` / `vfs-developer` / `vfs-reviewer` should be refactored into thin persona
  wrappers over shared skills rather than left as-is — this also removes boilerplate (git-lock
  workaround, `gh` setup) currently duplicated across the three files.
- GitHub Actions remain CI-only, consistent with ADR-004 — unaffected by this change.
- The workflow is local/interactive-first: a human must start the session per issue. There is no
  unattended, idle-repo automation (nothing runs on its own overnight). This is an accepted trade —
  reliability and quality over that specific slice of "minimal man in the middle."

## Alternatives Considered

1. **GitHub label events bound to Claude Code Routines** (this ADR's original draft). Rejected
   after real-world trial: expensive to fire often, and the cross-session hand-off degraded quality
   — consistent with the "sequential orchestrator that paraphrases" anti-pattern.
2. **GitHub Actions running agent execution directly** (poll + dedupe + cost cap), considered as a
   fallback once routines misbehaved. Rejected for now: solves the cost/dedupe problem but not the
   cross-session context-loss problem, since each poll-triggered invocation is still a fresh session
   reconstructing state from artifacts. Revisit only if a future need specifically requires
   unattended/idle-repo dispatch.
3. **Keep comment trigger phrases (status quo before this ADR).** Rejected: same manual toil this
   ADR set out to remove, and does not address the persona/skill duplication across `.skills/`.
