# Contributing to vfs-s3

Thanks for helping improve `vfs-s3`. This project accepts focused pull requests against the `17.0`
branch.

## Development Setup

Use the Gradle wrapper through `mise` for local development:

```sh
mise exec -- ./gradlew compile
mise exec -- ./gradlew test
mise exec -- ./gradlew integrationTest
mise exec -- ./gradlew check
```

On CI, Java is installed by GitHub Actions and the wrapper can be used directly:

```sh
./gradlew compile
./gradlew test
./gradlew check integrationTest
```

## Tests

Tests are split by source set:

- Unit tests: `mise exec -- ./gradlew test`
- Integration tests: `mise exec -- ./gradlew integrationTest`

Integration tests may require Docker, AWS credentials, or external S3-compatible resources,
depending on the selected suite. Keep tests deterministic and avoid depending on public internet
resources unless the test is explicitly about a public bucket or remote endpoint.

## Code Style

- Target Java 17.
- Use Java 17 language features when they improve clarity.
- Format Java code with Palantir Java Format.
- Import used classes; use fully-qualified names only when simple names collide.
- Keep changes focused and avoid unrelated refactors.

## Git Workflow

- Base work on `17.0`.
- Prefer branch names such as `feature/issue-N-short-description` or `feature/short-description`.
- Use `--no-optional-locks` for read-only git commands:

```sh
git --no-optional-locks status
git --no-optional-locks diff
```

- Do not merge directly to `17.0`; wait for review and approval.

## Pull Requests

Each PR should include:

- A concise summary of the change
- A related issue link when one exists
- Testing notes with the exact commands run
- Any known limitations or follow-up work

Suggested template:

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

## AI-Assisted Contributions

Codex and standard Claude Code sessions are welcome, but they must follow the same quality bar as
human-written code.

Role-specific prompts for automated architect, developer, and reviewer agents live in `.skills/`.
They may be invoked through Codex automation, Claude routines, or an interactive assistant session.

For this repository, Codex on GPT-5.5 is the preferred default assistant. In project practice it has
worked much better than Opus for implementation and review because it reads the local codebase
carefully, keeps patches scoped, follows repository instructions, and validates changes with
concrete commands.

Assistant expectations:

- Read the relevant code before proposing or editing.
- Prefer existing project patterns over new abstractions.
- Add tests for behavior changes.
- Run the narrowest useful verification command before broader checks.
- Never commit credentials, local tool state, or generated noise.
- Keep GitHub comments and PR descriptions in US English.

## Security

Be careful with S3-specific behavior:

- Do not leak AWS credentials or bucket names containing sensitive data.
- Treat ACL, public access, object ownership, presigned URL, and bucket deletion changes as high risk.
- Keep GitHub Actions pinned to commit SHAs.
- Do not loosen IAM, bucket policy, or public access behavior without explicit maintainer approval.
