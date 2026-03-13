# Build & Run

This project uses Maven with the `mise` tool manager for local development.

## Session Setup

**At the start of every Cowork session**, run the setup script:
```sh
source .cowork/setup.sh
```
This installs `gh` CLI, loads the bot token, configures git author, and trusts `mise`.

## Local Development (with mise)

- Use `mise exec -- mvn <args>` instead of `mvn` directly. There is no Maven wrapper (`mvnw`) in this project.

```sh
mise exec -- mvn compile
mise exec -- mvn test -pl <module>
mise exec -- mvn test -pl <module> -am
mise exec -- mvn verify
```

### Test Separation

Tests are split into two categories using Maven Surefire and Failsafe plugins:

- **Unit tests** (`mvn test`) — runs `*Test.java` files with Surefire
- **Integration tests** (`mvn verify`) — runs `*IT.java` files with Failsafe

Integration tests require AWS credentials and external resources (S3 buckets, etc.).

## CI Environment (GitHub Actions)

On CI runners, Java and Maven are installed via `actions/setup-java`. Use `mvn` directly:

```sh
mvn compile
mvn test
```

# Java Style

- Target: **Java 17**. Use language features wherever possible: `var`, `record`, `sealed`, pattern matching `instanceof`, text blocks, `switch` expressions, static imports.
- Code is formatted with **Palantir Java Format (Palantir style, 4-space indent, 120 char line)**. The pre-commit hook will reject unformatted code.
- All used classes should be imported. Use fully-qualified names ONLY when there are duplicate simple names.

# Project Structure

- `src/` - Main source and tests (single module, will be split into multi-module)
- `docs/` - Documentation, including ADRs in `docs/adr/`
- `samples/` - Usage examples

# Multi-Agent Development

This project uses Cowork skills for AI-assisted development. See `docs/multi-agent-development.md` for full details.

## Agent Roles

- **Architect** - Architectural review and API design (Opus) — triggered by `@vfs-s3-bot please prepare design doc`
- **Developer** - Feature implementation and bug fixes (Sonnet) — triggered by `@vfs-s3-bot please proceed with development`
- **Reviewer** - Code review on PRs (Sonnet) — triggered by `@vfs-s3-bot please review`

Only @abashev's mentions are processed. All other mentions are ignored.

## Workflow for Agents

1. All code changes must target `17.0`
2. Never merge PRs directly - always wait for owner (@abashev) approval
3. Check issue comments for @architect guidance before implementing
4. Use git worktrees for feature branches (`git worktree add ../vfs-s3-issue-N 17.0 -b issue-N`)
5. Run `mise exec -- mvn test` before committing
6. Keep PRs focused: one feature or fix per PR
7. Reference the related issue number in every PR
8. All GitHub postings must be in **US English**
9. Use `gh` CLI with bot token for creating PRs and posting comments

## Bot Identity

- **Git commits**: `Claude (vfs-s3 bot) <267615948+vfs-s3-bot@users.noreply.github.com>`
- **GitHub account**: `@vfs-s3-bot`
- **GitHub PRs/comments**: via `gh` CLI authenticated as the bot account
- **Token storage**: `.cowork/github-bot-token` (gitignored)

## Roadmap Context

When working on issues, be aware of the current roadmap:
- Migrate to Java 17, Gradle, and Palantir code style
- Update dependencies and set up local testing with LocalStack and MinIO
- Split into multi-module: native filesystem, Spring integration, Commons VFS adapter
- Make the project AI-native (this agent system)
- Shade AWS SDK to avoid version conflicts
