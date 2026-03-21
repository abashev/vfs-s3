# Build & Run

This project uses Gradle with the Gradle wrapper for local development.

## Session Setup

**At the start of every Cowork session**, run the setup script:
```sh
source .cowork/setup.sh
```
This installs `gh` CLI, loads the bot token, configures git author, and installs `mise` for Java toolchain management.

## Local Development (with mise + Gradle wrapper)

Use `mise exec -- ./gradlew` instead of `./gradlew` directly. `mise` ensures the correct Java version is used.

```sh
mise exec -- ./gradlew compile
mise exec -- ./gradlew test
mise exec -- ./gradlew integrationTest
mise exec -- ./gradlew check
```

### Test Separation

Tests are split into two categories using Gradle source sets:

- **Unit tests** (`./gradlew test`) — runs `*Test.java` files from `src/test/java`
- **Integration tests** (`./gradlew integrationTest`) — runs `*IT.java` files from `src/integrationTest/java`

Integration tests require AWS credentials and external resources (S3 buckets, etc.).

## CI Environment (GitHub Actions)

On CI runners, Java is installed via `actions/setup-java`. The Gradle wrapper is committed to the repo:

```sh
./gradlew compile
./gradlew test
./gradlew check integrationTest
```

# Java Style

- Target: **Java 17**. Use language features wherever possible: `var`, `record`, `sealed`, pattern matching `instanceof`, text blocks, `switch` expressions, static imports.
- Code is formatted with **Palantir Java Format (Palantir style, 4-space indent, 120 char line)**. The pre-commit hook will reject unformatted code.
- All used classes should be imported. Use fully-qualified names ONLY when there are duplicate simple names.

# Project Structure

- `commons-vfs/` - Apache Commons VFS2 adapter for S3 (package `com.github.vfss3.commonsvfs`)
- `jdk/` - JDK NIO.2 `FileSystemProvider` for S3 (package `com.github.vfss3.jdk`)
- `spring-6/` - Spring 6 `Resource` / `ResourceLoader` for S3 (package `com.github.vfss3.spring6`)
- `buildSrc/` - Shared Gradle convention plugin (`vfs-s3.java-conventions`)
- `docs/` - Documentation, including ADRs in `docs/adr/`
- `samples/` - Usage examples

# Multi-Agent Development

This project uses Cowork skills for AI-assisted development. See `docs/multi-agent-development.md` for full details.

## Agent Roles

- **Architect** - Architectural review and API design (Opus) — triggered by `@vfs-s3-bot please prepare design doc`
- **Developer** - Feature implementation and bug fixes (Sonnet) — triggered by `@vfs-s3-bot please proceed with development`
- **Reviewer** - Code review on PRs (Sonnet) — triggered by `@vfs-s3-bot please review`

Only @abashev's mentions are processed. All other mentions are ignored.

## Git Lock Workaround

The repo folder is shared between the host macOS and the Cowork VM. Claude Code's frequent
`git status` polling creates stale `.git/index.lock` files that block git operations
([claude-code#11005](https://github.com/anthropics/claude-code/issues/11005)).

**Rules for all agents:**
- Use `--no-optional-locks` flag on all read-only git commands (`git status`, `git diff`)
- The setup script configures `git s` and `git d` aliases that include `--no-optional-locks`
- Never use bare `git status` or `git diff` — always add `--no-optional-locks`

## Workflow for Agents

1. All code changes must target `17.0`
2. Never merge PRs directly - always wait for owner (@abashev) approval
3. Check issue comments for @architect guidance before implementing
4. Use git worktrees for feature branches (`git worktree add ../vfs-s3-issue-N 17.0 -b issue-N`)
5. Run `mise exec -- ./gradlew test` before committing
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
