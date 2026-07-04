# Build & Run

This project uses Gradle with the Gradle wrapper for local development.

## Local Development

Use `mise exec -- ./gradlew` instead of `./gradlew` directly. `mise` ensures the correct Java
version is used.

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

Integration tests may require AWS credentials and external resources such as S3 buckets.

## CI Environment

On CI runners, Java is installed via `actions/setup-java`. The Gradle wrapper is committed to the repo:

```sh
./gradlew compile
./gradlew test
./gradlew check integrationTest
```

# Java Style

- Target: **Java 17**. Use language features where they improve clarity: `var`, `record`, `sealed`, pattern matching `instanceof`, text blocks, `switch` expressions, and static imports.
- Code is formatted with **Palantir Java Format** (Palantir style, 4-space indent, 120 char line).
- All used classes should be imported. Use fully-qualified names only when there are duplicate simple names.

# Project Structure

- `commons-vfs/` - Apache Commons VFS2 adapter for S3 (package `com.github.vfss3.commonsvfs`)
- `jdk/` - JDK NIO.2 `FileSystemProvider` for S3 (package `com.github.vfss3.jdk`)
- `spring-6/` - Spring 6 `Resource` / `ResourceLoader` for S3 (package `com.github.vfss3.spring6`)
- `spring-7/` - Spring 7 `Resource` / `ResourceLoader` for S3 (package `com.github.vfss3.spring7`)
- `buildSrc/` - Shared Gradle convention plugin (`vfs-s3.java-conventions`)
- `docs/` - Documentation, including ADRs in `docs/adr/`
- `samples/` - Usage examples

# AI-Assisted Development

Claude Code is the engine for this workflow: one continuous session per issue moving through
Spec → Plan → Build in-context, with Review as a deliberately separate fresh session. See
[ADR-005](docs/adr/005-single-session-agent-lifecycle.md) and `docs/multi-agent-development.md`
for the full lifecycle. Role-specific persona prompts are kept in `.skills/`.

## Agent Roles

- **Architect** - Architectural review and API design before non-trivial implementation
- **Developer** - Feature implementation and bug fixes
- **Reviewer** - Code review on PRs, run as a separate fresh-context session

## Git Lock Workaround

This repository may be edited by local tools that poll git state frequently. To avoid stale
`.git/index.lock` files, use `--no-optional-locks` on read-only git commands.

**Rules for all agents:**
- Use `--no-optional-locks` for read-only git commands (`git status`, `git diff`)
- Never use bare `git status` or `git diff`; always add `--no-optional-locks`

## Workflow for Agents

1. All code changes must target `17.0`
2. Never merge PRs directly; always wait for review and approval
3. Check issue comments for architectural guidance before implementing
4. Use focused branches, preferably with the `feature/` prefix
5. Run `mise exec -- ./gradlew test` before committing
6. Keep PRs focused: one feature or fix per PR
7. Reference the related issue number in every PR when one exists
8. All GitHub postings must be in **US English**
9. Use `gh` CLI with an authenticated local environment for creating PRs and posting comments

## Roadmap Context

When working on issues, be aware of the current roadmap:

- Migrate to Java 17, Gradle, and Palantir code style
- Update dependencies and set up local testing with LocalStack and MinIO
- Split into multi-module: native filesystem, Spring integration, Commons VFS adapter
- Make the project AI-native with clear assistant-readable instructions
- Shade AWS SDK to avoid version conflicts
