# Build & Run

This project uses Maven with the `mise` tool manager for local development.

## Local Development (with mise)

- Always run `mise trust` at the start of a session before building.
- Use `mise exec -- mvn <args>` instead of `mvn` directly. There is no Maven wrapper (`mvnw`) in this project.

```sh
mise exec -- mvn compile
mise exec -- mvn test -pl <module>
mise exec -- mvn test -pl <module> -am
```

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

This project uses AI agents via GitHub Actions. See `docs/multi-agent-development.md` for full details.

## Agent Roles

- **@architect** - Architectural review and API design (Opus)
- **@developer** - Feature implementation and bug fixes (Sonnet)
- **@reviewer** - Automated code review on every PR (Sonnet)
- **Triage** - Auto-labels new issues (Haiku)

## Workflow for Agents

1. All code changes must target `branch-17.x.x`
2. Never merge PRs directly - always wait for owner (@abashev) approval
3. Check issue comments for @architect guidance before implementing
4. Run `mvn test` before creating or updating PRs (on CI, Java is pre-installed)
5. Keep PRs focused: one feature or fix per PR
6. Reference the related issue number in every PR

## Roadmap Context

When working on issues, be aware of the current roadmap:
- Migrate to Java 17, Gradle, and Palantir code style
- Update dependencies and set up local testing with LocalStack and MinIO
- Split into multi-module: native filesystem, Spring integration, Commons VFS adapter
- Make the project AI-native (this agent system)
- Shade AWS SDK to avoid version conflicts
