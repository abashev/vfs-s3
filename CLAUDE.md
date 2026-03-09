# Build & Run

This project uses Maven with the `mise` tool manager.

- Always run `mise trust` at the start of a session before building.
- Use `mise exec -- mvn <args>` instead of `mvn` directly. There is no Maven wrapper (`mvnw`) in this project.

Examples:

```sh
mise exec -- mvn compile
mise exec -- mvn test -pl <module>
mise exec -- mvn test -pl <module> -am
```

# Java Style

- Target: **Java 17**. Use language features wherever possible: `var`, `record`, `sealed`, pattern matching `instanceof`, text blocks, `switch` expressions, static imports.
- Code is formatted with **Palantir Java Format (Palantir style, 4-space indent, 120 char line)**. The pre-commit hook will reject unformatted code.
- All used classes should be imported. Use fully-qualified names ONLY when there are duplicate simple names.
