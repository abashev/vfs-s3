# Migrate integration tests to Testcontainers + LocalStack

## Summary

Replace the current integration test setup (external S3 endpoint via environment variables) with
Testcontainers and LocalStack. This makes tests self-contained, reproducible, and runnable without
AWS credentials.

## Motivation

Current integration tests require:
- An external S3-compatible endpoint (configured via `BASE_URL` env var)
- Real AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- Network access to the S3 endpoint during test execution

This makes tests impossible to run in Cowork sandbox (no Docker), hard to run locally without
credentials, and dependent on external infrastructure.

## Plan

### 1. Add Testcontainers + LocalStack dependencies

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>localstack</artifactId>
    <scope>test</scope>
</dependency>
```

### 2. Rewrite BaseIntegrationTest

Replace `EnvironmentConfiguration` + env var loading with a shared LocalStack container:

```java
@Testcontainers
public abstract class BaseIntegrationTest {
    @Container
    static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3"))
        .withServices(LocalStackContainer.Service.S3);

    @BeforeAll
    public final void initBucket() {
        // Configure S3FileSystemOptions to point at localstack.getEndpoint()
        // Use localstack.getAccessKey() / localstack.getSecretKey()
        // No more EnvironmentConfiguration or .envrc parsing
    }
}
```

### 3. Restructure tests into parallel suites

Migrate from the current 4 test classes to 7 independent test suites as described in
`docs/test-cases/`. Each suite uses its own S3 prefix and can run in parallel.

### 4. Update CI workflow

- Remove environment variable injection (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `BASE_URL`)
  from GitHub Actions workflow — tests are now self-contained via Testcontainers
- Ensure Docker is available in CI runners (standard for GitHub-hosted runners)
- Comment out test coverage checks (Jacoco/Codecov) temporarily until tests are stable

### 5. Remove obsolete files

- `src/test/java/com/github/vfss3/support/EnvironmentConfiguration.java` — no longer needed
- `.envrc` references in documentation

## CI Changes

### Disable environment-based test config

The workflow currently injects S3 credentials as environment variables. After migration:
- Remove `env:` block with AWS credentials from the test job
- Tests will start their own LocalStack container automatically

### Comment out test coverage

Temporarily disable coverage reporting until the test migration is complete and stable:
- Comment out Jacoco plugin execution in `pom.xml`
- Comment out Codecov upload step in CI workflow (if present)
- Re-enable after migration is verified

## Test Scenarios

The new test structure follows `docs/test-cases/`:

| Suite | Prefix | Description |
|-------|--------|-------------|
| A: File Lifecycle | `/file-lifecycle/` | Create, rename, move, delete |
| B: Directory Operations | `/dir-ops/` | Folders, listing, selectors |
| C: Upload & Download | `/upload/` | Upload, download, stream I/O |
| D: File Metadata | `/metadata/` | Content type, size, URLs, MD5 |
| E: Copy Operations | `/copy/` | Copy within bucket |
| F: ACL | `/acl/` | Access control (platform-dependent) |
| G: Concurrent Access | `/concurrent/` | Thread safety, deadlock detection |

## Removed from original tests

- `uploadBigFile` — external 64MB download, unreliable
- `checkSet` / `checkSet2` — commented-out ACL assertions

See `docs/test-cases/changes-from-original.md` for full mapping.

## Labels

`enhancement`, `needs-design`

## Blocks

- Blocked by #185 (TestNG → JUnit 5 migration) — must complete first
