# Spec: Real S3-backed `jdk` module (`java.nio.file.spi.FileSystemProvider`)

## Objective

`modules/jdk` currently ships an `S3FileSystemProvider` / `S3FileSystem` / `S3Path` that is an
explicit **local-tmp mock** (its own javadoc: *"Initially backed by a local-tmp mock; real S3
backend will be added in a follow-up"*). This spec replaces that mock with a real AWS-S3-backed
implementation, so `FileSystems.newFileSystem(URI.create("s3://bucket"), env)` performs real S3
operations (or against S3-compatible endpoints — LocalStack, MinIO) instead of writing to a temp
directory.

**Why now:** this is the last unfinished piece of the 17.0 roadmap's multi-module split —
`modules/commons-vfs` already has a production S3 backend; `modules/jdk` is still a scaffold.

**Scope decision:** this pass is deliberately minimal on configuration. The primary path is the
AWS SDK v2 default credentials/region provider chain (`S3Client.builder().build()` — env vars,
`~/.aws/credentials`, IAM role, etc.) — no explicit config required for real AWS. The `env` map
from `newFileSystem(uri, env)` is only for a handful of overrides needed for local/test wiring
(endpoint override for LocalStack-family emulators, static credentials). This does **not** fully
implement [issue #220](https://github.com/abashev/vfs-s3/issues/220)'s richer proposal
(`s3.createBucket`, `s3.serverSideEncryption`, etc.) — that stays a future increment on top of
this minimal layer, not part of this spec.

**Who:** library consumers who prefer the standard `java.nio.file` / `Path` API over Apache
Commons VFS's `FileObject` API for accessing S3.

**What success looks like (informal):** the scenarios already described in
`docs/test-cases/a-file-lifecycle.md` through `g-concurrent-access.md` — and already scaffolded
as 8 test classes in `modules/jdk/src/test/java` against the mock — pass against all 9 local
S3-compatible backends `modules/commons-vfs` already runs (MinIO, LocalStack, MiniStack, Floci,
Zenko CloudServer, SeaweedFS, Adobe S3Mock, Garage, RustFS), reached primarily through the AWS
SDK default provider chain with a small `env`-map override for local endpoints/credentials, with
CI wired to run all 9 for `modules:jdk` the same way it already does for `modules:commons-vfs`.

## Tech Stack

- Java 17, Gradle multi-module (`modules/jdk`), Palantir Java Format — unchanged, per `AGENTS.md`.
- AWS SDK v2 `S3Client` (`libs.aws.sdk.s3`, version `aws-sdk-v2` in `gradle/libs.versions.toml`) —
  new dependency for `modules/jdk`; already used by `modules/commons-vfs`.
- JUnit 5 + `junit-platform-suite` (`libs.junit.platform.suite`) for integration-test suite
  discovery — same mechanism `modules/commons-vfs` uses.
- Testcontainers (`libs.testcontainers.core`, `libs.testcontainers.localstack`,
  `libs.testcontainers.junit.jupiter`) for the 9 local S3-compatible emulator containers
  `modules/commons-vfs` already runs (MinIO, LocalStack, MiniStack, Floci, CloudServer,
  SeaweedFS, S3Mock, Garage, RustFS) — same dependencies, same containers, new module.
- The `vfs-s3.java-conventions` buildSrc plugin already wires the `integrationTest` Gradle source
  set (`buildSrc/src/main/groovy/vfs-s3.java-conventions.gradle`) — no new build-plumbing needed
  beyond adding dependencies to `modules/jdk/build.gradle`, mirroring
  `modules/commons-vfs/build.gradle`.

## Commands

Local development (from repo root, per `AGENTS.md`):

```sh
mise exec -- ./gradlew :modules:jdk:compileJava
mise exec -- ./gradlew :modules:jdk:test
mise exec -- ./gradlew :modules:jdk:integrationTest --tests "com.github.vfss3.jdk.local.<SuiteName>"
mise exec -- ./gradlew :modules:jdk:check
```

Where `<SuiteName>` is one of: `MinioSuite`, `LocalStackSuite`, `MiniStackSuite`, `FlociSuite`,
`CloudServerSuite`, `SeaweedFsSuite`, `S3MockSuite`, `GarageSuite`, `RustFsSuite` — the same 9
`modules/commons-vfs` already runs.

CI: same commands without `mise exec --`. CI also needs a new matrix job for
`:modules:jdk:integrationTest` in `.github/workflows/{pr-build,main-build}.yml`, mirroring the
existing `matrix.suite` job that currently only targets `:modules:commons-vfs:integrationTest`.

## Project Structure

```
modules/jdk/
  src/main/java/com/github/vfss3/jdk/
    S3FileSystemProvider.java   → real S3Client-backed implementation (replaces local-tmp mock)
    S3FileSystem.java           → holds S3Client + bucket + parsed S3FileSystemConfig
    S3Path.java                 → gains relativize(); otherwise unchanged
    S3FileSystemConfig.java     → NEW — minimal record: region/endpoint/credentialsProvider
                                  overrides on top of the AWS SDK default chain (fromEnv() / buildS3Client())
  src/test/java/com/github/vfss3/jdk/
    S3FileSystemProviderTest.java   → existing, stays (fast, no network)
    S3FileSystemConfigTest.java     → NEW — env-map overrides + fallback to SDK default chain
  src/integrationTest/java/com/github/vfss3/jdk/
    local/
      → NEW — one suite class per backend, each mirroring its
        modules/commons-vfs/.../local/*Suite.java counterpart 1:1 (container image, port,
        credentials, @Suite/@BeforeSuite/@AfterSuite wiring): MinioSuite, LocalStackSuite,
        MiniStackSuite, FlociSuite, CloudServerSuite, SeaweedFsSuite, S3MockSuite, RustFsSuite —
        all near-identical GenericContainer + static-credentials boilerplate — and GarageSuite,
        which additionally needs the post-start bootstrap (layout assignment + S3 key creation
        via execInContainer) its commons-vfs counterpart already implements.
    JdkIntegrationContext.java → NEW — shared FileSystem handle for suite test classes, mirrors S3IntegrationContext
    FileLifecycleTest.java        → MOVED from src/test/java (Suite A)
    DirectoryOperationsTest.java  → MOVED from src/test/java (Suite B)
    UploadDownloadTest.java       → MOVED from src/test/java (Suite C)
    FileMetadataTest.java         → MOVED from src/test/java (Suite D)
    CopyOperationsTest.java       → MOVED from src/test/java (Suite E)
    ConcurrentAccessTest.java     → MOVED from src/test/java (Suite G)
    AclTest.java                  → MOVED, stays @Disabled (Suite F — no JDK NIO.2 ACL analog)
  build.gradle → add aws-sdk-s3, junit-platform-suite, testcontainers-* deps + integrationTest
                 task filter, mirroring modules/commons-vfs/build.gradle

.github/workflows/{pr-build,main-build}.yml
  → add a matrix job for :modules:jdk:integrationTest (same 9 `matrix.suite` values), mirroring
    the existing :modules:commons-vfs:integrationTest job
```

## Code Style

One example showing the target style for the new config record (Java 17 idioms: record, `var`,
explicit imports, Palantir format). Deliberately minimal — the SDK default chain does the real
work; the record only carries the few overrides local/test wiring needs:

```java
public record S3FileSystemConfig(String region, URI endpoint, AwsCredentialsProvider credentialsProvider) {

    public static S3FileSystemConfig fromEnv(Map<String, ?> env) {
        var region = (String) env.get("aws.region");
        var endpoint = Optional.ofNullable((String) env.get("aws.endpoint"))
                .map(URI::create)
                .orElse(null);
        var credentialsProvider = (AwsCredentialsProvider)
                env.getOrDefault("aws.credentialsProvider", DefaultCredentialsProvider.create());

        return new S3FileSystemConfig(region, endpoint, credentialsProvider);
    }

    public S3Client buildS3Client() {
        var builder = S3Client.builder().credentialsProvider(credentialsProvider);
        if (region != null) builder.region(Region.of(region));
        if (endpoint != null) builder.endpointOverride(endpoint);
        return builder.build();
    }
}
```

Conventions: records for immutable config/DTOs, `var` for obvious local types, explicit imports,
4-space indent / 120-char lines (Palantir Java Format) — matches `AGENTS.md`.

## Testing Strategy

- **Unit tests** (`src/test/java`, `mise exec -- ./gradlew test`) — fast, no network:
  `S3FileSystemProviderTest` (scheme/URI parsing, existing) and a new `S3FileSystemConfigTest`
  (env-map overrides for region/endpoint/credentials, and fallback to the AWS SDK default chain
  when no `env` keys are given).
- **Integration tests** (`src/integrationTest/java`, `mise exec -- ./gradlew integrationTest`) — real
  S3-compatible backends via testcontainers, mirroring all 9 of `modules/commons-vfs`'s local
  suites:
  - Each of the 9 suites (`MinioSuite`, `LocalStackSuite`, `MiniStackSuite`, `FlociSuite`,
    `CloudServerSuite`, `SeaweedFsSuite`, `S3MockSuite`, `GarageSuite`, `RustFsSuite`) starts a
    container in `@BeforeSuite` (Garage additionally bootstraps a layout + S3 key via
    `execInContainer`, same as its commons-vfs counterpart), builds an `S3FileSystemConfig`,
    obtains a real `FileSystem` via `FileSystems.newFileSystem(uri, env)`, and shares it with all
    suite test classes via `JdkIntegrationContext` (mirrors `S3IntegrationContext`).
  - The 6 already-existing scenario test classes (Suites A, B, C, E, G — file lifecycle,
    directory operations, upload/download, copy, concurrent access) move from `src/test/java` to
    `src/integrationTest/java` and read the shared `FileSystem` from context instead of
    constructing the mock directly. Their assertions, already aligned with
    `docs/test-cases/a-g`, do not change.
  - `FileMetadataTest` (Suite D) also moves, keeping its existing NIO.2-only scope: size and
    `lastModifiedTime` via `BasicFileAttributes`. Content-type, signed URLs, and MD5 stay out of
    scope — no JDK NIO.2 analog (this is already how the existing test is scoped).
  - `AclTest` (Suite F) moves but stays `@Disabled` — JDK NIO.2 exposes no
    `AclFileAttributeView`; no behavior change needed.
- Run one backend at a time via `--tests`, same convention as `modules/commons-vfs`; all 9 local
  backends must pass before merging, and CI must run all 9 as a matrix job (see Project
  Structure) rather than relying on local-only verification.

## Boundaries

- **Always do:** run `mise exec -- ./gradlew :modules:jdk:test` and at least one integration suite before
  committing; keep `S3FileSystemConfig` immutable (a record); rely on the AWS SDK default
  provider chain as the primary configuration path, using `env` keys (`aws.region`,
  `aws.endpoint`, `aws.credentialsProvider`) only as explicit overrides.
- **Ask first:** adding an AWS SDK dependency version different from `aws-sdk-v2` in
  `gradle/libs.versions.toml`; expanding `S3FileSystemConfig` toward issue #220's fuller proposal
  (`s3.createBucket`, `s3.serverSideEncryption`, `aws.accessKeyId`/`aws.secretAccessKey` as raw
  keys) — that is explicitly out of scope for this pass; the exact disposition of the
  pre-existing mock-backed unit tests once moved (whether any stay behind as smoke tests).
- **Never do:** implement `AclFileAttributeView` or otherwise expose ACLs through this module
  (JDK NIO.2 has no such SPI hook — matches Suite F's existing exclusion); implement
  content-type/MD5/signed-URL metadata (no JDK analog — that's what `commons-vfs` is for); commit
  AWS credentials or bucket names with sensitive data; loosen S3 ACL/public-access behavior
  without explicit maintainer approval (per `CONTRIBUTING.md` Security section).

## Success Criteria

- `FileSystems.newFileSystem(URI.create("s3://bucket"), env)` returns a `FileSystem` backed by a
  real `S3Client`, built primarily from the AWS SDK v2 default provider chain; explicit `env`
  keys (`aws.region`, `aws.endpoint`, `aws.credentialsProvider`) override individual fields when
  present. Issue #220's fuller config surface is intentionally not implemented in this pass.
- `S3FileSystemProvider.getPath(URI)`, `getFileStore()` (on both the provider and the file
  system), and `S3Path.relativize()` are implemented — none of them throw
  `UnsupportedOperationException` anymore.
- All 6 scenario integration-test classes (Suites A, B, C, D, E, G) pass against all 9 local
  suites, matching `modules/commons-vfs`'s backend coverage exactly.
- `AclTest` (Suite F) remains `@Disabled` with its existing rationale; no ACL code is added.
- `./gradlew :modules:jdk:check` (compile + unit + integration + format) passes in CI for all 9
  backends, via a new CI matrix job for `:modules:jdk:integrationTest`.
- GitHub issue #220 stays open after this PR — this spec implements a minimal subset consistent
  with its direction, not the full proposal; the PR description should reference #220 without
  claiming to close it.

## Decisions

Resolved while drafting this spec (kept here for traceability, not as open questions):

1. The 6 mock-backed scenario test classes move to `src/integrationTest/java` rather than
   staying as unit tests against the mock.
2. All 9 `modules/commons-vfs` local backends get mirrored (MinIO, LocalStack, MiniStack, Floci,
   CloudServer, SeaweedFS, S3Mock, Garage, RustFS), including a matching CI matrix job for
   `:modules:jdk:integrationTest` — not a smaller subset.
3. `S3FileSystemConfig` lives directly in `com.github.vfss3.jdk` — no separate `config`
   sub-package.
4. This pass intentionally does not close issue #220; see the Scope decision in Objective.
