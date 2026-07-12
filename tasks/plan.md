# Implementation Plan: Real S3-backed `jdk` module

## Overview

Per `SPEC.md`: `modules/jdk`'s `S3FileSystemProvider`/`S3FileSystem`/`S3Path` are an explicit
local-tmp mock. This plan replaces the mock with a real `S3Client`-backed implementation, moves
the 6 already-existing mock-backed scenario test classes (`FileLifecycleTest`,
`DirectoryOperationsTest`, `UploadDownloadTest`, `FileMetadataTest`, `CopyOperationsTest`,
`ConcurrentAccessTest` — plus the already-`@Disabled` `AclTest`) into
`src/integrationTest/java`, wires them to run against all 9 local S3-compatible emulators
`modules/commons-vfs` already runs, and adds the matching CI matrix job.

## Architecture Decisions

- Config is deliberately minimal: AWS SDK v2 default provider chain is primary; `S3FileSystemConfig`
  only overrides `region`/`endpoint`/`credentialsProvider` from the `env` map. Issue #220's fuller
  proposal (`s3.createBucket`, `s3.serverSideEncryption`) is out of scope — see `SPEC.md`.
- Every new class mirrors an existing `modules/commons-vfs` counterpart rather than inventing new
  patterns: `S3IntegrationContext` → `JdkIntegrationContext`, `local/*Suite.java` → same 9 files,
  `S3FileObject.doAttach()`'s folder-marker convention → `createDirectory`/`newDirectoryStream`.
- Provider correctness is proven against one backend (MinIO) before mechanically rolling out to
  the remaining 8 — isolates "is our S3 semantics right" from "does this specific emulator behave
  like real S3."
- `S3FileSystemConfig` lives directly in `com.github.vfss3.jdk`, no separate `config` sub-package.

## Task List

### Phase 1: Foundation

- [ ] **Task 1: Gradle dependencies for the jdk module**
  - Description: Add `aws-sdk-s3`, `junit-platform-suite`, and the three `testcontainers-*`
    dependencies to `modules/jdk/build.gradle`, plus the `integrationTest` task's
    `includeEngines 'junit-platform-suite'` filter — mirroring
    `modules/commons-vfs/build.gradle` (dependency block and the `tasks.named('integrationTest', Test)` block) exactly.
  - Acceptance: `modules/jdk/build.gradle` resolves the same dependency set `commons-vfs` uses.
  - Verify: `mise exec -- ./gradlew :modules:jdk:compileJava`
  - Dependencies: None
  - Files: `modules/jdk/build.gradle`
  - Estimated scope: XS (1 file)

- [ ] **Task 2: `S3FileSystemConfig` — minimal env-override config**
  - Description: Add `S3FileSystemConfig` record (`region`, `endpoint`, `credentialsProvider`)
    with `fromEnv(Map<String,?>)` and `buildS3Client()`, per the Code Style section of `SPEC.md`.
    SDK default provider chain is the fallback when `env` supplies nothing.
  - Acceptance: `fromEnv()` reads `region`/`endpoint`/`credentialsProvider` when
    present; falls back to `DefaultCredentialsProvider` otherwise; `buildS3Client()` applies
    region/endpoint overrides only when set.
  - Verify: `mise exec -- ./gradlew :modules:jdk:test` (new `S3FileSystemConfigTest`)
  - Dependencies: Task 1
  - Files: `S3FileSystemConfig.java`, `S3FileSystemConfigTest.java`
  - Estimated scope: S (2 files)

### Checkpoint: Foundation
- [ ] `mise exec -- ./gradlew :modules:jdk:test` passes
- [ ] `mise exec -- ./gradlew :modules:jdk:compileJava` passes with new dependencies

### Phase 2: Core provider, proven against one backend (MinIO)

- [ ] **Task 3: Real file-level CRUD + MinIO harness (Suite A)**
  - Description: Wire `S3FileSystem` to hold a real `S3Client` (via `S3FileSystemConfig`)
    instead of a temp dir. Implement `S3FileSystemProvider.newByteChannel`, `delete`,
    `checkAccess`, `readAttributes(BasicFileAttributes)`, `isSameFile`, `move` against S3 objects
    (single-key operations only — no directory/prefix semantics yet). Add
    `JdkIntegrationContext` (mirrors `S3IntegrationContext`) and `local/MinioSuite.java` (mirrors
    `modules/commons-vfs/.../local/MinioSuite.java`). Move `FileLifecycleTest.java` from
    `src/test/java` to `src/integrationTest/java`, adapting it to read the `FileSystem` from
    `JdkIntegrationContext` instead of constructing the mock.
  - Acceptance: `FileLifecycleTest` (Suite A: create, rename/move, lastModifiedTime, self-move
    error, type checks) passes against a real MinIO container.
  - Verify: `mise exec -- ./gradlew :modules:jdk:integrationTest --tests "com.github.vfss3.jdk.local.MinioSuite"`
  - Dependencies: Task 2
  - Files: `S3FileSystem.java`, `S3FileSystemProvider.java`, `JdkIntegrationContext.java`,
    `local/MinioSuite.java`, `FileLifecycleTest.java` (moved)
  - Estimated scope: L (5 files) — foundational, not splittable without leaving something non-functional

- [ ] **Task 4: Directory operations (Suite B)**
  - Description: Implement `createDirectory` using the S3 folder-marker key convention (mirror
    `S3FileObject.doAttach()`'s marker-key probing from `modules/commons-vfs`), implement
    `newDirectoryStream` via real `ListObjectsV2` with prefix filtering, and the recursive-delete
    semantics `docs/test-cases/b-directory-operations.md` step 6 needs. Move
    `DirectoryOperationsTest.java` into `integrationTest`.
  - Acceptance: `DirectoryOperationsTest` (Suite B) passes against MinIO.
  - Verify: same `MinioSuite` command as Task 3
  - Dependencies: Task 3
  - Files: `S3FileSystemProvider.java`, `DirectoryOperationsTest.java` (moved)
  - Estimated scope: S/M (2 files)

- [ ] **Task 5: Upload/download streaming (Suite C)**
  - Description: Extend `newByteChannel`/stream handling for full read/write/overwrite cycles
    and nested-path auto-creation (reusing Task 4's directory-marker logic for intermediate
    "folders"). Move `UploadDownloadTest.java`.
  - Acceptance: `UploadDownloadTest` (Suite C: upload, overwrite, streaming I/O, nested path,
    download) passes against MinIO.
  - Verify: same `MinioSuite` command
  - Dependencies: Task 3, Task 4
  - Files: `S3FileSystemProvider.java`, `UploadDownloadTest.java` (moved)
  - Estimated scope: S (2 files)

- [ ] **Task 6: File metadata (Suite D)**
  - Description: Confirm/fix `readAttributes(BasicFileAttributes)` returns correct `size()` and
    `lastModifiedTime()` from a real S3 `HeadObject` response. Move `FileMetadataTest.java`,
    keeping its existing NIO.2-only scope (content-type/signed-URL/MD5 stay out of scope — no
    JDK analog, matches `SPEC.md` Boundaries).
  - Acceptance: `FileMetadataTest` (Suite D, steps 2-3 only) passes against MinIO.
  - Verify: same `MinioSuite` command
  - Dependencies: Task 3
  - Files: `S3FileSystemProvider.java` (if needed), `FileMetadataTest.java` (moved)
  - Estimated scope: S (1-2 files)

- [ ] **Task 7: Copy operations + `S3Path.relativize()` (Suite E)**
  - Description: Implement `S3Path.relativize()` (currently throws
    `UnsupportedOperationException`; the existing `CopyOperationsTest` works around this via
    string manipulation — remove that workaround once real `relativize()` exists). Implement
    `S3FileSystemProvider.copy()` for real S3 objects, including recursive directory-tree copy
    (walk + per-key `CopyObject`). Move `CopyOperationsTest.java`.
  - Acceptance: `CopyOperationsTest` (Suite E: tree copy, recursive counts, bulk delete) passes
    against MinIO using the real `relativize()` (no string-based workaround remaining).
  - Verify: same `MinioSuite` command
  - Dependencies: Task 3, Task 4
  - Files: `S3Path.java`, `S3FileSystemProvider.java`, `CopyOperationsTest.java` (moved)
  - Estimated scope: M (3 files)

- [ ] **Task 8: `getPath(URI)` and `getFileStore()`**
  - Description: Implement `S3FileSystemProvider.getPath(URI)` (round-trips with
    `S3Path.toUri()`) and `getFileStore()` on both the provider and `S3FileSystem` (a minimal
    `FileStore` — S3 has no real quota API, so a simple stub reporting the bucket as the single
    store is sufficient). These aren't covered by any lettered suite — add unit coverage
    directly.
  - Acceptance: `S3FileSystemProvider.getPath(URI)` and both `getFileStore()` overrides no longer
    throw `UnsupportedOperationException`; round-trip and basic `FileStore` properties covered by
    unit tests (no network needed).
  - Verify: `mise exec -- ./gradlew :modules:jdk:test`
  - Dependencies: Task 3
  - Files: `S3FileSystemProvider.java`, `S3FileSystem.java`, `S3FileSystemProviderTest.java`
  - Estimated scope: M (3 files)

- [ ] **Task 9: Concurrent access (Suite G) + AclTest move**
  - Description: Move `ConcurrentAccessTest.java` and `AclTest.java` (stays `@Disabled`) into
    `integrationTest`. Run the concurrency suite against MinIO; fix any thread-safety issues
    surfaced (AWS SDK v2 `S3Client` is thread-safe by design, but shared mutable state in
    `S3FileSystem`/`S3FileSystemProvider` needs checking under load).
  - Acceptance: `ConcurrentAccessTest` (Suite G: concurrent create/delete, concurrent read,
    deadlock detection via `ThreadMXBean`) passes against MinIO with zero deadlocks/wrong
    results; `AclTest` remains disabled.
  - Verify: same `MinioSuite` command
  - Dependencies: Task 3, Task 4, Task 5, Task 7
  - Files: `ConcurrentAccessTest.java` (moved), `AclTest.java` (moved), `S3FileSystemProvider.java`
    (only if a concurrency fix is needed)
  - Estimated scope: S/M (2-3 files)

### Checkpoint: MinIO-complete
- [ ] All 6 scenario suites (A, B, C, D, E, G) pass against `MinioSuite`
- [ ] `AclTest` remains `@Disabled`
- [ ] `mise exec -- ./gradlew :modules:jdk:check -x integrationTest` passes (unit tests + format)
- [ ] Review with human before mechanically rolling out to the remaining 8 backends — Garage in
  particular is a different shape (bootstrap script) worth a sanity check before committing to it

### Phase 3: Roll out to the remaining 8 backends (mechanical; independent, safe to parallelize)

- [ ] **Task 10: Simple GenericContainer backends — LocalStack, MiniStack, Floci, S3Mock**
  - Description: Add `local/{LocalStackSuite,MiniStackSuite,FlociSuite,S3MockSuite}.java`, each
    mirroring its `modules/commons-vfs` counterpart 1:1 (image, port, credentials). Run the full
    scenario set (Tasks 3-9's tests) against each; fix any backend-specific quirks surfaced.
  - Acceptance: all 6 scenario suites pass against all 4 of these backends.
  - Verify: `mise exec -- ./gradlew :modules:jdk:integrationTest --tests "com.github.vfss3.jdk.local.<SuiteName>"` for each of the 4
  - Dependencies: Checkpoint: MinIO-complete
  - Files: 4 new suite files under `local/`
  - Estimated scope: M (4 files)

- [ ] **Task 11: Env-configured backends — CloudServer, SeaweedFS, RustFS**
  - Description: Add `local/{CloudServerSuite,SeaweedFsSuite,RustFsSuite}.java`, mirroring their
    commons-vfs counterparts (container env vars for credentials/mode). Run the full scenario set
    against each; fix any quirks.
  - Acceptance: all 6 scenario suites pass against all 3 of these backends.
  - Verify: same per-suite `--tests` command
  - Dependencies: Checkpoint: MinIO-complete (parallel with Task 10)
  - Files: 3 new suite files under `local/`
  - Estimated scope: M (3 files)

- [ ] **Task 12: Garage (bootstrap-required backend)**
  - Description: Add `local/GarageSuite.java`, mirroring
    `modules/commons-vfs/.../local/GarageSuite.java`'s post-start bootstrap (layout assignment +
    S3 key creation via `execInContainer`) exactly. Run the full scenario set against it.
  - Acceptance: all 6 scenario suites pass against Garage.
  - Verify: `mise exec -- ./gradlew :modules:jdk:integrationTest --tests "com.github.vfss3.jdk.local.GarageSuite"`
  - Dependencies: Checkpoint: MinIO-complete (parallel with Task 10/11)
  - Files: `local/GarageSuite.java`
  - Estimated scope: S (1 file)

### Checkpoint: All 9 backends green
- [ ] All 6 scenario suites pass against all 9 local backends
- [ ] `mise exec -- ./gradlew :modules:jdk:check` (unit + every integration suite) passes locally

### Phase 4: CI wiring

- [ ] **Task 13: CI matrix job for `:modules:jdk:integrationTest`**
  - Description: In `.github/workflows/pr-build.yml` and `.github/workflows/main-build.yml`, add
    a sibling job to the existing `matrix.suite` job — same 9 suite names, targeting
    `:modules:jdk:integrationTest` instead of `:modules:commons-vfs:integrationTest`.
  - Acceptance: both workflow files have a jdk-module matrix job structurally identical to the
    existing commons-vfs one, just pointed at the new module/package.
  - Verify: run the new job locally with `act` before pushing, e.g.
    `act pull_request -W .github/workflows/pr-build.yml -j <job-id> --matrix suite:MinioSuite`
    (and at least one other suite) to confirm the job triggers and passes; final confirmation
    still comes from this PR's own CI run once pushed
  - Dependencies: Checkpoint: All 9 backends green
  - Files: `.github/workflows/pr-build.yml`, `.github/workflows/main-build.yml`
  - Estimated scope: S (2 files)

### Checkpoint: Complete
- [ ] All Success Criteria in `SPEC.md` met
- [ ] `mise exec -- ./gradlew :modules:jdk:check` passes
- [ ] CI matrix wired in both workflow files
- [ ] Ready for review (fresh session, `vfs-reviewer` persona, per ADR-005)

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| S3 "directory" emulation (marker keys/prefixes) behaves subtly differently across 9 emulators | High | Prove the provider against MinIO first (Checkpoint: MinIO-complete) before rolling out; per-backend failures in Phase 3 are isolated fix-forward tasks, not a reason to block the whole rollout |
| Recursive copy / `relativize()` edge cases (root paths, trailing slashes) | Medium | Suite E's existing assertions and `docs/test-cases/e-copy-operations.md` already give concrete, testable acceptance criteria |
| Garage's `execInContainer` bootstrap is slower/flakier than the other 8 backends | Medium | Isolated as its own task (12), not bundled with the simple backends; mirror the already-working `commons-vfs` `GarageSuite` exactly rather than reimplementing |
| Concurrent-access testing surfaces real thread-safety bugs in shared `S3FileSystem` state | Medium | Suite G's deadlock-detection test (`ThreadMXBean`) is designed to catch exactly this; treat any failure as blocking, not something to skip |
| CI matrix job adds 9 more containers per PR run (cost/time) | Low | Same trade-off `modules/commons-vfs` already accepted for its own 9-backend matrix; no new decision needed |

## Open Questions

None — all scope decisions were resolved in `SPEC.md` before this plan was drafted.
