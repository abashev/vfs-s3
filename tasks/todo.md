# Todo: Real S3-backed `jdk` module

See `tasks/plan.md` for full task descriptions, acceptance criteria, and verification commands.

## Phase 1: Foundation
- [x] Task 1: Gradle dependencies for the jdk module
- [x] Task 2: `S3FileSystemConfig` — minimal env-override config
- [x] Checkpoint: Foundation

## Phase 2: Core provider, proven against MinIO
- [x] Task 3: Real file-level CRUD + MinIO harness (Suite A)
- [x] Task 4: Directory operations (Suite B)
- [x] Task 5: Upload/download streaming (Suite C)
- [x] Task 6: File metadata (Suite D)
- [x] Task 7: Copy operations + `S3Path.relativize()` (Suite E)
- [x] Task 8: `getPath(URI)` and `getFileStore()`
- [x] Task 9: Concurrent access (Suite G) + AclTest move
- [x] Checkpoint: MinIO-complete (human review before Phase 3)

## Phase 3: Roll out to the remaining 8 backends
- [ ] Task 10: Simple GenericContainer backends — LocalStack, MiniStack, Floci, S3Mock
- [ ] Task 11: Env-configured backends — CloudServer, SeaweedFS, RustFS
- [ ] Task 12: Garage (bootstrap-required backend)
- [ ] Checkpoint: All 9 backends green

## Phase 4: CI wiring
- [ ] Task 13: CI matrix job for `:modules:jdk:integrationTest`
- [ ] Checkpoint: Complete
