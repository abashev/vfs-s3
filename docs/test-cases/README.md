# Integration Test Scenarios

Test scenarios for vfs-s3 described at the level of filesystem operations. Each file is an
independent test suite that can run in parallel with other suites.

## Design Principles

- Each suite works in its own isolated prefix (folder) inside the bucket
- Suites have no cross-dependencies and can run in parallel
- Within a suite, steps are numbered and ordered — each step may depend on previous ones
- Setup/teardown is per-suite: create prefix at start, delete prefix at end

## Parallel Execution Map

```
Suite A: File Lifecycle        ─┐
Suite B: Directory Operations  ─┤
Suite C: Upload & Download     ─┤  All run in parallel
Suite D: File Metadata         ─┤  (independent prefixes)
Suite E: Copy Operations       ─┤
Suite F: ACL                   ─┤
Suite G: Concurrent Access     ─┘
```

## Helper Files

- `backup.zip` — a 996,166-byte local binary file (src/test/resources/backup.zip)

## Suites

| File | Prefix | Description |
|------|--------|-------------|
| [a-file-lifecycle.md](a-file-lifecycle.md) | `/file-lifecycle/` | Create, rename, move, delete files |
| [b-directory-operations.md](b-directory-operations.md) | `/dir-ops/` | Create folders, list children, find files |
| [c-upload-download.md](c-upload-download.md) | `/upload/` | Upload, download, overwrite, stream I/O |
| [d-file-metadata.md](d-file-metadata.md) | `/metadata/` | Content type, size, timestamps, URLs, MD5 |
| [e-copy-operations.md](e-copy-operations.md) | `/copy/` | Copy directories within bucket |
| [f-acl.md](f-acl.md) | `/acl/` | Access control lists (platform-dependent) |
| [g-concurrent-access.md](g-concurrent-access.md) | `/concurrent/` | Thread safety and deadlock detection |

## Changes from Original Tests

See [changes-from-original.md](changes-from-original.md) for what was removed, consolidated,
and reorganized compared to the original Java test classes.
