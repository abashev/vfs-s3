# Changes from Original Tests

This document describes what was changed when restructuring the integration tests from
the original Java classes into the optimized test suite format.

## Removed

- **uploadBigFile** — downloaded a 64 MB ISO from `archive.ubuntu.com` at test time.
  Unreliable (external dependency), slow, and makes tests non-reproducible offline.
  If multipart upload testing is needed, generate a file locally of configurable size.

- **ACL checkSet / checkSet2** — the original code had commented-out `setAcl()` calls
  and assertions (`assertAllowed` lines were commented). The tests were not verifying
  the intended write behavior. Consolidated into a simplified ACL deny test in Suite F.

## Consolidated

- **Find files with selectors** appeared in both `S3ProviderTest.findFiles` and
  `CopyFilesTest.createDirOk` with nearly identical structure (create same tree, run
  same 4 selector queries). Merged: Suite B step 5 covers the standalone version,
  Suite E step 1 covers it in the copy context.

- **listChildrenRoot** was a complex test that listed children at `/`, `/test-place/`,
  and `/test-place` (with and without trailing slash), then copied an entire directory tree.
  It depended on `big_file.iso` existing from the upload test. Split into simpler checks
  across Suite B (list children) and Suite E (copy and list).

## Reorganized for Parallelism

The original `S3ProviderTest` was a single class with 23 ordered tests sharing mutable
state (`file`, `dir` fields). This created a strict sequential dependency chain where
everything depended on earlier steps.

The optimized structure splits this into 5 independent suites (A through E), each with
its own S3 prefix. This enables parallel execution — all 7 suites can run simultaneously
on the same bucket without interfering with each other.

## Original → Suite Mapping

| Original Class | Original Test | New Suite | Step |
|----------------|---------------|-----------|------|
| S3ProviderTest | createFileOk | A | 1, 2 |
| S3ProviderTest | createDirOk | B | 1 |
| S3ProviderTest | checkLastModified | A | 3 |
| S3ProviderTest | createFileFailed2 | A | 4 |
| S3ProviderTest | createDirFailed2 | B | 2 |
| S3ProviderTest | upload | C | 1 |
| S3ProviderTest | uploadMultiple | C | 2 |
| S3ProviderTest | uploadBigFile | *removed* | — |
| S3ProviderTest | outputStream | C | 3 |
| S3ProviderTest | listChildren | B | 4 |
| S3ProviderTest | listChildrenRoot | E | 3 (simplified) |
| S3ProviderTest | findFiles | B | 5 |
| S3ProviderTest | renameAndMove | A | 5 |
| S3ProviderTest | getType | A | 6 |
| S3ProviderTest | getTypeAfterCopyToSubFolder | C | 4 |
| S3ProviderTest | getContentType | D | 1 |
| S3ProviderTest | getSize | D | 2 |
| S3ProviderTest | getUrls | D | 4 |
| S3ProviderTest | getMD5Hash | D | 5 |
| S3ProviderTest | getLastModified | D | 3 |
| S3ProviderTest | exists | A | 7 |
| S3ProviderTest | download | C | 5 |
| S3ProviderTest | delete | B | 6 |
| AclHandlingTest | checkGet | F | 1 |
| AclHandlingTest | checkSet | *removed* | — |
| AclHandlingTest | checkSet2 | *removed* | — |
| AclHandlingTest | checkDenyAllForFile | F | 2 |
| AclHandlingTest | checkDenyAllForFolder | F | 3 |
| ConcurrentAccessTest | createFileOk | G | 1 |
| ConcurrentAccessTest | checkReadDeadlock | G | 2 |
| ConcurrentAccessTest | testGetChildrenGetParentDeadlock | G | 3 |
| CopyFilesTest | createDirOk | E | 1 |
| CopyFilesTest | copyInsideBucket | E | 2 |
| CopyFilesTest | checkDelete | E | 4 |
