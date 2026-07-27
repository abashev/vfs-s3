# ADR-007: Real S3 Backend for the `spring` Module

**Status:** Accepted
**Date:** 2026-07-27
**Author:** @abashev

## Context

The `spring` module shipped as a preview: `S3ResourceLoader` / `S3Resource` implemented the Spring
`Resource` API over a local-tmp mock (`{tmpDir}/bucket/key`), `S3ResourcePatternResolver` rejected
wildcard patterns outright, and the README promised "the real S3 backend is in development". The
README also already committed the module to the canonical secret-free URL scheme defined by
[ADR-006](006-jdk-module-s3-uri-contract.md).

Two structural facts distinguish the Spring surface from the sibling modules:

1. **No env map.** NIO.2 configures a `FileSystem` through `newFileSystem(uri, env)`; Spring's
   `ResourceLoader.getResource(String)` takes only a location string. ADR-006's "env > URI query >
   AWS defaults" precedence needs a Spring-shaped equivalent.
2. **One loader serves many locations over its lifetime** — unlike a NIO.2 `FileSystem`, which is
   opened once per bucket. Client lifecycle can't be "one client per opened thing"; it has to be
   decided explicitly.

## Decision

### Backend

The mock is **removed entirely** and replaced by a thin layer directly over the AWS SDK v2
`S3Client` — `HeadObject`, `GetObject`, `PutObject`, `ListObjectsV2` are the whole op set. The
module does **not** depend on `:modules:jdk` (or `:modules:commons-vfs`): the repo's modules stay
fully independent, publishable artifacts. The small overlap with the jdk module
(`S3ClientConfig.buildS3Client()` mirrors `S3FileSystemConfig.buildS3Client()` — chunked encoding
disabled for backend compatibility, path-style forced on endpoint overrides, `us-east-1` terminal
region fallback) is deliberate, bounded duplication, same as ADR-006's test-only adapter choice.

Building on `vfs-jdk`'s NIO.2 provider instead was rejected: it would introduce the repo's first
inter-module dependency, and the provider's model — one open `FileSystem` per bucket per JVM,
whole objects buffered in memory — is a poor fit for Spring applications (several contexts,
different credentials for one bucket, arbitrary-size uploads through `WritableResource`).

### Configuration contract

Locations follow the ADR-006 grammar: `s3://<bucket>[/<key>][?region=…&endpoint=…]`, userinfo
credentials rejected, unknown query parameters rejected fail-fast. The precedence becomes:

1. **Explicit loader configuration** (`S3ClientConfig` — region, endpoint, and the only channel
   for an `AwsCredentialsProvider`), if set.
2. **URI query** `region` / `endpoint`, if present.
3. **AWS SDK defaults** (region chain, then `us-east-1`; default credentials chain).

`S3ResourceLoader` lazily builds and caches **one `S3Client` per distinct effective
(region, endpoint) pair** and closes exactly those in `close()`. A caller-supplied pre-built
`S3Client` (the DI/testing seam) is used for every location instead and is never closed by the
loader; location query parameters are still validated but have no effect on a fixed client.

Resources store a **query-free, percent-encoded URI**: query parameters are configuration, not
identity, so `equals`/`hashCode`/`getURI()` never depend on which loader or query produced the
resource, and no configuration leaks through `toString()`.

### Resource semantics

`S3Resource extends AbstractResource implements WritableResource` (the `FileSystemResource`
shape). Reads pass the SDK's `ResponseInputStream` straight through — streaming, nothing buffered.
Writes spool to a local temporary file and issue one `PutObject` on `close()` (bounded memory for
arbitrary-size uploads — the commons-vfs module's proven pattern, deliberately not the jdk
module's in-memory channel, which that module itself scopes to "test-sized payloads").

Error mapping: 404/`NoSuchKey` → `false` from `exists()`, `FileNotFoundException` from
`getInputStream()` / `contentLength()` / `lastModified()`; 403 → `false` from `exists()` (S3
answers 403 for present and missing objects alike without permission — existence cannot be
confirmed) but a plain wrapped `IOException` elsewhere, so a permission problem is never
misreported as a missing object; other failures propagate rather than masquerading as absence.

### Pattern resolver and context integration

`S3ResourcePatternResolver extends S3ResourceLoader implements ResourcePatternResolver` — one
object, one client cache, one `close()`; non-`s3://` patterns delegate to an internal
`PathMatchingResourcePatternResolver`. An `s3://` pattern lists the bucket under the longest
wildcard-free key prefix (paginated `ListObjectsV2`, no delimiter — Ant `**` must cross `/`) and
matches full keys with an `AntPathMatcher`; folder markers (keys ending `/`) are excluded, zero
matches yield an empty array. The Ant `?` wildcard is unusable by construction — the first `?` of
a location always starts the query component; `*` / `**` cover the practical cases.

`S3ProtocolResolver` (a ~20-line `ProtocolResolver`) is the application-context seam: registered
via `context.addProtocolResolver(…)`, it makes `s3://` work in `@Value`, `@PropertySource` and any
ambient `getResource(…)` without replacing the context's `ResourceLoader`. Spring core has no
analogous plug-in point for *pattern* resolution, so glob support can never become ambient — a
caller holds the `S3ResourcePatternResolver` itself. No Spring Boot autoconfiguration; Spring
stays `compileOnly`.

## Consequences

- The published `vfs-spring` POM now carries `software.amazon.awssdk:s3` as its one compile
  dependency (`api` — SDK types appear in the public API). The README's "no dependencies" claim
  is updated; Spring itself remains `compileOnly`.
- The preview API breaks: the mock constructors (`S3ResourceLoader(Path)`, client-less
  `S3Resource(String/URI)`) are gone. Acceptable — the module was never released as non-preview.
- The module joins the full test matrix: unit tests run against an in-memory `FakeS3Client`
  (mirroring the jdk module's, trimmed to the four ops); `src/integrationTest` mirrors the
  jdk/commons-vfs shape — shared Suite A–G scenario classes, nine Testcontainers suites (same
  pinned images), a remote `EnvironmentBasedSuite` consuming the canonical `BASE_URL` natively,
  and a `dropRemoteBucket` cleanup task. CI gains `spring-local-suites` / `spring-remote-suites`
  jobs (`BUCKET_TOKEN` suffix `-spring`) with per-cell status-badge gists feeding the README
  tables, exactly like the other two modules.
- Suite F (ACL) stays disabled and Suite A's move/rename steps stay skipped — the Spring
  `Resource` API has no such operations, backend or no backend.

## Alternatives Considered

1. **Back the module with `vfs-jdk` (NIO.2).** Rejected — see Backend above: first inter-module
   coupling, one-`FileSystem`-per-bucket-per-JVM clashes with multi-context/multi-credential
   Spring apps, in-memory buffering caps upload size.
2. **Keep the mock as a selectable backend.** Rejected: a permanent mock branch in every I/O
   method and a public API stuck with "mock mode" forever, for a need the in-memory
   `FakeS3Client` (tests) and LocalStack/MinIO via Testcontainers (integration) already cover.
3. **`S3ResourcePatternResolver extends PathMatchingResourcePatternResolver`** (the preview's
   shape). Rejected: it would wrap a second, separately-closed loader and inherit a large
   filesystem/jar-oriented protected surface; implementing `ResourcePatternResolver` is the
   documented integration contract (`ResourcePatternUtils` checks the interface, not the class).
4. **Per-resource metadata caching** (one HEAD serving `exists` + `contentLength` +
   `lastModified`). Rejected for now: Spring's own file resources re-stat on every call, and a
   cached HEAD turns a cheap value object into a stateful handle with staleness semantics. Can be
   revisited if HEAD volume ever matters.
