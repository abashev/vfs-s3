# ADR-006: S3 URI Contract for the `jdk` Module

**Status:** Proposed
**Date:** 2026-07-09
**Author:** @abashev

## Context

The `jdk` module implements a JDK NIO.2 `FileSystemProvider` for `s3://` URIs. NIO.2 splits
configuration across two inputs:

```java
FileSystems.newFileSystem(URI uri, Map<String, ?> env)   // uri identifies, env configures
```

but it also exposes entry points with **no** env map — `FileSystems.newFileSystem(URI)`,
`Paths.get(URI)` / `Path.of(URI)` — where the URI is the only thing the caller has.

Until now the module's URI grammar was never written down, only implied by the code:
`S3FileSystemProvider.newFileSystem` reads **only** `uri.getHost()` as the bucket, `getPath(URI)`
additionally reads `uri.getPath()` as the object key, and everything else (region, endpoint,
credentials) is read from the `env` map keys `aws.region` / `aws.endpoint` /
`aws.credentialsProvider`. No other URI component is interpreted.

This contrasts sharply with the sibling `modules/commons-vfs`, whose `S3FileNameParser` packs
almost everything into the URL: the region is encoded in the host (`s3.eu-central-1.amazonaws.com`),
the endpoint is the host domain, credentials go in the userinfo
(`accessKey:secretKey[:region]@host`), and path-style vs. virtual-hosted is decided by whether the
bucket appears in the host or the path.

Two forces made the missing contract concrete:

1. A caller who only has a URI (`Paths.get`) currently cannot point the module at a non-default
   endpoint or region at all — that information has nowhere to live without the env map.
2. The remote integration harness (`RemoteEndpoint`) reuses commons-vfs's `BASE_URL`
   environment variable, which is a *commons-vfs* URL, and has to translate it into the jdk env
   map. An incomplete translation (it handled only virtual-hosted AWS hosts, not the path-style
   `s3://s3.<region>.amazonaws.com/<bucket>` that the AWS-1 CI environment actually uses) broke
   the remote CI job. That bug is fixed separately, but it exposed that "what a jdk `s3://` URI
   means" had never been decided on purpose.

## Decision

### Grammar

```
s3://<bucket>[/<key>][?aws.region=<region>&aws.endpoint=<uri>]
```

| URI component | Meaning |
|---|---|
| scheme | `s3` (required; anything else is rejected) |
| host | the **bucket name** (required) |
| path | the object **key** (read by `getPath`) |
| query | non-secret configuration — see below |
| userinfo (`…@`) | **rejected** — credentials must never be in the URI |
| port, fragment | ignored |

### Query parameters

Only two are recognized, and they use the **same names as the `env`-map keys** — one vocabulary
for one concept, whichever channel supplies it:

- `aws.region` — a region id, e.g. `eu-central-1`
- `aws.endpoint` — an endpoint override URI, e.g. `http://localhost:9000`

An unrecognized query parameter is **rejected** (fail-fast on typos like `?reigon=` — a silently
ignored region misconfiguration surfaces later as a cryptic AWS signing error, so we refuse it up
front).

Credentials are deliberately **not** expressible in the URI — not in the query, not in the
userinfo. They are supplied only through the `env`-map key `aws.credentialsProvider`, as an
`AwsCredentialsProvider` object.

### Resolution precedence

For `aws.region` and `aws.endpoint`, in order:

1. **`env` map** value, if present.
2. **URI query** value, if present.
3. **AWS default** — region falls through the AWS SDK's `DefaultAwsRegionProviderChain`
   (`AWS_REGION`, `~/.aws/config`, instance metadata, …) and finally to `us-east-1` (the region
   behind the global `s3.amazonaws.com` endpoint); endpoint defaults to none, letting the SDK
   derive AWS's own virtual-hosted address from the region.

`us-east-1` is kept as the terminal fallback because it is AWS's conventional default and matches
the module's existing behavior. (If a different house default is ever wanted, it changes here and
nowhere else.)

### Examples

```java
// Real AWS, region + credentials from the environment / default chain:
FileSystems.newFileSystem(URI.create("s3://my-bucket"), Map.of());

// Real AWS, region pinned in the URI (self-contained for Paths.get):
Path p = Path.of(URI.create("s3://my-bucket/reports/q3.csv?aws.region=eu-central-1"));

// Local MinIO — endpoint in the URI, credentials (only) in the env map:
FileSystems.newFileSystem(
        URI.create("s3://my-bucket?aws.endpoint=http://localhost:9000&aws.region=us-east-1"),
        Map.of("aws.credentialsProvider", staticProvider));

// env map wins over the URI when both set the same key:
FileSystems.newFileSystem(
        URI.create("s3://my-bucket?aws.region=eu-central-1"),
        Map.of("aws.region", "eu-west-1"));   // → eu-west-1
```

## Consequences

- A URI can now carry the common non-secret configuration (region, endpoint), so `Paths.get(URI)`
  and the no-env `newFileSystem(URI)` entry points can target a non-default endpoint/region without
  an env map. Credentials still require the env map — so a *fully* URI-only configuration is
  intentionally impossible; that is the price of never putting secrets in a URI.
- `S3Path.toUri()` keeps emitting only `s3://<bucket>/<key>` — no query, no userinfo. The
  round-trip is deliberately lossy for configuration: region/endpoint/credentials belong to the
  `FileSystem`, not to a `Path`, and a `Path`'s URI must never leak a secret. A URI produced by
  `toUri()` reopens correctly only against an already-open (hence already-configured) file system,
  which is the standard NIO.2 model.
- Secrets never reach logs, exception messages, thread dumps, or `toUri()` output through the URI,
  and callers keep the full `AwsCredentialsProvider` abstraction (IAM roles, STS, SSO, rotation),
  not just static keys.
- One naming vocabulary (`aws.region` / `aws.endpoint`) spans both the query string and the env
  map; there is nothing to learn twice and no mapping table.
- `RemoteEndpoint` stays a **test-only** adapter: it parses a commons-vfs `BASE_URL`
  (region-in-host, path-style/virtual bucket, userinfo credentials) and produces the jdk env map,
  purely so the remote suite can ride the existing AWS/Yandex CI environments. The jdk provider
  itself never parses commons-vfs URLs; the two URL dialects stay separate by design.
- Implementation work this ADR authorizes: `S3FileSystemConfig` gains URI-query merging under the
  precedence rule above; `S3FileSystem` passes the URI (not just the env map) into it; the provider
  rejects a URI carrying userinfo or an unknown query parameter.

## Alternatives Considered

1. **Identity-only URI; all config via the env map** (URI carries nothing but bucket + key).
   Rejected: the no-env entry points (`Paths.get(URI)`, `newFileSystem(URI)`) then cannot express a
   non-default endpoint or region at all — a real gap for the most common non-secret settings.
2. **Full commons-vfs parity** — credentials in userinfo (`accessKey:secretKey[:region]@host`),
   endpoint/region in the host. Rejected on two grounds: (a) secrets in a URI leak through logs,
   exception text, and `toUri()`, and (b) a URL can carry only static key strings, throwing away
   the `AwsCredentialsProvider` abstraction (IAM/STS/SSO/rotation) the env map preserves. Encoding
   the region in the host also couples bucket identity with region, which the `host = bucket` model
   keeps clean.
3. **Different names for the query vs. the env map** (e.g. `?region=` in the URI but `aws.region`
   in the map). Rejected: two vocabularies for one concept invites confusion and a mapping table;
   a single naming was chosen instead.
4. **`pathStyle` as a third query parameter.** Deferred: path-style addressing is already implied
   whenever an endpoint override is set (`S3FileSystemConfig` forces it for custom endpoints), so a
   separate switch has no use case yet. Add it here if one appears.
```
