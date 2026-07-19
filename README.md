Amazon S3 for Java — NIO.2, Commons VFS, and Spring
===================================================

_S3-backed file access for the JVM through the abstractions you already use._
Formerly **vfs-s3** — the Amazon S3 driver for Apache Commons VFS.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Build (17.0)](https://github.com/abashev/vfs-s3/actions/workflows/main-build.yml/badge.svg?branch=17.0)](https://github.com/abashev/vfs-s3/actions/workflows/main-build.yml)
[![vfs-jdk](https://img.shields.io/maven-central/v/com.github.abashev/vfs-jdk?label=vfs-jdk)](https://central.sonatype.com/artifact/com.github.abashev/vfs-jdk)
[![vfs-commons](https://img.shields.io/maven-central/v/com.github.abashev/vfs-commons?label=vfs-commons)](https://central.sonatype.com/artifact/com.github.abashev/vfs-commons)
[![vfs-spring](https://img.shields.io/maven-central/v/com.github.abashev/vfs-spring?label=vfs-spring)](https://central.sonatype.com/artifact/com.github.abashev/vfs-spring)

Read and write Amazon S3 — and any S3-compatible storage — from Java without dropping down to the
AWS SDK's object API.

> **Scope.** This project is not meant to be a complete implementation of the S3 API. It exists to
> simplify the handful of use-cases that come up most often: reading and writing config files,
> uploading user-generated content to storage, and serving access to it. For anything beyond that,
> reach for the AWS SDK directly.

- **Integrate S3 into Java libraries and apps** to access S3 data through standard abstractions:
  JDK **NIO.2** (`java.nio.file.Path` / `FileSystem`), Apache **Commons VFS** (`FileObject`), and
  **Spring** (`Resource` / `ResourceLoader`).
- **Plain, secret-free `s3://` URLs** — credentials come from the AWS SDK default chain, never the
  URL (see [URL scheme](#url-scheme)).
- **Built on the AWS SDK for Java v2**, Java 17.
- **Works with real AWS and many S3-compatible backends** — Yandex Object Storage, DigitalOcean
  Spaces, MinIO, Garage, LocalStack, and more.

### History

`vfs-s3` began as an Amazon S3 provider for **Apache Commons VFS** (the `3.x` / `4.x` line, Java 8).
The **`17.x`** line reworks it into a multi-module project on **Java 17** and the **AWS SDK v2**,
adding a native JDK **NIO.2** `FileSystemProvider` and **Spring** integration, while keeping the
Commons VFS provider for backward compatibility.

### Tested against

Every push builds and runs the `jdk` and `commons-vfs` integration suites against nine
S3-compatible emulators (via Testcontainers) and against real AWS and Yandex.

**Local emulators (Testcontainers):**

| Backend | `jdk` | `commons-vfs` | `spring` |
| --- | :---: | :---: | :---: |
| [LocalStack](https://www.localstack.cloud/) | ✅ | ✅ | — |
| [MinIO](https://min.io/) | ✅ | ✅ | — |
| [Garage](https://garagehq.deuxfleurs.fr/) | ✅ | ✅ | — |
| [SeaweedFS](https://github.com/seaweedfs/seaweedfs) | ✅ | ✅ | — |
| [Adobe S3Mock](https://github.com/adobe/S3Mock) | ✅ | ✅ | — |
| [Zenko CloudServer](https://github.com/scality/cloudserver) | ✅ | ✅ | — |
| [RustFS](https://hub.docker.com/r/rustfs/rustfs) | ✅ | ✅ | — |
| [MiniStack](https://hub.docker.com/r/ministackorg/ministack) | ✅ | ✅ | — |
| [Floci](https://hub.docker.com/r/floci/floci) | ✅ | ✅ | — |

**Cloud providers (remote suites):**

| Provider | `jdk` | `commons-vfs` | `spring` |
| --- | :---: | :---: | :---: |
| [Amazon S3](https://aws.amazon.com/s3/) | [![jdk / AWS](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fabashev%2Fbb4022ae143839ab84d016c9bc39fd85%2Fraw%2Fstatus.json&style=flat-square)](https://github.com/abashev/vfs-s3/actions/workflows/main-build.yml?query=branch%3A17.0) | [![commons-vfs / AWS](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fabashev%2Fc8e92c5273b3a71fee46cc9717c48bf2%2Fraw%2Fstatus.json&style=flat-square)](https://github.com/abashev/vfs-s3/actions/workflows/main-build.yml?query=branch%3A17.0) | — |
| [Yandex Object Storage](https://yandex.cloud/en/services/storage) | [![jdk / Yandex](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fabashev%2Fcc5795cc9160971453f4aad772ecc508%2Fraw%2Fstatus.json&style=flat-square)](https://github.com/abashev/vfs-s3/actions/workflows/main-build.yml?query=branch%3A17.0) | [![commons-vfs / Yandex](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fabashev%2F50a0153e062bd2853c43c74df6ee5a5b%2Fraw%2Fstatus.json&style=flat-square)](https://github.com/abashev/vfs-s3/actions/workflows/main-build.yml?query=branch%3A17.0) | — |

> The `spring` module is covered by unit tests only — it ships a mock backend today (see
> [Modules](#modules)).

### Versioning

Releases are numbered `17.X.N` (see [ADR-002](docs/adr/002-merge-automation-and-release-strategy.md)):

- **`17`** — the target JDK version (major); this line requires **Java 17**.
- **`X`** — the feature track (minor): **even (`0`, `2`, …) is unstable** (new features under test),
  **odd (`1`, `3`, …) is stable** (production-ready).
- **`N`** — an auto-incrementing release counter (patch).

So the current **`17.0.x`** is the **unstable development track**; the first stable line will be
`17.1.x`. The legacy **`4.x`** line (old versioning scheme) targets **Java 8** and is no longer
maintained.

### Modules

All published under the `com.github.abashev` group, at one shared version:

| Module | Artifact | What it gives you | URL dialect |
| --- | --- | --- | --- |
| jdk | `com.github.abashev:vfs-jdk` | JDK NIO.2 `FileSystemProvider` — `s3://…` via `java.nio.file` | [canonical](#url-scheme) |
| commons-vfs | `com.github.abashev:vfs-commons` | Apache Commons VFS provider (`FileObject`) | [legacy](#legacy-commons-vfs-format) |
| spring | `com.github.abashev:vfs-spring` | Spring `Resource` / `ResourceLoader` — **preview** | canonical |

> ⚠️ **The `spring` module is a preview.** It implements the Spring `Resource` API but is backed by
> a local-temp mock today; the real S3 backend is in development. Not for production yet.
>
> **`commons-vfs` is in maintenance mode** — not under active development. Its URL format is frozen
> for backward compatibility; new modules use the canonical [URL scheme](#url-scheme).

### Requirements

- **Java 17+** for the `17.x` line. (The legacy `4.x` line targets Java 8.)
- **AWS SDK for Java v2** — pulled in transitively; no separate dependency needed. It is not yet
  shaded, so align its version with any AWS SDK you already depend on.
- **`commons-vfs`** additionally needs **Apache Commons VFS 2.x** on the classpath.
- **`spring`** is declared `compileOnly`, so it **pulls no Spring into your build** (its published
  POM has no dependencies) — your application provides Spring. It is compiled against **Spring 6.x**.

### Installation

Depend on the module you need (Gradle shown; Maven is analogous):

```gradle
implementation 'com.github.abashev:vfs-jdk:17.0.0'      // JDK NIO.2 FileSystemProvider
implementation 'com.github.abashev:vfs-commons:17.0.0'  // Apache Commons VFS provider
implementation 'com.github.abashev:vfs-spring:17.0.0'   // Spring Resource (preview)
```

```xml
<dependency>
    <groupId>com.github.abashev</groupId>
    <artifactId>vfs-jdk</artifactId>
    <version>17.0.0</version>
</dependency>
```

> **None of the `vfs-*` artifacts are on Maven Central yet** — they publish from the next `17.0.x`
> release. The current `17.0.0` is available under the old coordinates (`jdk`, `commons-vfs`,
> `spring-6`, `spring-7`), now retired. `vfs-spring` is additionally a preview (mock backend).
>
> Legacy Java 8 line: `com.github.abashev:vfs-s3:4.4.0` (or `vfs-s3-with-awssdk-v1:4.4.0` with the
> AWS SDK v1 bundled).

### Quick start

**JDK NIO.2** (`jdk`) — the canonical, secret-free `s3://` scheme; credentials come from the AWS SDK
default chain:

```java
import java.net.URI;
import java.nio.file.*;
import java.util.Map;

// Region can ride in the URL (?region=…) or in the env map; credentials come from the SDK chain.
URI bucket = URI.create("s3://my-bucket?region=eu-central-1");
try (FileSystem fs = FileSystems.newFileSystem(bucket, Map.of())) {
    Path file = fs.getPath("/reports/q3.csv");
    Files.writeString(file, "hello,world\n");
    String csv = Files.readString(file);

    try (var dir = Files.newDirectoryStream(fs.getPath("/reports"))) {
        dir.forEach(System.out::println);
    }
}
```

→ more in [jdk module usage](#jdk-module).

**Spring** (`spring`) — ⚠️ **preview (mock backend today)**:

```java
// package: com.github.vfss3.spring
try (S3ResourceLoader loader = new S3ResourceLoader()) {
    WritableResource resource = (WritableResource) loader.getResource("s3://my-bucket/path/object.txt");
    try (OutputStream out = resource.getOutputStream()) {
        out.write("hello".getBytes());
    }
}
```

→ more in [spring module usage](#spring-module-preview).

**Apache Commons VFS** (`commons-vfs`) — the mature provider using the legacy dialect (region and
endpoint encoded in the host):

```java
FileSystemManager fs = VFS.getManager();
FileObject file = fs.resolveFile("s3://my-bucket.s3.eu-central-1.amazonaws.com/reports/q3.csv");
try (OutputStream out = file.getContent().getOutputStream()) {
    out.write("hello,world\n".getBytes());
}
```

→ more in [commons-vfs module usage](#commons-vfs-module).

### URL scheme

The canonical scheme is a plain, secret-free `s3://` URI — the bucket is the host, and the only
optional configuration rides in the query string:

    s3://<bucket>[/<key>][?region=<region>&endpoint=<endpoint-url>]

- **host** — the bucket name, and nothing else
- **`region`** — signing region (e.g. `eu-central-1`); when omitted it falls back to the AWS SDK
  default region chain
- **`endpoint`** — endpoint override for non-AWS / S3-compatible backends (e.g.
  `https://storage.yandexcloud.net`); omit it for real AWS
- **credentials are never carried in the URL** — they come from the AWS SDK default chain
  (environment variables, container/instance role, `~/.aws`, …)

Examples:

    s3://my-bucket
    s3://my-bucket/reports/q3.csv?region=eu-central-1
    s3://my-bucket?region=ru-central1&endpoint=https://storage.yandexcloud.net

This is what the `jdk` module implements, and what the remote test harness uses everywhere. See
[ADR-006](docs/adr/006-jdk-module-s3-uri-contract.md) for the full contract.

#### Legacy commons-vfs format

Previously every setting was packed into the URL itself — the region encoded in the host and,
optionally, the credentials in the userinfo:

    s3://[[access-key:secret-key]:sign-region]@endpoint-url/folder-or-bucket/

The `commons-vfs` module still parses this dialect, so existing URLs keep working — but new code
should prefer the secret-free scheme above.

### Module usage

#### jdk module

A JDK NIO.2 [`FileSystemProvider`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/file/spi/FileSystemProvider.html):
open a `FileSystem` for a bucket, then use the ordinary `java.nio.file` API (`Files`, `Path`,
directory streams, channels). Configuration comes from the [URL scheme](#url-scheme) and/or the
`env` map passed to `FileSystems.newFileSystem(uri, env)`:

- `region` — signing region
- `endpoint` — endpoint override for S3-compatible backends
- `credentialsProvider` — a pre-built `AwsCredentialsProvider` (env map only; never the URL)

```java
import software.amazon.awssdk.auth.credentials.*;

var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret"));
URI bucket = URI.create("s3://my-bucket?endpoint=https://storage.yandexcloud.net&region=ru-central1");
try (FileSystem fs = FileSystems.newFileSystem(bucket, Map.of("credentialsProvider", creds))) {
    Files.copy(Path.of("/tmp/backup.zip"), fs.getPath("/backups/backup.zip"));
}
```

The env map can also carry `region` / `endpoint`. When the same key is set in **both** the map and
the URL, the **env map wins** — precedence is env map → URL query → AWS default
([ADR-006](docs/adr/006-jdk-module-s3-uri-contract.md)):

```java
// The URL sets region=eu-central-1, but the env map overrides it.
URI bucket = URI.create("s3://my-bucket?region=eu-central-1");
var env = Map.of(
        "region", "eu-west-1",                          // wins over the URL's eu-central-1
        "endpoint", "https://storage.yandexcloud.net"); // endpoint supplied via the map
try (FileSystem fs = FileSystems.newFileSystem(bucket, env)) {
    // effective config: region = eu-west-1, endpoint = storage.yandexcloud.net (credentials from the SDK chain)
    Files.writeString(fs.getPath("/reports/q3.csv"), "hello,world\n");
}
```

#### commons-vfs module

The mature Apache Commons VFS provider. It resolves `s3://` URLs in its own [legacy
dialect](#legacy-commons-vfs-format) (endpoint and region in the host) and recognizes these
providers by host, falling back to a path-style custom endpoint for anything else:

Amazon S3 · Yandex Object Storage · Mail.ru Cloud Storage · Alibaba Cloud OSS · Oracle Cloud Object
Storage · DigitalOcean Spaces · SberCloud OBS.

```java
FileSystemManager fs = VFS.getManager();

// virtual-hosted (bucket in the host) …
FileObject a = fs.resolveFile("s3://my-bucket.storage.yandexcloud.net/dir/file.txt");
// … or path-style (bucket in the path)
FileObject b = fs.resolveFile("s3://s3.eu-central-1.amazonaws.com/my-bucket/dir/file.txt");
```

Credentials come from the AWS SDK default chain, or from `S3FileSystemOptions.setCredentialsProvider(...)`.

#### spring module (preview)

> ⚠️ **Preview.** `S3ResourceLoader` / `S3Resource` implement the Spring API but write to a local
> temporary directory today; the real S3 backend is in development. Use for experimentation, not
> production.

The `spring` module (package `com.github.vfss3.spring`) exposes an `S3ResourceLoader` that resolves
`s3://` locations to a Spring `WritableResource`. It is compiled against Spring 6.x; the core
`Resource` SPI it uses is unchanged in Spring 7.x:

```java
try (S3ResourceLoader loader = new S3ResourceLoader()) {
    WritableResource resource = (WritableResource) loader.getResource("s3://my-bucket/path/object.txt");
    try (OutputStream out = resource.getOutputStream()) {
        out.write("hello".getBytes());
    }
    try (InputStream in = resource.getInputStream()) {
        // read it back
    }
}
```

### Local development

This project uses [mise](https://mise.jdx.dev/) to manage the local Java toolchain and Gradle
(via the wrapper) to build.

    mise trust
    mise exec -- ./gradlew build                # compile + unit tests + formatting/lint checks
    mise exec -- ./gradlew :modules:jdk:test     # one module's unit tests

Handy `mise` tasks (see `mise.toml`): `mise run format`, `mise run format.check`, and
`mise run test:remote` (remote suites against AWS). Integration tests spin up S3-compatible
containers (LocalStack, MinIO, Garage, …) via Testcontainers.

On CI (GitHub Actions) Java is installed via `actions/setup-java`, so `./gradlew` runs directly
without `mise`.

### Running tests

The remote suites read a `BASE_URL` in the [URL scheme](#url-scheme) above, plus AWS credentials.
Provide them either way:

1.  Shell environment variables

        export AWS_ACCESS_KEY_ID=AAAAAAA
        export AWS_SECRET_KEY=SSSSSSS
        export BASE_URL='s3://<bucket>?region=<region>[&endpoint=<endpoint-url>]'

2. Or any standard AWS SDK mechanism (IAM role, `~/.aws`, …)

Locally, `mise run test:remote` runs both modules' remote suites against AWS with credentials read
from 1Password — see `mise.toml`.

**Make sure that you never commit your credentials!**

### Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

### License

Apache License 2.0 — see [LICENSE](LICENSE).
