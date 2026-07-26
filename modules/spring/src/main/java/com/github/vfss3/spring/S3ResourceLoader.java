package com.github.vfss3.spring;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Spring {@link org.springframework.core.io.ResourceLoader} that resolves {@code s3://} locations
 * to {@link S3Resource} instances backed by a real {@link S3Client}, delegating all other
 * locations to Spring's default resolution.
 *
 * <p>Locations follow the canonical {@code s3://<bucket>[/<key>][?region=…&endpoint=…]} contract
 * (see {@code docs/adr/006-jdk-module-s3-uri-contract.md}): configuration set on the loader wins
 * over a location's query parameters, which win over the AWS SDK default chains. Credentials are
 * never read from a location — supply an {@link AwsCredentialsProvider} through
 * {@link S3ClientConfig} (or rely on the SDK default chain).
 *
 * <p>The loader lazily builds one {@link S3Client} per distinct effective (region, endpoint)
 * pair and caches it; {@link #close()} closes every client the loader built. A pre-built client
 * supplied through {@link #S3ResourceLoader(S3Client)} is used for every location instead and is
 * never closed here — the caller owns it.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (S3ResourceLoader loader = new S3ResourceLoader()) {
 *     WritableResource resource = (WritableResource) loader.getResource("s3://my-bucket/path/to/object.txt");
 *     try (OutputStream out = resource.getOutputStream()) { ... }
 * }
 * }</pre>
 *
 * <p>Instances are thread-safe.
 */
public class S3ResourceLoader extends DefaultResourceLoader implements Closeable {

    /** Base config for built clients; {@code null} iff {@link #fixedClient} is set. */
    @Nullable
    private final S3ClientConfig config;

    /** Caller-owned client used for every location; {@code null} unless supplied. */
    @Nullable
    private final S3Client fixedClient;

    private final Map<S3ClientConfig, S3Client> clients = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /** Loader using the AWS SDK default credentials and region chains. */
    public S3ResourceLoader() {
        this(S3ClientConfig.defaults());
    }

    /**
     * Loader that builds and caches one {@link S3Client} per distinct effective (region,
     * endpoint) pair, starting from the given config. Built clients are closed by
     * {@link #close()}.
     */
    public S3ResourceLoader(S3ClientConfig config) {
        this.config = requireNonNull(config, "config");
        this.fixedClient = null;
    }

    /**
     * Loader that uses the given pre-built client for every {@code s3://} location. The caller
     * keeps ownership — {@link #close()} does not close it. Location query parameters
     * ({@code ?region=…&endpoint=…}) are still validated but have no effect on a fixed client.
     */
    public S3ResourceLoader(S3Client client) {
        this.config = null;
        this.fixedClient = requireNonNull(client, "client");
    }

    @Override
    public Resource getResource(String location) {
        if (!S3Uris.isS3Location(location)) {
            return super.getResource(location);
        }
        var parsed = S3Uris.parse(location);
        return new S3Resource(S3Uris.toUri(parsed.bucket(), parsed.key()), clientFor(parsed.query()));
    }

    /**
     * The client for a location's validated query parameters — the fixed client when one was
     * supplied, else a cached (possibly freshly built) client for the effective (region,
     * endpoint) pair.
     */
    S3Client clientFor(Map<String, String> query) {
        if (fixedClient != null) {
            return fixedClient;
        }
        if (closed) {
            throw new IllegalStateException("S3ResourceLoader is closed");
        }
        return clients.computeIfAbsent(requireNonNull(config).resolve(query), S3ClientConfig::buildS3Client);
    }

    /**
     * Closes every {@link S3Client} this loader built. A pre-built client supplied through
     * {@link #S3ResourceLoader(S3Client)} is left open — the caller owns it. Idempotent.
     */
    @Override
    public void close() {
        closed = true;
        clients.values().forEach(S3Client::close);
        clients.clear();
    }
}
