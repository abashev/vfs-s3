package com.github.vfss3.spring;

import java.io.IOException;
import java.util.ArrayList;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.AntPathMatcher;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Spring {@link ResourcePatternResolver} that resolves {@code s3://} Ant-style patterns
 * ({@code *} within one key segment, {@code **} across segments) against a real bucket listing,
 * and delegates every other location pattern (classpath, file system, …) to a
 * {@link PathMatchingResourcePatternResolver}.
 *
 * <p>Extends {@link S3ResourceLoader}, so it is also a full {@code s3://}-aware
 * {@link org.springframework.core.io.ResourceLoader} with the same configuration, client cache
 * and {@link #close()} lifecycle — one object to construct and close.
 *
 * <pre>{@code
 * try (var resolver = new S3ResourcePatternResolver()) {
 *     Resource[] reports = resolver.getResources("s3://my-bucket/reports/*.csv");
 * }
 * }</pre>
 *
 * <p>A pattern lists the bucket under the longest wildcard-free key prefix (paginated
 * {@code ListObjectsV2}) and matches full keys with an {@link AntPathMatcher}; results come back
 * in S3's lexicographic key order, and zero matches yield an empty array. Folder-marker objects
 * (keys ending in {@code /}) are excluded. Wildcards in the bucket name are not supported.
 *
 * <p>The Ant single-character wildcard {@code ?} is not usable in {@code s3://} patterns: the
 * first {@code ?} of a location always starts the {@code ?region=…&endpoint=…} query component.
 * Use {@code *} / {@code **}.
 */
public class S3ResourcePatternResolver extends S3ResourceLoader implements ResourcePatternResolver {

    private final PathMatchingResourcePatternResolver fallback = new PathMatchingResourcePatternResolver(this);
    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    /** Resolver using the AWS SDK default credentials and region chains. */
    public S3ResourcePatternResolver() {}

    /** Resolver that builds and caches clients from the given config — see {@link S3ResourceLoader}. */
    public S3ResourcePatternResolver(S3ClientConfig config) {
        super(config);
    }

    /** Resolver that uses the given pre-built client for every location; the caller owns it. */
    public S3ResourcePatternResolver(S3Client client) {
        super(client);
    }

    @Override
    public Resource[] getResources(String locationPattern) throws IOException {
        if (!S3Uris.isS3Location(locationPattern)) {
            return fallback.getResources(locationPattern);
        }
        // Parse first: this validates the location and splits off the ?region=…&endpoint=…
        // query, so a query's '?' is never mistaken for an Ant wildcard below.
        var parsed = S3Uris.parse(locationPattern);
        if (pathMatcher.isPattern(parsed.bucket())) {
            throw new IllegalArgumentException("Wildcards in the bucket name are not supported: " + locationPattern);
        }
        if (!pathMatcher.isPattern(parsed.key())) {
            return new Resource[] {getResource(locationPattern)};
        }
        return findMatching(parsed);
    }

    private Resource[] findMatching(S3Uris.S3Location parsed) throws IOException {
        var client = clientFor(parsed.query());
        var prefix = fixedPrefix(parsed.key());
        var matches = new ArrayList<Resource>();
        try {
            // No delimiter — Ant `**` crosses `/` boundaries, so the candidate set is the full
            // recursive listing under the wildcard-free prefix; the AntPathMatcher enforces
            // that a single `*` stays within one segment.
            for (var page :
                    client.listObjectsV2Paginator(b -> b.bucket(parsed.bucket()).prefix(prefix))) {
                for (var object : page.contents()) {
                    if (object.key().endsWith("/")) {
                        continue;
                    }
                    if (pathMatcher.match(parsed.key(), object.key())) {
                        matches.add(new S3Resource(S3Uris.toUri(parsed.bucket(), object.key()), client));
                    }
                }
            }
        } catch (SdkException e) {
            throw new IOException("Failed to list s3://" + parsed.bucket() + "/" + prefix, e);
        }
        return matches.toArray(new Resource[0]);
    }

    /**
     * The longest wildcard-free key prefix ending at a {@code /} boundary — used as the S3
     * listing prefix. Mirrors the technique of
     * {@code PathMatchingResourcePatternResolver#determineRootDir}.
     */
    private String fixedPrefix(String keyPattern) {
        var end = keyPattern.length();
        while (end > 0 && pathMatcher.isPattern(keyPattern.substring(0, end))) {
            end = keyPattern.lastIndexOf('/', end - 2) + 1;
        }
        return keyPattern.substring(0, end);
    }
}
