package com.github.vfss3.jdk.remote;

import com.github.vfss3.jdk.S3FileSystemConfig;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Splits the remote {@code BASE_URL} into the bucket name plus the region/endpoint overrides this
 * module's env-map-configured provider needs.
 *
 * <p>Since ADR-006 made the jdk URI the canonical CI {@code BASE_URL}
 * ({@code s3://<bucket>?region=…&endpoint=…} — see
 * {@code docs/adr/006-jdk-module-s3-uri-contract.md}), this is a thin pass-through rather than a
 * translator: the bucket is the URI host, and region/endpoint are read straight through the
 * production {@link S3FileSystemConfig#from(URI, Map)} contract (which also rejects userinfo
 * credentials and unknown query parameters). The translating adapter now lives on the other side —
 * {@code modules/commons-vfs}'s remote suite rebuilds a commons-vfs URL from this same
 * {@code BASE_URL}.
 */
record RemoteEndpoint(String bucket, String region, URI endpointOverride) {

    /**
     * @param urlTemplate a jdk-dialect {@code s3://<bucket>?region=…&endpoint=…} template with a
     *     {@code %s} placeholder in the bucket for the per-run bucket-name token
     * @param token substituted for the {@code %s} placeholder
     */
    static RemoteEndpoint resolve(String urlTemplate, String token) {
        var uri = URI.create(String.format(urlTemplate, token));
        var bucket = uri.getHost();
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException(
                    "BASE_URL has no bucket in the host position (expected s3://<bucket>?…): " + urlTemplate);
        }
        // Reuse the production URI-query contract to read region/endpoint (and validate the URI).
        var config = S3FileSystemConfig.from(uri, Map.of());
        return new RemoteEndpoint(bucket, config.region(), config.endpoint());
    }

    /** The {@code env} map for {@link S3FileSystemConfig#fromEnv} matching this endpoint. */
    Map<String, Object> toEnv(AwsCredentialsProvider credentialsProvider) {
        var env = new HashMap<String, Object>();
        if (region != null) {
            env.put(S3FileSystemConfig.ENV_REGION, region);
        }
        env.put(S3FileSystemConfig.ENV_CREDENTIALS_PROVIDER, credentialsProvider);
        if (endpointOverride != null) {
            env.put(S3FileSystemConfig.ENV_ENDPOINT, endpointOverride.toString());
        }
        return env;
    }
}
