package com.github.vfss3.spring.remote;

import com.github.vfss3.spring.S3ClientConfig;
import java.net.URI;
import org.springframework.lang.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Splits the remote {@code BASE_URL} into the bucket name plus the {@link S3ClientConfig} this
 * module's loader needs.
 *
 * <p>{@code BASE_URL} is written in the canonical {@code s3://<bucket>?region=…&endpoint=…}
 * dialect (ADR-006) that the spring module shares with {@code modules/jdk}, so this is a thin
 * pass-through rather than a translator: the bucket is the URI host and region/endpoint come
 * straight from the query.
 */
record RemoteEndpoint(
        String bucket, @Nullable String region, @Nullable URI endpointOverride) {

    /**
     * @param urlTemplate a canonical {@code s3://<bucket>?region=…&endpoint=…} template with a
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
        String region = null;
        URI endpoint = null;
        var query = uri.getQuery();
        if (query != null && !query.isBlank()) {
            for (var pair : query.split("&", -1)) {
                var eq = pair.indexOf('=');
                var name = eq < 0 ? pair : pair.substring(0, eq);
                var value = eq < 0 ? "" : pair.substring(eq + 1);
                switch (name) {
                    case "region" -> region = value;
                    case "endpoint" -> endpoint = URI.create(value);
                    default ->
                        throw new IllegalArgumentException(
                                "Unrecognized BASE_URL query parameter '" + name + "': " + urlTemplate);
                }
            }
        }
        return new RemoteEndpoint(bucket, region, endpoint);
    }

    /** The {@link S3ClientConfig} matching this endpoint. */
    S3ClientConfig toConfig(AwsCredentialsProvider credentialsProvider) {
        var config = S3ClientConfig.defaults().withCredentialsProvider(credentialsProvider);
        if (region != null) {
            config = config.withRegion(region);
        }
        if (endpointOverride != null) {
            config = config.withEndpoint(endpointOverride);
        }
        return config;
    }
}
