package com.github.vfss3.jdk;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Configuration for the S3-backed {@link S3FileSystemProvider}.
 *
 * <p>Configuration comes from two channels — see {@code docs/adr/006-jdk-module-s3-uri-contract.md}.
 * The {@code env} map (highest priority) carries these keys; the same {@link #ENV_REGION} /
 * {@link #ENV_ENDPOINT} names are also accepted as {@code s3://bucket?region=…&endpoint=…}
 * URI query parameters (lower priority). When neither supplies a value, the AWS SDK v2 default
 * region provider chain (and finally {@code us-east-1}) applies.
 *
 * <ul>
 *   <li>{@link #ENV_REGION} — a region id, e.g. {@code "eu-central-1"} (env or URI query)
 *   <li>{@link #ENV_ENDPOINT} — an endpoint override URI, e.g. {@code "http://localhost:9000"}
 *       (env or URI query)
 *   <li>{@link #ENV_CREDENTIALS_PROVIDER} — a pre-built {@link AwsCredentialsProvider} (env only;
 *       credentials are never accepted from a URI)
 * </ul>
 */
public record S3FileSystemConfig(String region, URI endpoint, AwsCredentialsProvider credentialsProvider) {

    /** Env/query key for the region id, e.g. {@code "eu-central-1"}. */
    public static final String ENV_REGION = "region";

    /** Env/query key for the endpoint override URI, e.g. {@code "http://localhost:9000"}. */
    public static final String ENV_ENDPOINT = "endpoint";

    /** Env key for a pre-built {@link AwsCredentialsProvider}. Never read from a URI. */
    public static final String ENV_CREDENTIALS_PROVIDER = "credentialsProvider";

    /** Config from the {@code env} map alone — no URI query channel. */
    public static S3FileSystemConfig fromEnv(Map<String, ?> env) {
        return build((String) env.get(ENV_REGION), (String) env.get(ENV_ENDPOINT), env.get(ENV_CREDENTIALS_PROVIDER));
    }

    /**
     * Config merged from the {@code env} map and the {@code uri}'s query parameters, per
     * ADR-006: {@code env} wins over the URI, which wins over AWS defaults. The URI must not
     * carry userinfo (credentials) and must use only the {@link #ENV_REGION} / {@link #ENV_ENDPOINT}
     * query keys.
     */
    public static S3FileSystemConfig from(URI uri, Map<String, ?> env) {
        var query = queryParams(uri);
        return build(
                env.get(ENV_REGION) != null ? (String) env.get(ENV_REGION) : query.get(ENV_REGION),
                env.get(ENV_ENDPOINT) != null ? (String) env.get(ENV_ENDPOINT) : query.get(ENV_ENDPOINT),
                env.get(ENV_CREDENTIALS_PROVIDER));
    }

    private static S3FileSystemConfig build(String region, String endpointValue, Object providerOverride) {
        var endpoint = endpointValue == null ? null : URI.create(endpointValue);
        var credentialsProvider = (AwsCredentialsProvider)
                (providerOverride != null ? providerOverride : DefaultCredentialsProvider.create());
        return new S3FileSystemConfig(region, endpoint, credentialsProvider);
    }

    /** Parses and validates the recognized {@code aws.*} query parameters from an {@code s3://} URI. */
    private static Map<String, String> queryParams(URI uri) {
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Credentials must not be encoded in an s3:// URI — pass an "
                    + ENV_CREDENTIALS_PROVIDER + " through the FileSystem env map instead: " + uri);
        }
        var query = uri.getQuery();
        if (query == null || query.isBlank()) {
            return Map.of();
        }
        var params = new HashMap<String, String>();
        for (var pair : query.split("&")) {
            var eq = pair.indexOf('=');
            var key = eq < 0 ? pair : pair.substring(0, eq);
            if (!key.equals(ENV_REGION) && !key.equals(ENV_ENDPOINT)) {
                throw new IllegalArgumentException("Unrecognized s3:// URI query parameter '" + key + "' — only "
                        + ENV_REGION + " and " + ENV_ENDPOINT + " are supported: " + uri);
            }
            params.put(key, eq < 0 ? "" : pair.substring(eq + 1));
        }
        return params;
    }

    public S3Client buildS3Client() {
        var builder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(resolveRegion())
                // Several S3-compatible servers (e.g. Garage) don't correctly validate the SDK's
                // default chunked/streaming payload signature and reject every request with
                // "Invalid payload signature". Disabling it trades a small efficiency loss for
                // compatibility across backends — modules/commons-vfs makes the same call
                // unconditionally, for every backend, not just the ones known to need it.
                .serviceConfiguration(
                        S3Configuration.builder().chunkedEncodingEnabled(false).build());
        if (endpoint != null) {
            // A custom endpoint is never an AWS virtual-hosted-style domain, so path-style
            // addressing is required (LocalStack, MinIO, and similar local emulators).
            builder.endpointOverride(endpoint).forcePathStyle(true);
        }
        return builder.build();
    }

    /**
     * Falls back to {@link Region#US_EAST_1} when neither an explicit override nor the SDK
     * default provider chain can resolve one — keeps client construction offline-safe (no
     * region configured anywhere) instead of throwing at build time.
     */
    private Region resolveRegion() {
        if (region != null) {
            return Region.of(region);
        }
        try {
            return DefaultAwsRegionProviderChain.builder().build().getRegion();
        } catch (SdkClientException e) {
            return Region.US_EAST_1;
        }
    }
}
