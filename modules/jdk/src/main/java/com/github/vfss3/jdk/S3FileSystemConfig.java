package com.github.vfss3.jdk;

import java.net.URI;
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
 * <p>The AWS SDK v2 default credentials/region provider chain is the primary configuration
 * path — no explicit {@code env} keys are required for real AWS. The keys below only override
 * individual fields, mainly for local-endpoint wiring (LocalStack, MinIO, and similar):
 *
 * <ul>
 *   <li>{@link #ENV_REGION} — a region id, e.g. {@code "eu-central-1"}
 *   <li>{@link #ENV_ENDPOINT} — an endpoint override URI, e.g. {@code "http://localhost:9000"}
 *   <li>{@link #ENV_CREDENTIALS_PROVIDER} — a pre-built {@link AwsCredentialsProvider}
 * </ul>
 */
public record S3FileSystemConfig(String region, URI endpoint, AwsCredentialsProvider credentialsProvider) {

    /** Env key for the region id, e.g. {@code "eu-central-1"}. */
    public static final String ENV_REGION = "aws.region";

    /** Env key for the endpoint override URI, e.g. {@code "http://localhost:9000"}. */
    public static final String ENV_ENDPOINT = "aws.endpoint";

    /** Env key for a pre-built {@link AwsCredentialsProvider}. */
    public static final String ENV_CREDENTIALS_PROVIDER = "aws.credentialsProvider";

    public static S3FileSystemConfig fromEnv(Map<String, ?> env) {
        var region = (String) env.get(ENV_REGION);
        var endpointValue = (String) env.get(ENV_ENDPOINT);
        var endpoint = endpointValue == null ? null : URI.create(endpointValue);
        var providerOverride = env.get(ENV_CREDENTIALS_PROVIDER);
        var credentialsProvider = (AwsCredentialsProvider)
                (providerOverride != null ? providerOverride : DefaultCredentialsProvider.create());

        return new S3FileSystemConfig(region, endpoint, credentialsProvider);
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
