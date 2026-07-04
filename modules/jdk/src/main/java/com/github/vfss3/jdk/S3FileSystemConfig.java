package com.github.vfss3.jdk;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Configuration for the S3-backed {@link S3FileSystemProvider}.
 *
 * <p>The AWS SDK v2 default credentials/region provider chain is the primary configuration
 * path — no explicit {@code env} keys are required for real AWS. The keys below only override
 * individual fields, mainly for local-endpoint wiring (LocalStack, MinIO, and similar):
 *
 * <ul>
 *   <li>{@code aws.region} — a region id, e.g. {@code "eu-central-1"}
 *   <li>{@code aws.endpoint} — an endpoint override URI, e.g. {@code "http://localhost:9000"}
 *   <li>{@code aws.credentialsProvider} — a pre-built {@link AwsCredentialsProvider}
 * </ul>
 */
public record S3FileSystemConfig(String region, URI endpoint, AwsCredentialsProvider credentialsProvider) {

    public static S3FileSystemConfig fromEnv(Map<String, ?> env) {
        var region = (String) env.get("aws.region");
        var endpoint = Optional.ofNullable((String) env.get("aws.endpoint"))
                .map(URI::create)
                .orElse(null);
        var providerOverride = env.get("aws.credentialsProvider");
        var credentialsProvider = (AwsCredentialsProvider)
                (providerOverride != null ? providerOverride : DefaultCredentialsProvider.create());

        return new S3FileSystemConfig(region, endpoint, credentialsProvider);
    }

    public S3Client buildS3Client() {
        var builder = S3Client.builder().credentialsProvider(credentialsProvider);
        if (region != null) {
            builder.region(Region.of(region));
        }
        if (endpoint != null) {
            builder.endpointOverride(endpoint);
        }
        return builder.build();
    }
}
