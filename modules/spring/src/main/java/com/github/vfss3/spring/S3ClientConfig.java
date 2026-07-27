package com.github.vfss3.spring;

import java.net.URI;
import java.util.Map;
import org.springframework.lang.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Configuration for the {@link S3Client} instances an {@link S3ResourceLoader} builds.
 *
 * <p>All parts are optional: a {@code null} region or endpoint falls through to a location's
 * {@code ?region=…&endpoint=…} query parameters and finally to the AWS SDK default chains; a
 * {@code null} credentials provider is normalized to {@link DefaultCredentialsProvider}. A value
 * set here always wins over a location's query parameters. Credentials are supplied only here —
 * never through a location string.
 *
 * <pre>{@code
 * var config = S3ClientConfig.defaults()
 *         .withRegion("ru-central1")
 *         .withEndpoint("https://storage.yandexcloud.net");
 * try (var loader = new S3ResourceLoader(config)) { ... }
 * }</pre>
 *
 * @param region the signing region id, e.g. {@code "eu-central-1"}; may be {@code null}
 * @param endpoint the endpoint override for S3-compatible backends; may be {@code null}
 * @param credentialsProvider the credentials source; {@code null} means the AWS SDK default chain
 */
public record S3ClientConfig(
        @Nullable String region, @Nullable URI endpoint, AwsCredentialsProvider credentialsProvider) {

    /** Normalizes a missing credentials provider to the AWS SDK default chain. */
    public S3ClientConfig {
        if (credentialsProvider == null) {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
    }

    /** Config with no explicit region/endpoint and the AWS SDK default credentials chain. */
    public static S3ClientConfig defaults() {
        return new S3ClientConfig(null, null, DefaultCredentialsProvider.create());
    }

    /** Copy of this config with the given signing region, e.g. {@code "eu-central-1"}. */
    public S3ClientConfig withRegion(String region) {
        return new S3ClientConfig(region, endpoint, credentialsProvider);
    }

    /** Copy of this config with the given endpoint override for S3-compatible backends. */
    public S3ClientConfig withEndpoint(URI endpoint) {
        return new S3ClientConfig(region, endpoint, credentialsProvider);
    }

    /** Copy of this config with the given endpoint override parsed from a string. */
    public S3ClientConfig withEndpoint(String endpoint) {
        return withEndpoint(URI.create(endpoint));
    }

    /** Copy of this config with the given credentials provider. */
    public S3ClientConfig withCredentialsProvider(AwsCredentialsProvider credentialsProvider) {
        return new S3ClientConfig(region, endpoint, credentialsProvider);
    }

    /**
     * The effective config for one location: a value set on this config wins, then the location's
     * validated query parameters, then (at client build time) the AWS SDK defaults.
     */
    S3ClientConfig resolve(Map<String, String> query) {
        var queryEndpoint = query.get(S3Uris.QUERY_ENDPOINT);
        return new S3ClientConfig(
                region != null ? region : query.get(S3Uris.QUERY_REGION),
                endpoint != null ? endpoint : (queryEndpoint != null ? URI.create(queryEndpoint) : null),
                credentialsProvider);
    }

    /** Builds a client for this config. The caller owns (and closes) the returned client. */
    public S3Client buildS3Client() {
        var builder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(resolveRegion())
                // Several S3-compatible servers (e.g. Garage) don't correctly validate the SDK's
                // default chunked/streaming payload signature and reject every request with
                // "Invalid payload signature". Disabling it trades a small efficiency loss for
                // compatibility across backends — modules/jdk and modules/commons-vfs make the
                // same call unconditionally.
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
