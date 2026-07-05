package com.github.vfss3.jdk.remote;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Splits a {@code BASE_URL}-style bucket hostname — the same printf template convention
 * {@code modules/commons-vfs}'s {@code EnvironmentBasedSuite} uses, e.g. {@code
 * s3-tests-<token>.s3.eu-west-1.amazonaws.com} or {@code s3-tests-<token>.storage.yandexcloud.net}
 * — into a bucket name, an AWS region, and an optional endpoint override.
 *
 * <p>Reusing the exact same {@code BASE_URL} CI environment variable lets this module's remote
 * suite ride on the GitHub environments (AWS-1, YANDEX-1, YANDEX-2) already configured for
 * commons-vfs, without introducing a second, module-specific set of environment variables. Only
 * the two providers actually wired into those environments are recognized; add a pattern here if
 * a new provider is ever configured (mirrors {@code S3FileNameParser} in commons-vfs, which
 * supports more providers because its host-parsing is part of the shipped library, not
 * test-only infrastructure).
 */
final class RemoteEndpoint {

    private static final Pattern AWS_HOST =
            Pattern.compile("(?<bucket>[a-z0-9-]+)\\.s3\\.((?<region>[a-z0-9-]+)\\.)?amazonaws\\.com");
    private static final Pattern YANDEX_HOST = Pattern.compile("(?<bucket>[a-z0-9-]+)\\.storage\\.yandexcloud\\.net");
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String YANDEX_REGION = "ru-central1";

    final String bucket;
    final String region;
    final URI endpointOverride;

    private RemoteEndpoint(String bucket, String region, URI endpointOverride) {
        this.bucket = bucket;
        this.region = region;
        this.endpointOverride = endpointOverride;
    }

    /**
     * @param urlTemplate a printf-style {@code s3://<host>/} template with a {@code %s}
     *     placeholder for the bucket-name token
     * @param token substituted for the {@code %s} placeholder
     */
    static RemoteEndpoint resolve(String urlTemplate, String token) {
        var host = URI.create(String.format(urlTemplate, token)).getHost();
        if (host == null) {
            throw new IllegalArgumentException("BASE_URL has no host: " + urlTemplate);
        }

        var aws = AWS_HOST.matcher(host);
        if (aws.matches()) {
            var region = aws.group("region");
            // No endpoint override for AWS — S3FileSystemConfig.buildS3Client() already
            // defaults to AWS's own virtual-hosted-style addressing when endpoint is null.
            return new RemoteEndpoint(aws.group("bucket"), region != null ? region : DEFAULT_AWS_REGION, null);
        }

        var yandex = YANDEX_HOST.matcher(host);
        if (yandex.matches()) {
            return new RemoteEndpoint(
                    yandex.group("bucket"), YANDEX_REGION, URI.create("https://storage.yandexcloud.net"));
        }

        throw new IllegalArgumentException("Unrecognized remote host in BASE_URL (" + urlTemplate + "): " + host);
    }
}
