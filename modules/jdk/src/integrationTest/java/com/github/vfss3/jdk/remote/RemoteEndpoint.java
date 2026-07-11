package com.github.vfss3.jdk.remote;

import com.github.vfss3.jdk.S3FileSystemConfig;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Splits a commons-vfs {@code BASE_URL} — the printf template {@code
 * modules/commons-vfs}'s {@code EnvironmentBasedSuite} uses — into the bucket name, AWS region,
 * and optional endpoint override that this module's env-map-configured provider needs. Both URL
 * styles commons-vfs accepts are handled: virtual-hosted ({@code
 * s3-tests-<token>.s3.eu-west-1.amazonaws.com}, bucket in the host) and path-style ({@code
 * s3.eu-central-1.amazonaws.com/s3-tests-<token>}, bucket in the path — this is what the AWS-1 CI
 * environment actually uses).
 *
 * <p>This is a test-only adapter: per ADR-006 the jdk provider itself never parses commons-vfs
 * URLs; it exists only so the remote suite can ride the GitHub environments (AWS-1, YANDEX-1,
 * YANDEX-2) already configured for commons-vfs, without a second set of variables. The host
 * patterns mirror {@code S3FileNameParser} in commons-vfs (optional bucket label, path fallback);
 * only the two providers wired into those environments are recognized — add a pattern here if a
 * new one is ever configured.
 */
record RemoteEndpoint(String bucket, String region, URI endpointOverride) {

    // Bucket label optional so one pattern matches both virtual-hosted and path-style hosts;
    // s3[-.] covers the modern dotted and the legacy dashed regional endpoints. Mirrors
    // commons-vfs's S3FileNameParser.
    private static final Pattern AWS_HOST =
            Pattern.compile("((?<bucket>[a-z0-9-]+)\\.)?s3[-.]((?<region>[a-z0-9-]+)\\.)?amazonaws\\.com");
    private static final Pattern YANDEX_HOST =
            Pattern.compile("((?<bucket>[a-z0-9-]+)\\.)?storage\\.yandexcloud\\.net");
    private static final Pattern PATH_BUCKET = Pattern.compile("/+(?<bucket>[^/]+).*");
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String YANDEX_REGION = "ru-central1";

    /**
     * @param urlTemplate a printf-style {@code s3://<host>/} template with a {@code %s}
     *     placeholder for the bucket-name token
     * @param token substituted for the {@code %s} placeholder
     */
    static RemoteEndpoint resolve(String urlTemplate, String token) {
        var uri = URI.create(String.format(urlTemplate, token));
        var host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("BASE_URL has no host: " + urlTemplate);
        }

        var aws = AWS_HOST.matcher(host);
        if (aws.matches()) {
            var region = aws.group("region");
            // No endpoint override for AWS — S3FileSystemConfig.buildS3Client() already
            // defaults to AWS's own virtual-hosted-style addressing when endpoint is null.
            return new RemoteEndpoint(
                    bucketOf(aws.group("bucket"), uri, urlTemplate),
                    region != null ? region : DEFAULT_AWS_REGION,
                    null);
        }

        var yandex = YANDEX_HOST.matcher(host);
        if (yandex.matches()) {
            return new RemoteEndpoint(
                    bucketOf(yandex.group("bucket"), uri, urlTemplate),
                    YANDEX_REGION,
                    URI.create("https://storage.yandexcloud.net"));
        }

        throw new IllegalArgumentException("Unrecognized remote host in BASE_URL (" + urlTemplate + "): " + host);
    }

    /**
     * The bucket is either a label in the host (virtual-hosted style) or the first path segment
     * (path-style) — whichever the BASE_URL uses.
     */
    private static String bucketOf(String hostBucket, URI uri, String urlTemplate) {
        if (hostBucket != null && !hostBucket.isBlank()) {
            return hostBucket;
        }
        if (uri.getPath() != null) {
            var pathBucket = PATH_BUCKET.matcher(uri.getPath());
            if (pathBucket.matches()) {
                return pathBucket.group("bucket");
            }
        }
        throw new IllegalArgumentException(
                "BASE_URL has no bucket, neither as a host label nor a path segment: " + urlTemplate);
    }

    /** The {@code env} map for {@link S3FileSystemConfig#fromEnv} matching this endpoint. */
    Map<String, Object> toEnv(AwsCredentialsProvider credentialsProvider) {
        var env = new HashMap<String, Object>();
        env.put(S3FileSystemConfig.ENV_REGION, region);
        env.put(S3FileSystemConfig.ENV_CREDENTIALS_PROVIDER, credentialsProvider);
        if (endpointOverride != null) {
            env.put(S3FileSystemConfig.ENV_ENDPOINT, endpointOverride.toString());
        }
        return env;
    }
}
