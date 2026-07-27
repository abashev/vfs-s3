package com.github.vfss3.spring.remote;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * Standalone cleanup entry point for the spring module's per-commit remote integration bucket.
 * Mirrors the jdk module's {@code RemoteBucketCleaner}: deletes through a raw {@link S3Client}
 * rather than via the Spring Resource API, since cleanup must work even if the loader under test
 * is itself broken (and the Resource API has no delete anyway).
 *
 * <p>Reads the same {@code BASE_URL} and {@code BUCKET_TOKEN} that {@link EnvironmentBasedSuite}
 * uses, so it can target the exact same bucket even when the test JVM died before {@code
 * @AfterSuite} teardown ran:
 *
 * <pre>{@code ./gradlew :modules:spring:dropRemoteBucket}</pre>
 *
 * <p>A bucket that is already gone (the happy path — the suite deleted it) is treated as
 * success; any other failure is propagated so CI surfaces it as a red step.
 */
public final class RemoteBucketCleaner {

    private RemoteBucketCleaner() {}

    public static void main(String[] args) {
        var urlTemplate = System.getenv(EnvironmentBasedSuite.ENV_BASE_URL);
        var token = System.getenv(EnvironmentBasedSuite.ENV_BUCKET_TOKEN);

        if (urlTemplate == null || urlTemplate.isBlank() || token == null || token.isBlank()) {
            System.out.println("RemoteBucketCleaner — " + EnvironmentBasedSuite.ENV_BASE_URL + " / "
                    + EnvironmentBasedSuite.ENV_BUCKET_TOKEN + " not set, nothing to clean up");
            return;
        }

        var remote = RemoteEndpoint.resolve(urlTemplate, token);

        try (var client = remote.toConfig(DefaultCredentialsProvider.create()).buildS3Client()) {
            deleteBucketAndContents(client, remote.bucket());
            System.out.println("RemoteBucketCleaner — deleted bucket " + remote.bucket());
        } catch (NoSuchBucketException e) {
            System.out.println("RemoteBucketCleaner — bucket " + remote.bucket() + " already gone, nothing to delete");
        } catch (AwsServiceException e) {
            if (e.statusCode() == 404) {
                System.out.println(
                        "RemoteBucketCleaner — bucket " + remote.bucket() + " already gone, nothing to delete");
            } else {
                throw e;
            }
        }
    }

    /** Deletes every object under {@code bucket} (paginated, batched) and then the bucket itself. */
    static void deleteBucketAndContents(S3Client client, String bucket) {
        for (var page : client.listObjectsV2Paginator(b -> b.bucket(bucket))) {
            var keys = page.contents().stream()
                    .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                    .toList();
            if (!keys.isEmpty()) {
                client.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(keys)));
            }
        }
        client.deleteBucket(b -> b.bucket(bucket));
    }
}
