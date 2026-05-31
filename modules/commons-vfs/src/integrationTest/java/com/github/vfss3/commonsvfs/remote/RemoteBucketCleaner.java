package com.github.vfss3.commonsvfs.remote;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

/**
 * Standalone cleanup entry point for the per-commit remote integration bucket.
 *
 * <p>Reads the same {@code BASE_URL} template and {@code BUCKET_TOKEN} that
 * {@link EnvironmentBasedSuite} uses, resolves the bucket root through VFS and runs
 * {@code deleteAll()} — which removes every object and the bucket itself. It reuses the
 * driver's own deletion path, so it works uniformly across every S3-compatible backend
 * (AWS, Yandex, …) without any provider-specific endpoint/region parsing.
 *
 * <p>It is meant to be invoked from CI as a separate {@code if: always()} step, so the
 * bucket is removed even when the test JVM failed or was killed before the suite's
 * {@code @AfterSuite} teardown could run:
 *
 * <pre>{@code ./gradlew :modules:commons-vfs:dropRemoteBucket}</pre>
 *
 * <p>A bucket that is already gone (the happy path — the suite deleted it) is treated as
 * success; any other failure is propagated so CI surfaces it as a red step.
 */
public final class RemoteBucketCleaner {

    private static final Logger log = LoggerFactory.getLogger(RemoteBucketCleaner.class);

    private RemoteBucketCleaner() {}

    public static void main(String[] args) throws Exception {
        String urlTemplate = System.getenv(EnvironmentBasedSuite.ENV_BASE_URL);
        String token = System.getenv(EnvironmentBasedSuite.ENV_BUCKET_TOKEN);

        if (isBlank(urlTemplate) || isBlank(token)) {
            log.info(
                    "RemoteBucketCleaner — {} / {} not set, nothing to clean up",
                    EnvironmentBasedSuite.ENV_BASE_URL,
                    EnvironmentBasedSuite.ENV_BUCKET_TOKEN);
            return;
        }

        String bucketUrl = String.format(urlTemplate, token);

        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(DefaultCredentialsProvider.create());

        try {
            FileObject root = VFS.getManager().resolveFile(bucketUrl, options.toFileSystemOptions());
            root.refresh();

            int deleted = root.deleteAll();

            log.info("RemoteBucketCleaner — deleted bucket {} ({} object(s) removed)", bucketUrl, deleted);
        } catch (FileSystemException e) {
            if (isMissingBucket(e)) {
                log.info("RemoteBucketCleaner — bucket {} already gone, nothing to delete", bucketUrl);
            } else {
                log.warn("RemoteBucketCleaner — failed to delete bucket {}", bucketUrl, e);
                throw e;
            }
        }
    }

    /** Walk the cause chain looking for the SDK's "no such bucket" / HTTP 404 signal. */
    private static boolean isMissingBucket(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof NoSuchBucketException) {
                return true;
            }
            if (cause instanceof AwsServiceException && ((AwsServiceException) cause).statusCode() == 404) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
