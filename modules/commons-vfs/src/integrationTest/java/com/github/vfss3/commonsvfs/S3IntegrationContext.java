package com.github.vfss3.commonsvfs;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.net.URI;
import java.util.Random;
import org.apache.commons.vfs2.FileSystemOptions;

/**
 * Process-wide data holder shared between a backend-specific {@code @Suite} (LocalStack,
 * MinIO, … or a remote AWS / Yandex bucket) and the test cases in
 * {@link com.github.vfss3.commonsvfs.tests}.
 *
 * <p>The suite calls one of the {@code initialize} overloads from {@code @BeforeSuite} and
 * {@link #reset()} from {@code @AfterSuite}. Test classes read {@link #rootUrl()} and
 * {@link #options()} and resolve VFS themselves — this class deliberately has no
 * dependency on Apache Commons VFS.
 */
public final class S3IntegrationContext {

    /** Path (relative to the module dir) of the local payload used by upload tests. */
    public static final String BINARY_FILE = "src/test/resources/backup.zip";

    /** URL of the multi-megabyte file used to exercise multipart upload. */
    public static final String BIG_FILE =
            "http://archive.ubuntu.com/ubuntu/dists/xenial-updates/main/installer-i386/current/images/netboot/mini.iso";

    private static String rootUrl;
    private static FileSystemOptions options;

    private S3IntegrationContext() {}

    /**
     * Build a unique bucket URL for this run on the supplied endpoint and store the
     * (URL, options) pair for the test cases to read. Used by the local container suites,
     * which can freely create and delete throwaway buckets.
     */
    public static void initialize(URI endpoint, S3FileSystemOptions backendOptions) {
        requireNonNull(endpoint, "endpoint");
        requireNonNull(backendOptions, "backendOptions");

        backendOptions.setCreateBucket(true);

        // Unique bucket name per suite run (3 random hex segments, lowercase S3-compatible).
        String token =
                new Random().ints(3).mapToObj(i -> String.format("%08x", i)).collect(joining());
        String bucketName = "vfs3-tests-" + token;

        // Custom-endpoint (path-style) URL: s3://host:port/bucket/
        initialize(String.format("s3://%s:%d/%s/", endpoint.getHost(), endpoint.getPort(), bucketName), backendOptions);
    }

    /**
     * Use the supplied bucket URL verbatim (no random bucket generation, no CreateBucket
     * call). Intended for remote suites that target a pre-existing AWS / Yandex / etc.
     * bucket the test user has read+write access to.
     */
    public static void initialize(String bucketUrl, S3FileSystemOptions backendOptions) {
        requireNonNull(bucketUrl, "bucketUrl");
        requireNonNull(backendOptions, "backendOptions");

        rootUrl = bucketUrl;
        options = backendOptions.toFileSystemOptions();
    }

    /** Drop the cached state. The container itself is stopped by the suite. */
    public static void reset() {
        rootUrl = null;
        options = null;
    }

    /**
     * Random lowercase hex token suitable for embedding in a per-run S3 bucket name.
     * Stable format (24 hex chars from 3 random ints) — matches what the URI-based
     * {@code initialize} uses internally so all suites generate consistent names.
     */
    public static String randomBucketToken() {
        return new Random().ints(3).mapToObj(i -> String.format("%08x", i)).collect(joining());
    }

    public static String rootUrl() {
        return requireNonNull(rootUrl, "S3IntegrationContext is not initialized — must run inside a configured @Suite");
    }

    public static FileSystemOptions options() {
        return requireNonNull(options, "S3IntegrationContext is not initialized");
    }
}
