package com.github.vfss3.jdk;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Process-wide data holder shared between a backend-specific {@code @Suite} (MinIO, LocalStack,
 * …) and the scenario test classes in {@link com.github.vfss3.jdk.tests}.
 *
 * <p>The suite calls {@link #initialize(URI, Map)} from {@code @BeforeSuite} and {@link #reset()}
 * from {@code @AfterSuite}. Test classes read {@link #fileSystem()}.
 */
public final class JdkIntegrationContext {

    private static FileSystem fileSystem;

    private JdkIntegrationContext() {}

    /**
     * Create a throwaway, randomly-named bucket on the given endpoint, open a real {@link
     * FileSystem} against it, and store it for the test classes to read. Used by the local
     * container suites (MinIO, LocalStack, …).
     */
    public static void initialize(URI endpoint, Map<String, ?> env) {
        requireNonNull(endpoint, "endpoint");
        requireNonNull(env, "env");

        var fullEnv = new HashMap<String, Object>(env);
        fullEnv.put("aws.endpoint", endpoint.toString());

        createBucketAndOpen("vfs3-tests-" + randomBucketToken(), fullEnv);
    }

    /**
     * Create a bucket with the given (already-decided) name, open a real {@link FileSystem}
     * against it, and store it for the test classes to read. Used by the remote suite, where the
     * bucket name is derived from {@code BASE_URL} rather than generated fresh with a fixed
     * prefix.
     */
    public static void initialize(String bucket, Map<String, ?> env) {
        requireNonNull(bucket, "bucket");
        requireNonNull(env, "env");

        createBucketAndOpen(bucket, env);
    }

    private static void createBucketAndOpen(String bucket, Map<String, ?> env) {
        var config = S3FileSystemConfig.fromEnv(env);
        try (var client = config.buildS3Client()) {
            client.createBucket(b -> b.bucket(bucket));
        }

        var uri = URI.create("s3://" + bucket);
        try {
            fileSystem = FileSystems.newFileSystem(uri, env);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open S3 file system for " + uri, e);
        }
    }

    /** Close the file system. The container itself is stopped by the suite. */
    public static void reset() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
            fileSystem = null;
        }
    }

    public static FileSystem fileSystem() {
        return requireNonNull(
                fileSystem, "JdkIntegrationContext is not initialized — must run inside a configured @Suite");
    }

    /**
     * Random lowercase hex token suitable for embedding in a per-run S3 bucket name. Also used
     * by {@code com.github.vfss3.jdk.remote.EnvironmentBasedSuite} for ad-hoc local runs where
     * CI's deterministic {@code BUCKET_TOKEN} isn't set.
     */
    public static String randomBucketToken() {
        return new Random().ints(3).mapToObj(i -> String.format("%08x", i)).collect(Collectors.joining());
    }
}
