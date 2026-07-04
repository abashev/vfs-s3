package com.github.vfss3.jdk;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
     * Create a throwaway bucket on the given endpoint, open a real {@link FileSystem} against
     * it, and store it for the test classes to read.
     */
    public static void initialize(URI endpoint, Map<String, ?> env) {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(env, "env");

        var fullEnv = new HashMap<String, Object>(env);
        fullEnv.put("aws.endpoint", endpoint.toString());
        var config = S3FileSystemConfig.fromEnv(fullEnv);

        var bucket = "vfs3-tests-" + randomBucketToken();
        try (var client = config.buildS3Client()) {
            client.createBucket(b -> b.bucket(bucket));
        }

        var uri = URI.create("s3://" + bucket);
        try {
            fileSystem = FileSystems.newFileSystem(uri, fullEnv);
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
        return Objects.requireNonNull(
                fileSystem, "JdkIntegrationContext is not initialized — must run inside a configured @Suite");
    }

    private static String randomBucketToken() {
        return new Random().ints(3).mapToObj(i -> String.format("%08x", i)).collect(Collectors.joining());
    }
}
