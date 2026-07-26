package com.github.vfss3.spring;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.net.URI;
import java.util.Random;
import org.springframework.lang.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * Process-wide data holder shared between a backend-specific {@code @Suite} (MinIO, LocalStack,
 * …) and the scenario test classes in {@code com.github.vfss3.spring.tests}.
 *
 * <p>The suite calls an {@code initialize} overload from {@code @BeforeSuite} and {@link #reset()}
 * from {@code @AfterSuite}. Test classes read {@link #loader()} / {@link #location(String)} and
 * clean their key prefix with {@link #deletePrefix(String)} — the Spring {@code Resource} API has
 * no delete operation, so cleanup goes through a raw {@link S3Client}.
 */
public final class SpringIntegrationContext {

    @Nullable
    private static S3ResourcePatternResolver loader;

    @Nullable
    private static S3Client cleanupClient;

    @Nullable
    private static String bucket;

    private SpringIntegrationContext() {}

    /**
     * Create a throwaway, randomly-named bucket on the given endpoint and build the loader the
     * test classes use. Used by the local container suites (MinIO, LocalStack, …).
     */
    public static void initialize(URI endpoint, AwsCredentialsProvider credentialsProvider) {
        requireNonNull(endpoint, "endpoint");
        requireNonNull(credentialsProvider, "credentialsProvider");

        // The fixed region keeps the suites hermetic — without it the SDK's default chain
        // could pick up the developer's ~/.aws region, and Garage validates the signing region.
        var config = S3ClientConfig.defaults()
                .withRegion("us-east-1")
                .withEndpoint(endpoint)
                .withCredentialsProvider(credentialsProvider);
        initialize("vfs3-tests-" + randomBucketToken(), config);
    }

    /**
     * Create a bucket with the given (already-decided) name and build the loader against the
     * given config. Used by the remote suite, where the bucket name is derived from
     * {@code BASE_URL} rather than generated fresh with a fixed prefix.
     */
    public static void initialize(String bucketName, S3ClientConfig config) {
        requireNonNull(bucketName, "bucketName");
        requireNonNull(config, "config");

        cleanupClient = config.buildS3Client();
        cleanupClient.createBucket(b -> b.bucket(bucketName));
        bucket = bucketName;
        loader = new S3ResourcePatternResolver(config);
    }

    /** Close the loader and the cleanup client. The container itself is stopped by the suite. */
    public static void reset() {
        if (loader != null) {
            loader.close();
            loader = null;
        }
        if (cleanupClient != null) {
            cleanupClient.close();
            cleanupClient = null;
        }
        bucket = null;
    }

    /** The loader under test — also a {@code ResourcePatternResolver} for wildcard scenarios. */
    public static S3ResourcePatternResolver loader() {
        return requireNonNull(
                loader, "SpringIntegrationContext is not initialized — must run inside a configured @Suite");
    }

    /** The full {@code s3://} location for a key in the suite's bucket. */
    public static String location(String key) {
        return "s3://" + requireNonNull(bucket, "SpringIntegrationContext is not initialized") + "/" + key;
    }

    /** Deletes every object under the given key prefix — cleanup between scenarios. */
    public static void deletePrefix(String keyPrefix) {
        var client = requireNonNull(cleanupClient, "SpringIntegrationContext is not initialized");
        var bucketName = requireNonNull(bucket, "SpringIntegrationContext is not initialized");
        for (var page : client.listObjectsV2Paginator(b -> b.bucket(bucketName).prefix(keyPrefix))) {
            var keys = page.contents().stream()
                    .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                    .toList();
            if (!keys.isEmpty()) {
                client.deleteObjects(b -> b.bucket(bucketName).delete(d -> d.objects(keys)));
            }
        }
    }

    /**
     * Random lowercase hex token suitable for embedding in a per-run S3 bucket name. Also used
     * by {@code com.github.vfss3.spring.remote.EnvironmentBasedSuite} for ad-hoc local runs where
     * CI's deterministic {@code BUCKET_TOKEN} isn't set.
     */
    public static String randomBucketToken() {
        return new Random().ints(3).mapToObj(i -> String.format("%08x", i)).collect(joining());
    }
}
