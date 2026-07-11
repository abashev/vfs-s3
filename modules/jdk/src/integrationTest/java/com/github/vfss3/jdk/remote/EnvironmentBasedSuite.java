package com.github.vfss3.jdk.remote;

import com.github.vfss3.jdk.JdkIntegrationContext;
import com.github.vfss3.jdk.S3FileSystemConfig;
import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;
import org.opentest4j.TestAbortedException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Runs every test in {@code com.github.vfss3.jdk.tests} against a real remote S3-compatible
 * service (AWS S3, Yandex Object Storage, …) configured through environment variables. Run via
 * {@code ./gradlew :modules:jdk:integrationTest --tests
 * "com.github.vfss3.jdk.remote.EnvironmentBasedSuite"}.
 *
 * <p>Reuses the exact same {@code BASE_URL} / {@code BUCKET_TOKEN} / {@code AWS_ACCESS_KEY_ID} /
 * {@code AWS_SECRET_KEY} environment variables as {@code
 * com.github.vfss3.commonsvfs.remote.EnvironmentBasedSuite} — see that class's javadoc for the
 * variable reference. {@link RemoteEndpoint} parses {@code BASE_URL}'s host into a bucket name,
 * region, and (if the host isn't AWS's) an endpoint override, since — unlike commons-vfs — this
 * module's {@code S3FileSystemProvider} addresses a bucket purely by name (the {@code s3://}
 * FileSystem URI's host), not by a full virtual-hosted-style domain.
 *
 * <p>The suite creates the bucket on startup and deletes it together with all its contents in
 * {@code @AfterSuite}, via a raw {@code S3Client} (not through the provider under test).
 */
@Suite
@SuiteDisplayName("Remote S3 integration tests (environment-configured)")
@SelectPackages("com.github.vfss3.jdk.tests")
public class EnvironmentBasedSuite {
    static final String ENV_BASE_URL = "BASE_URL";
    static final String ENV_BUCKET_TOKEN = "BUCKET_TOKEN";

    private static String bucket;
    private static S3Client cleanupClient;

    @BeforeSuite
    static void initialize() {
        var urlTemplate = System.getenv(ENV_BASE_URL);
        if (urlTemplate == null || urlTemplate.isBlank()) {
            throw new TestAbortedException(
                    "EnvironmentBasedSuite requires the " + ENV_BASE_URL + " environment variable");
        }

        var credentialsProvider = DefaultCredentialsProvider.create();
        try {
            credentialsProvider.resolveCredentials();
        } catch (Exception e) {
            throw new TestAbortedException("EnvironmentBasedSuite — DefaultCredentialsProvider couldn't resolve AWS"
                    + " credentials. Set AWS_ACCESS_KEY_ID + AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY), or"
                    + " configure ~/.aws/credentials / instance profile. " + e.getMessage());
        }

        // CI passes a deterministic token (commit SHA + module discriminator) so the standalone
        // RemoteBucketCleaner can target the same bucket; locally we fall back to a random one.
        var token = System.getenv(ENV_BUCKET_TOKEN);
        if (token == null || token.isBlank()) {
            token = JdkIntegrationContext.randomBucketToken();
        }

        var remote = RemoteEndpoint.resolve(urlTemplate, token);
        bucket = remote.bucket();

        var env = remote.toEnv(credentialsProvider);
        cleanupClient = S3FileSystemConfig.fromEnv(env).buildS3Client();

        System.out.println("EnvironmentBasedSuite — provisioning bucket " + bucket);
        JdkIntegrationContext.initialize(bucket, env);
    }

    @AfterSuite
    static void tearDown() {
        try {
            JdkIntegrationContext.reset();
        } catch (Exception e) {
            System.out.println("EnvironmentBasedSuite — failed to close file system cleanly: " + e);
        }
        try {
            RemoteBucketCleaner.deleteBucketAndContents(cleanupClient, bucket);
            System.out.println("EnvironmentBasedSuite — torn down bucket " + bucket);
        } catch (Exception e) {
            System.out.println("EnvironmentBasedSuite — failed to tear down bucket " + bucket + " cleanly: " + e);
        } finally {
            cleanupClient.close();
            cleanupClient = null;
            bucket = null;
        }
    }
}
