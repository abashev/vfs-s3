package com.github.vfss3.commonsvfs.remote;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import com.github.vfss3.commonsvfs.S3IntegrationContext;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against a real remote
 * S3-compatible service (AWS S3, Yandex Object Storage, DigitalOcean Spaces, …)
 * configured through environment variables. Run via
 * {@code ./gradlew :modules:commons-vfs:remoteIntegrationTest}.
 *
 * <h3>Environment variables</h3>
 *
 * <p>Only the bucket URL is suite-specific; credentials are picked up by the AWS SDK's
 * {@link DefaultCredentialsProvider} chain, so any combination of standard sources works
 * (env vars, system properties, {@code ~/.aws/credentials}, EC2/EKS instance role, …).
 *
 * <table>
 *   <tr><th>Variable</th><th>Required by</th><th>Notes</th></tr>
 *   <tr><td>{@code BASE_URL}</td><td>this suite</td>
 *       <td>{@code printf}-style URL template — the literal {@code %s} is replaced with a
 *           random hex token at suite start so every run targets a fresh, uniquely-named
 *           bucket. Examples:<br>
 *           {@code s3://s3-tests-%s.ams3.digitaloceanspaces.com/}<br>
 *           {@code s3://s3-tests-%s.s3.eu-west-1.amazonaws.com/}<br>
 *           {@code s3://s3-tests-%s.storage.yandexcloud.net/}</td></tr>
 *   <tr><td>{@code AWS_ACCESS_KEY_ID}</td><td>AWS SDK</td>
 *       <td>Picked up automatically by {@code DefaultCredentialsProvider}.</td></tr>
 *   <tr><td>{@code AWS_SECRET_KEY} / {@code AWS_SECRET_ACCESS_KEY}</td><td>AWS SDK</td>
 *       <td>Either name works — SDK v2's
 *           {@code EnvironmentVariableCredentialsProvider} accepts both.</td></tr>
 * </table>
 *
 * <p>The suite creates the unique bucket on startup (via {@code setCreateBucket(true)})
 * and deletes it together with all its contents in {@code @AfterSuite}.
 */
@Suite
@SuiteDisplayName("Remote S3 integration tests (environment-configured)")
@SelectPackages("com.github.vfss3.commonsvfs.tests")
public class EnvironmentBasedSuite {
    static final String ENV_BASE_URL = "BASE_URL";

    private static final Logger log = LoggerFactory.getLogger(EnvironmentBasedSuite.class);

    @BeforeSuite
    static void initialize() {
        String urlTemplate = System.getenv(ENV_BASE_URL);

        if (isBlank(urlTemplate)) {
            throw new TestAbortedException(
                    "EnvironmentBasedSuite requires the " + ENV_BASE_URL + " environment variable");
        }

        if (!urlTemplate.contains("%s")) {
            throw new IllegalStateException(ENV_BASE_URL + " must contain a '%s' placeholder for the random "
                    + "bucket-name token (e.g. s3://s3-tests-%s.s3.amazonaws.com/), got: " + urlTemplate);
        }

        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        try {
            credentialsProvider.resolveCredentials();
        } catch (Exception e) {
            throw new TestAbortedException("EnvironmentBasedSuite — DefaultCredentialsProvider couldn't resolve AWS"
                    + " credentials. Set AWS_ACCESS_KEY_ID + AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY), or"
                    + " configure ~/.aws/credentials / instance profile. " + e.getMessage());
        }

        String bucketUrl = String.format(urlTemplate, S3IntegrationContext.randomBucketToken());

        log.info("EnvironmentBasedSuite — provisioning fresh bucket at {}", bucketUrl);

        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(credentialsProvider);
        options.setCreateBucket(true);
        options.setObjectOwnership(ObjectOwnership.OBJECT_WRITER);

        S3IntegrationContext.initialize(bucketUrl, options);
    }

    @AfterSuite
    static void tearDown() {
        try {
            FileObject root =
                    VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
            root.refresh();
            // deleteAll() removes every object under the root and the bucket itself.
            root.deleteAll();
            log.info("EnvironmentBasedSuite — torn down bucket {}", root);
        } catch (Exception e) {
            log.warn("EnvironmentBasedSuite — failed to tear down bucket cleanly", e);
        } finally {
            S3IntegrationContext.reset();
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
