package com.github.vfss3.commonsvfs.local;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import com.github.vfss3.commonsvfs.S3IntegrationContext;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against a freshly-started
 * LocalStack container. The container is booted in {@link #startContainer()} and stopped in
 * {@link #stopContainer()} once the entire suite finishes.
 */
@SelectPackages("com.github.vfss3.commonsvfs.tests")
@Suite
@SuiteDisplayName("LocalStack integration tests")
@SuppressWarnings("NullAway")
public class LocalStackSuite {
    // testcontainers 1.19.8's LocalStackContainer wait strategy expects the older
    // "Ready." log line, which LocalStack dropped in the 4.x / 2026.x stream. Stay
    // on the latest 3.x major.minor until testcontainers is bumped.
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.8");

    private static LocalStackContainer container;

    @BeforeSuite
    static void startContainer() {
        container = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(S3);
        container.start();

        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())));
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);
        options.setObjectOwnership(ObjectOwnership.OBJECT_WRITER);

        S3IntegrationContext.initialize(container.getEndpointOverride(S3), options);
    }

    @AfterSuite
    static void stopContainer() {
        try {
            S3IntegrationContext.reset();
        } finally {
            if (container != null) {
                container.stop();
                container = null;
            }
        }
    }
}
