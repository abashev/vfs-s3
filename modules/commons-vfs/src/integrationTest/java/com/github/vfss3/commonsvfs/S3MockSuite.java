package com.github.vfss3.commonsvfs;

import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against Adobe S3Mock
 * (https://github.com/adobe/S3Mock) — Apache-2.0, ~96 MB image, no auth required.
 */
@Suite
@SuiteDisplayName("Adobe S3Mock integration tests")
@SelectPackages("com.github.vfss3.commonsvfs.tests")
public class S3MockSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("adobe/s3mock:5.0.0");
    private static final int HTTP_PORT = 9090;

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE).withExposedPorts(HTTP_PORT).waitingFor(Wait.forListeningPort());
        container.start();

        // S3Mock accepts any credentials.
        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);

        URI endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT));
        S3IntegrationContext.initialize(endpoint, options);
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
