package com.github.vfss3.jdk.local;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.jdk.tests} against Adobe S3Mock
 * (https://github.com/adobe/S3Mock) — Apache-2.0, ~96 MB image, no auth required — mirroring
 * {@code modules/commons-vfs}'s {@code local.S3MockSuite}.
 */
@Suite
@SuiteDisplayName("Adobe S3Mock integration tests")
@SelectPackages("com.github.vfss3.jdk.tests")
public class S3MockSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("adobe/s3mock:5.0.0");
    private static final int HTTP_PORT = 9090;

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE).withExposedPorts(HTTP_PORT).waitingFor(Wait.forListeningPort());
        container.start();

        // S3Mock accepts any credentials.
        var credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

        var endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT));
        JdkIntegrationContext.initialize(endpoint, credentialsProvider);
    }

    @AfterSuite
    static void stopContainer() throws Exception {
        try {
            JdkIntegrationContext.reset();
        } finally {
            if (container != null) {
                container.stop();
                container = null;
            }
        }
    }
}
