package com.github.vfss3.commonsvfs.local;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import com.github.vfss3.commonsvfs.S3IntegrationContext;
import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against a freshly-started
 * SeaweedFS container in S3 gateway mode.
 */
@Suite
@SuiteDisplayName("SeaweedFS integration tests")
@SelectPackages("com.github.vfss3.commonsvfs.tests")
public class SeaweedFsSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("chrislusf/seaweedfs:4.22");
    private static final int S3_PORT = 8333;

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE)
                .withCommand("server", "-s3", "-dir=/data")
                .withExposedPorts(S3_PORT)
                .waitingFor(Wait.forListeningPort());
        container.start();

        // SeaweedFS S3 API runs anonymous by default — any access key is accepted.
        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("any", "any")));
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);

        URI endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT));
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
