package com.github.vfss3.commonsvfs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against a freshly-started
 * RustFS container.
 */
@Suite
@SuiteDisplayName("RustFS integration tests")
@SelectPackages("com.github.vfss3.commonsvfs.tests")
public class RustFsSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("rustfs/rustfs:1.0.0-alpha.99");
    private static final int API_PORT = 9000;
    private static final String ACCESS_KEY = "rustfsadmin";
    private static final String SECRET_KEY = "rustfsadmin";

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE)
                .withEnv("RUSTFS_ROOT_USER", ACCESS_KEY)
                .withEnv("RUSTFS_ROOT_PASSWORD", SECRET_KEY)
                .withExposedPorts(API_PORT)
                .waitingFor(Wait.forListeningPort());
        container.start();

        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)));
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);

        URI endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(API_PORT));
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
