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
 * Zenko CloudServer container with the in-memory backend.
 */
@SelectPackages("com.github.vfss3.commonsvfs.tests")
@Suite
@SuiteDisplayName("CloudServer integration tests")
public class CloudServerSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("zenko/cloudserver:latest-7.70.10");
    private static final int API_PORT = 8000;
    // Hardcoded in /conf/authdata.json baked into the image.
    private static final String ACCESS_KEY = "accessKey1";
    private static final String SECRET_KEY = "verySecretKey1";

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE)
                .withEnv("S3BACKEND", "mem")
                .withEnv("REMOTE_MANAGEMENT_DISABLE", "1")
                .withEnv("ENDPOINT", "localhost")
                .withExposedPorts(API_PORT)
                .waitingFor(Wait.forListeningPort());
        container.start();

        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)));
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
