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
 * MinIO container. Uses {@link GenericContainer} because the bundled testcontainers
 * version pre-dates the dedicated {@code MinIOContainer} module — once we bump
 * testcontainers we can swap to that.
 */
@Suite
@SuiteDisplayName("MinIO integration tests")
@SelectPackages("com.github.vfss3.commonsvfs.tests")
public class MinioSuite {
    private static final DockerImageName MINIO_IMAGE =
            DockerImageName.parse("minio/minio:RELEASE.2025-09-07T16-13-09Z");
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final int API_PORT = 9000;

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(MINIO_IMAGE)
                .withCommand("server", "/data")
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                .withExposedPorts(API_PORT)
                .waitingFor(Wait.forHttp("/minio/health/live").forPort(API_PORT));
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
