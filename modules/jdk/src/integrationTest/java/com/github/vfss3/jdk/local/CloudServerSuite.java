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
 * Runs every test in {@code com.github.vfss3.jdk.tests} against a freshly-started Zenko
 * CloudServer container with the in-memory backend, mirroring {@code modules/commons-vfs}'s
 * {@code local.CloudServerSuite}.
 *
 * <p><b>Known limitation:</b> the {@code zenko/cloudserver} image is amd64-only (single-arch
 * manifest, no arm64 build). Under QEMU emulation on an arm64 host (e.g. Apple Silicon) its KMS
 * wrapper fails to load ({@code MODULE_NOT_FOUND}) and every S3 call returns a 500. CI runs on
 * amd64 (ubuntu-latest) where the image runs natively and this suite is expected to pass.
 */
@Suite
@SuiteDisplayName("CloudServer integration tests")
@SelectPackages("com.github.vfss3.jdk.tests")
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

        var endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(API_PORT));
        var credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));

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
