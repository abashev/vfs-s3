package com.github.vfss3.spring.local;

import com.github.vfss3.spring.SpringIntegrationContext;
import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.spring.tests} against a freshly-started SeaweedFS
 * container in S3 gateway mode, mirroring {@code modules/commons-vfs}'s
 * {@code local.SeaweedFsSuite}.
 */
@SelectPackages("com.github.vfss3.spring.tests")
@Suite
@SuiteDisplayName("SeaweedFS integration tests")
@SuppressWarnings("NullAway")
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

        // SeaweedFS S3 API runs anonymous by default without an identity config file — it
        // rejects signed (SigV4) requests outright, so send unsigned requests instead of fake
        // static credentials.
        var endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT));
        SpringIntegrationContext.initialize(endpoint, AnonymousCredentialsProvider.create());
    }

    @AfterSuite
    static void stopContainer() throws Exception {
        try {
            SpringIntegrationContext.reset();
        } finally {
            if (container != null) {
                container.stop();
                container = null;
            }
        }
    }
}
