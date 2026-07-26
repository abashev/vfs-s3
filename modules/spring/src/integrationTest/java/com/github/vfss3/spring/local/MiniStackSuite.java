package com.github.vfss3.spring.local;

import com.github.vfss3.spring.SpringIntegrationContext;
import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.spring.tests} against a freshly-started MiniStack
 * container — a free MIT-licensed drop-in replacement for LocalStack
 * (https://github.com/Nahuel990/ministack), mirroring {@code modules/commons-vfs}'s
 * {@code local.MiniStackSuite}.
 */
@SelectPackages("com.github.vfss3.spring.tests")
@Suite
@SuiteDisplayName("MiniStack integration tests")
@SuppressWarnings("NullAway")
public class MiniStackSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("ministackorg/ministack:1.3");
    private static final int API_PORT = 4566;
    // MiniStack accepts the same default credentials LocalStack used.
    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE).withExposedPorts(API_PORT).waitingFor(Wait.forListeningPort());
        container.start();

        var endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(API_PORT));
        var credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));

        SpringIntegrationContext.initialize(endpoint, credentialsProvider);
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
