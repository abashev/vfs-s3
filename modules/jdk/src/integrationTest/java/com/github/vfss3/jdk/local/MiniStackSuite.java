package com.github.vfss3.jdk.local;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.net.URI;
import java.util.Map;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.jdk.tests} against a freshly-started MiniStack
 * container — a free MIT-licensed drop-in replacement for LocalStack
 * (https://github.com/Nahuel990/ministack), mirroring {@code modules/commons-vfs}'s
 * {@code local.MiniStackSuite}.
 */
@Suite
@SuiteDisplayName("MiniStack integration tests")
@SelectPackages("com.github.vfss3.jdk.tests")
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
        var env = Map.<String, Object>of("aws.region", "us-east-1", "aws.credentialsProvider", credentialsProvider);

        JdkIntegrationContext.initialize(endpoint, env);
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
