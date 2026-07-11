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
 * Runs every test in {@code com.github.vfss3.jdk.tests} against a freshly-started Floci
 * container — a free open-source local AWS emulator that drops in on the same port as
 * LocalStack (https://github.com/floci-io/floci), mirroring {@code modules/commons-vfs}'s
 * {@code local.FlociSuite}.
 */
@Suite
@SuiteDisplayName("Floci integration tests")
@SelectPackages("com.github.vfss3.jdk.tests")
public class FlociSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("floci/floci:1.5.8");
    private static final int API_PORT = 4566;
    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE).withExposedPorts(API_PORT).waitingFor(Wait.forListeningPort());
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
