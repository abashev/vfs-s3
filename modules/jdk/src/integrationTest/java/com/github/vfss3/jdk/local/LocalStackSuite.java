package com.github.vfss3.jdk.local;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.util.Map;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.jdk.tests} against a freshly-started LocalStack
 * container, mirroring {@code modules/commons-vfs}'s {@code local.LocalStackSuite}.
 */
@Suite
@SuiteDisplayName("LocalStack integration tests")
@SelectPackages("com.github.vfss3.jdk.tests")
public class LocalStackSuite {
    // testcontainers 1.19.8's LocalStackContainer wait strategy expects the older "Ready." log
    // line, which LocalStack dropped in the 4.x / 2026.x stream. Stay on the latest 3.x
    // major.minor until testcontainers is bumped (same pin as modules/commons-vfs).
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.8");

    private static LocalStackContainer container;

    @BeforeSuite
    static void startContainer() {
        container = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(S3);
        container.start();

        var credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey()));
        var env = Map.<String, Object>of("aws.region", "us-east-1", "aws.credentialsProvider", credentialsProvider);

        JdkIntegrationContext.initialize(container.getEndpointOverride(S3), env);
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
