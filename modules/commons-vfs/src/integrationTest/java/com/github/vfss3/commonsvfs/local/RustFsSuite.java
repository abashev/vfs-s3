package com.github.vfss3.commonsvfs.local;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import com.github.vfss3.commonsvfs.S3IntegrationContext;
import com.github.vfss3.commonsvfs.parser.PlatformFeaturesImpl;
import java.net.URI;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against a freshly-started
 * RustFS container.
 */
@SelectPackages("com.github.vfss3.commonsvfs.tests")
@Suite
@SuiteDisplayName("RustFS integration tests")
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
                StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)));
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);
        options.setPlatformFeatures(new PlatformFeaturesImpl(true, true, false, true, false));

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
