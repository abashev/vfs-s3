package com.github.vfss3.spring.local;

import com.github.vfss3.spring.SpringIntegrationContext;
import java.net.URI;
import java.time.Duration;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.spring.tests} against a freshly-started Ceph
 * container — a single-node cluster (MON, MGR, OSD) with the RADOS Gateway, Ceph's S3 front end,
 * on top of it. See {@code modules/jdk}'s {@code local.CephSuite} for why each environment
 * variable below is needed, why the wait strategy keys off a log line, and why the image cannot
 * run on an arm64 host.
 */
@SelectPackages("com.github.vfss3.spring.tests")
@Suite
@SuiteDisplayName("Ceph integration tests")
@SuppressWarnings("NullAway")
public class CephSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("quay.io/ceph/demo:latest-squid");
    private static final int RGW_PORT = 8080;
    private static final String ACCESS_KEY = "vfs-s3-tests";
    private static final String SECRET_KEY = "vfs-s3-tests-secret";

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() {
        container = new GenericContainer<>(IMAGE)
                // Mandatory for the demo entrypoint; everything is reached through the published
                // RGW port, so loopback is enough.
                .withEnv("MON_IP", "127.0.0.1")
                .withEnv("CEPH_PUBLIC_NETWORK", "0.0.0.0/0")
                // MON and MGR are always bootstrapped — this drops MDS, NFS, rbd-mirror and crash.
                .withEnv("DEMO_DAEMONS", "osd rgw")
                .withEnv("CEPH_DEMO_UID", "vfs-s3")
                .withEnv("CEPH_DEMO_ACCESS_KEY", ACCESS_KEY)
                .withEnv("CEPH_DEMO_SECRET_KEY", SECRET_KEY)
                .withEnv("RGW_NAME", "localhost")
                // Keeps the monitor from refusing to start on a nearly-full Docker VM disk.
                .withEnv("CEPH_ARGS", "--mon-data-avail-crit=0 --mon-data-avail-warn=0")
                .withExposedPorts(RGW_PORT)
                // RGW listens before the demo user is created, so waiting on the port would race.
                .waitingFor(Wait.forLogMessage(".*bin/demo: SUCCESS.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
        container.start();

        var endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(RGW_PORT));
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
