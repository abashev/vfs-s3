package com.github.vfss3.jdk.local;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.net.URI;
import java.time.Duration;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.jdk.tests} against a freshly-started Ceph container.
 * Unlike the other local backends this is not an S3 emulator but the real thing — the demo image
 * bootstraps a single-node cluster (MON, MGR, OSD) and puts the RADOS Gateway, Ceph's S3 front
 * end, on top of it.
 *
 * <p><b>Image:</b> {@code quay.io/ceph/demo} is the only single-container Ceph distribution there
 * is. Its {@code ceph/ceph-container} project was archived in December 2024, so the {@code
 * latest-*} tags are frozen rather than rolling and there are no version-pinned tags to use
 * instead. {@code latest-squid} is Ceph 19.2.0; the older {@code latest-reef} / {@code
 * latest-quincy} tags predate parts of the checksum handling AWS SDK 2.30+ sends by default.
 *
 * <p><b>Known limitation:</b> the image is amd64-only, and unlike {@code CloudServerSuite} it does
 * not merely run slowly under emulation on an arm64 host — it cannot run at all. QEMU does not
 * implement {@code io_setup(2)}, so the OSD fails to create its object store, and since Ceph 19
 * dropped the non-AIO block device path ({@code KernelDevice.cc: "non-aio not supported"}) there
 * is no config option that avoids it. Docker Desktop's Rosetta emulation passes the syscall
 * through to the VM kernel and is the way to run this suite on Apple Silicon. CI runs on amd64
 * (ubuntu-latest) where the image runs natively and this suite is expected to pass.
 */
@SelectPackages("com.github.vfss3.jdk.tests")
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
                // The demo entrypoint aborts without both of these. Every daemon lives in this
                // one container and is only ever reached through the published RGW port, so the
                // monitor can bind to loopback and the "public network" can be anything.
                .withEnv("MON_IP", "127.0.0.1")
                .withEnv("CEPH_PUBLIC_NETWORK", "0.0.0.0/0")
                // MON and MGR are always bootstrapped; naming the two daemons the S3 API needs
                // skips MDS, NFS, rbd-mirror and crash, which are pure startup cost here.
                .withEnv("DEMO_DAEMONS", "osd rgw")
                // Creates an RGW user with these credentials once the gateway is up.
                .withEnv("CEPH_DEMO_UID", "vfs-s3")
                .withEnv("CEPH_DEMO_ACCESS_KEY", ACCESS_KEY)
                .withEnv("CEPH_DEMO_SECRET_KEY", SECRET_KEY)
                .withEnv("RGW_NAME", "localhost")
                // The monitor refuses to start when its data filesystem is more than 95% full.
                // That filesystem is the Docker VM's disk, so without this the suite fails on a
                // developer machine that merely has a lot of images cached. A throwaway
                // single-node cluster that stores a few kilobytes has no use for the check.
                .withEnv("CEPH_ARGS", "--mon-data-avail-crit=0 --mon-data-avail-warn=0")
                .withExposedPorts(RGW_PORT)
                // RGW starts listening well before the demo user exists, so waiting on the port
                // would race the user creation and every request would come back 403. SUCCESS is
                // the entrypoint's last line before it hands over to `ceph -w`.
                .waitingFor(Wait.forLogMessage(".*bin/demo: SUCCESS.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
        container.start();

        var endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(RGW_PORT));
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
