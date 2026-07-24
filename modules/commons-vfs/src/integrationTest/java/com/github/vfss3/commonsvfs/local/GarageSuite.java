package com.github.vfss3.commonsvfs.local;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import com.github.vfss3.commonsvfs.S3IntegrationContext;
import com.github.vfss3.commonsvfs.parser.PlatformFeaturesImpl;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.platform.suite.api.*;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Runs every test in {@code com.github.vfss3.commonsvfs.tests} against a freshly-started
 * Garage container. Garage requires post-start bootstrap (layout assignment + S3 key
 * creation) which is done via {@code execInContainer}.
 */
@SelectPackages("com.github.vfss3.commonsvfs.tests")
@Suite
@SuiteDisplayName("Garage integration tests")
@SuppressWarnings("NullAway")
public class GarageSuite {
    private static final DockerImageName IMAGE = DockerImageName.parse("dxflrs/garage:v2.3.0");
    private static final int S3_PORT = 3900;
    private static final int RPC_PORT = 3901;
    private static final int ADMIN_PORT = 3903;

    private static final String CONFIG_TOML = """
        metadata_dir = "/var/lib/garage/meta"
        data_dir = "/var/lib/garage/data"
        db_engine = "sqlite"
        replication_factor = 1
        rpc_bind_addr = "[::]:3901"
        rpc_public_addr = "127.0.0.1:3901"
        rpc_secret = "0000000000000000000000000000000000000000000000000000000000000000"

        [s3_api]
        s3_region = "us-east-1"
        api_bind_addr = "[::]:3900"
        root_domain = ".s3.garage"

        [admin]
        api_bind_addr = "[::]:3903"
        admin_token = "admintoken"
        """;

    private static final Pattern KEY_ID = Pattern.compile("Key ID:\\s+(\\S+)");
    private static final Pattern SECRET = Pattern.compile("Secret key:\\s+(\\S+)");

    private static GenericContainer<?> container;

    @BeforeSuite
    static void startContainer() throws Exception {
        container = new GenericContainer<>(IMAGE)
                .withCopyToContainer(Transferable.of(CONFIG_TOML), "/etc/garage.toml")
                .withExposedPorts(S3_PORT, RPC_PORT, ADMIN_PORT)
                .waitingFor(Wait.forLogMessage(".*S3 API server listening on .*", 1));
        container.start();

        // Bootstrap a single-node layout.
        String nodeId = exec("/garage", "node", "id", "-q").trim().split("@")[0];
        exec("/garage", "layout", "assign", "-z", "dc1", "-c", "1G", nodeId);
        exec("/garage", "layout", "apply", "--version", "1");

        // Create a key with bucket-creation permission so VFS can auto-create the test bucket.
        String keyOutput = exec("/garage", "key", "create", "test-key");
        String accessKey = match(KEY_ID, keyOutput, "Key ID");
        String secretKey = match(SECRET, keyOutput, "Secret key");
        exec("/garage", "key", "allow", "--create-bucket", accessKey);

        S3FileSystemOptions options = new S3FileSystemOptions();
        options.setCredentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);
        options.setPlatformFeatures(new PlatformFeaturesImpl(true, true, false, true, false));

        URI endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT));
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

    private static String exec(String... cmd) throws Exception {
        ExecResult result = container.execInContainer(cmd);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Garage command failed: " + String.join(" ", cmd) + "\nstdout: "
                    + result.getStdout() + "\nstderr: " + result.getStderr());
        }
        return result.getStdout();
    }

    private static String match(Pattern p, String text, String label) {
        Matcher m = p.matcher(text);
        if (!m.find()) {
            throw new IllegalStateException(label + " not found in garage output:\n" + text);
        }
        return m.group(1);
    }
}
