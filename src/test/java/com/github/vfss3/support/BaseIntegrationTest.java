package com.github.vfss3.support;

import static com.amazonaws.services.s3.model.ownership.ObjectOwnership.ObjectWriter;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.github.vfss3.S3FileSystemOptions;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for integration tests. Uses a shared LocalStack container (started once per JVM)
 * to provide a self-contained S3-compatible endpoint — no external credentials or network access needed.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public abstract class BaseIntegrationTest {
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.4");

    /** Shared container — started once for the entire test run. */
    static final LocalStackContainer localstack = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(S3);

    static {
        localstack.start();
    }

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected FileObject root;
    protected S3FileSystemOptions options;

    @BeforeAll
    public final void initBucket() throws IOException {
        this.options = new S3FileSystemOptions();

        // Use LocalStack's built-in test credentials
        options.setCredentialsProvider(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())));

        // LocalStack uses HTTP — disable HTTPS
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);
        options.setCreateBucket(true);
        options.setObjectOwnership(ObjectWriter);

        // Unique bucket name per test class run (3 random hex segments, lowercase S3-compatible)
        var endpoint = localstack.getEndpointOverride(S3);
        String token =
                new Random().ints(3).mapToObj(i -> String.format("%08x", i)).collect(joining());
        String bucketName = "vfs3-tests-" + token;

        // Build URL in custom-endpoint (path-style) format: s3://host:port/bucket/
        String url = String.format("s3://%s:%d/%s/", endpoint.getHost(), endpoint.getPort(), bucketName);

        log.info("Integration test bucket URL: {}", url);

        final FileSystemManager manager = VFS.getManager();
        this.root = manager.resolveFile(url, options.toFileSystemOptions());
    }

    @AfterAll
    public final void deleteBucket() throws IOException {
        if (root != null) {
            root.refresh();
            root.deleteAll();
        }
    }

    /**
     * Local binary file for doing tests.
     */
    public FileObject binaryFile() throws FileSystemException {
        File backupFile = new File("src/test/resources/backup.zip");

        assertTrue(backupFile.exists(), "Backup file should exist");

        return VFS.getManager().resolveFile(backupFile.getAbsolutePath());
    }

    /**
     * Returns path to a large file for upload tests.
     * Downloads a small netboot ISO from the Ubuntu archive.
     */
    public FileObject bigFile() throws FileSystemException {
        return VFS.getManager()
                .resolveFile(
                        "http://archive.ubuntu.com/ubuntu/dists/xenial-updates/main/installer-i386/current/images/netboot/mini.iso");
    }
}
