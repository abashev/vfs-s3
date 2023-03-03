package com.github.vfss3.support;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.github.vfss3.S3FileSystemOptions;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;
import static java.lang.Boolean.parseBoolean;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertTrue;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public abstract class BaseIntegrationTest {
    protected final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

    private static final String BASE_URL = "BASE_URL";
    private static final String USE_HTTP = "USE_HTTPS";
    private static final String DISABLE_CHUNKED_ENCODING = "DISABLE_CHUNKED_ENCODING";

    protected FileObject root;
    protected S3FileSystemOptions options;

    @BeforeClass
    public final void initBucket() throws IOException {
        this.options = new S3FileSystemOptions();

        EnvironmentConfiguration configuration = new EnvironmentConfiguration();

        // Try to load access and secret key from environment
        AWSCredentials awsCredentials = null;

        try {
            awsCredentials = (new EnvironmentVariableCredentialsProvider()).getCredentials();
        } catch (AmazonClientException e) {
            log.info("Not able to load credentials from environment - try .envrc file");
        }

        if (awsCredentials != null) {
            log.info("Will use AWS credentials from environment variables");
        } else {
            configuration.computeIfPresent(
                    ACCESS_KEY_ENV_VAR,
                    SECRET_KEY_ENV_VAR,
                    (access, secret) -> options.setCredentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials(access, secret))));
        }

        configuration.computeIfPresent(USE_HTTP, v -> options.setUseHttps(parseBoolean(v)));
        configuration.computeIfPresent(DISABLE_CHUNKED_ENCODING, v -> options.setDisableChunkedEncoding(parseBoolean(v)));

        options.setCreateBucket(true);

        String token = (new Random()).ints(3).mapToObj(Integer::toHexString).collect(joining());

        String baseUrl = configuration.get(BASE_URL).
                orElseThrow(() -> new IllegalStateException(BASE_URL + " should present in environment configuration"));

        assertTrue(baseUrl.contains("%s"), "BASE_URL should contain placeholder for token");

        final FileSystemManager manager = VFS.getManager();

        this.root = manager.resolveFile(String.format(baseUrl, token), options.toFileSystemOptions());
    }

    @AfterClass
    public final void deleteBucket() throws IOException {
        if (root != null) {
            root.refresh();
            root.deleteAll();
        }
    }

    /**
     * Local binary file for doing tests.
     *
     * @return
     */
    public FileObject binaryFile() throws FileSystemException {
        File backupFile = new File("src/test/resources/backup.zip");

        assertTrue(backupFile.exists(), "Backup file should exists");

        return VFS.getManager().resolveFile(backupFile.getAbsolutePath());
    }

    /**
     * Returns path to big file for doing upload test.
     *
     * @return
     */
    public FileObject bigFile() throws FileSystemException {
        return VFS.getManager().resolveFile(
                "http://archive.ubuntu.com/ubuntu/dists/xenial-updates/main/installer-i386/current/images/netboot/mini.iso"
        );
    }
}
