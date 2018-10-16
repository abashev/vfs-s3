package com.intridea.io.vfs.support;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.github.vfss3.S3FileSystemOptions;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
import static java.lang.System.setProperty;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public abstract class AbstractS3FileSystemTest {
    private final Logger log = LoggerFactory.getLogger(AbstractS3FileSystemTest.class);

    private static final String BUCKET_PARAMETER = "AWS_TEST_BUCKET";

    protected FileSystemManager vfs;
    protected FileObject bucket;

    @BeforeClass
    public final void initVFS() throws IOException {
        // Try to load access and secret key from environment
        try {
            if ((new EnvironmentVariableCredentialsProvider()).getCredentials() != null) {
                log.info("Will use AWS credentials from environment variables");
            }
        } catch (AmazonClientException e) {
            log.info("Not able to load credentials from environment - try .envrc file");
        }

        EnvironmentConfiguration configuration = new EnvironmentConfiguration();

        configuration.computeIfPresent(ACCESS_KEY_ENV_VAR, v -> setProperty(ACCESS_KEY_SYSTEM_PROPERTY, v));
        configuration.computeIfPresent(SECRET_KEY_ENV_VAR, v -> setProperty(SECRET_KEY_SYSTEM_PROPERTY, v));

        this.vfs = VFS.getManager();

        String bucketId = configuration.get(BUCKET_PARAMETER).
                orElseThrow(() -> new IllegalStateException(BUCKET_PARAMETER + " should present in environment configuration"));

        this.bucket = vfs.resolveFile("s3://" + bucketId);
    }

    public FileObject resolveFile(String path, Object ... args) throws FileSystemException {
        return bucket.resolveFile(String.format(path, args));
    }

    public FileObject resolveFile(S3FileSystemOptions options, String path, Object ... args) throws FileSystemException {
        return vfs.resolveFile(resolveFile(path, args).getURL().toString(), options.toFileSystemOptions());
    }

    /**
     * Local binary file for doing tests.
     *
     * @return
     */
    public File binaryFile() {
        return new File("src/test/resources/backup.zip");
    }

    /**
     * Returns path to big file for doing upload test.
     *
     * @return
     */
    public String bigFile() {
        return "http://archive.ubuntu.com/ubuntu/dists/xenial-updates/main/installer-i386/current/images/netboot/mini.iso";
    }
}
