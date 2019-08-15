package com.intridea.io.vfs.support;

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

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;
import static java.lang.Boolean.parseBoolean;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public abstract class AbstractS3FileSystemTest {
    private final Logger log = LoggerFactory.getLogger(AbstractS3FileSystemTest.class);

    private static final String BUCKET_PARAMETER = "AWS_TEST_BUCKET";
    private static final String USE_HTTP = "USE_HTTPS";
    private static final String CREATE_BUCKET = "CREATE_BUCKET";
    private static final String DISABLE_CHUNKED_ENCODING = "DISABLE_CHUNKED_ENCODING";

    protected FileSystemManager vfs;
    protected FileObject bucket;

    @BeforeClass
    public final void initVFS() throws IOException {
        final S3FileSystemOptions options = new S3FileSystemOptions();
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

        String bucketId = configuration.get(BUCKET_PARAMETER).
                orElseThrow(() -> new IllegalStateException(BUCKET_PARAMETER + " should present in environment configuration"));

        configuration.computeIfPresent(USE_HTTP, v -> options.setUseHttps(parseBoolean(v)));
        configuration.computeIfPresent(CREATE_BUCKET, v -> options.setCreateBucket(parseBoolean(v)));
        configuration.computeIfPresent(DISABLE_CHUNKED_ENCODING, v -> options.setDisableChunkedEncoding(parseBoolean(v)));

        this.vfs = VFS.getManager();
        this.bucket = vfs.resolveFile("s3://" + bucketId, options.toFileSystemOptions());
    }

    @AfterClass
    public final void closeFS() throws IOException {
        if ((vfs != null) && (bucket != null)) {
            vfs.closeFileSystem(bucket.getFileSystem());

            vfs = null;
            bucket = null;
        }
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
