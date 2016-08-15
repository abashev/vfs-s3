package com.intridea.io.vfs.support;

import com.intridea.io.vfs.provider.s3.S3FileSystemOptions;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class TestEnvironment {
    private static final String BUCKET_PARAMETER = "AWS_TEST_BUCKET";

    private final String bucket;
    private final FileSystemManager fileSystemManager;

    TestEnvironment(FileSystemManager fileSystemManager) {
        this.fileSystemManager = fileSystemManager;

        this.bucket = requireNonNull(
                (new EnvironmentConfiguration()).get(BUCKET_PARAMETER),
                BUCKET_PARAMETER + " should present in environment configuration"
        );
    }

    /**
     * Shorcut for resolving files inside test bucket.
     *
     * @param path
     * @param args
     * @return
     * @throws FileSystemException
     */
    public FileObject resolveFile(String path, Object ... args) throws FileSystemException {
        return fileSystemManager.resolveFile("s3://" + bucket + String.format(path, args));
    }

    /**
     * Resolve file with custom options.
     *
     * @param options
     * @param path
     * @param args
     * @return
     * @throws FileSystemException
     */
    public FileObject resolveFile(S3FileSystemOptions options, String path, Object ... args) throws FileSystemException {
        return fileSystemManager.resolveFile("s3://" + bucket + String.format(path, args), options.toFileSystemOptions());
    }

    /**
     * Returns bucket name as is
     * @return
     */
    public String bucketName() {
        return bucket;
    }

    /**
     * Local binary file for doing tests.
     *
     * @return
     */
    public String binaryFile() {
        return "src/test/resources/backup.zip";
    }
}
