package com.intridea.io.vfs.provider.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {

    public final static Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(
        Capability.CREATE,
        Capability.DELETE,
        Capability.GET_TYPE,
        Capability.GET_LAST_MODIFIED,
        Capability.SET_LAST_MODIFIED_FILE,
        Capability.SET_LAST_MODIFIED_FOLDER,
        Capability.LIST_CHILDREN,
        Capability.READ_CONTENT,
        Capability.URI,
        Capability.WRITE_CONTENT
    ));

    /**
     * Default options for S3 file system.
     */
    private static FileSystemOptions defaultOptions = new FileSystemOptions();

    /**
     * Returns default S3 file system options.
     * Use it to set AWS auth credentials.
     * @return default S3 file system options
     */
    public static FileSystemOptions getDefaultFileSystemOptions() {
        return defaultOptions;
    }

    /**
     * Logger instance
     */
    private final Log logger = LogFactory.getLog(S3FileProvider.class);

    public S3FileProvider() {
        super();
        setFileNameParser(S3FileNameParser.getInstance());
    }

    /**
     * Create a file system with the S3 root provided.
     *
     * @param fileName the S3 file name that defines the root (bucket)
     * @param fileSystemOptions file system options
     * @return an S3 file system
     * @throws FileSystemException if the file system cannot be created
     */
    @Override
    protected FileSystem doCreateFileSystem(
            FileName fileName, FileSystemOptions fileSystemOptions
    ) throws FileSystemException {
        final FileSystemOptions fsOptions = (fileSystemOptions != null) ? fileSystemOptions : getDefaultFileSystemOptions();
        final S3FileSystemConfigBuilder config = S3FileSystemConfigBuilder.getInstance();

        final AWSCredentials awsCredentials = config.getAWSCredentials(fsOptions);

        AmazonS3Client service = config.getAmazonS3Client(fsOptions);

        if (service == null) {
            ClientConfiguration clientConfiguration = config.getClientConfiguration(fsOptions);

            service = new AmazonS3Client(awsCredentials, clientConfiguration);

            String endpoint = config.getEndpoint(fsOptions);
            if (endpoint != null) {
                service.setEndpoint(endpoint);
            } else {

                Region region = config.getRegion(fsOptions);

                if (region != null) {
                    service.setRegion(region.toAWSRegion());
                }
            }
        }

        S3FileSystem fileSystem = new S3FileSystem((S3FileName) fileName, service, fsOptions);

        if (config.getAmazonS3Client(fsOptions) == null) {
            fileSystem.setShutdownServiceOnClose(true);
        }

        return fileSystem;
    }

    /**
     * Get the capabilities of the file system provider.
     *
     * @return the file system capabilities
     */
    @Override
    public Collection<Capability> getCapabilities() {
        return capabilities;
    }
}
