package com.github.vfss3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static com.amazonaws.regions.Regions.DEFAULT_REGION;

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {
    /**
     * Protocol prefix
     */
    public static final String PREFIX = "s3";

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

    public S3FileProvider() {
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
        final S3FileSystemOptions options = new S3FileSystemOptions(fileSystemOptions);

        AmazonS3Client service = options.getS3Client().orElseGet(() -> {
            if (DEFAULT_CLIENT != null) {
                return DEFAULT_CLIENT;
            } else {
                ClientConfiguration clientConfiguration = options.getClientConfiguration();

                final AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard().
                        enablePathStyleAccess().
                        withClientConfiguration(clientConfiguration).
                        withCredentials(new DefaultAWSCredentialsProviderChain());

                if (options.isDisableChunkedEncoding()) {
                    clientBuilder.disableChunkedEncoding();
                }

                options.getEndpoint().ifPresent(endpoint -> clientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, DEFAULT_REGION.getName())));
                options.getRegion().ifPresent(clientBuilder::withRegion);

                return (AmazonS3Client) clientBuilder.build();
            }
        });

        S3FileSystem fileSystem = new S3FileSystem((S3FileName) fileName, service, options);

        if (options.getS3Client().isPresent()) {
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

    private static AmazonS3Client DEFAULT_CLIENT = null;

    /**
     * commons-vfs doen't support default options so we have to do something with default S3 client.
     *
     * @param client it will be used in case of no client was specified.
     */
    public static void setDefaultClient(AmazonS3Client client) {
        DEFAULT_CLIENT = client;
    }

    /**
     * Return config builder.
     *
     * @return A config builder for S3FileSystem.
     * @see org.apache.commons.vfs2.provider.AbstractFileProvider#getConfigBuilder()
     */
    @Override
    public FileSystemConfigBuilder getConfigBuilder() {
        return S3FileSystemConfigBuilder.getInstance();
    }
}
