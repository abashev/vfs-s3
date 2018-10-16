package com.github.vfss3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
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

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {
    final static Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(
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

    private final S3FileNameParser parser = new S3FileNameParser();

    public S3FileProvider() {
        setFileNameParser(parser);
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
        final S3FileName file = (S3FileName) fileName;
        final S3FileSystemOptions options = new S3FileSystemOptions(fileSystemOptions);

        ClientConfiguration clientConfiguration = options.getClientConfiguration();

        final AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard().
                withClientConfiguration(clientConfiguration).
                withCredentials(new DefaultAWSCredentialsProviderChain());

        if (options.isDisableChunkedEncoding()) {
            clientBuilder.disableChunkedEncoding();
        }

        clientBuilder.enablePathStyleAccess();

        StringBuilder endpoint = new StringBuilder();

        if (options.isUseHttps()) {
            endpoint.append("https://");
        } else {
            endpoint.append("http://");
        }

        endpoint.append(file.getHostAndPort());


        clientBuilder.withEndpointConfiguration(new EndpointConfiguration(
                endpoint.toString(),
                parser.regionFromHost(file.getHostAndPort(), "us-east-1")
        ));

        final String bucket = file.getPathPrefix();

        return (new S3FileSystem(bucket, file, clientBuilder.build(), options));
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
