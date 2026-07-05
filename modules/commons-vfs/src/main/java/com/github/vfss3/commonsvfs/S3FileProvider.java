package com.github.vfss3.commonsvfs;

import static java.util.Collections.unmodifiableCollection;

import com.github.vfss3.commonsvfs.parser.S3FileNameParser;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileProvider extends CachingFileProvider {
    static final Collection<Capability> capabilities = unmodifiableCollection(Arrays.asList(
            Capability.CREATE,
            Capability.DELETE,
            Capability.GET_TYPE,
            Capability.GET_LAST_MODIFIED,
            Capability.LIST_CHILDREN,
            Capability.READ_CONTENT,
            Capability.URI,
            Capability.WRITE_CONTENT));
    private final Log log = LogFactory.getLog(getClass());

    public S3FileProvider() {
        setFileNameParser(new S3FileNameParser());
    }

    @Override
    protected FileSystem doCreateFileSystem(FileName fileName, FileSystemOptions fileSystemOptions)
            throws FileSystemException {
        final S3FileName parsedRoot = (S3FileName) fileName;
        final S3FileSystemOptions options = new S3FileSystemOptions(fileSystemOptions);
        final var platformFeatures = options.getPlatformFeatures();
        final S3FileName root =
                (platformFeatures != null) ? parsedRoot.withPlatformFeatures(platformFeatures) : parsedRoot;

        final AwsCredentialsProvider credentialsProvider;
        if (root.hasCredentials()) {
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(root.getAccessKey(), root.getSecretKey()));
        } else {
            credentialsProvider = options.getCredentialsProvider();
        }

        final URI endpoint = URI.create((options.isUseHttps() ? "https://" : "http://") + root.getEndpoint());

        if (log.isDebugEnabled()) {
            log.debug("Endpoint configuration [endpoint=" + endpoint + ",region=" + root.getSigningRegion() + "]");
        }

        if (options.getServerSideEncryption() && !root.getPlatformFeatures().supportsServerSideEncryption()) {
            log.warn("Try to use Server-Side Encryption with cloud that doesn't support it");
        }

        final S3Configuration.Builder s3ConfigBuilder =
                S3Configuration.builder().pathStyleAccessEnabled(root.hasPathPrefix());

        if (options.isDisableChunkedEncoding()) {
            s3ConfigBuilder.chunkedEncodingEnabled(false);
        }

        final S3ClientBuilder clientBuilder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(root.getSigningRegion()))
                .endpointOverride(endpoint)
                .serviceConfiguration(s3ConfigBuilder.build());

        if (options.getClientOverrideConfiguration() != null) {
            clientBuilder.overrideConfiguration(options.getClientOverrideConfiguration());
        }

        final S3Client s3Client = clientBuilder.build();

        final S3Presigner presigner = S3Presigner.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(root.getSigningRegion()))
                .endpointOverride(endpoint)
                .serviceConfiguration(s3ConfigBuilder.build())
                .build();

        return new S3FileSystem(root, options, s3Client, presigner);
    }

    @Override
    public Collection<Capability> getCapabilities() {
        return capabilities;
    }

    @Override
    public FileSystemConfigBuilder getConfigBuilder() {
        return S3FileSystemConfigBuilder.getInstance();
    }
}
