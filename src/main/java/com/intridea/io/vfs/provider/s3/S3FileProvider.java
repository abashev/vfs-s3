package com.intridea.io.vfs.provider.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.commons.vfs2.UserAuthenticationData.PASSWORD;
import static org.apache.commons.vfs2.UserAuthenticationData.USERNAME;
import static org.apache.commons.vfs2.util.UserAuthenticatorUtils.getData;

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
     * Auth data types necessary for AWS authentification.
     */
    public final static UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
        USERNAME, PASSWORD
    };

    /**
     * Default options for S3 file system.
     */
    private static FileSystemOptions defaultOptions = new FileSystemOptions();

    /**
     * Returns default S3 file system options.
     * Use it to set AWS auth credentials.
     * @return default S3 file system options
     */
    public static FileSystemOptions getDefaultFileSystemOptions () {
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

        FileSystemOptions fsOptions = (fileSystemOptions != null) ? fileSystemOptions : getDefaultFileSystemOptions();

        // Initialize once S3 service.
        UserAuthenticationData authData = null;

        AWSCredentials awsCredentials = null;
        AmazonS3Client service = null;
        ClientConfiguration clientConfiguration = S3FileSystemConfigBuilder
            .getInstance().getClientConfiguration(fsOptions);

        try {
            // Read authData from file system options
            authData = UserAuthenticatorUtils.authenticate(fsOptions, AUTHENTICATOR_TYPES);

            logger.info("Start to initialize Amazon S3 service client");

            // Fetch AWS key-id and secret key from authData
            String accessKey = UserAuthenticatorUtils.toString(getData(authData, USERNAME, null));
            String secretKey = UserAuthenticatorUtils.toString(getData(authData, PASSWORD, null));

            if (isEmpty(accessKey) || isEmpty(secretKey)) {
                throw new FileSystemException("Empty AWS credentials");
            }

            // Initialize S3 service client.
            awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

            service = new AmazonS3Client(awsCredentials, clientConfiguration);
        } finally {
            UserAuthenticatorUtils.cleanup(authData);
        }

        // Construct S3 file system
        return new S3FileSystem((S3FileName) fileName, awsCredentials, service, fsOptions);
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
     * Check for empty string FIXME find the same at Amazon SDK
     *
     * @param s string
     * @return true iff string is null or zero length
     */
    private boolean isEmpty(String s) {
        return ((s == null) || (s.length() == 0));
    }
}
