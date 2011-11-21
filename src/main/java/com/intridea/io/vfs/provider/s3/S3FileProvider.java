package com.intridea.io.vfs.provider.s3;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {

    public final static Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(
        Capability.CREATE,
        Capability.DELETE,
        Capability.RENAME,
        Capability.GET_TYPE,
        Capability.GET_LAST_MODIFIED,
        Capability.SET_LAST_MODIFIED_FILE,
        Capability.SET_LAST_MODIFIED_FOLDER,
        Capability.LIST_CHILDREN,
        Capability.READ_CONTENT,
        Capability.URI,
        Capability.WRITE_CONTENT
        //Capability.APPEND_CONTENT
    ));

    /**
     * Auth data types necessary for AWS authentification.
     */
    public final static UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
        UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
    };

    /**
     * Default options for S3 file system.
     */
    private static FileSystemOptions defaultOptions = new FileSystemOptions();

    /**
     * Returns default S3 file system options.
     * Use it to set AWS auth credentials.
     * @return
     */
    public static FileSystemOptions getDefaultFileSystemOptions () {
        return defaultOptions;
    }

    /**
     * S3 service instance
     */
    private S3Service service;

    /**
     * Logger instance
     */
    private Log logger = LogFactory.getLog(S3FileProvider.class);

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
    protected FileSystem doCreateFileSystem(FileName fileName,
            FileSystemOptions fileSystemOptions) throws FileSystemException {

        FileSystemOptions fsOptions = fileSystemOptions != null ?
                fileSystemOptions : getDefaultFileSystemOptions();

        // Initialize once S3 service.
        if (service == null) {
            UserAuthenticationData authData = null;
            try {
                // Read authData from file system options
                authData = UserAuthenticatorUtils.authenticate(fsOptions, AUTHENTICATOR_TYPES);

                logger.info("Initialize Amazon S3 service client ...");

                // Fetch AWS key-id and secret key from authData
                String keyId = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData, UserAuthenticationData.USERNAME, null));
                String key = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData, UserAuthenticationData.PASSWORD, null));
                if (keyId.length() + key.length() == 0) {
                    throw new FileSystemException("Empty AWS credentials");
                }

                // Initialize S3 service client.
                AWSCredentials awsCredentials = new AWSCredentials(keyId, key);
                service = new RestS3Service(awsCredentials);
                logger.info("... Ok");

            } catch (S3ServiceException e) {
                logger.error(String.format("Failed to initialize Amazon S3 service client. Reason: %s",
                        e.getS3ErrorMessage()), e);
                throw new FileSystemException(e);
            }
            finally {
                UserAuthenticatorUtils.cleanup(authData);
            }
        }

        // Construct S3 file system
        return new S3FileSystem((S3FileName) fileName, service, fsOptions);
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
