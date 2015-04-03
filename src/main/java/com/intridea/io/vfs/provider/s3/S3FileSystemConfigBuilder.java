package com.intridea.io.vfs.provider.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;

import static org.apache.commons.vfs2.UserAuthenticationData.PASSWORD;
import static org.apache.commons.vfs2.UserAuthenticationData.USERNAME;
import static org.apache.commons.vfs2.util.UserAuthenticatorUtils.getData;

public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
    private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();

    private static final String SERVER_SIDE_ENCRYPTION = S3FileSystemConfigBuilder.class.getName() + ".SERVER_SIDE_ENCRYPTION";
    private static final String REGION = S3FileSystemConfigBuilder.class.getName() + ".REGION";
    private static final String CLIENT_CONFIGURATION = S3FileSystemConfigBuilder.class.getName() + ".CLIENT_CONFIGURATION";
    private static final String MAX_UPLOAD_THREADS = S3FileSystemConfigBuilder.class.getName() + ".MAX_UPLOAD_THREADS";
    private static final String AWS_CREDENTIALS = S3FileSystemConfigBuilder.class.getName() + ".AWS_CREDENTIALS";
    private static final String AMAZON_S3_CLIENT = S3FileSystemConfigBuilder.class.getName() + ".AMAZON_S3_CLIENT";
    private static final String ENDPOINT = S3FileSystemConfigBuilder.class.getName() + ".ENDPOINT";

    public static final int DEFAULT_MAX_UPLOAD_THREADS = 2;

    /**
     * Auth data types necessary for AWS authentification.
     */
    private final static UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
            USERNAME, PASSWORD
    };

    private S3FileSystemConfigBuilder()
    {
        super("s3.");
    }

    public static S3FileSystemConfigBuilder getInstance()
    {
        return BUILDER;
    }

    @Override
    protected Class<? extends FileSystem> getConfigClass() {
        return S3FileSystem.class;
    }

    /**
     * use server-side encryption.
     *
     * @param opts The FileSystemOptions.
     * @param serverSideEncryption true if server-side encryption should be used.
     */
    public void setServerSideEncryption(FileSystemOptions opts, boolean serverSideEncryption)
    {
        setParam(opts, SERVER_SIDE_ENCRYPTION, serverSideEncryption);
    }

    /**
     * @param opts The FileSystemOptions.
     * @return true if server-side encryption is being used.
     * @see #setServerSideEncryption(org.apache.commons.vfs2.FileSystemOptions, boolean)
     */
    public Boolean getServerSideEncryption(FileSystemOptions opts)
    {
        return getBoolean(opts, SERVER_SIDE_ENCRYPTION, false);
    }

    /**
     * @param opts The FileSystemOptions.
     * @param region The S3 region to connect to (if null, then US Standard)
     */
    public void setRegion(FileSystemOptions opts, Region region) {
        if (!isEmpty(getEndpoint(opts))) {
            throw new IllegalArgumentException("Cannot set both Region and Endpoint");
        }
        setParam(opts, REGION, region.toString());
    }

    /**
     * @param opts The FileSystemOptions.
     * @return The S3 region to connect to (if null, then US Standard)
     */
    public Region getRegion(FileSystemOptions opts) {
        String r = getString(opts, REGION, "US");
        return (r == null) ? null : Region.fromValue(r);
    }

    /**
     * @param opts The FileSystemOptions.
     * @param clientConfiguration The AWS ClientConfiguration object to
     *                            use when creating the connection.
     */
    public void setClientConfiguration(FileSystemOptions opts, ClientConfiguration clientConfiguration) {
        setParam(opts, CLIENT_CONFIGURATION, clientConfiguration);
    }

    /**
     * @param opts The FileSystemOptions.
     * @return The AWS ClientConfiguration object to use when creating the
     * connection.  If none has been set, a default ClientConfiguration is returend,
     * with the following differences:
     *   1. The maxErrorRetry is 8 instead of the AWS default (3).  This
     *      is generally a better setting to use when operating in a production
     *      environment and means approximately up to 2 minutes of retries for
     *      failed operations.
     */
    public ClientConfiguration getClientConfiguration(FileSystemOptions opts) {
        ClientConfiguration clientConfiguration = (ClientConfiguration) getParam(opts, CLIENT_CONFIGURATION);
        if (clientConfiguration == null) {
            clientConfiguration = new ClientConfiguration();
            clientConfiguration.setMaxErrorRetry(8);
        }
        return clientConfiguration;
    }

    /**
     * Set maximum number of threads to use for a single large (16MB or more) upload
     * @param opts The FileSystemOptions
     * @param maxRetries maximum number of threads to use for a single large (16MB or more) upload
     */
    public void setMaxUploadThreads(FileSystemOptions opts, int maxRetries) {
        setParam(opts, MAX_UPLOAD_THREADS, maxRetries);
    }

    /**
     * Get maximum number of threads to use for a single large (16MB or more) upload
     * @param opts The FileSystemOptions
     * @return maximum number of threads to use for a single large (16MB or more) upload
     */
    public int getMaxUploadThreads(FileSystemOptions opts) {
        return getInteger(opts, MAX_UPLOAD_THREADS, DEFAULT_MAX_UPLOAD_THREADS);
    }

    /**
     * Set predefined AWSCredentials object with access and secret keys for accessing AWS.
     *
     * @param opts
     * @param credentials
     */
    public void setAWSCredentials(FileSystemOptions opts, AWSCredentials credentials) {
        setParam(opts, AWS_CREDENTIALS, credentials);
    }

    /**
     * Get predefined AWSCredentials object with access and secret keys for accessing AWS.
     *
     * @param options
     * @return
     */
    public AWSCredentials getAWSCredentials(FileSystemOptions options) throws FileSystemException {
        AWSCredentials credentials = (AWSCredentials) getParam(options, AWS_CREDENTIALS);

        if (credentials != null) {
            return credentials;
        }

        UserAuthenticationData authData = null;

        try {
            // Read authData from file system options
            authData = UserAuthenticatorUtils.authenticate(options, AUTHENTICATOR_TYPES);

            // Fetch AWS key-id and secret key from authData
            String accessKey = UserAuthenticatorUtils.toString(getData(authData, USERNAME, null));
            String secretKey = UserAuthenticatorUtils.toString(getData(authData, PASSWORD, null));

            if (isEmpty(accessKey) || isEmpty(secretKey)) {
                throw new FileSystemException("Empty AWS credentials");
            }

            // Initialize S3 service client.
            return (new BasicAWSCredentials(accessKey, secretKey));
        } finally {
            UserAuthenticatorUtils.cleanup(authData);
        }
    }

    /**
     * In case of many S3FileProviders (useful in multi-threaded environment to eliminate commons-vfs internal locks)
     * you could specify one amazon client for all providers.
     *
     * @param opts
     * @param client
     */
    public void setAmazonS3Client(FileSystemOptions opts, AmazonS3Client client) {
        setParam(opts, AMAZON_S3_CLIENT, client);
    }

    /**
     * Get preinitialized AmazonS3 client.
     *
     * @param opts
     * @return
     */
    public AmazonS3Client getAmazonS3Client(FileSystemOptions opts) {
        return (AmazonS3Client) getParam(opts, AMAZON_S3_CLIENT);
    }

    /**
     * @param opts The FileSystemOptions.
     * @param endpoint The S3 endpoint to connect to (if null, then determined by region)
     */
    public void setEndpoint(FileSystemOptions opts, String endpoint) {
        if (hasParam(opts, REGION)) {
            throw new IllegalArgumentException("Cannot set both Region and Endpoint");
        }
        setParam(opts, ENDPOINT, endpoint);
    }

    /**
     * @param opts The FileSystemOptions.
     * @return The S3 endpoint to connect to (if null, then determined by region)
     */
    public String getEndpoint(FileSystemOptions opts) {
        return getString(opts, ENDPOINT, null);
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
