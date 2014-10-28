package com.intridea.io.vfs.provider.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
    private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();

    private static final String SERVER_SIDE_ENCRYPTION = S3FileSystemConfigBuilder.class.getName() + ".SERVER_SIDE_ENCRYPTION";
    private static final String REGION = S3FileSystemConfigBuilder.class.getName() + ".REGION";
    private static final String CLIENT_CONFIGURATION = S3FileSystemConfigBuilder.class.getName() + ".CLIENT_CONFIGURATION";
    private static final String MAX_UPLOAD_THREADS = S3FileSystemConfigBuilder.class.getName() + ".MAX_UPLOAD_THREADS";

    public static final int DEFAULT_MAX_UPLOAD_THREADS = 2;

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
        setParam(opts, REGION, region.toString());
    }

    /**
     * @param opts The FileSystemOptions.
     * @return The S3 region to connect to (if null, then US Standard)
     */
    public Region getRegion(FileSystemOptions opts) {
        String r = getString(opts, REGION);
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

}
