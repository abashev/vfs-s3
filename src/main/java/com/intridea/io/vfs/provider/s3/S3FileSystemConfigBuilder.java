package com.intridea.io.vfs.provider.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import com.github.vfss3.S3FileSystemOptions;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;

/**
 * @deprecated Use {@link com.github.vfss3.S3FileSystemOptions}
 */
@Deprecated
public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
    private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();

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
    public void setServerSideEncryption(FileSystemOptions opts, boolean serverSideEncryption) {
        new S3FileSystemOptions(opts, false).setServerSideEncryption(serverSideEncryption);
    }

    /**
     * @param opts The FileSystemOptions.
     * @return true if server-side encryption is being used.
     * @see #setServerSideEncryption(org.apache.commons.vfs2.FileSystemOptions, boolean)
     */
    public Boolean getServerSideEncryption(FileSystemOptions opts) {
        return new S3FileSystemOptions(opts, false).getServerSideEncryption();
    }

    /**
     * @param opts The FileSystemOptions.
     * @param region The S3 region to connect to (if null, then US Standard)
     */
    public void setRegion(FileSystemOptions opts, Regions region) {
        new S3FileSystemOptions(opts, false).setRegion(region);
    }

    /**
     * @param opts The FileSystemOptions.
     * @return The S3 region to connect to (if null, then US Standard)
     */
    public Regions getRegion(FileSystemOptions opts) {
        return new S3FileSystemOptions(opts, false).getRegion().orElse(null);
    }

    /**
     * @param opts The FileSystemOptions.
     * @param clientConfiguration The AWS ClientConfiguration object to
     *                            use when creating the connection.
     */
    public void setClientConfiguration(FileSystemOptions opts, ClientConfiguration clientConfiguration) {
        new S3FileSystemOptions(opts, false).setClientConfiguration(clientConfiguration);
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
        return new S3FileSystemOptions(opts, false).getClientConfiguration();
    }

    /**
     * Set maximum number of threads to use for a single large (16MB or more) upload
     * @param opts The FileSystemOptions
     * @param maxRetries maximum number of threads to use for a single large (16MB or more) upload
     */
    public void setMaxUploadThreads(FileSystemOptions opts, int maxThread) {
        new S3FileSystemOptions(opts, false).setMaxUploadThreads(maxThread);
    }

    /**
     * Get maximum number of threads to use for a single large (16MB or more) upload
     * @param opts The FileSystemOptions
     * @return maximum number of threads to use for a single large (16MB or more) upload
     */
    public int getMaxUploadThreads(FileSystemOptions opts) {
        return new S3FileSystemOptions(opts, false).getMaxUploadThreads();
    }

    /**
     * Set predefined AWSCredentials object with access and secret keys for accessing AWS.
     *
     * @param opts
     * @param credentials
     */
    public void setAWSCredentials(FileSystemOptions opts, AWSCredentials credentials) {
        throw new UnsupportedOperationException("Setting AWS credentials is not supported in vfs-s3");
    }

    /**
     * Get predefined AWSCredentials object with access and secret keys for accessing AWS.
     *
     * @param options
     * @return
     */
    public AWSCredentials getAWSCredentials(FileSystemOptions options) throws FileSystemException {
        throw new UnsupportedOperationException("Setting AWS credentials is not supported in vfs-s3");
    }

    /**
     * In case of many S3FileProviders (useful in multi-threaded environment to eliminate commons-vfs internal locks)
     * you could specify one amazon client for all providers.
     *
     * @param opts
     * @param client
     */
    public void setAmazonS3Client(FileSystemOptions opts, AmazonS3Client client) {
        new S3FileSystemOptions(opts, false).setS3Client(client);
    }

    /**
     * Get preinitialized AmazonS3 client.
     *
     * @param opts
     * @return
     */
    public AmazonS3Client getAmazonS3Client(FileSystemOptions opts) {
        return new S3FileSystemOptions(opts, false).getS3Client().orElse(null);
    }
}
