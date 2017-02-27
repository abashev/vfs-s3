package com.github.vfss3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.vfs2.FileSystemOptions;

import java.util.Optional;

/**
 * Wrapper aroung FileSystemOptions for storing and retrieving various options. It can't be immutable because it use
 * system properties as default values.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class S3FileSystemOptions {

    private final FileSystemOptions options;

    public S3FileSystemOptions() {
        this(null);
    }

    /**
     * Create new object with copy of existed properties.
     *
     * @param options
     */
    public S3FileSystemOptions(FileSystemOptions options) {
        this(options, true);
    }

    /**
     * Create new options object based on existed options. cloneOptions useful for old config builder.
     *
     * @param options
     * @param cloneOptions
     */
    public S3FileSystemOptions(FileSystemOptions options, boolean cloneOptions) {
        if (options != null) {
            this.options = cloneOptions ? (FileSystemOptions) options.clone() : options;
        } else {
            this.options = new FileSystemOptions();
        }
    }

    /**
     * use server-side encryption.
     *
     * @param serverSideEncryption true if server-side encryption should be used.
     */
    public void setServerSideEncryption(boolean serverSideEncryption) {
        S3FileSystemConfigBuilder.getInstance().setServerSideEncryption(options, serverSideEncryption);
    }

    /**
     * @return true if server-side encryption is being used.
     * @see #setServerSideEncryption(boolean)
     */
    public boolean getServerSideEncryption() {
        return S3FileSystemConfigBuilder.getInstance().getServerSideEncryption(options);
    }

    /**
     * Set default region for S3 client
     *
     * @param region The S3 region to connect to (if null, then US Standard)
     */
    public void setRegion(Region region) {
        S3FileSystemConfigBuilder.getInstance().setRegion(options, region);
    }

    /**
     * @return The S3 region to connect to (if null, then US Standard)
     */
    public Optional<Region> getRegion() {
        return S3FileSystemConfigBuilder.getInstance().getRegion(options);
    }

    /**
     * Set default region for S3 client
     *
     * @param endpoint The S3 endpoint to connect to (if null, then use Region)
     */
    public void setEndpoint(String endpoint) {
        S3FileSystemConfigBuilder.getInstance().setEndpoint(options, endpoint);
    }

    /**
     * @return The S3 endpoint to connect to (if null, then use Region)
     */
    public Optional<String> getEndpoint() {
        return S3FileSystemConfigBuilder.getInstance().getEndpoint(options);
    }

    /**
     * @param clientConfiguration The AWS ClientConfiguration object to
     *                            use when creating the connection.
     */
    public void setClientConfiguration(ClientConfiguration clientConfiguration) {
        S3FileSystemConfigBuilder.getInstance().setClientConfiguration(options, clientConfiguration);
    }

    /**
     * @return The AWS ClientConfiguration object to use when creating the
     * connection.  If none has been set, a default ClientConfiguration is returend,
     * with the following differences:
     *   1. The maxErrorRetry is 8 instead of the AWS default (3).  This
     *      is generally a better setting to use when operating in a production
     *      environment and means approximately up to 2 minutes of retries for
     *      failed operations.
     */
    public ClientConfiguration getClientConfiguration() {
        return S3FileSystemConfigBuilder.getInstance().getClientConfiguration(options);
    }

    /**
     * Set maximum number of threads to use for a single large (16MB or more) upload
     *
     * @param maxUploadThreads maximum number of threads to use for a single large (16MB or more) upload
     */
    public void setMaxUploadThreads(int maxUploadThreads) {
        S3FileSystemConfigBuilder.getInstance().setMaxUploadThreads(options, maxUploadThreads);
    }

    /**
     * Get maximum number of threads to use for a single large (16MB or more) upload
     *
     * @return maximum number of threads to use for a single large (16MB or more) upload
     */
    public int getMaxUploadThreads() {
        return S3FileSystemConfigBuilder.getInstance().getMaxUploadThreads(options);
    }

    /**
     * In case of many S3FileProviders (useful in multi-threaded environment to eliminate commons-vfs internal locks)
     * you could specify one amazon client for all providers.
     *
     * @param client
     */
    public void setS3Client(AmazonS3Client client) {
        S3FileSystemConfigBuilder.getInstance().setS3Client(options, client);
    }

    /**
     * Get preinitialized AmazonS3 client.
     *
     * @return
     */
    public Optional<AmazonS3Client> getS3Client() {
        return S3FileSystemConfigBuilder.getInstance().getS3Client(options);
    }

    /**
     * Sets per-file locking.
     *
     * @param perFileLocking true if per-file locking should be used.
     */
    public void setPerFileLocking(boolean perFileLocking) {
        S3FileSystemConfigBuilder.getInstance().setPerFileLocking(options, perFileLocking);
    }

    /**
     * Gets per-file locking.
     *
     * @return true if per-file locking should be used.
     */
    public Optional<Boolean> getPerFileLocking() {
        return S3FileSystemConfigBuilder.getInstance().getPerFileLocking(options);
    }


    /**
     * Returns clone of options object for some legacy things.
     *
     * @return
     */
    public FileSystemOptions toFileSystemOptions() {
        return (FileSystemOptions) options.clone();
    }

    @Override
    public String toString() {
        return options.toString();
    }
}
