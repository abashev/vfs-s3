package com.github.vfss3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

/**
 * The config builder for various AWS S3 configuration options.
 */
public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {

    private static final String SERVER_SIDE_ENCRYPTION = "serverSideEncryption";
    private static final String REGION                 = "region";
    private static final String CLIENT_CONFIGURATION   = "clientConfiguration";
    private static final String MAX_UPLOAD_THREADS     = "maxUploadThreads";
    private static final String S3_CLIENT              = "S3Client";
    private static final String ENDPOINT               = "endpoint";
    private static final String NO_BUCKET_TEST         = "noBucketTest";

    private static final int DEFAULT_MAX_UPLOAD_THREADS = 2;
    private static final int DEFAULT_MAX_ERROR_RETRY = 8;

    private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();

    /**
     * Gets the singleton builder.
     *
     * @return the singleton builder.
     */
    public static S3FileSystemConfigBuilder getInstance(){
        return BUILDER;
    }

    private S3FileSystemConfigBuilder() {
        super("s3.");
    }

    @Override
    protected Class<? extends FileSystem> getConfigClass() {
        return S3FileSystem.class;
    }

    void setOption(FileSystemOptions opts, String name, Object value) {
        setParam(opts, name, value);
    }

    Object getOption(FileSystemOptions opts, String name) {
        return getParam(opts, name);
    }

    boolean getBooleanOption(FileSystemOptions opts, String name, boolean defaultValue) {
        return getBoolean(opts, name, defaultValue);
    }

    String getStringOption(FileSystemOptions opts, String name, String defaultValue) {
        return getString(opts, name, defaultValue);
    }

    int getIntegerOption(FileSystemOptions opts, String name, int defaultValue) {
        return getInteger(opts, name, defaultValue);
    }

    public Boolean getServerSideEncryption(final FileSystemOptions opts) {
        return getBooleanOption(opts, SERVER_SIDE_ENCRYPTION, false);
    }

    public void setServerSideEncryption(final FileSystemOptions opts, final Boolean serverSideEncryption) {
        setOption(opts, SERVER_SIDE_ENCRYPTION, serverSideEncryption);
    }

    /**
     * Set default region for S3 client
     *
     * @param region The S3 region to connect to (if null, then US Standard)
     */
    public void setRegion(final FileSystemOptions opts, Region region) {
        if (getEndpoint(opts).isPresent()) {
            throw new IllegalArgumentException("Cannot set both Region and Endpoint");
        }
        setOption(opts, REGION, requireNonNull(region).toString());
    }

    /**
     * @return The S3 region to connect to (if null, then US Standard)
     */
    public Optional<Region> getRegion(final FileSystemOptions opts) {
        String r = getStringOption(opts, REGION, null);

        return (r == null) ? empty() : Optional.of(Region.fromValue(r));
    }

    /**
     * @param clientConfiguration The AWS ClientConfiguration object to
     *                            use when creating the connection.
     */
    public void setClientConfiguration(final FileSystemOptions opts, ClientConfiguration clientConfiguration) {
        setOption(opts, CLIENT_CONFIGURATION, requireNonNull(clientConfiguration));
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
    public ClientConfiguration getClientConfiguration(final FileSystemOptions opts) {
        ClientConfiguration clientConfiguration = (ClientConfiguration) getOption(opts, CLIENT_CONFIGURATION);

        if (clientConfiguration == null) {
            clientConfiguration = new ClientConfiguration();

            clientConfiguration.setMaxErrorRetry(DEFAULT_MAX_ERROR_RETRY);
        }

        return clientConfiguration;
    }

    /**
     * Set maximum number of threads to use for a single large (16MB or more) upload
     *
     * @param maxUploadThreads maximum number of threads to use for a single large (16MB or more) upload
     */
    public void setMaxUploadThreads(final FileSystemOptions opts, int maxUploadThreads) {
        setOption(opts, MAX_UPLOAD_THREADS, maxUploadThreads);
    }

    /**
     * Get maximum number of threads to use for a single large (16MB or more) upload
     *
     * @return maximum number of threads to use for a single large (16MB or more) upload
     */
    public int getMaxUploadThreads(final FileSystemOptions opts) {
        return getIntegerOption(opts, MAX_UPLOAD_THREADS, DEFAULT_MAX_UPLOAD_THREADS);
    }

    /**
     * In case of many S3FileProviders (useful in multi-threaded environment to eliminate commons-vfs internal locks)
     * you could specify one amazon client for all providers.
     *
     * @param client
     */
    public void setS3Client(final FileSystemOptions opts, AmazonS3Client client) {
        setOption(opts, S3_CLIENT, requireNonNull(client));
    }

    /**
     * Get preinitialized AmazonS3 client.
     *
     * @return
     */
    public Optional<AmazonS3Client> getS3Client(final FileSystemOptions opts) {
        return ofNullable((AmazonS3Client) getOption(opts, S3_CLIENT));
    }

    /**
     * Set default region for S3 client
     *
     * @param endpoint The S3 endpoint to connect to (if null, then use Region)
     */
    public void setEndpoint(final FileSystemOptions opts, String endpoint) {
        if (getRegion(opts).isPresent()) {
            throw new IllegalArgumentException("Cannot set both Region and Endpoint");
        }
        setOption(opts, ENDPOINT, requireNonNull(endpoint));
    }

    /**
     * @return The S3 endpoint to connect to (if null, then use Region)
     */
    public Optional<String> getEndpoint(final FileSystemOptions opts) {
        String endpoint = getStringOption(opts, ENDPOINT, null);
        return Optional.ofNullable(endpoint);
    }

    /**
     * Sets no bucket test
     *
     * @param noBucketTest true if bucket existence and access shouldn't be tested
     */
    public void setNoBucketTest(final FileSystemOptions opts, boolean noBucketTest) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();


        builder.setOption(opts, NO_BUCKET_TEST, noBucketTest);
    }

    /**
     * Gets no bucket test
     *
     * @return true if bucket existence and access shouldn't be tested
     */
    public Optional<Boolean> getNoBucketTest(final FileSystemOptions opts) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return ofNullable((Boolean)builder.getOption(opts, NO_BUCKET_TEST));
    }

}
