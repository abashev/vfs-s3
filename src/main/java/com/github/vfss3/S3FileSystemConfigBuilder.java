package com.github.vfss3;

import com.amazonaws.ClientConfiguration;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import static com.github.vfss3.S3FileSystemOptions.PREFIX;
import static java.util.Objects.requireNonNull;

/**
 * The config builder for various AWS S3 configuration options.
 */
public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {

    private static final String SERVER_SIDE_ENCRYPTION   = "serverSideEncryption";
    private static final String CLIENT_CONFIGURATION     = "clientConfiguration";
    private static final String MAX_UPLOAD_THREADS       = "maxUploadThreads";
    private static final String DISABLE_BUCKET_TEST      = "disableBucketTest";
    private static final String PER_FILE_LOCKING         = "perFileLocking";
    private static final String DISABLE_CHUNKED_ENCODING = "disableChunkedEncoding"; // Useful for localstack
    private static final String USE_HTTPS                = "useHttps"; // Useful for localstack

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
        super(PREFIX + ".");
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

    public boolean getServerSideEncryption(FileSystemOptions opts) {
        return getBooleanOption(opts, SERVER_SIDE_ENCRYPTION, false);
    }

    public void setServerSideEncryption(FileSystemOptions opts, final boolean serverSideEncryption) {
        setOption(opts, SERVER_SIDE_ENCRYPTION, serverSideEncryption);
    }

    /**
     * @param clientConfiguration The AWS ClientConfiguration object to
     *                            use when creating the connection.
     */
    public void setClientConfiguration(FileSystemOptions opts, ClientConfiguration clientConfiguration) {
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
    public ClientConfiguration getClientConfiguration(FileSystemOptions opts) {
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
    public void setMaxUploadThreads(FileSystemOptions opts, int maxUploadThreads) {
        setOption(opts, MAX_UPLOAD_THREADS, maxUploadThreads);
    }

    /**
     * Get maximum number of threads to use for a single large (16MB or more) upload
     *
     * @return maximum number of threads to use for a single large (16MB or more) upload
     */
    public int getMaxUploadThreads(FileSystemOptions opts) {
        return getIntegerOption(opts, MAX_UPLOAD_THREADS, DEFAULT_MAX_UPLOAD_THREADS);
    }

    /**
     * Sets no bucket test
     *
     * @param noBucketTest true if bucket existence and access shouldn't be tested
     */
    public void setDisableBucketTest(FileSystemOptions opts, boolean noBucketTest) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(opts, DISABLE_BUCKET_TEST, noBucketTest);
    }

    /**
     * Gets no bucket test
     *
     * @return true if bucket existence and access shouldn't be tested
     */
    public boolean getDisableBucketTest(FileSystemOptions opts) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return builder.getBooleanOption(opts, DISABLE_BUCKET_TEST, false);
    }
  
    /**
     * Sets per-file locking.
     *
     * @param perFileLocking true if per-file locking should be used.
     */
    public void setPerFileLocking(FileSystemOptions opts, boolean perFileLocking) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(opts, PER_FILE_LOCKING, perFileLocking);
    }

    /**
     * Gets per-file locking.
     *
     * @return true if per-file locking should be used.
     */
    public boolean getPerFileLocking(FileSystemOptions opts) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return builder.getBooleanOption(opts, S3FileSystemConfigBuilder.PER_FILE_LOCKING, true);
    }

    /**
     * Don't use chunked encoding for AWS calls - useful for localstack because it doesn't support it.
     *
     * @return true if use https for all communications
     */
    public boolean getDisableChunkedEncoding(FileSystemOptions opts) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return builder.getBooleanOption(opts, DISABLE_CHUNKED_ENCODING, false);
    }

    /**
     * Don't use chunked encoding for AWS calls - useful for localstack because it doesn't support it.
     *
     * @param disableChunkedEncoding
     */
    public void setDisableChunkedEncoding(FileSystemOptions opts, boolean disableChunkedEncoding) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(opts, DISABLE_CHUNKED_ENCODING, disableChunkedEncoding);
    }

    /**
     * Use https for endpoint calls. true by default
     *
     * @return true if use https for all communications
     */
    public boolean isUseHttps(FileSystemOptions opts) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return builder.getBooleanOption(opts, USE_HTTPS, true);
    }

    /**
     * Use https for endpoint calls. true by default
     *
     * @param opts
     * @param useHttps
     */
    public void setUseHttps(FileSystemOptions opts, boolean useHttps) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(opts, USE_HTTPS, useHttps);
    }
}
