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
 * Wrapper aroung FileSystemOptions for storing and retrieving various options. It can't be immutable because it use
 * system properties as default values.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class S3FileSystemOptions {
    private static final String SERVER_SIDE_ENCRYPTION = "serverSideEncryption";
    private static final String REGION                 = "region";
    private static final String CLIENT_CONFIGURATION   = "clientConfiguration";
    private static final String MAX_UPLOAD_THREADS     = "maxUploadThreads";
    private static final String S3_CLIENT              = "S3Client";
    private static final String ENDPOINT               = "endpoint";

    private static final int DEFAULT_MAX_UPLOAD_THREADS = 2;
    private static final int DEFAULT_MAX_ERROR_RETRY = 8;

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
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(options, SERVER_SIDE_ENCRYPTION, serverSideEncryption);
    }

    /**
     * @return true if server-side encryption is being used.
     * @see #setServerSideEncryption(boolean)
     */
    public boolean getServerSideEncryption() {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return builder.getBooleanOption(options, SERVER_SIDE_ENCRYPTION, false);
    }

    /**
     * Set default region for S3 client
     *
     * @param region The S3 region to connect to (if null, then US Standard)
     */
    public void setRegion(Region region) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        if (getEndpoint().isPresent()) {
            throw new IllegalArgumentException("Cannot set both Region and Endpoint");
        }
        builder.setOption(options, REGION, requireNonNull(region).toString());
    }

    /**
     * @return The S3 region to connect to (if null, then US Standard)
     */
    public Optional<Region> getRegion() {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        String r = builder.getStringOption(options, REGION, null);

        return (r == null) ? empty() : Optional.of(Region.fromValue(r));
    }

    /**
     * Set default region for S3 client
     *
     * @param endpoint The S3 endpoint to connect to (if null, then use Region)
     */
    public void setEndpoint(String endpoint) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        if (getRegion().isPresent()) {
            throw new IllegalArgumentException("Cannot set both Region and Endpoint");
        }
        builder.setOption(options, ENDPOINT, requireNonNull(endpoint));
    }

    /**
     * @return The S3 endpoint to connect to (if null, then use Region)
     */
    public Optional<String> getEndpoint() {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        String endpoint = builder.getStringOption(options, ENDPOINT, null);

        return Optional.ofNullable(endpoint);
    }

    /**
     * @param clientConfiguration The AWS ClientConfiguration object to
     *                            use when creating the connection.
     */
    public void setClientConfiguration(ClientConfiguration clientConfiguration) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(options, CLIENT_CONFIGURATION, requireNonNull(clientConfiguration));
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
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        ClientConfiguration clientConfiguration = (ClientConfiguration) builder.getOption(options, CLIENT_CONFIGURATION);

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
    public void setMaxUploadThreads(int maxUploadThreads) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(options, MAX_UPLOAD_THREADS, maxUploadThreads);
    }

    /**
     * Get maximum number of threads to use for a single large (16MB or more) upload
     *
     * @return maximum number of threads to use for a single large (16MB or more) upload
     */
    public int getMaxUploadThreads() {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return builder.getIntegerOption(options, MAX_UPLOAD_THREADS, DEFAULT_MAX_UPLOAD_THREADS);
    }

    /**
     * In case of many S3FileProviders (useful in multi-threaded environment to eliminate commons-vfs internal locks)
     * you could specify one amazon client for all providers.
     *
     * @param client
     */
    public void setS3Client(AmazonS3Client client) {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        builder.setOption(options, S3_CLIENT, requireNonNull(client));
    }

    /**
     * Get preinitialized AmazonS3 client.
     *
     * @return
     */
    public Optional<AmazonS3Client> getS3Client() {
        final S3FileSystemConfigBuilder builder = new S3FileSystemConfigBuilder();

        return ofNullable((AmazonS3Client) builder.getOption(options, S3_CLIENT));
    }

    /**
     * Returns clone of options object for some legacy things.
     *
     * @return
     */
    public FileSystemOptions toFileSystemOptions() {
        return (FileSystemOptions) options.clone();
    }

    /**
     * Utility class for exposing some config builder methods.
     */
    private class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
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
    }

    @Override
    public String toString() {
        return options.toString();
    }
}
