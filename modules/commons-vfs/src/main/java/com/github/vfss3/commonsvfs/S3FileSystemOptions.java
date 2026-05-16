package com.github.vfss3.commonsvfs;

import com.github.vfss3.commonsvfs.operations.PlatformFeatures;
import org.apache.commons.vfs2.FileSystemOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;

/**
 * Wrapper around FileSystemOptions for storing and retrieving various options. It can't be immutable because it use
 * system properties as default values.
 *
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public class S3FileSystemOptions implements Cloneable {
    /**
     * Configuration prefix
     */
    public static final String PREFIX = "s3";

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

    public boolean getServerSideEncryption() {
        return S3FileSystemConfigBuilder.getInstance().getServerSideEncryption(options);
    }

    public void setServerSideEncryption(boolean serverSideEncryption) {
        S3FileSystemConfigBuilder.getInstance().setServerSideEncryption(options, serverSideEncryption);
    }

    /**
     * Override the default {@link ClientOverrideConfiguration} (retry strategy, timeouts, …) used when building the
     * S3 client.
     */
    public ClientOverrideConfiguration getClientOverrideConfiguration() {
        return S3FileSystemConfigBuilder.getInstance().getClientOverrideConfiguration(options);
    }

    public void setClientOverrideConfiguration(ClientOverrideConfiguration configuration) {
        S3FileSystemConfigBuilder.getInstance().setClientOverrideConfiguration(options, configuration);
    }

    public boolean isDisableChunkedEncoding() {
        return S3FileSystemConfigBuilder.getInstance().getDisableChunkedEncoding(options);
    }

    public void setDisableChunkedEncoding(boolean disableChunkedEncoding) {
        S3FileSystemConfigBuilder.getInstance().setDisableChunkedEncoding(options, disableChunkedEncoding);
    }

    public boolean isUseHttps() {
        return S3FileSystemConfigBuilder.getInstance().isUseHttps(options);
    }

    public void setUseHttps(boolean useHttps) {
        S3FileSystemConfigBuilder.getInstance().setUseHttps(options, useHttps);
    }

    public boolean isCreateBucket() {
        return S3FileSystemConfigBuilder.getInstance().isCreateBucket(options);
    }

    public void setCreateBucket(boolean createBucket) {
        S3FileSystemConfigBuilder.getInstance().setCreateBucket(options, createBucket);
    }

    /**
     * Get credentials provider for a file system - {@code DefaultCredentialsProvider} by default.
     */
    public AwsCredentialsProvider getCredentialsProvider() {
        return S3FileSystemConfigBuilder.getInstance().getCredentialsProvider(options);
    }

    public void setCredentialsProvider(AwsCredentialsProvider provider) {
        S3FileSystemConfigBuilder.getInstance().setCredentialsProvider(options, provider);
    }

    public ObjectOwnership getObjectOwnership() {
        return S3FileSystemConfigBuilder.getInstance().getObjectOwnership(options);
    }

    public void setObjectOwnership(ObjectOwnership ownership) {
        S3FileSystemConfigBuilder.getInstance().setObjectOwnership(options, ownership);
    }

    public BucketCannedACL getCannedAcl() {
        return S3FileSystemConfigBuilder.getInstance().getCannedAcl(options);
    }

    public void setCannedAcl(BucketCannedACL acl) {
        S3FileSystemConfigBuilder.getInstance().setCannedAcl(options, acl);
    }

    public PlatformFeatures getPlatformFeatures() {
        return S3FileSystemConfigBuilder.getInstance().getPlatformFeatures(options);
    }

    public void setPlatformFeatures(PlatformFeatures features) {
        S3FileSystemConfigBuilder.getInstance().setPlatformFeatures(options, features);
    }

    public FileSystemOptions toFileSystemOptions() {
        return (FileSystemOptions) options.clone();
    }

    @SuppressWarnings({"MethodDoesntCallSuperMethod", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override
    public S3FileSystemOptions clone() {
        return new S3FileSystemOptions(options);
    }

    @Override
    public String toString() {
        return options.toString();
    }
}
