package com.github.vfss3.commonsvfs;

import static com.github.vfss3.commonsvfs.S3FileSystemOptions.PREFIX;
import static java.util.Objects.requireNonNull;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;

/**
 * The config builder for various AWS S3 configuration options.
 */
public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
    private static final String SERVER_SIDE_ENCRYPTION = "serverSideEncryption";
    private static final String CLIENT_OVERRIDE_CONFIGURATION = "clientOverrideConfiguration";
    private static final String DISABLE_CHUNKED_ENCODING = "disableChunkedEncoding";
    private static final String USE_HTTPS = "useHttps";
    private static final String CREATE_BUCKET = "createBucket";
    private static final String CREDENTIALS_PROVIDER = "credentialsProvider";
    private static final String OBJECT_OWNERSHIP = "objectOwnership";
    private static final String CANNED_ACL = "cannedAcl";

    private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();

    private S3FileSystemConfigBuilder() {
        super(PREFIX + ".");
    }

    public static S3FileSystemConfigBuilder getInstance() {
        return BUILDER;
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

    public boolean getServerSideEncryption(FileSystemOptions opts) {
        return getBooleanOption(opts, SERVER_SIDE_ENCRYPTION, false);
    }

    public void setServerSideEncryption(FileSystemOptions opts, final boolean serverSideEncryption) {
        setOption(opts, SERVER_SIDE_ENCRYPTION, serverSideEncryption);
    }

    public void setClientOverrideConfiguration(FileSystemOptions opts, ClientOverrideConfiguration configuration) {
        setOption(opts, CLIENT_OVERRIDE_CONFIGURATION, requireNonNull(configuration));
    }

    public ClientOverrideConfiguration getClientOverrideConfiguration(FileSystemOptions opts) {
        return (ClientOverrideConfiguration) getOption(opts, CLIENT_OVERRIDE_CONFIGURATION);
    }

    public boolean getDisableChunkedEncoding(FileSystemOptions opts) {
        return getBooleanOption(opts, DISABLE_CHUNKED_ENCODING, false);
    }

    public void setDisableChunkedEncoding(FileSystemOptions opts, boolean disableChunkedEncoding) {
        setOption(opts, DISABLE_CHUNKED_ENCODING, disableChunkedEncoding);
    }

    public boolean isUseHttps(FileSystemOptions opts) {
        return getBooleanOption(opts, USE_HTTPS, true);
    }

    public void setUseHttps(FileSystemOptions opts, boolean useHttps) {
        setOption(opts, USE_HTTPS, useHttps);
    }

    public boolean isCreateBucket(FileSystemOptions opts) {
        return getBooleanOption(opts, CREATE_BUCKET, false);
    }

    public void setCreateBucket(FileSystemOptions opts, boolean createBucket) {
        setOption(opts, CREATE_BUCKET, createBucket);
    }

    public AwsCredentialsProvider getCredentialsProvider(FileSystemOptions opts) {
        AwsCredentialsProvider provider = (AwsCredentialsProvider) getOption(opts, CREDENTIALS_PROVIDER);

        return (provider != null) ? provider : DefaultCredentialsProvider.create();
    }

    public void setCredentialsProvider(FileSystemOptions opts, AwsCredentialsProvider provider) {
        setOption(opts, CREDENTIALS_PROVIDER, provider);
    }

    public ObjectOwnership getObjectOwnership(FileSystemOptions opts) {
        return (ObjectOwnership) getOption(opts, OBJECT_OWNERSHIP);
    }

    public void setObjectOwnership(FileSystemOptions opts, ObjectOwnership ownership) {
        setOption(opts, OBJECT_OWNERSHIP, ownership);
    }

    public BucketCannedACL getCannedAcl(FileSystemOptions opts) {
        return (BucketCannedACL) getOption(opts, CANNED_ACL);
    }

    public void setCannedAcl(FileSystemOptions opts, BucketCannedACL acl) {
        setOption(opts, CANNED_ACL, acl);
    }
}
