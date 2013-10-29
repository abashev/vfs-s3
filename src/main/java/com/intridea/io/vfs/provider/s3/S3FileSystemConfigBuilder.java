package com.intridea.io.vfs.provider.s3;

import com.amazonaws.services.s3.model.Region;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
    private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();
    
    private static final String SERVER_SIDE_ENCRYPTION = S3FileSystemConfigBuilder.class.getName() + ".SERVER_SIDE_ENCRYPTION";
    private static final String REGION = S3FileSystemConfigBuilder.class.getName() + ".REGION";

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

    public void setRegion(FileSystemOptions opts, Region region) {
        setParam(opts, REGION, region.toString());
    }

    public Region getRegion(FileSystemOptions opts) {
        String r = getString(opts, REGION);
        return (r == null) ? null : Region.fromValue(r);
    }
}
