package com.intridea.io.vfs.provider.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.util.Collection;

/**
 * An S3 file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileSystem extends AbstractFileSystem {

    private static final Log logger = LogFactory.getLog(S3FileSystem.class);

    private final AmazonS3Client service;
    private final Bucket bucket;

    private boolean shutdownServiceOnClose = false;

    public S3FileSystem(
            S3FileName fileName, AmazonS3Client service, FileSystemOptions fileSystemOptions
    ) throws FileSystemException {
        super(fileName, null, fileSystemOptions);

        String bucketId = fileName.getBucketId();

        this.service = service;

        try {
            if (service.doesBucketExist(bucketId)) {
                bucket = new Bucket(bucketId);
            } else {
                bucket = service.createBucket(bucketId);

                logger.debug("Created new bucket.");
            }

            logger.info("Created new S3 FileSystem " + bucketId);
        } catch (AmazonServiceException e) {
            String s3message = e.getMessage();

            if (s3message != null) {
                throw new FileSystemException(s3message, e);
            } else {
                throw new FileSystemException(e);
            }
        }
    }

    @Override
    protected void addCapabilities(Collection<Capability> caps) {
        caps.addAll(S3FileProvider.capabilities);
    }

    protected Bucket getBucket() {
        return bucket;
    }

    protected Region getRegion() {
        return S3FileSystemConfigBuilder.getInstance().getRegion(getFileSystemOptions());
    }


    protected String getEndpoint() {
        return S3FileSystemConfigBuilder.getInstance().getEndpoint(getFileSystemOptions());
    }

    protected AmazonS3 getService() {
        return service;
    }

    @Override
    protected FileObject createFile(AbstractFileName fileName) throws Exception {
        return new S3FileObject(fileName, this);
    }

    @Override
    protected void doCloseCommunicationLink() {
        if (shutdownServiceOnClose) {
            service.shutdown();
        }
    }

    public void setShutdownServiceOnClose(boolean shutdownServiceOnClose) {
        this.shutdownServiceOnClose = shutdownServiceOnClose;
    }
}
