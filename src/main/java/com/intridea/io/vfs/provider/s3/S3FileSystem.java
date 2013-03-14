package com.intridea.io.vfs.provider.s3;

import java.util.Collection;

import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * An S3 file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileSystem extends AbstractFileSystem {

    private static final Log logger = LogFactory.getLog(S3FileSystem.class);

    private final AWSCredentials awsCredentials;
    private final AmazonS3 service;
    private final Bucket bucket;
    private final TransferManager transferManager;

    public S3FileSystem(
            S3FileName fileName, AWSCredentials awsCredentials, AmazonS3 service, FileSystemOptions fileSystemOptions
    ) throws FileSystemException {
        super(fileName, null, fileSystemOptions);

        String bucketId = fileName.getBucketId();

        this.awsCredentials = awsCredentials;
        this.service = service;

        try {
            if (service.doesBucketExist(bucketId)) {
                bucket = new Bucket(bucketId);
            } else {
                bucket = service.createBucket(bucketId);

                logger.debug("Created new bucket.");
            }

            this.transferManager = new TransferManager(service);

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

    @Override
    protected FileObject createFile(AbstractFileName fileName) throws Exception {
        return new S3FileObject(fileName, this, awsCredentials, service, transferManager, bucket);
    }
}
