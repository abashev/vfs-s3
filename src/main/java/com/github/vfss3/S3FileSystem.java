package com.github.vfss3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.util.Collection;
import java.util.Optional;

import static com.amazonaws.services.s3.internal.Constants.*;

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
            S3FileName fileName, AmazonS3Client service, S3FileSystemOptions options
    ) throws FileSystemException {
        super(fileName, null, options.toFileSystemOptions());

        String bucketId = fileName.getBucketId();

        this.service = service;

        try {
            if (doesBucketExist(bucketId)) {
                bucket = new Bucket(bucketId);
            } else {
                bucket = service.createBucket(bucketId);

                logger.debug("Created new bucket.");
            }

            logger.info("Created new S3 FileSystem [name=" + bucketId + ",opts=" + options + "]");
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

    protected Optional<Region> getRegion() {
        return (new S3FileSystemOptions(getFileSystemOptions())).getRegion();
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

    /**
     * Implementation from AWS SDK but with exception on AccessForbidden status.
     *
     * @param bucketName
     * @return
     * @throws FileSystemException
     */
    private boolean doesBucketExist(String bucketName) throws FileSystemException {
        try {
            service.headBucket(new HeadBucketRequest(bucketName));

            return true;
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == BUCKET_ACCESS_FORBIDDEN_STATUS_CODE) {
                throw new FileSystemException("vfs.provider.s3/connection-forbidden.error", bucketName, e);
            }

            // A redirect error or a forbidden error means the bucket exists. So
            // returning true.
            if ((e.getStatusCode() == BUCKET_REDIRECT_STATUS_CODE)) {
                return true;
            }

            if (e.getStatusCode() == NO_SUCH_BUCKET_STATUS_CODE) {
                return false;
            }

            // Unknown exception
            throw new FileSystemException(e);
        }
    }

    /**
     * Custom resolveFile that can be safely used by S3FileObject.doListChildrenResolved() to avoid
     * possible deadlock with AbstractFileObject.getParent().
     * @param name The name of the file to locate.
     * @param key The s3 object key.
     * @param metadata The object metadata.
     * @return The FileObject.
     * @throws FileSystemException if an error occurs.
     */
    S3FileObject resolveChild(final AbstractFileName name, final String key, final ObjectMetadata metadata) throws FileSystemException
    {
        S3FileObject file;

        synchronized (this) {
            if (!getRootName().getRootURI().equals(name.getRootURI())) {
                throw new FileSystemException("vfs.provider/mismatched-fs-for-name.error",
                        name, getRootName(), name.getRootURI());
            }

            try {
                file = new S3FileObject(name, this);
                file.attachMetadata(key, metadata);
                putFileToCache(decorateFileObject(file));
            } catch (final Exception e) {
                throw new FileSystemException("vfs.provider/resolve-file.error", name, e);
            }
        }
        return file;
    }

}
