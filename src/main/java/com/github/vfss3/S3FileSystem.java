package com.github.vfss3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

import static com.amazonaws.services.s3.internal.Constants.BUCKET_ACCESS_FORBIDDEN_STATUS_CODE;
import static com.amazonaws.services.s3.internal.Constants.BUCKET_REDIRECT_STATUS_CODE;
import static com.amazonaws.services.s3.internal.Constants.NO_SUCH_BUCKET_STATUS_CODE;
import static java.util.Optional.empty;

/**
 * An S3 file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileSystem extends AbstractFileSystem {
    private final Logger log = LoggerFactory.getLogger(S3FileSystem.class);

    private static final Optional<Capability> PER_FILE_THREAD_LOCKING_CAPABILITY = discoverPerFileThreadLockingCapability();

    private final AmazonS3 service;
    private final Bucket bucket;

    private final boolean perFileLocking;

    public S3FileSystem(
            String bucketId, S3FileName fileName, AmazonS3 service, S3FileSystemOptions options
    ) throws FileSystemException {
        super(fileName, null, options.toFileSystemOptions());

        boolean perFileLocking =  false;

        if (PER_FILE_THREAD_LOCKING_CAPABILITY.isPresent()) {
            // if available and option not specified, then default to on
            perFileLocking = options.isPerFileLocking();
        } else if (options.isPerFileLocking()) {
            // if not available and option requested spit out a warning
            log.warn("per-file locking requested, but requires custom build of commons-vfs2. Falling back to per-filesystem locking");
        }

        this.perFileLocking = perFileLocking;
        this.service = service;

        log.info(
                "Init new S3 FileSystem [bucket={},fileName={},opts={}]",
                bucketId, fileName, options
        );

        try {
            if (options.isCreateBucket() && !doesBucketExist(bucketId)) {
                bucket = service.createBucket(bucketId);

                log.info("Created new bucket [{}].", bucket);
            } else {
                bucket = new Bucket(bucketId);
            }
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

        if (perFileLocking && PER_FILE_THREAD_LOCKING_CAPABILITY.isPresent()) {
            caps.add(PER_FILE_THREAD_LOCKING_CAPABILITY.get());
        }
    }

    protected Bucket getBucket() {
        return bucket;
    }

    protected AmazonS3 getService() {
        return service;
    }

    @Override
    protected FileObject createFile(AbstractFileName fileName) throws Exception {
        S3FileObject s3FileObject = new S3FileObject((S3FileName) fileName, this);

        return s3FileObject;
    }

    @Override
    protected void doCloseCommunicationLink() {
//        service.shutdown();
    }

    public boolean isPerFileLocking() {
        return perFileLocking;
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
            return service.doesBucketExistV2(bucketName);
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
     * Discovers if Capability.PER_FILE_THREAD_LOCKING is available
     * @return Capability.PER_FILE_THREAD_LOCKING if it is available, null otherwise
     */
    private static Optional<Capability> discoverPerFileThreadLockingCapability() {
        try {
            return Optional.of(Capability.valueOf("PER_FILE_THREAD_LOCKING"));
        } catch (IllegalArgumentException e) {
            return empty();
        }
    }
}
