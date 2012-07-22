package com.intridea.io.vfs.provider.s3;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Bucket;

/**
 * An S3 file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 */
public class S3FileSystem extends AbstractFileSystem {
    private final Log log = LogFactory.getLog(S3FileSystem.class);

    private S3Service service;
    private String bucketId;

    public S3FileSystem(
            S3FileName fileName, S3Service service, FileSystemOptions fileSystemOptions
    ) throws FileSystemException {
        super(fileName, null, fileSystemOptions);

        this.bucketId = fileName.getBucketId();
        this.service = service;
//      TODO Must be controller by FileSystemOptions
//            if (!service.isBucketAccessible(bucketId)) {
//                bucket = service.createBucket(bucketId);
//            }
        if (log.isInfoEnabled()) {
            log.info("Created new S3 FileSystem " + bucketId);
        }
    }

    @Override
    protected void addCapabilities(Collection<Capability> caps) {
        caps.addAll(S3FileProvider.capabilities);
    }

    @Override
    protected FileObject createFile(AbstractFileName fileName) throws Exception {
        return new S3FileObject(fileName, this, service, new S3Bucket(bucketId));
    }
}
