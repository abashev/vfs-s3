package com.github.vfss3.commonsvfs.operations;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

/**
 * File operation for gettin' direct urls to S3 objects.
 *
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public interface IPublicUrlsGetter extends FileOperation {
    /**
     * Get direct http url to file.
     *
     * @return the direct HTTP URL to the object
     */
    String getHttpUrl();

    /**
     * Get a time-limited signed url to the file.
     *
     * @param expireInSeconds how long the signed URL stays valid, in seconds
     * @return a pre-signed URL granting temporary access to the object
     */
    String getSignedUrl(int expireInSeconds) throws FileSystemException;
}
