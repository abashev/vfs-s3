package com.intridea.io.vfs.provider.s3.operations;

import org.apache.commons.vfs2.FileSystemException;

import com.intridea.io.vfs.operations.IPublicUrlsGetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 * @version $Id$
 */
class PublicUrlsGetter implements IPublicUrlsGetter {
    private final S3FileObject file;

    /**
     * @param file
     */
    public PublicUrlsGetter(S3FileObject file) {
        this.file = file;
    }

    /* (non-Javadoc)
     * @see com.intridea.io.vfs.operations.url.IPublicUrlsGetter#getHttpUrl()
     */
    @Override
    public String getHttpUrl() {
        return file.getHttpUrl();
    }

    /* (non-Javadoc)
     * @see com.intridea.io.vfs.operations.url.IPublicUrlsGetter#getPrivateUrl()
     */
    @Override
    public String getPrivateUrl() {
        return file.getPrivateUrl();
    }

    /* (non-Javadoc)
     * @see com.intridea.io.vfs.operations.url.IPublicUrlsGetter#getSignedUrl(long)
     */
    @Override
    public String getSignedUrl(int expireInSeconds) throws FileSystemException {
        return file.getSignedUrl(expireInSeconds);
    }

    /* (non-Javadoc)
     * @see org.apache.commons.vfs.operations.FileOperation#process()
     */
    @Override
    public void process() throws FileSystemException {
        // Nothing to do
    }
}
