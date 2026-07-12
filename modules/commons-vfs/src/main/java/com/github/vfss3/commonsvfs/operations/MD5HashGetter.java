package com.github.vfss3.commonsvfs.operations;

import com.github.vfss3.commonsvfs.S3FileObject;
import org.apache.commons.vfs2.FileSystemException;

/**
 * File operation that reads the MD5 hash of an S3 object.
 *
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public class MD5HashGetter implements IMD5HashGetter {
    private final S3FileObject file;

    /**
     * @param file the S3 file object to compute the MD5 hash for
     */
    public MD5HashGetter(S3FileObject file) {
        this.file = file;
    }

    /**
     * Returns the MD5 hash of the underlying S3 object, or {@code null} when it is not available.
     */
    @Override
    public String getMD5Hash() throws FileSystemException {
        return file.getMD5Hash().orElse(null);
    }

    /**
     * No-op: the MD5 hash is read on demand, so this operation has nothing to execute.
     */
    @Override
    public void process() throws FileSystemException {

        // Do nothing
    }
}
