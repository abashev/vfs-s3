package com.github.vfss3.commonsvfs.operations;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

/**
 * File operation to work with server-side encryption. Some cloud providers don't support it.
 *
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public interface ServerSideEncryption extends FileOperation {
    /**
     * No encryption for file.
     *
     * @return {@code true} if the file is stored without server-side encryption
     */
    boolean noEncryption() throws FileSystemException;

    /**
     * Check does file encrypted with algorithm or not.
     *
     * @param algorithm the server-side encryption algorithm to check for
     * @return {@code true} if the file is encrypted with the given algorithm
     */
    boolean encryptedWith(String algorithm) throws FileSystemException;
}
