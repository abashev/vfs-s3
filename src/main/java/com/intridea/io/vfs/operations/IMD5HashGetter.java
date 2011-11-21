package com.intridea.io.vfs.operations;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

/**
 * Get md5 hash for file.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 * @version $Id$
 */
public interface IMD5HashGetter extends FileOperation {
    /**
     * Get MD5 hash for object.
     *
     * @return
     */
    String getMD5Hash() throws FileSystemException;
}
