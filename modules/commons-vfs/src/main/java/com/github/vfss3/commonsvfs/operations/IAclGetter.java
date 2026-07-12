package com.github.vfss3.commonsvfs.operations;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

/**
 * Interface for getting file Access Control List.
 *
 * @author Marat Komarov
 */
public interface IAclGetter extends FileOperation {

    /**
     * Returns true when file is readable
     *
     * @param group the group to check read access for
     * @return {@code true} if the group may read the file
     */
    boolean canRead(Acl.Group group);

    /**
     * Returns true when file is writeable
     *
     * @param group the group to check write access for
     * @return {@code true} if the group may write the file
     */
    boolean canWrite(Acl.Group group);

    /**
     * Returns file ACL
     *
     * @return the file's access control list
     */
    Acl getAcl();

    /**
     * Executes getter operation.
     * Must be called before aby other operation methods
     */
    @Override
    void process() throws FileSystemException;
}
