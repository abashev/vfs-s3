package com.intridea.io.vfs.operations;

import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;

/**
 * Interface for getting file Access Control List.
 *
 * @author Marat Komarov
 */
public interface IAclGetter extends FileOperation {

    /**
     * Returns true when file is readable
     * @param group
     * @return
     */
    boolean canRead(Acl.Group group);

    /**
     * Returns true when file is writeable
     * @param group
     * @return
     */
    boolean canWrite(Acl.Group group);

    /**
     * Returns file ACL
     * @return
     */
    Acl getAcl();

    /**
     * Executes getter operation.
     * Must be called before aby other operation methods
     */
    void process() throws FileSystemException;
}
