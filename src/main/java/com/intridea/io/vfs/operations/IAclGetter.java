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
    public boolean canRead(Acl.Group group);

    /**
     * Returns true when file is writeable
     * @param group
     * @return
     */
    public boolean canWrite(Acl.Group group);

    /**
     * Returns file ACL
     * @return
     */
    public Acl getAcl();

    /**
     * Executes getter operation.
     * Must be called before aby other operation methods
     */
    public void process() throws FileSystemException;
}
