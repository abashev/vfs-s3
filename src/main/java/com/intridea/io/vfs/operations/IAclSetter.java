package com.intridea.io.vfs.operations;

import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;

/**
 * Interface for setting file Access Control List.
 *
 * @author Marat Komarov
 */
public interface IAclSetter extends FileOperation {

    /**
     * Sets file Access Control List.
     * @param acl
     */
    public void setAcl(Acl acl);

    /**
     * Executes setter operations.
     * Must be called after setAcl.
     */
    public void process() throws FileSystemException;

}
