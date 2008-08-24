package com.intridea.io.vfs.operations.acl;

import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;

public interface IAclGetter extends FileOperation {

	public boolean canRead(Acl.Group group);

	public boolean canWrite(Acl.Group group);

	public Acl getAcl();

	public void process() throws FileSystemException;

}