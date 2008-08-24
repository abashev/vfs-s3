package com.intridea.io.vfs.operations.acl;

import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;

public interface IAclSetter extends FileOperation {

	public void setAcl(Acl acl);

	public void process() throws FileSystemException;

}