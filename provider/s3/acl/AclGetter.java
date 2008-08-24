package com.intridea.io.vfs.provider.s3.acl;

import org.apache.commons.vfs.FileSystemException;

import com.intridea.io.vfs.operations.acl.Acl;
import com.intridea.io.vfs.operations.acl.IAclGetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;


public class AclGetter implements IAclGetter {

	private S3FileObject file;
	
	private Acl acl;
	
	public AclGetter(S3FileObject file) {
		this.file = file; 
	}

	public boolean canRead (Acl.Group group) {
		return acl.isAllowed(group, Acl.Right.READ);
	}
	
	public boolean canWrite (Acl.Group group) {
		return acl.isAllowed(group, Acl.Right.WRITE);
	}
	
	public Acl getAcl () {
		return acl;
	}
	
	public void process() throws FileSystemException {
		acl = file.getAcl();
	}

}
