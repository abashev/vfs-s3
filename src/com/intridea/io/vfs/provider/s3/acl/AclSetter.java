package com.intridea.io.vfs.provider.s3.acl;


import org.apache.commons.vfs.FileSystemException;

import com.intridea.io.vfs.operations.acl.Acl;
import com.intridea.io.vfs.operations.acl.IAclSetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;

public class AclSetter implements IAclSetter {

	private S3FileObject file;
	
	private Acl acl;	
	
	public AclSetter(S3FileObject file) {
		this.file = file;
	}
	
	public void setAcl(Acl acl) {
		this.acl = acl;
	}
	
	public void process() throws FileSystemException {
		file.setAcl(acl);
	}
}
