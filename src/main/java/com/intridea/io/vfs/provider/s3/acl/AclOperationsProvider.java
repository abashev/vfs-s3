package com.intridea.io.vfs.provider.s3.acl;

import java.util.Collection;


import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;

import com.intridea.io.vfs.operations.acl.IAclGetter;
import com.intridea.io.vfs.operations.acl.IAclSetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;

public class AclOperationsProvider implements
		org.apache.commons.vfs.operations.FileOperationProvider {

	@SuppressWarnings("unchecked")
	public void collectOperations(Collection operationsList, FileObject file)
			throws FileSystemException {
		if (file instanceof S3FileObject) {
			operationsList.add(AclGetter.class);
			operationsList.add(AclSetter.class);
		}
	}

	/**
	 * Depending on operationClass return getter/setter for file access control list.
	 */
	public FileOperation getOperation(FileObject file, @SuppressWarnings("unchecked") Class operationClass)
			throws FileSystemException {
		if (file instanceof S3FileObject) {
			if (operationClass.equals(IAclGetter.class)) {
				// getter
				return new AclGetter((S3FileObject)file);
			} else if (operationClass.equals(IAclSetter.class)) {
				// setter
				return new AclSetter((S3FileObject)file);
			}
		}
		throw new FileSystemException(String.format("Operation %s is not provided for file %s", 
				operationClass.getName(), file.getName()));
	}

}
