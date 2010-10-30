package com.intridea.io.vfs.provider.s3.operations;

import java.util.Collection;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;
import org.apache.commons.vfs.operations.FileOperationProvider;

import com.intridea.io.vfs.operations.IAclGetter;
import com.intridea.io.vfs.operations.IAclSetter;
import com.intridea.io.vfs.operations.IPublicUrlsGetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;

public class S3FileOperationsProvider implements FileOperationProvider {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void collectOperations(Collection operationsList, FileObject file) throws FileSystemException {
        if (file instanceof S3FileObject) {
            operationsList.add(AclGetter.class);
            operationsList.add(AclSetter.class);
            operationsList.add(PublicUrlsGetter.class);
        }
    }

    /**
     * Depending on operationClass return getter/setter for file access control list.
     */
    @SuppressWarnings("rawtypes")
    public FileOperation getOperation(FileObject file, Class operationClass) throws FileSystemException {
        if (file instanceof S3FileObject) {
            if (operationClass.equals(IAclGetter.class)) {
                // getter
                return new AclGetter((S3FileObject)file);
            } else if (operationClass.equals(IAclSetter.class)) {
                // setter
                return new AclSetter((S3FileObject)file);
            } else if (operationClass.equals(IPublicUrlsGetter.class)) {
                // public urls
                return new PublicUrlsGetter((S3FileObject)file);
            }
        }

        throw new FileSystemException(
                String.format(
                        "Operation %s is not provided for file %s",
                        operationClass.getName(),
                        file.getName()
                )
        );
    }
}
