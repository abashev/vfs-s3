package com.intridea.io.vfs.provider.s3.url;

import java.util.Collection;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.operations.FileOperation;
import org.apache.commons.vfs.operations.FileOperationProvider;

import com.intridea.io.vfs.provider.s3.S3FileObject;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 * @version $Id$
 */
public class PublicUrlsOperationsProvider implements FileOperationProvider {
    /* (non-Javadoc)
     * @see org.apache.commons.vfs.operations.FileOperationProvider#collectOperations(java.util.Collection, org.apache.commons.vfs.FileObject)
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void collectOperations(Collection operationsList, FileObject file) throws FileSystemException {
        if (file instanceof S3FileObject) {
            operationsList.add(PublicUrlsGetter.class);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.commons.vfs.operations.FileOperationProvider#getOperation(org.apache.commons.vfs.FileObject, java.lang.Class)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public FileOperation getOperation(FileObject file, Class operationClass) throws FileSystemException {
        if (file instanceof S3FileObject) {
            if (operationClass.equals(PublicUrlsGetter.class)) {
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
