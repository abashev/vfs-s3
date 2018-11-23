package com.github.vfss3.operations;

import com.github.vfss3.S3FileObject;
import com.intridea.io.vfs.operations.Acl;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;
import org.apache.commons.vfs2.operations.FileOperationProvider;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class S3FileOperationsProvider implements FileOperationProvider {
    @Override
    public void collectOperations(Collection<Class<? extends FileOperation>> operationsList, FileObject file) throws FileSystemException {
        if (file instanceof S3FileObject) {
            operationsList.add(IAclGetter.class);
            operationsList.add(IAclSetter.class);
            operationsList.add(IPublicUrlsGetter.class);
            operationsList.add(IMD5HashGetter.class);

            // Deprecated operations
            operationsList.add(com.intridea.io.vfs.operations.IAclGetter.class);
            operationsList.add(com.intridea.io.vfs.operations.IAclSetter.class);
            operationsList.add(com.intridea.io.vfs.operations.IPublicUrlsGetter.class);
            operationsList.add(com.intridea.io.vfs.operations.IMD5HashGetter.class);
        }
    }

    @Override
    public FileOperation getOperation(FileObject file, Class<? extends FileOperation> operationClass) throws FileSystemException {
        requireNonNull(file);
        requireNonNull(operationClass);

        if (file instanceof S3FileObject) {
            final S3FileObject s3file = (S3FileObject) file;

            if (operationClass.equals(IAclGetter.class)) {
                // getter
                return new AclGetter(s3file);
            } else if (operationClass.equals(IAclSetter.class)) {
                // setter
                return new AclSetter(s3file);
            } else if (operationClass.equals(IPublicUrlsGetter.class)) {
                // public urls
                return new PublicUrlsGetter(s3file);
            } else if (operationClass.equals(IMD5HashGetter.class)) {
                // get md5 hash
                return new MD5HashGetter(s3file);
            } else if (operationClass.equals(com.intridea.io.vfs.operations.IAclGetter.class)) {
                return new DeprecatedAclGetter(new AclGetter(s3file));
            } else if (operationClass.equals(com.intridea.io.vfs.operations.IAclSetter.class)) {
                return new DeprecatedAclSetter(new AclSetter(s3file));
            } else if (operationClass.equals(com.intridea.io.vfs.operations.IPublicUrlsGetter.class)) {
                return new DeprecatedPublicUrlsGetter(new PublicUrlsGetter(s3file));
            } else if (operationClass.equals(com.intridea.io.vfs.operations.IMD5HashGetter.class)) {
                return new DeprecatedMD5HashGetter(new MD5HashGetter(s3file));
            }
        }

        throw new FileSystemException(
                "Operation " + operationClass.getName() + " is not provided for file " + file.getName()
        );
    }

    private class DeprecatedAclGetter implements com.intridea.io.vfs.operations.IAclGetter {
        private final AclGetter getter;

        DeprecatedAclGetter(AclGetter getter) {
            this.getter = getter;
        }

        @Override
        public boolean canRead(Acl.Group group) {
            return getter.canRead(unwrap(group));
        }

        @Override
        public boolean canWrite(Acl.Group group) {
            return getter.canWrite(unwrap(group));
        }

        @Override
        public Acl getAcl() {
            return Acl.wrap(getter.getAcl());
        }

        @Override
        public void process() throws FileSystemException {
            getter.process();
        }

        private com.github.vfss3.operations.Acl.Group unwrap(Acl.Group group) {
            switch (group) {
                case OWNER: return com.github.vfss3.operations.Acl.Group.OWNER;
                case AUTHORIZED: return com.github.vfss3.operations.Acl.Group.AUTHORIZED;
                case EVERYONE: return com.github.vfss3.operations.Acl.Group.EVERYONE;
            }

            throw new IllegalStateException("Wrong state for deprecated group - " + this);
        }
    }

    private class DeprecatedAclSetter implements com.intridea.io.vfs.operations.IAclSetter {
        private final AclSetter setter;

        DeprecatedAclSetter(AclSetter setter) {
            this.setter = setter;
        }

        @Override
        public void setAcl(Acl acl) {
            setter.setAcl(acl.unwrap());
        }

        @Override
        public void process() throws FileSystemException {
            setter.process();
        }
    }

    private class DeprecatedPublicUrlsGetter implements com.intridea.io.vfs.operations.IPublicUrlsGetter {
        private final PublicUrlsGetter getter;

        DeprecatedPublicUrlsGetter(PublicUrlsGetter getter) {
            this.getter = getter;
        }

        @Override
        public String getHttpUrl() {
            return getter.getHttpUrl();
        }

        @Override
        public String getPrivateUrl() {
            throw new UnsupportedOperationException("Not able to get private url");
        }

        @Override
        public String getSignedUrl(int expireInSeconds) throws FileSystemException {
            return getter.getSignedUrl(expireInSeconds);
        }

        @Override
        public void process() throws FileSystemException {
            getter.process();
        }
    }

    private class DeprecatedMD5HashGetter implements com.intridea.io.vfs.operations.IMD5HashGetter {
        private final MD5HashGetter getter;

        DeprecatedMD5HashGetter(MD5HashGetter getter) {
            this.getter = getter;
        }

        @Override
        public String getMD5Hash() throws FileSystemException {
            return getter.getMD5Hash();
        }

        @Override
        public void process() throws FileSystemException {
            getter.process();
        }
    }
}
