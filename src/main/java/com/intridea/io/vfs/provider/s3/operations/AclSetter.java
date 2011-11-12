package com.intridea.io.vfs.provider.s3.operations;


import org.apache.commons.vfs2.FileSystemException;

import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.IAclSetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;

class AclSetter implements IAclSetter {

    private S3FileObject file;

    private Acl acl;

    public AclSetter(S3FileObject file) {
        this.file = file;
    }

    @Override
    public void setAcl(Acl acl) {
        this.acl = acl;
    }

    @Override
    public void process() throws FileSystemException {
        file.setAcl(acl);
    }
}
