package com.intridea.io.vfs.provider.s3.operations;

import org.apache.commons.vfs2.FileSystemException;

import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.Acl.Group;
import com.intridea.io.vfs.operations.IAclGetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;

class AclGetter implements IAclGetter {

    private S3FileObject file;

    private Acl acl;

    public AclGetter (S3FileObject file) {
        this.file = file;
    }

    @Override
    public boolean canRead(Group group) {
        return acl.isAllowed(group, Acl.Permission.READ);
    }

    @Override
    public boolean canWrite(Group group) {
        return acl.isAllowed(group, Acl.Permission.WRITE);
    }

    @Override
    public Acl getAcl() {
        return acl;
    }

    @Override
    public void process() throws FileSystemException {
        acl = file.getAcl();
    }

}
