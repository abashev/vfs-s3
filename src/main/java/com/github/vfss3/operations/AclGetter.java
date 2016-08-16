package com.intridea.io.vfs.provider.s3.operations;

import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.Acl.Group;
import com.intridea.io.vfs.operations.IAclGetter;
import com.intridea.io.vfs.provider.s3.S3FileObject;
import org.apache.commons.vfs2.FileSystemException;

import static com.intridea.io.vfs.operations.Acl.Permission.READ;
import static com.intridea.io.vfs.operations.Acl.Permission.WRITE;

class AclGetter implements IAclGetter {
    private S3FileObject file;
    private Acl acl;
    private boolean initialized = false;

    public AclGetter (S3FileObject file) {
        this.file = file;
    }

    @Override
    public boolean canRead(Group group) {
        if (!initialized) {
            throw new IllegalStateException("Call IAclGetter::process() method before using this object");
        }

        return acl.isAllowed(group, READ);
    }

    @Override
    public boolean canWrite(Group group) {
        if (!initialized) {
            throw new IllegalStateException("Call IAclGetter::process() method before using this object");
        }

        return acl.isAllowed(group, WRITE);
    }

    @Override
    public Acl getAcl() {
        if (!initialized) {
            throw new IllegalStateException("Call IAclGetter::process() method before using this object");
        }

        return acl;
    }

    @Override
    public void process() throws FileSystemException {
        initialized = true;
        acl = file.getAcl();
    }
}
