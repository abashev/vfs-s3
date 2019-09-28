package com.intridea.io.vfs.provider.s3;

import com.github.vfss3.operations.Acl;
import com.github.vfss3.operations.IAclGetter;
import com.github.vfss3.operations.IAclSetter;
import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.github.vfss3.operations.Acl.Group.AUTHORIZED;
import static com.github.vfss3.operations.Acl.Group.EVERYONE;
import static com.github.vfss3.operations.Acl.Group.OWNER;
import static com.github.vfss3.operations.Acl.Permission.READ;
import static com.github.vfss3.operations.Acl.Permission.WRITE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AclHandlingTest extends AbstractS3FileSystemTest {
    private FileObject file;
    private Acl fileAcl;

    @Test
    public void checkGet() throws FileSystemException {
        file = resolveFile("/acl/check_acl.zip");

        if (!file.exists()) {
            final File backupFile = binaryFile();

            assertTrue(backupFile.exists(), "Backup file should exists");

            FileObject src = vfs.resolveFile(backupFile.getAbsolutePath());

            file.copyFrom(src, Selectors.SELECT_SELF);
        }

        // Get ACL
        fileAcl = getAcl(file);

        assertNotNull(fileAcl);

        // Owner can read/write
//        Assert.assertTrue(aclGetter.canRead(OWNER));
//        Assert.assertTrue(aclGetter.canWrite(OWNER));
//
//        // Authorized coldn't read/write
//        Assert.assertFalse(aclGetter.canRead(AUTHORIZED));
//        Assert.assertFalse(aclGetter.canWrite(AUTHORIZED));
//
//        // Guest also coldn't read/write
//        Assert.assertFalse(aclGetter.canRead(EVERYONE));
//        Assert.assertFalse(aclGetter.canWrite(EVERYONE));
    }

    @Test(dependsOnMethods = "checkGet")
    public void checkSet() throws FileSystemException {
        // Set allow read to Guest
        fileAcl.allow(EVERYONE, READ);

        setAcl(file, fileAcl);

        // Verify
        file.refresh();

        Acl changedAcl = getAcl(file);

        // Guest can read
        assertTrue(changedAcl.isAllowed(EVERYONE, READ));
        // Write rules for guest not changed
        assertEquals(
            changedAcl.isAllowed(EVERYONE, WRITE),
            fileAcl.isAllowed(EVERYONE, WRITE)
        );
        // Read rules not spreaded to another groups
        assertEquals(
            changedAcl.isAllowed(AUTHORIZED, READ),
            fileAcl.isAllowed(AUTHORIZED, READ)
        );
        assertEquals(
            changedAcl.isAllowed(OWNER, READ),
            fileAcl.isAllowed(OWNER, READ)
        );

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods = "checkGet")
    public void checkSet2() throws FileSystemException {
        // Set allow all to Authorized
        fileAcl.allow(AUTHORIZED);

        setAcl(file, fileAcl);

        // Verify
        file.refresh();

        Acl changedAcl = getAcl(file);

        // Authorized can do everything
        assertTrue(changedAcl.isAllowed(AUTHORIZED, READ));
        assertTrue(changedAcl.isAllowed(AUTHORIZED, WRITE));

        // All other rules not changed
        assertEquals(
            changedAcl.isAllowed(EVERYONE, READ),
            fileAcl.isAllowed(EVERYONE, READ)
        );
        assertEquals(
            changedAcl.isAllowed(EVERYONE, WRITE),
            fileAcl.isAllowed(EVERYONE, WRITE)
        );
        assertEquals(
            changedAcl.isAllowed(OWNER, READ),
            fileAcl.isAllowed(OWNER, READ)
        );
        assertEquals(
            changedAcl.isAllowed(OWNER, WRITE),
            fileAcl.isAllowed(OWNER, WRITE)
        );

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods = {"checkSet2"})
    public void checkSet3() throws FileSystemException {
        // Set deny to all
        fileAcl.denyAll();

        setAcl(file, fileAcl);

        // Verify
        file.refresh();

        Acl changedAcl = getAcl(file);

        assertTrue(changedAcl.isDenied(OWNER, READ));
        assertTrue(changedAcl.isDenied(OWNER, WRITE));
        assertTrue(changedAcl.isDenied(AUTHORIZED, READ));
        assertTrue(changedAcl.isDenied(AUTHORIZED, WRITE));
        assertTrue(changedAcl.isDenied(EVERYONE, READ));
        assertTrue(changedAcl.isDenied(EVERYONE, WRITE));
    }

    @AfterClass
    public void restoreAcl() throws FileSystemException {
        if (file != null) {
            file.delete();
        }
    }

    private void setAcl(FileObject file, Acl acl) throws FileSystemException {
        IAclSetter aclSetter = (IAclSetter) file.getFileOperations().getOperation(IAclSetter.class);

        aclSetter.setAcl(acl);
        aclSetter.process();
    }

    private Acl getAcl(FileObject file) throws FileSystemException {
        IAclGetter getter = (IAclGetter) file.getFileOperations().getOperation(IAclGetter.class);

        getter.process();

        return getter.getAcl();
    }
}
