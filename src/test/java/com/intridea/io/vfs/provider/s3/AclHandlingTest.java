package com.intridea.io.vfs.provider.s3;

import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.IAclGetter;
import com.intridea.io.vfs.operations.IAclSetter;
import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static com.intridea.io.vfs.operations.Acl.Group.AUTHORIZED;
import static com.intridea.io.vfs.operations.Acl.Group.EVERYONE;
import static com.intridea.io.vfs.operations.Acl.Group.OWNER;
import static com.intridea.io.vfs.operations.Acl.Permission.READ;
import static com.intridea.io.vfs.operations.Acl.Permission.WRITE;
import static org.testng.Assert.assertTrue;

public class AclHandlingTest extends AbstractS3FileSystemTest {
    private FileObject file;
    private Acl fileAcl;
    
    @BeforeClass
    public void setUp() throws IOException {
        file = resolveFile("/acl/check_acl.zip");

        if (!file.exists()) {
            final File backupFile = binaryFile();

            assertTrue(backupFile.exists(), "Backup file should exists");

            FileObject src = vfs.resolveFile(backupFile.getAbsolutePath());

            file.copyFrom(src, Selectors.SELECT_SELF);
        }
    }

    @Test
    public void getAcl() throws FileSystemException {
        if (!file.exists()) {
            Assert.fail("Target file should be in place");
        }

//        FileObject[] files = file.findFiles(Selectors.SELECT_CHILDREN);

//        System.out.println(">>> files " + Arrays.toString(files));

        // Get ACL
        IAclGetter aclGetter = (IAclGetter) file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        fileAcl = aclGetter.getAcl();

        // Owner can read/write
        Assert.assertTrue(aclGetter.canRead(OWNER));
        Assert.assertTrue(aclGetter.canWrite(OWNER));

        // Authorized coldn't read/write
        Assert.assertFalse(aclGetter.canRead(AUTHORIZED));
        Assert.assertFalse(aclGetter.canWrite(AUTHORIZED));

        // Guest also coldn't read/write
        Assert.assertFalse(aclGetter.canRead(EVERYONE));
        Assert.assertFalse(aclGetter.canWrite(EVERYONE));
    }

    @Test(dependsOnMethods = "getAcl")
    public void setAcl() throws FileSystemException {
        if (!file.exists()) {
            Assert.fail("Target file should be in place");
        }

        // Set allow read to Guest
        fileAcl.allow(EVERYONE, READ);
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();

        // Verify
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        Acl changedAcl = aclGetter.getAcl();

        // Guest can read
        Assert.assertTrue(changedAcl.isAllowed(EVERYONE, READ));
        // Write rules for guest not changed
        Assert.assertEquals(
            changedAcl.isAllowed(EVERYONE, WRITE),
            fileAcl.isAllowed(EVERYONE, WRITE)
        );
        // Read rules not spreaded to another groups
        Assert.assertEquals(
            changedAcl.isAllowed(AUTHORIZED, READ),
            fileAcl.isAllowed(AUTHORIZED, READ)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(OWNER, READ),
            fileAcl.isAllowed(OWNER, READ)
        );

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods = "setAcl")
    public void setAcl2() throws FileSystemException {
        if (!file.exists()) {
            Assert.fail("Target file should be in place");
        }

        // Set allow all to Authorized
        fileAcl.allow(AUTHORIZED);
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();

        // Verify
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        Acl changedAcl = aclGetter.getAcl();

        // Authorized can do everything
        Assert.assertTrue(changedAcl.isAllowed(AUTHORIZED, READ));
        Assert.assertTrue(changedAcl.isAllowed(AUTHORIZED, WRITE));

        // All other rules not changed
        Assert.assertEquals(
            changedAcl.isAllowed(EVERYONE, READ),
            fileAcl.isAllowed(EVERYONE, READ)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(EVERYONE, WRITE),
            fileAcl.isAllowed(EVERYONE, WRITE)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(OWNER, READ),
            fileAcl.isAllowed(OWNER, READ)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(OWNER, WRITE),
            fileAcl.isAllowed(OWNER, WRITE)
        );

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods = {"setAcl2"})
    public void setAcl3() throws FileSystemException {
        // Set deny to all
        fileAcl.denyAll();
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();

        // Verify
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        Acl changedAcl = aclGetter.getAcl();

        Assert.assertTrue(changedAcl.isDenied(OWNER, READ));
        Assert.assertTrue(changedAcl.isDenied(OWNER, WRITE));
        Assert.assertTrue(changedAcl.isDenied(AUTHORIZED, READ));
        Assert.assertTrue(changedAcl.isDenied(AUTHORIZED, WRITE));
        Assert.assertTrue(changedAcl.isDenied(EVERYONE, READ));
        Assert.assertTrue(changedAcl.isDenied(EVERYONE, WRITE));
    }

    @AfterClass
    public void restoreAcl() throws FileSystemException {
        if (file != null) {
            file.delete();
        }
    }
}
