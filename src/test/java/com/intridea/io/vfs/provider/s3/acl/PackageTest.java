package com.intridea.io.vfs.provider.s3.acl;

import com.intridea.io.vfs.TestEnvironment;
import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.IAclGetter;
import com.intridea.io.vfs.operations.IAclSetter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static com.intridea.io.vfs.operations.Acl.Group.*;
import static com.intridea.io.vfs.operations.Acl.Permission.READ;
import static com.intridea.io.vfs.operations.Acl.Permission.WRITE;

public class PackageTest {
    private FileSystemManager fsManager;
    private FileObject file;
    private Acl fileAcl;
    private String bucketName;

    @BeforeClass
    public void setUp() throws IOException {
        Properties config = TestEnvironment.getInstance().getConfig();
        bucketName = config.getProperty("s3.testBucket", "vfs-s3-tests");

        fsManager = VFS.getManager();
        file = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
    }

    @Test
    public void getAcl() throws FileSystemException {
        // Get ACL
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
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

    @Test(dependsOnMethods="getAcl")
    public void setAcl() throws FileSystemException {
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

    @Test(dependsOnMethods="setAcl")
    public void setAcl2() throws FileSystemException {
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
        if (fileAcl != null) {
            fileAcl.denyAll();
            fileAcl.allow(OWNER);

            // Set ACL
            IAclSetter aclSetter = (IAclSetter) file.getFileOperations().getOperation(IAclSetter.class);
            aclSetter.setAcl(fileAcl);
            aclSetter.process();
        }
    }
}
