package com.intridea.io.vfs.provider.s3.acl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.intridea.io.vfs.TestEnvironment;
import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.IAclGetter;
import com.intridea.io.vfs.operations.IAclSetter;

@Test(groups={"storage", "acl"})
public class PackageTest {

    private FileSystemManager fsManager;

    private FileObject file;

    private Acl fileAcl;

    private String bucketName;

    @BeforeClass
    public void setUp () throws FileNotFoundException, IOException {
        Properties config = TestEnvironment.getInstance().getConfig();
        bucketName = config.getProperty("s3.testBucket", "vfs-s3-tests");

        fsManager = VFS.getManager();
        file = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
    }

    @Test
    public void getAcl () throws FileSystemException {
        // Get ACL
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        fileAcl = aclGetter.getAcl();

        // Owner can read/write
        Assert.assertTrue(aclGetter.canRead(Acl.Group.OWNER));
        Assert.assertTrue(aclGetter.canWrite(Acl.Group.OWNER));

        // Authorized coldn't read/write
        Assert.assertFalse(aclGetter.canRead(Acl.Group.AUTHORIZED));
        Assert.assertFalse(aclGetter.canWrite(Acl.Group.AUTHORIZED));

        // Guest also coldn't read/write
        Assert.assertFalse(aclGetter.canRead(Acl.Group.EVERYONE));
        Assert.assertFalse(aclGetter.canWrite(Acl.Group.EVERYONE));
    }

    @Test(dependsOnMethods="getAcl")
    public void setAcl () throws FileSystemException {
        // Set allow read to Guest
        fileAcl.allow(Acl.Group.EVERYONE, Acl.Permission.READ);
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();

        // Verify
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        Acl changedAcl = aclGetter.getAcl();

        // Guest can read
        Assert.assertTrue(changedAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.READ));
        // Write rules for guest not changed
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.WRITE),
            fileAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.WRITE)
        );
        // Read rules not spreaded to another groups
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ),
            fileAcl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.OWNER, Acl.Permission.READ),
            fileAcl.isAllowed(Acl.Group.OWNER, Acl.Permission.READ)
        );

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods="setAcl")
    public void setAcl2 () throws FileSystemException {
        // Set allow all to Authorized
        fileAcl.allow(Acl.Group.AUTHORIZED);
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();

        // Verify
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        Acl changedAcl = aclGetter.getAcl();

        // Authorized can do everything
        Assert.assertTrue(changedAcl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ));
        Assert.assertTrue(changedAcl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));

        // All other rules not changed
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.READ),
            fileAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.READ)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.WRITE),
            fileAcl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.WRITE)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.OWNER, Acl.Permission.READ),
            fileAcl.isAllowed(Acl.Group.OWNER, Acl.Permission.READ)
        );
        Assert.assertEquals(
            changedAcl.isAllowed(Acl.Group.OWNER, Acl.Permission.WRITE),
            fileAcl.isAllowed(Acl.Group.OWNER, Acl.Permission.WRITE)
        );

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods={"setAcl2"})
    public void setAcl3 () throws FileSystemException {
        // Set deny to all
        fileAcl.denyAll();
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();

        // Verify
        IAclGetter aclGetter = (IAclGetter)file.getFileOperations().getOperation(IAclGetter.class);
        aclGetter.process();
        Acl changedAcl = aclGetter.getAcl();

        Assert.assertTrue(changedAcl.isDenied(Acl.Group.OWNER, Acl.Permission.READ));
        Assert.assertTrue(changedAcl.isDenied(Acl.Group.OWNER, Acl.Permission.WRITE));
        Assert.assertTrue(changedAcl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.READ));
        Assert.assertTrue(changedAcl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(changedAcl.isDenied(Acl.Group.EVERYONE, Acl.Permission.READ));
        Assert.assertTrue(changedAcl.isDenied(Acl.Group.EVERYONE, Acl.Permission.WRITE));
    }

    @AfterClass
    public void restoreAcl () throws FileSystemException {
        fileAcl.denyAll();
        fileAcl.allow(Acl.Group.OWNER);

        // Set ACL
        IAclSetter aclSetter = (IAclSetter)file.getFileOperations().getOperation(IAclSetter.class);
        aclSetter.setAcl(fileAcl);
        aclSetter.process();
    }
}
