package com.intridea.io.vfs.provider.s3;

import com.github.vfss3.operations.Acl;
import com.github.vfss3.operations.Acl.Group;
import com.github.vfss3.operations.Acl.Permission;
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
    FileObject file;
    FileObject folder;
    Acl fileAcl;

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

        // Default permissions
        // By default AWS owner can read but for Yandex all access forbidden
        // assertAllowed(fileAcl, OWNER);
        assertDenied(fileAcl, AUTHORIZED);
        assertDenied(fileAcl, EVERYONE);
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
        assertAllowed(changedAcl, EVERYONE, READ);

        // Write rules for guest not changed
        assertSameAllowed(changedAcl, fileAcl, EVERYONE, WRITE);
        // Read rules not spreaded to another groups
        assertSameAllowed(changedAcl, fileAcl, AUTHORIZED, READ);
        assertSameAllowed(changedAcl, fileAcl, OWNER, READ);

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
        assertAllowed(changedAcl, AUTHORIZED);

        // All other rules not changed
        assertSameAllowed(changedAcl, fileAcl, EVERYONE, READ);
        assertSameAllowed(changedAcl, fileAcl, EVERYONE, WRITE);
        assertSameAllowed(changedAcl, fileAcl, OWNER, READ);
        assertSameAllowed(changedAcl, fileAcl, OWNER, WRITE);

        fileAcl = changedAcl;
    }

    @Test(dependsOnMethods = {"checkSet2"})
    public void checkDenyAllForFile() throws FileSystemException {
        // Set deny to all
        fileAcl.denyAll();

        setAcl(file, fileAcl);

        // Verify
        file.refresh();

        Acl changedAcl = getAcl(file);

        assertDenied(changedAcl, OWNER);
        assertDenied(changedAcl, AUTHORIZED);
        assertDenied(changedAcl, EVERYONE);
    }

    @Test(dependsOnMethods = {"checkSet2"})
    public void checkDenyAllForFolder() throws FileSystemException {
        folder = resolveFile("/acl/check_acl/");

        if (!folder.exists()) {
            folder.createFolder();
        }

        Acl folderAcl = getAcl(folder);

        // Set deny to all
        folderAcl.denyAll();

        setAcl(folder, folderAcl);

        // Verify
        folder.refresh();

        Acl changedAcl = getAcl(folder);

        assertDenied(changedAcl, OWNER);
        assertDenied(changedAcl, AUTHORIZED);
        assertDenied(changedAcl, EVERYONE);
    }

    @AfterClass
    public void restoreAcl() throws FileSystemException {
        if (file != null) {
            file.delete();
        }

        if (folder != null) {
            folder.delete();
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

    private void assertAllowed(Acl acl, Group group, Permission permission) {
        assertTrue(acl.isAllowed(group, permission));
    }

    private void assertAllowed(Acl acl, Group group) {
        assertTrue(acl.isAllowed(group, READ));
        assertTrue(acl.isAllowed(group, WRITE));
    }

    private void assertDenied(Acl acl, Group group) {
        assertTrue(acl.isDenied(group, READ));
        assertTrue(acl.isDenied(group, WRITE));
    }

    private void assertSameAllowed(Acl actual, Acl expected, Group group, Permission permission) {
        assertEquals(
                actual.isAllowed(group, permission),
                expected.isAllowed(group, permission)
        );
    }
}
