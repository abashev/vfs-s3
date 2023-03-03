package com.github.vfss3;

import com.github.vfss3.operations.Acl;
import com.github.vfss3.operations.Acl.Group;
import com.github.vfss3.operations.Acl.Permission;
import com.github.vfss3.operations.IAclGetter;
import com.github.vfss3.operations.IAclSetter;
import com.github.vfss3.operations.PlatformFeatures;
import com.github.vfss3.support.BaseIntegrationTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.testng.annotations.Test;

import static com.github.vfss3.operations.Acl.Group.*;
import static com.github.vfss3.operations.Acl.Permission.READ;
import static com.github.vfss3.operations.Acl.Permission.WRITE;
import static org.testng.Assert.*;

public class AclHandlingTest extends BaseIntegrationTest {
    private static final String FOLDER = "/acl";

    FileObject file;
    FileObject folder;
    Acl fileAcl;

    @Test
    public void checkGet() throws FileSystemException {
        file = root.resolveFile(FOLDER + "/check_acl.zip");

        if (!((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).supportsAcl()) {
            return;
        }

        if (!file.exists()) {
            file.copyFrom(binaryFile(), Selectors.SELECT_SELF);
        }

        // Get ACL
        fileAcl = getAcl(file);

        assertNotNull(fileAcl);

        // Default permissions
        // By default AWS owner can read but for Yandex all access forbidden
        if (((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).defaultAllowForOwner()) {
            assertAllowed(fileAcl, OWNER);
        } else {
            assertDenied(fileAcl, OWNER);
        }

        assertDenied(fileAcl, AUTHORIZED);
        assertDenied(fileAcl, EVERYONE);
    }

    @Test(dependsOnMethods = "checkGet")
    public void checkSet() throws FileSystemException {
        if (!((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).supportsAcl()) {
            return;
        }

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
        if (!((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).supportsAuthorizedGroup()) {
            // Doesn't support authorized group
            return;
        }

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
        if (!((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).supportsAcl()) {
            return;
        }

        // Set deny to all
        if (((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).allowDenyForOwner()) {
            fileAcl.denyAll();

            fileAcl.deny(OWNER, READ);
            fileAcl.deny(AUTHORIZED, READ);
        }

        fileAcl.deny(EVERYONE, READ);

        setAcl(file, fileAcl);

        // Verify
        file.refresh();

        Acl changedAcl = getAcl(file);

        if (((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).allowDenyForOwner()) {
            assertDenied(changedAcl, OWNER);
            assertDenied(changedAcl, AUTHORIZED);
        }

        assertDenied(changedAcl, EVERYONE);
    }

    @Test(dependsOnMethods = {"checkSet2"})
    public void checkDenyAllForFolder() throws FileSystemException {
        folder = root.resolveFile(FOLDER + "/check_acl/");

        if (!((PlatformFeatures) folder.getFileOperations().getOperation(PlatformFeatures.class)).supportsAcl()) {
            return;
        }

        if (!folder.exists()) {
            folder.createFolder();
        }

        Acl folderAcl = getAcl(folder);

        // Set deny to all
        if (((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).allowDenyForOwner()) {
            folderAcl.denyAll();
        }

        setAcl(folder, folderAcl);

        // Verify
        folder.refresh();

        Acl changedAcl = getAcl(folder);

        if (((PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class)).allowDenyForOwner()) {
            assertDenied(changedAcl, OWNER);
            assertDenied(changedAcl, AUTHORIZED);
            assertDenied(changedAcl, EVERYONE);
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
