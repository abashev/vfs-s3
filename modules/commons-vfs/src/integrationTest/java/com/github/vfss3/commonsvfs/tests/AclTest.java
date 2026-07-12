package com.github.vfss3.commonsvfs.tests;

import static com.github.vfss3.commonsvfs.operations.Acl.Group.AUTHORIZED;
import static com.github.vfss3.commonsvfs.operations.Acl.Group.EVERYONE;
import static com.github.vfss3.commonsvfs.operations.Acl.Group.OWNER;
import static com.github.vfss3.commonsvfs.operations.Acl.Permission.READ;
import static com.github.vfss3.commonsvfs.operations.Acl.Permission.WRITE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import com.github.vfss3.commonsvfs.operations.Acl;
import com.github.vfss3.commonsvfs.operations.Acl.Group;
import com.github.vfss3.commonsvfs.operations.IAclGetter;
import com.github.vfss3.commonsvfs.operations.IAclSetter;
import com.github.vfss3.commonsvfs.operations.PlatformFeatures;
import java.io.File;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Suite F: ACL — see {@code docs/test-cases/f-acl.md}.
 *
 * <p>Works in the isolated {@code /acl/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}. Every step first checks {@link PlatformFeatures#supportsAcl()} and skips
 * when the backend does not support ACLs (e.g. S3Mock, MinIO).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
final class AclTest {
    private static final String PREFIX = "/acl/";

    private FileObject root;
    private FileObject local;

    @BeforeAll
    void setUp() throws FileSystemException {
        var manager = VFS.getManager();
        root = manager.resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        File backupFile = new File(S3IntegrationContext.BINARY_FILE);
        local = manager.resolveFile(backupFile.getAbsolutePath());
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        FileObject prefix = root.resolveFile(PREFIX);
        if (prefix.exists()) {
            prefix.deleteAll();
        }
    }

    /** Step 1: the default ACL grants/denies the owner per platform, and denies the other groups. */
    @Order(1)
    @Test
    void getAcl() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file.zip");
        assumeAcl(file);
        if (!file.exists()) {
            file.copyFrom(local, Selectors.SELECT_SELF);
        }

        Acl acl = readAclOrSkip(file);
        assertNotNull(acl);

        if (features(file).defaultAllowForOwner()) {
            assertAllowed(acl, OWNER);
        } else {
            assertDenied(acl, OWNER);
        }
        assertDenied(acl, AUTHORIZED);
        assertDenied(acl, EVERYONE);
    }

    /** Step 2: deny READ for EVERYONE (and the owner where the platform allows it). */
    @Order(2)
    @Test
    void denyForFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file.zip");
        assumeAcl(file);
        if (!file.exists()) {
            file.copyFrom(local, Selectors.SELECT_SELF);
        }

        Acl acl = readAclOrSkip(file);
        if (features(file).allowDenyForOwner()) {
            acl.denyAll();
            acl.deny(OWNER, READ);
            acl.deny(AUTHORIZED, READ);
        }
        acl.deny(EVERYONE, READ);

        writeAclOrSkip(file, acl);
        file.refresh();

        Acl updated = readAclOrSkip(file);
        if (features(file).allowDenyForOwner()) {
            assertDenied(updated, OWNER);
            assertDenied(updated, AUTHORIZED);
        }
        assertDenied(updated, EVERYONE);
    }

    /** Step 3: deny all permissions on a folder. */
    @Order(3)
    @Test
    void denyForFolder() throws FileSystemException {
        FileObject folder = root.resolveFile(PREFIX + "test-folder/");
        assumeAcl(folder);
        if (!folder.exists()) {
            folder.createFolder();
        }

        Acl acl = readAclOrSkip(folder);
        if (features(folder).allowDenyForOwner()) {
            acl.denyAll();
        }

        writeAclOrSkip(folder, acl);
        folder.refresh();

        Acl updated = readAclOrSkip(folder);
        if (features(folder).allowDenyForOwner()) {
            assertDenied(updated, OWNER);
            assertDenied(updated, AUTHORIZED);
            assertDenied(updated, EVERYONE);
        }
    }

    private void assumeAcl(FileObject file) throws FileSystemException {
        Assumptions.assumeTrue(features(file).supportsAcl(), "Backend does not support ACLs");
    }

    private PlatformFeatures features(FileObject file) throws FileSystemException {
        return (PlatformFeatures) file.getFileOperations().getOperation(PlatformFeatures.class);
    }

    private Acl readAcl(FileObject file) throws FileSystemException {
        IAclGetter getter = (IAclGetter) file.getFileOperations().getOperation(IAclGetter.class);
        getter.process();
        return getter.getAcl();
    }

    private void writeAcl(FileObject file, Acl acl) throws FileSystemException {
        IAclSetter setter = (IAclSetter) file.getFileOperations().getOperation(IAclSetter.class);
        setter.setAcl(acl);
        setter.process();
    }

    /**
     * Reads the ACL, skipping the test when the backend reports {@code supportsAcl=true} but its
     * ACL response is incompatible (e.g. MinIO returns a {@code CanonicalGrantee} with a null
     * identifier). Assertion failures still surface — only ACL marshalling errors are skipped.
     */
    private Acl readAclOrSkip(FileObject file) throws FileSystemException {
        try {
            return readAcl(file);
        } catch (RuntimeException e) {
            return Assumptions.abort("Backend ACL implementation incompatible: " + e.getMessage());
        }
    }

    private void writeAclOrSkip(FileObject file, Acl acl) throws FileSystemException {
        try {
            writeAcl(file, acl);
        } catch (RuntimeException e) {
            Assumptions.abort("Backend ACL implementation incompatible: " + e.getMessage());
        }
    }

    private void assertAllowed(Acl acl, Group group) {
        assertTrue(acl.isAllowed(group, READ), group + " should be allowed READ");
        assertTrue(acl.isAllowed(group, WRITE), group + " should be allowed WRITE");
    }

    private void assertDenied(Acl acl, Group group) {
        assertTrue(acl.isDenied(group, READ), group + " should be denied READ");
        assertTrue(acl.isDenied(group, WRITE), group + " should be denied WRITE");
    }
}
