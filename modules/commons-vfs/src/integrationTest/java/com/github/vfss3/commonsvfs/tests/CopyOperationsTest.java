package com.github.vfss3.commonsvfs.tests;

import static com.github.vfss3.commonsvfs.FileAssert.assertHasChildren;
import static org.apache.commons.vfs2.Selectors.EXCLUDE_SELF;
import static org.apache.commons.vfs2.Selectors.SELECT_CHILDREN;
import static org.apache.commons.vfs2.Selectors.SELECT_FILES;
import static org.apache.commons.vfs2.Selectors.SELECT_FOLDERS;
import static org.apache.commons.vfs2.Selectors.SELECT_SELF_AND_CHILDREN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import java.util.Arrays;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Suite E: Copy Operations — see {@code docs/test-cases/e-copy-operations.md}.
 *
 * <p>Works in the isolated {@code /copy/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}. The steps are ordered: each builds on the previous one.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CopyOperationsTest {
    private static final String PREFIX = "/copy/";

    private FileObject root;
    private FileObject base;

    @BeforeAll
    void resolveRoot() throws FileSystemException {
        root = VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        base = root.resolveFile(PREFIX);
        if (base.exists()) {
            base.deleteAll();
        }
        base.createFolder();
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        if (base.exists()) {
            base.deleteAll();
        }
    }

    /** Step 1: build a tree and verify the find selectors. */
    @Test
    @Order(1)
    void testCreateTree() throws FileSystemException {
        base.resolveFile("child-file.tmp").createFile();
        base.resolveFile("child-file2.tmp").createFile();
        base.resolveFile("child-dir").createFolder();
        base.resolveFile("child-dir/descendant.tmp").createFile();
        base.resolveFile("child-dir/descendant2.tmp").createFile();
        base.resolveFile("child-dir/descendant-dir").createFolder();

        assertEquals(3, base.findFiles(SELECT_CHILDREN).length, "SELECT_CHILDREN");
        assertEquals(3, base.findFiles(SELECT_FOLDERS).length, "SELECT_FOLDERS");
        assertEquals(4, base.findFiles(SELECT_FILES).length, "SELECT_FILES");
        assertEquals(6, base.findFiles(EXCLUDE_SELF).length, "EXCLUDE_SELF");
    }

    /** Step 2: copy a sub-directory and confirm the copy mirrors the source. */
    @Test
    @Order(2)
    void testCopyInsideBucket() throws FileSystemException {
        FileObject source = base.resolveFile("child-dir");
        FileObject copy = base.resolveFile("child-dir-copy");

        assertTrue(source.exists());
        assertFalse(copy.exists());

        copy.copyFrom(source, SELECT_SELF_AND_CHILDREN);

        assertTrue(copy.exists());

        FileObject[] sourceFiles = source.findFiles(SELECT_SELF_AND_CHILDREN);
        FileObject[] copyFiles = copy.findFiles(SELECT_SELF_AND_CHILDREN);

        assertEquals(
                sourceFiles.length,
                copyFiles.length,
                Arrays.deepToString(sourceFiles) + " vs. " + Arrays.deepToString(copyFiles));
    }

    /** Step 3: the prefix lists the original tree plus the copy. */
    @Test
    @Order(3)
    void testListChildren() throws FileSystemException {
        // The copy in step 2 was created through a child handle, so the prefix's cached child
        // list can be stale — refresh before listing.
        base.refresh();
        assertHasChildren(base, "child-file.tmp", "child-file2.tmp", "child-dir", "child-dir-copy");
    }

    /** Step 4: deleting the children removes the whole tree. */
    @Test
    @Order(4)
    void testDeleteChildren() throws FileSystemException {
        assertTrue(base.delete(EXCLUDE_SELF) > 0, "Delete should report a positive count");
        assertFalse(base.resolveFile("child-dir").exists(), "child-dir should be gone");
    }
}
