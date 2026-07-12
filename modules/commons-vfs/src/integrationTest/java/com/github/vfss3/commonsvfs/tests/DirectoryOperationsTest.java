package com.github.vfss3.commonsvfs.tests;

import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.Selectors.EXCLUDE_SELF;
import static org.apache.commons.vfs2.Selectors.SELECT_ALL;
import static org.apache.commons.vfs2.Selectors.SELECT_CHILDREN;
import static org.apache.commons.vfs2.Selectors.SELECT_FILES;
import static org.apache.commons.vfs2.Selectors.SELECT_FOLDERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
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
 * Suite B: Directory Operations — see {@code docs/test-cases/b-directory-operations.md}.
 *
 * <p>Works in the isolated {@code /dir-ops/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
final class DirectoryOperationsTest {
    private static final String PREFIX = "/dir-ops/";

    private FileObject root;

    @BeforeAll
    void resolveRoot() throws FileSystemException {
        root = VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        FileObject prefix = root.resolveFile(PREFIX);
        if (prefix.exists()) {
            prefix.deleteAll();
        }
    }

    /** Step 1: create a folder and assert it exists with FOLDER type. */
    @Order(1)
    @Test
    void createFolder() throws FileSystemException {
        FileObject folder = root.resolveFile(PREFIX + "my-folder");
        folder.createFolder();

        assertTrue(folder.exists(), "Folder should exist after creation");
        assertEquals(FOLDER, folder.getName().getType(), "Created object should be a folder");
    }

    /** Step 2: createFile on an existing folder path fails. */
    @Order(2)
    @Test
    void createFileOnExistingFolder() throws FileSystemException {
        FileObject folder = root.resolveFile(PREFIX + "my-folder");
        folder.createFolder();

        assertThrows(
                FileSystemException.class,
                folder::createFile,
                "Creating file on existing folder should throw FileSystemException");
    }

    /** Step 3: create a folder whose name contains a space. */
    @Order(3)
    @Test
    void createFolderWithSpace() throws FileSystemException {
        FileObject folder = root.resolveFile(PREFIX + "folder with space");
        folder.createFolder();

        assertTrue(folder.exists(), "Folder with space in name should exist after creation");
    }

    /** Step 4: create five files inside a folder, then list its children. */
    @Order(4)
    @Test
    void listChildren() throws FileSystemException {
        FileObject folder = root.resolveFile(PREFIX + "my-folder");
        folder.createFolder();

        for (int i = 0; i < 5; i++) {
            folder.resolveFile(i + ".tmp").createFile();
        }

        assertEquals(5, folder.getChildren().length, "Folder should have 5 children");
    }

    /** Step 5: build a nested tree and check the find selectors. */
    @Order(5)
    @Test
    void findWithSelectors() throws FileSystemException {
        FileObject base = root.resolveFile(PREFIX + "find-tests");
        base.createFolder();

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

    /** Step 6: delete the children of the tree and confirm only the folder itself remains. */
    @Order(6)
    @Test
    void deleteChildren() throws FileSystemException {
        FileObject base = root.resolveFile(PREFIX + "find-tests");
        base.createFolder();

        base.resolveFile("child-file.tmp").createFile();
        base.resolveFile("child-dir").createFolder();
        base.resolveFile("child-dir/descendant.tmp").createFile();

        base.delete(EXCLUDE_SELF);

        assertEquals(1, base.findFiles(SELECT_ALL).length, "Only the folder itself should remain");
    }
}
