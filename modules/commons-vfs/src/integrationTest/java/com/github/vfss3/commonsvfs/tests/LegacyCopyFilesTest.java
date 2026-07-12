package com.github.vfss3.commonsvfs.tests;

import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.Selectors.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import java.util.Arrays;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
final class CopyFilesTest {
    private FileObject root;

    @BeforeAll
    void resolveRoot() throws FileSystemException {
        root = VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        if (root.exists()) {
            root.delete(EXCLUDE_SELF);
            root.refresh();
        }
    }

    @Order(1)
    @Test
    void createDirOk() throws FileSystemException {
        assertTrue(root.exists());
        assertEquals(FOLDER, root.getName().getType());

        root.resolveFile("child-file.tmp").createFile();
        root.resolveFile("child-file2.tmp").createFile();
        root.resolveFile("child-dir").createFolder();
        root.resolveFile("child-dir/descendant.tmp").createFile();
        root.resolveFile("child-dir/descendant2.tmp").createFile();
        root.resolveFile("child-dir/descendant-dir").createFolder();

        FileObject[] files;
        files = root.findFiles(SELECT_CHILDREN);
        assertEquals(3, files.length);
        files = root.findFiles(SELECT_FOLDERS);
        assertEquals(3, files.length);
        files = root.findFiles(SELECT_FILES);
        assertEquals(4, files.length);
        files = root.findFiles(EXCLUDE_SELF);
        assertEquals(6, files.length);
    }

    @Order(2)
    @Test
    void copyInsideBucket() throws FileSystemException {
        FileObject testsDir = root.resolveFile("child-dir");
        FileObject testsDirCopy = root.resolveFile("child-dir-copy");

        assertTrue(testsDir.exists());
        assertFalse(testsDirCopy.exists());

        testsDirCopy.copyFrom(testsDir, SELECT_SELF_AND_CHILDREN);

        assertTrue(testsDirCopy.exists());

        FileObject[] files = testsDir.findFiles(SELECT_SELF_AND_CHILDREN);
        FileObject[] filesCopy = testsDirCopy.findFiles(SELECT_SELF_AND_CHILDREN);

        assertEquals(
                files.length, filesCopy.length, Arrays.deepToString(files) + " vs. " + Arrays.deepToString(filesCopy));
    }

    @Order(3)
    @Test
    void checkDelete() throws FileSystemException {
        assertTrue(root.delete(EXCLUDE_SELF) > 0);

        assertFalse(root.resolveFile("child-dir").exists());
    }
}
