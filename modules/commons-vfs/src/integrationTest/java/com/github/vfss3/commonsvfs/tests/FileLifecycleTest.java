package com.github.vfss3.commonsvfs.tests;

import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.IMAGINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileLifecycleTest {
    private static final String PREFIX = "/file-lifecycle/";

    private FileObject root;

    @BeforeAll
    void resolveRoot() throws FileSystemException {
        root = VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        if (root.exists()) {
            root.delete(Selectors.EXCLUDE_SELF);
        }
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        FileObject prefixDir = root.resolveFile(PREFIX);
        if (prefixDir.exists()) {
            prefixDir.deleteAll();
        }
    }

    @Test
    void testCreateFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertTrue(file.exists(), "File should exist after creation");
    }

    @Test
    void testCreateFileWithSpace() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "name with space");
        file.createFile();

        assertTrue(file.exists(), "File with space in name should exist after creation");
    }

    @Test
    void testLastModifiedTime() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        long lastModified = file.getContent().getLastModifiedTime();
        assertThat(lastModified).as("Last modified time should be positive").isGreaterThan(0);

        assertThrows(
                FileSystemException.class,
                () -> file.getContent().setLastModifiedTime(111L),
                "Setting last modified time should throw FileSystemException");
    }

    @Test
    void testCreateFolderOnExistingFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertThrows(
                FileSystemException.class,
                () -> file.createFolder(),
                "Creating folder on existing file should throw FileSystemException");
    }

    @Test
    void testMoveAndRename() throws FileSystemException {
        FileObject sourceFile = root.resolveFile(PREFIX + "test-file");
        sourceFile.createFile();

        FileObject targetFile = root.resolveFile(PREFIX + "renamed");

        sourceFile.moveTo(targetFile);

        assertTrue(targetFile.exists(), "Renamed file should exist");
        assertFalse(sourceFile.exists(), "Original file should not exist after move");

        targetFile.moveTo(sourceFile);

        assertTrue(sourceFile.exists(), "Original file should exist after move back");
        assertFalse(targetFile.exists(), "Renamed file should not exist after move back");

        assertThrows(
                FileSystemException.class,
                () -> sourceFile.moveTo(sourceFile),
                "Moving file to itself should throw FileSystemException");
    }

    @Test
    void testGetType() throws FileSystemException {
        FileObject nonexistent = root.resolveFile(PREFIX + "nonexistent");
        assertEquals(IMAGINARY, nonexistent.getType(), "Nonexistent file should have IMAGINARY type");

        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertEquals(FILE, file.getType(), "Existing file should have FILE type");
    }

    @Test
    void testNonexistentPathDoesNotExist() throws FileSystemException {
        FileObject nonexistent = root.resolveFile(PREFIX + "does/not/exist");

        assertFalse(nonexistent.exists(), "Nonexistent path with multiple segments should not exist");
    }
}
