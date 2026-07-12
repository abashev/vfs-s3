package com.github.vfss3.commonsvfs.tests;

import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.IMAGINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Suite A: File Lifecycle — see {@code docs/test-cases/a-file-lifecycle.md}.
 *
 * <p>Works in the isolated {@code /file-lifecycle/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}. Each test creates the files it needs so the suite is order-independent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class FileLifecycleTest {
    private static final String PREFIX = "/file-lifecycle/";

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

    /** Step 1: create a file and assert it exists. */
    @Test
    void createFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertTrue(file.exists(), "File should exist after creation");
    }

    /** Step 2: create a file whose name contains a space. */
    @Test
    void createFileWithSpace() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "name with space");
        file.createFile();

        assertTrue(file.exists(), "File with space in name should exist after creation");
    }

    /** Step 3: lastModifiedTime is positive; setLastModifiedTime is not supported. */
    @Test
    void lastModifiedTimeAndImmutability() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertThat(file.getContent().getLastModifiedTime())
                .as("Last modified time should be positive")
                .isGreaterThan(0);

        assertThrows(
                FileSystemException.class,
                () -> file.getContent().setLastModifiedTime(111L),
                "Setting last modified time should throw FileSystemException");
    }

    /** Step 4: createFolder on an existing file path fails. */
    @Test
    void createFolderOnExistingFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertThrows(
                FileSystemException.class,
                file::createFolder,
                "Creating folder on existing file should throw FileSystemException");
    }

    /** Step 5: move/rename a file there and back; moving onto itself fails. */
    @Test
    void moveAndRename() throws FileSystemException {
        FileObject sourceFile = root.resolveFile(PREFIX + "test-file");
        sourceFile.createFile();

        FileObject targetFile = root.resolveFile(PREFIX + "renamed");
        if (targetFile.exists()) {
            targetFile.delete();
        }

        sourceFile.moveTo(targetFile);

        assertTrue(targetFile.exists(), "Renamed file should exist");
        assertFalse(sourceFile.exists(), "Original file should not exist after move");

        targetFile.moveTo(sourceFile);

        assertTrue(sourceFile.exists(), "Original file should exist after moving back");
        assertFalse(targetFile.exists(), "Renamed file should not exist after moving back");

        assertThrows(
                FileSystemException.class,
                () -> sourceFile.moveTo(sourceFile),
                "Moving file onto itself should throw FileSystemException");
    }

    /** Step 6: IMAGINARY for a non-existent path, FILE for an existing file. */
    @Test
    void getType() throws FileSystemException {
        FileObject nonexistent = root.resolveFile(PREFIX + "nonexistent");
        assertEquals(IMAGINARY, nonexistent.getType(), "Nonexistent file should have IMAGINARY type");

        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertEquals(FILE, file.getType(), "Existing file should have FILE type");
    }

    /** Step 7: a deeply nested non-existent path simply does not exist. */
    @Test
    void nonexistentPathDoesNotExist() throws FileSystemException {
        FileObject nonexistent = root.resolveFile(PREFIX + "does/not/exist");

        assertFalse(nonexistent.exists(), "Nonexistent path with multiple segments should not exist");
    }
}
