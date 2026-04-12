package com.github.vfss3.commonsvfs;

import com.github.vfss3.commonsvfs.support.BaseIntegrationTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.IMAGINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Suite A: File Lifecycle.
 *
 * Tests VFS2 API operations on files: creation, modification, type checking, moving/renaming.
 * Uses the S3FileProvider with a LocalStack S3 backend.
 *
 * @author <A href="mailto:claude at anthropic dot com">Claude (vfs-s3 bot)</A>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileLifecycleIT extends BaseIntegrationTest {
    private static final String PREFIX = "/file-lifecycle/";

    @AfterAll
    void tearDown() throws FileSystemException {
        // Delete the entire prefix directory recursively
        FileObject prefixDir = root.resolveFile(PREFIX);
        if (prefixDir.exists()) {
            prefixDir.deleteAll();
        }
    }

    /**
     * Test 1: Create file at /file-lifecycle/test-file
     * Assert: file exists
     */
    @Test
    void testCreateFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertTrue(file.exists(), "File should exist after creation");
    }

    /**
     * Test 2: Create file with space in name at /file-lifecycle/name with space
     * Assert: file exists
     */
    @Test
    void testCreateFileWithSpace() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "name with space");
        file.createFile();

        assertTrue(file.exists(), "File with space in name should exist after creation");
    }

    /**
     * Test 3: Check last modified time
     * Assert: lastModifiedTime > 0
     * Try: setLastModifiedTime(111)
     * Assert: throws FileSystemException
     */
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

    /**
     * Test 4: Try to create folder at path of existing file
     * Assert: throws FileSystemException
     */
    @Test
    void testCreateFolderOnExistingFile() throws FileSystemException {
        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertThrows(
                FileSystemException.class,
                () -> file.createFolder(),
                "Creating folder on existing file should throw FileSystemException");
    }

    /**
     * Test 5: Move/rename file
     * Assert: /renamed exists, /test-file does not exist
     * Move back: /renamed → /test-file
     * Assert: /test-file exists, /renamed does not exist
     * Try: move /test-file to itself
     * Assert: throws error
     */
    @Test
    void testMoveAndRename() throws FileSystemException {
        FileObject sourceFile = root.resolveFile(PREFIX + "test-file");
        sourceFile.createFile();

        FileObject targetFile = root.resolveFile(PREFIX + "renamed");

        // Move to renamed
        sourceFile.moveTo(targetFile);

        assertTrue(targetFile.exists(), "Renamed file should exist");
        assertFalse(sourceFile.exists(), "Original file should not exist after move");

        // Move back
        targetFile.moveTo(sourceFile);

        assertTrue(sourceFile.exists(), "Original file should exist after move back");
        assertFalse(targetFile.exists(), "Renamed file should not exist after move back");

        // Try to move to itself
        assertThrows(
                FileSystemException.class,
                () -> sourceFile.moveTo(sourceFile),
                "Moving file to itself should throw FileSystemException");
    }

    /**
     * Test 6: Get file type
     * Assert: nonexistent path → type is IMAGINARY
     * Assert: existing file → type is FILE
     */
    @Test
    void testGetType() throws FileSystemException {
        FileObject nonexistent = root.resolveFile(PREFIX + "nonexistent");
        assertEquals(IMAGINARY, nonexistent.getType(), "Nonexistent file should have IMAGINARY type");

        FileObject file = root.resolveFile(PREFIX + "test-file");
        file.createFile();

        assertEquals(FILE, file.getType(), "Existing file should have FILE type");
    }

    /**
     * Test 7: Check exists() for nonexistent path with multiple segments
     * Assert: /file-lifecycle/does/not/exist does not exist
     */
    @Test
    void testNonexistentPathDoesNotExist() throws FileSystemException {
        FileObject nonexistent = root.resolveFile(PREFIX + "does/not/exist");

        assertFalse(nonexistent.exists(), "Nonexistent path with multiple segments should not exist");
    }
}
