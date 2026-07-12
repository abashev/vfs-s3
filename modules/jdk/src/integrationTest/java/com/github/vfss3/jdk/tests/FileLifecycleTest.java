package com.github.vfss3.jdk.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Suite A: File Lifecycle — see {@code docs/test-cases/a-file-lifecycle.md}. Runs against
 * whatever real S3-compatible backend the enclosing {@code @Suite} wired up via
 * {@link JdkIntegrationContext}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
final class FileLifecycleTest {

    private static final String PREFIX = "/file-lifecycle/";
    private static final String CONTENT = "Hello, S3!";

    private FileSystem fs;

    @BeforeEach
    void setUp() {
        fs = JdkIntegrationContext.fileSystem();
    }

    @AfterEach
    void tearDown() throws IOException {
        for (var name : List.of("test-file", "file.txt", "name with space", "renamed", "existing-destination")) {
            Files.deleteIfExists(fs.getPath(PREFIX + name));
        }
    }

    /** Step 1: Create a file and assert it exists. Content is readable back. */
    @DisplayName("Step 1: create file and verify content is readable back")
    @Order(1)
    @Test
    void step1_createFileAndVerifyExists() throws IOException {
        var path = fs.getPath(PREFIX + "test-file");

        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertTrue(Files.exists(path), "File should exist after write");
        assertArrayEquals(CONTENT.getBytes(UTF_8), Files.readAllBytes(path));
    }

    /** Step 1b: Verify getFileName() returns the last segment. */
    @DisplayName("Step 1b: getFileName() returns the last path segment")
    @Order(2)
    @Test
    void step1b_getFilenameReturnsLastSegment() throws IOException {
        var path = fs.getPath(PREFIX + "file.txt");
        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertNotNull(path.getFileName());
        assertTrue(path.getFileName().toString().equals("file.txt"), "getFileName() should return 'file.txt'");
    }

    /** Step 2: Create a file with spaces in its name and assert it exists. */
    @DisplayName("Step 2: create file with spaces in name")
    @Order(3)
    @Test
    void step2_createFileWithSpacesInName() throws IOException {
        var path = fs.getPath(PREFIX + "name with space");

        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertTrue(Files.exists(path), "File with spaces in name should exist after write");
    }

    /**
     * Step 3: lastModifiedTime returns a positive timestamp. setLastModifiedTime is not
     * supported and throws an exception.
     */
    @DisplayName("Step 3: lastModifiedTime is positive; setAttribute throws UnsupportedOperationException")
    @Order(4)
    @Test
    void step3_lastModifiedTimeIsPositiveAndSetLastModifiedTimeThrows() throws IOException {
        var path = fs.getPath(PREFIX + "test-file");
        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        var lastModified = Files.getLastModifiedTime(path);
        assertTrue(lastModified.toMillis() > 0, "lastModifiedTime should be positive");

        // setLastModifiedTime delegates to setAttribute which throws UnsupportedOperationException
        assertThrows(
                UnsupportedOperationException.class,
                () -> Files.setAttribute(path, "basic:lastModifiedTime", lastModified));
    }

    /** Step 4: Attempt createDirectory on an existing regular file path → FileAlreadyExistsException. */
    @DisplayName("Step 4: createDirectory on existing file throws FileAlreadyExistsException")
    @Order(5)
    @Test
    void step4_createDirectoryOnExistingFileThrows() throws IOException {
        var path = fs.getPath(PREFIX + "test-file");
        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertThrows(FileAlreadyExistsException.class, () -> Files.createDirectory(path));
    }

    /**
     * Step 5: Move file, verify old path gone and new path exists. Move back. Try self-move →
     * throws error.
     */
    @DisplayName("Step 5: move file succeeds; self-move throws FileSystemException")
    @Order(6)
    @Test
    void step5_moveFileAndSelfMoveThrows() throws IOException {
        var original = fs.getPath(PREFIX + "test-file");
        var renamed = fs.getPath(PREFIX + "renamed");

        Files.write(original, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        // Move to renamed
        Files.move(original, renamed);
        assertFalse(Files.exists(original), "/test-file should not exist after move");
        assertTrue(Files.exists(renamed), "/renamed should exist after move");

        // Move back
        Files.move(renamed, original);
        assertTrue(Files.exists(original), "/test-file should exist after moving back");
        assertFalse(Files.exists(renamed), "/renamed should not exist after moving back");

        // Self-move → error
        assertThrows(FileSystemException.class, () -> Files.move(original, original));
    }

    /** Step 5b: move onto an existing destination without REPLACE_EXISTING throws. */
    @DisplayName("Step 5b: move onto existing destination throws without REPLACE_EXISTING")
    @Order(7)
    @Test
    void step5b_moveOntoExistingDestinationThrows() throws IOException {
        var source = fs.getPath(PREFIX + "test-file");
        var destination = fs.getPath(PREFIX + "existing-destination");

        Files.write(source, CONTENT.getBytes(UTF_8), CREATE, WRITE);
        Files.write(destination, "already here".getBytes(UTF_8), CREATE, WRITE);

        assertThrows(FileAlreadyExistsException.class, () -> Files.move(source, destination));
        assertTrue(Files.exists(source), "Source should remain untouched after a rejected move");
        assertArrayEquals(
                "already here".getBytes(UTF_8),
                Files.readAllBytes(destination),
                "Destination content should be untouched after a rejected move");
    }

    /**
     * Step 6: Non-existent path → does not exist. Existing file path → exists and is a regular
     * file.
     */
    @DisplayName("Step 6: file type checks — exists() and isRegularFile()")
    @Order(8)
    @Test
    void step6_fileTypeChecks() throws IOException {
        var existing = fs.getPath(PREFIX + "test-file");
        var nonexistent = fs.getPath(PREFIX + "nonexistent");

        Files.write(existing, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertFalse(Files.exists(nonexistent), "Non-existent path should not exist");
        assertTrue(Files.exists(existing), "Written file should exist");
        assertTrue(Files.isRegularFile(existing), "Written path should be a regular file");
    }

    /** Step 7: Deeply nested non-existent path → does not exist. No exception thrown. */
    @DisplayName("Step 7: deeply nested non-existent path returns false without exception")
    @Order(9)
    @Test
    void step7_deeplyNestedNonExistentPathDoesNotExist() {
        var path = fs.getPath(PREFIX + "does/not/exist");
        assertFalse(Files.exists(path), "Deeply nested non-existent path should return false");
    }

    /** Verify reading a non-existent file throws NoSuchFileException. */
    @DisplayName("Read non-existent file throws NoSuchFileException")
    @Order(10)
    @Test
    void readNonExistentFileThrows() {
        var path = fs.getPath("/ghost.txt");
        assertThrows(NoSuchFileException.class, () -> Files.readAllBytes(path));
    }
}
