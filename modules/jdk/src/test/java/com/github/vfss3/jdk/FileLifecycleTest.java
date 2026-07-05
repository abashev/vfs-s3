package com.github.vfss3.jdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Comparator.reverseOrder;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Suite A: File Lifecycle tests for the JDK NIO.2 mock backend.
 *
 * <p>Tests use the standard {@link Files} API via the S3 {@link FileSystem}.
 * The mock backend stores all data in a temporary directory.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FileLifecycleTest {

    private static final String BUCKET = "test-bucket";
    private static final String CONTENT = "Hello, S3!";

    private FileSystem fs;

    @BeforeEach
    void setUp() throws IOException {
        fs = FileSystems.newFileSystem(URI.create("s3://" + BUCKET), Map.of());
    }

    @AfterEach
    void tearDown() throws IOException {
        // Delete /file-lifecycle/ recursively before closing the filesystem
        var lifecycleDir = fs.getPath("/file-lifecycle");
        if (Files.exists(lifecycleDir)) {
            try (var walk = Files.walk(lifecycleDir)) {
                walk.sorted(reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
            }
        }
        fs.close();
    }

    /** Step 1: Create a file and assert it exists. Content is readable back. */
    @Test
    @Order(1)
    @DisplayName("Step 1: create file and verify content is readable back")
    void step1_createFileAndVerifyExists() throws IOException {
        var path = fs.getPath("/file-lifecycle/test-file");

        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertTrue(Files.exists(path), "File should exist after write");
        assertArrayEquals(CONTENT.getBytes(UTF_8), Files.readAllBytes(path));
    }

    /** Step 1b: Verify getFileName() returns the last segment. */
    @Test
    @Order(2)
    @DisplayName("Step 1b: getFileName() returns the last path segment")
    void step1b_getFilenameReturnsLastSegment() throws IOException {
        var path = fs.getPath("/file-lifecycle/file.txt");
        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertNotNull(path.getFileName());
        assertTrue(path.getFileName().toString().equals("file.txt"), "getFileName() should return 'file.txt'");
    }

    /** Step 2: Create a file with spaces in its name and assert it exists. */
    @Test
    @Order(3)
    @DisplayName("Step 2: create file with spaces in name")
    void step2_createFileWithSpacesInName() throws IOException {
        var path = fs.getPath("/file-lifecycle/name with space");

        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertTrue(Files.exists(path), "File with spaces in name should exist after write");
    }

    /**
     * Step 3: lastModifiedTime returns a positive timestamp.
     * setLastModifiedTime is not supported and throws an exception.
     */
    @Test
    @Order(4)
    @DisplayName("Step 3: lastModifiedTime is positive; setAttribute throws UnsupportedOperationException")
    void step3_lastModifiedTimeIsPositiveAndSetLastModifiedTimeThrows() throws IOException {
        var path = fs.getPath("/file-lifecycle/test-file");
        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        var lastModified = Files.getLastModifiedTime(path);
        assertTrue(lastModified.toMillis() > 0, "lastModifiedTime should be positive");

        // setLastModifiedTime delegates to setAttribute which throws UnsupportedOperationException
        assertThrows(
                UnsupportedOperationException.class,
                () -> Files.setAttribute(path, "basic:lastModifiedTime", lastModified));
    }

    /**
     * Step 4: Attempt createDirectory on an existing regular file path → FileAlreadyExistsException.
     */
    @Test
    @Order(5)
    @DisplayName("Step 4: createDirectory on existing file throws FileAlreadyExistsException")
    void step4_createDirectoryOnExistingFileThrows() throws IOException {
        var path = fs.getPath("/file-lifecycle/test-file");
        Files.write(path, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertThrows(FileAlreadyExistsException.class, () -> Files.createDirectory(path));
    }

    /**
     * Step 5: Move file, verify old path gone and new path exists.
     * Move back. Try self-move → throws error.
     */
    @Test
    @Order(6)
    @DisplayName("Step 5: move file succeeds; self-move throws FileSystemException")
    void step5_moveFileAndSelfMoveThrows() throws IOException {
        var original = fs.getPath("/file-lifecycle/test-file");
        var renamed = fs.getPath("/file-lifecycle/renamed");

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

    /**
     * Step 6: Non-existent path → does not exist.
     * Existing file path → exists and is a regular file.
     */
    @Test
    @Order(7)
    @DisplayName("Step 6: file type checks — exists() and isRegularFile()")
    void step6_fileTypeChecks() throws IOException {
        var existing = fs.getPath("/file-lifecycle/test-file");
        var nonexistent = fs.getPath("/file-lifecycle/nonexistent");

        Files.write(existing, CONTENT.getBytes(UTF_8), CREATE, WRITE);

        assertFalse(Files.exists(nonexistent), "Non-existent path should not exist");
        assertTrue(Files.exists(existing), "Written file should exist");
        assertTrue(Files.isRegularFile(existing), "Written path should be a regular file");
    }

    /** Step 7: Deeply nested non-existent path → does not exist. No exception thrown. */
    @Test
    @Order(8)
    @DisplayName("Step 7: deeply nested non-existent path returns false without exception")
    void step7_deeplyNestedNonExistentPathDoesNotExist() {
        var path = fs.getPath("/file-lifecycle/does/not/exist");
        assertFalse(Files.exists(path), "Deeply nested non-existent path should return false");
    }

    /** Verify reading a non-existent file throws NoSuchFileException. */
    @Test
    @Order(9)
    @DisplayName("Read non-existent file throws NoSuchFileException")
    void readNonExistentFileThrows() {
        var path = fs.getPath("/ghost.txt");
        assertThrows(NoSuchFileException.class, () -> Files.readAllBytes(path));
    }
}
