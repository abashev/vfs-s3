package com.github.vfss3.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.core.io.WritableResource;

/**
 * Suite A: File Lifecycle tests for the Spring Resource mock backend.
 *
 * <p>Tests use the Spring {@link org.springframework.core.io.Resource} API via
 * {@link S3ResourceLoader}. The mock backend stores all data in a temporary directory.
 *
 * <p>Note: Spring Resource API does not have native move/rename — those Suite A steps
 * are skipped. Focus is on read/write/exists/metadata operations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
final class FileLifecycleTest {

    private static final String BUCKET = "test-bucket";
    private static final String CONTENT = "Hello, Spring S3!";

    private S3ResourceLoader loader;

    @BeforeEach
    void setUp() {
        loader = new S3ResourceLoader();
    }

    @AfterEach
    void tearDown() throws IOException {
        loader.close();
    }

    /** Step 1: Write file via WritableResource.getOutputStream(). Verify exists and content. */
    @DisplayName("Step 1: write file and verify exists and content")
    @Order(1)
    @Test
    void step1_writeFileAndVerifyExistsAndContent() throws IOException {
        var resource = (WritableResource) loader.getResource("s3://" + BUCKET + "/file-lifecycle/file.txt");

        try (var out = resource.getOutputStream()) {
            out.write(CONTENT.getBytes(UTF_8));
        }

        assertTrue(resource.exists(), "Resource should exist after write");
        try (InputStream in = resource.getInputStream()) {
            var bytes = in.readAllBytes();
            assertEquals(CONTENT, new String(bytes, UTF_8), "Content should match what was written");
        }
    }

    /** Step 2: Check getFilename() returns the last segment of the key. */
    @DisplayName("Step 2: getFilename() returns the last segment of the key")
    @Order(2)
    @Test
    void step2_getFilenameReturnsLastSegment() throws IOException {
        var resource = (WritableResource) loader.getResource("s3://" + BUCKET + "/file-lifecycle/file.txt");

        try (var out = resource.getOutputStream()) {
            out.write(CONTENT.getBytes(UTF_8));
        }

        assertEquals("file.txt", resource.getFilename(), "getFilename() should return 'file.txt'");
    }

    /** Step 2b: Create a file with spaces in its name. */
    @DisplayName("Step 2b: create file with spaces in its name")
    @Order(3)
    @Test
    void step2b_createFileWithSpacesInName() throws IOException {
        var resource = (WritableResource) loader.getResource("s3://" + BUCKET + "/file-lifecycle/name with spaces.txt");

        try (var out = resource.getOutputStream()) {
            out.write(CONTENT.getBytes(UTF_8));
        }

        assertTrue(resource.exists(), "Resource with spaces in name should exist after write");
        assertEquals("name with spaces.txt", resource.getFilename());
    }

    /** Step 4: lastModified() returns a valid (positive) timestamp. */
    @DisplayName("Step 4: lastModified() returns a positive timestamp")
    @Order(4)
    @Test
    void step4_lastModifiedReturnsPositiveTimestamp() throws IOException {
        var resource = (WritableResource) loader.getResource("s3://" + BUCKET + "/file-lifecycle/file.txt");

        try (var out = resource.getOutputStream()) {
            out.write(CONTENT.getBytes(UTF_8));
        }

        assertTrue(resource.lastModified() > 0, "lastModified() should return a positive timestamp");
    }

    /** Step 5: contentLength() matches the number of bytes written. */
    @DisplayName("Step 5: contentLength() matches the number of bytes written")
    @Order(5)
    @Test
    void step5_contentLengthMatchesWrittenBytes() throws IOException {
        var resource = (WritableResource) loader.getResource("s3://" + BUCKET + "/file-lifecycle/file.txt");
        var contentBytes = CONTENT.getBytes(UTF_8);

        try (var out = resource.getOutputStream()) {
            out.write(contentBytes);
        }

        assertEquals(contentBytes.length, resource.contentLength(), "contentLength() should match bytes written");
    }

    /** Step 6: Non-existent resource — exists() == false, getInputStream() throws IOException. */
    @DisplayName("Step 6: non-existent resource returns exists()=false and getInputStream() throws")
    @Order(6)
    @Test
    void step6_nonExistentResourceExistsFalseAndGetInputStreamThrows() {
        var resource = loader.getResource("s3://" + BUCKET + "/file-lifecycle/nonexistent.txt");

        assertFalse(resource.exists(), "Non-existent resource should return false for exists()");
        assertThrows(
                IOException.class,
                resource::getInputStream,
                "getInputStream() on non-existent resource should throw IOException");
    }

    /** isWritable() returns true for mock-backed resources. */
    @DisplayName("isWritable() returns true for mock-backed S3Resource")
    @Order(7)
    @Test
    void isWritableReturnsTrueForMockResource() {
        var resource = (WritableResource) loader.getResource("s3://" + BUCKET + "/file-lifecycle/file.txt");
        assertTrue(resource.isWritable(), "isWritable() should return true for mock-backed S3Resource");
    }
}
