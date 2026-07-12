package com.github.vfss3.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite D: File Metadata for the Spring Resource mock backend — see
 * {@code docs/test-cases/d-file-metadata.md}.
 *
 * <p>Only the metadata the Spring {@link org.springframework.core.io.Resource} API exposes is
 * checked: {@code contentLength()} and {@code lastModified()}. The S3-specific items (content
 * type, signed URL, MD5 hash) have no Resource analog and are intentionally omitted.
 */
class FileMetadataTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "s3://" + BUCKET + "/metadata/backup.bin";
    private static final byte[] CONTENT = "the-backup-payload".getBytes(UTF_8);

    private S3ResourceLoader loader;
    private WritableResource resource;

    @BeforeEach
    void setUp() throws IOException {
        loader = new S3ResourceLoader();
        resource = (WritableResource) loader.getResource(KEY);
        try (OutputStream out = resource.getOutputStream()) {
            out.write(CONTENT);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        loader.close();
    }

    /** Step 2: content size matches the written payload. */
    @Test
    @DisplayName("Step 2: contentLength matches the written payload")
    void step2_contentLength() throws IOException {
        assertEquals(CONTENT.length, resource.contentLength());
    }

    /** Step 3: last-modified time is a positive timestamp. */
    @Test
    @DisplayName("Step 3: lastModified is a positive timestamp")
    void step3_lastModified() throws IOException {
        assertTrue(resource.lastModified() > 0, "lastModified() should be positive");
    }

    /** getFilename returns the last segment of the key. */
    @Test
    @DisplayName("getFilename returns the last key segment")
    void getFilenameReturnsLastSegment() {
        assertEquals("backup.bin", resource.getFilename());
    }
}
