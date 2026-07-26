package com.github.vfss3.spring.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.spring.SpringIntegrationContext;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite D: File Metadata through the Spring Resource API — see
 * {@code docs/test-cases/d-file-metadata.md}.
 *
 * <p>Only the metadata the Spring {@link org.springframework.core.io.Resource} API exposes is
 * checked: {@code contentLength()} and {@code lastModified()}. The S3-specific items (content
 * type, signed URL, MD5 hash) have no Resource analog and are intentionally omitted.
 */
final class FileMetadataTest {

    private static final String PREFIX = "metadata/";
    private static final byte[] CONTENT = "the-backup-payload".getBytes(UTF_8);

    private WritableResource resource;

    @BeforeEach
    void setUp() throws IOException {
        resource = (WritableResource)
                SpringIntegrationContext.loader().getResource(SpringIntegrationContext.location(PREFIX + "backup.bin"));
        try (OutputStream out = resource.getOutputStream()) {
            out.write(CONTENT);
        }
    }

    @AfterEach
    void tearDown() {
        SpringIntegrationContext.deletePrefix(PREFIX);
    }

    /** Step 2: content size matches the written payload. */
    @DisplayName("Step 2: contentLength matches the written payload")
    @Test
    void step2_contentLength() throws IOException {
        assertEquals(CONTENT.length, resource.contentLength());
    }

    /** Step 3: last-modified time is a positive timestamp. */
    @DisplayName("Step 3: lastModified is a positive timestamp")
    @Test
    void step3_lastModified() throws IOException {
        assertTrue(resource.lastModified() > 0, "lastModified() should be positive");
    }

    /** getFilename returns the last segment of the key. */
    @DisplayName("getFilename returns the last key segment")
    @Test
    void getFilenameReturnsLastSegment() {
        assertEquals("backup.bin", resource.getFilename());
    }
}
