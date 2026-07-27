package com.github.vfss3.spring.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.spring.S3ResourcePatternResolver;
import com.github.vfss3.spring.SpringIntegrationContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite C: Upload &amp; Download through the Spring Resource API — see
 * {@code docs/test-cases/c-upload-download.md}. Uses {@link WritableResource#getOutputStream()}
 * and {@link org.springframework.core.io.Resource#getInputStream()} against whatever real
 * S3-compatible backend the enclosing {@code @Suite} wired up via
 * {@link SpringIntegrationContext}.
 */
final class UploadDownloadTest {

    private static final String PREFIX = "upload/";

    private S3ResourcePatternResolver loader;
    private byte[] payload;

    @BeforeEach
    void setUp() {
        loader = SpringIntegrationContext.loader();
        payload = new byte[64 * 1024];
        new Random(42).nextBytes(payload);
    }

    @AfterEach
    void tearDown() {
        SpringIntegrationContext.deletePrefix(PREFIX);
    }

    private WritableResource resource(String key) {
        return (WritableResource) loader.getResource(SpringIntegrationContext.location(PREFIX + key));
    }

    /** Step 1: upload a payload to a key. */
    @DisplayName("Step 1: upload a binary payload")
    @Test
    void step1_upload() throws IOException {
        var dest = resource("backup.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        assertTrue(dest.exists(), "Uploaded resource should exist");
        assertEquals(payload.length, dest.contentLength());
    }

    /** Step 2: overwrite the same key. */
    @DisplayName("Step 2: overwrite an existing key")
    @Test
    void step2_overwrite() throws IOException {
        var dest = resource("overwrite.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(new byte[] {1, 2, 3});
        }
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        assertEquals(payload.length, dest.contentLength(), "Size should reflect the second write");
    }

    /** Step 3: round-trip the bytes through output / input streams. */
    @DisplayName("Step 3: output/input stream round-trip")
    @Test
    void step3_outputStreamRoundTrip() throws IOException {
        var dest = resource("output.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        try (InputStream in = dest.getInputStream()) {
            assertArrayEquals(payload, in.readAllBytes(), "Read-back content should match");
        }
    }

    /** Step 4: a deep key writes without any intermediate "folders" — S3 keys are flat. */
    @DisplayName("Step 4: nested upload needs no intermediate folders")
    @Test
    void step4_nestedUpload() throws IOException {
        var dest = resource("deep/sub1/sub2/backup.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        assertTrue(dest.exists(), "Nested resource should exist");
        assertEquals(payload.length, dest.contentLength());
    }

    /** Step 5: download the uploaded resource into a byte array via the input stream. */
    @DisplayName("Step 5: download via the input stream")
    @Test
    void step5_download() throws IOException {
        var dest = resource("download.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write("payload-content".getBytes(UTF_8));
        }

        byte[] downloaded;
        try (InputStream in = dest.getInputStream()) {
            downloaded = in.readAllBytes();
        }

        assertEquals(dest.contentLength(), downloaded.length, "Downloaded size should match");
        assertEquals("payload-content", new String(downloaded, UTF_8));
    }
}
