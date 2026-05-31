package com.github.vfss3.spring6;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Suite C: Upload &amp; Download for the Spring Resource mock backend — see
 * {@code docs/test-cases/c-upload-download.md}. Uses {@link WritableResource#getOutputStream()}
 * and {@link org.springframework.core.io.Resource#getInputStream()}.
 */
class UploadDownloadTest {

    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "s3://" + BUCKET + "/upload/";

    private S3ResourceLoader loader;
    private byte[] payload;

    @BeforeEach
    void setUp() {
        loader = new S3ResourceLoader();
        payload = new byte[64 * 1024];
        new Random(42).nextBytes(payload);
    }

    @AfterEach
    void tearDown() throws IOException {
        loader.close();
    }

    private WritableResource resource(String key) {
        return (WritableResource) loader.getResource(PREFIX + key);
    }

    /** Step 1: upload a payload to a key. */
    @Test
    @DisplayName("Step 1: upload a binary payload")
    void step1_upload() throws IOException {
        var dest = resource("backup.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        assertTrue(dest.exists(), "Uploaded resource should exist");
        assertEquals(payload.length, dest.contentLength());
    }

    /** Step 2: overwrite the same key. */
    @Test
    @DisplayName("Step 2: overwrite an existing key")
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
    @Test
    @DisplayName("Step 3: output/input stream round-trip")
    void step3_outputStreamRoundTrip() throws IOException {
        var dest = resource("output.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        try (InputStream in = dest.getInputStream()) {
            assertArrayEquals(payload, in.readAllBytes(), "Read-back content should match");
        }
    }

    /** Step 4: writing to a deep key auto-creates the intermediate folders. */
    @Test
    @DisplayName("Step 4: nested upload writes through intermediate folders")
    void step4_nestedUpload() throws IOException {
        var dest = resource("deep/sub1/sub2/backup.bin");
        try (OutputStream out = dest.getOutputStream()) {
            out.write(payload);
        }

        assertTrue(dest.exists(), "Nested resource should exist");
        assertEquals(payload.length, dest.contentLength());
    }

    /** Step 5: download the uploaded resource into a byte array via the input stream. */
    @Test
    @DisplayName("Step 5: download via the input stream")
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
