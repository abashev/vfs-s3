package com.github.vfss3.jdk;

import static java.util.Comparator.reverseOrder;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Suite C: Upload &amp; Download for the JDK NIO.2 mock backend — see
 * {@code docs/test-cases/c-upload-download.md}. A locally-generated binary payload stands in
 * for {@code backup.zip}; streaming goes through {@link Files} input/output streams.
 */
class UploadDownloadTest {

    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "/upload/";

    private FileSystem fs;
    private byte[] payload;

    @BeforeEach
    void setUp() throws IOException {
        fs = FileSystems.newFileSystem(URI.create("s3://" + BUCKET), Map.of());
        payload = new byte[64 * 1024];
        new Random(42).nextBytes(payload);
    }

    @AfterEach
    void tearDown() throws IOException {
        var prefix = fs.getPath(PREFIX);
        if (Files.exists(prefix)) {
            try (Stream<java.nio.file.Path> walk = Files.walk(prefix)) {
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

    /** Step 1: upload the payload to a key. */
    @Test
    @DisplayName("Step 1: upload a binary payload")
    void step1_upload() throws IOException {
        var dest = fs.getPath(PREFIX + "backup.bin");
        Files.write(dest, payload);

        assertTrue(Files.exists(dest), "Uploaded file should exist");
        assertTrue(Files.isRegularFile(dest), "Uploaded path should be a regular file");
        assertEquals(payload.length, Files.size(dest));
    }

    /** Step 2: overwrite the same key. */
    @Test
    @DisplayName("Step 2: overwrite an existing key")
    void step2_overwrite() throws IOException {
        var dest = fs.getPath(PREFIX + "overwrite.bin");
        Files.write(dest, new byte[] {1, 2, 3});
        Files.write(dest, payload);

        assertTrue(Files.exists(dest));
        assertEquals(payload.length, Files.size(dest), "Size should reflect the second write");
    }

    /** Step 3: round-trip bytes through output/input streams. */
    @Test
    @DisplayName("Step 3: output/input stream round-trip")
    void step3_outputStreamRoundTrip() throws IOException {
        var dest = fs.getPath(PREFIX + "output.bin");

        try (OutputStream os = Files.newOutputStream(dest)) {
            os.write(payload);
        }

        assertTrue(Files.exists(dest));
        assertEquals(payload.length, Files.size(dest));

        try (InputStream in = Files.newInputStream(dest)) {
            assertArrayEquals(payload, in.readAllBytes(), "Read-back content should match");
        }

        Files.delete(dest);
        assertTrue(!Files.exists(dest), "File should be gone after delete");
    }

    /** Step 4: writing to a deep key auto-creates the intermediate folders. */
    @Test
    @DisplayName("Step 4: nested upload creates intermediate folders")
    void step4_nestedUploadCreatesFolders() throws IOException {
        var dest = fs.getPath(PREFIX + "deep/sub1/sub2/backup.bin");
        Files.write(dest, payload);

        assertTrue(Files.isRegularFile(dest), "Nested file should exist");
        assertTrue(Files.isDirectory(fs.getPath(PREFIX + "deep/sub1")), "sub1 should be a directory");
        assertTrue(Files.isDirectory(fs.getPath(PREFIX + "deep/sub1/sub2")), "sub2 should be a directory");
    }

    /** Step 5: download the uploaded file back into a local temp file. */
    @Test
    @DisplayName("Step 5: download to a local temp file")
    void step5_download() throws IOException {
        var dest = fs.getPath(PREFIX + "download.bin");
        Files.write(dest, payload);

        var temp = Files.createTempFile("vfs-jdk.", ".s3-test");
        try {
            try (InputStream in = Files.newInputStream(dest)) {
                Files.copy(in, temp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            assertEquals(Files.size(dest), Files.size(temp), "Downloaded size should match");
        } finally {
            Files.deleteIfExists(temp);
        }
    }
}
