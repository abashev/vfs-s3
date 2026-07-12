package com.github.vfss3.jdk.tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Suite C: Upload &amp; Download — see {@code docs/test-cases/c-upload-download.md}. A
 * locally-generated binary payload stands in for {@code backup.zip}; streaming goes through
 * {@link Files} input/output streams. Runs against whatever real S3-compatible backend the
 * enclosing {@code @Suite} wired up via {@link JdkIntegrationContext}.
 */
final class UploadDownloadTest {

    private static final String PREFIX = "/upload/";

    private FileSystem fs;
    private byte[] payload;

    @BeforeEach
    void setUp() {
        fs = JdkIntegrationContext.fileSystem();
        payload = new byte[64 * 1024];
        new Random(42).nextBytes(payload);
    }

    @AfterEach
    void tearDown() throws IOException {
        JdkIntegrationContext.deleteRecursively(fs.getPath(PREFIX));
    }

    /** Step 1: upload the payload to a key. */
    @DisplayName("Step 1: upload a binary payload")
    @Test
    void step1_upload() throws IOException {
        var dest = fs.getPath(PREFIX + "backup.bin");
        Files.write(dest, payload);

        assertTrue(Files.exists(dest), "Uploaded file should exist");
        assertTrue(Files.isRegularFile(dest), "Uploaded path should be a regular file");
        assertEquals(payload.length, Files.size(dest));
    }

    /** Step 2: overwrite the same key. */
    @DisplayName("Step 2: overwrite an existing key")
    @Test
    void step2_overwrite() throws IOException {
        var dest = fs.getPath(PREFIX + "overwrite.bin");
        Files.write(dest, new byte[] {1, 2, 3});
        Files.write(dest, payload);

        assertTrue(Files.exists(dest));
        assertEquals(payload.length, Files.size(dest), "Size should reflect the second write");
    }

    /** Step 3: round-trip bytes through output/input streams. */
    @DisplayName("Step 3: output/input stream round-trip")
    @Test
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
    @DisplayName("Step 4: nested upload creates intermediate folders")
    @Test
    void step4_nestedUploadCreatesFolders() throws IOException {
        var dest = fs.getPath(PREFIX + "deep/sub1/sub2/backup.bin");
        Files.write(dest, payload);

        assertTrue(Files.isRegularFile(dest), "Nested file should exist");
        assertTrue(Files.isDirectory(fs.getPath(PREFIX + "deep/sub1")), "sub1 should be a directory");
        assertTrue(Files.isDirectory(fs.getPath(PREFIX + "deep/sub1/sub2")), "sub2 should be a directory");
    }

    /** Step 5: download the uploaded file back into a local temp file. */
    @DisplayName("Step 5: download to a local temp file")
    @Test
    void step5_download() throws IOException {
        var dest = fs.getPath(PREFIX + "download.bin");
        Files.write(dest, payload);

        var temp = Files.createTempFile("vfs-jdk.", ".s3-test");
        try {
            try (InputStream in = Files.newInputStream(dest)) {
                Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
            }
            assertEquals(Files.size(dest), Files.size(temp), "Downloaded size should match");
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /** Step 6: cross-provider round-trip — {@code Files.copy(local, s3)} up, then {@code Files.copy(s3, local)} down. */
    @DisplayName("Step 6: local↔S3 round-trip via Files.copy(Path, Path)")
    @Test
    void step6_crossProviderCopyRoundTrip() throws IOException {
        var localSource = Files.createTempFile("vfs-jdk-src.", ".bin");
        var localTarget = Files.createTempFile("vfs-jdk-dst.", ".bin");
        var remote = fs.getPath(PREFIX + "roundtrip.bin");
        try {
            Files.write(localSource, payload);

            // Upload: local (default FS) path -> S3 path. Different providers, so NIO.2 streams the
            // bytes across via newInputStream/newOutputStream rather than S3FileSystemProvider.copy().
            Files.copy(localSource, remote);
            assertTrue(Files.exists(remote), "Uploaded S3 object should exist");
            assertEquals(payload.length, Files.size(remote));

            // Download: S3 path -> local (default FS) path. localTarget already exists (temp file).
            Files.copy(remote, localTarget, StandardCopyOption.REPLACE_EXISTING);
            assertArrayEquals(payload, Files.readAllBytes(localTarget), "Downloaded content should match the source");
        } finally {
            Files.deleteIfExists(localSource);
            Files.deleteIfExists(localTarget);
        }
    }
}
