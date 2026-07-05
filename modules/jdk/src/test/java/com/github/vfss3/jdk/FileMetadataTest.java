package com.github.vfss3.jdk;

import static java.time.ZoneOffset.UTC;
import static java.util.Comparator.reverseOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Suite D: File Metadata for the JDK NIO.2 mock backend — see
 * {@code docs/test-cases/d-file-metadata.md}.
 *
 * <p>Only the metadata the NIO.2 API exposes is checked: size and last-modified time. The
 * S3-specific items from the original suite (content type, signed URL, MD5 hash) have no NIO.2
 * analog and are intentionally omitted.
 */
class FileMetadataTest {

    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "/metadata/";

    private FileSystem fs;
    private byte[] payload;

    @BeforeEach
    void setUp() throws IOException {
        fs = FileSystems.newFileSystem(URI.create("s3://" + BUCKET), Map.of());
        payload = new byte[12_345];
        new Random(7).nextBytes(payload);
        Files.write(fs.getPath(PREFIX + "backup.bin"), payload);
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

    /** Step 2: content size matches the written payload. */
    @Test
    @DisplayName("Step 2: size matches the written payload")
    void step2_size() throws IOException {
        assertEquals(payload.length, Files.size(fs.getPath(PREFIX + "backup.bin")));
    }

    /** Step 3: last-modified time is a recent (post-2010) timestamp. */
    @Test
    @DisplayName("Step 3: last-modified time is recent")
    void step3_lastModified() throws IOException {
        var lastModified = Files.getLastModifiedTime(fs.getPath(PREFIX + "backup.bin"));
        assertTrue(lastModified.toMillis() > 0, "lastModifiedTime should be positive");

        int year = lastModified.toInstant().atZone(UTC).getYear();
        assertTrue(year > 2010, "Last modified year should be after 2010 but was " + year);
    }

    /** Bonus: BasicFileAttributes reports size and a regular-file flag consistently. */
    @Test
    @DisplayName("BasicFileAttributes are consistent")
    void readAttributesAreConsistent() throws IOException {
        var attrs = Files.readAttributes(fs.getPath(PREFIX + "backup.bin"), BasicFileAttributes.class);
        assertEquals(payload.length, attrs.size());
        assertTrue(attrs.isRegularFile(), "Payload should be a regular file");
    }
}
