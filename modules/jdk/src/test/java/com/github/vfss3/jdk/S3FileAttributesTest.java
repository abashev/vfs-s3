package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.attribute.FileTime;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class S3FileAttributesTest {

    @Test
    void fileAttributesMirrorTheHeadResponse() {
        var lastModified = Instant.parse("2024-06-15T12:30:00Z");
        var attrs = S3FileAttributes.file(1234, lastModified);

        assertTrue(attrs.isRegularFile());
        assertFalse(attrs.isDirectory());
        assertFalse(attrs.isSymbolicLink());
        assertFalse(attrs.isOther());
        assertEquals(1234, attrs.size());
        assertEquals(FileTime.from(lastModified), attrs.lastModifiedTime());
        // S3 tracks a single timestamp; access and creation times mirror it.
        assertEquals(attrs.lastModifiedTime(), attrs.lastAccessTime());
        assertEquals(attrs.lastModifiedTime(), attrs.creationTime());
        assertNull(attrs.fileKey());
    }

    @Test
    void directoryAttributesAreSyntheticAndEmpty() {
        var attrs = S3FileAttributes.directory();

        assertTrue(attrs.isDirectory());
        assertFalse(attrs.isRegularFile());
        assertEquals(0, attrs.size());
        assertEquals(FileTime.fromMillis(0), attrs.lastModifiedTime());
    }
}
