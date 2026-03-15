package com.github.vfss3.jdk;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3FileSystemProviderTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Test
    void getSchemeReturnsS3() {
        assertEquals("s3", provider.getScheme());
    }

    @Test
    void newFileSystemCreatesOpenFileSystem() throws Exception {
        var uri = URI.create("s3://test-bucket");
        try (var fs = provider.newFileSystem(uri, Map.of())) {
            assertNotNull(fs);
            assertTrue(fs.isOpen());
        }
    }

    @Test
    void newFileSystemRejectsBadScheme() {
        var uri = URI.create("file:///tmp");
        assertThrows(IllegalArgumentException.class, () -> provider.newFileSystem(uri, Map.of()));
    }

    @Test
    void fileSystemBucketIsExtractedFromUri() throws Exception {
        var uri = URI.create("s3://my-test-bucket");
        try (var fs = (S3FileSystem) provider.newFileSystem(uri, Map.of())) {
            assertEquals("my-test-bucket", fs.getBucket());
        }
    }

    @Test
    void pathCreationRoundtrips() throws Exception {
        var uri = URI.create("s3://my-test-bucket");
        try (var fs = (S3FileSystem) provider.newFileSystem(uri, Map.of())) {
            var path = (S3Path) fs.getPath("/prefix/key.txt");
            assertEquals("/prefix/key.txt", path.toString());
            assertTrue(path.isAbsolute());
        }
    }
}
