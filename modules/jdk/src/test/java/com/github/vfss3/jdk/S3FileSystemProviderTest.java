package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
    void newFileSystemThrowsWhenAlreadyOpenForBucket() throws Exception {
        var uri = URI.create("s3://test-bucket");
        try (var fs = provider.newFileSystem(uri, Map.of())) {
            assertThrows(FileSystemAlreadyExistsException.class, () -> provider.newFileSystem(uri, Map.of()));
        }
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

    @Test
    void getFileSystemReturnsTheRegisteredInstance() throws Exception {
        var uri = URI.create("s3://my-test-bucket");
        try (var fs = provider.newFileSystem(uri, Map.of())) {
            assertSame(fs, provider.getFileSystem(uri));
        }
    }

    @Test
    void getFileSystemThrowsWhenNotOpen() {
        var uri = URI.create("s3://never-opened-bucket");
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(uri));
    }

    @Test
    void getFileSystemThrowsAfterClose() throws Exception {
        var uri = URI.create("s3://my-test-bucket");
        var fs = provider.newFileSystem(uri, Map.of());
        fs.close();
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(uri));
    }

    @Test
    void getPathRoundtripsWithToUri() throws Exception {
        var uri = URI.create("s3://my-test-bucket");
        try (var fs = (S3FileSystem) provider.newFileSystem(uri, Map.of())) {
            var path = (S3Path) provider.getPath(URI.create("s3://my-test-bucket/prefix/key.txt"));
            assertEquals(fs.getPath("/prefix/key.txt"), path);
            assertEquals(URI.create("s3://my-test-bucket/prefix/key.txt"), path.toUri());
        }
    }

    @Test
    void getFileStoreReturnsTheBucketAsASingleStore() throws Exception {
        var uri = URI.create("s3://my-test-bucket");
        try (var fs = provider.newFileSystem(uri, Map.of())) {
            var path = fs.getPath("/some/file.txt");
            var store = provider.getFileStore(path);
            assertNotNull(store);
            assertEquals("my-test-bucket", store.name());
        }
    }
}
