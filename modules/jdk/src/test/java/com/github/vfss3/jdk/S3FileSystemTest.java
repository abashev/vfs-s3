package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

final class S3FileSystemTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();
    private final S3FileSystem fs = new S3FileSystem(provider, "test-bucket", new FakeS3Client());

    private S3Path path(String p) {
        return (S3Path) fs.getPath(p);
    }

    @Test
    void toKeyStripsTheLeadingSlash() {
        assertEquals("a/b.txt", fs.toKey(path("/a/b.txt")));
        assertEquals("a/b.txt", fs.toKey(path("a/b.txt")));
    }

    @Test
    void toKeyStripsAllTrailingSlashes() {
        // Without this, "/copy/" would produce the same key for the plain-file probe and the
        // folder-marker probe.
        assertEquals("copy", fs.toKey(path("/copy/")));
        assertEquals("copy", fs.toKey(path("/copy//")));
        assertEquals("", fs.toKey(path("/")));
    }

    @Test
    void staticProperties() {
        assertEquals("/", fs.getSeparator());
        assertFalse(fs.isReadOnly());
        assertEquals("s3", fs.provider().getScheme());
        assertEquals("S3FileSystem[s3://test-bucket]", fs.toString());
    }

    @Test
    void rootDirectoriesIsTheSingleRoot() {
        var roots = fs.getRootDirectories().iterator();
        assertEquals("/", roots.next().toString());
        assertFalse(roots.hasNext());
    }

    @Test
    void fileStoresIsTheSingleBucketStore() {
        var stores = fs.getFileStores().iterator();
        assertEquals("test-bucket", stores.next().name());
        assertFalse(stores.hasNext());
    }

    @Test
    void onlyBasicAttributeViewIsSupported() {
        assertEquals(Set.of("basic"), fs.supportedFileAttributeViews());
    }

    @Test
    void closeMarksTheFileSystemClosed() {
        assertTrue(fs.isOpen());
        fs.close();
        assertFalse(fs.isOpen());
    }

    @Test
    void unsupportedFeaturesThrow() {
        assertThrows(UnsupportedOperationException.class, () -> fs.getPathMatcher("glob:*"));
        assertThrows(UnsupportedOperationException.class, fs::getUserPrincipalLookupService);
        assertThrows(UnsupportedOperationException.class, fs::newWatchService);
    }

    @Test
    void equalityIsByBucket() {
        var sameBucket = new S3FileSystem(provider, "test-bucket", new FakeS3Client());
        var otherBucket = new S3FileSystem(provider, "other-bucket", new FakeS3Client());

        assertEquals(fs, sameBucket);
        assertEquals(fs.hashCode(), sameBucket.hashCode());
        assertNotEquals(fs, otherBucket);
        assertNotEquals(fs, "test-bucket");
    }
}
