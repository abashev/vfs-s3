package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class S3PathTest {

    private final S3FileSystem fs = new S3FileSystem(new S3FileSystemProvider(), "test-bucket", new FakeS3Client());
    private final S3FileSystem otherFs =
            new S3FileSystem(new S3FileSystemProvider(), "other-bucket", new FakeS3Client());

    private S3Path path(String p) {
        return (S3Path) fs.getPath(p);
    }

    @Test
    void multiPartConstructorJoinsSegmentsWithSlashes() {
        assertEquals("/a/b/c", fs.getPath("/a", "b", "c").toString());
        assertEquals("/a/b", fs.getPath("/a/", "b").toString());
        assertEquals("/a/b", fs.getPath("/a", "", "b").toString());
    }

    @Test
    void absoluteAndRelativePaths() {
        assertTrue(path("/a/b").isAbsolute());
        assertFalse(path("a/b").isAbsolute());
    }

    @Test
    void rootOfAbsolutePathIsSlashAndOfRelativeIsNull() {
        assertEquals("/", path("/a/b").getRoot().toString());
        assertNull(path("a/b").getRoot());
    }

    @Test
    void fileNameIsTheLastSegment() {
        assertEquals("b.txt", path("/a/b.txt").getFileName().toString());
        assertNull(path("/").getFileName(), "The root has no file name");
        var relative = path("plain");
        assertSame(relative, relative.getFileName(), "A single relative segment is its own file name");
    }

    @Test
    void parentDropsTheLastSegment() {
        assertEquals("/a", path("/a/b").getParent().toString());
        assertNull(path("/a").getParent(), "A top-level entry has no parent below the root");
        assertNull(path("plain").getParent());
    }

    @Test
    void nameCountAndIndexedAccess() {
        var p = path("/a/b/c");
        assertEquals(3, p.getNameCount());
        assertEquals(0, path("/").getNameCount());
        assertEquals("a", p.getName(0).toString());
        assertEquals("c", p.getName(2).toString());
    }

    @Test
    void subpathJoinsTheSelectedRange() {
        assertEquals("b/c", path("/a/b/c/d").subpath(1, 3).toString());
    }

    @Test
    void iteratorWalksTheSegments() {
        var names = new ArrayList<String>();
        for (Path name : path("/a/b/c")) {
            names.add(name.toString());
        }
        assertEquals(List.of("a", "b", "c"), names);
    }

    @Test
    void startsWithAndEndsWithCompareTextually() {
        assertTrue(path("/a/b/c").startsWith(path("/a/b")));
        assertFalse(path("/a/b/c").startsWith(path("/b")));
        assertTrue(path("/a/b/c").endsWith(path("b/c")));
        assertFalse(path("/a/b/c").endsWith(path("a")));
    }

    @Test
    void normalizeReturnsSelf() {
        var p = path("/a/b");
        assertSame(p, p.normalize());
    }

    @Test
    void resolveAppendsRelativeAndKeepsAbsoluteOther() {
        assertEquals("/a/b/c", path("/a/b").resolve(path("c")).toString());
        assertEquals("/a/c", path("/a/").resolve(path("c")).toString(), "No double slash after a trailing slash");
        assertEquals("/x", path("/a/b").resolve(path("/x")).toString());
    }

    @Test
    void relativizeStripsThePrefix() {
        assertEquals("b/c", path("/a").relativize(path("/a/b/c")).toString());
        assertEquals("", path("/a").relativize(path("/a")).toString());
    }

    @Test
    void relativizeRejectsNonDescendantsAndForeignPaths() {
        assertThrows(IllegalArgumentException.class, () -> path("/a").relativize(path("/b/c")));
        assertThrows(IllegalArgumentException.class, () -> path("/a").relativize(Path.of("/a/b")));
        assertThrows(IllegalArgumentException.class, () -> path("/a").relativize(otherFs.getPath("/a/b")));
    }

    @Test
    void toUriIncludesTheBucket() {
        assertEquals(URI.create("s3://test-bucket/a/b.txt"), path("/a/b.txt").toUri());
        assertEquals(URI.create("s3://test-bucket/rel"), path("rel").toUri(), "Relative paths gain a leading slash");
    }

    @Test
    void toAbsolutePathPrefixesRelativePaths() {
        assertEquals("/rel", path("rel").toAbsolutePath().toString());
        var absolute = path("/abs");
        assertSame(absolute, absolute.toAbsolutePath());
        assertEquals("/rel", path("rel").toRealPath().toString());
    }

    @Test
    void toFileAndWatchRegistrationAreUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> path("/a").toFile());
        assertThrows(UnsupportedOperationException.class, () -> path("/a").register(null, null));
    }

    @Test
    void compareToOrdersByPathText() {
        assertTrue(path("/a").compareTo(path("/b")) < 0);
        assertEquals(0, path("/a").compareTo(path("/a")));
    }

    @Test
    void equalityRequiresSameFileSystemAndPath() {
        assertEquals(path("/a"), path("/a"));
        assertEquals(path("/a").hashCode(), path("/a").hashCode());
        assertNotEquals(path("/a"), path("/b"));
        assertNotEquals(path("/a"), otherFs.getPath("/a"));
        assertNotEquals(path("/a"), "/a");
    }
}
