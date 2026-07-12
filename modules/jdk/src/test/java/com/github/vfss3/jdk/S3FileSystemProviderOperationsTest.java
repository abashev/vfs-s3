package com.github.vfss3.jdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Behavior of every S3-backed provider operation against an in-memory {@link FakeS3Client} —
 * the same code paths the integration suites exercise against real backends, minus the network.
 */
final class S3FileSystemProviderOperationsTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();
    private final FakeS3Client s3 = new FakeS3Client();
    private final S3FileSystem fs = new S3FileSystem(provider, "test-bucket", s3);

    private S3Path path(String p) {
        return (S3Path) fs.getPath(p);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    // ---- readAttributes / checkAccess ----

    @Test
    void readAttributesOfFileReportsSizeAndTimestamps() throws IOException {
        s3.putBytes("docs/a.txt", bytes("hello"));

        var attrs = provider.readAttributes(path("/docs/a.txt"), BasicFileAttributes.class);

        assertTrue(attrs.isRegularFile());
        assertFalse(attrs.isDirectory());
        assertFalse(attrs.isSymbolicLink());
        assertFalse(attrs.isOther());
        assertEquals(5, attrs.size());
        assertEquals(FileTime.from(FakeS3Client.LAST_MODIFIED), attrs.lastModifiedTime());
        assertEquals(attrs.lastModifiedTime(), attrs.lastAccessTime());
        assertEquals(attrs.lastModifiedTime(), attrs.creationTime());
        assertNull(attrs.fileKey());
    }

    @Test
    void readAttributesOfMarkerObjectReportsDirectory() throws IOException {
        s3.putBytes("docs/", new byte[0]);

        var attrs = provider.readAttributes(path("/docs"), BasicFileAttributes.class);

        assertTrue(attrs.isDirectory());
        assertEquals(0, attrs.size());
    }

    @Test
    void readAttributesOfVirtualFolderReportsDirectory() throws IOException {
        // No object at "docs" or "docs/" — only a descendant makes it a directory.
        s3.putBytes("docs/inner/file.txt", bytes("x"));

        var attrs = provider.readAttributes(path("/docs"), BasicFileAttributes.class);

        assertTrue(attrs.isDirectory());
    }

    @Test
    void readAttributesOfRootReportsDirectory() throws IOException {
        var attrs = provider.readAttributes(path("/"), BasicFileAttributes.class);
        assertTrue(attrs.isDirectory());
    }

    @Test
    void readAttributesOfMissingPathThrows() {
        assertThrows(
                NoSuchFileException.class, () -> provider.readAttributes(path("/nope"), BasicFileAttributes.class));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void readAttributesRejectsNonBasicTypes() {
        // Only reachable through a raw cast — the signature already bounds the type parameter.
        Class rawType = String.class;
        assertThrows(
                UnsupportedOperationException.class,
                () -> provider.readAttributes(path("/nope"), (Class<BasicFileAttributes>) rawType));
    }

    @Test
    void readAttributesAsMapExposesTheFourBasicAttributes() throws IOException {
        s3.putBytes("a.txt", bytes("abc"));

        var map = provider.readAttributes(path("/a.txt"), "basic:*");

        assertEquals(3L, map.get("size"));
        assertEquals(FileTime.from(FakeS3Client.LAST_MODIFIED), map.get("lastModifiedTime"));
        assertEquals(false, map.get("isDirectory"));
        assertEquals(true, map.get("isRegularFile"));
    }

    @Test
    void checkAccessPassesForRootExistingFileAndDirectory() throws IOException {
        s3.putBytes("a.txt", bytes("x"));
        s3.putBytes("dir/", new byte[0]);

        provider.checkAccess(path("/"));
        provider.checkAccess(path("/a.txt"));
        provider.checkAccess(path("/dir"));
    }

    @Test
    void checkAccessThrowsForMissingPath() {
        assertThrows(NoSuchFileException.class, () -> provider.checkAccess(path("/missing")));
    }

    // ---- newByteChannel: read ----

    @Test
    void readChannelStreamsExistingContent() throws IOException {
        s3.putBytes("a.txt", bytes("hello"));

        try (var channel = provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.READ))) {
            assertEquals(5, channel.size());
            assertEquals(0, channel.position());
            assertArrayEquals(bytes("hello"), readFully(channel));
        }
    }

    @Test
    void readChannelForMissingObjectThrows() {
        assertThrows(
                NoSuchFileException.class,
                () -> provider.newByteChannel(path("/nope"), Set.of(StandardOpenOption.READ)));
    }

    @Test
    void readChannelRejectsWrites() throws IOException {
        s3.putBytes("a.txt", bytes("hello"));

        try (var channel = provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.READ))) {
            assertThrows(NonWritableChannelException.class, () -> channel.write(ByteBuffer.wrap(bytes("x"))));
        }
    }

    // ---- newByteChannel: write ----

    @Test
    void writeChannelUploadsOnCloseOnly() throws IOException {
        var channel =
                provider.newByteChannel(path("/new.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        channel.write(ByteBuffer.wrap(bytes("payload")));

        assertFalse(s3.containsKey("new.txt"), "Nothing should be uploaded before close");
        channel.close();
        assertArrayEquals(bytes("payload"), s3.bytes("new.txt"));
    }

    @Test
    void writeChannelReplacesExistingContentFromScratch() throws IOException {
        s3.putBytes("a.txt", bytes("old content"));

        try (var channel =
                provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
            channel.write(ByteBuffer.wrap(bytes("new")));
        }

        assertArrayEquals(bytes("new"), s3.bytes("a.txt"));
    }

    @Test
    void createNewThrowsWhenFileExists() {
        s3.putBytes("a.txt", bytes("x"));
        assertThrows(
                FileAlreadyExistsException.class,
                () -> provider.newByteChannel(
                        path("/a.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)));
    }

    @Test
    void createNewThrowsWhenDirectoryExistsAtPath() {
        s3.putBytes("dir/", new byte[0]);
        assertThrows(
                FileAlreadyExistsException.class,
                () -> provider.newByteChannel(
                        path("/dir"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)));
    }

    @Test
    void createNewCreatesMissingFile() throws IOException {
        try (var channel = provider.newByteChannel(
                path("/fresh.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW))) {
            channel.write(ByteBuffer.wrap(bytes("v1")));
        }
        assertArrayEquals(bytes("v1"), s3.bytes("fresh.txt"));
    }

    @Test
    void plainWriteWithoutCreateRequiresExistingObject() {
        assertThrows(
                NoSuchFileException.class,
                () -> provider.newByteChannel(path("/nope"), Set.of(StandardOpenOption.WRITE)));
    }

    @Test
    void appendStartsAtEndOfExistingContent() throws IOException {
        s3.putBytes("log.txt", bytes("abc"));

        try (var channel = provider.newByteChannel(path("/log.txt"), Set.of(StandardOpenOption.APPEND))) {
            assertEquals(3, channel.position());
            channel.write(ByteBuffer.wrap(bytes("def")));
        }

        assertArrayEquals(bytes("abcdef"), s3.bytes("log.txt"));
    }

    @Test
    void appendWithCreateStartsEmptyForMissingObject() throws IOException {
        try (var channel = provider.newByteChannel(
                path("/log.txt"), Set.of(StandardOpenOption.APPEND, StandardOpenOption.CREATE))) {
            assertEquals(0, channel.position());
            channel.write(ByteBuffer.wrap(bytes("def")));
        }

        assertArrayEquals(bytes("def"), s3.bytes("log.txt"));
    }

    // ---- channel mechanics ----

    @Test
    void channelSupportsSeekingAndPartialReads() throws IOException {
        s3.putBytes("a.txt", bytes("abcdef"));

        try (var channel = provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.READ))) {
            channel.position(4);
            var buffer = ByteBuffer.allocate(2);
            assertEquals(2, channel.read(buffer));
            assertArrayEquals(bytes("ef"), buffer.array());
            assertEquals(-1, channel.read(ByteBuffer.allocate(1)), "EOF after the last byte");
        }
    }

    @Test
    void truncateShrinksAndClampsPosition() throws IOException {
        try (var channel =
                provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
            channel.write(ByteBuffer.wrap(bytes("abcdef")));
            assertEquals(6, channel.position());

            channel.truncate(3);
            assertEquals(3, channel.size());
            assertEquals(3, channel.position());

            channel.truncate(10);
            assertEquals(3, channel.size(), "Truncating above the size must not grow the channel");
        }
        assertArrayEquals(bytes("abc"), s3.bytes("a.txt"));
    }

    @Test
    void channelGrowsItsBufferAcrossWrites() throws IOException {
        var chunk = new byte[64 * 1024];
        try (var channel = provider.newByteChannel(
                path("/big.bin"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
            channel.write(ByteBuffer.wrap(chunk));
            channel.write(ByteBuffer.wrap(chunk));
        }
        assertEquals(2 * chunk.length, s3.bytes("big.bin").length);
    }

    @Test
    void closedChannelRejectsEveryOperation() throws IOException {
        var channel =
                provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        channel.close();

        assertFalse(channel.isOpen());
        assertThrows(ClosedChannelException.class, () -> channel.read(ByteBuffer.allocate(1)));
        assertThrows(ClosedChannelException.class, () -> channel.write(ByteBuffer.wrap(bytes("x"))));
        assertThrows(ClosedChannelException.class, channel::position);
        assertThrows(ClosedChannelException.class, () -> channel.position(0));
        assertThrows(ClosedChannelException.class, channel::size);
        assertThrows(ClosedChannelException.class, () -> channel.truncate(0));
    }

    @Test
    void closeIsIdempotent() throws IOException {
        var channel =
                provider.newByteChannel(path("/a.txt"), Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        channel.write(ByteBuffer.wrap(bytes("once")));
        channel.close();
        channel.close();

        assertArrayEquals(bytes("once"), s3.bytes("a.txt"));
    }

    // ---- newDirectoryStream ----

    @Test
    void directoryStreamListsFilesAndSubdirectoriesSkippingTheMarker() throws IOException {
        s3.putBytes("dir/", new byte[0]);
        s3.putBytes("dir/a.txt", bytes("a"));
        s3.putBytes("dir/b.txt", bytes("b"));
        s3.putBytes("dir/sub/inner.txt", bytes("i"));
        s3.putBytes("other/x.txt", bytes("x"));

        // Subdirectories (common prefixes) come first, then files — both alphabetical.
        assertEquals(List.of("sub", "a.txt", "b.txt"), childNames(path("/dir"), p -> true));
    }

    @Test
    void directoryStreamAppliesTheFilter() throws IOException {
        s3.putBytes("dir/a.txt", bytes("a"));
        s3.putBytes("dir/b.txt", bytes("b"));

        var names = childNames(path("/dir"), p -> !p.getFileName().toString().startsWith("b"));

        assertEquals(List.of("a.txt"), names);
    }

    @Test
    void directoryStreamOfEmptyMarkerDirectoryIsEmpty() throws IOException {
        s3.putBytes("dir/", new byte[0]);
        assertEquals(List.of(), childNames(path("/dir"), p -> true));
    }

    @DisplayName("directory stream deduplicates entries repeated across listing pages")
    @Test
    void directoryStreamPaginatesWithoutDuplicates() throws IOException {
        // Page size 2 forces the "dir/sub/" common prefix to be reported on several pages.
        var pagingS3 = new FakeS3Client(2);
        var pagingFs = new S3FileSystem(new S3FileSystemProvider(), "paging-bucket", pagingS3);
        pagingS3.putBytes("dir/a.txt", bytes("a"));
        pagingS3.putBytes("dir/sub/x.txt", bytes("x"));
        pagingS3.putBytes("dir/sub/y.txt", bytes("y"));
        pagingS3.putBytes("dir/sub/z.txt", bytes("z"));
        pagingS3.putBytes("dir/tail.txt", bytes("t"));

        var names = new ArrayList<String>();
        try (var children = pagingFs.provider().newDirectoryStream((S3Path) pagingFs.getPath("/dir"), p -> true)) {
            children.forEach(child -> names.add(child.getFileName().toString()));
        }

        assertEquals(List.of("sub", "a.txt", "tail.txt"), names);
    }

    // ---- createDirectory ----

    @Test
    void createDirectoryPutsAFolderMarker() throws IOException {
        provider.createDirectory(path("/newdir"));
        assertTrue(s3.containsKey("newdir/"));
    }

    @Test
    void createDirectoryThrowsWhenAnythingExistsAtPath() {
        s3.putBytes("file.txt", bytes("x"));
        s3.putBytes("marker/", new byte[0]);
        s3.putBytes("virtual/child.txt", bytes("c"));

        assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(path("/file.txt")));
        assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(path("/marker")));
        assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(path("/virtual")));
    }

    // ---- delete ----

    @Test
    void deleteRemovesAFile() throws IOException {
        s3.putBytes("a.txt", bytes("x"));
        provider.delete(path("/a.txt"));
        assertFalse(s3.containsKey("a.txt"));
    }

    @Test
    void deleteRemovesAMarkerDirectory() throws IOException {
        s3.putBytes("dir/", new byte[0]);
        provider.delete(path("/dir"));
        assertFalse(s3.containsKey("dir/"));
    }

    @Test
    void deletePrefersTheFileWhenBothFileAndMarkerExist() throws IOException {
        s3.putBytes("x", bytes("file"));
        s3.putBytes("x/", new byte[0]);

        provider.delete(path("/x"));

        assertFalse(s3.containsKey("x"));
        assertTrue(s3.containsKey("x/"), "The marker twin must survive a file delete");
    }

    @Test
    void deleteOfVirtualFolderOrMissingPathThrows() {
        s3.putBytes("virtual/child.txt", bytes("c"));

        assertThrows(NoSuchFileException.class, () -> provider.delete(path("/virtual")));
        assertThrows(NoSuchFileException.class, () -> provider.delete(path("/absent")));
    }

    // ---- copy ----

    @Test
    void copyDuplicatesAFile() throws IOException {
        s3.putBytes("src.txt", bytes("data"));

        provider.copy(path("/src.txt"), path("/dst.txt"));

        assertArrayEquals(bytes("data"), s3.bytes("dst.txt"));
        assertArrayEquals(bytes("data"), s3.bytes("src.txt"));
    }

    @Test
    void copyOntoExistingTargetRequiresReplaceExisting() {
        s3.putBytes("src.txt", bytes("new"));
        s3.putBytes("dst.txt", bytes("old"));

        assertThrows(FileAlreadyExistsException.class, () -> provider.copy(path("/src.txt"), path("/dst.txt")));
        assertArrayEquals(bytes("old"), s3.bytes("dst.txt"));
    }

    @Test
    void copyWithReplaceExistingOverwrites() throws IOException {
        s3.putBytes("src.txt", bytes("new"));
        s3.putBytes("dst.txt", bytes("old"));

        provider.copy(path("/src.txt"), path("/dst.txt"), StandardCopyOption.REPLACE_EXISTING);

        assertArrayEquals(bytes("new"), s3.bytes("dst.txt"));
    }

    @Test
    void copyOfMarkerDirectoryCreatesAnEmptyDirectoryWithoutEntries() throws IOException {
        s3.putBytes("src/", new byte[0]);
        s3.putBytes("src/child.txt", bytes("c"));

        provider.copy(path("/src"), path("/dst"));

        assertTrue(s3.containsKey("dst/"));
        assertFalse(s3.containsKey("dst/child.txt"), "Files.copy does not recurse into directories");
    }

    @Test
    void copyOfVirtualFolderCreatesNothing() throws IOException {
        s3.putBytes("src/child.txt", bytes("c"));

        provider.copy(path("/src"), path("/dst"));

        assertFalse(s3.containsKey("dst/"));
        assertFalse(s3.containsKey("dst/child.txt"));
    }

    @Test
    void copyOfMissingSourceThrows() {
        assertThrows(NoSuchFileException.class, () -> provider.copy(path("/nope"), path("/dst")));
    }

    // ---- move ----

    @Test
    void moveTransfersContentAndRemovesTheSource() throws IOException {
        s3.putBytes("src.txt", bytes("data"));

        provider.move(path("/src.txt"), path("/dst.txt"));

        assertFalse(s3.containsKey("src.txt"));
        assertArrayEquals(bytes("data"), s3.bytes("dst.txt"));
    }

    @Test
    void moveOntoExistingTargetRequiresReplaceExisting() {
        s3.putBytes("src.txt", bytes("new"));
        s3.putBytes("dst.txt", bytes("old"));

        assertThrows(FileAlreadyExistsException.class, () -> provider.move(path("/src.txt"), path("/dst.txt")));
        assertArrayEquals(bytes("new"), s3.bytes("src.txt"));
        assertArrayEquals(bytes("old"), s3.bytes("dst.txt"));
    }

    @Test
    void moveWithReplaceExistingOverwrites() throws IOException {
        s3.putBytes("src.txt", bytes("new"));
        s3.putBytes("dst.txt", bytes("old"));

        provider.move(path("/src.txt"), path("/dst.txt"), StandardCopyOption.REPLACE_EXISTING);

        assertFalse(s3.containsKey("src.txt"));
        assertArrayEquals(bytes("new"), s3.bytes("dst.txt"));
    }

    @Test
    void moveToItselfThrows() {
        s3.putBytes("src.txt", bytes("x"));
        assertThrows(FileSystemException.class, () -> provider.move(path("/src.txt"), path("/src.txt")));
    }

    @Test
    void moveOfMissingSourceThrows() {
        assertThrows(NoSuchFileException.class, () -> provider.move(path("/nope"), path("/dst.txt")));
    }

    // ---- misc ----

    @Test
    void isSameFileComparesPaths() {
        assertTrue(provider.isSameFile(path("/a"), path("/a")));
        assertFalse(provider.isSameFile(path("/a"), path("/b")));
    }

    @Test
    void isHiddenIsAlwaysFalse() {
        assertFalse(provider.isHidden(path("/.hidden")));
    }

    @Test
    void getFileAttributeViewIsNotExposed() {
        assertNull(provider.getFileAttributeView(path("/a"), BasicFileAttributeView.class));
    }

    @Test
    void setAttributeIsUnsupported() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> provider.setAttribute(path("/a"), "basic:lastModifiedTime", FileTime.fromMillis(0)));
    }

    @Test
    void operationsRejectForeignPathImplementations() {
        assertThrows(IllegalArgumentException.class, () -> provider.checkAccess(Path.of("/tmp")));
    }

    @Test
    void getPathOnBareBucketUriYieldsRoot() throws Exception {
        var registryProvider = new S3FileSystemProvider();
        try (var ignored = registryProvider.newFileSystem(URI.create("s3://bare-bucket"), Map.of())) {
            assertEquals(
                    "/",
                    registryProvider.getPath(URI.create("s3://bare-bucket")).toString());
        }
    }

    // ---- helpers ----

    private List<String> childNames(S3Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        var names = new ArrayList<String>();
        try (var children = provider.newDirectoryStream(dir, filter)) {
            children.forEach(child -> names.add(child.getFileName().toString()));
        }
        return names;
    }

    private static byte[] readFully(SeekableByteChannel channel) throws IOException {
        var buffer = ByteBuffer.allocate((int) channel.size());
        while (channel.read(buffer) > 0) {
            // keep reading
        }
        return buffer.array();
    }
}
