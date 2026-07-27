package com.github.vfss3.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class S3ResourceTest {

    private FakeS3Client s3;

    @BeforeEach
    void setUp() {
        s3 = new FakeS3Client();
    }

    private S3Resource resource(String location) {
        return new S3Resource(location, s3);
    }

    @Test
    void bucketAndKeyAreExtractedFromUri() {
        var resource = resource("s3://my-bucket/path/to/object.txt");
        assertEquals("my-bucket", resource.getBucket());
        assertEquals("path/to/object.txt", resource.getKey());
    }

    @Test
    void filenameIsLastSegment() {
        assertEquals("object.txt", resource("s3://my-bucket/path/to/object.txt").getFilename());
    }

    @Test
    void descriptionContainsUri() {
        assertEquals(
                "S3 resource [s3://my-bucket/key]",
                resource("s3://my-bucket/key").getDescription());
    }

    @Test
    void rejectsBadScheme() {
        assertThrows(IllegalArgumentException.class, () -> new S3Resource("file:///tmp/foo", s3));
    }

    @Test
    void rejectsCredentialsInLocation() {
        assertThrows(IllegalArgumentException.class, () -> new S3Resource("s3://access:secret@bucket/key", s3));
    }

    @Test
    void createRelativeAddsPath() throws IOException {
        var base = resource("s3://my-bucket/data/");
        var relative = base.createRelative("file.txt");
        assertEquals("s3://my-bucket/data/file.txt", relative.getURI().toString());
    }

    @Test
    void createRelativeResolvesSiblings() throws IOException {
        var base = resource("s3://my-bucket/data/one.txt");
        assertEquals("data/two.txt", base.createRelative("two.txt").getKey());
    }

    @Test
    void createRelativeSharesTheClient() throws IOException {
        s3.putBytes("data/two.txt", "x".getBytes(UTF_8));
        var relative = resource("s3://my-bucket/data/one.txt").createRelative("two.txt");
        assertTrue(relative.exists(), "The relative resource must be backed by the same client");
    }

    @Test
    void existsChecksTheBackend() {
        s3.putBytes("present.txt", "x".getBytes(UTF_8));
        assertTrue(resource("s3://my-bucket/present.txt").exists());
        assertFalse(resource("s3://my-bucket/missing.txt").exists());
    }

    @Test
    void existsReadsPermissionDeniedAsFalse() {
        s3.denyKey("secret.txt");
        assertFalse(resource("s3://my-bucket/secret.txt").exists());
    }

    @Test
    void readStreamsTheObject() throws IOException {
        s3.putBytes("file.txt", "Hello, S3!".getBytes(UTF_8));
        try (var in = resource("s3://my-bucket/file.txt").getInputStream()) {
            assertEquals("Hello, S3!", new String(in.readAllBytes(), UTF_8));
        }
    }

    @Test
    void readMissingObjectThrowsFileNotFound() {
        var resource = resource("s3://my-bucket/missing.txt");
        assertThrows(FileNotFoundException.class, resource::getInputStream);
    }

    @Test
    void readWithoutPermissionThrowsPlainIoException() {
        s3.denyKey("secret.txt");
        var resource = resource("s3://my-bucket/secret.txt");
        var e = assertThrows(IOException.class, resource::getInputStream);
        assertFalse(e instanceof FileNotFoundException, "A permission failure must not read as a missing object");
    }

    @Test
    void writeUploadsOnClose() throws IOException {
        var resource = resource("s3://my-bucket/out.bin");
        try (var out = resource.getOutputStream()) {
            out.write("payload".getBytes(UTF_8));
            assertFalse(s3.containsKey("out.bin"), "Upload must happen on close, not before");
        }
        assertArrayEquals("payload".getBytes(UTF_8), s3.bytes("out.bin"));
    }

    @Test
    void writeCloseIsIdempotent() throws IOException {
        var out = resource("s3://my-bucket/out.bin").getOutputStream();
        out.write("once".getBytes(UTF_8));
        out.close();
        out.close();
        assertArrayEquals("once".getBytes(UTF_8), s3.bytes("out.bin"));
    }

    @Test
    void contentLengthAndLastModifiedComeFromHead() throws IOException {
        s3.putBytes("file.txt", new byte[42]);
        var resource = resource("s3://my-bucket/file.txt");
        assertEquals(42, resource.contentLength());
        assertEquals(FakeS3Client.LAST_MODIFIED.toEpochMilli(), resource.lastModified());
    }

    @Test
    void metadataOfMissingObjectThrowsFileNotFound() {
        var resource = resource("s3://my-bucket/missing.txt");
        assertThrows(FileNotFoundException.class, resource::contentLength);
        assertThrows(FileNotFoundException.class, resource::lastModified);
    }

    @Test
    void urlAndFileAreNotResolvable() {
        var resource = resource("s3://my-bucket/key");
        assertThrows(FileNotFoundException.class, resource::getURL);
        assertThrows(FileNotFoundException.class, resource::getFile);
    }

    @Test
    void isWritableIsTrue() {
        assertTrue(resource("s3://my-bucket/key").isWritable());
    }

    @Test
    void equalityIsByUri() {
        var first = resource("s3://my-bucket/key");
        var second = resource("s3://my-bucket/key");
        var other = resource("s3://my-bucket/other");
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, other);
    }

    @Test
    void keyWithSpacesRoundTrips() throws IOException {
        var resource = resource("s3://my-bucket/name with spaces.txt");
        assertEquals("name with spaces.txt", resource.getKey());
        assertEquals(
                "s3://my-bucket/name%20with%20spaces.txt", resource.getURI().toString());
        try (var out = resource.getOutputStream()) {
            out.write("x".getBytes(UTF_8));
        }
        assertTrue(s3.containsKey("name with spaces.txt"), "The raw key, not the encoded one, must reach S3");
        assertTrue(resource.exists());
    }

    @Test
    void contentAsStringDefaultWorks() throws IOException {
        s3.putBytes("file.txt", "text".getBytes(UTF_8));
        assertEquals("text", resource("s3://my-bucket/file.txt").getContentAsString(UTF_8));
    }
}
