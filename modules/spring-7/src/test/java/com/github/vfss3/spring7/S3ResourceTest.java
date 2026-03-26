package com.github.vfss3.spring7;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class S3ResourceTest {

    @Test
    void bucketAndKeyAreExtractedFromUri() {
        var resource = new S3Resource("s3://my-bucket/path/to/object.txt");
        assertEquals("my-bucket", resource.getBucket());
        assertEquals("path/to/object.txt", resource.getKey());
    }

    @Test
    void filenameIsLastSegment() {
        var resource = new S3Resource("s3://my-bucket/path/to/object.txt");
        assertEquals("object.txt", resource.getFilename());
    }

    @Test
    void descriptionContainsUri() {
        var resource = new S3Resource("s3://my-bucket/key");
        assertEquals("S3 resource [s3://my-bucket/key]", resource.getDescription());
    }

    @Test
    void existsReturnsFalseByDefault() {
        assertFalse(new S3Resource("s3://my-bucket/key").exists());
    }

    @Test
    void rejectsBadScheme() {
        assertThrows(IllegalArgumentException.class, () -> new S3Resource("file:///tmp/foo"));
    }

    @Test
    void createRelativeAddsPath() throws IOException {
        var base = new S3Resource("s3://my-bucket/data/");
        var relative = base.createRelative("file.txt");
        assertEquals("s3://my-bucket/data/file.txt", relative.getURI().toString());
    }

    @Test
    void resourceLoaderResolvesS3Uri() {
        var loader = new S3ResourceLoader();
        var resource = loader.getResource("s3://my-bucket/key.json");
        assertInstanceOf(S3Resource.class, resource);
        assertEquals("my-bucket", ((S3Resource) resource).getBucket());
    }

    @Test
    void resourceLoaderDelegatesNonS3Uri() {
        var loader = new S3ResourceLoader();
        var resource = loader.getResource("classpath:test.properties");
        assertFalse(resource instanceof S3Resource);
    }
}
