package com.github.vfss3.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class S3UrisTest {

    @Test
    void recognizesS3LocationsCaseInsensitively() {
        assertTrue(S3Uris.isS3Location("s3://bucket/key"));
        assertTrue(S3Uris.isS3Location("S3://bucket/key"));
        assertFalse(S3Uris.isS3Location("file:///tmp/x"));
        assertFalse(S3Uris.isS3Location("classpath:x"));
        assertFalse(S3Uris.isS3Location(null));
    }

    @Test
    void parsesBucketAndKey() {
        var parsed = S3Uris.parse("s3://bucket/path/to/file.txt");
        assertEquals("bucket", parsed.bucket());
        assertEquals("path/to/file.txt", parsed.key());
        assertEquals(Map.of(), parsed.query());
    }

    @Test
    void parsesBucketOnlyLocations() {
        assertEquals("", S3Uris.parse("s3://bucket").key());
        assertEquals("", S3Uris.parse("s3://bucket/").key());
    }

    @Test
    void parsesQueryParameters() {
        var parsed = S3Uris.parse("s3://bucket/key?region=eu-central-1&endpoint=http://localhost:9000");
        assertEquals("eu-central-1", parsed.query().get(S3Uris.QUERY_REGION));
        assertEquals("http://localhost:9000", parsed.query().get(S3Uris.QUERY_ENDPOINT));
        assertEquals("key", parsed.key());
    }

    @Test
    void keyKeepsSpacesLiterally() {
        assertEquals(
                "my folder/name with spaces.txt",
                S3Uris.parse("s3://bucket/my folder/name with spaces.txt").key());
    }

    @Test
    void rejectsUnknownQueryParameters() {
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse("s3://bucket/key?reigon=eu-west-1"));
    }

    @Test
    void rejectsUserinfoCredentials() {
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse("s3://access:secret@bucket/key"));
    }

    @Test
    void rejectsMissingBucket() {
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse("s3://"));
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse("s3:///key"));
    }

    @Test
    void rejectsNonS3Locations() {
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse("file:///tmp/x"));
    }

    @Test
    void parsesAndValidatesUris() {
        var parsed = S3Uris.parse(URI.create("s3://bucket/path/file.txt"));
        assertEquals("bucket", parsed.bucket());
        assertEquals("path/file.txt", parsed.key());
    }

    @Test
    void uriParseRejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse(URI.create("file:///tmp/x")));
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse(URI.create("s3://user:pass@bucket/key")));
        assertThrows(IllegalArgumentException.class, () -> S3Uris.parse(URI.create("s3:///key")));
    }

    @Test
    void toUriEncodesAndKeyDecodesBack() {
        var uri = S3Uris.toUri("bucket", "my file.txt");
        assertEquals("s3://bucket/my%20file.txt", uri.toString());
        assertEquals("my file.txt", S3Uris.key(uri));
    }

    @Test
    void toUriOfEmptyKeyIsTheBucketRoot() {
        var uri = S3Uris.toUri("bucket", "");
        assertEquals("s3://bucket", uri.toString());
        assertEquals("", S3Uris.key(uri));
    }

    @Test
    void keyStripsTheLeadingSlash() {
        assertEquals("path/file.txt", S3Uris.key(URI.create("s3://bucket/path/file.txt")));
    }
}
