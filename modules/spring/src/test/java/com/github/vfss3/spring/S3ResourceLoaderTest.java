package com.github.vfss3.spring;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

final class S3ResourceLoaderTest {

    @Test
    void resolvesS3LocationsToS3Resources() {
        try (var loader = new S3ResourceLoader(new FakeS3Client())) {
            var resource = loader.getResource("s3://my-bucket/key.json");
            assertInstanceOf(S3Resource.class, resource);
            assertEquals("my-bucket", ((S3Resource) resource).getBucket());
            assertEquals("key.json", ((S3Resource) resource).getKey());
        }
    }

    @Test
    void delegatesNonS3LocationsToSpring() {
        try (var loader = new S3ResourceLoader(new FakeS3Client())) {
            assertFalse(loader.getResource("classpath:test.properties") instanceof S3Resource);
        }
    }

    @Test
    void rejectsUnknownQueryParameters() {
        try (var loader = new S3ResourceLoader(new FakeS3Client())) {
            var e = assertThrows(
                    IllegalArgumentException.class, () -> loader.getResource("s3://bucket/key?reigon=eu-west-1"));
            assertTrue(requireNonNull(e.getMessage()).contains("reigon"), "The rejected parameter must be named");
        }
    }

    @Test
    void rejectsCredentialsInLocation() {
        try (var loader = new S3ResourceLoader(new FakeS3Client())) {
            assertThrows(IllegalArgumentException.class, () -> loader.getResource("s3://access:secret@bucket/key"));
        }
    }

    @Test
    void rejectsMissingBucket() {
        try (var loader = new S3ResourceLoader(new FakeS3Client())) {
            assertThrows(IllegalArgumentException.class, () -> loader.getResource("s3://"));
            assertThrows(IllegalArgumentException.class, () -> loader.getResource("s3:///key"));
        }
    }

    @Test
    void queryStaysOutOfTheResourceUri() {
        try (var loader = new S3ResourceLoader(new FakeS3Client())) {
            var resource = (S3Resource)
                    loader.getResource("s3://bucket/key?region=eu-central-1&endpoint=http://localhost:9000");
            // Query parameters are configuration, not identity.
            assertEquals("s3://bucket/key", resource.getURI().toString());
        }
    }

    @Test
    void cachesOneClientPerEffectiveRegionEndpointPair() {
        try (var loader = new S3ResourceLoader(S3ClientConfig.defaults().withRegion("us-east-1"))) {
            var plain = loader.clientFor(Map.of());
            // The loader's explicit region wins, so a query region resolves to the same client.
            var queryRegion = loader.clientFor(Map.of(S3Uris.QUERY_REGION, "eu-west-1"));
            assertSame(plain, queryRegion);
            // The endpoint is unset on the loader, so a query endpoint is a new effective pair.
            var queryEndpoint = loader.clientFor(Map.of(S3Uris.QUERY_ENDPOINT, "http://localhost:9000"));
            assertNotSame(plain, queryEndpoint);
        }
    }

    @Test
    void queryRegionIsUsedWhenTheLoaderHasNone() {
        try (var loader = new S3ResourceLoader(S3ClientConfig.defaults())) {
            var first = loader.clientFor(Map.of(S3Uris.QUERY_REGION, "eu-west-1"));
            var second = loader.clientFor(Map.of(S3Uris.QUERY_REGION, "eu-west-1"));
            var other = loader.clientFor(Map.of(S3Uris.QUERY_REGION, "us-east-1"));
            assertSame(first, second);
            assertNotSame(first, other);
        }
    }

    @Test
    void closeLeavesACallerSuppliedClientOpen() {
        var fixed = new FakeS3Client();
        var loader = new S3ResourceLoader(fixed);
        loader.getResource("s3://bucket/key");
        loader.close();
        assertFalse(fixed.isClosed(), "A caller-supplied client must stay open after close()");
    }

    @Test
    void closedLoaderRejectsNewClients() {
        var loader = new S3ResourceLoader(S3ClientConfig.defaults());
        loader.close();
        assertThrows(IllegalStateException.class, () -> loader.getResource("s3://bucket/key"));
    }

    @Test
    void closeIsIdempotent() {
        var loader = new S3ResourceLoader(S3ClientConfig.defaults());
        loader.close();
        loader.close();
    }
}
