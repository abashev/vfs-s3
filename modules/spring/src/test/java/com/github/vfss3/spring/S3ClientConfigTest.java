package com.github.vfss3.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

final class S3ClientConfigTest {

    @Test
    void defaultsHaveNoRegionOrEndpoint() {
        var config = S3ClientConfig.defaults();
        assertNull(config.region());
        assertNull(config.endpoint());
        assertNotNull(config.credentialsProvider());
    }

    @Test
    void withersReplaceOneFieldEach() {
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));
        var config = S3ClientConfig.defaults()
                .withRegion("eu-central-1")
                .withEndpoint("http://localhost:9000")
                .withCredentialsProvider(credentials);
        assertEquals("eu-central-1", config.region());
        assertEquals(URI.create("http://localhost:9000"), config.endpoint());
        assertSame(credentials, config.credentialsProvider());
    }

    // Passing an explicit null is the point here — the record must normalize it.
    @SuppressWarnings("NullAway")
    @Test
    void nullCredentialsNormalizeToTheDefaultChain() {
        assertNotNull(new S3ClientConfig("eu-central-1", null, null).credentialsProvider());
    }

    @Test
    void explicitConfigWinsOverTheQuery() {
        var config = S3ClientConfig.defaults().withRegion("eu-west-1").withEndpoint("http://localhost:9000");
        var resolved = config.resolve(Map.of(
                S3Uris.QUERY_REGION, "us-east-1",
                S3Uris.QUERY_ENDPOINT, "http://localhost:4566"));
        assertEquals("eu-west-1", resolved.region());
        assertEquals(URI.create("http://localhost:9000"), resolved.endpoint());
    }

    @Test
    void queryFillsUnsetValues() {
        var resolved = S3ClientConfig.defaults()
                .resolve(Map.of(
                        S3Uris.QUERY_REGION, "us-east-1",
                        S3Uris.QUERY_ENDPOINT, "http://localhost:9000"));
        assertEquals("us-east-1", resolved.region());
        assertEquals(URI.create("http://localhost:9000"), resolved.endpoint());
    }

    @Test
    void emptyQueryLeavesValuesUnset() {
        var resolved = S3ClientConfig.defaults().resolve(Map.of());
        assertNull(resolved.region());
        assertNull(resolved.endpoint());
    }

    @Test
    void buildS3ClientWorksWithNoOverrides() {
        // Client construction must stay offline-safe — no region/credentials configured anywhere.
        try (var client = S3ClientConfig.defaults().buildS3Client()) {
            assertNotNull(client);
        }
    }

    @Test
    void buildS3ClientWorksWithEndpointOverride() {
        try (var client = S3ClientConfig.defaults()
                .withRegion("us-east-1")
                .withEndpoint("http://localhost:9000")
                .buildS3Client()) {
            assertNotNull(client);
        }
    }
}
