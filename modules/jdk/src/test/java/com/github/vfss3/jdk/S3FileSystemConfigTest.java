package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

class S3FileSystemConfigTest {

    @Test
    void fromEnvFallsBackToSdkDefaultsWhenEnvIsEmpty() {
        var config = S3FileSystemConfig.fromEnv(Map.of());

        assertNull(config.region());
        assertNull(config.endpoint());
        assertInstanceOf(DefaultCredentialsProvider.class, config.credentialsProvider());
    }

    @Test
    void fromEnvReadsRegionOverride() {
        var config = S3FileSystemConfig.fromEnv(Map.of("aws.region", "eu-central-1"));

        assertEquals("eu-central-1", config.region());
    }

    @Test
    void fromEnvReadsEndpointOverride() {
        var config = S3FileSystemConfig.fromEnv(Map.of("aws.endpoint", "http://localhost:9000"));

        assertEquals(URI.create("http://localhost:9000"), config.endpoint());
    }

    @Test
    void fromEnvReadsCredentialsProviderOverride() {
        AwsCredentialsProvider provider = StaticCredentialsProvider.create(AwsBasicCredentials.create("k", "s"));

        var config = S3FileSystemConfig.fromEnv(Map.of("aws.credentialsProvider", provider));

        assertSame(provider, config.credentialsProvider());
    }

    @Test
    void buildS3ClientAppliesOverridesWithoutThrowing() {
        AwsCredentialsProvider provider = StaticCredentialsProvider.create(AwsBasicCredentials.create("k", "s"));
        var config = new S3FileSystemConfig("us-east-1", URI.create("http://localhost:9000"), provider);

        try (var client = config.buildS3Client()) {
            assertNotNull(client);
        }
    }

    @Test
    void buildS3ClientWorksWithNoOverrides() {
        var config = S3FileSystemConfig.fromEnv(Map.of());

        try (var client = config.buildS3Client()) {
            assertNotNull(client);
        }
    }
}
