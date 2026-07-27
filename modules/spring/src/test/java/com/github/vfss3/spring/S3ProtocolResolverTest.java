package com.github.vfss3.spring;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;

final class S3ProtocolResolverTest {

    @Test
    void resolvesS3LocationsThroughTheLoader() {
        try (var s3Loader = new S3ResourceLoader(new FakeS3Client())) {
            var resolver = new S3ProtocolResolver(s3Loader);
            assertInstanceOf(S3Resource.class, resolver.resolve("s3://bucket/key", new DefaultResourceLoader()));
        }
    }

    @Test
    void returnsNullForOtherLocations() {
        try (var s3Loader = new S3ResourceLoader(new FakeS3Client())) {
            var resolver = new S3ProtocolResolver(s3Loader);
            assertNull(resolver.resolve("classpath:test.properties", new DefaultResourceLoader()));
        }
    }

    @Test
    void plugsIntoAnExistingResourceLoader() {
        try (var s3Loader = new S3ResourceLoader(new FakeS3Client())) {
            var appLoader = new DefaultResourceLoader();
            appLoader.addProtocolResolver(new S3ProtocolResolver(s3Loader));

            assertInstanceOf(S3Resource.class, appLoader.getResource("s3://bucket/config.yml"));
            assertFalse(appLoader.getResource("classpath:test.properties") instanceof S3Resource);
        }
    }
}
