package com.github.vfss3.spring.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.spring.S3ResourcePatternResolver;
import com.github.vfss3.spring.SpringIntegrationContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite E: Copy Operations through the Spring Resource API — see
 * {@code docs/test-cases/e-copy-operations.md}.
 *
 * <p>The Spring {@link org.springframework.core.io.Resource} API has no native copy or
 * directory-tree operation, so a copy is performed by streaming the source into the target.
 * Recursive directory copy from the original suite is not expressible and is omitted.
 */
final class CopyOperationsTest {

    private static final String PREFIX = "copy/";
    private static final byte[] CONTENT = "content-to-copy".getBytes(UTF_8);

    private S3ResourcePatternResolver loader;

    @BeforeEach
    void setUp() {
        loader = SpringIntegrationContext.loader();
    }

    @AfterEach
    void tearDown() {
        SpringIntegrationContext.deletePrefix(PREFIX);
    }

    private WritableResource resource(String key) {
        return (WritableResource) loader.getResource(SpringIntegrationContext.location(PREFIX + key));
    }

    /** Copy a single resource by streaming source → target; verify content and size match. */
    @DisplayName("Copy a resource by streaming source into target")
    @Test
    void copySingleResource() throws IOException {
        var source = resource("child-file.tmp");
        try (OutputStream out = source.getOutputStream()) {
            out.write(CONTENT);
        }

        var target = resource("child-file-copy.tmp");
        try (InputStream in = source.getInputStream();
                OutputStream out = target.getOutputStream()) {
            in.transferTo(out);
        }

        assertTrue(target.exists(), "Copy should exist");
        assertEquals(source.contentLength(), target.contentLength(), "Copy size should match source");

        try (InputStream in = target.getInputStream()) {
            assertArrayEquals(CONTENT, in.readAllBytes(), "Copy content should match source");
        }
    }
}
