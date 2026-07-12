package com.github.vfss3.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite B: Directory Operations for the Spring Resource mock backend — see
 * {@code docs/test-cases/b-directory-operations.md}.
 *
 * <p>The Spring {@link org.springframework.core.io.Resource} abstraction has no directory or
 * listing concept, so the suite covers the directory-shaped behaviour that <em>is</em>
 * expressible: writing several keys under a common prefix and resolving each one. Wildcard
 * listing through {@link S3ResourcePatternResolver} is asserted to be unsupported (it requires
 * a real S3 listing call), matching the current scaffolding contract.
 */
final class DirectoryOperationsTest {

    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "s3://" + BUCKET + "/dir-ops/";

    private S3ResourceLoader loader;

    @BeforeEach
    void setUp() {
        loader = new S3ResourceLoader();
    }

    @AfterEach
    void tearDown() throws IOException {
        loader.close();
    }

    /** Step 4 (analog): write several keys under a prefix and resolve each one back. */
    @DisplayName("Steps 1/4 analog: keys written under a prefix each resolve and exist")
    @Test
    void keysUnderPrefixResolveIndependently() throws IOException {
        for (int i = 0; i < 5; i++) {
            var resource = (WritableResource) loader.getResource(PREFIX + "my-folder/" + i + ".tmp");
            try (OutputStream out = resource.getOutputStream()) {
                out.write(("file-" + i).getBytes(UTF_8));
            }
        }

        for (int i = 0; i < 5; i++) {
            var resource = loader.getResource(PREFIX + "my-folder/" + i + ".tmp");
            assertTrue(resource.exists(), i + ".tmp should exist");
            assertEquals(i + ".tmp", resource.getFilename());
        }
    }

    /** A key written under a deep prefix can hold a name containing a space. */
    @DisplayName("Step 3 analog: a key name with a space round-trips")
    @Test
    void keyNameWithSpace() throws IOException {
        var resource = (WritableResource) loader.getResource(PREFIX + "folder with space/file.tmp");
        try (OutputStream out = resource.getOutputStream()) {
            out.write("x".getBytes(UTF_8));
        }
        assertTrue(resource.exists(), "Key under a spaced prefix should exist");
    }

    /** Step 5 (not applicable): wildcard pattern listing is not supported by the scaffolding. */
    @DisplayName("Step 5: wildcard listing is unsupported on the Resource API")
    @Test
    void wildcardListingIsUnsupported() {
        var resolver = new S3ResourcePatternResolver();
        assertThrows(
                UnsupportedOperationException.class,
                () -> resolver.getResources(PREFIX + "my-folder/*.tmp"),
                "Wildcard S3 listing should be reported as unsupported");
    }
}
