package com.github.vfss3.spring.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.spring.S3ResourcePatternResolver;
import com.github.vfss3.spring.SpringIntegrationContext;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite B: Directory Operations through the Spring Resource API — see
 * {@code docs/test-cases/b-directory-operations.md}.
 *
 * <p>The Spring {@link org.springframework.core.io.Resource} abstraction has no directory or
 * listing concept, so the suite covers the directory-shaped behaviour that <em>is</em>
 * expressible: writing several keys under a common prefix, resolving each one, and listing them
 * back with Ant-style wildcard patterns through {@link S3ResourcePatternResolver}.
 */
final class DirectoryOperationsTest {

    private static final String PREFIX = "dir-ops/";

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

    private void write(String key, String content) throws IOException {
        try (OutputStream out = resource(key).getOutputStream()) {
            out.write(content.getBytes(UTF_8));
        }
    }

    /** Step 4 (analog): write several keys under a prefix and resolve each one back. */
    @DisplayName("Steps 1/4 analog: keys written under a prefix each resolve and exist")
    @Test
    void keysUnderPrefixResolveIndependently() throws IOException {
        for (int i = 0; i < 5; i++) {
            write("my-folder/" + i + ".tmp", "file-" + i);
        }

        for (int i = 0; i < 5; i++) {
            var resource = resource("my-folder/" + i + ".tmp");
            assertTrue(resource.exists(), i + ".tmp should exist");
            assertEquals(i + ".tmp", resource.getFilename());
        }
    }

    /** A key written under a deep prefix can hold a name containing a space. */
    @DisplayName("Step 3 analog: a key name with a space round-trips")
    @Test
    void keyNameWithSpace() throws IOException {
        write("folder with space/file.tmp", "x");
        assertTrue(resource("folder with space/file.tmp").exists(), "Key under a spaced prefix should exist");
    }

    /** Step 5: wildcard listing through the pattern resolver returns the matching keys. */
    @DisplayName("Step 5: wildcard listing returns the matching keys in order")
    @Test
    void wildcardListingReturnsMatches() throws IOException {
        for (int i = 0; i < 5; i++) {
            write("my-folder/" + i + ".tmp", "file-" + i);
        }
        write("my-folder/other.dat", "x");

        var resources = loader.getResources(SpringIntegrationContext.location(PREFIX + "my-folder/*.tmp"));

        assertEquals(5, resources.length, "Only the five *.tmp keys should match");
        for (int i = 0; i < 5; i++) {
            assertEquals(i + ".tmp", resources[i].getFilename(), "Matches should come back in key order");
        }
    }

    /** Step 5b: a recursive {@code **} pattern crosses prefix levels. */
    @DisplayName("Step 5b: a recursive pattern matches keys at every depth")
    @Test
    void recursiveWildcardCrossesLevels() throws IOException {
        write("my-folder/top.tmp", "t");
        write("my-folder/sub/deep.tmp", "d");

        var resources = loader.getResources(SpringIntegrationContext.location(PREFIX + "my-folder/**"));

        assertEquals(2, resources.length, "Both depths should match a recursive pattern");
    }

    /** Step 5c: a pattern with no matches yields an empty array, not an exception. */
    @DisplayName("Step 5c: zero matches yield an empty array")
    @Test
    void zeroMatchesYieldAnEmptyArray() throws IOException {
        write("my-folder/only.tmp", "x");
        assertEquals(0, loader.getResources(SpringIntegrationContext.location(PREFIX + "my-folder/*.absent")).length);
    }
}
