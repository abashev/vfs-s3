package com.github.vfss3.jdk.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.reverseOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Suite E: Copy Operations — see {@code docs/test-cases/e-copy-operations.md}. A recursive copy
 * is implemented with {@link Files#walk} since {@link Files#copy} does not recurse into
 * directories. Runs against whatever real S3-compatible backend the enclosing {@code @Suite}
 * wired up via {@link JdkIntegrationContext}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CopyOperationsTest {

    private static final String PREFIX = "/copy/";

    private FileSystem fs;

    @BeforeEach
    void setUp() {
        fs = JdkIntegrationContext.fileSystem();
    }

    @AfterEach
    void tearDown() throws IOException {
        var prefix = fs.getPath(PREFIX);
        if (Files.exists(prefix)) {
            try (Stream<Path> walk = Files.walk(prefix)) {
                walk.sorted(reverseOrder()).forEach(CopyOperationsTest::deleteQuietly);
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Steps 1–4: build a tree, copy a sub-directory, list, then delete children")
    void copyTreeRoundTrip() throws IOException {
        var base = fs.getPath(PREFIX);
        Files.createDirectories(base);

        // Step 1: build the tree.
        Files.write(base.resolve("child-file.tmp"), bytes("a"));
        Files.write(base.resolve("child-file2.tmp"), bytes("b"));
        Files.createDirectories(base.resolve("child-dir"));
        Files.write(base.resolve("child-dir/descendant.tmp"), bytes("c"));
        Files.write(base.resolve("child-dir/descendant2.tmp"), bytes("d"));
        Files.createDirectories(base.resolve("child-dir/descendant-dir"));

        try (Stream<Path> walk = Files.walk(base)) {
            assertEquals(4, walk.filter(Files::isRegularFile).count(), "SELECT_FILES");
        }

        // Step 2: recursively copy child-dir → child-dir-copy.
        var source = base.resolve("child-dir");
        var copy = base.resolve("child-dir-copy");
        copyRecursively(source, copy);

        assertTrue(Files.exists(copy), "Copy should exist");
        assertEquals(walkCount(source), walkCount(copy), "Copy should mirror the source");

        // Step 3: the prefix lists the originals plus the copy.
        var names = new TreeSet<String>();
        try (DirectoryStream<Path> children = Files.newDirectoryStream(base)) {
            for (Path child : children) {
                names.add(child.getFileName().toString());
            }
        }
        assertEquals(
                new TreeSet<>(java.util.List.of("child-file.tmp", "child-file2.tmp", "child-dir", "child-dir-copy")),
                names);

        // Step 4: delete every child of the prefix.
        try (Stream<Path> walk = Files.walk(base)) {
            walk.filter(p -> !p.equals(base)).sorted(reverseOrder()).forEach(CopyOperationsTest::deleteQuietly);
        }
        assertFalse(Files.exists(base.resolve("child-dir")), "child-dir should be gone");
    }

    private static void copyRecursively(Path source, Path target) throws IOException {
        try (Stream<Path> walk = Files.walk(source)) {
            for (Path path : (Iterable<Path>) walk::iterator) {
                var relative = source.relativize(path);
                var dest = relative.toString().isEmpty() ? target : target.resolve(relative.toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(dest);
                } else {
                    var parent = dest.getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    Files.copy(path, dest);
                }
            }
        }
    }

    private static long walkCount(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.count();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
            // best-effort
        }
    }
}
