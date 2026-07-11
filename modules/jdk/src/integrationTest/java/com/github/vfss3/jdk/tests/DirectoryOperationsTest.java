package com.github.vfss3.jdk.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.reverseOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.jdk.JdkIntegrationContext;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Suite B: Directory Operations — see {@code docs/test-cases/b-directory-operations.md}. Folder
 * listing and the find selectors are expressed with {@link DirectoryStream} and
 * {@link Files#walk}. Runs against whatever real S3-compatible backend the enclosing
 * {@code @Suite} wired up via {@link JdkIntegrationContext}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DirectoryOperationsTest {

    private static final String PREFIX = "/dir-ops/";

    private FileSystem fs;

    @BeforeEach
    void setUp() {
        fs = JdkIntegrationContext.fileSystem();
    }

    @AfterEach
    void tearDown() throws IOException {
        JdkIntegrationContext.deleteRecursively(fs.getPath(PREFIX));
    }

    /** Step 1: create a folder and assert it exists as a directory. */
    @Test
    @Order(1)
    @DisplayName("Step 1: create folder and verify it is a directory")
    void step1_createFolder() throws IOException {
        var folder = fs.getPath(PREFIX + "my-folder");
        Files.createDirectories(folder);

        assertTrue(Files.exists(folder), "Folder should exist after creation");
        assertTrue(Files.isDirectory(folder), "Created path should be a directory");
    }

    /** Step 2: createFile on an existing folder path throws FileAlreadyExistsException. */
    @Test
    @Order(2)
    @DisplayName("Step 2: createFile on existing folder throws FileAlreadyExistsException")
    void step2_createFileOnExistingFolderThrows() throws IOException {
        var folder = fs.getPath(PREFIX + "my-folder");
        Files.createDirectories(folder);

        assertThrows(FileAlreadyExistsException.class, () -> Files.createFile(folder));
    }

    /** Step 3: create a folder whose name contains a space. */
    @Test
    @Order(3)
    @DisplayName("Step 3: create folder with a space in its name")
    void step3_createFolderWithSpace() throws IOException {
        var folder = fs.getPath(PREFIX + "folder with space");
        Files.createDirectories(folder);

        assertTrue(Files.exists(folder), "Folder with space in name should exist");
    }

    /** Step 4: create five files inside a folder and list its children. */
    @Test
    @Order(4)
    @DisplayName("Step 4: list children returns all five files")
    void step4_listChildren() throws IOException {
        var folder = fs.getPath(PREFIX + "my-folder");
        Files.createDirectories(folder);

        for (int i = 0; i < 5; i++) {
            Files.write(folder.resolve(i + ".tmp"), new byte[0]);
        }

        int count = 0;
        try (DirectoryStream<Path> children = Files.newDirectoryStream(folder)) {
            for (Path ignored : children) {
                count++;
            }
        }
        assertEquals(5, count, "Folder should have five children");
    }

    /** Step 5: build a nested tree and verify the find selectors via Files.walk. */
    @Test
    @Order(5)
    @DisplayName("Step 5: find selectors over a nested tree")
    void step5_findWithSelectors() throws IOException {
        var base = fs.getPath(PREFIX + "find-tests");
        Files.createDirectories(base);
        Files.write(base.resolve("child-file.tmp"), bytes("a"));
        Files.write(base.resolve("child-file2.tmp"), bytes("b"));
        Files.createDirectories(base.resolve("child-dir"));
        Files.write(base.resolve("child-dir/descendant.tmp"), bytes("c"));
        Files.write(base.resolve("child-dir/descendant2.tmp"), bytes("d"));
        Files.createDirectories(base.resolve("child-dir/descendant-dir"));

        // Immediate children.
        long children;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(base)) {
            children = count(stream);
        }
        assertEquals(3, children, "SELECT_CHILDREN");

        // All folders (base + child-dir + descendant-dir).
        try (Stream<Path> walk = Files.walk(base)) {
            assertEquals(3, walk.filter(Files::isDirectory).count(), "SELECT_FOLDERS");
        }
        // All files.
        try (Stream<Path> walk = Files.walk(base)) {
            assertEquals(4, walk.filter(Files::isRegularFile).count(), "SELECT_FILES");
        }
        // Everything except the base itself.
        try (Stream<Path> walk = Files.walk(base)) {
            assertEquals(6, walk.filter(p -> !p.equals(base)).count(), "EXCLUDE_SELF");
        }
    }

    /** Step 6: delete the children of the tree, leaving only the folder itself. */
    @Test
    @Order(6)
    @DisplayName("Step 6: delete children leaves only the folder")
    void step6_deleteChildren() throws IOException {
        var base = fs.getPath(PREFIX + "find-tests");
        Files.createDirectories(base);
        Files.write(base.resolve("child-file.tmp"), bytes("a"));
        Files.createDirectories(base.resolve("child-dir"));
        Files.write(base.resolve("child-dir/descendant.tmp"), bytes("c"));

        try (Stream<Path> walk = Files.walk(base)) {
            walk.filter(p -> !p.equals(base)).sorted(reverseOrder()).forEach(JdkIntegrationContext::deleteQuietly);
        }

        try (Stream<Path> walk = Files.walk(base)) {
            assertEquals(1, walk.count(), "Only the folder itself should remain");
        }
    }

    private static long count(DirectoryStream<Path> stream) {
        long count = 0;
        for (Path ignored : stream) {
            count++;
        }
        return count;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
