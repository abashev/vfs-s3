package com.github.vfss3.jdk.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Suite G: Concurrent Access — see {@code docs/test-cases/g-concurrent-access.md}. Exercises the
 * provider from many threads and watches for deadlocks via
 * {@link java.lang.management.ThreadMXBean}.
 *
 * <p>Not yet adapted to {@link com.github.vfss3.jdk.JdkIntegrationContext} — depends on
 * directory operations (Task 4); re-enabled in Task 9 of {@code tasks/plan.md}.
 */
@Disabled("Depends on createDirectory/newDirectoryStream — see Task 9 of tasks/plan.md")
class ConcurrentAccessTest {

    private static final String BUCKET = "test-bucket";
    private static final String FOLDERS = "/concurrent/folders/";
    private static final String READ_TEST = "/concurrent/read-test/";

    private FileSystem fs;

    @BeforeEach
    void setUp() throws IOException {
        fs = FileSystems.newFileSystem(URI.create("s3://" + BUCKET), Map.of());
        Files.createDirectories(fs.getPath(FOLDERS));
        Files.createDirectories(fs.getPath(READ_TEST));
        Files.write(fs.getPath(READ_TEST + "file1"), new byte[0]);
        Files.write(fs.getPath(READ_TEST + "file2"), new byte[0]);
    }

    @AfterEach
    void tearDown() throws IOException {
        var prefix = fs.getPath("/concurrent/");
        if (Files.exists(prefix)) {
            try (Stream<Path> walk = Files.walk(prefix)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
            }
        }
        fs.close();
    }

    /** Step 1: many threads create / verify / delete their own folder. */
    @Test
    @DisplayName("Step 1: concurrent create and delete")
    void step1_concurrentCreateDelete() throws Exception {
        runConcurrently(8, 200, i -> {
            var folder = fs.getPath(FOLDERS + "folder-" + Thread.currentThread().getId() + "-" + i);
            Files.createDirectories(folder);
            assertTrue(Files.exists(folder));
            Files.delete(folder);
            assertTrue(!Files.exists(folder));
        });
    }

    /** Step 2: many threads resolve a folder and walk parent / children. */
    @Test
    @DisplayName("Step 2: concurrent read")
    void step2_concurrentRead() throws Exception {
        runConcurrently(8, 200, i -> {
            var dir = fs.getPath(READ_TEST);
            assertNotNull(dir.getParent());
            assertTrue(Files.exists(dir));
            try (DirectoryStream<Path> children = Files.newDirectoryStream(dir)) {
                assertNotNull(children.iterator());
            }
        });
    }

    /** Step 3: hammer getParent / listChildren and confirm no deadlock and correct counts. */
    @Test
    @DisplayName("Step 3: deadlock detection")
    void step3_deadlockDetection() throws Exception {
        var parent = fs.getPath(FOLDERS);
        final int childCount = 10;
        for (int i = 0; i < childCount; i++) {
            Files.write(parent.resolve("deadlock-" + i), new byte[0]);
        }

        var wrongResults = new AtomicInteger(0);
        var stop = new AtomicBoolean(false);
        var threads = new java.util.ArrayList<Thread>();

        Thread getParent = new Thread(
                () -> {
                    while (!stop.get()) {
                        for (int i = 0; i < childCount; i++) {
                            if (parent.resolve("deadlock-" + i).getParent() == null) {
                                wrongResults.incrementAndGet();
                            }
                        }
                    }
                },
                "getParent");
        getParent.setDaemon(true);
        threads.add(getParent);

        for (int t = 0; t < 3; t++) {
            Thread getChildren = new Thread(
                    () -> {
                        while (!stop.get()) {
                            try (DirectoryStream<Path> children = Files.newDirectoryStream(parent)) {
                                int count = 0;
                                for (Path ignored : children) {
                                    count++;
                                }
                                if (count != childCount) {
                                    wrongResults.incrementAndGet();
                                }
                            } catch (IOException e) {
                                wrongResults.incrementAndGet();
                            }
                        }
                    },
                    "getChildren" + t);
            getChildren.setDaemon(true);
            threads.add(getChildren);
        }

        threads.forEach(Thread::start);
        try {
            var threadMXBean = ManagementFactory.getThreadMXBean();
            for (int i = 0; i < 3; i++) {
                Thread.sleep(500);
                long[] deadlocked = threadMXBean.findDeadlockedThreads();
                if (deadlocked != null) {
                    throw new AssertionError("threads are deadlocked");
                }
            }
        } finally {
            stop.set(true);
            for (Thread thread : threads) {
                thread.join(1000);
            }
        }

        assertEquals(0, wrongResults.get(), "Number of wrong calculations should be zero");
    }

    private void runConcurrently(int threads, int iterations, IndexedTask task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        var failures = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
        try {
            for (int i = 0; i < iterations; i++) {
                final int index = i;
                var unused = pool.submit(() -> {
                    try {
                        task.run(index);
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                });
            }
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "Tasks should finish within 60s");
        }
        Throwable first = failures.peek();
        if (first != null) {
            throw new AssertionError("Concurrent task failed", first);
        }
    }

    @FunctionalInterface
    private interface IndexedTask {
        void run(int index) throws Exception;
    }
}
