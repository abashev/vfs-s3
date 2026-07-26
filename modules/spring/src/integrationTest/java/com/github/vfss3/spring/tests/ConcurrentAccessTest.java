package com.github.vfss3.spring.tests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.spring.S3ResourcePatternResolver;
import com.github.vfss3.spring.SpringIntegrationContext;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.WritableResource;

/**
 * Suite G: Concurrent Access through the Spring Resource API — see
 * {@code docs/test-cases/g-concurrent-access.md}. Many threads write and read resources
 * concurrently through one shared loader (and its shared, cached {@code S3Client}); any failure
 * on a worker thread fails the test.
 */
final class ConcurrentAccessTest {

    private static final String PREFIX = "concurrent/";

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

    /** Step 1 analog: concurrent write + verify of independent resources. */
    @DisplayName("Step 1: concurrent writes to independent resources")
    @Test
    void step1_concurrentWrites() throws Exception {
        runConcurrently(8, 200, i -> {
            var resource = resource("write/file-" + i + ".bin");
            byte[] body = ("payload-" + i).getBytes(UTF_8);
            try (OutputStream out = resource.getOutputStream()) {
                out.write(body);
            }
            assertTrue(resource.exists());
        });
    }

    /** Step 2 analog: concurrent read of a shared resource. */
    @DisplayName("Step 2: concurrent reads of a shared resource")
    @Test
    void step2_concurrentReads() throws Exception {
        var shared = resource("read/shared.bin");
        byte[] body = "shared-content".getBytes(UTF_8);
        try (OutputStream out = shared.getOutputStream()) {
            out.write(body);
        }

        runConcurrently(8, 200, i -> {
            var resource = resource("read/shared.bin");
            assertTrue(resource.exists());
            try (InputStream in = resource.getInputStream()) {
                assertArrayEquals(body, in.readAllBytes());
            }
        });
    }

    private void runConcurrently(int threads, int iterations, IndexedTask task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        var failures = new ConcurrentLinkedQueue<Throwable>();
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
            assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS), "Tasks should finish within 120s");
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
