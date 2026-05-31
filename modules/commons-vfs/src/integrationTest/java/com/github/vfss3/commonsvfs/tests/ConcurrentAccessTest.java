package com.github.vfss3.commonsvfs.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Suite G: Concurrent Access — see {@code docs/test-cases/g-concurrent-access.md}.
 *
 * <p>Works in the isolated {@code /concurrent/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConcurrentAccessTest {
    private static final String FOLDERS = "/concurrent/folders/";
    private static final String READ_TEST = "/concurrent/read-test/";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private FileObject root;

    @BeforeAll
    void setUp() throws IOException {
        root = VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        FileObject prefix = root.resolveFile("/concurrent/");
        if (prefix.exists()) {
            prefix.deleteAll();
        }
        root.resolveFile(FOLDERS).createFolder();
        root.resolveFile(READ_TEST).createFolder();
        root.resolveFile(READ_TEST + "file1").createFile();
        root.resolveFile(READ_TEST + "file2").createFile();
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        FileObject prefix = root.resolveFile("/concurrent/");
        if (prefix.exists()) {
            prefix.deleteAll();
        }
    }

    /** Step 1: repeatedly create, refresh, and delete a per-iteration folder. */
    @RepeatedTest(200)
    @Execution(ExecutionMode.CONCURRENT)
    void testConcurrentCreateDelete() throws FileSystemException {
        String name = "folder-" + Thread.currentThread().getId() + "-" + new Random().nextInt(1000) + "/";

        FileObject folder = root.resolveFile(FOLDERS).resolveFile(name);

        folder.createFolder();
        assertTrue(folder.exists());

        folder.refresh();
        assertTrue(folder.exists());

        folder.delete();

        folder.refresh();
        assertFalse(folder.exists());
    }

    /** Step 2: repeatedly resolve a folder and walk parent/children. */
    @RepeatedTest(200)
    @Execution(ExecutionMode.CONCURRENT)
    void testConcurrentRead() throws FileSystemException {
        FileObject file = root.resolveFile(READ_TEST);

        assertNotNull(file.getParent());

        file.refresh();

        assertNotNull(file.getChildren());
        assertTrue(file.exists());
    }

    /** Step 3: hammer getParent()/getChildren() from many threads and watch for deadlocks. */
    @Test
    void testGetChildrenGetParentDeadlock() throws FileSystemException, InterruptedException {
        final FileObject parent = root.resolveFile(FOLDERS);
        parent.delete(Selectors.EXCLUDE_SELF);

        final int childCount =
                Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockTestChildCount", "10"));
        final int duration =
                Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockTestDuration", "5"));
        final long interval =
                Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockCheckInterval", "1000"));

        for (int i = 0; i < childCount; i++) {
            FileObject file = parent.resolveFile("deadlock-" + i);
            file.createFile();
            assertTrue(file.exists());
        }

        final AtomicInteger wrongResults = new AtomicInteger(0);
        final AtomicBoolean stopFlag = new AtomicBoolean(false);

        List<Thread> threads = new ArrayList<>();

        Thread getParent = new Thread(
                () -> {
                    while (!stopFlag.get()) {
                        for (int i = 0; i < childCount; i++) {
                            String name = "deadlock-" + i;
                            try {
                                FileObject p = parent.resolveFile(name).getParent();
                                if (p == null) {
                                    wrongResults.incrementAndGet();
                                    log.error("Parent is null");
                                }
                            } catch (FileSystemException e) {
                                log.error("Not able to get parent for {}", name, e);
                            }
                        }
                    }
                },
                "getParent");
        getParent.setDaemon(true);
        threads.add(getParent);
        getParent.start();

        for (int i = 0; i < 3; i++) {
            Thread getChildren = new Thread(
                    () -> {
                        while (!stopFlag.get()) {
                            try {
                                FileObject p = root.resolveFile(FOLDERS);
                                int count = p.getChildren().length;
                                if (count != childCount) {
                                    wrongResults.incrementAndGet();
                                    log.error("Wrong number of children - {}", count);
                                }
                                p.refresh();
                            } catch (FileSystemException e) {
                                log.error("Not able to get children for {}", FOLDERS, e);
                            }
                        }
                    },
                    "getChildren" + i);
            getChildren.setDaemon(true);
            threads.add(getChildren);
            getChildren.start();
        }

        try {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

            for (int i = 0; i < duration; i++) {
                Thread.sleep(interval);
                long[] deadlocked = threadMXBean.findDeadlockedThreads();
                if (deadlocked != null) {
                    System.err.print("Deadlock detected\n\n");
                    for (ThreadInfo info : threadMXBean.getThreadInfo(deadlocked, true, true)) {
                        System.err.printf(
                                "'%s\n   java.lang.Thread.State: %s\n", info.getThreadName(), info.getThreadState());
                        for (StackTraceElement element : info.getStackTrace()) {
                            System.err.printf("        at %s\n", element);
                        }
                        System.err.print("\n\n");
                    }
                    parent.getFileSystem()
                            .getFileSystemManager()
                            .getFilesCache()
                            .clear(parent.getFileSystem());
                    throw new AssertionError("threads are deadlocked");
                }
            }
        } finally {
            stopFlag.set(true);
            for (Thread t : threads) {
                t.join(1000);
            }
            parent.delete(Selectors.SELECT_CHILDREN);
        }

        assertEquals(0, wrongResults.get(), "Number of wrong calculations should be zero");
    }
}
