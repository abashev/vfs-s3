package com.github.vfss3.commonsvfs.tests;

import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class LegacyConcurrentAccessTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private FileObject root;

    @BeforeAll
    void setUp() throws IOException {
        root = VFS.getManager().resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        if (root.exists()) {
            root.delete(Selectors.EXCLUDE_SELF);
        }
        root.resolveFile("/concurrent/").createFolder();
        root.resolveFile("/read-deadlock/").createFolder();
        root.resolveFile("/read-deadlock/file1").createFile();
        root.resolveFile("/read-deadlock/file2").createFile();
    }

    @Execution(ExecutionMode.CONCURRENT)
    @RepeatedTest(200)
    void createFileOk() throws FileSystemException {
        String fileName = "folder-" + Thread.currentThread().getId() + "-" + new Random().nextInt(1000) + "/";

        FileObject parent = root.resolveFile("/concurrent/");
        FileObject file = parent.resolveFile(fileName);

        file.createFolder();
        assertTrue(file.exists());

        file.refresh();

        assertTrue(file.exists());

        file.delete();

        file.refresh();

        assertFalse(file.exists());
    }

    @Execution(ExecutionMode.CONCURRENT)
    @RepeatedTest(200)
    void checkReadDeadlock() throws FileSystemException {
        FileObject file = root.resolveFile("/read-deadlock");

        assertNotNull(file.getParent());

        file.refresh();

        assertNotNull(file.getChildren());

        assertTrue(file.exists());
    }

    @Test
    void getChildrenGetParentDeadlock() throws FileSystemException, InterruptedException {
        final FileObject parent = root.resolveFile("/concurrent/");
        parent.delete(Selectors.EXCLUDE_SELF);

        final int childCount =
                Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockTestChildCount", "10"));
        final int duration =
                Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockTestDuration", "5"));
        final long interval =
                Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockCheckInterval", "1000"));

        for (int i = 0; i < childCount; i++) {
            String fileName = "deadlock-" + i;
            FileObject file = parent.resolveFile(fileName);
            file.createFile();

            assertTrue(file.exists());
        }

        final AtomicInteger wrongResults = new AtomicInteger(0);
        final AtomicBoolean stopFlag = new AtomicBoolean(false);

        List<Thread> threads = new ArrayList<>();

        Thread thread = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        while (!stopFlag.get()) {
                            for (int i = 0; i < childCount; i++) {
                                String fileName = "deadlock-" + i;

                                try {
                                    FileObject file = parent.resolveFile(fileName);
                                    FileObject p = file.getParent();

                                    if (p == null) {
                                        wrongResults.incrementAndGet();

                                        log.error("Parent is null");
                                    }
                                } catch (FileSystemException e) {
                                    log.error("Not able to get parent for {}", fileName, e);
                                }
                            }
                        }
                    }
                },
                "getParent");
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();

        for (int i = 0; i < 3; i++) {
            thread = new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            while (!stopFlag.get()) {
                                try {
                                    final FileObject parent = root.resolveFile("/concurrent/");
                                    int count = parent.getChildren().length;

                                    if (count != childCount) {
                                        wrongResults.incrementAndGet();

                                        log.error("Wrong number of children - {}", count);
                                    }

                                    parent.refresh();
                                } catch (FileSystemException e) {
                                    log.error("Not able to get children for /concurrent/", e);
                                }
                            }
                        }
                    },
                    "getChildren" + i);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }

        try {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

            for (int i = 0; i < duration; i++) {
                Thread.sleep(interval);
                long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
                if (deadlockedThreads != null) {
                    System.err.printf("Deadlock detected\n\n");
                    for (ThreadInfo threadInfo : threadMXBean.getThreadInfo(deadlockedThreads, true, true)) {
                        System.err.printf(
                                "'%s\n   java.lang.Thread.State: %s\n",
                                threadInfo.getThreadName(), threadInfo.getThreadState());
                        final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
                        for (StackTraceElement stackTraceElement : stackTraceElements) {
                            System.err.printf("        at %s\n", stackTraceElement);
                        }
                        System.err.printf("\n\n");
                    }
                    for (Thread t : threads) {
                        t.stop();
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
                try {
                    t.join(1000);
                } catch (InterruptedException ignored) {
                }
            }

            root.resolveFile("/concurrent/").delete(Selectors.SELECT_CHILDREN);
        }

        assertEquals(0, wrongResults.get(), "Number of wrong calculations should be zero");
    }
}
