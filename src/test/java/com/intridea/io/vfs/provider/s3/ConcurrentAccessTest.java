package com.intridea.io.vfs.provider.s3;

import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class ConcurrentAccessTest extends AbstractS3FileSystemTest {
    static final Random random = new Random();

    @BeforeClass
    public void setUp() throws IOException {
        env.resolveFile("/concurrent/").createFolder();
    }

    @Test(invocationCount = 200, threadPoolSize = 10)
    public void createFileOk() throws FileSystemException {
        // was running into too many random collisions with random numbers in the range of 0-999
        // so added thread id into the mix
        String fileName = "folder-" + Thread.currentThread().getId() + "-" + (new Random()).nextInt(1000) + "/";
        ;

        FileObject parent = env.resolveFile("/concurrent/");
        FileObject file = vfs.resolveFile(parent, fileName);

        file.createFolder();
        assertTrue(file.exists());

        file.refresh();

        assertTrue(file.exists());

        file.delete();

        file.refresh();

        assertFalse(file.exists());
    }

    @SuppressWarnings("deprecation")
    @Test()
    public void testGetChildrenGetParentDeadlock() throws FileSystemException, InterruptedException {
        final FileObject parent = env.resolveFile("/concurrent/");
        parent.delete(Selectors.EXCLUDE_SELF);

        final int childCount = Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockTestChildCount", "10"));
        final int duration = Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockTestDuration", "5"));
        final long interval = Integer.parseUnsignedInt(System.getProperty("ConcurrentAccessTest.deadlockCheckInterval", "1000"));

        // create a bunch of files
        for (int i = 0; i < childCount; i++) {
            String fileName = "deadlock-" + i;
            FileObject file = vfs.resolveFile(parent, fileName);
            file.createFile();
            assertTrue(file.exists());
        }

        // create threads to continuously do getParent and another one to list files

        final AtomicBoolean stopFlag = new AtomicBoolean(false);

        List<Thread> threads = new ArrayList<>();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!stopFlag.get()) {
                    for (int i = 0; i < childCount; i++) {
                        String fileName = "deadlock-" + i;
                        try {
                            FileObject file = vfs.resolveFile(parent, fileName);
                            file.getParent();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }, "getParent");
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();

        for (int i = 0; i < 3; i++) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!stopFlag.get()) {
                        try {
                            final FileObject parent = env.resolveFile("/concurrent/");
                            int count = parent.getChildren().length;
                            assertEquals(count, childCount, "parent.getChildren().length");
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, "getChildren" + i);
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
                        System.err.printf("'%s\n   java.lang.Thread.State: %s\n",
                                threadInfo.getThreadName(),
                                threadInfo.getThreadState().toString());
                        final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
                        for (StackTraceElement stackTraceElement : stackTraceElements) {
                            System.err.printf("        at %s\n", stackTraceElement.toString());
                        }
                        System.err.printf("\n\n");
                    }
                    for (Thread t : threads) {
                        // at least make an attempt at killing the deadlocked threads
                        t.stop();
                    }
                    // clear the cache because there are locked file objects in there and may block when we try to delete them
                    parent.getFileSystem().getFileSystemManager().getFilesCache().clear(parent.getFileSystem());
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
            env.resolveFile("/concurrent/").delete(Selectors.SELECT_CHILDREN);
        }
    }

    @AfterClass
    public void tearDown() throws FileSystemException {
        env.resolveFile("/concurrent/").deleteAll();
    }
}
