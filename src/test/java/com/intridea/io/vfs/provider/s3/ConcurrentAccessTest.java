package com.intridea.io.vfs.provider.s3;

import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class ConcurrentAccessTest extends AbstractS3FileSystemTest {
    @BeforeClass
    public void setUp() throws IOException {
        env.resolveFile("/concurrent/").createFolder();
    }

    @Test(invocationCount = 200, threadPoolSize = 10)
    public void createFileOk() throws FileSystemException {
        String fileName = "folder-" + (new Random()).nextInt(1000) + "/";

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

    @Test()
    public void testGetChildrenGetParentDeadlock() throws FileSystemException, InterruptedException {
        final FileObject parent = env.resolveFile("/concurrent/");

        final int childCount = 5;

        // create a bunch of files
        for (int i = 0; i < childCount; i++) {
            String fileName = "deadlock-" + i;
            FileObject file = vfs.resolveFile(parent, fileName);
            file.createFile();
            assertTrue(file.exists());
        }

        // create one thread to continuously do getParent and another one to list files

        final CountDownLatch stopLatch = new CountDownLatch(1);

        Thread getParentThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(stopLatch.getCount() != 0) {
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
        getParentThread.setDaemon(true);

        Thread getChildrenThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(stopLatch.getCount() != 0) {
                    try {
                        parent.getChildren();
                    } catch (FileSystemException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "getChildren");
        getChildrenThread.setDaemon(true);

        getParentThread.start();
        getChildrenThread.start();

        try {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

            for (int i = 0; i < 5; i++) {
                Thread.sleep(1000);
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
                    // at least make an attempt at killing the deadlocked threads
                    getChildrenThread.stop();
                    getParentThread.stop();
                    // clear the cache because there are locked file objects in there and may block when we try to delete them
                    parent.getFileSystem().getFileSystemManager().getFilesCache().clear(parent.getFileSystem());
                    throw new AssertionError("threads are deadlocked");
                }
            }
        } finally {
            stopLatch.countDown();
            getChildrenThread.join(1000);
            getParentThread.join(1000);
            env.resolveFile("/concurrent/").delete(Selectors.SELECT_CHILDREN);
        }
    }

    @AfterClass
    public void tearDown() throws FileSystemException {
        env.resolveFile("/concurrent/").deleteAll();
    }
}
