package com.intridea.io.vfs.provider.s3;

import com.intridea.io.vfs.TestEnvironment;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class ConcurrentAccessTest {
    private FileSystemManager fsManager;
    private FileObject parentFile;

    @BeforeClass
    public void setUp() throws IOException {
        Properties config = TestEnvironment.getInstance().getConfig();

        fsManager = VFS.getManager();
        String bucketName = config.getProperty("s3.testBucket", "vfs-s3-tests");

        parentFile = fsManager.resolveFile("s3://" + bucketName + "/concurrent/");

        parentFile.createFolder();
    }

    @Test(invocationCount = 200, threadPoolSize = 10)
    public void createFileOk() throws FileSystemException {
        String fileName = "folder-" + (new Random()).nextInt(1000) + "/";

        FileObject file = fsManager.resolveFile(parentFile, fileName);

        file.createFolder();
        assertTrue(file.exists());

        file.refresh();

        assertTrue(file.exists());

        file.delete();

        file.refresh();

        assertFalse(file.exists());
    }

    @AfterClass
    public void tearDown() throws FileSystemException {
        parentFile.deleteAll();
    }
}
