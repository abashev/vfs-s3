package com.intridea.io.vfs.provider.s3;

import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Random;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

    @AfterClass
    public void tearDown() throws FileSystemException {
        env.resolveFile("/concurrent/").deleteAll();
    }
}
