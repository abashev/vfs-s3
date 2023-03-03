package com.github.vfss3;

import com.github.vfss3.support.BaseIntegrationTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.Selectors.*;
import static org.testng.Assert.*;

/**
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public class CopyFilesTest extends BaseIntegrationTest {
    @Test
    public void createDirOk() throws FileSystemException {
        assertTrue(root.exists());
        assertEquals(root.getName().getType(), FOLDER);

        // Create files and dirs
        root.resolveFile("child-file.tmp").createFile();
        root.resolveFile("child-file2.tmp").createFile();
        root.resolveFile("child-dir").createFolder();
        root.resolveFile("child-dir/descendant.tmp").createFile();
        root.resolveFile("child-dir/descendant2.tmp").createFile();
        root.resolveFile("child-dir/descendant-dir").createFolder();

        FileObject[] files;
        files = root.findFiles(SELECT_CHILDREN);
        assertEquals(files.length, 3);
        files = root.findFiles(Selectors.SELECT_FOLDERS);
        assertEquals(files.length, 3);
        files = root.findFiles(Selectors.SELECT_FILES);
        assertEquals(files.length, 4);
        files = root.findFiles(Selectors.EXCLUDE_SELF);
        assertEquals(files.length, 6);
    }

    @Test(dependsOnMethods = "createDirOk")
    public void copyInsideBucket() throws FileSystemException {
        FileObject testsDir = root.resolveFile("child-dir");
        FileObject testsDirCopy = root.resolveFile("child-dir-copy");

        assertTrue(testsDir.exists());
        assertFalse(testsDirCopy.exists());

        testsDirCopy.copyFrom(testsDir, SELECT_SELF_AND_CHILDREN);

        assertTrue(testsDirCopy.exists());

        // Should have same number of files
        FileObject[] files = testsDir.findFiles(SELECT_SELF_AND_CHILDREN);
        FileObject[] filesCopy = testsDirCopy.findFiles(SELECT_SELF_AND_CHILDREN);

        assertEquals(files.length, filesCopy.length,
                Arrays.deepToString(files) + " vs. " + Arrays.deepToString(filesCopy));
    }

    @Test(dependsOnMethods = "copyInsideBucket")
    public void checkDelete() throws FileSystemException {
        assertTrue(root.delete(EXCLUDE_SELF) > 0);

        assertFalse(root.resolveFile("child-dir").exists());
    }
}
