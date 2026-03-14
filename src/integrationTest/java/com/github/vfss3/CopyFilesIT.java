package com.github.vfss3;

import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.Selectors.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.vfss3.support.BaseIntegrationTest;
import java.util.Arrays;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CopyFilesIT extends BaseIntegrationTest {
    @Test
    @Order(1)
    public void createDirOk() throws FileSystemException {
        assertTrue(root.exists());
        assertEquals(FOLDER, root.getName().getType());

        // Create files and dirs
        root.resolveFile("child-file.tmp").createFile();
        root.resolveFile("child-file2.tmp").createFile();
        root.resolveFile("child-dir").createFolder();
        root.resolveFile("child-dir/descendant.tmp").createFile();
        root.resolveFile("child-dir/descendant2.tmp").createFile();
        root.resolveFile("child-dir/descendant-dir").createFolder();

        FileObject[] files;
        files = root.findFiles(SELECT_CHILDREN);
        assertEquals(3, files.length);
        files = root.findFiles(Selectors.SELECT_FOLDERS);
        assertEquals(3, files.length);
        files = root.findFiles(Selectors.SELECT_FILES);
        assertEquals(4, files.length);
        files = root.findFiles(Selectors.EXCLUDE_SELF);
        assertEquals(6, files.length);
    }

    @Test
    @Order(2)
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

        assertEquals(
                files.length, filesCopy.length, Arrays.deepToString(files) + " vs. " + Arrays.deepToString(filesCopy));
    }

    @Test
    @Order(3)
    public void checkDelete() throws FileSystemException {
        assertTrue(root.delete(EXCLUDE_SELF) > 0);

        assertFalse(root.resolveFile("child-dir").exists());
    }
}
