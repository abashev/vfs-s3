package com.github.vfss3;

import com.github.vfss3.operations.ServerSideEncryption;
import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;

import static com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;
import static org.apache.commons.vfs2.Selectors.SELECT_ALL;
import static org.apache.commons.vfs2.Selectors.SELECT_SELF_AND_CHILDREN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public class CopyFilesTest extends AbstractS3FileSystemTest {
    private String rootName = "/copy-tests-" + (new Random()).nextInt(1000);
    private FileObject rootFolder;

    @Test
    public void createDirOk() throws FileSystemException {
        rootFolder = resolveFile(rootName);
        rootFolder.createFolder();

        assertTrue(rootFolder.exists());
        assertEquals(rootFolder.getName().getType(), FileType.FOLDER);

        // Create files and dirs
        rootFolder.resolveFile("child-file.tmp").createFile();
        rootFolder.resolveFile("child-file2.tmp").createFile();
        rootFolder.resolveFile("child-dir").createFolder();
        rootFolder.resolveFile("child-dir/descendant.tmp").createFile();
        rootFolder.resolveFile("child-dir/descendant2.tmp").createFile();
        rootFolder.resolveFile("child-dir/descendant-dir").createFolder();

        FileObject[] files;
        files = rootFolder.findFiles(Selectors.SELECT_CHILDREN);
        assertEquals(files.length, 3);
        files = rootFolder.findFiles(Selectors.SELECT_FOLDERS);
        assertEquals(files.length, 3);
        files = rootFolder.findFiles(Selectors.SELECT_FILES);
        assertEquals(files.length, 4);
        files = rootFolder.findFiles(Selectors.EXCLUDE_SELF);
        assertEquals(files.length, 6);
    }

    @Test(dependsOnMethods = "createDirOk")
    public void copyInsideBucket() throws FileSystemException {
        FileObject testsDir = rootFolder.resolveFile("child-dir");
        FileObject testsDirCopy = rootFolder.resolveFile("child-dir-copy");

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

    @Test(dependsOnMethods = "createDirOk")
    public void copyAllToEncryptedInsideBucket() throws FileSystemException {
        FileObject testsDir = rootFolder.resolveFile("child-dir");
        FileObject testsDirCopy = resolveFile(rootFolder.getName().getPath(), o -> o.setServerSideEncryption(true)).
                resolveFile("child-dir-encrypted-copy");

        assertTrue(testsDir.exists());
        assertFalse(testsDirCopy.exists());

        testsDirCopy.copyFrom(testsDir, SELECT_ALL);

        // Should have same number of files
        FileObject[] files = testsDir.findFiles(SELECT_ALL);
        FileObject[] filesCopy = testsDirCopy.findFiles(SELECT_ALL);

        assertEquals(files.length, filesCopy.length,
                Arrays.deepToString(files) + " vs. " + Arrays.deepToString(filesCopy));

        for (int i = 0; i < files.length; i++) {
            if (files[i].getType() == FileType.FILE) {
                assertNoEncryption(files[i]);
                assertAES256Encryption(filesCopy[i]);
            }
        }
    }

    @Test(dependsOnMethods = "copyAllToEncryptedInsideBucket")
    public void checkDelete() throws FileSystemException {
        rootFolder = resolveFile(rootName);

        assertTrue(rootFolder.deleteAll() > 0);

        rootFolder = resolveFile(rootName);

        assertFalse(rootFolder.exists());
    }

    public FileObject resolveFile(String path) throws FileSystemException {
        return vfs.resolveFile(baseUrl + path, options.toFileSystemOptions());
    }

    private void assertNoEncryption(FileObject file) throws FileSystemException {
        ServerSideEncryption sse = (ServerSideEncryption) file.getFileOperations().getOperation(ServerSideEncryption.class);

        sse.process();

        assertTrue(sse.noEncryption());
    }

    private void assertAES256Encryption(FileObject file) throws FileSystemException {
        ServerSideEncryption sse = (ServerSideEncryption) file.getFileOperations().getOperation(ServerSideEncryption.class);

        sse.process();

        assertTrue(sse.encryptedWith(AES_256_SERVER_SIDE_ENCRYPTION));
    }

}
