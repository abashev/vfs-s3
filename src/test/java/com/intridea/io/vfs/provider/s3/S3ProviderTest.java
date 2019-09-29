package com.intridea.io.vfs.provider.s3;

import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.github.vfss3.S3FileObject;
import com.github.vfss3.operations.IMD5HashGetter;
import com.github.vfss3.operations.IPublicUrlsGetter;
import com.github.vfss3.operations.ServerSideEncryption;
import com.intridea.io.vfs.support.AbstractS3FileSystemTest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import static com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;
import static com.intridea.io.vfs.FileAssert.assertHasChildren;
import static java.nio.file.Files.readAllBytes;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.util.Optional.empty;
import static org.apache.commons.vfs2.Selectors.SELECT_ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class S3ProviderTest extends AbstractS3FileSystemTest {
    private static final String BIG_FILE = "big_file.iso";

    private String fileName, encryptedFileName, dirName;
    private FileObject file, dir;

    @BeforeClass
    public void setUp() {
        Random r = new Random();
        encryptedFileName = "vfs-encrypted-file" + r.nextInt(1000);
        fileName = "vfs-file" + r.nextInt(1000);
        dirName = "vfs-dir" + r.nextInt(1000);
    }

    @Test
    public void createFileOk() throws FileSystemException {
        file = resolveFile("/test-place/" + fileName);
        file.createFile();
        assertTrue(file.exists());

        FileObject f1 = resolveFile("/test-place/name with space");
        f1.createFile();
        assertTrue(f1.exists());

        FileObject f2 = resolveFile("/test-place/folder with space");
        f2.createFolder();
        assertTrue(f2.exists());
    }

    @Test(dependsOnMethods = {"createFileOk"})
    public void createEncryptedFileOk() throws FileSystemException {
        file = resolveFile("/test-place/" + encryptedFileName, o -> o.setServerSideEncryption(true));

        file.createFile();

        assertTrue(file.exists());
        assertAES256Encryption(file);
    }

    @Test(dependsOnMethods = {"createFileOk"}, expectedExceptions = { FileSystemException.class })
    public void checkLastModified() throws FileSystemException {
        file = resolveFile("/test-place/" + fileName);

        assertTrue(file.exists());

        assertTrue(file.getContent().getLastModifiedTime() > 0);

        file.getContent().setLastModifiedTime(111L);
    }

    @Test(expectedExceptions = { FileSystemException.class })
    public void createFileFailed() throws FileSystemException {
        FileObject tmpFile = vfs.resolveFile("s3://../new-mpoint/vfs-bad-file");
        tmpFile.createFile();
    }

    /**
     * Create folder on already existed file
     * @throws FileSystemException
     */
    @Test(expectedExceptions={FileSystemException.class}, dependsOnMethods={"createFileOk"})
    public void createFileFailed2() throws FileSystemException {
        FileObject tmpFile = resolveFile("/test-place/" + fileName);
        tmpFile.createFolder();
    }

    @Test
    public void createDirOk() throws FileSystemException {
        dir = resolveFile("/test-place/" + dirName);
        dir.createFolder();

        assertTrue(dir.exists());
        assertEquals(dir.getName().getType(), FileType.FOLDER);
    }

    @Test(expectedExceptions={FileSystemException.class})
    public void createDirFailed() throws FileSystemException {
        FileObject tmpFile = vfs.resolveFile("s3://../new-mpoint/vfs-bad-dir");
        tmpFile.createFolder();
    }

    /**
     * Create file on already existed folder
     * @throws FileSystemException
     */
    @Test(expectedExceptions = FileSystemException.class, dependsOnMethods = "createDirOk")
    public void createDirFailed2() throws FileSystemException {
        FileObject tmpFile = resolveFile("/test-place/" + dirName);
        tmpFile.createFile();
    }

    @Test(dependsOnMethods={"upload"})
    public void exists() throws IOException {
        // Existed dir
        FileObject existedDir = resolveFile("/test-place");
        assertTrue(existedDir.exists());

        // Non-existed dir
        FileObject nonExistedDir = vfs.resolveFile(existedDir, "path/to/non/existed/dir");
        Assert.assertFalse(nonExistedDir.exists());

        // Existed file
        FileObject existedFile = resolveFile("/test-place/backup.zip");
        assertTrue(existedFile.exists());

        // Non-existed file
        FileObject nonExistedFile = resolveFile("/ne/bыlo/i/net");
        Assert.assertFalse(nonExistedFile.exists());
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void upload() throws IOException {
        FileObject dest = resolveFile("/test-place/backup.zip");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = binaryFile();

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = vfs.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
        assertNoEncryption(dest);
    }

    @Test(dependsOnMethods = {"createEncryptedFileOk"})
    public void uploadEncrypted() throws IOException {
        FileObject dest = resolveFile("/test-place/backup.zip", o -> o.setServerSideEncryption(true));

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = binaryFile();

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = vfs.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
        assertAES256Encryption(dest);
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void uploadMultiple() throws Exception {
        FileObject dest = resolveFile("/test-place/backup.zip");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = binaryFile();

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = vfs.resolveFile(backupFile.getAbsolutePath());

        // copy twice
        dest.copyFrom(src, Selectors.SELECT_SELF);
        Thread.sleep(2000L);
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void uploadBigFile() throws IOException {
        FileObject dest = resolveFile("/" + BIG_FILE);

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        FileObject src = vfs.resolveFile(bigFile());

        assertTrue(src.exists(), "Big file should exists");

        assertTrue(src.getContent().getSize() > (new TransferManagerConfiguration()).getMultipartUploadThreshold());

        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists(), "Destination file should be on place");
        assertEquals(dest.getType(), FileType.FILE);
        assertEquals(src.getContent().getSize(), dest.getContent().getSize());
        assertEquals(63963136, dest.getContent().getSize());
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void outputStream() throws IOException {
        FileObject dest = resolveFile("/test-place/output.txt");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final byte[] bytes = readAllBytes(binaryFile().toPath());

        try (OutputStream os = dest.getContent().getOutputStream()) {
            os.write(bytes);
        }

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
        assertEquals(dest.getContent().getSize(), binaryFile().length());
        assertEquals(((S3FileObject) dest).getSSEAlgorithm(), empty());

        try (InputStream in = dest.getContent().getInputStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            IOUtils.copy(in, buffer);

            assertArrayEquals(bytes, buffer.toByteArray());
        }

        dest.delete();
    }

    @Test(dependsOnMethods = {"createEncryptedFileOk"})
    public void outputStreamEncrypted() throws IOException {
        FileObject dest = resolveFile("/test-place/output.txt", o -> o.setServerSideEncryption(true));

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final byte[] bytes = readAllBytes(binaryFile().toPath());

        try (OutputStream os = dest.getContent().getOutputStream()) {
            os.write(bytes);
        }

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
        assertEquals(dest.getContent().getSize(), binaryFile().length());
        assertAES256Encryption(dest);

        try (InputStream in = dest.getContent().getInputStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            IOUtils.copy(in, buffer);

            assertArrayEquals(bytes, buffer.toByteArray());
        }

        dest.delete();
    }

    @Test(dependsOnMethods={"getSize"})
    public void download() throws IOException {
        FileObject typica = resolveFile("/test-place/backup.zip");
        File localCache =  File.createTempFile("vfs.", ".s3-test");

        // Copy from S3 to localfs
        FileOutputStream out = new FileOutputStream(localCache);
        IOUtils.copy(typica.getContent().getInputStream(), out);

        // Test that file sizes equals
        assertEquals(localCache.length(), typica.getContent().getSize());

        localCache.delete();
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void listChildren() throws FileSystemException {
        FileObject baseDir = dir.resolveFile("list-children-test");
        baseDir.createFolder();

        for (int i=0; i<5; i++) {
            FileObject tmpFile = vfs.resolveFile(baseDir, i + ".tmp");
            tmpFile.createFile();
        }

        FileObject[] children = baseDir.getChildren();
        assertEquals(children.length, 5);
    }

    @Test(dependsOnMethods = {"createFileOk", "createDirOk", "uploadBigFile"})
    public void listChildrenRoot() throws FileSystemException {
        assertHasChildren(resolveFile("/"), "test-place", BIG_FILE, "acl", "read-deadlock");
        assertHasChildren(
                resolveFile("/test-place/"),
                "backup.zip", dirName, encryptedFileName, "folder with space", "name with space"
        );
        assertHasChildren(
                resolveFile("/test-place"),
                "backup.zip", dirName, encryptedFileName, "folder with space", "name with space"
        );

        final FileObject destFile = resolveFile("/test-place-2");

        destFile.copyFrom(resolveFile("/test-place"), SELECT_ALL);

        assertHasChildren(
                destFile,
                "backup.zip", dirName, encryptedFileName, "folder with space", "name with space"
        );
    }

    @Test(dependsOnMethods={"createDirOk"})
    public void findFiles() throws FileSystemException {
        FileObject baseDir = dir.resolveFile("find-tests");
        baseDir.createFolder();

        // Create files and dirs
        vfs.resolveFile(baseDir, "child-file.tmp").createFile();
        vfs.resolveFile(baseDir, "child-file2.tmp").createFile();
        vfs.resolveFile(baseDir, "child-dir").createFolder();
        vfs.resolveFile(baseDir, "child-dir/descendant.tmp").createFile();
        vfs.resolveFile(baseDir, "child-dir/descendant2.tmp").createFile();
        vfs.resolveFile(baseDir, "child-dir/descendant-dir").createFolder();

        FileObject[] files;
        files = baseDir.findFiles(Selectors.SELECT_CHILDREN);
        assertEquals(files.length, 3);
        files = baseDir.findFiles(Selectors.SELECT_FOLDERS);
        assertEquals(files.length, 3);
        files = baseDir.findFiles(Selectors.SELECT_FILES);
        assertEquals(files.length, 4);
        files = baseDir.findFiles(Selectors.EXCLUDE_SELF);
        assertEquals(files.length, 6);
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void renameAndMove() throws FileSystemException {
        FileObject sourceFile = resolveFile("/test-place/" + fileName);
        FileObject targetFile = resolveFile("/test-place/rename-target");

        assertTrue(sourceFile.exists());

        if (targetFile.exists()) {
            targetFile.delete();
        }

        assertFalse(targetFile.exists());
        assertFalse(sourceFile.canRenameTo(targetFile));

        sourceFile.moveTo(targetFile);

        assertTrue(targetFile.exists());
        assertFalse(sourceFile.exists());

        targetFile.moveTo(sourceFile);

        assertTrue(sourceFile.exists());
        assertFalse(targetFile.exists());

        try {
            sourceFile.moveTo(sourceFile);

            fail("Should block copy into itself");
        } catch (FileSystemException ignored) {
        }
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void getType() throws FileSystemException {
        FileObject imagine = dir.resolveFile("imagine-there-is-no-countries");

        assertEquals(imagine.getType(), FileType.IMAGINARY);
        assertEquals(dir.getType(), FileType.FOLDER);
        assertEquals(file.getType(), FileType.FILE);
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void getTypeAfterCopyToSubFolder() throws FileSystemException {
        FileObject dest = dir.resolveFile("type-tests/sub1/sub2/backup.zip");

        // Copy data
        final File backupFile = binaryFile();

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = vfs.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));

        FileObject sub1 = dir.resolveFile("type-tests/sub1");
        assertTrue(sub1.exists());
        assertTrue(sub1.getType().equals(FileType.FOLDER));

        FileObject sub2 = dir.resolveFile("type-tests/sub1/sub2");
        assertTrue(sub2.exists());
        assertTrue(sub2.getType().equals(FileType.FOLDER));
    }

    @Test(dependsOnMethods={"upload"})
    public void getContentType() throws FileSystemException {
        FileObject backup = resolveFile("/test-place/backup.zip");
        assertEquals(backup.getContent().getContentInfo().getContentType(), "application/zip");
    }

    @Test(dependsOnMethods={"upload"})
    public void getSize() throws FileSystemException {
        FileObject backup = resolveFile("/test-place/backup.zip");

        assertEquals(backup.getContent().getSize(), 996166);
    }

    @Test(dependsOnMethods = {"createFileOk", "upload"})
    public void getUrls() throws FileSystemException {
        FileObject backup = resolveFile("/test-place/backup.zip");

        assertTrue(backup.getFileOperations().hasOperation(IPublicUrlsGetter.class));
        assertNotNull(backup.getFileOperations().getOperation(IPublicUrlsGetter.class));

        IPublicUrlsGetter urlsGetter = (IPublicUrlsGetter) backup.getFileOperations().getOperation(IPublicUrlsGetter.class);

        assertThat(urlsGetter.getHttpUrl()).contains(
                "https",
                "/test-place/backup.zip"
        );

        final String signedUrl = urlsGetter.getSignedUrl(60);

        assertThat(signedUrl).contains(
                "https",
                "/test-place/backup.zip",
                "Signature=",
                "Expires=",
                "X-Amz-Credential=",
                "X-Amz-Signature="
        );
    }

    @Test(dependsOnMethods = {"createFileOk", "upload"})
    public void getMD5Hash() throws NoSuchAlgorithmException, IOException {
        FileObject backup = resolveFile("/test-place/backup.zip");

        assertTrue(backup.getFileOperations().hasOperation(IMD5HashGetter.class));

        IMD5HashGetter md5Getter = (IMD5HashGetter) backup.getFileOperations().getOperation(IMD5HashGetter.class);

        String md5Remote = md5Getter.getMD5Hash();

        Assert.assertNotNull(md5Remote);

        final File backupFile = binaryFile();

        assertTrue(backupFile.exists(), "Backup file should exists");

        String md5Local = toHex(computeMD5Hash(new FileInputStream(backupFile)));

        assertEquals(md5Remote, md5Local, "Local and remote md5 should be equal");
    }

    @Test(dependsOnMethods = "upload")
    public void getLastModified() throws FileSystemException {
        FileObject backup = resolveFile("/test-place/backup.zip");

        assertTrue(ofEpochMilli(backup.getContent().getLastModifiedTime()).atZone(UTC).getYear() > 2010);
    }

    @Test(dependsOnMethods={"findFiles"})
	public void copyInsideBucket() throws FileSystemException {
        FileObject testsDir = dir.resolveFile("find-tests");
        FileObject testsDirCopy = dir.resolveFile("find-tests-copy");
        testsDirCopy.copyFrom(testsDir, Selectors.SELECT_SELF_AND_CHILDREN);

        // Should have same number of files
        FileObject[] files = testsDir.findFiles(Selectors.SELECT_SELF_AND_CHILDREN);
        FileObject[] filesCopy = testsDirCopy.findFiles(Selectors.SELECT_SELF_AND_CHILDREN);
        assertEquals(files.length, filesCopy.length,
            Arrays.deepToString(files) + " vs. " + Arrays.deepToString(filesCopy));
	}

    @Test(dependsOnMethods={"createDirOk", "findFiles"})
    public void copyAllToEncryptedInsideBucket() throws FileSystemException {
        FileObject testsDir = dir.resolveFile("find-tests");

        FileObject testsDirCopy =
                resolveFile("/test-place/" + dirName, o -> o.setServerSideEncryption(true)).
                resolveFile("find-tests-encrypted-copy");

        assertTrue(testsDir.exists());
        assertFalse(testsDirCopy.exists());

        testsDirCopy.copyFrom(testsDir, SELECT_ALL);

        // Should have same number of files
        FileObject[] files = testsDir.findFiles(SELECT_ALL);
        FileObject[] filesCopy = testsDirCopy.findFiles(SELECT_ALL);

        assertEquals(files.length, filesCopy.length);

        for (int i = 0; i < files.length; i++) {
            if (files[i].getType() == FileType.FILE) {
                assertNoEncryption(files[i]);
                assertAES256Encryption(filesCopy[i]);
            }
        }
    }

    @Test(dependsOnMethods={"findFiles", "download"})
    public void delete() throws FileSystemException {
        FileObject testsDir = dir.resolveFile("find-tests");
        testsDir.delete(Selectors.EXCLUDE_SELF);

        // Only tests dir must remains
        FileObject[] files = testsDir.findFiles(SELECT_ALL);
        assertEquals(files.length, 1);
    }

    @AfterClass
    public void tearDown() throws FileSystemException {
        try {
            resolveFile("/" + BIG_FILE).delete();
            resolveFile("/test-place").delete(SELECT_ALL);
            resolveFile("/test-place-2").delete(SELECT_ALL);

            ((AbstractFileSystem) resolveFile("/").getFileSystem()).close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Converts byte data to a Hex-encoded string.
     *
     * @param data data to hex encode.
     * @return hex-encoded string.
     */
    private String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte aData : data) {
            String hex = Integer.toHexString(aData);
            if (hex.length() == 1) {
                // Append leading zero.
                sb.append("0");
            } else if (hex.length() == 8) {
                // Remove ff prefix from negative numbers.
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase(Locale.getDefault());
    }

    private byte[] computeMD5Hash(InputStream is) throws NoSuchAlgorithmException, IOException {
        BufferedInputStream bis = new BufferedInputStream(is);
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[16384];
            int bytesRead;
            while ((bytesRead = bis.read(buffer, 0, buffer.length)) != -1) {
                messageDigest.update(buffer, 0, bytesRead);
            }
            return messageDigest.digest();
        } finally {
            try {
                bis.close();
            } catch (Exception e) {
                System.err.println("Unable to close input stream of hash candidate: " + e);
            }
        }
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
