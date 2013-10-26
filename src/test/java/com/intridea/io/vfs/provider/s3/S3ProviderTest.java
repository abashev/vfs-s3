package com.intridea.io.vfs.provider.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.intridea.io.vfs.TestEnvironment;
import com.intridea.io.vfs.operations.IMD5HashGetter;
import com.intridea.io.vfs.operations.IPublicUrlsGetter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.io.FileNotFoundException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


@Test(groups={"storage"})
public class S3ProviderTest {

    private static final String BACKUP_ZIP = "src/test/resources/backup.zip";

    private FileSystemManager fsManager;
    private String fileName, encryptedFileName, dirName, bucketName, bigFile;
    private FileObject file, dir;

    private FileSystemOptions opts;

    @BeforeClass
    public void setUp() throws FileNotFoundException, IOException {
        Properties config = TestEnvironment.getInstance().getConfig();

        fsManager = VFS.getManager();
        Random r = new Random();
        encryptedFileName = "vfs-encrypted-file" + r.nextInt(1000);
        fileName = "vfs-file" + r.nextInt(1000);
        dirName = "vfs-dir" + r.nextInt(1000);
        bucketName = config.getProperty("s3.testBucket", "vfs-s3-tests");
        bigFile = config.getProperty("big.file");
    }

    @Test
    public void createFileOk() throws FileSystemException {
        file = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + fileName, opts);
        file.createFile();
        assertTrue(file.exists());
    }

    @Test
    public void createEncryptedFileOk() throws FileSystemException {
        file = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + encryptedFileName, opts);
        ((S3FileSystem)file.getFileSystem()).setServerSideEncryption(true);
        file.createFile();
        assertTrue(file.exists());
        assertEquals(
            ((S3FileObject) file).getObjectMetadata().getServerSideEncryption(),
            ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }

    @Test(expectedExceptions={FileSystemException.class})
    public void createFileFailed() throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://../new-mpoint/vfs-bad-file");
        tmpFile.createFile();
    }

    /**
     * Create folder on already existed file
     * @throws FileSystemException
     */
    @Test(expectedExceptions={FileSystemException.class}, dependsOnMethods={"createFileOk"})
    public void createFileFailed2() throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + fileName);
        tmpFile.createFolder();
    }

    @Test
    public void createDirOk() throws FileSystemException {
        dir = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + dirName);
        dir.createFolder();
        assertTrue(dir.exists());
    }

    @Test(expectedExceptions={FileSystemException.class})
    public void createDirFailed() throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://../new-mpoint/vfs-bad-dir");
        tmpFile.createFolder();
    }

    /**
     * Create file on already existed folder
     * @throws FileSystemException
     */
    @Test(expectedExceptions={FileSystemException.class}, dependsOnMethods={"createDirOk"})
    public void createDirFailed2() throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + dirName);
        tmpFile.createFile();
    }

    @Test(dependsOnMethods={"upload"})
    public void exists() throws FileNotFoundException, IOException {
        // Existed dir
        FileObject existedDir = fsManager.resolveFile("s3://" + bucketName + "/test-place");
        assertTrue(existedDir.exists());

        // Non-existed dir
        FileObject nonExistedDir = fsManager.resolveFile(existedDir, "path/to/non/existed/dir");
        Assert.assertFalse(nonExistedDir.exists());

        // Existed file
        FileObject existedFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        assertTrue(existedFile.exists());

        // Non-existed file
        FileObject nonExistedFile = fsManager.resolveFile("s3://" + bucketName + "/ne/bыlo/i/net");
        Assert.assertFalse(nonExistedFile.exists());
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void upload() throws FileNotFoundException, IOException {
        FileObject dest = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = new File(BACKUP_ZIP);

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
        assertEquals(((S3FileObject)dest).getObjectMetadata().getServerSideEncryption(),
            null);
    }

    @Test(dependsOnMethods = {"createEncryptedFileOk"})
    public void uploadEncrypted() throws FileNotFoundException, IOException {
        FileObject dest = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        ((S3FileSystem)dest.getFileSystem()).setServerSideEncryption(true);

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = new File(BACKUP_ZIP);

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
        assertEquals(((S3FileObject)dest).getObjectMetadata().getServerSideEncryption(),
            ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void uploadMultiple() throws Exception {
        FileObject dest = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = new File(BACKUP_ZIP);

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(backupFile.getAbsolutePath());

        // copy twice
        dest.copyFrom(src, Selectors.SELECT_SELF);
        Thread.sleep(2000L);
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void uploadBigFile() throws FileNotFoundException, IOException {
        FileObject dest = fsManager.resolveFile("s3://" + bucketName + "/big_file.iso");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File file = new File(bigFile);

        assertTrue(file.exists(), "Big file should exists");

        FileObject src = fsManager.resolveFile(file.getAbsolutePath());

        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
    }

    @Test(dependsOnMethods={"getSize"})
    public void download() throws IOException {
        FileObject typica = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
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
        FileObject baseDir = fsManager.resolveFile(dir, "list-children-test");
        baseDir.createFolder();

        for (int i=0; i<5; i++) {
            FileObject tmpFile = fsManager.resolveFile(baseDir, i + ".tmp");
            tmpFile.createFile();
        }

        FileObject[] children = baseDir.getChildren();
        assertEquals(children.length, 5);
    }

    @Test(dependsOnMethods={"createDirOk"})
    public void findFiles() throws FileSystemException {
        FileObject baseDir = fsManager.resolveFile(dir, "find-tests");
        baseDir.createFolder();

        // Create files and dirs
        fsManager.resolveFile(baseDir, "child-file.tmp").createFile();
        fsManager.resolveFile(baseDir, "child-file2.tmp").createFile();
        fsManager.resolveFile(baseDir, "child-dir").createFolder();
        fsManager.resolveFile(baseDir, "child-dir/descendant.tmp").createFile();
        fsManager.resolveFile(baseDir, "child-dir/descendant2.tmp").createFile();
        fsManager.resolveFile(baseDir, "child-dir/descendant-dir").createFolder();

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
        FileObject sourceFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + fileName, opts);
        FileObject targetFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/rename-target", opts);

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

            assertTrue(false); // Should block copy into itself
        } catch (FileSystemException e) {
        }
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void getType() throws FileSystemException {
        FileObject imagine = fsManager.resolveFile(dir, "imagine-there-is-no-countries");
        assertEquals(imagine.getType(), FileType.IMAGINARY);
        assertEquals(dir.getType(), FileType.FOLDER);
        assertEquals(file.getType(), FileType.FILE);
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void getTypeAfterCopyToSubFolder() throws FileSystemException {
        FileObject dest = fsManager.resolveFile(dir, "type-tests/sub1/sub2/backup.zip");

        // Copy data
        final File backupFile = new File(BACKUP_ZIP);

        assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));

        FileObject sub1 = fsManager.resolveFile(dir, "type-tests/sub1");
        assertTrue(sub1.exists());
        assertTrue(sub1.getType().equals(FileType.FOLDER));

        FileObject sub2 = fsManager.resolveFile(dir, "type-tests/sub1/sub2");
        assertTrue(sub2.exists());
        assertTrue(sub2.getType().equals(FileType.FOLDER));
    }

    @Test(dependsOnMethods={"upload"})
    public void getContentType() throws FileSystemException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        assertEquals(backup.getContent().getContentInfo().getContentType(), "application/zip");
    }

    @Test(dependsOnMethods={"upload"})
    public void getSize() throws FileSystemException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        assertEquals(backup.getContent().getSize(), 996166);
    }

    @Test(dependsOnMethods={"upload"})
    public void getUrls() throws FileSystemException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        assertTrue(backup.getFileOperations().hasOperation(IPublicUrlsGetter.class));

        IPublicUrlsGetter urlsGetter = (IPublicUrlsGetter) backup.getFileOperations().getOperation(IPublicUrlsGetter.class);

        assertEquals(urlsGetter.getHttpUrl(), "http://" + bucketName + ".s3.amazonaws.com/test-place/backup.zip");
        assertTrue(urlsGetter.getPrivateUrl().endsWith("@" + bucketName + "/test-place/backup.zip"));

        final String signedUrl = urlsGetter.getSignedUrl(60);

        assertTrue(
            signedUrl.startsWith("https://s3.amazonaws.com/" + bucketName + "/test-place%2Fbackup.zip?"),
            signedUrl);
        assertTrue(signedUrl.indexOf("Signature=") != (-1));
        assertTrue(signedUrl.indexOf("Expires=") != (-1));
        assertTrue(signedUrl.indexOf("AWSAccessKeyId=") != (-1));
    }

    @Test(dependsOnMethods={"upload"})
    public void getMD5Hash() throws NoSuchAlgorithmException, FileNotFoundException, IOException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        assertTrue(backup.getFileOperations().hasOperation(IMD5HashGetter.class));

        IMD5HashGetter md5Getter = (IMD5HashGetter) backup.getFileOperations().getOperation(IMD5HashGetter.class);

        String md5Remote = md5Getter.getMD5Hash();

        Assert.assertNotNull(md5Remote);

        final File backupFile = new File(BACKUP_ZIP);

        assertTrue(backupFile.exists(), "Backup file should exists");

        String md5Local = toHex(computeMD5Hash(new FileInputStream(backupFile)));

        assertEquals(md5Remote, md5Local, "Local and remote md5 should be equal");
    }

    @Test(dependsOnMethods={"findFiles"})
	public void copyInsideBucket() throws FileSystemException {
        FileObject testsDir = fsManager.resolveFile(dir, "find-tests");
        FileObject testsDirCopy = testsDir.getParent().resolveFile("find-tests-copy");
        testsDirCopy.copyFrom(testsDir, Selectors.SELECT_SELF_AND_CHILDREN);

        // Should have same number of files
        FileObject[] files = testsDir.findFiles(Selectors.SELECT_SELF_AND_CHILDREN);
        FileObject[] filesCopy = testsDirCopy.findFiles(Selectors.SELECT_SELF_AND_CHILDREN);
        assertEquals(files.length, filesCopy.length,
            Arrays.deepToString(files) + " vs. " + Arrays.deepToString(filesCopy));
	}

    @Test(dependsOnMethods={"findFiles"})
    public void copyAllToEncryptedInsideBucket() throws FileSystemException {
        FileObject testsDir = fsManager.resolveFile(dir, "find-tests");
        FileObject testsDirCopy = testsDir.getParent().resolveFile("find-tests-encrypted-copy");
        ((S3FileSystem)testsDirCopy.getFileSystem()).setServerSideEncryption(true);

        testsDirCopy.copyFrom(testsDir, Selectors.SELECT_ALL);

        // Should have same number of files
        FileObject[] files = testsDir.findFiles(Selectors.SELECT_ALL);
        FileObject[] filesCopy = testsDirCopy.findFiles(Selectors.SELECT_ALL);
        assertEquals(files.length, filesCopy.length);

        for (int i = 0; i < files.length; i++) {
            if (files[i].getType() == FileType.FILE) {
                assertEquals(((S3FileObject)files[i]).getObjectMetadata().getServerSideEncryption(),
                    null);
                assertEquals(((S3FileObject)filesCopy[i]).getObjectMetadata().getServerSideEncryption(),
                    ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            }
        }
    }

    @Test(dependsOnMethods={"findFiles", "download"})
    public void delete() throws FileSystemException {
        FileObject testsDir = fsManager.resolveFile(dir, "find-tests");
        testsDir.delete(Selectors.EXCLUDE_SELF);

        // Only tests dir must remains
        FileObject[] files = testsDir.findFiles(Selectors.SELECT_ALL);
        assertEquals(files.length, 1);
    }

    @AfterClass
    public void tearDown() throws FileSystemException {
        try {
            FileObject vfsTestDir = fsManager.resolveFile(dir, "..");
            vfsTestDir.delete(Selectors.SELECT_ALL);
        } catch (Exception e) {
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
        for (int i = 0; i < data.length; i++) {
            String hex = Integer.toHexString(data[i]);
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
            int bytesRead = -1;
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
}
