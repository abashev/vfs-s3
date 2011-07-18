package com.intridea.io.vfs.provider.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.Selectors;
import org.apache.commons.vfs.VFS;
import static org.jets3t.service.utils.ServiceUtils.*;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.intridea.io.vfs.TestEnvironment;
import com.intridea.io.vfs.operations.IMD5HashGetter;
import com.intridea.io.vfs.operations.IPublicUrlsGetter;

@Test(groups={"storage"})
public class S3ProviderTest {

    /**
     *
     */
    private static final String BACKUP_ZIP = "src/test/resources/backup.zip";

    private FileSystemManager fsManager;

    private String fileName, dirName, bucketName, bigFile;
    private FileObject file, dir;

    private FileSystemOptions opts;

    @BeforeClass
    public void setUp () throws FileNotFoundException, IOException {
        Properties config = TestEnvironment.getInstance().getConfig();

        fsManager = VFS.getManager();
        Random r = new Random();
        fileName = "vfs-file" + r.nextInt(1000);
        dirName = "vfs-dir" + r.nextInt(1000);
        bucketName = config.getProperty("s3.testBucket", "vfs-s3-tests");
        bigFile = config.getProperty("big.file");
    }

    @Test
    public void createFileOk () throws FileSystemException {
        file = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + fileName, opts);
        file.createFile();
        Assert.assertTrue(file.exists());
    }

    @Test(expectedExceptions={FileSystemException.class})
    public void createFileFailed () throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://../new-mpoint/vfs-bad-file");
        tmpFile.createFile();
    }

    /**
     * Create folder on already existed file
     * @throws FileSystemException
     */
    @Test(expectedExceptions={FileSystemException.class}, dependsOnMethods={"createFileOk"})
    public void createFileFailed2 () throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + fileName);
        tmpFile.createFolder();
    }

    @Test
    public void createDirOk () throws FileSystemException {
        dir = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + dirName);
        dir.createFolder();
        Assert.assertTrue(dir.exists());
    }

    @Test(expectedExceptions={FileSystemException.class})
    public void createDirFailed () throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://../new-mpoint/vfs-bad-dir");
        tmpFile.createFolder();
    }

    /**
     * Create file on already existed folder
     * @throws FileSystemException
     */
    @Test(expectedExceptions={FileSystemException.class}, dependsOnMethods={"createDirOk"})
    public void createDirFailed2 () throws FileSystemException {
        FileObject tmpFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/" + dirName);
        tmpFile.createFile();
    }

    @Test(dependsOnMethods={"upload"})
    public void exists () throws FileNotFoundException, IOException {
        // Existed dir
        FileObject existedDir = fsManager.resolveFile("s3://" + bucketName + "/test-place");
        Assert.assertTrue(existedDir.exists());

        // Non-existed dir
        FileObject nonExistedDir = fsManager.resolveFile(existedDir, "path/to/non/existed/dir");
        Assert.assertFalse(nonExistedDir.exists());

        // Existed file
        FileObject existedFile = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        Assert.assertTrue(existedFile.exists());

        // Non-existed file
        FileObject nonExistedFile = fsManager.resolveFile("s3://" + bucketName + "/ne/bыlo/i/net");
        Assert.assertFalse(nonExistedFile.exists());
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void upload () throws FileNotFoundException, IOException {
        FileObject dest = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = new File(BACKUP_ZIP);

        Assert.assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(backupFile.getAbsolutePath());
        dest.copyFrom(src, Selectors.SELECT_SELF);

        Assert.assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
    }

    @Test(dependsOnMethods={"createFileOk"})
    public void upload_multiple() throws Exception {
        FileObject dest = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        // Delete file if exists
        if (dest.exists()) {
            dest.delete();
        }

        // Copy data
        final File backupFile = new File(BACKUP_ZIP);

        Assert.assertTrue(backupFile.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(backupFile.getAbsolutePath());

        // copy twice
        dest.copyFrom(src, Selectors.SELECT_SELF);
        Thread.sleep(2000L);
        dest.copyFrom(src, Selectors.SELECT_SELF);

        Assert.assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
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

        Assert.assertTrue(file.exists(), "Backup file should exists");

        FileObject src = fsManager.resolveFile(file.getAbsolutePath());

        dest.copyFrom(src, Selectors.SELECT_SELF);

        Assert.assertTrue(dest.exists() && dest.getType().equals(FileType.FILE));
    }

    @Test(dependsOnMethods={"getSize"})
    public void download () throws IOException {
        FileObject typica = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        File localCache =  File.createTempFile("vfs.", ".s3-test");

        // Copy from S3 to localfs
        FileOutputStream out = new FileOutputStream(localCache);
        IOUtils.copy(typica.getContent().getInputStream(), out);

        // Test that file sizes equals
        Assert.assertEquals(localCache.length(), typica.getContent().getSize());

        localCache.delete();
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void listChildren () throws FileSystemException {
        FileObject baseDir = fsManager.resolveFile(dir, "list-children-test");
        baseDir.createFolder();

        for (int i=0; i<5; i++) {
            FileObject tmpFile = fsManager.resolveFile(baseDir, i + ".tmp");
            tmpFile.createFile();
        }

        FileObject[] children = baseDir.getChildren();
        Assert.assertEquals(children.length, 5);
    }

    @Test(dependsOnMethods={"createDirOk"})
    public void findFiles () throws FileSystemException {
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
        Assert.assertEquals(files.length, 3);
        files = baseDir.findFiles(Selectors.SELECT_FOLDERS);
        Assert.assertEquals(files.length, 3);
        files = baseDir.findFiles(Selectors.SELECT_FILES);
        Assert.assertEquals(files.length, 4);
        files = baseDir.findFiles(Selectors.EXCLUDE_SELF);
        Assert.assertEquals(files.length, 6);
    }

    @Test(dependsOnMethods={"createFileOk", "createDirOk"})
    public void getType () throws FileSystemException {
        FileObject imagine = fsManager.resolveFile(dir, "imagine-there-is-no-countries");
        Assert.assertEquals(imagine.getType(), FileType.IMAGINARY);
        Assert.assertEquals(dir.getType(), FileType.FOLDER);
        Assert.assertEquals(file.getType(), FileType.FILE);
    }

    @Test(dependsOnMethods={"upload"})
    public void getContentType () throws FileSystemException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        Assert.assertEquals(backup.getContent().getContentInfo().getContentType(), "application/zip");
    }

    @Test(dependsOnMethods={"upload"})
    public void getSize () throws FileSystemException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");
        Assert.assertEquals(backup.getContent().getSize(), 996166);
    }

    @Test(dependsOnMethods={"upload"})
    public void getUrls() throws FileSystemException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        Assert.assertTrue(backup.getFileOperations().hasOperation(IPublicUrlsGetter.class));

        IPublicUrlsGetter urlsGetter = (IPublicUrlsGetter) backup.getFileOperations().getOperation(IPublicUrlsGetter.class);

        Assert.assertEquals(urlsGetter.getHttpUrl(), "http://" + bucketName + ".s3.amazonaws.com/test-place/backup.zip");
        Assert.assertTrue(urlsGetter.getPrivateUrl().endsWith("@" + bucketName + "/test-place/backup.zip"));
        Assert.assertTrue(urlsGetter.getSignedUrl(60).startsWith("https://" + bucketName + ".s3.amazonaws.com/test-place/backup.zip?AWSAccessKeyId="));
    }

    @Test(dependsOnMethods={"upload"})
    public void getMD5Hash() throws NoSuchAlgorithmException, FileNotFoundException, IOException {
        FileObject backup = fsManager.resolveFile("s3://" + bucketName + "/test-place/backup.zip");

        Assert.assertTrue(backup.getFileOperations().hasOperation(IMD5HashGetter.class));

        IMD5HashGetter md5Getter = (IMD5HashGetter) backup.getFileOperations().getOperation(IMD5HashGetter.class);

        String md5Remote = md5Getter.getMD5Hash();

        Assert.assertNotNull(md5Remote);

        final File backupFile = new File(BACKUP_ZIP);

        Assert.assertTrue(backupFile.exists(), "Backup file should exists");

        String md5Local = toHex(computeMD5Hash(new FileInputStream(backupFile)));

        Assert.assertEquals(md5Remote, md5Local, "Local and remote md5 should be equal");
    }

    @Test(dependsOnMethods={"findFiles", "download"})
    public void delete () throws FileSystemException {
        FileObject testsDir = fsManager.resolveFile(dir, "find-tests");
        testsDir.delete(Selectors.EXCLUDE_SELF);

        // Only tests dir must remains
        FileObject[] files = testsDir.findFiles(Selectors.SELECT_ALL);
        Assert.assertEquals(files.length, 1);
    }

    @AfterClass
    public void tearDown () throws FileSystemException {
        try {
            FileObject vfsTestDir = fsManager.resolveFile(dir, "..");
            vfsTestDir.delete(Selectors.SELECT_ALL);
        } catch (Exception e) {
        }
    }
}
