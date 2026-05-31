package com.github.vfss3.commonsvfs.tests;

import static com.github.vfss3.commonsvfs.FileAssert.assertHasChildren;
import static java.nio.file.Files.readAllBytes;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.Selectors.SELECT_ALL;
import static org.apache.commons.vfs2.Selectors.SELECT_SELF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import com.github.vfss3.commonsvfs.operations.IMD5HashGetter;
import com.github.vfss3.commonsvfs.operations.IPublicUrlsGetter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3ProviderTest {
    private static final String BIG_FILE = "big_file.iso";

    private FileObject root;
    private FileObject binaryFile;
    private FileObject bigFile;

    private String fileName, dirName;
    private FileObject file, dir;

    @BeforeAll
    void setUp() throws FileSystemException {
        var manager = VFS.getManager();
        root = manager.resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        // Each test class starts with a clean bucket — replicates the original per-class isolation
        // (the suite shares one bucket across all tests).
        if (root.exists()) {
            root.delete(Selectors.EXCLUDE_SELF);
        }
        File backupFile = new File(S3IntegrationContext.BINARY_FILE);
        assertTrue(backupFile.exists(), "Backup file should exist at " + backupFile.getAbsolutePath());
        binaryFile = manager.resolveFile(backupFile.getAbsolutePath());
        bigFile = manager.resolveFile(S3IntegrationContext.BIG_FILE);

        Random r = new Random();
        fileName = "vfs-file" + r.nextInt(1000);
        dirName = "vfs-dir" + r.nextInt(1000);
    }

    @Test
    @Order(1)
    void createFileOk() throws FileSystemException {
        file = root.resolveFile("/test-place/" + fileName);
        file.createFile();
        assertTrue(file.exists());

        FileObject f1 = root.resolveFile("/test-place/name with space");
        f1.createFile();
        assertTrue(f1.exists());

        FileObject f2 = root.resolveFile("/test-place/folder with space");
        f2.createFolder();
        assertTrue(f2.exists());
    }

    @Test
    @Order(3)
    void checkLastModified() throws FileSystemException {
        file = root.resolveFile("/test-place/" + fileName);

        assertTrue(file.exists());

        assertTrue(file.getContent().getLastModifiedTime() > 0);

        assertThrows(FileSystemException.class, () -> {
            file.getContent().setLastModifiedTime(111L);
        });
    }

    @Test
    @Order(3)
    void createFileFailed2() throws FileSystemException {
        FileObject tmpFile = root.resolveFile("/test-place/" + fileName);
        assertThrows(FileSystemException.class, () -> {
            tmpFile.createFolder();
        });
    }

    @Test
    @Order(2)
    void createDirOk() throws FileSystemException {
        dir = root.resolveFile("/test-place/" + dirName);
        dir.createFolder();

        assertTrue(dir.exists());
        assertEquals(FileType.FOLDER, dir.getName().getType());
    }

    @Test
    @Order(3)
    void createDirFailed2() throws FileSystemException {
        FileObject tmpFile = root.resolveFile("/test-place/" + dirName);
        assertThrows(FileSystemException.class, () -> {
            tmpFile.createFile();
        });
    }

    @Test
    @Order(8)
    void exists() throws IOException {
        FileObject existedDir = root.resolveFile("/test-place");
        assertTrue(existedDir.exists());

        FileObject existedFile = root.resolveFile("/test-place/backup.zip");
        assertTrue(existedFile.exists());

        FileObject nonExistedFile = root.resolveFile("/ne/bыlo/i/net");
        assertFalse(nonExistedFile.exists());
    }

    @Test
    @Order(4)
    void upload() throws IOException {
        FileObject dest = root.resolveFile("/test-place/backup.zip");

        if (dest.exists()) {
            dest.delete();
        }

        dest.copyFrom(binaryFile, SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FILE));
    }

    @Test
    @Order(5)
    void uploadMultiple() throws Exception {
        FileObject dest = root.resolveFile("/test-place/backup.zip");

        if (dest.exists()) {
            dest.delete();
        }

        dest.copyFrom(binaryFile, SELECT_SELF);
        Thread.sleep(2000L);
        dest.copyFrom(binaryFile, SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FILE));
    }

    @Test
    @Order(6)
    @Disabled("Unreliable external dependency: downloads a ~64 MB ISO from archive.ubuntu.com at "
            + "test time and asserts an exact byte count. See docs/test-cases/changes-from-original.md")
    void uploadBigFile() throws IOException {
        FileObject dest = root.resolveFile("/" + BIG_FILE);

        if (dest.exists()) {
            dest.delete();
        }

        assertTrue(bigFile.exists(), "Big file should exists");

        // 16 MB — historical multipart-upload threshold of AWS SDK v1's TransferManager.
        assertTrue(bigFile.getContent().getSize() > 16L * 1024 * 1024);

        dest.copyFrom(bigFile, SELECT_SELF);

        assertTrue(dest.exists(), "Destination file should be on place");
        assertEquals(FILE, dest.getType());
        assertEquals(bigFile.getContent().getSize(), dest.getContent().getSize());
        assertEquals(63963136, dest.getContent().getSize());
    }

    @Test
    @Order(5)
    void outputStream() throws IOException {
        FileObject dest = root.resolveFile("/test-place/output.txt");

        if (dest.exists()) {
            dest.delete();
        }

        final byte[] bytes = readAllBytes(binaryFile.getPath());

        try (OutputStream os = dest.getContent().getOutputStream()) {
            os.write(bytes);
        }

        assertTrue(dest.exists() && dest.getType().equals(FILE));
        assertEquals(binaryFile.getPath().toFile().length(), dest.getContent().getSize());

        try (InputStream in = dest.getContent().getInputStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            IOUtils.copy(in, buffer);

            assertArrayEquals(bytes, buffer.toByteArray());
        }

        dest.delete();
    }

    @Test
    @Order(9)
    void download() throws IOException {
        FileObject typica = root.resolveFile("/test-place/backup.zip");
        File localCache = File.createTempFile("vfs.", ".s3-test");

        FileOutputStream out = new FileOutputStream(localCache);
        IOUtils.copy(typica.getContent().getInputStream(), out);

        assertEquals(typica.getContent().getSize(), localCache.length());

        localCache.delete();
    }

    @Test
    @Order(7)
    void listChildren() throws FileSystemException {
        FileObject baseDir = dir.resolveFile("list-children-test");
        baseDir.createFolder();

        for (int i = 0; i < 5; i++) {
            FileObject tmpFile = baseDir.resolveFile(i + ".tmp");
            tmpFile.createFile();
        }

        FileObject[] children = baseDir.getChildren();
        assertEquals(5, children.length);
    }

    @Test
    @Order(7)
    @Disabled("Depends on big_file.iso created by the disabled uploadBigFile; the listing checks "
            + "are split into Suite B and Suite E. See docs/test-cases/changes-from-original.md")
    void listChildrenRoot() throws FileSystemException {
        assertHasChildren(root.resolveFile("/"), "test-place", BIG_FILE);
        assertHasChildren(
                root.resolveFile("/test-place/"),
                "backup.zip",
                dirName,
                fileName,
                "folder with space",
                "name with space");
        assertHasChildren(
                root.resolveFile("/test-place"),
                "backup.zip",
                dirName,
                fileName,
                "folder with space",
                "name with space");

        final FileObject destFile = root.resolveFile("/test-place-2");

        destFile.copyFrom(root.resolveFile("/test-place"), SELECT_ALL);

        assertHasChildren(destFile, "backup.zip", dirName, fileName, "folder with space", "name with space");
    }

    @Test
    @Order(7)
    void findFiles() throws FileSystemException {
        FileObject baseDir = dir.resolveFile("find-tests");
        baseDir.createFolder();

        baseDir.resolveFile("child-file.tmp").createFile();
        baseDir.resolveFile("child-file2.tmp").createFile();
        baseDir.resolveFile("child-dir").createFolder();
        baseDir.resolveFile("child-dir/descendant.tmp").createFile();
        baseDir.resolveFile("child-dir/descendant2.tmp").createFile();
        baseDir.resolveFile("child-dir/descendant-dir").createFolder();

        FileObject[] files;
        files = baseDir.findFiles(Selectors.SELECT_CHILDREN);
        assertEquals(3, files.length);
        files = baseDir.findFiles(Selectors.SELECT_FOLDERS);
        assertEquals(3, files.length);
        files = baseDir.findFiles(Selectors.SELECT_FILES);
        assertEquals(4, files.length);
        files = baseDir.findFiles(Selectors.EXCLUDE_SELF);
        assertEquals(6, files.length);
    }

    @Test
    @Order(7)
    void renameAndMove() throws FileSystemException {
        FileObject sourceFile = root.resolveFile("/test-place/" + fileName);
        FileObject targetFile = root.resolveFile("/test-place/rename-target");

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

        assertThrows(FileSystemException.class, () -> {
            sourceFile.moveTo(sourceFile);
        });
    }

    @Test
    @Order(7)
    void getType() throws FileSystemException {
        FileObject imagine = dir.resolveFile("imagine-there-is-no-countries");

        assertEquals(FileType.IMAGINARY, imagine.getType());
        assertEquals(FileType.FOLDER, dir.getType());
        assertEquals(FILE, file.getType());
    }

    @Test
    @Order(7)
    void getTypeAfterCopyToSubFolder() throws FileSystemException {
        FileObject dest = dir.resolveFile("type-tests/sub1/sub2/backup.zip");

        dest.copyFrom(binaryFile, SELECT_SELF);

        assertTrue(dest.exists() && dest.getType().equals(FILE));

        FileObject sub1 = dir.resolveFile("type-tests/sub1");
        assertTrue(sub1.exists());
        assertTrue(sub1.getType().equals(FileType.FOLDER));

        FileObject sub2 = dir.resolveFile("type-tests/sub1/sub2");
        assertTrue(sub2.exists());
        assertTrue(sub2.getType().equals(FileType.FOLDER));
    }

    @Test
    @Order(7)
    void getContentType() throws FileSystemException {
        FileObject backup = root.resolveFile("/test-place/backup.zip");
        assertEquals("application/zip", backup.getContent().getContentInfo().getContentType());
    }

    @Test
    @Order(7)
    void getSize() throws FileSystemException {
        FileObject backup = root.resolveFile("/test-place/backup.zip");

        assertEquals(996166, backup.getContent().getSize());
    }

    @Test
    @Order(7)
    void getUrls() throws FileSystemException {
        FileObject backup = root.resolveFile("/test-place/backup.zip");

        assertTrue(backup.getFileOperations().hasOperation(IPublicUrlsGetter.class));
        assertNotNull(backup.getFileOperations().getOperation(IPublicUrlsGetter.class));

        IPublicUrlsGetter urlsGetter =
                (IPublicUrlsGetter) backup.getFileOperations().getOperation(IPublicUrlsGetter.class);

        assertThat(urlsGetter.getHttpUrl()).contains("http", "/test-place/backup.zip");

        final String signedUrl = urlsGetter.getSignedUrl(60);

        assertThat(signedUrl)
                .contains(
                        "http",
                        "/test-place/backup.zip",
                        "Signature=",
                        "Expires=",
                        "X-Amz-Credential=",
                        "X-Amz-Signature=");
    }

    @Test
    @Order(7)
    void getMD5Hash() throws NoSuchAlgorithmException, IOException {
        FileObject backup = root.resolveFile("/test-place/backup.zip");

        assertTrue(backup.getFileOperations().hasOperation(IMD5HashGetter.class));

        IMD5HashGetter md5Getter = (IMD5HashGetter) backup.getFileOperations().getOperation(IMD5HashGetter.class);

        String md5Remote = md5Getter.getMD5Hash();

        assertNotNull(md5Remote);

        String md5Local = toHex(computeMD5Hash(binaryFile.getContent().getInputStream()));

        assertTrue(md5Remote.equalsIgnoreCase(md5Local), "Local and remote md5 should be equal");
    }

    @Test
    @Order(7)
    void getLastModified() throws FileSystemException {
        FileObject backup = root.resolveFile("/test-place/backup.zip");

        assertTrue(ofEpochMilli(backup.getContent().getLastModifiedTime())
                        .atZone(UTC)
                        .getYear()
                > 2010);
    }

    @Test
    @Order(10)
    void delete() throws FileSystemException {
        FileObject testsDir = dir.resolveFile("find-tests");
        testsDir.delete(Selectors.EXCLUDE_SELF);

        FileObject[] files = testsDir.findFiles(SELECT_ALL);
        assertEquals(1, files.length);
    }

    private String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte aData : data) {
            String hex = Integer.toHexString(aData);
            if (hex.length() == 1) {
                sb.append("0");
            } else if (hex.length() == 8) {
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
}
