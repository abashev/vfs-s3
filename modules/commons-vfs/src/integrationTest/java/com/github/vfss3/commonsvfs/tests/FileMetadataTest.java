package com.github.vfss3.commonsvfs.tests;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static org.apache.commons.vfs2.Selectors.SELECT_SELF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import com.github.vfss3.commonsvfs.operations.IMD5HashGetter;
import com.github.vfss3.commonsvfs.operations.IPublicUrlsGetter;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Suite D: File Metadata — see {@code docs/test-cases/d-file-metadata.md}.
 *
 * <p>Works in the isolated {@code /metadata/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}. A single {@code backup.zip} is uploaded once for the whole class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileMetadataTest {
    private static final String PREFIX = "/metadata/";
    private static final long BACKUP_SIZE = 996_166L;

    private FileObject root;
    private FileObject local;
    private FileObject backup;

    @BeforeAll
    void setUp() throws FileSystemException {
        var manager = VFS.getManager();
        root = manager.resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        File backupFile = new File(S3IntegrationContext.BINARY_FILE);
        assertTrue(backupFile.exists(), "Backup file should exist at " + backupFile.getAbsolutePath());
        local = manager.resolveFile(backupFile.getAbsolutePath());

        backup = root.resolveFile(PREFIX + "backup.zip");
        backup.copyFrom(local, SELECT_SELF);
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        FileObject prefix = root.resolveFile(PREFIX);
        if (prefix.exists()) {
            prefix.deleteAll();
        }
    }

    /** Step 1: content type is reported as application/zip. */
    @Test
    void testContentType() throws FileSystemException {
        assertEquals("application/zip", backup.getContent().getContentInfo().getContentType());
    }

    /** Step 2: content size matches the local file. */
    @Test
    void testContentSize() throws FileSystemException {
        assertEquals(BACKUP_SIZE, backup.getContent().getSize());
    }

    /** Step 3: last-modified time is a recent (post-2010) timestamp. */
    @Test
    void testLastModified() throws FileSystemException {
        int year = ofEpochMilli(backup.getContent().getLastModifiedTime())
                .atZone(UTC)
                .getYear();
        assertThat(year).as("Last modified year").isGreaterThan(2010);
    }

    /** Step 4: public and signed URLs are exposed via IPublicUrlsGetter. */
    @Test
    void testUrls() throws FileSystemException {
        assertTrue(backup.getFileOperations().hasOperation(IPublicUrlsGetter.class));

        IPublicUrlsGetter urls = (IPublicUrlsGetter) backup.getFileOperations().getOperation(IPublicUrlsGetter.class);
        assertNotNull(urls);

        assertThat(urls.getHttpUrl()).contains("http", PREFIX + "backup.zip");

        assertThat(urls.getSignedUrl(60))
                .contains("http", PREFIX + "backup.zip", "X-Amz-Credential=", "X-Amz-Signature=");
    }

    /** Step 5: the remote MD5 hash matches the locally computed one. */
    @Test
    void testMd5Hash() throws NoSuchAlgorithmException, IOException {
        assertTrue(backup.getFileOperations().hasOperation(IMD5HashGetter.class));

        IMD5HashGetter md5Getter = (IMD5HashGetter) backup.getFileOperations().getOperation(IMD5HashGetter.class);

        String remote = md5Getter.getMD5Hash();
        assertNotNull(remote, "Remote MD5 should not be null");

        String localHash = toHex(computeMd5(local.getContent().getInputStream()));

        assertTrue(remote.equalsIgnoreCase(localHash), "Local and remote MD5 should match");
    }

    private static String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString().toLowerCase(Locale.ROOT);
    }

    private static byte[] computeMd5(InputStream is) throws NoSuchAlgorithmException, IOException {
        try (BufferedInputStream bis = new BufferedInputStream(is)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[16384];
            int read;
            while ((read = bis.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return digest.digest();
        }
    }
}
