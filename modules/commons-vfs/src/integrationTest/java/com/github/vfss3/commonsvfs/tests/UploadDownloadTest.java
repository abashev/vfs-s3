package com.github.vfss3.commonsvfs.tests;

import static java.nio.file.Files.readAllBytes;
import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.Selectors.SELECT_SELF;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3IntegrationContext;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Suite C: Upload &amp; Download — see {@code docs/test-cases/c-upload-download.md}.
 *
 * <p>Works in the isolated {@code /upload/} prefix; the whole prefix is deleted in
 * {@code @AfterAll}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadDownloadTest {
    private static final String PREFIX = "/upload/";

    private FileObject root;
    private FileObject local;

    @BeforeAll
    void resolveBackend() throws FileSystemException {
        var manager = VFS.getManager();
        root = manager.resolveFile(S3IntegrationContext.rootUrl(), S3IntegrationContext.options());
        File backupFile = new File(S3IntegrationContext.BINARY_FILE);
        assertTrue(backupFile.exists(), "Backup file should exist at " + backupFile.getAbsolutePath());
        local = manager.resolveFile(backupFile.getAbsolutePath());
    }

    @AfterAll
    void tearDown() throws FileSystemException {
        FileObject prefix = root.resolveFile(PREFIX);
        if (prefix.exists()) {
            prefix.deleteAll();
        }
    }

    /** Step 1: copy a local binary file to the bucket. */
    @Test
    void testUpload() throws FileSystemException {
        FileObject dest = root.resolveFile(PREFIX + "backup.zip");
        if (dest.exists()) {
            dest.delete();
        }

        dest.copyFrom(local, SELECT_SELF);

        assertTrue(dest.exists(), "Uploaded file should exist");
        assertEquals(FILE, dest.getType(), "Uploaded object should be a file");
    }

    /** Step 2: overwrite the same key after a short delay. */
    @Test
    void testOverwrite() throws Exception {
        FileObject dest = root.resolveFile(PREFIX + "overwrite.zip");
        if (dest.exists()) {
            dest.delete();
        }

        dest.copyFrom(local, SELECT_SELF);
        Thread.sleep(2000L);
        dest.copyFrom(local, SELECT_SELF);

        assertTrue(dest.exists(), "Overwritten file should exist");
        assertEquals(FILE, dest.getType(), "Overwritten object should be a file");
    }

    /** Step 3: round-trip the bytes through an output stream and read them back. */
    @Test
    void testOutputStreamRoundTrip() throws IOException {
        FileObject dest = root.resolveFile(PREFIX + "output.txt");
        if (dest.exists()) {
            dest.delete();
        }

        byte[] bytes = readAllBytes(local.getPath());

        try (OutputStream os = dest.getContent().getOutputStream()) {
            os.write(bytes);
        }

        assertTrue(dest.exists(), "File written via output stream should exist");
        assertEquals(FILE, dest.getType());
        assertEquals(bytes.length, dest.getContent().getSize(), "Written size should match the local file");

        try (InputStream in = dest.getContent().getInputStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            IOUtils.copy(in, buffer);
            assertArrayEquals(bytes, buffer.toByteArray(), "Read-back content should match");
        }

        dest.delete();
    }

    /** Step 4: uploading to a deep key auto-creates the intermediate folders. */
    @Test
    void testNestedUploadCreatesFolders() throws FileSystemException {
        FileObject dest = root.resolveFile(PREFIX + "deep/sub1/sub2/backup.zip");
        dest.copyFrom(local, SELECT_SELF);

        assertTrue(dest.exists(), "Nested file should exist");
        assertEquals(FILE, dest.getType());

        FileObject sub1 = root.resolveFile(PREFIX + "deep/sub1");
        assertTrue(sub1.exists(), "Intermediate folder sub1 should exist");
        assertEquals(FOLDER, sub1.getType());

        FileObject sub2 = root.resolveFile(PREFIX + "deep/sub1/sub2");
        assertTrue(sub2.exists(), "Intermediate folder sub2 should exist");
        assertEquals(FOLDER, sub2.getType());
    }

    /** Step 5: download the uploaded file back into a local temp file. */
    @Test
    void testDownload() throws IOException {
        FileObject dest = root.resolveFile(PREFIX + "download.zip");
        if (!dest.exists()) {
            dest.copyFrom(local, SELECT_SELF);
        }

        File temp = File.createTempFile("vfs.", ".s3-test");
        try (InputStream in = dest.getContent().getInputStream();
                FileOutputStream out = new FileOutputStream(temp)) {
            IOUtils.copy(in, out);
        }

        assertEquals(dest.getContent().getSize(), temp.length(), "Downloaded size should match the remote file");

        assertTrue(temp.delete(), "Temp file should be deleted");
    }
}
