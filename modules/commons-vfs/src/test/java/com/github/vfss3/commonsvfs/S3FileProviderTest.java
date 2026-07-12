package com.github.vfss3.commonsvfs;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.Test;

/**
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
final class S3FileProviderTest {
    @Test
    void checkCache() throws FileSystemException {
        final FileSystemManager manager = VFS.getManager();

        FileSystem fs = manager.resolveFile(
                        "s3://access:secret@s3.eu-central-1.amazonaws.com/bucket/concurrent/",
                        new S3FileSystemOptions().toFileSystemOptions())
                .getFileSystem();

        assertSame(
                fs,
                manager.resolveFile(
                                "s3://access:secret@s3.eu-central-1.amazonaws.com/bucket/concurrent/",
                                new S3FileSystemOptions().toFileSystemOptions())
                        .getFileSystem());

        assertSame(
                fs,
                manager.resolveFile(
                                "s3://access:secret@s3.eu-central-1.amazonaws.com/bucket/concurrent/",
                                new S3FileSystemOptions().toFileSystemOptions())
                        .getFileSystem());
    }
}
