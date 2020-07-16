package com.github.vfss3;

import org.apache.commons.vfs2.*;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public class S3FileProviderTest {
    @Test
    public void checkCache() throws FileSystemException {
        final FileSystemManager manager = VFS.getManager();

        FileSystem fs = manager.resolveFile(
                "s3://access:secret@s3.eu-central-1.amazonaws.com/bucket/concurrent/",
                new S3FileSystemOptions().toFileSystemOptions()
        ).getFileSystem();

        assertSame(manager.resolveFile(
                "s3://access:secret@s3.eu-central-1.amazonaws.com/bucket/concurrent/",
                new S3FileSystemOptions().toFileSystemOptions()
        ).getFileSystem(), fs);

        assertSame(manager.resolveFile(
                "s3://access:secret@s3.eu-central-1.amazonaws.com/bucket/concurrent/",
                new S3FileSystemOptions().toFileSystemOptions()
        ).getFileSystem(), fs);
    }
}
