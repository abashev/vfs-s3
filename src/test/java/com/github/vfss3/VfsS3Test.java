package com.github.vfss3;

import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;
import org.testng.annotations.Test;

/**
 * Unit test for vfs-s3
 */
public class VfsS3Test {

    @Test
    public void testCreate() throws Exception {
        FileSystemManager manager = VFS.getManager();
        FileSystemOptions options = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder builder = new DelegatingFileSystemOptionsBuilder(manager);
        builder.setConfigString(options, "s3", "serverSideEncryption", "true");
        options.clone();
    }
}
