package com.github.vfss3;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;
import org.testng.annotations.Test;

import static com.github.vfss3.S3FileSystemOptions.PREFIX;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit test for vfs-s3
 */
public class VfsS3Test {

    @Test
    public void testCreate() throws Exception {
        FileSystemManager manager = VFS.getManager();
        FileSystemOptions options = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder builder = new DelegatingFileSystemOptionsBuilder(manager);

        builder.setConfigString(options, PREFIX, "serverSideEncryption", "true");

        options.clone();
    }

    @Test(description = "Test how VFS-S3 plugin can access public OSM bucket, see https://registry.opendata.aws/osm/")
    public void testResolvePublicBucket() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        FileSystemOptions options = new FileSystemOptions();

        String bucket = "s3://osm-pds.s3.amazonaws.com/";

        final FileObject[] children = manager.resolveFile(bucket, options).getChildren();

        assertNotNull(children, "Public bucket " + bucket + " is not resolved");
        assertTrue(children.length > 0, "Public bucket " + bucket + " is not resolved");
    }
}
