package com.github.vfss3;

import com.amazonaws.regions.Regions;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

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

    @Test(description = "Test how VFS-S3 plugin can access public OSM bucket, see https://registry.opendata.aws/osm/")
    public void testResolvePublicBucket() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        FileSystemOptions options = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder builder = new DelegatingFileSystemOptionsBuilder(manager);
        builder.setConfigString(options, S3FileProvider.SCHEMA, "region", Regions.US_EAST_1.getName());
        String bucket = "s3://osm-pds";
        FileObject file = manager.resolveFile(bucket, options);

        assertNotNull(file, "Public bucket " + bucket + " is not resolved");
    }
}
