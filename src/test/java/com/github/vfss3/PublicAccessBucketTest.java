package com.github.vfss3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
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
 * Unit test for accessing public buckets.
 */
public class PublicAccessBucketTest {

    @Test
    public void testCreate() throws Exception {
        FileSystemManager manager = VFS.getManager();
        FileSystemOptions options = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder builder = new DelegatingFileSystemOptionsBuilder(manager);

        builder.setConfigString(options, PREFIX, "serverSideEncryption", "true");

        options.clone();
    }

    @Test(description = "Test how VFS-S3 plugin can access public OSM bucket, see https://registry.opendata.aws/osm/")
    public void testResolvePublicBucket1() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        S3FileSystemOptions options = new S3FileSystemOptions();

        options.setCredentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));

        String bucket = "s3://osm-pds.s3.amazonaws.com/";

        final FileObject[] children = manager.resolveFile(bucket, options.toFileSystemOptions()).getChildren();

        assertNotNull(children, "Public bucket " + bucket + " is not resolved");
        assertTrue(children.length > 0, "Public bucket " + bucket + " is not resolved");
    }

    @Test(description = "Test how VFS-S3 plugin can access public OSM bucket, see https://registry.opendata.aws/osm/")
    public void testResolvePublicBucket2() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        S3FileSystemOptions options = new S3FileSystemOptions();

        options.setCredentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));

        String bucket = "s3://s3.amazonaws.com/osm-pds";

        final FileObject[] children = manager.resolveFile(bucket, options.toFileSystemOptions()).getChildren();

        assertNotNull(children, "Public bucket " + bucket + " is not resolved");
        assertTrue(children.length > 0, "Public bucket " + bucket + " is not resolved");
    }
}
