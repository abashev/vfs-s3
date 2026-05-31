package com.github.vfss3.commonsvfs.tests;

import static com.github.vfss3.commonsvfs.S3FileSystemOptions.PREFIX;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;

/**
 * Integration test for accessing public buckets — does not need the suite's container.
 */
public class LegacyPublicAccessBucketTest {

    @Test
    void testCreate() throws Exception {
        FileSystemManager manager = VFS.getManager();
        FileSystemOptions options = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder builder = new DelegatingFileSystemOptionsBuilder(manager);

        builder.setConfigString(options, PREFIX, "serverSideEncryption", "true");

        var cloned = options.clone();

        assertNotNull(cloned);
    }

    @Test
    @DisplayName("Test how VFS-S3 plugin can access public OSM bucket, see https://registry.opendata.aws/osm/")
    void testResolvePublicBucket1() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        S3FileSystemOptions options = new S3FileSystemOptions();

        options.setCredentialsProvider(AnonymousCredentialsProvider.create());

        String bucket = "s3://osm-pds.s3.amazonaws.com/";

        final FileObject[] children =
                manager.resolveFile(bucket, options.toFileSystemOptions()).getChildren();

        assertNotNull(children, "Public bucket " + bucket + " is not resolved");
        assertTrue(children.length > 0, "Public bucket " + bucket + " is not resolved");
    }

    @Test
    @DisplayName("Test how VFS-S3 plugin can access public OSM bucket, see https://registry.opendata.aws/osm/")
    void testResolvePublicBucket2() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        S3FileSystemOptions options = new S3FileSystemOptions();

        options.setCredentialsProvider(AnonymousCredentialsProvider.create());

        String bucket = "s3://s3.amazonaws.com/osm-pds";

        final FileObject[] children =
                manager.resolveFile(bucket, options.toFileSystemOptions()).getChildren();

        assertNotNull(children, "Public bucket " + bucket + " is not resolved");
        assertTrue(children.length > 0, "Public bucket " + bucket + " is not resolved");
    }
}
