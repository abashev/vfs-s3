package com.github.vfss3.commonsvfs;

import static org.junit.jupiter.api.Assertions.assertSame;

import com.github.vfss3.commonsvfs.parser.PlatformFeaturesImpl;
import org.junit.jupiter.api.Test;

final class S3FileSystemOptionsTest {
    @Test
    void checkPlatformFeaturesOption() {
        var features = new PlatformFeaturesImpl(true, true, false, true, false);
        var options = new S3FileSystemOptions();

        options.setPlatformFeatures(features);

        assertSame(features, options.getPlatformFeatures());
        assertSame(features, new S3FileSystemOptions(options.toFileSystemOptions()).getPlatformFeatures());
    }
}
