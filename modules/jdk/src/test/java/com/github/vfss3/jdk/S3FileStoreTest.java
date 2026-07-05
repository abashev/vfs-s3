package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.attribute.BasicFileAttributeView;
import org.junit.jupiter.api.Test;

class S3FileStoreTest {

    private final S3FileStore store = new S3FileStore("my-bucket");

    @Test
    void nameIsTheBucket() {
        assertEquals("my-bucket", store.name());
    }

    @Test
    void doesNotClaimToSupportAFileAttributeViewItNeverReturns() {
        // S3FileSystemProvider.getFileAttributeView always returns null (no view is exposed),
        // so supportsFileAttributeView(Class) must not claim otherwise.
        assertFalse(store.supportsFileAttributeView(BasicFileAttributeView.class));
    }
}
