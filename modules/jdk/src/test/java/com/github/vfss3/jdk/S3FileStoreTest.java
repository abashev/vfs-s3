package com.github.vfss3.jdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
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

    @Test
    void supportsOnlyTheBasicViewByName() {
        assertTrue(store.supportsFileAttributeView("basic"));
        assertFalse(store.supportsFileAttributeView("posix"));
    }

    @Test
    void isAWritableS3StoreWithUnboundedSpace() {
        assertEquals("s3", store.type());
        assertFalse(store.isReadOnly());
        // S3 has no quota API — space figures are reported as unbounded.
        assertEquals(Long.MAX_VALUE, store.getTotalSpace());
        assertEquals(Long.MAX_VALUE, store.getUsableSpace());
        assertEquals(Long.MAX_VALUE, store.getUnallocatedSpace());
    }

    @Test
    void storeAttributesAreNotExposed() {
        assertNull(store.getFileStoreAttributeView(FileStoreAttributeView.class));
        assertThrows(UnsupportedOperationException.class, () -> store.getAttribute("totalSpace"));
    }
}
