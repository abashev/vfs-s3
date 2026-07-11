package com.github.vfss3.jdk;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

/**
 * Minimal {@link FileStore} representing an S3 bucket. S3 has no real quota/free-space API, so
 * space figures are reported as unbounded.
 */
final class S3FileStore extends FileStore {

    private final String bucket;

    S3FileStore(String bucket) {
        this.bucket = bucket;
    }

    @Override
    public String name() {
        return bucket;
    }

    @Override
    public String type() {
        return "s3";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public long getTotalSpace() {
        return Long.MAX_VALUE;
    }

    @Override
    public long getUsableSpace() {
        return Long.MAX_VALUE;
    }

    @Override
    public long getUnallocatedSpace() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        // S3FileSystemProvider.getFileAttributeView always returns null — no FileAttributeView
        // is ever exposed, so no Class<? extends FileAttributeView> can be claimed as supported
        // here. (Reading "basic" attributes is still supported, via readAttributes(Path,
        // Class<BasicFileAttributes>, ...) — that's a different, non-view-based API.)
        return false;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        return "basic".equals(name);
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        throw new UnsupportedOperationException(attribute);
    }
}
