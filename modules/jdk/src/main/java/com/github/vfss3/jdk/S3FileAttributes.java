package com.github.vfss3.jdk;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

/** Minimal {@link BasicFileAttributes} backed by an S3 object's {@code HeadObject} response. */
final class S3FileAttributes implements BasicFileAttributes {

    private final long size;
    private final FileTime lastModifiedTime;
    private final boolean directory;

    private S3FileAttributes(long size, FileTime lastModifiedTime, boolean directory) {
        this.size = size;
        this.lastModifiedTime = lastModifiedTime;
        this.directory = directory;
    }

    static S3FileAttributes file(long size, Instant lastModified) {
        return new S3FileAttributes(size, FileTime.from(lastModified), false);
    }

    static S3FileAttributes directory() {
        return new S3FileAttributes(0, FileTime.fromMillis(0), true);
    }

    @Override
    public FileTime lastModifiedTime() {
        return lastModifiedTime;
    }

    @Override
    public FileTime lastAccessTime() {
        return lastModifiedTime;
    }

    @Override
    public FileTime creationTime() {
        return lastModifiedTime;
    }

    @Override
    public boolean isRegularFile() {
        return !directory;
    }

    @Override
    public boolean isDirectory() {
        return directory;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Object fileKey() {
        return null;
    }
}
