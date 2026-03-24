package com.github.vfss3.jdk;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

/**
 * {@link FileSystem} implementation for an S3 bucket.
 *
 * <p>Obtained via:
 * <pre>{@code
 * FileSystem fs = FileSystems.newFileSystem(URI.create("s3://my-bucket"), Map.of());
 * }</pre>
 *
 * <p>Initially backed by a local-tmp mock; real S3 backend will be added in a follow-up.
 */
public class S3FileSystem extends FileSystem {

    private final S3FileSystemProvider provider;
    private final String bucket;
    private volatile boolean open = true;

    S3FileSystem(S3FileSystemProvider provider, URI uri, Map<String, ?> env) {
        this.provider = provider;
        this.bucket = uri.getHost();
    }

    /** Returns the bucket name this file system is bound to. */
    public String getBucket() {
        return bucket;
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Set.of(getPath("/"));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return Set.of();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Set.of("basic");
    }

    @Override
    public Path getPath(String first, String... more) {
        return new S3Path(this, first, more);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
