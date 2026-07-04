package com.github.vfss3.jdk;

import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * {@link FileSystem} implementation for an S3 bucket, backed by a real {@link S3Client}.
 *
 * <p>Obtained via:
 *
 * <pre>{@code
 * FileSystem fs = FileSystems.newFileSystem(URI.create("s3://my-bucket"), env);
 * }</pre>
 *
 * See {@link S3FileSystemConfig} for what {@code env} accepts.
 */
public class S3FileSystem extends FileSystem {

    private final S3FileSystemProvider provider;
    private final String bucket;
    private final S3Client client;
    private volatile boolean open = true;

    S3FileSystem(S3FileSystemProvider provider, URI uri, Map<String, ?> env) {
        this.provider = provider;
        this.bucket = uri.getHost();
        this.client = S3FileSystemConfig.fromEnv(env).buildS3Client();
    }

    /** Returns the bucket name this file system is bound to. */
    public String getBucket() {
        return bucket;
    }

    /** The {@link S3Client} backing this file system. */
    S3Client client() {
        return client;
    }

    /** Maps an {@link S3Path} to its S3 object key (no leading slash). */
    String toKey(S3Path s3Path) {
        var key = s3Path.toString();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        // Always normalized without a trailing slash, even if the path string has one (e.g.
        // fs.getPath("/copy/")) — otherwise the plain-file key and the folder-marker key
        // (S3FileSystemProvider.folderPrefix) would collide for such paths.
        while (key.endsWith("/")) {
            key = key.substring(0, key.length() - 1);
        }
        return key;
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() {
        open = false;
        client.close();
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
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("S3 paths do not support WatchService");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3FileSystem other)) return false;
        return Objects.equals(bucket, other.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket);
    }

    @Override
    public String toString() {
        return "S3FileSystem[s3://" + bucket + "]";
    }
}
