package com.github.vfss3.jdk;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@link FileSystem} implementation for an S3 bucket.
 *
 * <p>Obtained via:
 * <pre>{@code
 * FileSystem fs = FileSystems.newFileSystem(URI.create("s3://my-bucket"), Map.of());
 * }</pre>
 *
 * <p>Initially backed by a local-tmp mock — all S3 paths are stored under a temporary
 * directory as {@code {tmpDir}/{key}}. Real S3 backend will be added in a follow-up.
 */
public class S3FileSystem extends FileSystem {

    private final S3FileSystemProvider provider;
    private final String bucket;
    private final Path tmpDir;
    private volatile boolean open = true;

    S3FileSystem(S3FileSystemProvider provider, URI uri, Map<String, ?> env) throws IOException {
        this.provider = provider;
        this.bucket = uri.getHost();
        this.tmpDir = Files.createTempDirectory("vfs-s3-mock-" + bucket + "-");
    }

    /** Returns the bucket name this file system is bound to. */
    public String getBucket() {
        return bucket;
    }

    /**
     * Maps an {@link S3Path} to a local {@link Path} under the mock temp directory.
     *
     * <p>{@code s3://my-bucket/path/to/file.txt} → {@code {tmpDir}/path/to/file.txt}
     */
    Path toLocalPath(S3Path s3Path) {
        var key = s3Path.toString();
        // Strip leading slash so Path.resolve works correctly
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        return key.isEmpty() ? tmpDir : tmpDir.resolve(key);
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        open = false;
        deleteRecursively(tmpDir);
    }

    private static void deleteRecursively(Path root) {
        try {
            if (!Files.exists(root)) {
                return;
            }
            try (var walk = Files.walk(root)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort cleanup
                    }
                });
            }
        } catch (IOException ignored) {
            // best-effort cleanup
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3FileSystem other)) return false;
        return Objects.equals(bucket, other.bucket) && Objects.equals(tmpDir, other.tmpDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, tmpDir);
    }

    @Override
    public String toString() {
        return "S3FileSystem[s3://" + bucket + "]";
    }
}
