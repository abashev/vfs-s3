package com.github.vfss3.jdk;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

/**
 * {@link FileSystemProvider} implementation for {@code s3://} URIs.
 *
 * <p>Registered via {@code META-INF/services/java.nio.file.spi.FileSystemProvider}.
 * Usage:
 * <pre>{@code
 * FileSystem fs = FileSystems.newFileSystem(URI.create("s3://my-bucket"), Map.of());
 * }</pre>
 *
 * <p>Initially backed by a local-tmp mock; real S3 backend will be added in a follow-up.
 */
public class S3FileSystemProvider extends FileSystemProvider {

    static final String SCHEME = "s3";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        checkUri(uri);
        return new S3FileSystem(this, uri, env);
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        checkUri(uri);
        throw new UnsupportedOperationException("Use newFileSystem to obtain an S3FileSystem");
    }

    @Override
    public Path getPath(URI uri) {
        checkUri(uri);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public SeekableByteChannel newByteChannel(
            Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void delete(Path path) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return path.toAbsolutePath().equals(path2.toAbsolutePath());
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private static void checkUri(URI uri) {
        if (!SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("URI scheme must be '" + SCHEME + "': " + uri);
        }
    }
}
