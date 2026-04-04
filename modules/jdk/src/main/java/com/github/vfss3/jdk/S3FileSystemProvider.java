package com.github.vfss3.jdk;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
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
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        var local = localPath(path);
        // Ensure parent directories exist when opening for write
        if (options.stream()
                .anyMatch(o -> o == StandardOpenOption.WRITE
                        || o == StandardOpenOption.CREATE
                        || o == StandardOpenOption.CREATE_NEW)) {
            var parent = local.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
        return Files.newByteChannel(local, options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException {
        var localDir = localPath(dir);
        var s3Fs = s3FileSystem(dir);
        var s3DirPath = dir.toString();
        var prefix = s3DirPath.endsWith("/") ? s3DirPath : s3DirPath + "/";
        var delegate = Files.newDirectoryStream(localDir, entry -> {
            var relative = localDir.relativize(entry).toString();
            var s3Entry = s3Fs.getPath(prefix + relative);
            return filter.accept(s3Entry);
        });
        return new DirectoryStream<>() {
            @Override
            public Iterator<Path> iterator() {
                return new Iterator<>() {
                    private final Iterator<Path> inner = delegate.iterator();

                    @Override
                    public boolean hasNext() {
                        return inner.hasNext();
                    }

                    @Override
                    public Path next() {
                        var localEntry = inner.next();
                        var relative = localDir.relativize(localEntry).toString();
                        return s3Fs.getPath(prefix + relative);
                    }
                };
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        var local = localPath(dir);
        if (Files.isRegularFile(local)) {
            throw new FileAlreadyExistsException(dir.toString(), null, "Path exists as a regular file");
        }
        Files.createDirectories(local);
    }

    @Override
    public void delete(Path path) throws IOException {
        var local = localPath(path);
        Files.delete(local);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        Files.copy(localPath(source), localPath(target), options);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        var localSource = localPath(source);
        var localTarget = localPath(target);
        if (localSource.equals(localTarget)) {
            throw new FileSystemException(source.toString(), target.toString(), "Source and target are the same path");
        }
        var parent = localTarget.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.move(localSource, localTarget, options);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return localPath(path).equals(localPath(path2));
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
        var local = localPath(path);
        if (!Files.exists(local)) {
            throw new NoSuchFileException(path.toString());
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        // BasicFileAttributeView intentionally not exposed — setTimes is not supported
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException {
        if (!BasicFileAttributes.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException("Only BasicFileAttributes is supported");
        }
        var local = localPath(path);
        if (!Files.exists(local)) {
            throw new NoSuchFileException(path.toString());
        }
        return (A) Files.readAttributes(local, BasicFileAttributes.class, options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        var local = localPath(path);
        return Files.readAttributes(local, attributes, options);
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Setting file attributes is not supported for S3 resources");
    }

    // ---- helpers ----

    private static Path localPath(Path path) {
        if (!(path instanceof S3Path s3Path)) {
            throw new IllegalArgumentException("Expected S3Path, got: " + path.getClass());
        }
        return s3Path.getFileSystem().toLocalPath(s3Path);
    }

    private static S3FileSystem s3FileSystem(Path path) {
        if (!(path instanceof S3Path s3Path)) {
            throw new IllegalArgumentException("Expected S3Path, got: " + path.getClass());
        }
        return s3Path.getFileSystem();
    }

    private static void checkUri(URI uri) {
        if (!SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("URI scheme must be '" + SCHEME + "': " + uri);
        }
    }
}
