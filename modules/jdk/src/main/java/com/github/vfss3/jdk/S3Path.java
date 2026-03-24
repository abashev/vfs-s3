package com.github.vfss3.jdk;

import java.io.File;
import java.net.URI;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * {@link Path} implementation for S3 object keys within an {@link S3FileSystem}.
 *
 * <p>Paths follow Unix conventions using {@code /} as the separator.
 */
public class S3Path implements Path {

    private final S3FileSystem fileSystem;
    private final String path;

    S3Path(S3FileSystem fileSystem, String first, String... more) {
        this.fileSystem = fileSystem;
        if (more.length == 0) {
            this.path = first;
        } else {
            var sb = new StringBuilder(first);
            for (var part : more) {
                if (!part.isEmpty()) {
                    if (!sb.isEmpty() && sb.charAt(sb.length() - 1) != '/') {
                        sb.append('/');
                    }
                    sb.append(part);
                }
            }
            this.path = sb.toString();
        }
    }

    @Override
    public S3FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return path.startsWith("/");
    }

    @Override
    public Path getRoot() {
        return isAbsolute() ? fileSystem.getPath("/") : null;
    }

    @Override
    public Path getFileName() {
        var idx = path.lastIndexOf('/');
        if (idx < 0) {
            return this;
        }
        var name = path.substring(idx + 1);
        return name.isEmpty() ? null : new S3Path(fileSystem, name);
    }

    @Override
    public Path getParent() {
        var idx = path.lastIndexOf('/');
        if (idx <= 0) {
            return null;
        }
        return new S3Path(fileSystem, path.substring(0, idx));
    }

    @Override
    public int getNameCount() {
        var stripped = path.startsWith("/") ? path.substring(1) : path;
        if (stripped.isEmpty()) {
            return 0;
        }
        return stripped.split("/").length;
    }

    @Override
    public Path getName(int index) {
        var parts = (path.startsWith("/") ? path.substring(1) : path).split("/");
        return new S3Path(fileSystem, parts[index]);
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        var parts = (path.startsWith("/") ? path.substring(1) : path).split("/");
        return new S3Path(fileSystem, String.join("/", Arrays.copyOfRange(parts, beginIndex, endIndex)));
    }

    @Override
    public boolean startsWith(Path other) {
        return path.startsWith(other.toString());
    }

    @Override
    public boolean endsWith(Path other) {
        return path.endsWith(other.toString());
    }

    @Override
    public Path normalize() {
        return this;
    }

    @Override
    public Path resolve(Path other) {
        if (other.isAbsolute()) {
            return other;
        }
        var sep = path.endsWith("/") ? "" : "/";
        return new S3Path(fileSystem, path + sep + other);
    }

    @Override
    public Path relativize(Path other) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public URI toUri() {
        return URI.create("s3://" + fileSystem.getBucket() + (path.startsWith("/") ? path : "/" + path));
    }

    @Override
    public Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }
        return new S3Path(fileSystem, "/" + path);
    }

    @Override
    public Path toRealPath(LinkOption... options) {
        return toAbsolutePath();
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException("S3 paths cannot be converted to java.io.File");
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
        throw new UnsupportedOperationException("S3 paths do not support WatchService");
    }

    @Override
    public Iterator<Path> iterator() {
        return new Iterator<>() {
            private int index = 0;
            private final int count = getNameCount();

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Path next() {
                return getName(index++);
            }
        };
    }

    @Override
    public int compareTo(Path other) {
        return path.compareTo(other.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3Path other)) return false;
        return Objects.equals(fileSystem, other.fileSystem) && Objects.equals(path, other.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSystem, path);
    }

    @Override
    public String toString() {
        return path;
    }
}
