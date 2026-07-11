package com.github.vfss3.jdk;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * {@link FileSystemProvider} implementation for {@code s3://} URIs, backed by a real
 * {@link S3Client}.
 *
 * <p>Registered via {@code META-INF/services/java.nio.file.spi.FileSystemProvider}. Usage:
 *
 * <pre>{@code
 * FileSystem fs = FileSystems.newFileSystem(URI.create("s3://my-bucket"), Map.of());
 * }</pre>
 */
public class S3FileSystemProvider extends FileSystemProvider {

    static final String SCHEME = "s3";

    private final Map<String, S3FileSystem> openFileSystems = new ConcurrentHashMap<>();

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
        checkUri(uri);
        var bucket = uri.getHost();
        var fs = new S3FileSystem(this, uri, env);
        if (openFileSystems.putIfAbsent(bucket, fs) != null) {
            throw new FileSystemAlreadyExistsException("s3://" + bucket);
        }
        return fs;
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        checkUri(uri);
        var fs = openFileSystems.get(uri.getHost());
        if (fs == null) {
            throw new FileSystemNotFoundException("s3://" + uri.getHost());
        }
        return fs;
    }

    @Override
    public Path getPath(URI uri) {
        checkUri(uri);
        var fs = (S3FileSystem) getFileSystem(uri);
        var path = uri.getPath();
        return fs.getPath(path == null || path.isEmpty() ? "/" : path);
    }

    /** Called from {@link S3FileSystem#close()} so a closed bucket can be reopened. */
    void unregister(String bucket) {
        openFileSystems.remove(bucket);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        var s3Path = asS3Path(path);
        var fs = s3Path.getFileSystem();
        var bucket = fs.getBucket();
        var key = fs.toKey(s3Path);
        var client = fs.client();

        boolean write = options.contains(StandardOpenOption.WRITE)
                || options.contains(StandardOpenOption.CREATE)
                || options.contains(StandardOpenOption.CREATE_NEW)
                || options.contains(StandardOpenOption.APPEND);

        if (!write) {
            return new S3ByteChannel(readObject(client, bucket, key), client, bucket, key, false);
        }

        boolean create = options.contains(StandardOpenOption.CREATE);
        boolean createNew = options.contains(StandardOpenOption.CREATE_NEW);
        boolean append = options.contains(StandardOpenOption.APPEND);

        if (createNew && pathExists(client, bucket, key)) {
            throw new FileAlreadyExistsException(path.toString());
        }
        if (!create && !createNew && !objectExists(client, bucket, key)) {
            throw new NoSuchFileException(path.toString());
        }

        var initial = append ? readObjectOrEmpty(client, bucket, key) : new byte[0];
        var channel = new S3ByteChannel(initial, client, bucket, key, true);
        if (append) {
            channel.position(initial.length);
        }
        return channel;
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException {
        var s3Path = asS3Path(dir);
        var fs = s3Path.getFileSystem();
        var client = fs.client();
        var bucket = fs.getBucket();
        var prefix = folderPrefix(fs.toKey(s3Path));

        // TreeSets keep the entries ordered and deduplicated even if a backend repeats
        // them across pages.
        var commonPrefixes = new TreeSet<String>();
        var contentKeys = new TreeSet<String>();
        for (var page : client.listObjectsV2Paginator(
                b -> b.bucket(bucket).delimiter("/").prefix(prefix))) {
            page.commonPrefixes().forEach(p -> commonPrefixes.add(p.prefix()));
            page.contents().forEach(o -> contentKeys.add(o.key()));
        }

        var children = new ArrayList<Path>();
        for (var commonPrefix : commonPrefixes) {
            var name = commonPrefix.substring(prefix.length());
            name = name.endsWith("/") ? name.substring(0, name.length() - 1) : name;
            if (name.isEmpty()) {
                continue;
            }
            var child = dir.resolve(name);
            if (filter.accept(child)) {
                children.add(child);
            }
        }
        for (var contentKey : contentKeys) {
            if (contentKey.equals(prefix)) {
                continue;
            }
            var name = contentKey.substring(prefix.length());
            if (name.isEmpty()) {
                continue;
            }
            var child = dir.resolve(name);
            if (filter.accept(child)) {
                children.add(child);
            }
        }
        return new DirectoryStream<>() {
            @Override
            public Iterator<Path> iterator() {
                return children.iterator();
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        var s3Path = asS3Path(dir);
        var fs = s3Path.getFileSystem();
        var bucket = fs.getBucket();
        var key = fs.toKey(s3Path);
        if (pathExists(fs.client(), bucket, key)) {
            throw new FileAlreadyExistsException(dir.toString());
        }
        fs.client().putObject(b -> b.bucket(bucket).key(folderPrefix(key)), RequestBody.empty());
    }

    @Override
    public void delete(Path path) throws IOException {
        var s3Path = asS3Path(path);
        var fs = s3Path.getFileSystem();
        var client = fs.client();
        var bucket = fs.getBucket();
        var key = fs.toKey(s3Path);

        switch (resolve(client, bucket, key).kind()) {
            case FILE -> client.deleteObject(b -> b.bucket(bucket).key(key));
            case MARKER_DIR -> client.deleteObject(b -> b.bucket(bucket).key(folderPrefix(key)));
            // A virtual folder has no object of its own to delete — same outcome as a
            // missing path.
            case VIRTUAL_DIR, ABSENT -> throw new NoSuchFileException(path.toString());
        }
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        var src = asS3Path(source);
        var dst = asS3Path(target);
        var fs = src.getFileSystem();
        var client = fs.client();
        var bucket = fs.getBucket();
        var srcKey = fs.toKey(src);
        var dstKey = fs.toKey(dst);

        var srcResolution = resolve(client, bucket, srcKey);
        if (!srcResolution.exists()) {
            throw new NoSuchFileException(source.toString());
        }

        if (srcResolution.attributes().isDirectory()) {
            // Per Files.copy's contract: copying a directory creates an empty directory at the
            // target — entries are not copied (that's the caller's job, e.g. via Files.walk).
            // A virtual folder has no marker object of its own, so there is nothing to create.
            if (srcResolution.kind() == PathKind.MARKER_DIR) {
                client.putObject(b -> b.bucket(bucket).key(folderPrefix(dstKey)), RequestBody.empty());
            }
            return;
        }

        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)
                && pathExists(client, bucket, dstKey)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        client.copyObject(b -> b.sourceBucket(bucket)
                .sourceKey(srcKey)
                .destinationBucket(bucket)
                .destinationKey(dstKey));
    }

    /**
     * Implemented as server-side copy followed by delete — not an atomic rename. An error
     * between the two calls can leave both the source and the destination present.
     */
    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        var src = asS3Path(source);
        var dst = asS3Path(target);
        var fs = src.getFileSystem();
        var client = fs.client();
        var bucket = fs.getBucket();
        if (src.equals(dst)) {
            throw new FileSystemException(source.toString(), target.toString(), "Source and target are the same path");
        }
        var srcKey = fs.toKey(src);
        var dstKey = fs.toKey(dst);
        if (!objectExists(client, bucket, srcKey)) {
            throw new NoSuchFileException(source.toString());
        }
        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)
                && pathExists(client, bucket, dstKey)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        client.copyObject(b -> b.sourceBucket(bucket)
                .sourceKey(srcKey)
                .destinationBucket(bucket)
                .destinationKey(dstKey));
        client.deleteObject(b -> b.bucket(bucket).key(srcKey));
    }

    @Override
    public boolean isSameFile(Path path, Path path2) {
        return asS3Path(path).equals(asS3Path(path2));
    }

    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) {
        return new S3FileStore(asS3Path(path).getFileSystem().getBucket());
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        var s3Path = asS3Path(path);
        var fs = s3Path.getFileSystem();
        if (isRoot(s3Path)) {
            return;
        }
        if (!pathExists(fs.client(), fs.getBucket(), fs.toKey(s3Path))) {
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
        var s3Path = asS3Path(path);
        var fs = s3Path.getFileSystem();

        if (isRoot(s3Path)) {
            return (A) S3FileAttributes.directory();
        }

        var resolution = resolve(fs.client(), fs.getBucket(), fs.toKey(s3Path));
        if (!resolution.exists()) {
            throw new NoSuchFileException(path.toString());
        }
        return (A) resolution.attributes();
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        // The attribute selector is deliberately ignored — the four basic attributes below
        // are all this provider supports.
        var attrs = readAttributes(path, BasicFileAttributes.class, options);
        return Map.of(
                "size", attrs.size(),
                "lastModifiedTime", attrs.lastModifiedTime(),
                "isDirectory", attrs.isDirectory(),
                "isRegularFile", attrs.isRegularFile());
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {
        throw new UnsupportedOperationException("Setting file attributes is not supported for S3 resources");
    }

    // ---- helpers ----

    private static S3Path asS3Path(Path path) {
        if (!(path instanceof S3Path s3Path)) {
            throw new IllegalArgumentException("Expected S3Path, got: " + path.getClass());
        }
        return s3Path;
    }

    private static boolean isRoot(S3Path path) {
        var s = path.toString();
        return s.isEmpty() || s.equals("/");
    }

    private static byte[] readObject(S3Client client, String bucket, String key) throws IOException {
        try (var response = client.getObject(b -> b.bucket(bucket).key(key))) {
            return response.readAllBytes();
        } catch (NoSuchKeyException e) {
            throw new NoSuchFileException(key);
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new NoSuchFileException(key);
            }
            throw new IOException("Failed to read s3://" + bucket + "/" + key, e);
        }
    }

    /** Like {@link #readObject}, but a missing object reads as empty — how APPEND starts a new file. */
    private static byte[] readObjectOrEmpty(S3Client client, String bucket, String key) throws IOException {
        try {
            return readObject(client, bucket, key);
        } catch (NoSuchFileException e) {
            return new byte[0];
        }
    }

    /** HEAD on the exact {@code key}, empty when S3 reports 404/NoSuchKey. */
    private static Optional<HeadObjectResponse> tryHead(S3Client client, String bucket, String key) {
        try {
            return Optional.of(client.headObject(b -> b.bucket(bucket).key(key)));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /**
     * Whether a real object exists at exactly {@code key} — no folder-marker or virtual-folder
     * fallback; contrast {@link #resolve}.
     */
    private static boolean objectExists(S3Client client, String bucket, String key) {
        return tryHead(client, bucket, key).isPresent();
    }

    /** {@code key} with a trailing {@code /} — the S3 folder-marker convention for {@code key}. */
    private static String folderPrefix(String key) {
        return key.isEmpty() || key.endsWith("/") ? key : key + "/";
    }

    /** Whether {@code key} resolves to anything at all — a file or a (marker/virtual) folder. */
    private static boolean pathExists(S3Client client, String bucket, String key) {
        return resolve(client, bucket, key).exists();
    }

    /** What an S3 key resolves to — see {@link #resolve}. */
    private enum PathKind {
        /** A real object at exactly the key. */
        FILE,
        /** A real zero-byte folder-marker object at {@code key + "/"}. */
        MARKER_DIR,
        /** No object at the key or its marker, but objects exist under the {@code key/} prefix. */
        VIRTUAL_DIR,
        ABSENT
    }

    /** Result of {@link #resolve}; {@code attributes} is {@code null} iff the path is absent. */
    private record Resolution(PathKind kind, S3FileAttributes attributes) {
        boolean exists() {
            return kind != PathKind.ABSENT;
        }
    }

    /**
     * Resolves what {@code key} points at, trying — in order — a real file object at the key
     * itself, a real folder-marker object ({@code key/}), and finally a "virtual" folder (any
     * object found under the {@code key/} prefix). Mirrors {@code modules/commons-vfs}'s
     * {@code S3FileObject.doAttach()} cascade.
     *
     * <p>The marker probe must stay a HEAD rather than being folded into the prefix listing:
     * some backends (SeaweedFS) expose directory entries to HEAD but return an empty listing
     * for the same prefix.
     */
    private static Resolution resolve(S3Client client, String bucket, String key) {
        var head = tryHead(client, bucket, key);
        if (head.isPresent()) {
            return new Resolution(
                    PathKind.FILE,
                    S3FileAttributes.file(head.get().contentLength(), head.get().lastModified()));
        }

        var markerKey = folderPrefix(key);
        if (tryHead(client, bucket, markerKey).isPresent()) {
            return new Resolution(PathKind.MARKER_DIR, S3FileAttributes.directory());
        }

        var listing =
                client.listObjectsV2(b -> b.bucket(bucket).prefix(markerKey).maxKeys(1));
        if (listing.contents().isEmpty()) {
            return new Resolution(PathKind.ABSENT, null);
        }
        return new Resolution(PathKind.VIRTUAL_DIR, S3FileAttributes.directory());
    }

    /**
     * In-memory {@link SeekableByteChannel} for a single S3 object. Buffers the whole object in
     * memory — acceptable for this module's scope (test-sized payloads); a true streaming
     * multipart upload is a possible future enhancement, not required by any described scenario.
     */
    private static final class S3ByteChannel implements SeekableByteChannel {
        private byte[] data;
        private int size;
        private long position;
        private boolean open = true;
        private final S3Client client;
        private final String bucket;
        private final String key;
        private final boolean writable;

        S3ByteChannel(byte[] initial, S3Client client, String bucket, String key, boolean writable) {
            this.data = initial;
            this.size = initial.length;
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.writable = writable;
        }

        @Override
        public synchronized int read(ByteBuffer dst) throws IOException {
            ensureOpen();
            if (position >= size) {
                return -1;
            }
            int n = Math.min(dst.remaining(), (int) (size - position));
            dst.put(data, (int) position, n);
            position += n;
            return n;
        }

        @Override
        public synchronized int write(ByteBuffer src) throws IOException {
            ensureOpen();
            if (!writable) {
                throw new NonWritableChannelException();
            }
            int n = src.remaining();
            if (position > Integer.MAX_VALUE - n) {
                // The whole object is buffered in memory as a single byte[] (see class
                // javadoc) — fail loudly here rather than silently truncating/wrapping the
                // int arithmetic below once an object would exceed array-size limits.
                throw new IOException("Object exceeds the " + Integer.MAX_VALUE + "-byte in-memory buffer limit");
            }
            ensureCapacity((int) position + n);
            src.get(data, (int) position, n);
            position += n;
            size = (int) Math.max(size, position);
            return n;
        }

        @Override
        public synchronized long position() throws IOException {
            ensureOpen();
            return position;
        }

        @Override
        public synchronized SeekableByteChannel position(long newPosition) throws IOException {
            ensureOpen();
            this.position = newPosition;
            return this;
        }

        @Override
        public synchronized long size() throws IOException {
            ensureOpen();
            return size;
        }

        @Override
        public synchronized SeekableByteChannel truncate(long newSize) throws IOException {
            ensureOpen();
            size = (int) Math.min(size, newSize);
            if (position > size) {
                position = size;
            }
            return this;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public synchronized void close() {
            if (!open) {
                return;
            }
            open = false;
            if (writable) {
                // RequestBody copies the buffer's [0, size) window, so the live array never
                // escapes the channel.
                client.putObject(
                        b -> b.bucket(bucket).key(key), RequestBody.fromByteBuffer(ByteBuffer.wrap(data, 0, size)));
            }
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity > data.length) {
                data = Arrays.copyOf(data, Math.max(minCapacity, data.length * 2 + 16));
            }
        }

        private void ensureOpen() throws ClosedChannelException {
            if (!open) {
                throw new ClosedChannelException();
            }
        }
    }

    private static void checkUri(URI uri) {
        if (!SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("URI scheme must be '" + SCHEME + "': " + uri);
        }
    }
}
