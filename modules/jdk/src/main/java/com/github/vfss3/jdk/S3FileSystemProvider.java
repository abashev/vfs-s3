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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
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
 *
 * <p>{@link #copy} is not yet implemented — it lands together with {@code S3Path.relativize()}.
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
        var key = fs.toKey(s3Path);
        var client = fs.client();

        boolean write = options.contains(StandardOpenOption.WRITE)
                || options.contains(StandardOpenOption.CREATE)
                || options.contains(StandardOpenOption.CREATE_NEW)
                || options.contains(StandardOpenOption.APPEND);

        if (!write) {
            return new S3ByteChannel(readObject(client, fs.getBucket(), key), client, fs.getBucket(), key, false);
        }

        boolean createNew = options.contains(StandardOpenOption.CREATE_NEW);
        boolean append = options.contains(StandardOpenOption.APPEND);
        boolean existsAsFile = objectExists(client, fs.getBucket(), key);

        if (createNew && pathExists(client, fs.getBucket(), key)) {
            throw new FileAlreadyExistsException(path.toString());
        }
        if (!existsAsFile && !options.contains(StandardOpenOption.CREATE) && !createNew) {
            throw new NoSuchFileException(path.toString());
        }

        var initial = (append && existsAsFile) ? readObject(client, fs.getBucket(), key) : new byte[0];
        var channel = new S3ByteChannel(initial, client, fs.getBucket(), key, true);
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
        var prefix = folderPrefix(fs.toKey(s3Path));

        var commonPrefixes = new TreeSet<String>();
        var contentKeys = new TreeSet<String>();
        String continuationToken = null;
        do {
            var listing = client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(fs.getBucket())
                    .delimiter("/")
                    .prefix(prefix)
                    .continuationToken(continuationToken)
                    .build());
            listing.commonPrefixes().forEach(p -> commonPrefixes.add(p.prefix()));
            listing.contents().forEach(o -> contentKeys.add(o.key()));
            continuationToken = Boolean.TRUE.equals(listing.isTruncated()) ? listing.nextContinuationToken() : null;
        } while (continuationToken != null);

        var children = new ArrayList<Path>();
        for (var commonPrefix : commonPrefixes) {
            var name = commonPrefix.substring(prefix.length());
            name = name.endsWith("/") ? name.substring(0, name.length() - 1) : name;
            if (!name.isEmpty()) {
                children.add(dir.resolve(name));
            }
        }
        for (var contentKey : contentKeys) {
            if (!contentKey.equals(prefix)) {
                var name = contentKey.substring(prefix.length());
                if (!name.isEmpty()) {
                    children.add(dir.resolve(name));
                }
            }
        }

        var filtered = new ArrayList<Path>();
        for (var child : children) {
            if (filter.accept(child)) {
                filtered.add(child);
            }
        }
        return new DirectoryStream<>() {
            @Override
            public java.util.Iterator<Path> iterator() {
                return filtered.iterator();
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        var s3Path = asS3Path(dir);
        var fs = s3Path.getFileSystem();
        var key = fs.toKey(s3Path);
        if (pathExists(fs.client(), fs.getBucket(), key)) {
            throw new FileAlreadyExistsException(dir.toString());
        }
        fs.client()
                .putObject(
                        PutObjectRequest.builder()
                                .bucket(fs.getBucket())
                                .key(folderPrefix(key))
                                .build(),
                        RequestBody.empty());
    }

    @Override
    public void delete(Path path) throws IOException {
        var s3Path = asS3Path(path);
        var fs = s3Path.getFileSystem();
        var client = fs.client();
        var key = fs.toKey(s3Path);

        if (objectExists(client, fs.getBucket(), key)) {
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(fs.getBucket())
                    .key(key)
                    .build());
            return;
        }
        var markerKey = folderPrefix(key);
        if (objectExists(client, fs.getBucket(), markerKey)) {
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(fs.getBucket())
                    .key(markerKey)
                    .build());
            return;
        }
        throw new NoSuchFileException(path.toString());
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        var src = asS3Path(source);
        var dst = asS3Path(target);
        var fs = src.getFileSystem();
        var client = fs.client();
        var srcKey = fs.toKey(src);
        var dstKey = fs.toKey(dst);

        var attrs = lookupAttributes(client, fs.getBucket(), srcKey)
                .orElseThrow(() -> new NoSuchFileException(source.toString()));

        if (attrs.isDirectory()) {
            // Per Files.copy's contract: copying a directory creates an empty directory at the
            // target — entries are not copied (that's the caller's job, e.g. via Files.walk).
            if (objectExists(client, fs.getBucket(), folderPrefix(srcKey))) {
                client.putObject(
                        PutObjectRequest.builder()
                                .bucket(fs.getBucket())
                                .key(folderPrefix(dstKey))
                                .build(),
                        RequestBody.empty());
            }
            return;
        }

        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)
                && pathExists(client, fs.getBucket(), dstKey)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        client.copyObject(CopyObjectRequest.builder()
                .sourceBucket(fs.getBucket())
                .sourceKey(srcKey)
                .destinationBucket(fs.getBucket())
                .destinationKey(dstKey)
                .build());
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        var src = asS3Path(source);
        var dst = asS3Path(target);
        var fs = src.getFileSystem();
        if (src.equals(dst)) {
            throw new FileSystemException(source.toString(), target.toString(), "Source and target are the same path");
        }
        var srcKey = fs.toKey(src);
        var dstKey = fs.toKey(dst);
        if (!objectExists(fs.client(), fs.getBucket(), srcKey)) {
            throw new NoSuchFileException(source.toString());
        }
        fs.client()
                .copyObject(CopyObjectRequest.builder()
                        .sourceBucket(fs.getBucket())
                        .sourceKey(srcKey)
                        .destinationBucket(fs.getBucket())
                        .destinationKey(dstKey)
                        .build());
        fs.client()
                .deleteObject(DeleteObjectRequest.builder()
                        .bucket(fs.getBucket())
                        .key(srcKey)
                        .build());
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

        var key = fs.toKey(s3Path);
        return (A) lookupAttributes(fs.client(), fs.getBucket(), key)
                .orElseThrow(() -> new NoSuchFileException(path.toString()));
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
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
        try (var response = client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build())) {
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

    private static boolean objectExists(S3Client client, String bucket, String key) {
        try {
            client.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    /** {@code key} with a trailing {@code /} — the S3 folder-marker convention for {@code key}. */
    private static String folderPrefix(String key) {
        return key.isEmpty() || key.endsWith("/") ? key : key + "/";
    }

    private static boolean pathExists(S3Client client, String bucket, String key) {
        return lookupAttributes(client, bucket, key).isPresent();
    }

    /**
     * Resolves {@code key} to its attributes, trying — in order — a real file object, a real
     * folder-marker object ({@code key/}), and finally a "virtual" folder (any object found
     * under the {@code key/} prefix). Mirrors {@code modules/commons-vfs}'s
     * {@code S3FileObject.doAttach()} cascade.
     */
    private static Optional<S3FileAttributes> lookupAttributes(S3Client client, String bucket, String key) {
        try {
            var head = client.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return Optional.of(S3FileAttributes.file(head.contentLength(), head.lastModified()));
        } catch (NoSuchKeyException e) {
            // fall through to folder lookup
        } catch (S3Exception e) {
            if (e.statusCode() != 404) {
                throw e;
            }
        }

        var markerKey = folderPrefix(key);
        if (objectExists(client, bucket, markerKey)) {
            return Optional.of(S3FileAttributes.directory());
        }

        var listing = client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(markerKey)
                .maxKeys(1)
                .build());
        if (!listing.contents().isEmpty()) {
            return Optional.of(S3FileAttributes.directory());
        }
        return Optional.empty();
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
                var content = Arrays.copyOf(data, size);
                client.putObject(
                        PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromBytes(content));
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
