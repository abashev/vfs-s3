package com.github.vfss3.spring;

import static java.util.Objects.requireNonNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.WritableResource;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Spring {@link WritableResource} for a single S3 object, backed by a real {@link S3Client}.
 *
 * <p>Reads stream straight from S3 ({@code GetObject} — the response stream is passed through,
 * nothing is buffered); writes spool to a local temporary file and upload the whole object with
 * one {@code PutObject} when the stream is closed. Metadata calls ({@link #exists()},
 * {@link #contentLength()}, {@link #lastModified()}) each issue a fresh {@code HeadObject} —
 * like Spring's own file-system resources, an {@code S3Resource} is a cheap, re-checkable
 * descriptor, not a stateful handle.
 *
 * <p>Instances are usually obtained from an {@link S3ResourceLoader}:
 *
 * <pre>{@code
 * try (S3ResourceLoader loader = new S3ResourceLoader()) {
 *     WritableResource resource = (WritableResource) loader.getResource("s3://my-bucket/path/to/object.txt");
 *     try (OutputStream out = resource.getOutputStream()) {
 *         out.write("Hello".getBytes());
 *     }
 *     try (InputStream in = resource.getInputStream()) {
 *         // read back
 *     }
 * }
 * }</pre>
 */
public class S3Resource extends AbstractResource implements WritableResource {

    private final URI uri;
    private final S3Client client;

    /**
     * Creates a resource for an {@code s3://bucket/key} location string backed by the given
     * client. Query configuration parameters ({@code ?region=…&endpoint=…}) are validated but
     * have no effect — the given client is used as-is.
     *
     * @param location the location, e.g. {@code s3://my-bucket/path/to/object.txt}
     * @param client the client to talk to S3 with; the caller keeps ownership
     * @throws IllegalArgumentException if the location is not a valid {@code s3://} location
     */
    public S3Resource(String location, S3Client client) {
        var parsed = S3Uris.parse(location);
        this.uri = S3Uris.toUri(parsed.bucket(), parsed.key());
        this.client = requireNonNull(client, "client");
    }

    /**
     * Creates a resource for an {@code s3://} URI backed by the given client. Any query
     * component of the URI is ignored — the given client is used as-is.
     *
     * @param uri the S3 URI, e.g. {@code s3://my-bucket/path/to/object.txt}
     * @param client the client to talk to S3 with; the caller keeps ownership
     * @throws IllegalArgumentException if the URI scheme is not {@code s3}, the bucket is
     *     missing, or the URI carries userinfo credentials
     */
    public S3Resource(URI uri, S3Client client) {
        var parsed = S3Uris.parse(requireNonNull(uri, "uri"));
        this.uri = S3Uris.toUri(parsed.bucket(), parsed.key());
        this.client = requireNonNull(client, "client");
    }

    /** The bucket name (the URI host). */
    public String getBucket() {
        return uri.getHost();
    }

    /** The object key (the URI path without its leading slash), decoded. */
    public String getKey() {
        return S3Uris.key(uri);
    }

    /**
     * Whether the object exists, via {@code HeadObject}. A 403 also reads as {@code false}: S3
     * answers 403 for present and missing objects alike when the caller lacks permission, so
     * existence cannot be confirmed. Any other failure propagates as the SDK's unchecked
     * exception rather than masquerading as a missing object.
     */
    @Override
    public boolean exists() {
        try {
            return tryHead().isPresent();
        } catch (S3Exception e) {
            if (e.statusCode() == 403) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public long contentLength() throws IOException {
        return requireHead().contentLength();
    }

    @Override
    public long lastModified() throws IOException {
        return requireHead().lastModified().toEpochMilli();
    }

    /**
     * A resource for a path relative to this one, sharing this resource's client. Follows
     * Spring's usual sibling semantics ({@link StringUtils#applyRelativePath}): relative to the
     * containing "directory" of this key, so a key not ending in {@code /} resolves siblings.
     */
    @Override
    public S3Resource createRelative(String relativePath) {
        return new S3Resource(S3Uris.toUri(getBucket(), StringUtils.applyRelativePath(getKey(), relativePath)), client);
    }

    @Nullable
    @Override
    public String getFilename() {
        var key = getKey();
        if (key.isEmpty()) {
            return null;
        }
        var idx = key.lastIndexOf('/');
        return idx < 0 ? key : key.substring(idx + 1);
    }

    @Override
    public String getDescription() {
        return "S3 resource [" + uri + "]";
    }

    /**
     * Opens the object for reading — a single {@code GetObject} whose response stream is
     * returned directly, so content is streamed rather than buffered. Close the stream to
     * release the connection.
     *
     * @throws FileNotFoundException if the object does not exist
     */
    @Override
    public InputStream getInputStream() throws IOException {
        try {
            return client.getObject(b -> b.bucket(getBucket()).key(getKey()));
        } catch (NoSuchKeyException e) {
            throw notFound(e);
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw notFound(e);
            }
            throw new IOException("Failed to open " + getDescription(), e);
        }
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    /**
     * Opens the object for writing. Bytes are spooled to a local temporary file and uploaded
     * with a single {@code PutObject} when the stream is closed — memory use stays bounded for
     * uploads of any size, and readers of the key never observe a partially written object.
     */
    @Override
    public OutputStream getOutputStream() throws IOException {
        return new S3OutputStream(client, getBucket(), getKey(), getDescription());
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other instanceof S3Resource that && uri.equals(that.uri));
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    /** HEAD on the object; empty when S3 reports 404/NoSuchKey, any other failure rethrown. */
    private Optional<HeadObjectResponse> tryHead() {
        try {
            return Optional.of(client.headObject(b -> b.bucket(getBucket()).key(getKey())));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /** HEAD on the object, mapping a missing object and S3 failures to {@link IOException}s. */
    private HeadObjectResponse requireHead() throws IOException {
        Optional<HeadObjectResponse> head;
        try {
            head = tryHead();
        } catch (S3Exception e) {
            throw new IOException("Failed to read metadata of " + getDescription(), e);
        }
        return head.orElseThrow(() -> new FileNotFoundException(getDescription() + " does not exist"));
    }

    private FileNotFoundException notFound(Throwable cause) {
        var e = new FileNotFoundException(getDescription() + " does not exist");
        e.initCause(cause);
        return e;
    }

    /**
     * Buffers writes in a local temporary file and uploads the whole object with one
     * {@code PutObject} on {@link #close()}. Close is idempotent, and the temporary file is
     * always removed — whether the upload succeeded or not.
     */
    private static final class S3OutputStream extends OutputStream {

        private final S3Client client;
        private final String bucket;
        private final String key;
        private final String description;
        private final Path tempFile;
        private final OutputStream delegate;
        private boolean closed;

        S3OutputStream(S3Client client, String bucket, String key, String description) throws IOException {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.description = description;
            this.tempFile = Files.createTempFile("vfs-spring-", ".s3");
            this.delegate = Files.newOutputStream(tempFile);
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            // Flushes the local spool only — S3 has no partial-object write; the upload
            // happens on close().
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                delegate.close();
                client.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromFile(tempFile));
            } catch (SdkException e) {
                throw new IOException("Failed to upload " + description, e);
            } finally {
                Files.deleteIfExists(tempFile);
            }
        }
    }
}
