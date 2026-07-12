package com.github.vfss3.spring;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.springframework.core.io.WritableResource;

/**
 * Spring {@link WritableResource} implementation for {@code s3://bucket/key} URIs.
 *
 * <p>Initially backed by a local-tmp mock — all S3 paths are stored under a temporary
 * directory as {@code {tmpDir}/bucket/key}. Real S3 backend will be added in a follow-up.
 *
 * <p>Usage (via loader):
 * <pre>{@code
 * S3ResourceLoader loader = new S3ResourceLoader();
 * WritableResource resource = (WritableResource) loader.getResource("s3://my-bucket/path/to/object.txt");
 * try (OutputStream out = resource.getOutputStream()) {
 *     out.write("Hello".getBytes());
 * }
 * try (InputStream in = resource.getInputStream()) {
 *     // read back
 * }
 * }</pre>
 */
public class S3Resource implements WritableResource {

    static final String S3_SCHEME = "s3";

    private final URI uri;

    /**
     * Local path for mock backend. When {@code null}, I/O operations throw; {@link #exists()}
     * returns {@code false}.
     */
    private final Path localPath;

    /**
     * Creates a new {@code S3Resource} from a {@code s3://} URI string (no mock backend).
     *
     * @param uri the S3 URI, e.g. {@code s3://my-bucket/path/to/object.txt}
     * @throws IllegalArgumentException if the URI scheme is not {@code s3}
     */
    public S3Resource(String uri) {
        this(URI.create(uri), null);
    }

    /**
     * Creates a new {@code S3Resource} from a {@link URI} (no mock backend).
     *
     * @param uri the S3 URI
     * @throws IllegalArgumentException if the URI scheme is not {@code s3}
     */
    public S3Resource(URI uri) {
        this(uri, null);
    }

    /**
     * Creates a new {@code S3Resource} backed by a local file for mock I/O.
     *
     * @param uri       the S3 URI
     * @param localPath the local path where S3 data is stored (mock mode); may be {@code null}
     * @throws IllegalArgumentException if the URI scheme is not {@code s3}
     */
    S3Resource(URI uri, Path localPath) {
        if (!S3_SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("URI scheme must be 's3': " + uri);
        }
        this.uri = uri;
        this.localPath = localPath;
    }

    /** Returns the bucket extracted from the URI host. */
    public String getBucket() {
        return uri.getHost();
    }

    /** Returns the object key extracted from the URI path (leading slash removed). */
    public String getKey() {
        var path = uri.getPath();
        return path != null && path.startsWith("/") ? path.substring(1) : path;
    }

    @Override
    public boolean exists() {
        if (localPath == null) {
            return false;
        }
        return Files.exists(localPath);
    }

    @Override
    public URL getURL() throws IOException {
        return uri.toURL();
    }

    @Override
    public URI getURI() throws IOException {
        return uri;
    }

    @Override
    public File getFile() throws IOException {
        throw new IOException("S3 resources cannot be resolved as java.io.File");
    }

    @Override
    public long contentLength() throws IOException {
        requireLocalPath("contentLength");
        return Files.size(localPath);
    }

    @Override
    public long lastModified() throws IOException {
        requireLocalPath("lastModified");
        return Files.getLastModifiedTime(localPath).toMillis();
    }

    @Override
    public S3Resource createRelative(String relativePath) throws IOException {
        var base = uri.toString();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        return new S3Resource(URI.create(base + relativePath), null);
    }

    @Override
    public String getFilename() {
        var path = uri.getPath();
        if (path == null || path.isEmpty()) {
            return null;
        }
        var idx = path.lastIndexOf('/');
        return idx < 0 ? path : path.substring(idx + 1);
    }

    @Override
    public String getDescription() {
        return "S3 resource [" + uri + "]";
    }

    @Override
    public InputStream getInputStream() throws IOException {
        requireLocalPath("getInputStream");
        if (!Files.exists(localPath)) {
            throw new IOException("S3 resource does not exist: " + uri);
        }
        return Files.newInputStream(localPath);
    }

    @Override
    public boolean isWritable() {
        return localPath != null;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        requireLocalPath("getOutputStream");
        var parent = localPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return Files.newOutputStream(localPath);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3Resource other)) return false;
        return Objects.equals(uri, other.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uri);
    }

    @Override
    public String toString() {
        return getDescription();
    }

    private void requireLocalPath(String operation) throws IOException {
        if (localPath == null) {
            throw new IOException("Cannot perform " + operation + " without a mock backend — real S3 backend pending");
        }
    }
}
