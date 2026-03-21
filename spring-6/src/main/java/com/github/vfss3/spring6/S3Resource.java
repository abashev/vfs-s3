package com.github.vfss3.spring6;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Objects;
import org.springframework.core.io.Resource;

/**
 * Spring {@link Resource} implementation for {@code s3://bucket/key} URIs.
 *
 * <p>Initially backed by a local-tmp mock; real S3 backend will be added in a follow-up.
 *
 * <p>Usage:
 * <pre>{@code
 * Resource resource = new S3Resource("s3://my-bucket/path/to/object.txt");
 * InputStream stream = resource.getInputStream();
 * }</pre>
 */
public class S3Resource implements Resource {

    static final String S3_SCHEME = "s3";

    private final URI uri;

    /**
     * Creates a new {@code S3Resource} from a {@code s3://} URI string.
     *
     * @param uri the S3 URI, e.g. {@code s3://my-bucket/path/to/object.txt}
     * @throws IllegalArgumentException if the URI scheme is not {@code s3}
     */
    public S3Resource(String uri) {
        this(URI.create(uri));
    }

    /**
     * Creates a new {@code S3Resource} from a {@link URI}.
     *
     * @param uri the S3 URI
     * @throws IllegalArgumentException if the URI scheme is not {@code s3}
     */
    public S3Resource(URI uri) {
        if (!S3_SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("URI scheme must be 's3': " + uri);
        }
        this.uri = uri;
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
        return false;
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
        throw new IOException("Not yet implemented");
    }

    @Override
    public long lastModified() throws IOException {
        throw new IOException("Not yet implemented");
    }

    @Override
    public Resource createRelative(String relativePath) throws IOException {
        var base = uri.toString();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        return new S3Resource(base + relativePath);
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
        throw new IOException("Not yet implemented — real S3 backend pending");
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
}
