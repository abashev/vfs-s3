package com.github.vfss3.spring6;

import static java.util.Comparator.reverseOrder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

/**
 * Spring {@link org.springframework.core.io.ResourceLoader} that resolves {@code s3://} URIs
 * to {@link S3Resource} instances, delegating all other URIs to Spring's default resolution.
 *
 * <p>Initially backed by a local-tmp mock — all S3 paths are stored under a temporary
 * directory as {@code {tmpDir}/bucket/key}. Real S3 backend will be added in a follow-up.
 *
 * <p>Usage:
 * <pre>{@code
 * try (S3ResourceLoader loader = new S3ResourceLoader()) {
 *     WritableResource resource = (WritableResource) loader.getResource("s3://my-bucket/path/to/object.txt");
 *     try (OutputStream out = resource.getOutputStream()) { ... }
 * }
 * }</pre>
 */
public class S3ResourceLoader extends DefaultResourceLoader implements Closeable {

    private final Path tmpDir;

    /**
     * Creates a new {@code S3ResourceLoader} with a fresh temporary directory for mock I/O.
     *
     * @throws RuntimeException if the temp directory cannot be created
     */
    public S3ResourceLoader() {
        try {
            this.tmpDir = Files.createTempDirectory("vfs-s3-spring-mock-");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create mock temp directory for S3ResourceLoader", e);
        }
    }

    /**
     * Creates a new {@code S3ResourceLoader} using an existing directory as the mock backend.
     *
     * @param tmpDir the base directory for mock S3 storage
     */
    public S3ResourceLoader(Path tmpDir) {
        this.tmpDir = tmpDir;
    }

    /**
     * Returns the base temporary directory used by the mock backend.
     *
     * <p>Useful for test teardown.
     */
    public Path getTmpDir() {
        return tmpDir;
    }

    @Override
    public Resource getResource(String location) {
        if (location != null && location.startsWith(S3Resource.S3_SCHEME + "://")) {
            var uri = parseS3Uri(location);
            var localPath = toLocalPath(uri);
            return new S3Resource(uri, localPath);
        }
        return super.getResource(location);
    }

    /**
     * Parses an s3:// location string into a {@link URI}, properly encoding any special
     * characters (such as spaces) in the path.
     *
     * <p>Uses the multi-argument {@link URI} constructor so that spaces and other chars
     * in bucket names or keys are percent-encoded in the URI but decoded back in
     * {@link URI#getPath()}.
     */
    private static URI parseS3Uri(String location) {
        // Strip the "s3://" prefix and split bucket from key
        var withoutScheme = location.substring((S3Resource.S3_SCHEME + "://").length());
        var slashIdx = withoutScheme.indexOf('/');
        String bucket;
        String path;
        if (slashIdx < 0) {
            bucket = withoutScheme;
            path = "";
        } else {
            bucket = withoutScheme.substring(0, slashIdx);
            path = withoutScheme.substring(slashIdx); // includes leading slash
        }
        try {
            // Multi-arg constructor encodes the path, preserving spaces in getPath()
            return new URI(S3Resource.S3_SCHEME, bucket, path, null);
        } catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException("Invalid S3 URI: " + location, e);
        }
    }

    /**
     * Maps an S3 URI to a local path under the mock temp directory.
     *
     * <p>{@code s3://my-bucket/path/to/file.txt} → {@code {tmpDir}/my-bucket/path/to/file.txt}
     */
    private Path toLocalPath(URI uri) {
        var bucket = uri.getHost();
        var key = uri.getPath();
        if (key != null && key.startsWith("/")) {
            key = key.substring(1);
        }
        if (key == null || key.isEmpty()) {
            return tmpDir.resolve(bucket);
        }
        return tmpDir.resolve(bucket).resolve(key);
    }

    /**
     * Closes this loader and deletes the mock temp directory.
     *
     * <p>Should be called in test teardown to avoid leaving orphaned temp files.
     */
    @Override
    public void close() throws IOException {
        if (!Files.exists(tmpDir)) {
            return;
        }
        try (var walk = Files.walk(tmpDir)) {
            walk.sorted(reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
