package com.github.vfss3.spring;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.springframework.lang.Nullable;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * In-memory {@link S3Client} covering exactly the operations the spring module uses:
 * HEAD/GET/PUT on single keys plus ListObjectsV2 with prefix and continuation-token pagination.
 * Backed by a sorted key → bytes map, matching S3's lexicographic listing order. The SDK's
 * consumer-builder overloads and {@code listObjectsV2Paginator} are inherited default methods
 * that funnel into the request-object methods overridden here. Mirrors the jdk module's
 * {@code FakeS3Client}, trimmed to this module's op set.
 *
 * <p>{@link #denyKey(String)} makes every operation on one key fail with a 403 — for testing
 * the error mapping without a mocking framework.
 */
final class FakeS3Client implements S3Client {

    static final Instant LAST_MODIFIED = Instant.parse("2024-01-01T00:00:00Z");

    private final ConcurrentSkipListMap<String, byte[]> objects = new ConcurrentSkipListMap<>();
    private final Set<String> deniedKeys = new HashSet<>();
    private final int pageSize;
    private boolean closed;

    FakeS3Client() {
        this(1000);
    }

    /** A page cap below 1000 forces multi-page listings, exercising pagination handling. */
    FakeS3Client(int pageSize) {
        this.pageSize = pageSize;
    }

    // ---- test hooks ----

    void putBytes(String key, byte[] content) {
        objects.put(key, content);
    }

    @Nullable
    byte[] bytes(String key) {
        return objects.get(key);
    }

    boolean containsKey(String key) {
        return objects.containsKey(key);
    }

    /** Every operation on this key now fails with a 403, like S3 without permission. */
    void denyKey(String key) {
        deniedKeys.add(key);
    }

    boolean isClosed() {
        return closed;
    }

    // ---- S3Client ----

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest request) {
        checkDenied(request.key());
        var content = objects.get(request.key());
        if (content == null) {
            throw noSuchKey(request.key());
        }
        return HeadObjectResponse.builder()
                .contentLength((long) content.length)
                .lastModified(LAST_MODIFIED)
                .build();
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
        checkDenied(request.key());
        var content = objects.get(request.key());
        if (content == null) {
            throw noSuchKey(request.key());
        }
        var response = GetObjectResponse.builder()
                .contentLength((long) content.length)
                .lastModified(LAST_MODIFIED)
                .build();
        return new ResponseInputStream<>(response, AbortableInputStream.create(new ByteArrayInputStream(content)));
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
        checkDenied(request.key());
        try (var stream = body.contentStreamProvider().newStream()) {
            objects.put(request.key(), stream.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return PutObjectResponse.builder().build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        var prefix = request.prefix() == null ? "" : request.prefix();
        var afterToken = request.continuationToken();
        var max = Math.min(request.maxKeys() == null ? 1000 : request.maxKeys(), pageSize);

        var contents = new ArrayList<S3Object>();
        String lastConsumedKey = null;
        var truncated = false;

        for (var entry : objects.tailMap(prefix).entrySet()) {
            var key = entry.getKey();
            if (!key.startsWith(prefix)) {
                break;
            }
            // Resume strictly after the raw key the previous page stopped at.
            if (afterToken != null && key.compareTo(afterToken) <= 0) {
                continue;
            }
            if (contents.size() >= max) {
                truncated = true;
                break;
            }
            contents.add(S3Object.builder()
                    .key(key)
                    .size((long) entry.getValue().length)
                    .lastModified(LAST_MODIFIED)
                    .build());
            lastConsumedKey = key;
        }

        return ListObjectsV2Response.builder()
                .contents(contents)
                .isTruncated(truncated)
                .nextContinuationToken(truncated ? lastConsumedKey : null)
                .build();
    }

    private void checkDenied(String key) {
        if (deniedKeys.contains(key)) {
            throw (S3Exception) S3Exception.builder()
                    .message("Access Denied: " + key)
                    .statusCode(403)
                    .build();
        }
    }

    private static NoSuchKeyException noSuchKey(String key) {
        return (NoSuchKeyException)
                NoSuchKeyException.builder().message(key).statusCode(404).build();
    }

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public void close() {
        closed = true;
    }
}
