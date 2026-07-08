package com.github.vfss3.jdk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.TreeMap;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * In-memory {@link S3Client} covering exactly the operations {@link S3FileSystemProvider} uses:
 * HEAD/GET/PUT/COPY/DELETE on single keys plus ListObjectsV2 with prefix, delimiter, maxKeys and
 * continuation-token pagination. Backed by a sorted key → bytes map, matching S3's lexicographic
 * listing order. The SDK's consumer-builder overloads and {@code listObjectsV2Paginator} are
 * inherited default methods that funnel into the request-object methods overridden here.
 */
final class FakeS3Client implements S3Client {

    static final Instant LAST_MODIFIED = Instant.parse("2024-01-01T00:00:00Z");

    private final TreeMap<String, byte[]> objects = new TreeMap<>();
    private final int pageSize;

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

    byte[] bytes(String key) {
        return objects.get(key);
    }

    boolean containsKey(String key) {
        return objects.containsKey(key);
    }

    // ---- S3Client ----

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest request) {
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
        try (var stream = body.contentStreamProvider().newStream()) {
            objects.put(request.key(), stream.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return PutObjectResponse.builder().build();
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest request) {
        var content = objects.get(request.sourceKey());
        if (content == null) {
            throw noSuchKey(request.sourceKey());
        }
        objects.put(request.destinationKey(), content);
        return CopyObjectResponse.builder().build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
        objects.remove(request.key());
        return DeleteObjectResponse.builder().build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        var prefix = request.prefix() == null ? "" : request.prefix();
        var delimiter = request.delimiter();
        var afterToken = request.continuationToken();
        var max = Math.min(request.maxKeys() == null ? 1000 : request.maxKeys(), pageSize);

        var contents = new ArrayList<S3Object>();
        var commonPrefixes = new LinkedHashSet<String>();
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
            if (contents.size() + commonPrefixes.size() >= max) {
                truncated = true;
                break;
            }
            var rest = key.substring(prefix.length());
            var delimiterIdx = delimiter == null ? -1 : rest.indexOf(delimiter);
            if (delimiterIdx >= 0) {
                commonPrefixes.add(prefix + rest.substring(0, delimiterIdx + delimiter.length()));
            } else {
                contents.add(S3Object.builder()
                        .key(key)
                        .size((long) entry.getValue().length)
                        .lastModified(LAST_MODIFIED)
                        .build());
            }
            lastConsumedKey = key;
        }

        return ListObjectsV2Response.builder()
                .contents(contents)
                .commonPrefixes(commonPrefixes.stream()
                        .map(p -> CommonPrefix.builder().prefix(p).build())
                        .toList())
                .isTruncated(truncated)
                .nextContinuationToken(truncated ? lastConsumedKey : null)
                .build();
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
    public void close() {}
}
