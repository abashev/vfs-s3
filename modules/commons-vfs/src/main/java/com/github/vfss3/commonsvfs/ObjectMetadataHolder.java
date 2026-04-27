package com.github.vfss3.commonsvfs;

import static java.util.Optional.ofNullable;

import java.net.URLConnection;
import java.time.Instant;
import java.util.Optional;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * Lightweight holder for S3 object metadata. AWS SDK v2 dropped the v1 {@code ObjectMetadata}
 * mutable wrapper — metadata lives on the response and request types directly. This class
 * keeps just the fields the provider cares about (size, last-modified, ETag, content-type,
 * SSE algorithm) so the rest of the codebase doesn't need to know whether the metadata came
 * from a {@code HeadObject} response or a list-object summary.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
class ObjectMetadataHolder {
    private final long contentLength;
    private final long lastModifiedMillis;
    private final String eTag;
    private final String contentType;
    private final String serverSideEncryption;
    private final boolean virtual;

    private ObjectMetadataHolder(
            long contentLength,
            long lastModifiedMillis,
            String eTag,
            String contentType,
            String serverSideEncryption,
            boolean virtual) {
        this.contentLength = contentLength;
        this.lastModifiedMillis = lastModifiedMillis;
        this.eTag = eTag;
        this.contentType = contentType;
        this.serverSideEncryption = serverSideEncryption;
        this.virtual = virtual;
    }

    /** Empty placeholder used while the file's real metadata isn't fetched yet. */
    ObjectMetadataHolder() {
        this(0L, 0L, null, null, null, true);
    }

    ObjectMetadataHolder(HeadObjectResponse response) {
        this(
                response.contentLength() == null ? 0L : response.contentLength(),
                response.lastModified() == null ? 0L : response.lastModified().toEpochMilli(),
                stripQuotes(response.eTag()),
                response.contentType(),
                response.serverSideEncryptionAsString(),
                false);
    }

    ObjectMetadataHolder(S3Object summary) {
        this(
                summary.size() == null ? 0L : summary.size(),
                summary.lastModified() == null ? 0L : summary.lastModified().toEpochMilli(),
                stripQuotes(summary.eTag()),
                null,
                null,
                false);
    }

    /**
     * AWS SDK v2 returns the {@code ETag} as the server sends it on the wire — including the
     * surrounding quotes (e.g. {@code "9bb58f26192e4ba00f01e2e7b136bbd8"}). v1 used to strip
     * the quotes inside the SDK; v2 leaves them in. The MD5 helpers across the codebase
     * compare hex strings, so normalise here.
     */
    private static String stripQuotes(String eTag) {
        if (eTag == null) return null;
        if (eTag.length() >= 2 && eTag.startsWith("\"") && eTag.endsWith("\"")) {
            return eTag.substring(1, eTag.length() - 1);
        }
        return eTag;
    }

    ObjectMetadataHolder withZeroContentLength() {
        return new ObjectMetadataHolder(0L, lastModifiedMillis, eTag, contentType, serverSideEncryption, virtual);
    }

    ObjectMetadataHolder withContentLength(long length) {
        return new ObjectMetadataHolder(length, lastModifiedMillis, eTag, contentType, serverSideEncryption, virtual);
    }

    ObjectMetadataHolder withContentType(String filenameOrType) {
        String resolved = (filenameOrType == null) ? null : URLConnection.guessContentTypeFromName(filenameOrType);
        if (resolved == null) {
            resolved = "application/octet-stream";
        }
        return new ObjectMetadataHolder(
                contentLength, lastModifiedMillis, eTag, resolved, serverSideEncryption, virtual);
    }

    ObjectMetadataHolder withLastModifiedNow() {
        return new ObjectMetadataHolder(
                contentLength, Instant.now().toEpochMilli(), eTag, contentType, serverSideEncryption, virtual);
    }

    ObjectMetadataHolder withServerSideEncryption(boolean useEncryption) {
        String sse = useEncryption ? ServerSideEncryption.AES256.toString() : null;
        return new ObjectMetadataHolder(contentLength, lastModifiedMillis, eTag, contentType, sse, virtual);
    }

    String getServerSideEncryption() {
        return serverSideEncryption;
    }

    public boolean isVirtual() {
        return virtual;
    }

    public Optional<String> getMD5Hash() {
        return ofNullable(eTag);
    }

    public long getContentLength() {
        return contentLength;
    }

    public long getLastModified() {
        return lastModifiedMillis;
    }

    /** Copy this metadata onto a {@link PutObjectRequest.Builder}. */
    public void sendWith(PutObjectRequest.Builder request) {
        if (contentLength > 0) {
            request.contentLength(contentLength);
        }
        if (contentType != null) {
            request.contentType(contentType);
        }
        if (serverSideEncryption != null) {
            request.serverSideEncryption(serverSideEncryption);
        }
    }

    /** Copy this metadata onto a {@link CopyObjectRequest.Builder}. */
    public void sendWith(CopyObjectRequest.Builder request) {
        if (contentType != null) {
            request.contentType(contentType);
        }
        if (serverSideEncryption != null) {
            request.serverSideEncryption(serverSideEncryption);
        }
    }
}
