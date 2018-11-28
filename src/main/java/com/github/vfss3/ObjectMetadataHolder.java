package com.github.vfss3;

import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Date;

import static com.amazonaws.services.s3.Headers.ETAG;
import static com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;
import static java.util.Objects.requireNonNull;

/**
 * Build class for S3 ObjectMetadata
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
class ObjectMetadataHolder {
    private final ObjectMetadata metadata;
    private final boolean virtual;

    ObjectMetadataHolder() {
        this.metadata = new ObjectMetadata();
        this.virtual = true;
    }

    ObjectMetadataHolder(ObjectMetadata metadata) {
        this(metadata, false);
    }

    private ObjectMetadataHolder(ObjectMetadata metadata, boolean virtual) {
        this.metadata = requireNonNull(metadata);
        this.virtual = virtual;
    }

    ObjectMetadataHolder(S3ObjectSummary summary) {
        this();

        metadata.setContentLength(requireNonNull(summary).getSize());
        metadata.setLastModified(summary.getLastModified());
        metadata.setHeader(ETAG, summary.getETag());
    }

    ObjectMetadataHolder withZeroContentLength() {
        return this.withContentLength(0);
    }

    ObjectMetadataHolder withContentLength(long length) {
        ObjectMetadata newMeta = metadata.clone();

        metadata.setContentLength(length);

        return new ObjectMetadataHolder(newMeta, virtual);
    }

    ObjectMetadataHolder withContentType(String type) {
        ObjectMetadata newMeta = metadata.clone();

        metadata.setContentType(Mimetypes.getInstance().getMimetype(type));

        return new ObjectMetadataHolder(newMeta, virtual);
    }

    ObjectMetadataHolder withLastModifiedNow() {
        ObjectMetadata newMeta = metadata.clone();

        metadata.setLastModified(new Date());

        return new ObjectMetadataHolder(newMeta, virtual);
    }

    ObjectMetadataHolder withServerSideEncryption(boolean useEncryption) {
        ObjectMetadata newMeta = metadata.clone();

        if (useEncryption) {
            newMeta.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
        }

        return new ObjectMetadataHolder(newMeta, virtual);
    }

    String getServerSideEncryption() {
        return metadata.getSSEAlgorithm();
    }

    public boolean isVirtual() {
        return virtual;
    }

    public String getMD5Hash() {
        return metadata.getETag();
    }

    public boolean hasMD5Hash(String md5) {
        if (md5 == null) {
            return (metadata.getETag() == null);
        } else {
            return ((metadata.getETag() != null) && metadata.getETag().equals(md5));
        }
    }

    public long getContentLength() {
        return metadata.getContentLength();
    }

    public long getLastModified() {
        Date lastModified = metadata.getLastModified();

        return (lastModified != null) ? lastModified.getTime() : 0L;
    }

    /**
     * Send metadata with PUT request.
     *
     * @param request
     */
    public void sendWith(PutObjectRequest request) {
        request.setMetadata(metadata.clone());
    }

    public void sendWith(CopyObjectRequest request) {
        request.setNewObjectMetadata(metadata.clone());
    }
}
