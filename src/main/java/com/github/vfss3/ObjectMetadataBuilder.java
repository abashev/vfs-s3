package com.github.vfss3;

import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;

import java.util.Date;
import java.util.Objects;

import static com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;

/**
 * Build class for S3 ObjectMetadata
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
class ObjectMetadataBuilder {
    private final ObjectMetadata metadata;

    public ObjectMetadataBuilder() {
        this(new ObjectMetadata());
    }

    public ObjectMetadataBuilder(ObjectMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata);
    }

    ObjectMetadataBuilder withZeroContentLength() {
        return this.withContentLength(0);
    }

    ObjectMetadataBuilder withContentLength(long length) {
        metadata.setContentLength(length);

        return this;
    }

    ObjectMetadataBuilder withContentType(String type) {
        metadata.setContentType(Mimetypes.getInstance().getMimetype(type));

        return this;
    }

    ObjectMetadataBuilder withLastModifiedNow() {
        metadata.setLastModified(new Date());

        return this;
    }

    ObjectMetadataBuilder withLastModified(Date date) {
        metadata.setLastModified(date);

        return this;
    }

    ObjectMetadataBuilder withHeader(String key, Object value) {
        metadata.setHeader(key, value);

        return this;
    }

    ObjectMetadataBuilder withServerSideEncryption(boolean useEncryption) {
        if (useEncryption) {
            metadata.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
        }

        return this;
    }

    void attachTo(S3FileObject fileObject, FileType withType) throws FileSystemException {
        fileObject.doAttach(withType, metadata);

    }

    void attachTo(S3FileObject fileObject) throws FileSystemException {
        attachTo(fileObject, null);
    }

    void attachTo(CopyObjectRequest request) {
        request.setNewObjectMetadata(metadata);
    }

    ObjectMetadata toObjectMetadata() {
        return metadata;
    }
}
