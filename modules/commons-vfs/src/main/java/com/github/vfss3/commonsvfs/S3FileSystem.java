package com.github.vfss3.commonsvfs;

import static java.util.Optional.ofNullable;

import com.github.vfss3.commonsvfs.operations.PlatformFeatures;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * An S3 file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileSystem extends AbstractFileSystem {
    private static final int BUCKET_REDIRECT_STATUS_CODE = 301;
    private static final int BUCKET_ACCESS_FORBIDDEN_STATUS_CODE = 403;

    private final Log log = LogFactory.getLog(getClass());
    private final PlatformFeatures platformFeatures;
    private S3Client service;
    private S3Presigner presigner;

    S3FileSystem(S3FileName rootName, S3FileSystemOptions options, S3Client service, S3Presigner presigner)
            throws FileSystemException {
        super(rootName, null, options.toFileSystemOptions());

        this.service = service;
        this.presigner = presigner;
        this.platformFeatures = options.getPlatformFeatures();

        if (log.isInfoEnabled()) {
            log.info("Init new S3 FileSystem [root=" + rootName + ",opts=" + options + "]");
        }

        try {
            if (!doesBucketExist(rootName.getBucket()) && options.isCreateBucket()) {
                CreateBucketRequest.Builder builder =
                        CreateBucketRequest.builder().bucket(rootName.getBucket());

                ofNullable(options.getObjectOwnership()).ifPresent(builder::objectOwnership);
                ofNullable(options.getCannedAcl()).ifPresent(builder::acl);

                service.createBucket(builder.build());

                if (log.isInfoEnabled()) {
                    log.info("Created new bucket [" + rootName.getBucket() + "]");
                }
            }
        } catch (AwsServiceException e) {
            String message = e.getMessage();
            if (message != null) {
                throw new FileSystemException(message, e);
            } else {
                throw new FileSystemException(e);
            }
        }
    }

    @Override
    protected void addCapabilities(Collection<Capability> caps) {
        caps.addAll(S3FileProvider.capabilities);
    }

    S3Client getService() {
        return service;
    }

    S3Presigner getPresigner() {
        return presigner;
    }

    @Override
    protected FileObject createFile(FileName fileName) throws Exception {
        var s3FileName = (S3FileName) fileName;
        if (platformFeatures != null) {
            s3FileName = s3FileName.withPlatformFeatures(platformFeatures);
        }
        return new S3FileObject(s3FileName, this);
    }

    @Override
    protected void doCloseCommunicationLink() {
        if (presigner != null) {
            presigner.close();
            presigner = null;
        }
        if (service != null) {
            service.close();
            service = null;
        }
    }

    private boolean doesBucketExist(String bucketName) throws FileSystemException {
        try {
            service.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        } catch (AwsServiceException e) {
            int status = e.statusCode();
            if (status == BUCKET_ACCESS_FORBIDDEN_STATUS_CODE) {
                throw new FileSystemException("vfs.provider.s3/connection-forbidden.error", bucketName, e);
            }
            // A redirect status means the bucket exists in another region — treat as exists.
            if (status == BUCKET_REDIRECT_STATUS_CODE) {
                return true;
            }
            if (status == 404) {
                return false;
            }
            throw new FileSystemException(e);
        }
    }
}
