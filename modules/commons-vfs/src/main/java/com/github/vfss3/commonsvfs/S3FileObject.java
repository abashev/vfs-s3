package com.github.vfss3.commonsvfs;

import static com.github.vfss3.commonsvfs.operations.Acl.Permission.READ;
import static com.github.vfss3.commonsvfs.operations.Acl.Permission.WRITE;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.vfs2.FileName.ROOT_PATH;
import static org.apache.commons.vfs2.FileName.SEPARATOR;
import static org.apache.commons.vfs2.FileType.*;
import static org.apache.commons.vfs2.NameScope.CHILD;
import static org.apache.commons.vfs2.NameScope.DESCENDENT_OR_SELF;

import com.github.vfss3.commonsvfs.operations.Acl;
import com.github.vfss3.commonsvfs.operations.IAclGetter;
import java.io.*;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Type;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

/**
 * Implementation of the virtual S3 file system object using the AWS-SDK.
 * Based on Matthias Jugel code.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 * @author Shon Vella
 */
public class S3FileObject extends AbstractFileObject<S3FileSystem> {
    private static final String ALL_USERS_GROUP = "http://acs.amazonaws.com/groups/global/AllUsers";
    private static final String AUTHENTICATED_USERS_GROUP = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

    private final Log log = LogFactory.getLog(getClass());

    /** Cached S3 metadata for this file. */
    private ObjectMetadataHolder objectMetadataHolder;

    private ObjectContentHolder objectContentHolder;

    public S3FileObject(S3FileName fileName, S3FileSystem fileSystem) {
        super(fileName, fileSystem);
    }

    @Override
    public boolean isAttached() {
        return (objectMetadataHolder != null);
    }

    @Override
    public void setAttached(boolean attached) {
        if ((attached && (objectMetadataHolder == null)) || (!attached && (objectMetadataHolder != null))) {
            throw new IllegalStateException("Wrong usage of 'attached' property");
        }
    }

    @Override
    protected void doAttach() throws FileSystemException {
        if (getName().getPath().equals(ROOT_PATH)) {
            if (log.isDebugEnabled()) {
                log.debug("Attach S3FileObject to the bucket " + getName());
            }

            doAttachVirtualFolder();

            return;
        }

        try {
            String candidateKey = getName().getS3KeyAs(FILE);
            doAttach(FILE, new ObjectMetadataHolder(headObject(candidateKey)));

            if (log.isDebugEnabled()) {
                log.debug("Attach file to S3 Object " + getName());
            }

            return;
        } catch (NoSuchKeyException e) {
            // fall through to folder/virtual-folder lookup
        } catch (AwsServiceException e) {
            if (e.statusCode() == 403) {
                doAttach(FILE, new ObjectMetadataHolder());

                if (log.isDebugEnabled()) {
                    log.debug("Attach to forbidden S3 object " + getName());
                }

                return;
            }
            // fall through to folder/virtual-folder lookup
        }

        try {
            String candidateKey = getName().getS3KeyAs(FOLDER);
            doAttach(FOLDER, new ObjectMetadataHolder(headObject(candidateKey)));

            if (log.isDebugEnabled()) {
                log.debug("Attach folder to S3 Object " + getName());
            }

            return;
        } catch (AwsServiceException ignored) {
            // No folder marker — fall through to virtual-folder probe
        }

        try {
            String candidateKey = getName().getS3KeyAs(FOLDER);

            ListObjectsV2Response listing = getService()
                    .listObjectsV2(ListObjectsV2Request.builder()
                            .bucket(getBucketName())
                            .prefix(candidateKey)
                            .maxKeys(1)
                            .build());

            if (!listing.contents().isEmpty()) {
                doAttachVirtualFolder();

                if (log.isDebugEnabled()) {
                    log.debug("Attach folder to virtual S3 folder " + getName());
                }

                return;
            }
        } catch (AwsServiceException ignored) {
        }

        if (objectMetadataHolder == null) {
            doAttach(null, new ObjectMetadataHolder());

            if (log.isDebugEnabled()) {
                log.debug("Attach to empty S3 object " + getName());
            }
        }
    }

    private HeadObjectResponse headObject(String key) {
        return getService()
                .headObject(HeadObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build());
    }

    protected final void doAttachVirtualFolder() throws FileSystemException {
        doAttach(FOLDER, new ObjectMetadataHolder().withZeroContentLength().withContentType(""));
    }

    protected void doAttach(FileType type, ObjectMetadataHolder metadata) throws FileSystemException {
        if (objectMetadataHolder != null) {
            throw new FileSystemException("Try to reattach file " + getName() + " without detach");
        }

        objectMetadataHolder = requireNonNull(metadata);

        if (type != null) {
            injectType(type);
        }
    }

    @Override
    protected void doDetach() throws FileSystemException {
        if (objectMetadataHolder == null) {
            throw new FileSystemException("Try to detach file " + getName() + " without attach");
        }

        if (log.isDebugEnabled()) {
            log.debug("Detach [" + getName() + "]");
        }

        objectMetadataHolder = null;
    }

    @Override
    protected void doDelete() throws Exception {
        final String bucket = getBucketName();

        if (getName().getS3Key().isPresent()) {
            final String key =
                    getName().getS3Key().orElseThrow(() -> new IllegalStateException("No value after isPresent"));

            if (log.isDebugEnabled()) {
                log.debug("Delete object [bucket=" + bucket + ",name=" + key + "]");
            }

            getService()
                    .deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build());
        } else {
            getService()
                    .deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        }
    }

    @Override
    protected void doCreateFolder() throws Exception {
        final String key = getName().getS3KeyAs(FOLDER);

        if (log.isDebugEnabled()) {
            log.debug("Create new folder in bucket [" + getBucketName() + "] with key [" + key + "]");
        }

        if (!isAttached()) {
            throw new FileSystemException("Need to attach first");
        }

        getService()
                .putObject(
                        PutObjectRequest.builder()
                                .bucket(getBucketName())
                                .key(key)
                                .contentLength(0L)
                                .build(),
                        RequestBody.empty());
    }

    @Override
    protected long doGetLastModifiedTime() {
        return objectMetadataHolder.getLastModified();
    }

    @Override
    protected InputStream doGetInputStream() throws Exception {
        if (objectContentHolder == null) {
            objectContentHolder = new ObjectContentHolder();
        }

        final String objectPath =
                getName().getS3Key().orElseThrow(() -> new FileSystemException("Not able to get object path"));

        if ((objectMetadataHolder.getContentLength() == 0)
                || !objectMetadataHolder.getMD5Hash().isPresent()) {
            if (log.isWarnEnabled()) {
                log.warn("Try to get input stream from empty object [" + objectPath + "]. Return empty stream");
            }

            return new ByteArrayInputStream(new byte[0]);
        } else if (!objectContentHolder.sameData(objectMetadataHolder)) {
            try (ResponseInputStream<GetObjectResponse> stream = getService()
                    .getObject(GetObjectRequest.builder()
                            .bucket(getBucketName())
                            .key(objectPath)
                            .build())) {
                objectContentHolder.populateData(stream, objectMetadataHolder);
            }
        }

        return objectContentHolder.getInputStream();
    }

    @Override
    protected OutputStream doGetOutputStream(boolean append) throws Exception {
        if (append) {
            throw new FileSystemException("Append mode is not supported for S3 because of inconsistency");
        }

        if (objectContentHolder == null) {
            objectContentHolder = new ObjectContentHolder();
        }

        return objectContentHolder.getOutputStream(this);
    }

    @Override
    protected FileType doGetType() {
        return IMAGINARY;
    }

    @Override
    protected String[] doListChildren() {
        throw new UnsupportedOperationException("this should never get called since we override getChildren()");
    }

    @Override
    protected FileObject[] doListChildrenResolved() throws FileSystemException {
        assertType(FOLDER);

        final String path = getName().getS3Key().orElse("");

        final List<S3Object> summaries = new ArrayList<>();
        final Set<String> commonPrefixes = new TreeSet<>();

        String continuationToken = null;
        do {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(getBucketName())
                    .delimiter(SEPARATOR)
                    .prefix(path)
                    .continuationToken(continuationToken)
                    .build();

            ListObjectsV2Response listing = getService().listObjectsV2(request);

            summaries.addAll(listing.contents());
            listing.commonPrefixes().forEach(p -> commonPrefixes.add(p.prefix()));

            continuationToken = Boolean.TRUE.equals(listing.isTruncated()) ? listing.nextContinuationToken() : null;
        } while (continuationToken != null);

        List<FileObject> resolvedChildren = new ArrayList<>(summaries.size() + commonPrefixes.size());

        for (String commonPrefix : commonPrefixes) {
            String stripPath = commonPrefix.substring(path.length());
            if (!stripPath.equals(ROOT_PATH)) {
                FileObject childObject = resolveFile(stripPath, CHILD);
                S3FileObject s3FileObject = (S3FileObject) FileObjectUtils.unwrap(childObject);

                if (s3FileObject != null) {
                    s3FileObject.doAttachVirtualFolder();
                    s3FileObject.setParent(this);

                    resolvedChildren.add(childObject);
                }
            }
        }

        for (S3Object summary : summaries) {
            if (!summary.key().equals(path)) {
                final String stripPath = summary.key().substring(path.length());
                FileObject childObject = resolveFile(stripPath, CHILD);
                S3FileObject s3FileObject = (S3FileObject) FileObjectUtils.unwrap(childObject);

                if (s3FileObject != null) {
                    s3FileObject.doAttach(FILE, new ObjectMetadataHolder(summary));
                    s3FileObject.setParent(this);

                    resolvedChildren.add(childObject);
                }
            }
        }

        return resolvedChildren.toArray(new FileObject[0]);
    }

    @Override
    protected long doGetContentSize() {
        return objectMetadataHolder.getContentLength();
    }

    @Override
    public S3FileName getName() {
        return (S3FileName) super.getName();
    }

    @Override
    protected boolean checkBeforeDelete(FileObject file) throws FileSystemException {
        return false;
    }

    @Override
    public void close() throws FileSystemException {
        super.close();

        if (objectContentHolder != null) {
            objectContentHolder.close();
            objectContentHolder = null;
        }
    }

    /**
     * Returns access control list for this file.
     *
     * @see FileObject#getFileOperations()
     * @see IAclGetter
     */
    public Acl getAcl() throws FileSystemException {
        assertType(FILE, FOLDER);

        Acl myAcl = new Acl();
        Optional<String> key = getName().getS3Key();

        Owner owner;
        List<Grant> grants;

        try {
            if (key.isPresent()) {
                GetObjectAclResponse response = getService()
                        .getObjectAcl(GetObjectAclRequest.builder()
                                .bucket(getBucketName())
                                .key(key.get())
                                .build());
                owner = response.owner();
                grants = response.grants();
            } else {
                GetBucketAclResponse response = getService()
                        .getBucketAcl(GetBucketAclRequest.builder()
                                .bucket(getBucketName())
                                .build());
                owner = response.owner();
                grants = response.grants();
            }
        } catch (AwsServiceException e) {
            throw new FileSystemException(e);
        }

        if (grants == null || grants.isEmpty()) {
            return myAcl;
        }

        boolean mappedAny = false;

        for (Grant item : grants) {
            Permission perm = item.permission();
            Acl.Permission[] rights;
            if (perm == Permission.FULL_CONTROL) {
                rights = Acl.Permission.values();
            } else if (perm == Permission.READ) {
                rights = new Acl.Permission[] {READ};
            } else if (perm == Permission.WRITE) {
                rights = new Acl.Permission[] {WRITE};
            } else {
                log.error("Skip unknown permission " + perm);
                continue;
            }

            Grantee grantee = item.grantee();
            if (grantee == null) {
                continue;
            }

            if (grantee.type() == Type.GROUP) {
                if (ALL_USERS_GROUP.equals(grantee.uri())) {
                    myAcl.allow(Acl.Group.EVERYONE, rights);
                    mappedAny = true;
                } else if (AUTHENTICATED_USERS_GROUP.equals(grantee.uri())) {
                    myAcl.allow(Acl.Group.AUTHORIZED, rights);
                    mappedAny = true;
                }
            } else if (grantee.type() == Type.CANONICAL_USER) {
                String id = grantee.id();
                String ownerId = (owner != null) ? owner.id() : null;
                if (id != null && id.equals(ownerId)) {
                    myAcl.allow(Acl.Group.OWNER, rights);
                    mappedAny = true;
                }
            }
        }

        if (!mappedAny) {
            // Some S3-compatible backends (notably MinIO, RustFS) return ACL grants whose
            // canonical-user identifiers don't match the bucket-owner id, or omit the id
            // altogether. There's nothing meaningful we can attribute to OWNER/EVERYONE/
            // AUTHORIZED in that case — surface it so callers can decide how to react
            // (the integration tests treat this as a backend compat issue and skip).
            throw new FileSystemException(
                    "Backend returned " + grants.size() + " ACL grant(s) but none matched the bucket"
                            + " owner or a known group — ACL response is incompatible");
        }

        return myAcl;
    }

    /** Set access control list for this file. */
    public void setAcl(Acl acl) throws FileSystemException {
        assertType(FILE, FOLDER);

        Owner owner;
        try {
            Optional<String> key = getName().getS3Key();
            if (key.isPresent()) {
                owner = getService()
                        .getObjectAcl(GetObjectAclRequest.builder()
                                .bucket(getBucketName())
                                .key(key.get())
                                .build())
                        .owner();
            } else {
                owner = getService()
                        .getBucketAcl(GetBucketAclRequest.builder()
                                .bucket(getBucketName())
                                .build())
                        .owner();
            }
        } catch (AwsServiceException e) {
            throw new FileSystemException(e);
        }

        Map<Acl.Group, Acl.Permission[]> rules = acl.getRules();
        final Acl.Permission[] allRights = Acl.Permission.values();
        List<Grant> grants = new ArrayList<>();

        for (Acl.Group group : rules.keySet()) {
            Acl.Permission[] rights = rules.get(group);
            if (rights.length == 0) {
                continue;
            }

            Permission perm;
            if (Arrays.equals(rights, allRights)) {
                perm = Permission.FULL_CONTROL;
            } else if (acl.isAllowed(group, READ)) {
                perm = Permission.READ;
            } else if (acl.isAllowed(group, WRITE)) {
                perm = Permission.WRITE;
            } else {
                log.error("Skip unknown set of rights " + Arrays.toString(rights));
                continue;
            }

            Grantee grantee;
            if (group.equals(Acl.Group.EVERYONE)) {
                grantee =
                        Grantee.builder().type(Type.GROUP).uri(ALL_USERS_GROUP).build();
            } else if (group.equals(Acl.Group.AUTHORIZED)) {
                grantee = Grantee.builder()
                        .type(Type.GROUP)
                        .uri(AUTHENTICATED_USERS_GROUP)
                        .build();
            } else if (group.equals(Acl.Group.OWNER)) {
                if (owner == null || owner.id() == null) {
                    log.error("Skip OWNER group — bucket has no canonical owner id");
                    continue;
                }
                grantee = Grantee.builder()
                        .type(Type.CANONICAL_USER)
                        .id(owner.id())
                        .build();
            } else {
                log.error("Skip unknown group " + group);
                continue;
            }

            grants.add(Grant.builder().grantee(grantee).permission(perm).build());
        }

        AccessControlPolicy policy =
                AccessControlPolicy.builder().owner(owner).grants(grants).build();

        try {
            Optional<String> key = getName().getS3Key();
            if (key.isPresent()) {
                getService()
                        .putObjectAcl(PutObjectAclRequest.builder()
                                .bucket(getBucketName())
                                .key(key.get())
                                .accessControlPolicy(policy)
                                .build());
            } else {
                getService()
                        .putBucketAcl(PutBucketAclRequest.builder()
                                .bucket(getBucketName())
                                .accessControlPolicy(policy)
                                .build());
            }
        } catch (Exception e) {
            throw new FileSystemException(e);
        }
    }

    public String getSignedUrl(int expireInSeconds) throws FileSystemException {
        assertType(FILE, FOLDER);

        try {
            S3Presigner presigner = getPresigner();
            String key = getName()
                    .getS3Key()
                    .orElseThrow(() -> new FileSystemException("Not able get presigned url for a bucket"));

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(expireInSeconds))
                    .getObjectRequest(GetObjectRequest.builder()
                            .bucket(getBucketName())
                            .key(key)
                            .build())
                    .build();

            return presigner.presignGetObject(presignRequest).url().toString();
        } catch (AwsServiceException e) {
            throw new FileSystemException(e);
        }
    }

    public Optional<String> getMD5Hash() throws FileSystemException {
        assertType(FILE, FOLDER);
        return objectMetadataHolder.getMD5Hash();
    }

    public Optional<String> getSSEAlgorithm() throws FileSystemException {
        assertType(FILE, FOLDER);

        if (objectMetadataHolder.isVirtual()) {
            if (log.isDebugEnabled()) {
                log.debug("Have to fetch real metadata for [" + getName() + "]");
            }

            refresh();
            getType();
        }

        if (objectMetadataHolder.isVirtual()) {
            throw new FileSystemException("Not able to fetch real metadata from " + getName());
        }

        return Optional.of(objectMetadataHolder).map(ObjectMetadataHolder::getServerSideEncryption);
    }

    public String getCacheFile() throws FileSystemException {
        if (objectContentHolder != null) {
            return objectContentHolder.getFile();
        } else {
            return null;
        }
    }

    protected void assertType(FileType... types) throws FileSystemException {
        final FileType type = getType();
        boolean val = false;

        for (FileType t : types) {
            val = (type == t) || val;
        }

        if (!val) {
            throw new FileSystemException("File type should be one of " + Arrays.toString(types));
        }
    }

    protected S3Client getService() {
        return ((S3FileSystem) getFileSystem()).getService();
    }

    protected S3Presigner getPresigner() {
        return ((S3FileSystem) getFileSystem()).getPresigner();
    }

    protected String getBucketName() {
        return ((S3FileName) getFileSystem().getRootName()).getBucket();
    }

    @Override
    public boolean canRenameTo(FileObject newfile) {
        return false;
    }

    @Override
    public void copyFrom(final FileObject file, final FileSelector selector) throws FileSystemException {
        if (!file.exists()) {
            throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
        }

        final ArrayList<FileObject> files = new ArrayList<>();
        file.findFiles(selector, false, files);

        if (log.isDebugEnabled()) {
            log.debug("Found files " + files);
        }

        Map<FileObject, FileObject> filesToCopy = new LinkedHashMap<>();

        for (FileObject srcFile : files) {
            final FileObject unwrappedSrcFile = FileObjectUtils.unwrap(srcFile);
            final String relPath = file.getName().getRelativeName(unwrappedSrcFile.getName());
            final FileObject destFile = resolveFile(relPath, DESCENDENT_OR_SELF);
            final FileObject unwrappedDestFile = FileObjectUtils.unwrap(destFile);

            if (!allowS3Copy(unwrappedSrcFile, unwrappedDestFile)) {
                log.warn("One of files don't allow S3 copy - fallback to default implementation [from="
                        + unwrappedSrcFile + ",to=" + unwrappedDestFile + "]");

                super.copyFrom(file, selector);
                refresh();

                return;
            }

            filesToCopy.put(unwrappedSrcFile, unwrappedDestFile);
        }

        for (Map.Entry<FileObject, FileObject> entry : filesToCopy.entrySet()) {
            final FileObject source = entry.getKey();
            final FileObject destination = entry.getValue();

            if (log.isDebugEnabled()) {
                log.debug("Do file copy from [" + source.getName() + "] to [" + destination.getName() + "]");
            }

            doCopyFrom(source, destination);
        }

        refresh();
    }

    protected boolean allowS3Copy(FileObject fromFile, FileObject toFile) throws FileSystemException {
        if (fromFile.getType().hasChildren()) {
            return true;
        } else if ((fromFile instanceof S3FileObject s3FileObject) && s3FileObject.sameFileSystem(toFile)) {
            return true;
        } else if (fromFile.getType().hasContent()
                && fromFile.getURL().getProtocol().equals("file")
                && (toFile instanceof S3FileObject)) {
            try {
                fromFile.getURL().toURI();
                return true;
            } catch (URISyntaxException e) {
                // not a valid URI
            }
        }

        return false;
    }

    protected void doCopyFrom(FileObject fromFile, FileObject toFile) throws FileSystemException {
        if (log.isDebugEnabled()) {
            log.debug("Do S3 copy [from=" + fromFile + ",to=" + toFile + "]");
        }

        if (toFile.exists()) {
            if (fromFile.getType() != toFile.getType()) {
                toFile.delete(Selectors.SELECT_ALL);
            }
        } else {
            FileObject parent = getParent();
            if (parent != null) {
                parent.createFolder();
            }
        }

        try {
            if (fromFile.getType().hasChildren()) {
                toFile.createFolder();
            } else if ((fromFile instanceof S3FileObject s3SrcFile) && s3SrcFile.sameFileSystem(toFile)) {

                S3FileObject s3DestFile = (S3FileObject) toFile;

                String srcBucketName = s3SrcFile.getBucketName();
                String srcFileName = s3SrcFile
                        .getName()
                        .getS3Key()
                        .orElseThrow(() -> new FileSystemException("Not able to copy whole bucket"));
                String destBucketName = s3DestFile.getBucketName();
                String destFileName = s3DestFile.getName().getS3KeyAs(FILE);

                if (!s3SrcFile.exists()) {
                    throw new FileSystemException("Source file doesn't exist [" + s3SrcFile + "]");
                }

                if (!s3DestFile.exists()) {
                    s3DestFile.createFile();
                }

                CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                        .sourceBucket(srcBucketName)
                        .sourceKey(srcFileName)
                        .destinationBucket(destBucketName)
                        .destinationKey(destFileName);

                if (s3SrcFile.getType() == FILE) {
                    if (s3SrcFile.objectMetadataHolder.isVirtual()) {
                        s3SrcFile.refresh();
                        s3SrcFile.getType();
                    }

                    if (s3SrcFile.objectMetadataHolder.isVirtual()) {
                        throw new FileSystemException("Not able to fetch real metadata from " + getName());
                    }

                    s3SrcFile
                            .objectMetadataHolder
                            .withServerSideEncryption(getServerSideEncryption())
                            .sendWith(builder);
                }

                CopyObjectResponse response = getService().copyObject(builder.build());
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Copy succeeded eTag=" + response.copyObjectResult().eTag());
                }
            } else if (fromFile.getType().hasContent()
                    && fromFile.getURL().getProtocol().equals("file")
                    && (toFile instanceof S3FileObject s3DestFile)) {

                try {
                    File localFile = new File(fromFile.getURL().toURI());
                    s3DestFile.upload(localFile);
                } catch (URISyntaxException e) {
                    // couldn't convert URL to URI; the slower default copy stays available via super.copyFrom
                }
            }
        } catch (IOException | SdkException e) {
            throw new FileSystemException("vfs.provider/copy-file.error", e, fromFile, toFile);
        } finally {
            toFile.close();
        }
    }

    protected boolean sameFileSystem(FileObject toFile) {
        if (!(toFile instanceof S3FileObject s3FileObject)) {
            return false;
        }

        final S3FileName destFile = s3FileObject.getName();

        return Objects.equals(getName().getEndpoint(), destFile.getEndpoint())
                && Objects.equals(getName().getAccessKey(), destFile.getAccessKey())
                && Objects.equals(getName().getSecretKey(), destFile.getSecretKey());
    }

    /**
     * Uploads file to S3.
     *
     * @return the eTag of the uploaded result
     */
    String upload(File file) throws IOException {
        final String key = (getType() == IMAGINARY)
                ? getName().getS3KeyAs(FILE)
                : getName().getS3Key().orElseThrow(() -> new FileSystemException("Not able to copy whole bucket"));

        PutObjectRequest.Builder requestBuilder =
                PutObjectRequest.builder().bucket(getBucketName()).key(key);

        new ObjectMetadataHolder()
                .withContentLength(file.length())
                .withContentType(getName().getBaseName())
                .withServerSideEncryption(getServerSideEncryption())
                .sendWith(requestBuilder);

        if (log.isDebugEnabled()) {
            log.debug("Upload request [file=" + file + ",key=" + key + ",length=" + file.length() + ",type="
                    + getName().getBaseName() + "]");
        }

        String md5;

        try {
            PutObjectResponse response = getService().putObject(requestBuilder.build(), RequestBody.fromFile(file));
            md5 = stripQuotes(response.eTag());
        } catch (SdkClientException e) {
            throw new IOException(e);
        }

        ObjectMetadataHolder newMetadata;
        try {
            newMetadata = new ObjectMetadataHolder(headObject(getName().getS3KeyAs(FILE)));
        } catch (AwsServiceException e) {
            throw new IOException(e);
        }

        if (newMetadata.getContentLength() != file.length()) {
            throw new FileSystemException("Wrong content length after upload. Expected [" + file.length()
                    + "] but have [" + newMetadata.getContentLength() + "]");
        }

        if ((md5 != null)
                && newMetadata.getMD5Hash().isPresent()
                && !md5.equalsIgnoreCase(newMetadata.getMD5Hash().get())) {
            throw new FileSystemException("Wrong MD5 for content after upload. Expected [" + md5 + "] but have ["
                    + newMetadata.getMD5Hash().get() + "]");
        }

        objectMetadataHolder = null;
        doAttach(FILE, newMetadata);

        return md5;
    }

    private boolean getServerSideEncryption() {
        if (!getName().getPlatformFeatures().supportsServerSideEncryption()) {
            return false;
        }

        return new S3FileSystemOptions(getFileSystem().getFileSystemOptions()).getServerSideEncryption();
    }

    private static String stripQuotes(String eTag) {
        if (eTag == null) return null;
        if (eTag.length() >= 2 && eTag.startsWith("\"") && eTag.endsWith("\"")) {
            return eTag.substring(1, eTag.length() - 1);
        }
        return eTag;
    }
}
