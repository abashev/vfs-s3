package com.github.vfss3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.github.vfss3.operations.Acl;
import com.github.vfss3.operations.IAclGetter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.util.MonitorOutputStream;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.vfss3.operations.Acl.Permission.READ;
import static com.github.vfss3.operations.Acl.Permission.WRITE;
import static java.util.Calendar.SECOND;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.apache.commons.vfs2.FileName.ROOT_PATH;
import static org.apache.commons.vfs2.FileName.SEPARATOR;
import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.FileType.IMAGINARY;
import static org.apache.commons.vfs2.NameScope.CHILD;
import static org.apache.commons.vfs2.NameScope.DESCENDENT_OR_SELF;

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
    private final Log log = LogFactory.getLog(getClass());

    /**
     * Amazon S3 object
     */
    private ObjectMetadataHolder objectMetadataHolder;

    /**
     * Local cache of file content
     */
    private S3TempFile cacheFile;

    /**
     * Lock to control output stream - not thread specific, just based on the output stream being open
     */
    private final AtomicBoolean outputInProgress = new AtomicBoolean();

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
        try {
            if (getName().getPath().equals(ROOT_PATH)) {
                if (log.isDebugEnabled()) {
                    log.debug("Attach S3FileObject to the bucket " + getName());
                }

                doAttachVirtualFolder();

                return;
            }

            try {
                // Do we have file with name?
                String candidateKey = getName().getS3KeyAs(FILE);

                doAttach(FILE, new ObjectMetadataHolder(getService().getObjectMetadata(getBucketName(), candidateKey)));

                if (log.isDebugEnabled()) {
                    log.debug("Attach file to S3 Object " + getName());
                }

                return;
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 403) { // Forbidden
                    doAttach(FILE, new ObjectMetadataHolder());

                    if (log.isDebugEnabled()) {
                        log.debug("Attach to forbidden S3 object " + getName());
                    }

                    return;
                }

                // We are attempting to attach to the root bucket
            }

            try {
                // Do we have folder with that name?
                String candidateKey = getName().getS3KeyAs(FOLDER);

                doAttach(FOLDER, new ObjectMetadataHolder(getService().getObjectMetadata(getBucketName(), candidateKey)));

                if (log.isDebugEnabled()) {
                    log.debug("Attach folder to S3 Object " + getName());
                }

                return;
            } catch (AmazonServiceException e) {
                // No, we don't
            }

            try {
                // Do, we have subordinate objects
                String candidateKey = getName().getS3KeyAs(FOLDER);

                ObjectListing listing = getService().listObjects(
                        new ListObjectsRequest().
                                withBucketName(getBucketName()).
                                withPrefix(candidateKey).
                                withMaxKeys(1)
                );

                if (!listing.getObjectSummaries().isEmpty()) {
                    // subordinate objects so we need to pretend there is a directory
                    doAttachVirtualFolder();

                    if (log.isDebugEnabled()) {
                        log.debug("Attach folder to virtual S3 folder " + getName());
                    }

                    return;
                }

            } catch (AmazonServiceException ignored) {
            }

            // Create a new
            if (objectMetadataHolder == null) {
                doAttach(null, new ObjectMetadataHolder());

                if (log.isDebugEnabled()) {
                    log.debug("Attach to empty S3 object " + getName());
                }
            }
        } finally {
            checkCacheFile(objectMetadataHolder);
        }
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

    // avoid calling internally because it only partially detaches and is not thread safe by itself, call detachInternal() instead
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

    // should only be called when inputLock is locked
    private S3TempFile checkCacheFile(ObjectMetadataHolder metadata) {
        if (cacheFile != null) {
            if (metadata == null || !metadata.hasMD5Hash(cacheFile.getMD5Hash())) {
                // content has changed, let the cache file be deleted as soon as the last stream referencing it is closed
                cacheFile.release();
                cacheFile = null;
            }
        }
        return cacheFile;
    }

    @Override
    protected void doDelete() throws Exception {
        final String bucket = getBucketName();
        final String key = getName().getS3Key().orElseThrow(() -> new FileSystemException("Can't delete whole bucket"));

        if (log.isDebugEnabled()) {
            log.debug("Delete object [bucket=" + bucket + ",name=" + key + "]");
        }

        getService().deleteObject(bucket, key);
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

        InputStream input = new ByteArrayInputStream(new byte[0]);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        getService().putObject(new PutObjectRequest(getBucketName(), key, input, metadata));
    }

    @Override
    protected long doGetLastModifiedTime() throws Exception {
        return objectMetadataHolder.getLastModified();
    }

    @Override
    protected InputStream doGetInputStream() throws Exception {
        return downloadOnce().getInputStream();
    }

    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
        if (outputInProgress.compareAndSet(false, true)) {
            try {
                return new S3OutputStream(new S3TempFile());
            } catch (Exception t) {
                outputInProgress.set(false);

                throw t;
            }
        } else {
            throw new IOException("File already open for writing");
        }
    }

    @Override
    protected FileType doGetType() {
        // If we comes here then it is an imaginary file.
        return FileType.IMAGINARY;
    }

    @Override
    protected String[] doListChildren() {
        throw new UnsupportedOperationException("this should never get called since we override getChildren()");
    }

    @Override
    protected FileObject[] doListChildrenResolved() throws FileSystemException {
        assertType(FOLDER);

        final String path = getName().getS3Key().orElse("");

        ObjectListing listing = getService().listObjects(
                new ListObjectsRequest().
                        withBucketName(getBucketName()).
                        withDelimiter(SEPARATOR).
                        withPrefix(path)
        );

        final List<S3ObjectSummary> summaries = new ArrayList<>(listing.getObjectSummaries());
        final Set<String> commonPrefixes = new TreeSet<>(listing.getCommonPrefixes());

        while (listing.isTruncated()) {
            listing = getService().listNextBatchOfObjects(listing);

            summaries.addAll(listing.getObjectSummaries());
            commonPrefixes.addAll(listing.getCommonPrefixes());
        }

        List<FileObject> resolvedChildren = new ArrayList<>(summaries.size() + commonPrefixes.size());

        // add the prefixes (non-empty subdirs) first
        for (String commonPrefix : commonPrefixes) {
            // strip path from name (leave only base name)
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

        for (S3ObjectSummary summary : summaries) {
            if (!summary.getKey().equals(path)) {
                // strip path from name (leave only base name)
                final String stripPath = summary.getKey().substring(path.length());
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
        // Delete all keys
        return false;
    }

    // Utility methods

    /**
     * Download S3 object content and save it in temporary file.
     * Do it only if object was not already downloaded.
     */
    private S3TempFile downloadOnce() throws FileSystemException {
        final String objectPath = getName().getS3Key().orElseThrow(() -> new FileSystemException("Not able to download a bucket"));

        S3Object obj = null;
        S3TempFile tempFile = cacheFile;

        try {
            obj = getService().getObject(getBucketName(), objectPath);
            ObjectMetadataHolder holder = new ObjectMetadataHolder(obj.getObjectMetadata());

            if (log.isDebugEnabled()) {
                log.debug("Downloading S3 Object: " + objectPath);
            }

            tempFile = checkCacheFile(holder);

            if (tempFile == null) {
                tempFile = new S3TempFile();
                tempFile.setMD5Hash(holder.getMD5Hash());

                if (holder.getContentLength() > 0) {
                    InputStream is = obj.getObjectContent();

                    ReadableByteChannel rbc = null;
                    FileChannel cacheFc = null;

                    try {
                        rbc = Channels.newChannel(is);
                        cacheFc = tempFile.getFileChannel(StandardOpenOption.WRITE);

                        cacheFc.transferFrom(rbc, 0, obj.getObjectMetadata().getContentLength());
                    } finally {
                        if (rbc != null) {
                            try {
                                rbc.close();
                            } catch (IOException ignored) {
                            }
                        }

                        if (cacheFc != null) {
                            try {
                                cacheFc.close();
                            } catch (IOException ignored) {
                            }
                        }
                    }
                }
            }
        } catch (AmazonServiceException | IOException e) {
            final String failedMessage = "Failed to download S3 Object %s. %s";
            if (tempFile != null) {
                tempFile.release();
            }
            throw new FileSystemException(String.format(failedMessage, objectPath, e.getMessage()), e);
        } finally {
            if (obj != null) {
                try {
                    obj.close();
                } catch (IOException e) {
                    log.warn("Not able to close S3 object " + objectPath, e);
                }
            }
        }

        cacheFile = tempFile;

        return tempFile;
    }

    // ACL extension methods
    /**
     * Get S3 ACL list
     *
     * @return acl list
     */
    private AccessControlList getS3Acl() throws FileSystemException {
        Optional<String> key = getName().getS3Key();
        final String bucketName = getBucketName();

        if (key.isPresent()) {
            if ((getType() != FILE) && (getType() != FOLDER)) {
                throw new FileSystemException("Wrong type to get acl " + getName());
            }

            if (log.isDebugEnabled()) {
                log.debug("Get acl for object [bucket=" + bucketName + ",key=" + key.get() + "]");
            }

            return getService().getObjectAcl(bucketName, key.get());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Get acl for bucket " + bucketName);
            }

            return getService().getBucketAcl(bucketName);
        }
    }

    /**
     * Put S3 ACL list
     *
     * @param s3Acl acl list
     */
    private void putS3Acl(AccessControlList s3Acl) throws FileSystemException {
        Optional<String> key = getName().getS3Key();

        // Determine context. Object or Bucket
        if (key.isPresent()) {
            if ((getType() != FILE) && (getType() != FOLDER)) {
                throw new FileSystemException("Wrong type to put acl " + getName());
            }

            // Put ACL to S3
            getService().setObjectAcl(getBucketName(), key.get(), s3Acl);
        } else {
            getService().setBucketAcl(getBucketName(), s3Acl);
        }
    }

    /**
     * Returns access control list for this file.
     * <p>
     * VFS interfaces doesn't provide interface to manage permissions. ACL can be accessed through {@link FileObject#getFileOperations()}
     * Sample: <code>file.getFileOperations().getOperation(IAclGetter.class)</code>
     *
     * @return Current Access control list for a file
     * @throws FileSystemException if unable to get ACL
     * @see FileObject#getFileOperations()
     * @see IAclGetter
     */
    public Acl getAcl() throws FileSystemException {
        assertType(FILE, FOLDER);

        Acl myAcl = new Acl();
        AccessControlList s3Acl;
        try {
            s3Acl = getS3Acl();
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }

        // Get S3 file owner
        Owner owner = s3Acl.getOwner();

        // Read S3 ACL list and build VFS ACL.
        List<Grant> grants = s3Acl.getGrantsAsList();

        for (Grant item : grants) {
            // Map enums to jets3t ones
            Permission perm = item.getPermission();
            Acl.Permission[] rights;
            if (perm.equals(Permission.FullControl)) {
                rights = Acl.Permission.values();
            } else if (perm.equals(Permission.Read)) {
                rights = new Acl.Permission[1];
                rights[0] = READ;
            } else if (perm.equals(Permission.Write)) {
                rights = new Acl.Permission[1];
                rights[0] = WRITE;
            } else {
                // Skip unknown permission
                log.error("Skip unknown permission " + perm);

                continue;
            }

            // Set permissions for groups
            if (item.getGrantee() instanceof GroupGrantee) {
                GroupGrantee grantee = (GroupGrantee) item.getGrantee();
                if (GroupGrantee.AllUsers.equals(grantee)) {
                    // Allow rights to GUEST
                    myAcl.allow(Acl.Group.EVERYONE, rights);
                } else if (GroupGrantee.AuthenticatedUsers.equals(grantee)) {
                    // Allow rights to AUTHORIZED
                    myAcl.allow(Acl.Group.AUTHORIZED, rights);
                }
            } else if (item.getGrantee() instanceof CanonicalGrantee) {
                CanonicalGrantee grantee = (CanonicalGrantee) item.getGrantee();
                if (grantee.getIdentifier().equals(owner.getId())) {
                    // The same owner and grantee understood as OWNER group
                    myAcl.allow(Acl.Group.OWNER, rights);
                }
            }

        }

        return myAcl;
    }

    /**
     * Returns access control list for this file.
     * <p>
     * VFS interfaces doesn't provide interface to manage permissions. ACL can be accessed through {@link FileObject#getFileOperations()}
     * Sample: <code>file.getFileOperations().getOperation(IAclGetter.class)</code>
     *
     * @param acl the access control list
     * @throws FileSystemException if unable to set ACL
     * @see FileObject#getFileOperations
     * @see IAclGetter
     */
    public void setAcl(Acl acl) throws FileSystemException {
        assertType(FILE, FOLDER);

        // Create empty S3 ACL list
        AccessControlList s3Acl = new AccessControlList();

        // Get file owner
        Owner owner;

        try {
            owner = getS3Acl().getOwner();
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }

        s3Acl.setOwner(owner);

        // Iterate over VFS ACL rules and fill S3 ACL list
        Map<Acl.Group, Acl.Permission[]> rules = acl.getRules();

        final Acl.Permission[] allRights = Acl.Permission.values();

        for (Acl.Group group : rules.keySet()) {
            Acl.Permission[] rights = rules.get(group);

            if (rights.length == 0) {
                // Skip empty rights
                continue;
            }

            // Set permission
            Permission perm;
            if (Arrays.equals(rights, allRights)) {
                perm = Permission.FullControl;
            } else if (acl.isAllowed(group, READ)) {
                perm = Permission.Read;
            } else if (acl.isAllowed(group, WRITE)) {
                perm = Permission.Write;
            } else {
                log.error("Skip unknown set of rights " + Arrays.toString(rights));

                continue;
            }

            // Set grantee
            Grantee grantee;
            if (group.equals(Acl.Group.EVERYONE)) {
                grantee = GroupGrantee.AllUsers;
            } else if (group.equals(Acl.Group.AUTHORIZED)) {
                grantee = GroupGrantee.AuthenticatedUsers;
            } else if (group.equals(Acl.Group.OWNER)) {
                grantee = new CanonicalGrantee(owner.getId());
            } else {
                log.error("Skip unknown group " + group);

                continue;
            }

            // Grant permission
            s3Acl.grantPermission(grantee, perm);
        }

        // Put ACL to S3
        try {
            putS3Acl(s3Acl);
        } catch (Exception e) {
            throw new FileSystemException(e);
        }
    }

    /**
     * Temporary accessible url for object.
     * @param expireInSeconds seconds until expiration
     * @return temporary accessible url for object
     * @throws FileSystemException if unable to get signed URL
     */
    public String getSignedUrl(int expireInSeconds) throws FileSystemException {
        assertType(FILE, FOLDER);

        final Calendar cal = Calendar.getInstance();

        cal.add(SECOND, expireInSeconds);

        try {
            return getService().generatePresignedUrl(
                    getBucketName(),
                    getName().getS3Key().orElseThrow(() -> new FileSystemException("Not able get presigned url for a bucket")),
                    cal.getTime()
            ).toString();
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }
    }

    /**
     * Get MD5 hash for the file
     * @return md5 hash for file
     * @throws FileSystemException if unable to get MD5Hash
     */
    public Optional<String> getMD5Hash() throws FileSystemException {
        assertType(FILE, FOLDER);

        return ofNullable(objectMetadataHolder).map(ObjectMetadataHolder::getMD5Hash);
    }

    /**
     * Get object metadata hash for the file
     *
     * @return the object metadata
     * @throws FileSystemException if unablt to get the object metadata
     */
    public Optional<String> getSSEAlgorithm() throws FileSystemException {
        assertType(FILE, FOLDER);

        if (objectMetadataHolder.isVirtual()) {
            if (log.isDebugEnabled()) {
                log.debug("Have to fetch real metadata for [" + getName() + "]");
            }

            refresh();
            getType(); // Force fetch metadata from S3
        }

        if (objectMetadataHolder.isVirtual()) {
            throw new FileSystemException("Not able to fetch real metadata from " + getName());
        }

        return of(objectMetadataHolder).map(ObjectMetadataHolder::getServerSideEncryption);
    }

    /**
     * Returns file that was used as local cache. Useful to do something with local tools like image resizing and so on
     *
     * @return absolute path to file or nul if nothing were downloaded
     */
    public String getCacheFile() {
        if (cacheFile != null) {
            return cacheFile.getAbsolutePath();
        } else {
            return null;
        }
    }

    protected void assertType(FileType ... types) throws FileSystemException {
        final FileType type = getType();
        boolean val = false;

        for (FileType t : types) {
            val = (type == t) || val;
        }

        if (!val) {
            throw new FileSystemException("File type should be one of " + Arrays.toString(types));
        }
    }

    @SuppressWarnings("WeakerAccess")
    protected AmazonS3 getService() {
        return ((S3FileSystem) getFileSystem()).getService();
    }

    @SuppressWarnings("WeakerAccess")
    protected TransferManager getTransferManager() {
        return ((S3FileSystem) getFileSystem()).getTransferManager();
    }

    /**
     * Amazon S3 bucket
     */
    protected String getBucketName() {
        return ((S3FileName) getFileSystem().getRootName()).getBucket();
    }

    /**
     * Output stream that saves all contents in temporary file, onClose sends contents to S3.
     *
     * @author Marat Komarov
     * @author Shon Vella
     */
    private class S3OutputStream extends MonitorOutputStream {
        private S3TempFile outputFile;

        S3OutputStream(S3TempFile outputFile) throws IOException {
            super(outputFile.getOutputStream());

            this.outputFile = outputFile;
        }

        @Override
        protected void onClose() throws IOException {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Start to upload file " + outputFile.getPath());
                }

                String md5 = upload(outputFile.getPath().toFile());
                outputFile.setMD5Hash(md5);

                if (cacheFile != null) {
                    cacheFile.release();
                    cacheFile = null;
                }

                refresh();

                cacheFile = outputFile;
                cacheFile.use();
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                outputFile.release();
                outputFile = null;
                outputInProgress.set(false);
            }
        }
    }

    /**
     * Queries the object if a simple rename to the filename of <code>newfile</code> is possible.
     *
     * @param newfile the new filename
     * @return true if rename is possible
     */
    @Override
    public boolean canRenameTo(FileObject newfile) {
        return false;
    }

    /**
     * Copies another file to this file.
     *
     * @param file     The FileObject to copy.
     * @param selector The FileSelector.
     * @throws FileSystemException if an error occurs.
     */
    @Override
    public void copyFrom(final FileObject file, final FileSelector selector) throws FileSystemException {
        if (!file.exists()) {
            throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
        }
        // Locate the files to copy across
        final ArrayList<FileObject> files = new ArrayList<>();
        file.findFiles(selector, false, files);

        Map<FileObject, FileObject> filesToCopy = new HashMap<>();

        // Copy everything across
        for (FileObject srcFile : files) {
            final FileObject unwrappedSrcFile = FileObjectUtils.unwrap(srcFile);
            // Determine the destination file
            final String relPath = file.getName().getRelativeName(unwrappedSrcFile.getName());
            final FileObject destFile = resolveFile(relPath, DESCENDENT_OR_SELF);
            final FileObject unwrappedDestFile = FileObjectUtils.unwrap(destFile);

            if (!allowS3Copy(unwrappedSrcFile, unwrappedDestFile)) {
                log.warn(
                        "One of files don't allow S3 copy - fallback to default implementation [from=" +
                                unwrappedSrcFile +
                                ",to=" +
                                unwrappedDestFile +
                                "]"
                );

                super.copyFrom(file, selector);

                refresh();

                return;
            }

            filesToCopy.put(unwrappedSrcFile, unwrappedDestFile);
        }

        for (Map.Entry<FileObject, FileObject> entry : filesToCopy.entrySet()) {
            final FileObject source = entry.getKey();
            final FileObject destination = entry.getValue();

            doCopyFrom(source, destination);
        }

        refresh();
    }

    protected boolean allowS3Copy(FileObject fromFile, FileObject toFile) throws FileSystemException {
        if (fromFile.getType().hasChildren()) {
            return true;
        } else if ((fromFile instanceof S3FileObject) && (((S3FileObject) fromFile).sameFileSystem(toFile))) {
            return true;
        } else if (fromFile.getType().hasContent() && fromFile.getURL().getProtocol().equals("file") && (toFile instanceof S3FileObject)) {
            try {
                fromFile.getURL().toURI();

                return true;
            } catch (URISyntaxException e) {
            }
        }

        return false;
    }

    /**
     *
     * @param fromFile
     * @param toFile
     * @throws FileSystemException
     */
    protected void doCopyFrom(FileObject fromFile, FileObject toFile) throws FileSystemException {
        if (log.isDebugEnabled()) {
            log.debug("Do S3 copy [from=" + fromFile + ",to=" + toFile + "]");
        }

        // Clean up the destination file, if necessary
        if (toFile.exists()) {
            if (fromFile.getType() != toFile.getType()) {
                // The destination file exists, and is not of the same type,
                // so delete it
                // TODO - add a pluggable policy for deleting and overwriting existing files
                toFile.delete(Selectors.SELECT_ALL);
            }
        } else {
            FileObject parent = getParent();

            if (parent != null) {
                parent.createFolder();
            }
        }

        // Copy across
        try {
            if (fromFile.getType().hasChildren()) {
                toFile.createFolder();
            } else if ((fromFile instanceof S3FileObject) && (((S3FileObject) fromFile).sameFileSystem(toFile))) {
                S3FileObject s3SrcFile = (S3FileObject) fromFile;
                S3FileObject s3DestFile = (S3FileObject) toFile;

                // do server side copy if both source and dest are in same file system

                String srcBucketName = s3SrcFile.getBucketName();
                String srcFileName = s3SrcFile.getName().getS3Key().orElseThrow(() -> new FileSystemException("Not able to copy whole bucket"));
                String destBucketName = s3DestFile.getBucketName();
                String destFileName = s3DestFile.getName().getS3KeyAs(FILE); // Because target could be not exists

                if (!s3SrcFile.exists()) {
                    throw new FileSystemException("Source file doesn't exist [" + s3SrcFile + "]");
                }

                if (!s3DestFile.exists()) {
                    s3DestFile.createFile();
                }

                CopyObjectRequest copy = new CopyObjectRequest(srcBucketName, srcFileName, destBucketName, destFileName);

                if (s3SrcFile.getType() == FILE) {
                    if (s3SrcFile.objectMetadataHolder.isVirtual()) {
                        s3SrcFile.refresh();
                        s3SrcFile.getType(); // Force fetch metadata from S3
                    }

                    if (s3SrcFile.objectMetadataHolder.isVirtual()) {
                        throw new FileSystemException("Not able to fetch real metadata from " + getName());
                    }

                    s3SrcFile.objectMetadataHolder.withServerSideEncryption(getServerSideEncryption()).sendWith(copy);
                }

                getService().copyObject(copy);
            } else if (fromFile.getType().hasContent() && fromFile.getURL().getProtocol().equals("file") && (toFile instanceof S3FileObject)) {
                // do direct upload from file to avoid overhead of making a copy of the file
                S3FileObject s3DestFile = (S3FileObject) toFile;

                try {
                    File localFile = new File(fromFile.getURL().toURI());

                    s3DestFile.upload(localFile);
                } catch (URISyntaxException e) {
                    // couldn't convert URL to URI, but should still be able to do the slower way
                }
            }
        } catch (IOException | AmazonClientException e) {
            throw new FileSystemException("vfs.provider/copy-file.error", e, fromFile, toFile);
        } finally {
            toFile.close();
        }
    }

    /**
     * Check S3 file system endpoint and credentials
     *
     * @param toFile
     * @return
     */
    protected boolean sameFileSystem(FileObject toFile) {
        if (!(toFile instanceof S3FileObject)) {
            return false;
        }

        final S3FileName destFile = ((S3FileObject) toFile).getName();

        return (
                Objects.equals(getName().getEndpoint(), destFile.getEndpoint()) &&
                Objects.equals(getName().getAccessKey(), destFile.getAccessKey()) &&
                Objects.equals(getName().getSecretKey(), destFile.getSecretKey())
        );
    }

    /**
     * Uploads File to S3
     *
     * @param file the File
     * @return the eTag of the uploaded result
     * @throws IOException if the upload failed
     */
    private String upload(File file) throws IOException {
        final String key = (getType() == IMAGINARY) ?
                getName().getS3KeyAs(FILE) :
                getName().getS3Key().orElseThrow(() -> new FileSystemException("Not able to copy whole bucket"));

        PutObjectRequest request = new PutObjectRequest(getBucketName(), key, file);

        new ObjectMetadataHolder().
                withContentLength(file.length()).
                withContentType(getName().getBaseName()).
                withServerSideEncryption(getServerSideEncryption()).
                sendWith(request);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Upload request [file=" +
                    file +
                    ",key=" +
                    key +
                    ",length=" +
                    file.length() +
                    ",type=" +
                    getName().getBaseName() +
                    "]"
            );
        }

        try {
            Upload upload = getTransferManager().upload(request);

            return upload.waitForUploadResult().getETag();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    private boolean getServerSideEncryption() {
        if (!getName().supportsSSE()) {
            return false;
        }

        return (new S3FileSystemOptions(getFileSystem().getFileSystemOptions())).getServerSideEncryption();
    }
}
