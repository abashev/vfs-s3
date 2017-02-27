package com.github.vfss3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.github.vfss3.operations.Acl;
import com.github.vfss3.operations.IAclGetter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.util.MonitorOutputStream;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;
import static com.github.vfss3.AmazonS3ClientHack.extractCredentials;
import static com.github.vfss3.operations.Acl.Permission.READ;
import static com.github.vfss3.operations.Acl.Permission.WRITE;
import static java.util.Calendar.SECOND;
import static org.apache.commons.vfs2.FileName.SEPARATOR;
import static org.apache.commons.vfs2.NameScope.CHILD;
import static org.apache.commons.vfs2.util.FileObjectUtils.getAbstractFileObject;

/**
 * Implementation of the virtual S3 file system object using the AWS-SDK.<p/>
 * Based on Matthias Jugel code.
 * {@link http://thinkberg.com/svn/moxo/trunk/modules/vfs.s3/ }
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 * @author Shon Vella
 */
@SuppressWarnings({"JavadocReference", "unused"})
public class S3FileObject extends AbstractFileObject<S3FileSystem> {
    private static final Log logger = LogFactory.getLog(S3FileObject.class);

    private static final String MIMETYPE_JETS3T_DIRECTORY = "application/x-directory";

    /**
     * Amazon S3 object
     */
    private ObjectMetadata objectMetadata;
    private String objectKey;

    /**
     * Local cache of file content
     */
    private S3TempFile cacheFile;

    /**
     * Local flag that tells us to ignore attach/detach because we know S3 data is up-to-date
     */
    private boolean attachingMetaData = false;

    /**
     * Amazon file owner. Used in ACL
     */
    private Owner fileOwner;

    /**
     * Cache of children;
     */
    private final AtomicReference<List<FileObject>> childCache = new AtomicReference<>();

    /**
     * Lock to allow only one getChildren() at a time so all threads can benefit from the work of one
     */
    private final Lock getChildrenLock = new ReentrantLock();

    /**
     * Lock to control access to input stream cache
     */
    private final Lock inputLock = new ReentrantLock();

    /**
     * Lock to control output stream - not thread specific, just based on the output stream being open
     */
    private final AtomicBoolean outputInProgress = new AtomicBoolean();

    /**
     * Lock copy of monitorLock used by AbstractFileObject
     */
    private final Object monitorLock;


    @SuppressWarnings("WeakerAccess")
    public S3FileObject(AbstractFileName fileName,
                        S3FileSystem fileSystem) throws FileSystemException {
        super(fileName, fileSystem);
        monitorLock = fileSystem.isPerFileLocking() ? this : fileSystem;
    }

    /**
     * Attaches to the file.
     *
     * @throws FileSystemException if an error occurs.
     */
    private void attachInternal() throws FileSystemException {
        // since AbstractFileObject doesn't expose attach(), we need need a way to call it to ensure fully attached
        // and getType() is a cheap way to do that
        getType();
    }

    /**
     * Reattaches to a file.
     *
     * @throws FileSystemException if an error occurs.
     */
    private void reattachInternal() throws FileSystemException {
        synchronized (monitorLock) {
            if (isAttached()) {
                doDetach();
                doAttach();
            } else {
                attachInternal();
            }
        }
    }

    private void detachInternal() throws FileSystemException {
        // since AbstractFileObject doesn't expose detach(), we need need a way to call it to ensure fully detached
        // and reset() is a cheap way to do that
        refresh();
    }

    // avoid calling internally because it only partially attaches and is not thread safe by itself, call attachInternal() instead
    @Override
    protected void doAttach() throws FileSystemException {
        try {
            if (!attachingMetaData) {
                try {
                    // Do we have file with name?
                    String candidateKey = getS3Key();
                    objectMetadata = getService().getObjectMetadata(getBucket().getName(), candidateKey);
                    objectKey = candidateKey;

                    logger.debug("Attach file to S3 Object [" + objectKey + "]");
                    return;
                } catch (AmazonClientException e) {
                    // We are attempting to attach to the root bucket
                }

                try {
                    // Do we have folder with that name?
                    String candidateKey = getS3Key() + FileName.SEPARATOR;
                    objectMetadata = getService().getObjectMetadata(getBucket().getName(), candidateKey);
                    objectKey = candidateKey;
                    logger.debug("Attach folder to S3 Object [" + objectKey + "]");
                    return;
                } catch (AmazonServiceException e) {
                    // No, we don't
                }

                try {
                    // Do, we have subordinate objects
                    String candidateKey = getS3Key() + FileName.SEPARATOR;
                    final ListObjectsRequest loReq = new ListObjectsRequest();
                    loReq.setBucketName(getBucket().getName());
                    loReq.setPrefix(candidateKey);
                    loReq.setMaxKeys(1);

                    ObjectListing listing = getService().listObjects(loReq);
                    if (!listing.getObjectSummaries().isEmpty()) {
                        // subordinate objects so we need to pretend there is a directory
                        objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentLength(0);
                        objectMetadata.setContentType(
                                Mimetypes.getInstance().getMimetype(getName().getBaseName()));
                        objectMetadata.setLastModified(new Date());
                        objectKey = candidateKey;
                        logger.debug("Attach folder to virtual S3 Folder [" + objectKey + "]");
                        return;
                    }

                } catch (AmazonServiceException ignored) {
                }

                // Create a new
                if (objectMetadata == null) {
                    objectMetadata = new ObjectMetadata();
                    objectKey = getS3Key();
                    objectMetadata.setLastModified(new Date());

                    logger.debug("Attach new S3 Object [" + objectKey + "]");
                }
            }
        } finally {
            if (inputLock.tryLock()) {
                try {
                    checkCacheFile(objectMetadata);
                } finally {
                    inputLock.unlock();
                }
            }
        }
    }

    // avoid calling internally because it only partially detaches and is not thread safe by itself, call detachInternal() instead
    @Override
    protected void doDetach() {
        if (!attachingMetaData) {
            logger.debug("Detach from S3 Object [" + objectKey + "]");
            objectMetadata = null;
        }
        fileOwner = null;
        childCache.set(null);
    }

    // should only be called when inputLock is locked
    private S3TempFile checkCacheFile(ObjectMetadata md) throws FileSystemException {
        if (cacheFile != null) {
            if (md == null || md.getETag() == null || !md.getETag().equals(cacheFile.getETag())) {
                // content has changed, let the cache file be deleted as soon as the last stream referencing it is closed
                cacheFile.release();
                cacheFile = null;
            }
        }
        return cacheFile;
    }

    private void attachMetadata(String key, ObjectMetadata metadata) throws FileSystemException {
        synchronized (monitorLock) {
            attachingMetaData = true;
            try {
                this.objectKey = key;
                this.objectMetadata = metadata;
                reattachInternal();
            } finally {
                attachingMetaData = false;
            }
        }
    }

    @Override
    protected void doDelete() throws Exception {
        getService().deleteObject(getBucket().getName(), objectKey);
    }

    @Override
    protected void doCreateFolder() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Create new folder in bucket [" +
                            ((getBucket() != null) ? getBucket().getName() : "null") +
                            "] with key [" +
                            ((objectMetadata != null) ? objectKey : "null") +
                            "]"
            );
        }

        if (objectMetadata == null) {
            return;
        }

        InputStream input = new ByteArrayInputStream(new byte[0]);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        if (getServerSideEncryption()) {
            metadata.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
        }

        String dirName = objectKey.endsWith(SEPARATOR) ? objectKey : objectKey + SEPARATOR;
        getService().putObject(new PutObjectRequest(getBucket().getName(), dirName, input, metadata));
    }

    @Override
    protected long doGetLastModifiedTime() throws Exception {
        Date lastModified = objectMetadata.getLastModified();

        return (lastModified != null) ? lastModified.getTime() : 0L;
    }

    @Override
    protected boolean doSetLastModifiedTime(final long modtime) throws Exception {
        long oldModified = objectMetadata.getLastModified().getTime();
        boolean differentModifiedTime = oldModified != modtime;
        if (differentModifiedTime) {
            objectMetadata.setLastModified(new Date(modtime));
        }
        return differentModifiedTime;
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
            } catch (Throwable t) {
                outputInProgress.set(false);
                if (t instanceof Exception) {
                    throw (Exception) t;
                } else {
                    throw new Exception(t);
                }
            }
        } else {
            throw new IOException("File already open for writing");
        }
    }

    @Override
    protected FileType doGetType() throws Exception {
        if (objectMetadata.getContentType() == null) {
            return FileType.IMAGINARY;
        }

        if ("".equals(objectKey) || isDirectoryPlaceholder()) {
            return FileType.FOLDER;
        }

        return FileType.FILE;
    }

    /**
     * Returns the children of the file. This completely replaces the implementation in AbstractFileObject
     * because it deadlocks with concurrent calls to getParent() when using LockByFileStrategyFactory
     * Also it caches child names and re-resolves each name individually when it uses the cached list and
     * that is grossly inefficient for S3
     *
     * @return an array of FileObjects, one per child.
     * @throws FileSystemException if an error occurs.
     */
    @Override
    public FileObject[] getChildren() throws FileSystemException {
        interruptibleLock(getChildrenLock);
        try {
            FileType type = getType();
            List<FileObject> children = childCache.get();
            if (childCache.get() == null) {
                try {
                    children = doListChildrenResolvedExt();
                    childCache.set(Collections.synchronizedList(children));
                } catch (final Exception exc) {
                    throw new FileSystemException("vfs.provider/list-children.error", exc, getName());
                }
            }
            return children.toArray(new FileObject[children.size()]);
        } finally {
            getChildrenLock.unlock();
        }
    }

    @Override
    protected String[] doListChildren() throws Exception {
        // this should never get called since we
        // overrode getChildren()
        return new String[0];
    }

    /**
     * Lists the children of this file.
     *
     * @return The children of this FileObject.
     * @throws Exception if an error occurs.
     */
    private List<FileObject> doListChildrenResolvedExt() throws Exception {
        attachInternal();
        String path = objectKey;

        // make sure we add a '/' slash at the end to find children
        if ((!"".equals(path)) && (!path.endsWith(SEPARATOR))) {
            path = path + "/";
        }

        final ListObjectsRequest loReq = new ListObjectsRequest();
        loReq.setBucketName(getBucket().getName());
        loReq.setDelimiter("/");
        loReq.setPrefix(path);

        ObjectListing listing = getService().listObjects(loReq);
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
            if (!stripPath.equals("/")) {
                FileObject childObject = resolveFile(stripPath, CHILD);
                S3FileObject s3FileObject = (S3FileObject) getAbstractFileObject(childObject);
                if (s3FileObject != null) {
                    ObjectMetadata childMetadata = new ObjectMetadata();
                    childMetadata.setContentLength(0);
                    childMetadata.setContentType(
                            Mimetypes.getInstance().getMimetype(s3FileObject.getName().getBaseName()));
                    childMetadata.setLastModified(new Date());
                    s3FileObject.attachMetadata(commonPrefix, childMetadata);
                }
                resolvedChildren.add(childObject);
            }
        }

        for (S3ObjectSummary summary : summaries) {
            if (!summary.getKey().equals(path)) {
                // strip path from name (leave only base name)
                final String stripPath = summary.getKey().substring(path.length());
                FileObject childObject = resolveFile(stripPath, CHILD);
                S3FileObject s3FileObject = (S3FileObject) getAbstractFileObject(childObject);
                if (s3FileObject != null) {
                    ObjectMetadata childMetadata = new ObjectMetadata();
                    childMetadata.setContentLength(summary.getSize());
                    childMetadata.setContentType(
                            Mimetypes.getInstance().getMimetype(s3FileObject.getName().getBaseName()));
                    childMetadata.setLastModified(summary.getLastModified());
                    childMetadata.setHeader(Headers.ETAG, summary.getETag());
                    s3FileObject.attachMetadata(summary.getKey(), childMetadata);
                }
                resolvedChildren.add(childObject);
            }
        }

        return resolvedChildren;
    }

    /**
     * Called when the children of this file change.  Allows subclasses to
     * refresh any cached information about the children of this file.
     * <p>
     * This implementation does nothing.
     *
     * @param child   The name of the child that changed.
     * @param newType The type of the file.
     * @throws Exception if an error occurs.
     */
    protected void onChildrenChanged(final FileName child, final FileType newType) throws Exception {
        List<FileObject> children = childCache.get();
        if (children != null) {
            FileObject childFile = getFileSystem().resolveFile(child);
            if (newType.equals(FileType.IMAGINARY)) {
                children.remove(childFile);
            } else {
                children.add(childFile);
            }
        }
    }


    @Override
    protected long doGetContentSize() throws Exception {
        return objectMetadata.getContentLength();
    }

    // Utility methods

    /**
     * Download S3 object content and save it in temporary file.
     * Do it only if object was not already downloaded.
     */
    private S3TempFile downloadOnce() throws FileSystemException {
        interruptibleLock(inputLock);
        try {
            final String objectPath = getName().getPath();

            S3Object obj = null;
            S3TempFile tempFile = cacheFile;

            try {
                obj = getService().getObject(getBucket().getName(), objectKey);
                ObjectMetadata md = obj.getObjectMetadata();

                logger.debug(String.format("Downloading S3 Object: %s", objectPath));

                tempFile = checkCacheFile(md);
                if (tempFile == null) {
                    tempFile = new S3TempFile();
                    tempFile.setETag(md.getETag());

                    if (md.getContentLength() > 0) {
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
                        logger.warn("Not able to close S3 object [" + objectPath + "]", e);
                    }
                }
            }
            cacheFile = tempFile;
            return tempFile;
        } finally {
            inputLock.unlock();
        }
    }

    /**
     * Same as in Jets3t library, to be compatible.
     */
    private boolean isDirectoryPlaceholder() {
        // Recognize "standard" directory place-holder indications used by
        // Amazon's AWS Console and Panic's Transmit.
        if (objectKey.endsWith("/") && objectMetadata.getContentLength() == 0) {
            return true;
        }

        // Recognize s3sync.rb directory placeholders by MD5/ETag value.
        if ("d66759af42f282e1ba19144df2d405d0".equals(objectMetadata.getETag())) {
            return true;
        }

        // Recognize place-holder objects created by the Google Storage console
        // or S3 Organizer Firefox extension.
        if (objectKey.endsWith("_$folder$") && (objectMetadata.getContentLength() == 0)) {
            return true;
        }

        // Recognize legacy JetS3t directory place-holder objects, only gives
        // accurate results if an object's metadata is populated.
        if (objectMetadata.getContentLength() == 0
                && MIMETYPE_JETS3T_DIRECTORY.equals(objectMetadata.getContentType())) {
            return true;
        }
        return false;
    }


    /**
     * Create an S3 key from a commons-vfs path. This simply strips the slash
     * from the beginning if it exists.
     *
     * @return the S3 object key
     */
    private String getS3Key() {
        return getS3Key(getName());
    }

    private String getS3Key(FileName fileName) {
        String path = fileName.getPath();

        if ("".equals(path)) {
            return path;
        } else {
            return path.substring(1);
        }
    }

    // ACL extension methods

    /**
     * Returns S3 file owner.
     * Loads it from S3 if needed.
     */
    private Owner getS3Owner() throws FileSystemException {
        if (fileOwner == null) {
            AccessControlList s3Acl = getS3Acl();
            fileOwner = s3Acl.getOwner();
        }
        return fileOwner;
    }

    /**
     * Get S3 ACL list
     *
     * @return acl list
     */
    private AccessControlList getS3Acl() throws FileSystemException {
        final String key = getS3Key();
        final String bucketName = getBucket().getName();

        if ("".equals(key)) {
            logger.debug("Get acl for bucket [" + bucketName + "]");

            return getService().getBucketAcl(bucketName);
        } else {
            synchronized (monitorLock) {
                attachInternal();
                logger.debug("Get acl for object [bucket=" + bucketName + ",key=" + objectKey + "]");
                return getService().getObjectAcl(bucketName, objectKey);
            }
        }
    }

    /**
     * Put S3 ACL list
     *
     * @param s3Acl acl list
     */
    private void putS3Acl(AccessControlList s3Acl) throws FileSystemException {
        String key = getS3Key();
        // Determine context. Object or Bucket
        if ("".equals(key)) {
            getService().setBucketAcl(getBucket().getName(), s3Acl);
        } else {
            // Before any operations with object it must be attached
            attachInternal();
            // Put ACL to S3
            getService().setObjectAcl(getBucket().getName(), objectKey, s3Acl);
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
        Acl myAcl = new Acl();
        AccessControlList s3Acl;
        try {
            s3Acl = getS3Acl();
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }

        // Get S3 file owner
        Owner owner = s3Acl.getOwner();
        fileOwner = owner;

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
                logger.error(String.format("Skip unknown permission %s", perm));
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

        // Create empty S3 ACL list
        AccessControlList s3Acl = new AccessControlList();

        // Get file owner
        Owner owner;
        try {
            owner = getS3Owner();
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
                logger.error(String.format("Skip unknown set of rights %s", Arrays.toString(rights)));
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
                logger.error(String.format("Skip unknown group %s", group));
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
     * Get direct http url to S3 object.
     *
     * @return the direct http url to S3 object
     */
    public String getHttpUrl() {
        StringBuilder sb = new StringBuilder("http://" + getBucket().getName() + ".s3.amazonaws.com/");
        String key = getS3Key();

        // Determine context. Object or Bucket
        if ("".equals(key)) {
            return sb.toString();
        } else {
            return sb.append(key).toString();
        }
    }

    /**
     * Get private url with access key and secret key.
     *
     * @return the private url
     */
    public String getPrivateUrl() throws FileSystemException {
        AWSCredentials awsCredentials = extractCredentials(getService());

        if (awsCredentials == null) {
            throw new FileSystemException("Not able to build private URL - empty AWS credentials");
        }

        return String.format(
                "s3://%s:%s@%s/%s",
                awsCredentials.getAWSAccessKeyId(),
                awsCredentials.getAWSSecretKey(),
                getBucket().getName(),
                getS3Key()
        );
    }

    /**
     * Temporary accessible url for object.
     * @param expireInSeconds seconds until expiration
     * @return temporary accessible url for object
     * @throws FileSystemException if unable to get signed URL
     */
    public String getSignedUrl(int expireInSeconds) throws FileSystemException {
        final Calendar cal = Calendar.getInstance();

        cal.add(SECOND, expireInSeconds);

        try {
            return getService().generatePresignedUrl(
                    getBucket().getName(),
                    getS3Key(), cal.getTime()).toString();
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }
    }

    /**
     * Get MD5 hash for the file
     * @return md5 hash for file
     * @throws FileSystemException if unable to get MD5Hash
     */
    public String getMD5Hash() throws FileSystemException {
        String hash = null;

        ObjectMetadata metadata = getObjectMetadata();
        if (metadata != null) {
            hash = metadata.getETag(); // TODO this is something different than mentioned in methodname / javadoc
        }

        return hash;
    }

    /**
     * Get object metadata hash for the file
     *
     * @return the object metadata
     * @throws FileSystemException if unablt to get the object metadata
     */
    public ObjectMetadata getObjectMetadata() throws FileSystemException {
        try {
            synchronized (monitorLock) {
                attachInternal();
                return getService().getObjectMetadata(getBucket().getName(), getS3Key());
            }
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }
    }

    /**
     * Returns file that was used as local cache. Useful to do something with local tools like image resizing and so on
     *
     * @return absolute path to file or nul if nothing were downloaded
     */
    public String getCacheFile() {
        try {
            interruptibleLock(inputLock);
        } catch (FileSystemException e) {
            // should probably allow this to propagate, but don't want to change method signature
            return null;
        }
        try {
            if (cacheFile != null) {
                return cacheFile.getAbsolutePath();
            } else {
                return null;
            }
        } finally {
            inputLock.unlock();
        }
    }

    @SuppressWarnings("WeakerAccess")
    protected AmazonS3 getService() {
        return ((S3FileSystem) getFileSystem()).getService();
    }

    /**
     * Amazon S3 bucket
     */
    protected Bucket getBucket() {
        return ((S3FileSystem) getFileSystem()).getBucket();
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
                String eTag = upload(outputFile.getPath().toFile());
                outputFile.setETag(eTag);
                interruptibleLock(inputLock);
                try {
                    if (cacheFile != null) {
                        cacheFile.release();
                        cacheFile = null;
                    }
                    detachInternal();
                    cacheFile = outputFile;
                    cacheFile.use();
                } finally {
                    inputLock.unlock();
                }
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
    public void copyFrom(final FileObject file, final FileSelector selector)
            throws FileSystemException {
        if (!file.exists()) {
            throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
        }
        // Locate the files to copy across
        final ArrayList<FileObject> files = new ArrayList<>();
        file.findFiles(selector, false, files);

        // Copy everything across
        for (FileObject srcFile : files) {
            final AbstractFileObject unwrappedSrcFile = getAbstractFileObject(srcFile);
            // Determine the destination file
            final String relPath = file.getName().getRelativeName(srcFile.getName());
            final FileObject destFile = resolveFile(relPath, NameScope.DESCENDENT_OR_SELF);
            final AbstractFileObject unwrappedDestFile = getAbstractFileObject(destFile);


            // Clean up the destination file, if necessary
            if (destFile.exists()) {
                if (destFile.getType() != srcFile.getType()) {
                    // The destination file exists, and is not of the same type,
                    // so delete it
                    // TODO - add a pluggable policy for deleting and overwriting existing files
                    destFile.delete(Selectors.SELECT_ALL);
                }
            } else {
                FileObject parent = getParent();
                if (parent != null) {
                    parent.createFolder();
                }
            }

            // Copy across
            try {
                if (srcFile.getType().hasChildren()) {
                    destFile.createFolder();
                    // do server side copy if both source and dest are in same file system
                    // (we could probably check credentials instead of file system to allow bucket-to-bucket direct
                    // copy, but that would be a pita
                } else if (srcFile instanceof S3FileObject && unwrappedDestFile instanceof S3FileObject
                        && srcFile.getFileSystem() == unwrappedDestFile.getFileSystem()) {
                    S3FileObject s3SrcFile = (S3FileObject) srcFile;
                    S3FileObject s3DestFile = (S3FileObject) unwrappedDestFile;
                    String srcBucketName = s3SrcFile.getBucket().getName();
                    String srcFileName = s3SrcFile.getS3Key();
                    String destBucketName = s3DestFile.getBucket().getName();
                    String destFileName = s3DestFile.getS3Key();
                    CopyObjectRequest copy = new CopyObjectRequest(
                            srcBucketName, srcFileName, destBucketName, destFileName);
                    if (srcFile.getType() == FileType.FILE && getServerSideEncryption()) {
                        ObjectMetadata meta = s3SrcFile.getObjectMetadata();
                        meta.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
                        copy.setNewObjectMetadata(meta);
                    }
                    getService().copyObject(copy);
                } else if (srcFile.getType().hasContent() && srcFile.getURL().getProtocol().equals("file") && unwrappedDestFile instanceof S3FileObject) {
                    // do direct upload from file to avoid overhead of making a copy of the file
                    S3FileObject s3DestFile = (S3FileObject) unwrappedDestFile;
                    try {
                        File localFile = new File(srcFile.getURL().toURI());
                        s3DestFile.upload(localFile);
                    } catch (URISyntaxException e) {
                        // couldn't convert URL to URI, but should still be able to do the slower way
                        super.copyFrom(file, selector);
                    }
                } else {
                    // fall back to default implementation
                    super.copyFrom(file, selector);
                }
            } catch (IOException | AmazonClientException e) {
                throw new FileSystemException("vfs.provider/copy-file.error", e, srcFile, destFile);
            } finally {
                destFile.close();
            }
        }
    }

    /**
     * Creates an executor service for use with a TransferManager. This allows us to control the maximum number
     * of threads used because for the TransferManager default of 10 is way too many.
     *
     * @return an executor service
     */
    private ExecutorService createTransferManagerExecutorService() {
        int maxThreads = (new S3FileSystemOptions(getFileSystem().getFileSystemOptions())).getMaxUploadThreads();
        ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("s3-upload-" + getName().getBaseName() + "-" + threadCount++);
                return thread;
            }
        };
        return Executors.newFixedThreadPool(maxThreads, threadFactory);
    }

    /**
     * Uploads File to S3
     *
     * @param file the File
     * @return the eTag of the uploaded result
     * @throws IOException if the upload failed
     */
    private String upload(File file) throws IOException {
        PutObjectRequest request = new PutObjectRequest(getBucket().getName(), getS3Key(), file);

        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(file.length());
        md.setContentType(Mimetypes.getInstance().getMimetype(getName().getBaseName()));
        // set encryption if needed
        if (getServerSideEncryption()) {
            md.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
        }

        request.setMetadata(md);
        try {
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            // if length is below multi-part threshold, just use put, otherwise create and use a TransferManager
            if (md.getContentLength() < tmConfig.getMultipartUploadThreshold()) {
                return getService().putObject(request).getETag();
            } else {
                TransferManager transferManager = new TransferManager(getService(), createTransferManagerExecutorService());
                try {
                    Upload upload = transferManager.upload(request);
                    return upload.waitForUploadResult().getETag();
                } finally {
                    transferManager.shutdownNow(false);
                }
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    private boolean getServerSideEncryption() {
        return (new S3FileSystemOptions(getFileSystem().getFileSystemOptions())).getServerSideEncryption();
    }

    private static void interruptibleLock(Lock lock) throws FileSystemException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            // propagate the interrupt state since we can't propagate the exception through vfs
            Thread.currentThread().interrupt();
            throw new FileSystemException(e);
        }

    }
}
