package com.intridea.io.vfs.provider.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.IAclGetter;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.intridea.io.vfs.operations.Acl.Permission.READ;
import static com.intridea.io.vfs.operations.Acl.Permission.WRITE;
import static java.nio.channels.Channels.newInputStream;
import static java.util.Calendar.SECOND;
import static org.apache.commons.vfs2.FileName.SEPARATOR;
import static org.apache.commons.vfs2.NameScope.CHILD;
import static org.apache.commons.vfs2.NameScope.FILE_SYSTEM;

/**
 * Implementation of the virtual S3 file system object using the AWS-SDK.<p/>
 * Based on Matthias Jugel code.
 * {@link http://thinkberg.com/svn/moxo/trunk/modules/vfs.s3/}
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 * @author Shon Vella
 */
public class S3FileObject extends AbstractFileObject {
    private static final Log logger = LogFactory.getLog(S3FileObject.class);

    private static final String MIMETYPE_JETS3T_DIRECTORY = "application/x-directory";

    /** Amazon S3 object */
    private ObjectMetadata objectMetadata;
    private String objectKey;

    /**
     * True when content attached to file
     */
    private boolean attached = false;

    /**
     * True when content downloaded.
     * It's an extended flag to <code>attached</code>.
     */
    private boolean downloaded = false;

    /**
     * Local cache of file content
     */
    private File cacheFile;

    /**
     * Local for output stream
     */
    private File outputFile;

    /**
     * Amazon file owner. Used in ACL
     */
    private Owner fileOwner;

    public S3FileObject(AbstractFileName fileName,
                        S3FileSystem fileSystem) throws FileSystemException {
        super(fileName, fileSystem);
    }

    @Override
    protected void doAttach() {
        if (!attached) {
            try {
                // Do we have file with name?
                String candidateKey = getS3Key();
                objectMetadata = getService().getObjectMetadata(getBucket().getName(), candidateKey);
                objectKey = candidateKey;
                logger.info("Attach file to S3 Object: " + objectKey);

                attached = true;
                return;
            } catch (AmazonServiceException e) {
                // No, we don't
            }
            catch (AmazonClientException e) {
                // We are attempting to attach to the root bucket
            }

            try {
                // Do we have folder with that name?
                String candidateKey = getS3Key() + FileName.SEPARATOR;
                objectMetadata = getService().getObjectMetadata(getBucket().getName(), candidateKey);
                objectKey = candidateKey;
                logger.info("Attach folder to S3 Object: " + objectKey);

                attached = true;
                return;
            } catch (AmazonServiceException e) {
                // No, we don't
            }

            // Create a new
            if (objectMetadata == null) {
                objectMetadata = new ObjectMetadata();
                objectKey = getS3Key();
                objectMetadata.setLastModified(new Date());

                logger.info("Attach new S3 Object: " + objectKey);

                downloaded = true;
                attached = true;
            }
        }
    }

    @Override
    protected void doDetach() throws Exception {
        if (attached) {
            logger.info("Detach from S3 Object: " + objectKey);
            objectMetadata = null;
            if (cacheFile != null) {
                if (!cacheFile.delete()) {
                    logger.error("Unable to delete temporary file: " + cacheFile.getPath());
                }
                cacheFile = null;
            }
            downloaded = false;
            attached = false;
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
        if (((S3FileSystem)getFileSystem()).getServerSideEncryption())
            metadata.setServerSideEncryption(
                ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        String dirName = objectKey.endsWith(SEPARATOR) ? objectKey : objectKey + SEPARATOR;
        getService().putObject(new PutObjectRequest(getBucket().getName(), dirName, input, metadata));
    }

    @Override
    protected long doGetLastModifiedTime() throws Exception {
        return objectMetadata.getLastModified().getTime();
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
        downloadOnce();
        return newInputStream(getCacheFileChannel());
    }

    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
        return new S3OutputStream(Channels.newOutputStream(getOutputFileChannel()));
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

    @Override
    protected String[] doListChildren() throws Exception {
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
        final List<S3ObjectSummary> summaries = new ArrayList<S3ObjectSummary>(listing.getObjectSummaries());
        final Set<String> commonPrefixes = new TreeSet<String>(listing.getCommonPrefixes());
        while (listing.isTruncated()) {
            listing = getService().listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
            commonPrefixes.addAll(listing.getCommonPrefixes());
        }

        List<String> childrenNames = new ArrayList<String>(summaries.size() + commonPrefixes.size());

        // add the prefixes (non-empty subdirs) first
        for (String commonPrefix : commonPrefixes) {
            // strip path from name (leave only base name)
            final String stripPath = commonPrefix.substring(path.length());
            childrenNames.add(stripPath);
        }

        for (S3ObjectSummary summary : summaries) {
            if (!summary.getKey().equals(path)) {
                // strip path from name (leave only base name)
                final String stripPath = summary.getKey().substring(path.length());
                childrenNames.add(stripPath);
            }
        }

        return childrenNames.toArray(new String[childrenNames.size()]);
    }

    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br>
     * Other than <code>doListChildren</code> you could return FileObject's to e.g. reinitialize the
     * type of the file.<br>
     * (Introduced for Webdav: "permission denied on resource" during getType())
     * @return The children of this FileObject.
     * @throws Exception if an error occurs.
     */
    @Override
    protected FileObject[] doListChildrenResolved() throws Exception
    {
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
        final List<S3ObjectSummary> summaries = new ArrayList<S3ObjectSummary>(listing.getObjectSummaries());
        final Set<String> commonPrefixes = new TreeSet<String>(listing.getCommonPrefixes());
        while (listing.isTruncated()) {
            listing = getService().listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
            commonPrefixes.addAll(listing.getCommonPrefixes());
        }

        List<FileObject> resolvedChildren = new ArrayList<FileObject>(summaries.size() + commonPrefixes.size());

        // add the prefixes (non-empty subdirs) first
        for (String commonPrefix : commonPrefixes) {
            // strip path from name (leave only base name)
            String stripPath = commonPrefix.substring(path.length());
            FileObject childObject = resolveFile(stripPath, (stripPath.equals("/")) ? FILE_SYSTEM : CHILD);

            if ((childObject instanceof S3FileObject) && !stripPath.equals("/")) {
                resolvedChildren.add(childObject);
            }
        }

        for (S3ObjectSummary summary : summaries) {
            if (!summary.getKey().equals(path)) {
                // strip path from name (leave only base name)
                final String stripPath = summary.getKey().substring(path.length());
                FileObject childObject = resolveFile(stripPath, CHILD);
                if (childObject instanceof S3FileObject) {
                    S3FileObject s3FileObject = (S3FileObject) childObject;
                    ObjectMetadata childMetadata = new ObjectMetadata();
                    childMetadata.setContentLength(summary.getSize());
                    childMetadata.setContentType(
                        Mimetypes.getInstance().getMimetype(s3FileObject.getName().getBaseName()));
                    childMetadata.setLastModified(summary.getLastModified());
                    childMetadata.setHeader(Headers.ETAG, summary.getETag());
                    s3FileObject.objectMetadata = childMetadata;
                    s3FileObject.objectKey = summary.getKey();
                    s3FileObject.attached = true;
                    resolvedChildren.add(s3FileObject);
                }
            }
        }

        return resolvedChildren.toArray(new FileObject[resolvedChildren.size()]);
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
    private void downloadOnce () throws FileSystemException {
        if (!downloaded) {
            final String failedMessage = "Failed to download S3 Object %s. %s";
            final String objectPath = getName().getPath();
            try {
                S3Object obj = getService().getObject(getBucket().getName(), objectKey);
                logger.info(String.format("Downloading S3 Object: %s", objectPath));
                InputStream is = obj.getObjectContent();
                if (obj.getObjectMetadata().getContentLength() > 0) {
                    ReadableByteChannel rbc = Channels.newChannel(is);
                    FileChannel cacheFc = getCacheFileChannel();
                    cacheFc.transferFrom(rbc, 0, obj.getObjectMetadata().getContentLength());
                    cacheFc.close();
                    rbc.close();
                } else {
                    is.close();
                }
            } catch (AmazonServiceException e) {
                throw new FileSystemException(String.format(failedMessage, objectPath, e.getMessage()), e);
            } catch (IOException e) {
                throw new FileSystemException(String.format(failedMessage, objectPath, e.getMessage()), e);
            }

            downloaded = true;
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

    /**
     * Get or create temporary file channel for file cache
     * @return temporary file channel for file cache
     * @throws IOException
     */
    private FileChannel getCacheFileChannel() throws IOException {
        if (cacheFile == null) {
            cacheFile = File.createTempFile("scalr.", ".s3");
        }
        return new RandomAccessFile(cacheFile, "rw").getChannel();
    }

    /**
     * Get or create temporary file channel for file output cache
     * @return temporary file channel for file output cache
     * @throws IOException
     */
    private FileChannel getOutputFileChannel() throws IOException {
        if (outputFile == null) {
            outputFile = File.createTempFile("scalr.", ".s3");
        }
        return new RandomAccessFile(outputFile, "rw").getChannel();
    }

    // ACL extension methods

    /**
     * Returns S3 file owner.
     * Loads it from S3 if needed.
     */
    private Owner getS3Owner() {
        if (fileOwner == null) {
            AccessControlList s3Acl = getS3Acl();
            fileOwner = s3Acl.getOwner();
        }
        return fileOwner;
    }

    /**
     * Get S3 ACL list
     * @return acl list
     */
    private AccessControlList getS3Acl() {
        String key = getS3Key();
        return "".equals(key) ? getService().getBucketAcl(getBucket().getName()) : getService().getObjectAcl(getBucket().getName(), key);
    }

    /**
     * Put S3 ACL list
     * @param s3Acl acl list
     */
    private void putS3Acl (AccessControlList s3Acl) {
        String key = getS3Key();
        // Determine context. Object or Bucket
        if ("".equals(key)) {
            getService().setBucketAcl(getBucket().getName(), s3Acl);
        } else {
            // Before any operations with object it must be attached
            doAttach();
            // Put ACL to S3
            getService().setObjectAcl(getBucket().getName(), objectKey, s3Acl);
        }
    }

    /**
     * Returns access control list for this file.
     *
     * VFS interfaces doesn't provide interface to manage permissions. ACL can be accessed through {@link FileObject#getFileOperations()}
     * Sample: <code>file.getFileOperations().getOperation(IAclGetter.class)</code>
     * @see {@link FileObject#getFileOperations()}
     * @see {@link IAclGetter}
     *
     * @return Current Access control list for a file
     * @throws FileSystemException
     */
    public Acl getAcl () throws FileSystemException {
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
        Set<Grant> grants = s3Acl.getGrants();

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
                GroupGrantee grantee = (GroupGrantee)item.getGrantee();
                if (GroupGrantee.AllUsers.equals(grantee)) {
                    // Allow rights to GUEST
                    myAcl.allow(Acl.Group.EVERYONE, rights);
                } else if (GroupGrantee.AuthenticatedUsers.equals(grantee)) {
                    // Allow rights to AUTHORIZED
                    myAcl.allow(Acl.Group.AUTHORIZED, rights);
                }
            } else if (item.getGrantee() instanceof CanonicalGrantee) {
                CanonicalGrantee grantee = (CanonicalGrantee)item.getGrantee();
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
     *
     * VFS interfaces doesn't provide interface to manage permissions. ACL can be accessed through {@link FileObject#getFileOperations()}
     * Sample: <code>file.getFileOperations().getOperation(IAclGetter.class)</code>
     * @see {@link FileObject#getFileOperations()}
     * @see {@link IAclGetter}
     *
     * @param acl the access control list
     * @throws FileSystemException
     */
    public void setAcl (Acl acl) throws FileSystemException {

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
    public String getPrivateUrl() {
        return String.format(
                "s3://%s:%s@%s/%s",
                getAwsCredentials().getAWSAccessKeyId(),
                getAwsCredentials().getAWSSecretKey(),
                getBucket().getName(),
                getS3Key()
        );
    }

    /**
     * Temporary accessible url for object.
     * @param expireInSeconds seconds until expiration
     * @return temporary accessible url for object
     * @throws FileSystemException
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
     * @throws FileSystemException
     */
    public String getMD5Hash() throws FileSystemException {
        String hash = null;

        ObjectMetadata metadata = getObjectMetadata();
        if (metadata != null) {
            hash = metadata.getETag(); // TODO this is something different than mentioned in methodname / javadoc
        }

        return hash;
    }

    public ObjectMetadata getObjectMetadata() throws FileSystemException {
        try {
            return getService().getObjectMetadata(getBucket().getName(), getS3Key());
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }
    }

    /** FileSystem object containing configuration */
    protected AWSCredentials getAwsCredentials() {
        return ((S3FileSystem)getFileSystem()).getAwsCredentials();
    }

    protected AmazonS3Client getService() {
        return ((S3FileSystem)getFileSystem()).getService();
    }

    /** Amazon S3 bucket */
    protected Bucket getBucket() {
        return ((S3FileSystem)getFileSystem()).getBucket();
    }

    /**
     * Special JetS3FileObject output stream.
     * It saves all contents in temporary file, onClose sends contents to S3.
     *
     * @author Marat Komarov
     */
    private class S3OutputStream extends MonitorOutputStream {
        public S3OutputStream(OutputStream out) {
            super(out);
        }

        @Override
        protected void onClose() throws IOException {
 			doAttach();
            try {
                upload(outputFile);
            } catch (AmazonServiceException e) {
                throw new IOException(e);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                if (!attached) {
                    if (!outputFile.delete()) {
                        logger.error("Unable to delete temporary file: " + outputFile.getName());
                    }
                } else {
                    cacheFile = outputFile;
                    downloaded = true;
                }
                outputFile = null;
            }
        }
    }

    /**
     * Queries the object if a simple rename to the filename of <code>newfile</code> is possible.
     *
     * @param newfile
     *  the new filename
     * @return true if rename is possible
     */
    @Override
    public boolean canRenameTo(FileObject newfile) {
        return false;
    }

    @Override
    /**
     * Copies another file to this file.
     * @param file The FileObject to copy.
     * @param selector The FileSelector.
     * @throws FileSystemException if an error occurs.
     */
    public void copyFrom(final FileObject file, final FileSelector selector)
        throws FileSystemException
    {
		if (!file.exists()) {
			throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
		}
		// Locate the files to copy across
		final ArrayList<FileObject> files = new ArrayList<FileObject>();
		file.findFiles(selector, false, files);

		// Copy everything across
		for (final FileObject srcFile : files) {
			// Determine the destination file
			final String relPath = file.getName().getRelativeName(srcFile.getName());
			final S3FileObject destFile = (S3FileObject) resolveFile(relPath, NameScope.DESCENDENT_OR_SELF);

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
					// do server side copy if both source and dest are in S3 and using same credentials
				} else if (srcFile instanceof S3FileObject &&
				    ((S3FileObject)srcFile).getAwsCredentials().getAWSAccessKeyId().equals(getAwsCredentials().getAWSAccessKeyId()) &&
				    ((S3FileObject)srcFile).getAwsCredentials().getAWSSecretKey().equals(getAwsCredentials().getAWSSecretKey())) {
				     S3FileObject s3SrcFile = (S3FileObject)srcFile;
                    String srcBucketName = s3SrcFile.getBucket().getName();
                    String srcFileName = s3SrcFile.getS3Key();
                    String destBucketName = destFile.getBucket().getName();
                    String destFileName = destFile.getS3Key();
                    CopyObjectRequest copy = new CopyObjectRequest(
                        srcBucketName, srcFileName, destBucketName, destFileName);
                    if (srcFile.getType() == FileType.FILE
                        && ((S3FileSystem)destFile.getFileSystem()).getServerSideEncryption()) {
                        ObjectMetadata meta = s3SrcFile.getObjectMetadata();
                        meta.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                        copy.setNewObjectMetadata(meta);
                    }
                    getService().copyObject(copy);
                } else if (srcFile.getType().hasContent() && srcFile.getURL().getProtocol().equals("file")) {
                    // do direct upload from file to avoid overhead of making a copy of the file
                    try {
                        File localFile = new File(srcFile.getURL().toURI());
                        destFile.upload(localFile);
                    } catch (URISyntaxException e) {
                        // couldn't convert URL to URI, but should still be able to do the slower way
                        super.copyFrom(file, selector);
                    }
                } else {
                    super.copyFrom(file, selector);
                }
			} catch (IOException e) {
				throw new FileSystemException("vfs.provider/copy-file.error", new Object[]{srcFile, destFile}, e);
			} catch (AmazonClientException e) {
				throw new FileSystemException("vfs.provider/copy-file.error", new Object[]{srcFile, destFile}, e);
			} finally {
				destFile.close();
			}
		}
    }

    /**
     * Uploads File to S3
     *
     * @param file the File
     * @param size        the size of the stream
     */
    private void upload(File file)
            throws IOException {
        PutObjectRequest request = new PutObjectRequest(getBucket().getName(), getS3Key(), file);

        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(file.length());
        md.setContentType(Mimetypes.getInstance().getMimetype(getName().getBaseName()));
        // set encryption if needed
        if (((S3FileSystem) getFileSystem()).getServerSideEncryption()) {
            md.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
        request.setMetadata(md);
        try {
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            // if length is below multi-part threshold, just use put, otherwise create and use a TransferManager
            if (md.getContentLength() < tmConfig.getMultipartUploadThreshold()) {
                getService().putObject(request);
            } else {
                TransferManager transferManager = new TransferManager(getService(), createTransferManagerExecutorService());
                try {
                    Upload upload = transferManager.upload(request);
                    upload.waitForCompletion();
                } finally {
                    transferManager.shutdownNow(false);
                }
            }
            doDetach();
            doAttach();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        } catch (Exception e) {
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
    }

    /**
     * Creates an executor service for use with a TransferManager. This allows us to control the maximum number
     * of threads used because for the TransferManager default of 10 is way too many.
     *
     * @return an executor service
     */
    private ExecutorService createTransferManagerExecutorService() {
        int maxThreads = S3FileSystemConfigBuilder.getInstance().getMaxUploadThreads(getFileSystem().getFileSystemOptions());
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

}
