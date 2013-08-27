package com.intridea.io.vfs.provider.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
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
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.*;

import static com.amazonaws.services.s3.model.ProgressEvent.COMPLETED_EVENT_CODE;
import static com.intridea.io.vfs.operations.Acl.Permission.READ;
import static com.intridea.io.vfs.operations.Acl.Permission.WRITE;
import static java.nio.channels.Channels.newInputStream;
import static java.util.Calendar.SECOND;
import static org.apache.commons.vfs2.FileName.SEPARATOR;
import static org.apache.commons.vfs2.FileName.SEPARATOR_CHAR;

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

    /** Amazon S3 service */
    private final AWSCredentials awsCredentials;
    private final AmazonS3 service;

    private final TransferManager transferManager;

    /** Amazon S3 bucket */
     private final Bucket bucket;

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
     * Local cache of file content
    */
    private File outputFile;

    /**
     * Amazon file owner. Used in ACL
     */
    private Owner fileOwner;

    public S3FileObject(
            AbstractFileName fileName, S3FileSystem fileSystem, AWSCredentials awsCredentials, AmazonS3 service,
            TransferManager transferManager, Bucket bucket
    ) throws FileSystemException {
        super(fileName, fileSystem);

        this.awsCredentials = awsCredentials;
        this.service = service;
        this.bucket = bucket;
        this.transferManager = transferManager;
    }

    @Override
    protected void doAttach() {
        if (!attached) {
            try {
                // Do we have file with name?
                String candidateKey = getS3Key();
                objectMetadata = service.getObjectMetadata(bucket.getName(), candidateKey);
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
                objectMetadata = service.getObjectMetadata(bucket.getName(), candidateKey);
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
                cacheFile.delete();
                cacheFile = null;
            }
            downloaded = false;
            attached = false;
        }
    }

    @Override
    protected void doDelete() throws Exception {
        service.deleteObject(bucket.getName(), objectKey);
    }

    @Override
    protected void doCreateFolder() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Create new folder in bucket [" +
                    ((bucket != null) ? bucket.getName() : "null") +
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
        service.putObject(new PutObjectRequest(bucket.getName(), objectKey + FileName.SEPARATOR, input, metadata));
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
		loReq.setBucketName(bucket.getName());
		loReq.setDelimiter("/");
		loReq.setPrefix(path);

		ObjectListing listing = service.listObjects(loReq);
		final List<S3ObjectSummary> summaries = new ArrayList<S3ObjectSummary>(listing.getObjectSummaries());
		final Set<String> commonPrefixes = new TreeSet<>(listing.getCommonPrefixes());
		while (listing.isTruncated()) {
			listing = service.listNextBatchOfObjects(listing);
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
		loReq.setBucketName(bucket.getName());
		loReq.setDelimiter("/");
		loReq.setPrefix(path);

		ObjectListing listing = service.listObjects(loReq);
		final List<S3ObjectSummary> summaries = new ArrayList<S3ObjectSummary>(listing.getObjectSummaries());
		final Set<String> commonPrefixes = new TreeSet<>(listing.getCommonPrefixes());
		while (listing.isTruncated()) {
			listing = service.listNextBatchOfObjects(listing);
			summaries.addAll(listing.getObjectSummaries());
			commonPrefixes.addAll(listing.getCommonPrefixes());
		}

		List<FileObject> resolvedChildren = new ArrayList<FileObject>(summaries.size() + commonPrefixes.size());

		// add the prefixes (non-empty subdirs) first
		for (String commonPrefix : commonPrefixes) {
			// strip path from name (leave only base name)
			final String stripPath = commonPrefix.substring(path.length());
			FileObject childObject = resolveFile(stripPath, NameScope.CHILD);
			if (childObject instanceof S3FileObject) {
				S3FileObject s3FileObject = (S3FileObject)childObject;
				resolvedChildren.add(s3FileObject);
			}
		}

		for (S3ObjectSummary summary : summaries) {
			if (!summary.getKey().equals(path)) {
				// strip path from name (leave only base name)
				final String stripPath = summary.getKey().substring(path.length());
				FileObject childObject = resolveFile(stripPath, NameScope.CHILD);
				if (childObject instanceof S3FileObject) {
					S3FileObject s3FileObject = (S3FileObject)childObject;
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
                S3Object obj = service.getObject(bucket.getName(), objectKey);
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
     * @return
     * @throws IOException
     */
    private FileChannel getCacheFileChannel() throws IOException {
        if (cacheFile == null) {
            cacheFile = File.createTempFile("scalr.", ".s3");
        }
        return new RandomAccessFile(cacheFile, "rw").getChannel();
    }

    /**
     * Get or create temporary file channel for file cache
     * @return
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
     * @return
     */
    private AccessControlList getS3Acl() {
        String key = getS3Key();
        return "".equals(key) ? service.getBucketAcl(bucket.getName()) : service.getObjectAcl(bucket.getName(), key);
    }

    /**
     * Put S3 ACL list
     * @param s3Acl
     * @throws Exception
     */
    private void putS3Acl (AccessControlList s3Acl) {
        String key = getS3Key();
        // Determine context. Object or Bucket
        if ("".equals(key)) {
            service.setBucketAcl(bucket.getName(), s3Acl);
        } else {
            // Before any operations with object it must be attached
            doAttach();
            // Put ACL to S3
            service.setObjectAcl(bucket.getName(), objectKey, s3Acl);
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
     * @param acl
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
                logger.error(String.format("Skip unknown set of rights %s", rights.toString()));
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
     * @return
     */
    public String getHttpUrl() {
        StringBuilder sb = new StringBuilder("http://" + bucket.getName() + ".s3.amazonaws.com/");
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
     * @return
     */
    public String getPrivateUrl() {
        return String.format(
                "s3://%s:%s@%s/%s",
                awsCredentials.getAWSAccessKeyId(),
                awsCredentials.getAWSSecretKey(),
                bucket.getName(),
                getS3Key()
        );
    }

    /**
     * Tempary accessable url for object.
     * @param expireInSeconds
     * @return
     * @throws FileSystemException
     */
    public String getSignedUrl(int expireInSeconds) throws FileSystemException {
        final Calendar cal = Calendar.getInstance();

        cal.add(SECOND, expireInSeconds);

        try {
            return service.generatePresignedUrl(bucket.getName(), getS3Key(), cal.getTime()).toString();
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }
    }

    /**
     * Get MD5 hash for the file
     * @return
     * @throws FileSystemException
     */
    public String getMD5Hash() throws FileSystemException {
        final String key = getS3Key();
        String hash = null;

        try {
            ObjectMetadata metadata = service.getObjectMetadata(bucket.getName(), key);
            if (metadata != null) {
                hash = metadata.getETag(); // TODO this is something different than mentioned in methodname / javadoc
            }
        } catch (AmazonServiceException e) {
            throw new FileSystemException(e);
        }

        return hash;
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
        			doAttach();
            FileChannel cacheFileChannel = getOutputFileChannel();
           objectMetadata.setContentLength(cacheFileChannel.size());
            objectMetadata.setContentType(Mimetypes.getInstance().getMimetype(getName().getBaseName()));

            try {
                final Upload upload = transferManager.upload(
                        bucket.getName(), objectKey, newInputStream(cacheFileChannel), objectMetadata
                );

                upload.addProgressListener(new ProgressListener() {
                    private final int REPORT_THRESHOLD = 25; // Report every 25 percents

                    private double lastValue = 0;

                    // This method is called periodically as your transfer progresses
                    public void progressChanged(ProgressEvent progressEvent) {
                        double progress = upload.getProgress().getPercentTransfered();

                        if ((progress - lastValue) > REPORT_THRESHOLD) {
                            logger.info(
                                    "File " + objectKey +
                                    " was uploaded to " + bucket.getName() +
                                    " for " + (int) progress + "%"
                            );

                            lastValue = progress;
                        }

                        if (progressEvent.getEventCode() == COMPLETED_EVENT_CODE) {
                            logger.info("File " + objectKey + " was successfully uploaded to " + bucket.getName());
                        }
                    }
                });

                upload.waitForCompletion();
				doDetach();
				doAttach();
            } catch (AmazonServiceException e) {
                throw new IOException(e);
            } catch (InterruptedException e) {
                throw new IOException(e);
            } catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					cacheFileChannel.close();
				} catch (IOException e) {
					logger.error("Unable to delete temporary file: " + outputFile.getName(), e);
					try {
						doDetach();
					} catch (Exception e1) {
						logger.error("Couldn't detach from S3 Object: " + objectKey, e);
					}
				} finally {
					if (!attached) {
						outputFile.delete();
					} else {
						cacheFile = outputFile;
						downloaded = true;
					}
					outputFile = null;
				}
            }
        }
    }

	/**
	 * Queries the object if a simple rename to the filename of <code>newfile</code> is possible.
	 *
	 * @param newfile
	 * 	the new filename
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
		if (!(file instanceof S3FileObject)) {
			super.copyFrom(file, selector);
		} else {
			if (!file.exists())
			{
				throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
			}

			// Locate the files to copy across
			final ArrayList<FileObject> files = new ArrayList<FileObject>();
			file.findFiles(selector, false, files);

			// Copy everything across
			final int count = files.size();
			for (int i = 0; i < count; i++)
			{
				final FileObject srcFile = files.get(i);

				// Determine the destination file
				final String relPath = file.getName().getRelativeName(srcFile.getName());
				final FileObject destFile = resolveFile(relPath, NameScope.DESCENDENT_OR_SELF);

				// Clean up the destination file, if necessary
				if (destFile.exists() && destFile.getType() != srcFile.getType())
				{
					// The destination file exists, and is not of the same type,
					// so delete it
					// TODO - add a pluggable policy for deleting and overwriting existing files
					destFile.delete(Selectors.SELECT_ALL);
				}

				// Copy across
				try
				{
					if (srcFile.getType() == FileType.FOLDER) {
						service.copyObject(
							((S3FileObject)srcFile).bucket.getName(),
							((S3FileObject)srcFile).getS3Key() + FileName.SEPARATOR,
							((S3FileObject)destFile).bucket.getName(),
							((S3FileObject)destFile).getS3Key() + FileName.SEPARATOR
						);
					} else {
						service.copyObject(
							((S3FileObject)srcFile).bucket.getName(),
							((S3FileObject)srcFile).getS3Key(),
							((S3FileObject)destFile).bucket.getName(),
							((S3FileObject)destFile).getS3Key()
						);
					}
				} catch (AmazonServiceException e) {
					throw new FileSystemException("vfs.provider/copy-file.error", new Object[]{srcFile, destFile}, e);
				}
				catch (AmazonClientException e) {
					throw new FileSystemException("vfs.provider/copy-file.error", new Object[]{srcFile, destFile}, e);
				}
			}
		}
	}

}
