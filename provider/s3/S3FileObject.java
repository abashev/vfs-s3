package com.intridea.io.vfs.provider.s3;


import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.AbstractFileObject;
import org.apache.commons.vfs.util.MonitorOutputStream;
import org.apache.log4j.Logger;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.GrantAndPermission;
import org.jets3t.service.acl.GranteeInterface;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.S3Owner;
import org.jets3t.service.utils.Mimetypes;

import com.intridea.io.vfs.operations.acl.Acl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

/**
 * Implementation of the virtual S3 file system object using the Jets3t library.
 * Based on Matthias Jugel code 
 * http://thinkberg.com/svn/moxo/trunk/src/main/java/com/thinkberg/moxo/
 *
 * @author Marat Komarov 
 * @author Matthias L. Jugel
 */
public class S3FileObject extends AbstractFileObject {
	/**
	 * Amazon S3 service
	 */
	private final S3Service service;
	
	/**
	 * Amazon S3 bucket
	 */
	private final S3Bucket bucket;

	/**
	 * Amazon S3 object
	 */
	private S3Object object;	
	
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
	 * Amazon file owner. Used in ACL
	 */
	private S3Owner fileOwner;
	
	/**
	 * Class logger
	 */
	private Logger logger = Logger.getLogger(S3FileObject.class);

	
	public S3FileObject(FileName fileName, S3FileSystem fileSystem,
			S3Service service, S3Bucket bucket) throws FileSystemException {

		super(fileName, fileSystem);
		this.service = service;
		this.bucket = bucket;
	}

	protected void doAttach() throws Exception {
		if (!attached) {
			try {
				// Get an object representing the details of an item WITHOUT content
				object = service.getObjectDetails(bucket, getS3Key());
				logger.info(String.format("Attach file to S3 Object: %s", object));
			} catch (S3ServiceException e) {
				object = new S3Object(bucket, getS3Key());
				object.setLastModifiedDate(new Date());
				logger.info(String.format("Attach file to S3 Object: %s", object));
				downloaded = true;
			}
			attached = true;
		}
	}

	protected void doDetach() throws Exception {
		if (attached) {
			object = null;
			if (cacheFile != null) {
				cacheFile.delete();
				cacheFile = null;
			}
			downloaded = false;
			attached = false;
		}
	}

	protected void doDelete() throws Exception {
		service.deleteObject(bucket, object.getKey());
	}

	protected void doRename(FileObject newfile) throws Exception {
		// TODO: implement
		// jets3t doesn't support AWS copyObject command that is responsible for copying and renaming objects
		super.doRename(newfile);
	}

	protected void doCreateFolder() throws Exception {
		if (!Mimetypes.MIMETYPE_JETS3T_DIRECTORY
				.equals(object.getContentType())) {
			object.setContentType(Mimetypes.MIMETYPE_JETS3T_DIRECTORY);
			service.putObject(bucket, object);
		}
	}

	protected long doGetLastModifiedTime() throws Exception {
		return object.getLastModifiedDate().getTime();
	}

	protected void doSetLastModifiedTime(final long modtime) throws Exception {
		// TODO: last modified date will be changed only when content changed
		object.setLastModifiedDate(new Date(modtime));
	}

	protected InputStream doGetInputStream() throws Exception {
		downloadOnce();
		return Channels.newInputStream(getCacheFileChannel());
	}

	protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
		return new S3OutputStream(Channels.newOutputStream(getCacheFileChannel()), service, object);
	}

	protected FileType doGetType() throws Exception {
		if (null == object.getContentType()) {
			return FileType.IMAGINARY;
		}

		String contentType = object.getContentType();
		if ("".equals(object.getKey())
				|| Mimetypes.MIMETYPE_JETS3T_DIRECTORY.equals(contentType)) {
			return FileType.FOLDER;
		}

		return FileType.FILE;
	}

	protected String[] doListChildren() throws Exception {
		String path = object.getKey();
		// make sure we add a '/' slash at the end to find children
		if (!"".equals(path)) {
			path = path + "/";
		}

		S3Object[] children = service.listObjects(bucket, path, "/");
		String[] childrenNames = new String[children.length];
		for (int i = 0; i < children.length; i++) {
			if (!children[i].getKey().equals(path)) {
				// strip path from name (leave only base name)
				childrenNames[i] = children[i].getKey().replaceAll("[^/]*//*",
						"");
			}
		}
		return childrenNames;
	}

	protected long doGetContentSize() throws Exception {
		return object.getContentLength();
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
				S3Object obj = service.getObject(bucket, getS3Key());
				logger.info(String.format("Download S3 Object: %s", objectPath));
				InputStream is = obj.getDataInputStream();
				if (obj.getContentLength() > 0) {
					ReadableByteChannel rbc = Channels.newChannel(is);
					FileChannel cacheFc = getCacheFileChannel();
					cacheFc.transferFrom(rbc, 0, obj.getContentLength());
					cacheFc.close();
					rbc.close();
				} else {
					is.close();
				}
			} catch (S3ServiceException e) {
				throw new FileSystemException(String.format(failedMessage, objectPath, e.getMessage()), e);
			} catch (IOException e) {
				throw new FileSystemException(String.format(failedMessage, objectPath, e.getMessage()), e);
			}
			
			downloaded = true;
		}
		
	}
	
	/**
	 * Create an S3 key from a commons-vfs path. This simply strips the slash
	 * from the beginning if it exists.
	 * 
	 * @return the S3 object key
	 */
	private String getS3Key() {
		String path = getName().getPath();
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
	
	// ACL extension methods
	
	/**
	 * Returns S3 file owner.
	 * Loads it from S3 if needed.
	 */
	private S3Owner getS3Owner () throws S3ServiceException {
		if (fileOwner == null) {
			AccessControlList s3Acl = getS3Acl();
			fileOwner = s3Acl.getOwner();
		}
		return fileOwner;
	}
	
	/**
	 * Get S3 ACL list 
	 * @return
	 * @throws S3ServiceException
	 */
	private AccessControlList getS3Acl () throws S3ServiceException {
		String key = getS3Key();
		return "".equals(key) ? service.getBucketAcl(bucket) :	service.getObjectAcl(bucket, key);
	}
	
	/**
	 * Put S3 ACL list
	 * @param s3Acl
	 * @throws Exception 
	 */
	private void putS3Acl (AccessControlList s3Acl) throws Exception {
		String key = getS3Key();
		// Determine context. Object or Bucket
		if ("".equals(key)) {
			bucket.setAcl(s3Acl);
			service.putBucketAcl(bucket);
		} else {
			// Before any operations with object it must be attached
			doAttach();
			// Put ACL to S3
			object.setAcl(s3Acl);
			service.putObjectAcl(bucket, object);
		}
	}
	
	/**
	 * Returns access control list for this file. 
	 * 
	 * VFS interfaces not provides ACL operations. ACL can be accessed throught FileOperations 
	 * <code>file.getFileOperations().getOperation(IAclGetter.class)</code>
	 * @see com.intridea.io.vfs.operations.acl.IAclGetter
	 * 
	 * @return
	 * @throws FileSystemException
	 */
	public Acl getAcl () throws FileSystemException {
		Acl myAcl = new Acl();
		AccessControlList s3Acl;
		try {	
			s3Acl = getS3Acl();
		} catch (S3ServiceException e) {
			throw new FileSystemException(e);
		}

		// Get S3 file owner
		S3Owner owner = s3Acl.getOwner();
		fileOwner = owner;

		// Read S3 ACL list and build VFS ACL.
		@SuppressWarnings("unchecked")		
		Set<GrantAndPermission> grants = s3Acl.getGrants();
		Iterator<GrantAndPermission> it = grants.iterator();
		while (it.hasNext()) {
			GrantAndPermission item = it.next();
			
			// Get rights
			Permission perm = item.getPermission();			
			Acl.Right[] rights;
			if (perm.equals(Permission.PERMISSION_FULL_CONTROL)) {
				rights = Acl.Right.values(); 
			} else if (perm.equals(Permission.PERMISSION_READ)) {
				rights = new Acl.Right[1];
				rights[0] = Acl.Right.READ;
			} else if (perm.equals(Permission.PERMISSION_WRITE)) {
				rights = new Acl.Right[1];
				rights[0] = Acl.Right.WRITE;
			} else {
				// Skip unknown permission
				logger.error(String.format("Skip unknown permission %s", perm));
				continue;
			}

			// Set rights for groups			
			if (item.getGrantee() instanceof GroupGrantee) {
				GroupGrantee grantee = (GroupGrantee)item.getGrantee();
				if (GroupGrantee.ALL_USERS.equals(grantee)) {
					// Allow rights to GUEST
					myAcl.allow(Acl.Group.GUEST, rights);
				} else if (GroupGrantee.AUTHENTICATED_USERS.equals(grantee)) {
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
	 * Sets access controll list for this file.
	 * 
	 * VFS interfaces not provides ACL operations. ACL can be addesed throught FileOperations 
	 * <code>file.getFileOperations().getOperation(IAclSetter.class)</code>
	 * @see com.intridea.io.vfs.operations.acl.IAclSetter
	 *  
	 * @param acl
	 * @throws FileSystemException
	 */
	public void setAcl (Acl acl) throws FileSystemException {
		
		// Create empty S3 ACL list
		AccessControlList s3Acl = new AccessControlList();
		
		// Get file owner
		S3Owner owner;
		try {
			owner = getS3Owner();
		} catch (S3ServiceException e) {
			throw new FileSystemException(e);
		}
		s3Acl.setOwner(owner);
		
		// Iterate over VFS ACL rules and fill S3 ACL list
		Hashtable<Acl.Group, Acl.Right[]> rules = acl.getRules();
		Enumeration<Acl.Group> keys = rules.keys();
		Acl.Right[] allRights = Acl.Right.values();
		while (keys.hasMoreElements()) {
			Acl.Group group = keys.nextElement();
			Acl.Right[] rights = rules.get(group);
			
			if (rights.length == 0) {
				// Skip empty rights
				continue;
			}
			
			// Set permission
			Permission perm;
			if (ArrayUtils.isEquals(rights, allRights)) {
				// Use ArrayUtils istead of native equals method.
				// JRE1.6 enum[].equals behavoiur is very strange:
				// Two equal by elements arrays are not equal
				perm = Permission.PERMISSION_FULL_CONTROL; 
			} else if (acl.isAllowed(group, Acl.Right.READ)) {
				perm = Permission.PERMISSION_READ;
			} else if (acl.isAllowed(group, Acl.Right.WRITE)) {
				perm = Permission.PERMISSION_WRITE;
			} else {
				logger.error(String.format("Skip unknown set of rights %s", rights.toString()));
				continue;
			}
			
			// Set grantee
			GranteeInterface grantee;
			if (group.equals(Acl.Group.GUEST)) {
				grantee = GroupGrantee.ALL_USERS;
			} else if (group.equals(Acl.Group.AUTHORIZED)) {
				grantee = GroupGrantee.AUTHENTICATED_USERS;
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
	 * Special JetS3FileObject output stream. 
	 * It saves all contents in temporary file, onClose sends contents to S3. 
	 * 
	 * @author Marat Komarov
	 */
	private class S3OutputStream extends MonitorOutputStream {

		private S3Service service;
		
		private S3Object object;
		
		public S3OutputStream(OutputStream out, S3Service service, S3Object object) {
			super(out);
			this.service = service;
			this.object = object;
		}
		
	    protected void onClose() throws IOException {
	    	object.setDataInputStream(Channels.newInputStream(getCacheFileChannel()));
	    	try {
				service.putObject(object.getBucketName(), object);
			} catch (S3ServiceException e) {
				throw new IOException(e);
			}
	    }
	}
}
