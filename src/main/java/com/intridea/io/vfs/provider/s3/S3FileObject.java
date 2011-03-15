package com.intridea.io.vfs.provider.s3;

import static org.apache.commons.vfs.FileName.SEPARATOR;
import static org.apache.commons.vfs.FileName.SEPARATOR_CHAR;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSelector;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.FileUtil;
import org.apache.commons.vfs.NameScope;
import org.apache.commons.vfs.Selectors;
import org.apache.commons.vfs.provider.AbstractFileObject;
import org.apache.commons.vfs.provider.local.LocalFile;
import org.apache.commons.vfs.util.MonitorOutputStream;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.GrantAndPermission;
import org.jets3t.service.acl.GranteeInterface;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.model.StorageOwner;
import org.jets3t.service.utils.Mimetypes;

import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.IAclGetter;

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
    private StorageObject object;

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
    private StorageOwner fileOwner;

    /**
     * Class logger
     */
    private Log logger = LogFactory.getLog(S3FileObject.class);


    public S3FileObject(FileName fileName, S3FileSystem fileSystem,
            S3Service service, S3Bucket bucket) throws FileSystemException {

        super(fileName, fileSystem);
        this.service = service;
        this.bucket = bucket;
    }

    protected void doAttach() throws Exception {
        if (!attached) {
            try {
                // Do we have file with name?
                object = service.getObjectDetails(bucket.getName(), getS3Key());

                logger.info("Attach file to S3 Object: " + object);

                attached = true;
                return;
            } catch (ServiceException e) {
                // No, we don't
            }

            try {
                // Do we have folder with that name?
                object = service.getObjectDetails(bucket.getName(), getS3Key() + FileName.SEPARATOR);

                logger.info("Attach folder to S3 Object: " + object);

                attached = true;
                return;
            } catch (ServiceException e) {
                // No, we don't
            }

            // Create a new
            if (object == null) {
                object = new S3Object(bucket, getS3Key());
                object.setLastModifiedDate(new Date());

                logger.info(String.format("Attach file to S3 Object: %s", object));

                downloaded = true;
                attached = true;
            }
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
    	S3Object newObject = new S3Object(bucket, getS3Key(newfile.getName()));

    	service.moveObject(bucket.getName(), object.getKey(), bucket.getName(), newObject, false);
    }

    protected void doCreateFolder() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Create new folder in bucket [" +
                    ((bucket != null) ? bucket.getName() : "null") +
                    "] with key [" +
                    ((object != null) ? object.getKey() : "null") +
                    "]"
            );
        }

        if (object == null) {
            return;
        }

        service.putObject(bucket.getName(), new S3Object(object.getKey() + FileName.SEPARATOR));
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
        return new S3OutputStream(Channels.newOutputStream(getCacheFileChannel()), object);
    }

    protected FileType doGetType() throws Exception {
        if (null == object.getContentType()) {
            return FileType.IMAGINARY;
        }

        if ("".equals(object.getKey()) || object.isDirectoryPlaceholder()) {
            return FileType.FOLDER;
        }

        return FileType.FILE;
    }

    protected String[] doListChildren() throws Exception {
        String path = object.getKey();
        // make sure we add a '/' slash at the end to find children
        if ((!"".equals(path)) && (!path.endsWith(SEPARATOR))) {
            path = path + "/";
        }

        S3Object[] children = service.listObjects(bucket.getName(), path, null);
        List<String> childrenNames = new ArrayList<String>(children.length);

        for (int i = 0; i < children.length; i++) {
            if (!children[i].getKey().equals(path)) {
                // strip path from name (leave only base name)
                final String stripPath = children[i].getKey().substring(path.length());

                // Only one slash in the end OR no slash at all
                if ((stripPath.endsWith(SEPARATOR) && (stripPath.indexOf(SEPARATOR_CHAR) == stripPath.lastIndexOf(SEPARATOR_CHAR))) ||
                        (stripPath.indexOf(SEPARATOR_CHAR) == (-1))) {
                    childrenNames.add(stripPath);
                }
            }
        }

        return childrenNames.toArray(new String[childrenNames.size()]);
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
                S3Object obj = service.getObject(bucket.getName(), getS3Key());
                logger.info(String.format("Downloading S3 Object: %s", objectPath));
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
            } catch (ServiceException e) {
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

    // ACL extension methods

    /**
     * Returns S3 file owner.
     * Loads it from S3 if needed.
     */
    private StorageOwner getS3Owner() throws S3ServiceException {
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
            service.putObjectAcl(bucket.getName(), object);
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
        } catch (S3ServiceException e) {
            throw new FileSystemException(e);
        }

        // Get S3 file owner
        StorageOwner owner = s3Acl.getOwner();
        fileOwner = owner;

        // Read S3 ACL list and build VFS ACL.
        GrantAndPermission[] grants = s3Acl.getGrantAndPermissions();

        for (GrantAndPermission item : grants) {
            // Map enums to jets3t ones
            Permission perm = item.getPermission();
            Acl.Permission[] rights;
            if (perm.equals(Permission.PERMISSION_FULL_CONTROL)) {
                rights = Acl.Permission.values();
            } else if (perm.equals(Permission.PERMISSION_READ)) {
                rights = new Acl.Permission[1];
                rights[0] = Acl.Permission.READ;
            } else if (perm.equals(Permission.PERMISSION_WRITE)) {
                rights = new Acl.Permission[1];
                rights[0] = Acl.Permission.WRITE;
            } else {
                // Skip unknown permission
                logger.error(String.format("Skip unknown permission %s", perm));
                continue;
            }

            // Set permissions for groups
            if (item.getGrantee() instanceof GroupGrantee) {
                GroupGrantee grantee = (GroupGrantee)item.getGrantee();
                if (GroupGrantee.ALL_USERS.equals(grantee)) {
                    // Allow rights to GUEST
                    myAcl.allow(Acl.Group.EVERYONE, rights);
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
        StorageOwner owner;
        try {
            owner = getS3Owner();
        } catch (S3ServiceException e) {
            throw new FileSystemException(e);
        }
        s3Acl.setOwner(owner);

        // Iterate over VFS ACL rules and fill S3 ACL list
        Hashtable<Acl.Group, Acl.Permission[]> rules = acl.getRules();
        Enumeration<Acl.Group> keys = rules.keys();
        Acl.Permission[] allRights = Acl.Permission.values();
        while (keys.hasMoreElements()) {
            Acl.Group group = keys.nextElement();
            Acl.Permission[] rights = rules.get(group);

            if (rights.length == 0) {
                // Skip empty rights
                continue;
            }

            // Set permission
            Permission perm;
            if (ArrayUtils.isEquals(rights, allRights)) {
                // Use ArrayUtils instead of native equals method.
                // JRE1.6 enum[].equals behavior is very strange:
                // Two equal by elements arrays are not equal
                // Yeah, AFAIK its like that for any array.
                perm = Permission.PERMISSION_FULL_CONTROL;
            } else if (acl.isAllowed(group, Acl.Permission.READ)) {
                perm = Permission.PERMISSION_READ;
            } else if (acl.isAllowed(group, Acl.Permission.WRITE)) {
                perm = Permission.PERMISSION_WRITE;
            } else {
                logger.error(String.format("Skip unknown set of rights %s", rights.toString()));
                continue;
            }

            // Set grantee
            GranteeInterface grantee;
            if (group.equals(Acl.Group.EVERYONE)) {
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
                service.getProviderCredentials().getAccessKey(),
                service.getProviderCredentials().getSecretKey(),
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

        cal.add(Calendar.SECOND, expireInSeconds);

        try {
            return service.createSignedGetUrl(
                    bucket.getName(),
                    getS3Key(),
                    cal.getTime(),
                    false
            );
        } catch (S3ServiceException e) {
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
            StorageObject metadata = service.getObjectDetails(bucket.getName(), key);

            if (metadata != null) {
                hash = metadata.getETag();
            }
        } catch (ServiceException e) {
            throw new FileSystemException(e);
        }

        return hash;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.vfs.provider.AbstractFileObject#copyFrom(org.apache.commons.vfs.FileObject, org.apache.commons.vfs.FileSelector)
     */
    @Override
    public void copyFrom(FileObject file, FileSelector selector) throws FileSystemException {
        if (!file.exists())
        {
            throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
        }
        if (!isWriteable())
        {
            throw new FileSystemException("vfs.provider/copy-read-only.error", new Object[]{file.getType(), file.getName(), this}, null);
        }

        // Locate the files to copy across
        final List<FileObject> files = new ArrayList<FileObject>();
        file.findFiles(selector, false, files);

        // Copy everything across
        for (FileObject srcFile : files) {
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
                if (srcFile.getType().hasContent())
                {
                    doCopy(srcFile, destFile);
                }
                else if (srcFile.getType().hasChildren())
                {
                    destFile.createFolder();
                }
            }
            catch (final IOException e)
            {
                throw new FileSystemException("vfs.provider/copy-file.error", new Object[]{srcFile, destFile}, e);
            }
        }
    }

    protected void doCopy(FileObject sourceObj, FileObject targetObj) throws IOException {
        boolean doStandardCopy = true;

        if ((sourceObj instanceof LocalFile) && (targetObj instanceof S3FileObject)) {
            if (logger.isInfoEnabled()) {
                logger.info("Do fast copy from " + sourceObj + " to " + targetObj);
            }

            try {
                File file = getLocalFile(sourceObj);
                S3FileObject s3 = (S3FileObject) targetObj;

                s3.object.setContentLength(file.length());
                s3.object.setContentType(Mimetypes.getInstance().getMimetype(file));
                s3.object.setDataInputFile(file);

                s3.object = s3.service.putObject(s3.object.getBucketName(), s3.object);

                refresh();

                doStandardCopy = false;
            } catch (Exception e) {
                logger.warn("Unable to do fast copy", e);
            }
        }

        if (doStandardCopy) {
            FileUtil.copyContent(sourceObj, targetObj);
        }
    }

    private File getLocalFile(FileObject sourceObj) throws IOException {
        try {
            Method method = LocalFile.class.getDeclaredMethod("getLocalFile");

            method.setAccessible(true);

            return (File) method.invoke(sourceObj);
        } catch (SecurityException e) {
            logger.warn("Looks like API was changed and fallback to standard impl");

            throw new IOException("API changed");
        } catch (NoSuchMethodException e) {
            logger.warn("Looks like API was changed and fallback to standard impl");

            throw new IOException("API changed");
        } catch (IllegalArgumentException e) {
            logger.warn("Looks like API was changed and fallback to standard impl");

            throw new IOException("API changed");
        } catch (IllegalAccessException e) {
            logger.warn("Looks like API was changed and fallback to standard impl");

            throw new IOException("API changed");
        } catch (InvocationTargetException e) {
            logger.warn("Looks like API was changed and fallback to standard impl");

            throw new IOException("API changed");
        }
    }

    /**
     * Special JetS3FileObject output stream.
     * It saves all contents in temporary file, onClose sends contents to S3.
     *
     * @author Marat Komarov
     */
    private class S3OutputStream extends MonitorOutputStream {

        private StorageObject object;

        public S3OutputStream(OutputStream out, StorageObject object) {
            super(out);
            this.object = object;
        }

        protected void onClose() throws IOException {
            object.setDataInputStream(Channels.newInputStream(getCacheFileChannel()));
            try {
                service.putObject(object.getBucketName(), object);
            } catch (ServiceException e) {
                throw new IOException(e);
            }
        }
    }
}
