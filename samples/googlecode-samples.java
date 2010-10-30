/**
* A sample code for http://code.google.com/p/vfs-s3
*/

/**
* Basic ops
*/

// Create bucket
FileSystemManager fsManager = VFS.getManager();
FileObject dir = fsManager.resolveFile("s3://vfs-test-bucket");
dir.createFolder();

// Upload file to S3
FileObject src = fsManager.resolveFile(new File("/etc/shells").getAbsolutePath());
FileObject dest = fsManager.resolveFile("s3://vfs-test-bucket/shells");
dest.copyFrom(src, Selectors.SELECT_SELF);

// Download from S3
FileObject remote_file = fsManager.resolveFile("s3://vfs-bucket/backup.zip");
File local_file = File.createTempFile("vfs.", ".s3");
FileOutputStream out = new FileOutputStream(local_file);
InputStream in = remote_file.getContent().getInputStream();
IOUtils.copy(in, out);

// Delete bucket
FileObject dir = fsManager.resolveFile("s3://vfs-bucket");
dir.delete();

/*
* ACL (Access Control List) extension
*/
FileOperationProvider operationProvider = (FileOperationProvider) new AclOperationsProvider();
String[] schemes = {"s3"};
fsManager.addOperationProvider(schemes, operationProvider);


// Get ACL
IAclGetter aclGetter = (IAclGetter)remote_file.getFileOperations().getOperation(IAclGetter.class);
aclGetter.process();
fileAcl = aclGetter.getAcl();
if (aclGetter.canRead(Acl.Group.GUEST)) {
    System.out.println("Anyone can read s3://vfs-bucket/backup.zip");
}

// Permit all authorized users to read remote_file
fileAcl.allow(Acl.Group.AUTHORIZED, Acl.Right.READ);
IAclSetter aclSetter = (IAclSetter)remote_file.getFileOperations().getOperation(IAclSetter.class);
aclSetter.setAcl(fileAcl);
aclSetter.process();
