Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================


Branch | Description | Build Status
------------ | ------------- | ------------
branch-2.4.x | **Current** See below | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.4.x)](http://travis-ci.org/abashev/vfs-s3)
branch-2.3.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.3.x)](http://travis-ci.org/abashev/vfs-s3)
branch-2.2.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.2.x)](http://travis-ci.org/abashev/vfs-s3)

Using with Maven
----------------

Add this section to your repository configuration

    <repositories>
        <repository>
            <id>vfs-s3.repository</id>
            <name>vfs-s3 project repository</name>
            <url>http://dl.bintray.com/content/abashev/vfs-s3</url>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
        </repository>
    </repositories>

And use it as dependency

    <dependency>
        <groupId>com.github</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>2.4.2</version>
    </dependency>


Direct downloads from Bintray
----------------

We need this patched version of _commons-vfs_ because some concurrency issues could be solved only internally

Branch | Build Status 
------------ |  ------------ 
commons-vfs2 | [![Download](https://api.bintray.com/packages/abashev/vfs-s3/commons-vfs2/images/download.svg) ](https://bintray.com/abashev/vfs-s3/commons-vfs2/_latestVersion) 
vfs-s3 | [![Download](https://api.bintray.com/packages/abashev/vfs-s3/vfs-s3/images/download.svg) ](https://bintray.com/abashev/vfs-s3/vfs-s3/_latestVersion)


TODO for branch-2.4.x development
---
1. Merge changes back to `commons-vfs` project



Sample Java Code
----------------

	// Create bucket
	FileSystemManager fsManager = VFS.getManager();
	FileObject dir = fsManager.resolveFile("s3://simple-bucket/test-folder/");
	dir.createFolder();

	// Upload file to S3
	FileObject dest = fsManager.resolveFile("s3://test-bucket/backup.zip");
	FileObject src = fsManager.resolveFile(new File("/path/to/local/file.zip").getAbsolutePath());
	dest.copyFrom(src, Selectors.SELECT_SELF);


Running the tests
-----------------
For running tests you need active credentials for AWS. You can specify them as

1.  Shell environment properties

        export AWS_ACCESS_KEY=AAAAAAA
        export AWS_SECRET_KEY=SSSSSSS

2. Or any standard ways how to do it in AWS SDK (iam role and so on)


**Make sure that you never commit your credentials!**
