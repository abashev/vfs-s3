Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================


Branch       | Description   | Build Status | Code coverage
------------ | ------------- | ------------ | ------------
branch-3.0.x | **Current**   | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-3.0.x)](http://travis-ci.org/abashev/vfs-s3) |[![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-3.0.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.4.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.4.x)](http://travis-ci.org/abashev/vfs-s3) | [![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-2.4.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.3.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.3.x)](http://travis-ci.org/abashev/vfs-s3) |
branch-2.2.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.2.x)](http://travis-ci.org/abashev/vfs-s3) |



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
        <groupId>com.github.abashev</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>3.0.0</version>
    </dependency>


By default, vfs-s3 depends on a patched build of commons-vfs2 2.2 to allow greatly improved performance when multiple
threads are using the same VFS FileSystemManager concurrently from multiple threads, however as of version 3.0.0 it will
also work with the standard release of commons-vfs2 2.2.

    <dependency>
        <groupId>com.github.abashev</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>3.0.0</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.commons</groupId>
                    <artifactId>commons-vfs2</artifactId>
                </exclusion>
            </exclusions>
    </dependency>

Direct downloads from Bintray
----------------

Branch | Build Status 
------------ |  ------------ 
commons-vfs2 | [![Download](https://api.bintray.com/packages/abashev/vfs-s3/commons-vfs2/images/download.svg) ](https://bintray.com/abashev/vfs-s3/commons-vfs2/_latestVersion) 
vfs-s3 | [![Download](https://api.bintray.com/packages/abashev/vfs-s3/vfs-s3/images/download.svg) ](https://bintray.com/abashev/vfs-s3/vfs-s3/_latestVersion)


TODO for branch-3.0.x development
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
