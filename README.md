Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================


Branch       | Description   | Build Status | Code coverage
------------ | ------------- | ------------ | ------------
branch-3.0.x | **Work in progress** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-3.0.x)](http://travis-ci.org/abashev/vfs-s3) |[![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-3.0.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.4.x | **Latest** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.4.x)](http://travis-ci.org/abashev/vfs-s3) | [![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-2.4.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.3.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.3.x)](http://travis-ci.org/abashev/vfs-s3) |
branch-2.2.x | **Out-dated** | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.2.x)](http://travis-ci.org/abashev/vfs-s3) |

## :bangbang: Note :bangbang:

If you are going to start new project then better to take version 3.0.0 - it is not compatible with version 2.x in terms of configuration, package names, urls and version 2.x not be supported with any fixes. But we have a lot of things to do on version 3.x :arrow_down:


### Using 2.4.x version with Maven

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
    
### Using 3.x version with Maven 

Version 3.x will be submitted to Maven Central and will be available as 

    <dependency>
        <groupId>com.github.abashev</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>3.0.0</version>
    </dependency>

But right now you have only one option - do checkout and build it with `mvn install`

### Direct downloads from Bintray

Branch | Build Status 
------------ |  ------------ 
commons-vfs2 | [![Download](https://api.bintray.com/packages/abashev/vfs-s3/commons-vfs2/images/download.svg) ](https://bintray.com/abashev/vfs-s3/commons-vfs2/_latestVersion) 
vfs-s3 | [![Download](https://api.bintray.com/packages/abashev/vfs-s3/vfs-s3/images/download.svg) ](https://bintray.com/abashev/vfs-s3/vfs-s3/_latestVersion)

### Sample Java Code for version 2.x

	// Create bucket
	FileSystemManager fsManager = VFS.getManager();
	FileObject dir = fsManager.resolveFile("s3://simple-bucket/test-folder/");
	dir.createFolder();

	// Upload file to S3
	FileObject dest = fsManager.resolveFile("s3://test-bucket/backup.zip");
	FileObject src = fsManager.resolveFile(new File("/path/to/local/file.zip").getAbsolutePath());
	dest.copyFrom(src, Selectors.SELECT_SELF);

### Sample Java Code for version 3.x

	// Create bucket
	FileSystemManager fsManager = VFS.getManager();
	FileObject dir = fsManager.resolveFile("s3://simple-bucket.s3-eu-west-1.amazonaws.com/test-folder/");
	dir.createFolder();

	// Upload file to S3
	FileObject dest = fsManager.resolveFile("s3://s3-eu-west-1.amazonaws.com/test-bucket/backup.zip");
	FileObject src = fsManager.resolveFile(new File("/path/to/local/file.zip").getAbsolutePath());
	dest.copyFrom(src, Selectors.SELECT_SELF);


### Running tests

For running tests you need active credentials for AWS. You can specify them as

1.  Shell environment properties

        export AWS_ACCESS_KEY=AAAAAAA
        export AWS_SECRET_KEY=SSSSSSS
        export AWS_TEST_BUCKET=<full url like simple-bucket.s3-eu-west-1.amazonaws.com or s3-eu-west-1.amazonaws.com/test-bucket>

2. Or any standard ways how to do it in AWS SDK (iam role and so on)


**Make sure that you never commit your credentials!**

### TODO 

- [ ] Fix tests for 2.x branch
- [ ] Fix tests for 3.x branch
- [ ] Submit Release Candidate or SNAPSHOT builds for 3.x branch
- [ ] Submit artifacts to Maven central
- [ ] Merge changes into `commons-vfs` project
- [ ] Add ability to work with vanilla `commons-vfs`
