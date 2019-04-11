Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================

## Latest branch 3.0.x

[![vfs-s3](https://maven-badges.herokuapp.com/maven-central/com.github.abashev/vfs-s3/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.abashev/vfs-s3)
[![Custom commons-vfs2](https://maven-badges.herokuapp.com/maven-central/com.github.abashev/commons-vfs2/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.abashev/commons-vfs2)
[![Build Status](https://travis-ci.org/abashev/vfs-s3.svg?branch=branch-3.0.x)](https://travis-ci.org/abashev/vfs-s3)
[![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-3.0.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)

### How to add as dependency into your Maven build

    <dependency>
        <groupId>com.github.abashev</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>3.0.0.RC2</version>
    </dependency>

_Please be aware that vfs-s3 is using custom build of commons-vfs2. It works in the same way as original commons-vfs2 but it was patched to fix some concurrency issues._

### How to add as dependency into your Gradle build
    
    implementation 'com.github.abashev:commons-vfs2:2.2-20181130'
    implementation 'com.github.abashev:vfs-s3:3.0.0.RC2'

### Sample Java Code

	// Create a folder
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
- [x] Fix tests for 3.x branch
- [x] Submit Release Candidate or SNAPSHOT builds for 3.x branch
- [x] Submit artifacts to Maven central
- [ ] Merge changes into `commons-vfs` project
- [ ] Add ability to work with vanilla `commons-vfs`
- [ ] Support commons-vfs as forked submodule 
- [ ] Add support for Yandex cloud
- [ ] Shadow all dependencies inside vfs-s3 artifact

### Old releases 

Branch       |  Build Status | Code coverage
------------ | ------------ | ------------
branch-2.4.x |  [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.4.x)](http://travis-ci.org/abashev/vfs-s3) | [![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-2.4.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.3.x |  [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.3.x)](http://travis-ci.org/abashev/vfs-s3) |
branch-2.2.x |  [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.2.x)](http://travis-ci.org/abashev/vfs-s3) |
