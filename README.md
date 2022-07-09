Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================

## Latest branch 4.x.x

[![vfs-s3](https://maven-badges.herokuapp.com/maven-central/com.github.abashev/vfs-s3/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.abashev/vfs-s3)
[![Build with integration tests and code coverage](https://github.com/abashev/vfs-s3/actions/workflows/build.yml/badge.svg)](https://github.com/abashev/vfs-s3/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-4.x.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)

### How to add as dependency into your Maven build

For an artifact with embedded AWS SDK (an easiest way to bootstrap)

    <dependency>
        <groupId>com.github.abashev</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>4.3.6</version>
        <classifier>with-aws-sdk</classifier>
    </dependency>

For an artifact without dependencies 

    <dependency>
        <groupId>com.github.abashev</groupId>
        <artifactId>vfs-s3</artifactId>
        <version>4.3.6</version>
    </dependency>


### How to add as dependency into your Gradle build
    
    implementation 'com.github.abashev:vfs-s3:4.3.6'

### URL format

    s3://[[access-key:secret-key]:sign-region]@endpoint-url/folder-or-bucket/
    
- Access key and Secret key could come from default AWS SDK chain (environment, container etc)
- Sign-region is useful for custom implementations
- If endpoint URL from known providers then we will try to get bucket name from host, if not able to do it then bucket is first path segment

### Supported providers

Provider | URL | Example URL
----- | ------- | -------
Amazon S3 | https://aws.amazon.com/s3/ | s3://s3-tests.s3-eu-west-1.amazonaws.com
Yandex Object Storage | https://cloud.yandex.ru/services/storage | s3://s3-tests.storage.yandexcloud.net/
Mail.ru Cloud Storage | https://mcs.mail.ru/storage/ | s3://s3-tests.hb.bizmrg.com/
Alibaba Cloud Object Storage Service | https://www.alibabacloud.com/product/oss | s3://s3-tests.oss-eu-central-1.aliyuncs.com/
Oracle Cloud Object Storage | https://www.oracle.com/cloud/storage/object-storage.html | s3://frifqsbag2em.compat.objectstorage.eu-frankfurt-1.oraclecloud.com/s3-tests/
DigitalOcean Spaces Object Storage | https://www.digitalocean.com/products/spaces/ | s3://s3-tests2.ams3.digitaloceanspaces.com
SberCloud Object Storage Service | https://sbercloud.ru/ru/products/object-storage-service | s3://s3-tests.obs.ru-moscow-1.hc.sbercloud.ru

### Sample groovy scripts - https://github.com/abashev/vfs-s3/tree/branch-4.x.x/samples

`s3-copy` able to copy between clouds, via http url or between different accounts

    s3-copy s3://access1:secret1@s3-tests.storage.yandexcloud.net/javadoc.jar s3://access2:secret2@s3.eu-central-1.amazonaws.com/s3-tests-2/javadoc.jar

    s3-copy https://oss.sonatype.org/some-name/120133-1-javadoc.jar s3://s3.eu-central-1.amazonaws.com/s3-tests-2/javadoc.jar



### Sample Java Code for AWS S3

	// Create a folder
	FileSystemManager fsManager = VFS.getManager();
	FileObject dir = fsManager.resolveFile("s3://simple-bucket.s3-eu-west-1.amazonaws.com/test-folder/");
	dir.createFolder();

	// Upload file to S3
	FileObject dest = fsManager.resolveFile("s3://s3-eu-west-1.amazonaws.com/test-bucket/backup.zip");
	FileObject src = fsManager.resolveFile(new File("/path/to/local/file.zip").getAbsolutePath());
	dest.copyFrom(src, Selectors.SELECT_SELF);

### Sample Java Code for Yandex Cloud https://cloud.yandex.ru/

	// Upload file
	FileObject dest = fsManager.resolveFile("s3://s3-tests.storage.yandexcloud.net/backup.zip");
	FileObject src = fsManager.resolveFile(new File("/path/to/local/file.zip").getAbsolutePath());
	dest.copyFrom(src, Selectors.SELECT_SELF);
    

### Running tests

For running tests you need active credentials for AWS. You can specify them as

1.  Shell environment properties

        export AWS_ACCESS_KEY=AAAAAAA
        export AWS_SECRET_KEY=SSSSSSS
        export BASE_URL=s3://<full url like simple-bucket.s3-eu-west-1.amazonaws.com or s3-eu-west-1.amazonaws.com/test-bucket>

2. Or any standard ways how to do it in AWS SDK (iam role and so on)


**Make sure that you never commit your credentials!**

### TODO 

- [x] Shadow all dependencies inside vfs-s3 artifact

### Old releases 

Branch       |  Build Status | Code coverage
------------ | ------------ | ------------
branch-3.0.x |  [![Build Status](https://travis-ci.org/abashev/vfs-s3.svg?branch=branch-3.0.x)](https://travis-ci.org/abashev/vfs-s3) | [![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-3.0.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.4.x |  [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.4.x)](http://travis-ci.org/abashev/vfs-s3) | [![codecov](https://codecov.io/gh/abashev/vfs-s3/branch/branch-2.4.x/graph/badge.svg)](https://codecov.io/gh/abashev/vfs-s3)
branch-2.3.x |  [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.3.x)](http://travis-ci.org/abashev/vfs-s3) |
branch-2.2.x |  [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.2.x)](http://travis-ci.org/abashev/vfs-s3) |
