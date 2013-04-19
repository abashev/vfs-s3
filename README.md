Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================


Branch | Description | Build Status
------------ | ------------- | ------------
branch-2.2.x | See below | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.2.x)](http://travis-ci.org/abashev/vfs-s3)
branch-2.1.x | **Current** Switch to Amazon SDK for better integration and stability | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.1.x)](http://travis-ci.org/abashev/vfs-s3)
branch-2.0.x | **Out-dated** It uses Jets3t as back-end for interracting with Amazon S3 | [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=branch-2.0.x)](http://travis-ci.org/abashev/vfs-s3)

Scope of branch-2.2.x development
---
1. Total refactoring for package names - move everything into com.github
1. Integration with Java 7 as file system provider
1. Make code more concurrency aware
1. 


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


Bootstraping with Spring
------------------------

The class `com.scoyo.commons.vfs.S3Util` can be used to easily bootstrap
the vfs-s3 provider with the Spring Framework:

	<bean id="S3Initializer" class="org.springframework.beans.factory.config.MethodInvokingFactoryBean" lazy-init="false">
		<property name="targetClass" value="com.scoyo.commons.vfs.S3Util" />
		<property name="targetMethod" value="initS3Provider" />
		<property name="arguments">
			<list>
				<value>${aws.key-id}</value>
				<value>${aws.key}</value>
			</list>
		</property>
	</bean>

After that you can use VFS as with any other file system.


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
    And run maven build with profile travis-ci `mvn test -Ptravis-ci`

1. Or you can update your settings.xml file with default or profile's properties

        <properties>
            <aws.accessKey>AAAAAAAAAAA</aws.accessKey>
            <aws.secretKey>SSSSSSSSSSS</aws.secretKey>
        </properties>

**Make sure that you never commit your credentials!**

***

This code is based on <http://code.google.com/p/vfs-s3/> which [is no longer supported.](http://code.google.com/p/vfs-s3/issues/detail?id=4)

