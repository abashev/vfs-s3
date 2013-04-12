Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================

Master branch: [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=master)](http://travis-ci.org/abashev/vfs-s3)

Develop branch: [![Build Status](https://secure.travis-ci.org/abashev/vfs-s3.png?branch=develop)](http://travis-ci.org/abashev/vfs-s3)


This code is based on <http://code.google.com/p/vfs-s3/> which [is no longer supported.](http://code.google.com/p/vfs-s3/issues/detail?id=4)

It provides S3 support for [Commons VFS](http://commons.apache.org/vfs/).

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
	FileObject dir = fsManager.resolveFile("s3://simpe-bucket");
	dir.createFolder();

	// Upload file to S3
	FileObject dest = fsManager.resolveFile("s3://test-bucket/backup.zip");
	FileObject src = fsManager.resolveFile(new File("/path/to/local/file.zip").getAbsolutePath());
	dest.copyFrom(src, Selectors.SELECT_SELF);


Running the tests
-----------------

Tests are currently disabled by default, because one has to provide AWS credentials and a large binary file.

To run the tests you have to edit the file pom.xml and change this section:
      
	<configuration>
		<skipTests>false</skipTests>
	</configuration>


Before running 'mvn test' you have to edit the file

	src/test/resources/config.properties

Fill your AWS Key and Id. Then change the bucket name, because it must be globally
unique (you can just add a suffix). Finally you have to provide the path to backup.zip which
is in the same directory.

Make sure that you never commit your credentials!
