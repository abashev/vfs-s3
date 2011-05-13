Amazon S3 driver for VFS (Apache Commons Virtual File System)
=============================================================

This code is based on <http://code.google.com/p/vfs-s3/> which [is no longer supported.](http://code.google.com/p/vfs-s3/issues/detail?id=4)

It provides S3 support for [Commons VFS](http://commons.apache.org/vfs/). 


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