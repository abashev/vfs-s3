package com.scoyo.commons.vfs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;

import com.intridea.io.vfs.provider.s3.S3FileProvider;

/**
 * Utility for initializing S3 VFS-provider.
 *
 * @author Achim Heiland
 * @author Moritz Siuts
 *
 */
@Deprecated
public class S3Util {

	private static final Log log = LogFactory.getLog(S3Util.class);

	/**
	 * Utility class. No instantiation allowed.
	 */
	private S3Util() {
	}

	/**
	 * Initialize S3 VFS provider.
	 *
	 * @param propertyFileLocation the property file containing aws.key-id and aws.key
	 *
	 * @throws IOException if the property file could not be read
	 */
    @Deprecated
	public static void initS3Provider(final String propertyFileLocation) throws IOException {
		// load authentication information via property file
		final Properties userConfig = new Properties();
		// look at the class path
		final InputStream propertyResource = S3Util.class.getResourceAsStream(propertyFileLocation);
		if (propertyResource == null) {
			throw new IOException(propertyFileLocation + " not found");
		}
		userConfig.load(propertyResource);
		// create authenticator
		final StaticUserAuthenticator userAuthenticator = new StaticUserAuthenticator(null, userConfig.getProperty("aws.key-id", ""),
				userConfig.getProperty("aws.key", ""));
		final FileSystemOptions options = S3FileProvider.getDefaultFileSystemOptions();
		DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, userAuthenticator);
		log.info("s3 provider initialized");
	}

	/**
	 * Initialize S3 VFS provider.
	 *
	 * @param awsKeyId the amazon s3 aws.key-id
	 * @param awsKey the amazon s3 aws.key
	 *
	 * @throws FileSystemException file system configuration could not be build
	 */
    @Deprecated
	public static void initS3Provider(final String awsKeyId, final String awsKey) throws FileSystemException {
		// create authenticator
		final StaticUserAuthenticator userAuthenticator = new StaticUserAuthenticator(null, awsKeyId, awsKey);
		final FileSystemOptions options = S3FileProvider.getDefaultFileSystemOptions();
		DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, userAuthenticator);
		log.info("s3 provider initialized");
	}
}
