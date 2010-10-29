package com.intridea.io.vfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.auth.StaticUserAuthenticator;
import org.apache.commons.vfs.impl.DefaultFileSystemConfigBuilder;
import org.testng.Assert;

import com.intridea.io.vfs.provider.s3.S3FileProvider;

public class TestEnvironment {

	private static TestEnvironment instance;
	static {
		try {
			instance = new TestEnvironment();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static TestEnvironment getInstance () {
		return instance;
	}

	private Properties config;

	private TestEnvironment () throws FileNotFoundException, IOException {
		// Load configuration
		config = new Properties();

		InputStream configFile = TestEnvironment.class.getResourceAsStream("/config.properties");

		Assert.assertNotNull(configFile);

        config.load(configFile);

		// Configure logger
//		PropertyConfigurator.configure(config);

		// Configure VFS
		StaticUserAuthenticator auth = new StaticUserAuthenticator(null, config.getProperty("aws.key-id"), config.getProperty("aws.key"));
		FileSystemOptions opts = S3FileProvider.getDefaultFileSystemOptions();
		DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
	}

	public Properties getConfig () {
		return config;
	}
}
