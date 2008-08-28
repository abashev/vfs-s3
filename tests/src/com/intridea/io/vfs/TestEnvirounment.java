package com.intridea.io.vfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.auth.StaticUserAuthenticator;
import org.apache.commons.vfs.impl.DefaultFileSystemConfigBuilder;
import org.apache.log4j.PropertyConfigurator;

import com.intridea.io.vfs.provider.s3.S3FileProvider;

public class TestEnvirounment {
	
	private static TestEnvirounment instance;
	static {
		try {
			instance = new TestEnvirounment();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static TestEnvirounment getInstance () {
		return instance;
	}
	
	private Properties config;
	
	private TestEnvirounment () throws FileNotFoundException, IOException {
		// Load configuration
		config = new Properties();
		config.load(new FileInputStream("config.properties"));
		
		// Configure logger
		PropertyConfigurator.configure(config);
		
		// Configure VFS
		StaticUserAuthenticator auth = new StaticUserAuthenticator(null, config.getProperty("aws.key-id"), config.getProperty("aws.key"));
		FileSystemOptions opts = S3FileProvider.getDefaultFileSystemOptions();
		DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
	}
	
	public Properties getConfig () {
		return config;
	}
}
