/*
 * Copyright 2007 Matthias L. Jugel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intridea.io.vfs.provider.s3;

import org.apache.commons.vfs.Capability;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs.util.UserAuthenticatorUtils;
import org.apache.commons.vfs.UserAuthenticationData;
import org.apache.log4j.Logger;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 * 
 * @author Marat Komarov
 * @author Matthias L. Jugel
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {

	public final static Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(
			Capability.CREATE,
			Capability.DELETE,
			// TODO: rename not supported by jets3t now
			//Capability.RENAME, 
			Capability.GET_TYPE,
			Capability.GET_LAST_MODIFIED,
			Capability.SET_LAST_MODIFIED_FILE,
			Capability.SET_LAST_MODIFIED_FOLDER,
			Capability.LIST_CHILDREN, 
			Capability.READ_CONTENT,
			Capability.URI, 
			Capability.WRITE_CONTENT
			//Capability.APPEND_CONTENT
	));
	
	public final static UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
		UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
	};	
	
	private static FileSystemOptions defaultOptions = new FileSystemOptions();
	
	public static FileSystemOptions getDefaultFileSystemOptions () {
		return defaultOptions;
	}
	
	private S3Service service;

	private Logger logger;
	
	public S3FileProvider() {
		super();
		logger = Logger.getLogger(getClass());		
		setFileNameParser(S3FileNameParser.getInstance());
	}

	/**
	 * Create a file system with the S3 root provided.
	 * 
	 * @param fileName
	 *            the S3 file name that defines the root (bucket)
	 * @param fileSystemOptions
	 *            file system options
	 * @return an S3 file system
	 * @throws FileSystemException
	 *             if te file system cannot be created
	 */
	protected FileSystem doCreateFileSystem(FileName fileName,
			FileSystemOptions fileSystemOptions) throws FileSystemException {
		
		FileSystemOptions fsOptions = fileSystemOptions != null ? 
				fileSystemOptions : getDefaultFileSystemOptions();
		
		if (service == null) {
			UserAuthenticationData authData = null;
			try {
				authData = UserAuthenticatorUtils.authenticate(fsOptions, AUTHENTICATOR_TYPES);
				
				logger.info("Authenticated to Amazon S3");
				String keyId = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData, UserAuthenticationData.USERNAME, null));
				String key = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData, UserAuthenticationData.PASSWORD, null)); 
				if (keyId.length() + key.length() == 0) {
					throw new FileSystemException("Empty AWS credentials");
				}
				
				AWSCredentials awsCredentials = new AWSCredentials(keyId, key);
				service = new RestS3Service(awsCredentials);
				logger.info("Success");				
			} catch (S3ServiceException e) {
				System.err.println(e.getS3ErrorMessage());
				throw new FileSystemException(e);
			}
			finally {
				UserAuthenticatorUtils.cleanup(authData);
			}
		}
		
		return new S3FileSystem((S3FileName) fileName, service, fsOptions);
	}
	
	/**
	 * Get the capabilities of the file system provider.
	 * 
	 * @return the file system capabilities
	 */
	public Collection<Capability> getCapabilities() {
		return capabilities;
	}
}
