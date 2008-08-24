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

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.AbstractFileNameParser;
import org.apache.commons.vfs.provider.UriParser;
import org.apache.commons.vfs.provider.VfsComponentContext;

/**
 * @author Matthias L. Jugel
 */
public class S3FileNameParser extends AbstractFileNameParser {
	private static final S3FileNameParser instance = new S3FileNameParser();

	public static S3FileNameParser getInstance() {
		return instance;
	}

	private S3FileNameParser() {

	}

	public FileName parseUri(final VfsComponentContext context,
			final FileName base, final String filename)
			throws FileSystemException {
		StringBuffer name = new StringBuffer();

		String scheme = UriParser.extractScheme(filename, name);
		UriParser.canonicalizePath(name, 0, name.length(), this);

		UriParser.fixSeparators(name);

		// Normalise the path
		FileType fileType = UriParser.normalisePath(name);

		// Extract the root prefix
		final String bucketName = UriParser.extractFirstElement(name);

		return new S3FileName(scheme, bucketName, name.toString(), fileType);
	}

}
