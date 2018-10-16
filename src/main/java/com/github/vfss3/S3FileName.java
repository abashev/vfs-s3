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
package com.github.vfss3;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.FOLDER;

public class S3FileName extends AbstractFileName {
    /**
     * Host and port for S3 url - endpoint for the client
     */
    private final String hostAndPort;

    /**
     * First segment of path as a bucket
     */
    private final String pathPrefix;

    S3FileName(String hostAndPort, String pathPrefix, String path, FileType type) {
        super("s3", path, type);

        this.hostAndPort = requireNonNull(hostAndPort);

        if (pathPrefix != null && (pathPrefix.contains("/") || pathPrefix.contains(" ") || (pathPrefix.trim().length() == 0))) {
            throw new IllegalArgumentException("Path prefix [" + pathPrefix + "] shouldn't contain / and has to be valid bucket name");
        }

        this.pathPrefix = requireNonNull(pathPrefix);
    }

    @Override
    public FileName createName(String absPath, FileType type) {
        return new S3FileName(hostAndPort, pathPrefix, absPath, type);
    }

    @Override
    protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
        buffer.append(getScheme());
        buffer.append("://");
        buffer.append(hostAndPort);

        if (pathPrefix != null) {
            buffer.append('/').append(pathPrefix);
        }
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Returns S3 key from name or empty for a bucket.
     *
     * @return
     * @throws FileSystemException
     */
    public Optional<String> getS3Key() throws FileSystemException {
        if ((type != FILE) && (type != FOLDER)) {
            throw new FileSystemException("Not able to get key from imaginary file");
        }

        if (getPathDecoded().equals(ROOT_PATH)) {
            return empty();
        }

        StringBuilder path = new StringBuilder(getPathDecoded());

        if ((path.indexOf(SEPARATOR) == 0) && (path.length() > 1)) {
            path.deleteCharAt(0);
        }

        if (type == FOLDER) {
            path.append(SEPARATOR_CHAR);
        }

        return Optional.of(path.toString());
    }

    public String getS3KeyAs(FileType fileType) throws FileSystemException {
        StringBuilder path = new StringBuilder(getPathDecoded());

        if ((path.indexOf(SEPARATOR) == 0) && (path.length() > 1)) {
            path.deleteCharAt(0);
        }

        if (fileType == FOLDER) {
            path.append(SEPARATOR_CHAR);
        }

        return path.toString();
    }

    @Override
    public String toString() {
        return "S3FileName{" +
                "hostAndPort='" + hostAndPort + '\'' +
                ", pathPrefix='" + pathPrefix + '\'' +
                ", path='" + getPath() + '\'' +
                ", type='" + getType() + '\'' +
                '}';
    }
}
