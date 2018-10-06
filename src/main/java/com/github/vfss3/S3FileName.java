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
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

import static java.util.Objects.requireNonNull;

public class S3FileName extends AbstractFileName {
    /**
     * Host and port for S3 url - endpoint for the client
     */
    private final String hostAndPort;

    /**
     * First segment of path - for path-style file name it bucket, for virtual-host it is empty
     */
    private final String pathPrefix;

    protected S3FileName(String hostAndPort, String pathPrefix, String path, FileType type) {
        super("s3", path, type);

        this.hostAndPort = requireNonNull(hostAndPort);

        if (pathPrefix != null && (pathPrefix.contains("/") || pathPrefix.contains(" ") || (pathPrefix.trim().length() == 0))) {
            throw new IllegalArgumentException("Path prefix [" + pathPrefix + "] shouldn't contain / and has to be valid bucket name");
        }

        this.pathPrefix = pathPrefix;
    }

    protected S3FileName(String hostAndPort, String path, FileType type) {
        this(hostAndPort, null, path, type);
    }

    protected S3FileName(StringBuilder hostAndPort, String pathPrefix, String path, FileType type) {
        this(hostAndPort.toString(), pathPrefix, path, type);
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

    public boolean isPathPrefixNotEmpty() {
        return (pathPrefix != null);
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
