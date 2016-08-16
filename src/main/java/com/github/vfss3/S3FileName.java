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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

public class S3FileName extends AbstractFileName {

    private final String s3BucketId;

    protected S3FileName(final String scheme, final String bucketId, final String path, final FileType type) {
        super(scheme, path, type);
        this.s3BucketId = bucketId;
    }

    public String getBucketId() {
        return s3BucketId;
    }


    @Override
    public FileName createName(String absPath, FileType type) {
        return new S3FileName(getScheme(), s3BucketId, absPath, type);
    }

    @Override
    protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
        buffer.append(getScheme());
        buffer.append("://");
        buffer.append(s3BucketId);
    }
}
