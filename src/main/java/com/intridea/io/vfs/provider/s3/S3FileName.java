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

import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.local.LocalFileName;

public class S3FileName extends LocalFileName {
    protected S3FileName(final String scheme, final String rootFile, final String path, final FileType type) {
        super(scheme, rootFile, path, type);
    }
}
