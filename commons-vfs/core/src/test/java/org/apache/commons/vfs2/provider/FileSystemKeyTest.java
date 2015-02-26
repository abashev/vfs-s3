/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2.provider;

import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class FileSystemKeyTest {
    @Test
    public void compareKeys() {
        assertEquals(new FileSystemKey("key", null), new FileSystemKey("key", null));
        assertEquals(new FileSystemKey("key", null), new FileSystemKey("key", new FileSystemOptions()));

        FileSystemOptions opts1 = new FileSystemOptions();

        FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(opts1, 100);

        FileSystemOptions opts2 = new FileSystemOptions();

        FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(opts2, 100);

        assertEquals(new FileSystemKey("key", opts1), new FileSystemKey("key", opts2));

        FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(opts2, 200);

        assertNotEquals(new FileSystemKey("key", opts1), new FileSystemKey("key", opts2));

    }

    @Test
    public void saveNullKey() {
        Map<FileSystemKey, Object> map = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            map.put(new FileSystemKey("key", null), i);
        }

        assertEquals(1, map.size());
        assertEquals(9, map.get(new FileSystemKey("key", null)));
    }

    @Test
    public void saveNotEmptyKey() {
        Map<FileSystemKey, Object> map = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            FileSystemOptions opts = new FileSystemOptions();

            FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(opts, 500);

            map.put(new FileSystemKey("key", opts), i);
        }

        FileSystemOptions opts1 = new FileSystemOptions();

        FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(opts1, 500);

        assertEquals(1, map.size());
        assertEquals(9, map.get(new FileSystemKey("key", opts1)));
    }
}