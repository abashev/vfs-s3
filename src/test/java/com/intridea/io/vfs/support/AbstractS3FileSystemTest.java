package com.intridea.io.vfs.support;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.testng.annotations.Guice;
import org.testng.annotations.Listeners;

import javax.inject.Inject;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
@Guice(modules = InitializeEnvironmentModule.class)
public abstract class AbstractS3FileSystemTest {
    @Inject protected FileSystemManager vfs;

    /**
     * Test configuration
     */
    @Inject protected TestEnvironment env;
}
