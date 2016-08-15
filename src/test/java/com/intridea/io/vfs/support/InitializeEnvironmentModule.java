package com.intridea.io.vfs.support;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.amazonaws.SDKGlobalConfiguration.*;
import static java.lang.System.setProperty;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
class InitializeEnvironmentModule implements Module {
    private final Logger log = LoggerFactory.getLogger(InitializeEnvironmentModule.class);

    @Override
    public void configure(Binder binder) {
        initializeCredentials();

        FileSystemManager fileSystemManager;

        try {
            fileSystemManager = VFS.getManager();
        } catch (FileSystemException e) {
            throw new IllegalStateException("Not able to initialize VFS manager", e);
        }

        binder.bind(FileSystemManager.class).toInstance(fileSystemManager);
        binder.bind(TestEnvironment.class).toInstance(new TestEnvironment(fileSystemManager));
    }

    private void initializeCredentials() {
        // Try to load access and secret key from environment
        try {
            if ((new EnvironmentVariableCredentialsProvider()).getCredentials() != null) {
                log.info("Will use AWS credentials from enviroment variables");
            }
        } catch (AmazonClientException e) {
            log.info("Not able to load credentials from enviroment - try .envrc file");
        }

        EnvironmentConfiguration configuration = new EnvironmentConfiguration();

        configuration.computeIfPresent(ACCESS_KEY_ENV_VAR, v -> setProperty(ACCESS_KEY_SYSTEM_PROPERTY, v));
        configuration.computeIfPresent(SECRET_KEY_ENV_VAR, v -> setProperty(SECRET_KEY_SYSTEM_PROPERTY, v));
    }
}
