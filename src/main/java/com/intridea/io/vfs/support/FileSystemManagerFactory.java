package com.intridea.io.vfs.support;

import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import com.intridea.io.vfs.provider.s3.S3FileProvider;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 * @version $Id$
 */
public class FileSystemManagerFactory implements FactoryBean<FileSystemManager> {
    private String awsAccessKey;
    private String awsSecretKey;

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
    @Override
    public FileSystemManager getObject() throws Exception {
        // Configure VFS
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, awsAccessKey, awsSecretKey);
        FileSystemOptions opts = S3FileProvider.getDefaultFileSystemOptions();
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

        return VFS.getManager();
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.FactoryBean#getObjectType()
     */
    @Override
    public Class<?> getObjectType() {
        return FileSystemManager.class;
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.FactoryBean#isSingleton()
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * @param awsAccessKey the awsAccessKey to set
     */
    @Required
    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    /**
     * @param awsSecretKey the awsSecretKey to set
     */
    @Required
    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }
}