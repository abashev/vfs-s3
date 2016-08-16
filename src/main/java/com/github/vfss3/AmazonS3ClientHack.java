package com.github.vfss3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;

/**
 * Utility class to access AmazonS3Client internals.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
final class AmazonS3ClientHack {
    private static final Log log = LogFactory.getLog(AmazonS3ClientHack.class);

    private static Field awsCredentialsProviderField = null;

    static {
        try {
            awsCredentialsProviderField = AmazonS3Client.class.getDeclaredField("awsCredentialsProvider");

            awsCredentialsProviderField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            log.error("Not able to find 'awsCredentialsProvider' field inside AmazonS3Client.  Something wrong with your AWS SDK version.");
        }
    }

    /**
     * Extract current credentials from client object. Return null if not able to do it.
     *
     * @param client S3 client object for extracting current credentials
     * @return current S3 access credentials or null if not able to get them
     */
    public static AWSCredentials extractCredentials(AmazonS3 client) {
        if (!(client.getClass().equals(AmazonS3Client.class))) {
            throw new IllegalArgumentException("Not able to extract credentials from " + client.getClass());
        }

        AWSCredentialsProvider provider = null;

        try {
            provider = (AWSCredentialsProvider) awsCredentialsProviderField.get(client);
        } catch (IllegalAccessException e) {
            log.error("Not able to get access to 'awsCredentialsProvider'. Something wrong with your AWS SDK version.", e);
        }

        return (provider != null) ? provider.getCredentials() : null;
    }

    private AmazonS3ClientHack() {
    }
}
