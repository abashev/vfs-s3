package com.intridea.io.vfs.provider.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.KMSEncryptionMaterials;
import org.testng.annotations.Test;

import static com.intridea.io.vfs.provider.s3.AmazonS3ClientHack.extractCredentials;
import static org.testng.Assert.*;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class AmazonS3ClientHackTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void wrongClass() throws Exception {
        AmazonS3 s3 = new AmazonS3EncryptionClient(new KMSEncryptionMaterials("test string"));

        extractCredentials(s3);
    }

    @Test
    public void staticCredentials() throws Exception {
        AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials("access", "secret"));

        AWSCredentials credentials = extractCredentials(s3);

        assertNotNull(credentials);
        assertEquals(credentials.getAWSAccessKeyId(), "access");
        assertEquals(credentials.getAWSSecretKey(), "secret");
    }
}
