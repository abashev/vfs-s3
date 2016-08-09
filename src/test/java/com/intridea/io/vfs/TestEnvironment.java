package com.intridea.io.vfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TestEnvironment {
    public static final String ACCESS_KEY = "aws.accessKey";
    public static final String SECRET_KEY = "aws.secretKey";

    private static final String NO_CREDENTIALS_MESSAGE =
            "Did you forget to initialize 'config.properties' with correct AWS credentials?";

    private static TestEnvironment instance;
    static {
        try {
            instance = new TestEnvironment();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static TestEnvironment getInstance () {
        return instance;
    }

    private Properties config;

    private TestEnvironment() throws IOException {
        // Load configuration
        config = new Properties();

        try (InputStream configFile = TestEnvironment.class.getResourceAsStream("/config.properties")) {
            assertNotNull(configFile);

            config.load(configFile);
        };

        assertFalse(isEmpty(config.getProperty(ACCESS_KEY)), NO_CREDENTIALS_MESSAGE);
        assertFalse(isEmpty(config.getProperty(SECRET_KEY)), NO_CREDENTIALS_MESSAGE);

        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, config.getProperty(ACCESS_KEY));
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, config.getProperty(SECRET_KEY));
    }

    public Properties getConfig () {
        return config;
    }

    private final boolean isEmpty(String s) {
        return ((s == null) || (s.length() == 0));
    }
}
