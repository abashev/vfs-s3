package com.github.vfss3;

import com.amazonaws.regions.Regions;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.*;

/**
 * Unit tests for {@link com.github.vfss3.S3FileSystemOptions}
 */
public class S3FileSystemOptionsTest {

    @Test
    public void testWriteReadRegion() {
        S3FileSystemOptions options = new S3FileSystemOptions();

        Regions expectedValue = Regions.DEFAULT_REGION;
        options.setRegion(expectedValue);
        Optional<Regions> actual = options.getRegion();

        assertNotNull(actual, "Region object cannot be null");
        assertTrue(actual.isPresent(), "Region cannot be null");
        assertEquals(actual.get(), expectedValue);
    }

    @Test
    public void testRegion() {
        S3FileSystemOptions options = new S3FileSystemOptions();
        Optional<Regions> region = options.getRegion();

        assertNotNull(region, "Region object cannot be null");
        assertFalse(region.isPresent(), "Region value should be empty");
    }
}
