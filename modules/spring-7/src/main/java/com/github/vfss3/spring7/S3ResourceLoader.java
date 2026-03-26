package com.github.vfss3.spring7;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

/**
 * Spring {@link org.springframework.core.io.ResourceLoader} that resolves {@code s3://} URIs
 * to {@link S3Resource} instances, delegating all other URIs to Spring's default resolution.
 *
 * <p>Usage:
 * <pre>{@code
 * ResourceLoader loader = new S3ResourceLoader();
 * Resource resource = loader.getResource("s3://my-bucket/path/to/object.txt");
 * }</pre>
 */
public class S3ResourceLoader extends DefaultResourceLoader {

    @Override
    public Resource getResource(String location) {
        if (location != null && location.startsWith(S3Resource.S3_SCHEME + "://")) {
            return new S3Resource(location);
        }
        return super.getResource(location);
    }
}
