package com.github.vfss3.spring7;

import java.io.IOException;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Spring {@link org.springframework.core.io.support.ResourcePatternResolver} that resolves
 * {@code s3://} glob patterns.
 *
 * <p>Currently delegates single-resource lookups to {@link S3ResourceLoader} and pattern-based
 * lookups raise {@link UnsupportedOperationException} until a real S3 backend is wired in.
 *
 * <p>Usage:
 * <pre>{@code
 * ResourcePatternResolver resolver = new S3ResourcePatternResolver();
 * Resource[] resources = resolver.getResources("s3://my-bucket/data/*.json");
 * }</pre>
 */
public class S3ResourcePatternResolver extends PathMatchingResourcePatternResolver {

    public S3ResourcePatternResolver() {
        super(new S3ResourceLoader());
    }

    @Override
    public Resource[] getResources(String locationPattern) throws IOException {
        if (locationPattern != null && locationPattern.startsWith(S3Resource.S3_SCHEME + "://")) {
            // Wildcard pattern support requires an S3 listing call — not yet implemented.
            if (getPathMatcher().isPattern(locationPattern)) {
                throw new UnsupportedOperationException(
                        "S3 wildcard patterns are not yet supported: " + locationPattern);
            }
            return new Resource[] {new S3Resource(locationPattern)};
        }
        return super.getResources(locationPattern);
    }
}
