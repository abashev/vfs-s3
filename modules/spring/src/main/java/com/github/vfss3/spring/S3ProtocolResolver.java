package com.github.vfss3.spring;

import static java.util.Objects.requireNonNull;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.lang.Nullable;

/**
 * Spring {@link ProtocolResolver} that plugs {@code s3://} support into an existing resource
 * loader or application context — the seam that makes {@code s3://} work in {@code @Value},
 * {@code @PropertySource} and any ambient {@code getResource(…)} call without replacing the
 * application's own {@code ResourceLoader}:
 *
 * <pre>{@code
 * var s3Loader = new S3ResourceLoader();
 * context.addProtocolResolver(new S3ProtocolResolver(s3Loader));   // before refresh()
 * // anywhere in the app now: @Value("s3://my-bucket/config/app.properties") Resource config;
 * }</pre>
 *
 * <p>The resolver never owns the loader's lifecycle — close the {@link S3ResourceLoader} where
 * it was created (as a Spring bean its {@code close()} destroy method is inferred).
 */
public final class S3ProtocolResolver implements ProtocolResolver {

    private final S3ResourceLoader loader;

    /** Creates a resolver delegating {@code s3://} locations to an externally-owned loader. */
    public S3ProtocolResolver(S3ResourceLoader loader) {
        this.loader = requireNonNull(loader, "loader");
    }

    @Nullable
    @Override
    public Resource resolve(String location, ResourceLoader resourceLoader) {
        return S3Uris.isS3Location(location) ? loader.getResource(location) : null;
    }
}
