package com.github.vfss3;

import com.amazonaws.regions.Regions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.vfs2.FileName.ROOT_PATH;
import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.FileType.IMAGINARY;

/**
 * @author Matthias L. Jugel
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class S3FileNameParser extends AbstractFileNameParser {
    private static final String DEFAULT_SIGNING_REGION = "us-east-1";

    private final Log log = LogFactory.getLog(S3FileNameParser.class);

    private static final Pattern AWS_HOST_PATTERN = compile("((?<bucket>[a-z0-9\\-]+)\\.)?s3[-.]((?<region>[a-z0-9\\-]+)\\.)?amazonaws\\.com");
    private static final Pattern YANDEX_HOST_PATTERN = compile("(?<bucket>[a-z0-9\\-]+)\\.storage\\.yandexcloud\\.net");

    private static final Pattern PATH = compile("^/+(?<bucket>[^/]+)/*(?<key>/.*)?");


    public S3FileNameParser() {
    }

    /**
     * Parses URI and constructs S3 file name.
     */
    @Override
    public FileName parseUri(
            VfsComponentContext context, FileName base, String filename
    ) throws FileSystemException {
        if (log.isDebugEnabled()) {
            log.debug("Parse uri [base=" + base + ",filename=" + filename + "]");
        }

        URI uri;

        try {
            uri = new URI(filename.replace(" ", "%20"));
        } catch (URISyntaxException e) {
            throw new FileSystemException(e);
        }

        if (!uri.getScheme().equalsIgnoreCase("s3")) {
            throw new FileSystemException("vfs.impl/unknown-scheme.error", uri.getScheme(), filename);
        }

        if ((uri.getHost() == null) || (uri.getHost().trim().length() == 0)) {
            throw new FileSystemException("Not able to find host in url [" + filename + "]");
        }

        if (base != null) {
            // We already have all configuration
            S3FileName file = buildS3FileName(base, filename);

            if (log.isDebugEnabled()) {
                log.debug("From [base=" + base + ",file=" + filename + "] got " + file);
            }

            return file;
        }

        Matcher hostNameMatcher;

        if ((hostNameMatcher = AWS_HOST_PATTERN.matcher(uri.getHost())).matches()) {
            // Standard AWS endpoint
            String region = hostNameMatcher.group("region");

            checkRegion(region);

            String bucket = hostNameMatcher.group("bucket");
            String host = uri.getHost();
            String key = uri.getPath();

            if ((bucket != null) && (bucket.trim().length() > 0)) {
                // Has bucket inside URL
                if ((region != null) && (region.trim().length() > 0)) {
                    host = "s3-" + region + ".amazonaws.com";
                } else {
                    host = "s3.amazonaws.com";
                }
            } else {
                final Matcher pathMatcher = PATH.matcher(uri.getPath());

                if (pathMatcher.matches()) {
                    String pathBucket = pathMatcher.group("bucket");

                    if ((pathBucket != null) && (pathBucket.trim().length() > 0)) {
                        bucket = pathMatcher.group("bucket");
                    }

                    key = pathMatcher.group("key");
                }
            }

            if ((bucket == null) || (bucket.trim().length() == 0)) {
                throw new FileSystemException("Not able to find bucket inside [" + filename + "]");
            }

            if (region == null) {
                region = DEFAULT_SIGNING_REGION;
            }

            S3FileName file = buildS3FileName(host, null, bucket, bucket, region, key, true);

            if (log.isDebugEnabled()) {
                log.debug("From uri " + filename + " got " + file);
            }

            return file;
        } else if ((hostNameMatcher = YANDEX_HOST_PATTERN.matcher(uri.getHost())).matches()) {
            String bucket = hostNameMatcher.group("bucket");
            String key = uri.getPath();

            S3FileName file = buildS3FileName(
                    "storage.yandexcloud.net", bucket, null, bucket, "ru-central1", key, false
            );

            if (log.isDebugEnabled()) {
                log.debug("From uri " + filename + " got " + file);
            }

            return file;
        } else {
            // Custom endpoint like localstack
            StringBuilder host = new StringBuilder(uri.getHost());

            if (uri.getPort() != (-1)) {
                host.append(':').append(uri.getPort());
            }

            final Matcher pathMatcher = PATH.matcher(uri.getPath());

            if (pathMatcher.matches()) {
                S3FileName file = buildS3FileName(
                        host.toString(),
                        null,
                        pathMatcher.group("bucket"),
                        pathMatcher.group("bucket"),
                        DEFAULT_SIGNING_REGION,
                        pathMatcher.group("key"),
                        false
                );

                if (log.isDebugEnabled()) {
                    log.debug("From uri " + filename + " got " + file);
                }

                return file;
            } else {
                throw new FileSystemException("Not able to find bucket inside [" + filename + "]");
            }
        }
    }

    /**
     * Check region for correct name.
     *
     * @param regionName
     * @throws FileSystemException
     */
    private void checkRegion(String regionName) throws FileSystemException {
        if ((regionName != null) && (regionName.trim().length() > 0)) {
            try {
                requireNonNull(Regions.fromName(regionName));
            } catch (IllegalArgumentException e) {
                throw new FileSystemException("Not able to parse region [" + regionName + "]");
            }
        }
    }

    private S3FileName buildS3FileName(FileName base, String key) {
        S3FileName s3Base = (S3FileName) base;

        return s3Base.createName(key.substring(s3Base.getRootURI().length()), IMAGINARY);
    }

    private S3FileName buildS3FileName(
            String endpoint, String urlPrefix, String pathPrefix,
            String bucket, String signingRegion,
            String key, boolean supportsSSE
    ) throws FileSystemException {
        if ((key == null) || (key.trim().length() == 0)) {
            key = ROOT_PATH;
        }

        if (!key.equals(ROOT_PATH)) {
            StringBuilder sb = new StringBuilder(key);

            UriParser.fixSeparators(sb);
            UriParser.normalisePath(sb);

            key = sb.toString();
        }

        FileType type = (ROOT_PATH.equals(key)) ? FOLDER : IMAGINARY;

        return (new S3FileName(endpoint, urlPrefix, pathPrefix, bucket, signingRegion, key, type, supportsSSE));
    }
}
