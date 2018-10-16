package com.github.vfss3;

import com.amazonaws.regions.Regions;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger log = LoggerFactory.getLogger(S3FileNameParser.class);

    private static final Pattern HOST_PATTERN = compile("((?<bucket>[a-z0-9\\-]+)\\.)?s3-?(?<region>[a-z0-9\\-]*)\\.amazonaws\\.com");

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
        log.debug("Parse uri [context={},base={},filename={}", context, base, filename);

        URI uri;

        try {
            uri = new URI(filename);
        } catch (URISyntaxException e) {
            throw new FileSystemException(e);
        }

        if (!uri.getScheme().equalsIgnoreCase("s3")) {
            throw new FileSystemException("vfs.impl/unknown-scheme.error", uri.getScheme(), filename);
        }

        if ((uri.getHost() == null) || (uri.getHost().trim().length() == 0)) {
            throw new FileSystemException("Not able to find host in url [" + filename + "]");
        }

        final Matcher hostNameMatcher = HOST_PATTERN.matcher(uri.getHost());

        if (hostNameMatcher.matches()) {
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

            S3FileName file = buildS3FileName(host, bucket, key);

            log.debug("From uri {} got {}", filename, file);

            return file;
        } else {
            // Custom endpoint like localstack
            StringBuilder host = new StringBuilder(uri.getHost());

            if (uri.getPort() != (-1)) {
                host.append(':').append(uri.getPort());
            }

            final Matcher pathMatcher = PATH.matcher(uri.getPath());

            if (pathMatcher.matches()) {
                S3FileName file = buildS3FileName(host, pathMatcher);

                log.debug("From uri {} got {}", filename, file);

                return file;
            } else {
                throw new FileSystemException("Not able to find bucket inside [" + filename + "]");
            }
        }
    }

    /**
     * Extract region name from host name.
     *
     * @param host
     * @param defaultRegion
     * @return
     */
    public String regionFromHost(String host, String defaultRegion) {
        final Matcher hostNameMatcher = HOST_PATTERN.matcher(host);

        if (hostNameMatcher.matches()) {
            String candidate = hostNameMatcher.group("region");
            Regions region = null;

            if ((candidate != null) && (candidate.trim().length() > 0)) {
                try {
                    region = Regions.fromName(candidate);
                } catch (IllegalArgumentException e) {
                }
            }

            if (region != null) {
                return region.getName();
            }
        }

        return defaultRegion;
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

    private S3FileName buildS3FileName(String host, String bucket, String key) {
        if ((key != null) && (key.trim().length() > 0) && (!key.equals(ROOT_PATH))) {
            StringBuilder sb = new StringBuilder(key);

            UriParser.fixSeparators(sb);

            key = sb.toString();
        }

        FileType type = (ROOT_PATH.equals(key)) ? FOLDER : IMAGINARY;

        return (new S3FileName(host, bucket, key, type));
    }

    private S3FileName buildS3FileName(StringBuilder host, Matcher pathMatcher) throws FileSystemException {
        return buildS3FileName(host.toString(), pathMatcher.group("bucket"), pathMatcher.group("key"));
    }
}
